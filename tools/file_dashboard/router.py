from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List
import urllib.error
from urllib.parse import urlparse
import urllib.request
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse

from backend.shared.cors_utils import build_cors_headers
from db.maria_operations import maria_execute_query
from db.mysql_db_config import mysql_execute_query
from tools.file_dashboard.paths import dashboard_html_path, seed_urls_count_html_path
from utils.attachment_url_normalize import (
    canonicalize_attachment_url_for_learn_list,
    extract_attachment_key_candidates,
)
from utils.url import canonicalize_url_for_dedup, ensure_url_scheme


router = APIRouter(prefix="/file-dashboard", tags=["file-dashboard"])
api_router = APIRouter(tags=["file-dashboard"])
logger = logging.getLogger("tools.file_dashboard")
STATUS_SNAPSHOT_TIMEOUT_SEC = 1.0
STATUS_COUNTER_KEYS = (
    "total_count",
    "scan_count",
    "collection_count",
    "save_count",
    "save_done_count",
    "save_success_count",
    "save_failed_count",
    "study_count",
    "study_done_count",
    "study_success_count",
    "study_failed_count",
    "study_skipped_count",
    "file_study_count",
    "file_study_done_count",
    "file_study_success_count",
    "file_study_failed_count",
    "file_study_skipped_count",
    "file_duplicate_reuse_learned_count",
)
_STATUS_STATS_CACHE: Dict[str, Dict[str, Any]] = {}
EXPLORATION_TABLE = "ASADAL_CRAWLING_EXPLORATION"
LEARN_LIST_TABLE = "ASADAL_CRAWLING_LEARN_LIST"
EXPLORATION_SELECT_CANDIDATES = (
    "id",
    "url",
    "type",
    "chat_bot_id",
    "learn_list_id",
    "is_active",
    "merge_status",
    "reg_date",
    "content_created_at",
    "content_updated_at",
    "created_at",
    "updated_at",
)


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def file_dashboard_page() -> HTMLResponse:
    html_path = dashboard_html_path()
    return HTMLResponse(
        html_path.read_text(encoding="utf-8"),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )


@router.get("/seed-urls", response_class=HTMLResponse)
@router.get("/seed_urls", response_class=HTMLResponse)
async def seed_urls_count_page() -> HTMLResponse:
    html_path = seed_urls_count_html_path()
    return HTMLResponse(
        html_path.read_text(encoding="utf-8"),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )


async def _request_json_object(request: Request) -> Dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as exc:
        logger.warning("[FileDashboard] invalid json body | error=%s", exc)
        return {}
    if isinstance(payload, dict):
        return payload
    logger.warning("[FileDashboard] unsupported body type | body_type=%s", type(payload).__name__)
    return {}


async def _table_columns(db_name: str, table_name: str) -> List[str]:
    rows = await maria_execute_query(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """,
        (db_name, table_name),
        fetch=True,
        dbname=db_name,
    )
    columns: List[str] = []
    for row in rows or []:
        if isinstance(row, dict):
            name = row.get("COLUMN_NAME") or row.get("column_name")
        else:
            name = None
        if name:
            columns.append(str(name))
    return columns


def _safe_int(value: Any, default: int, *, minimum: int = 0, maximum: int = 50000) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))


def _has_value(data: Dict[str, Any], key: str) -> bool:
    return key in data and data.get(key) is not None and data.get(key) != ""


def _stats_from_snapshot_fallback(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    stats = dict(snapshot.get("stats") or {})
    if stats:
        return stats
    history = snapshot.get("history") if isinstance(snapshot.get("history"), dict) else {}
    final_stats = history.get("final_stats") if isinstance(history.get("final_stats"), dict) else {}
    if final_stats:
        return dict(final_stats)
    memory_state = snapshot.get("memory_state") if isinstance(snapshot.get("memory_state"), dict) else {}
    return {key: memory_state.get(key) for key in STATUS_COUNTER_KEYS if _has_value(memory_state, key)}


def _stabilize_status_stats(job_id: str, snapshot: Dict[str, Any]) -> Dict[str, Any]:
    stats = _stats_from_snapshot_fallback(snapshot)
    previous = _STATUS_STATS_CACHE.get(job_id) or {}
    if not stats and previous:
        return dict(previous)
    if not stats:
        return {}

    out = dict(stats)
    for left, right in (("scan_count", "total_count"), ("total_count", "scan_count")):
        if not _has_value(out, left) and _has_value(out, right):
            out[left] = out.get(right)

    # Dashboard polls can time out into a minimal snapshot with empty stats.
    # Reuse previous keys only when the new snapshot omits them entirely; real
    # final stats are allowed to correct an optimistic live counter downward.
    for key in STATUS_COUNTER_KEYS:
        if not _has_value(out, key) and _has_value(previous, key):
            out[key] = previous.get(key)

    _STATUS_STATS_CACHE[job_id] = dict(out)
    return out


def _short(value: Any, limit: int = 220) -> str:
    text = str(value or "")
    return text[:limit]


def _target_domains_from_urls(urls: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in urls or []:
        try:
            host = urlparse(ensure_url_scheme(str(raw or "").strip())).netloc
        except Exception:
            host = ""
        host = host.strip().lower()
        if host and host not in seen:
            seen.add(host)
            out.append(host)
    return out


def _target_url_from_payload(payload: Dict[str, Any]) -> str:
    raw = (
        payload.get("contents_url")
        or payload.get("access_url")
        or payload.get("target_url")
        or ""
    )
    if isinstance(raw, list):
        raw = raw[0] if raw else ""
    try:
        return ensure_url_scheme(str(raw or "").strip())
    except Exception:
        return str(raw or "").strip()


def _is_domain_root_url(url: str) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(ensure_url_scheme(url))
    except Exception:
        return False
    return bool(parsed.scheme and parsed.netloc) and (parsed.path or "/") in {"", "/"} and not parsed.query and not parsed.fragment


def _domain_root_url_variants(url: str) -> List[str]:
    try:
        parsed = urlparse(ensure_url_scheme(url))
    except Exception:
        return []
    host = parsed.netloc.strip().lower()
    if not host:
        return []
    hosts = [host]
    if host.startswith("www."):
        hosts.append(host[4:])
    else:
        hosts.append(f"www.{host}")
    out: List[str] = []
    seen: set[str] = set()
    for scheme in ("https", "http"):
        for h in hosts:
            for suffix in ("", "/"):
                candidate = f"{scheme}://{h}{suffix}"
                if candidate not in seen:
                    seen.add(candidate)
                    out.append(candidate)
    return out


def _domain_child_url_like_patterns(url: str) -> List[str]:
    try:
        parsed = urlparse(ensure_url_scheme(url))
    except Exception:
        return []
    host = parsed.netloc.strip().lower()
    if not host:
        return []
    hosts = [host]
    if host.startswith("www."):
        hosts.append(host[4:])
    else:
        hosts.append(f"www.{host}")
    out: List[str] = []
    seen: set[str] = set()
    for scheme in ("https", "http"):
        for h in hosts:
            for pattern in (
                f"{scheme}://{h}/%",
                f"{scheme}://%.{h}/%",
            ):
                if pattern not in seen:
                    seen.add(pattern)
                    out.append(pattern)
    return out


def _dedupe_target_rows(rows: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        url = str(row.get("url") or row.get("content_value") or row.get("content") or "").strip()
        if not url:
            continue
        key = canonicalize_url_for_dedup(url) or url
        key = key.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped, max(0, len(rows or []) - len(deduped))


def _attachment_match_keys(url: Any) -> set[str]:
    keys: set[str] = set()
    raw = str(url or "").strip()
    if not raw:
        return keys
    for value in (
        raw,
        canonicalize_attachment_url_for_learn_list(raw),
        canonicalize_url_for_dedup(raw),
    ):
        if value:
            keys.add(str(value).strip().lower())
    for value in extract_attachment_key_candidates(raw):
        if value:
            keys.add(str(value).strip().lower())
    return {key for key in keys if key}


def _learn_row_matches_attachment(row: Dict[str, Any], attachment_url: str) -> bool:
    row_content = str((row or {}).get("content") or "").strip()
    if not row_content or not attachment_url:
        return False
    attach_keys = _attachment_match_keys(attachment_url)
    row_keys = _attachment_match_keys(row_content)
    if attach_keys & row_keys:
        return True
    row_content_lc = row_content.lower()
    return any(key and key in row_content_lc for key in attach_keys)


async def _load_file_learn_rows_for_sources(
    *,
    db_name: str,
    chat_bot_id: str,
    source_urls: List[str],
    limit_per_source: int = 30,
) -> Dict[str, List[Dict[str, Any]]]:
    if not db_name or not chat_bot_id or not source_urls:
        return {}
    from db.mariadb_save_update import (
        ensure_learn_list_standard_columns,
        resolve_learn_list_table_name_for_chatbot,
    )

    learn_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    if not learn_table:
        return {}
    cols = await ensure_learn_list_standard_columns(db_name, learn_table)
    if not cols:
        return {}
    if "source_page" not in cols and "content" not in cols:
        return {}

    select_candidates = (
        "id",
        "subject",
        "content",
        "status",
        "source_page",
    )
    select_cols = [col for col in select_candidates if col in cols]
    if "id" not in select_cols:
        select_cols.insert(0, "id")
    where_parts = []
    if "content_type" in cols:
        where_parts.append("LOWER(COALESCE(`content_type`, '')) = 'file'")
    if "status" in cols:
        where_parts.append("UPPER(COALESCE(`status`, '')) = 'Y'")
    type_where = " AND ".join(where_parts) if where_parts else "1=1"
    by_source: Dict[str, List[Dict[str, Any]]] = {url: [] for url in source_urls}
    if "source_page" not in cols:
        return by_source

    source_list = [str(url or "").strip() for url in source_urls if str(url or "").strip()]
    if not source_list:
        return by_source
    placeholders = ", ".join(["%s"] * len(source_list))
    total_limit = max(1, min(int(limit_per_source or 30), 1000)) * len(source_list)
    rows = await mysql_execute_query(
        f"""
        SELECT {", ".join(f"`{col}`" for col in select_cols)}
        FROM `{learn_table}`
        WHERE {type_where} AND `source_page` IN ({placeholders})
        ORDER BY `id` DESC
        LIMIT %s
        """,
        tuple(source_list + [total_limit]),
        fetch=True,
        dbname=db_name,
    )
    per_source_limit = max(1, min(int(limit_per_source or 30), 1000))
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        source_page = str(row.get("source_page") or "").strip()
        if source_page not in by_source:
            continue
        if len(by_source[source_page]) >= per_source_limit:
            continue
        by_source[source_page].append(row)
    return by_source


async def _probe_file_attachments_fast(
    *,
    url: str,
    db_name: str,
    chat_bot_id: str,
    job_id: str,
    fetch_timeout_sec: int,
) -> Dict[str, Any]:
    from backend.file.file_download_workflow import FileDownloadWorkflow, _extract_file_author_info

    workflow = FileDownloadWorkflow()
    workflow.db_name = db_name
    workflow.chat_bot_id = chat_bot_id
    workflow.job_id = job_id
    workflow.enable_db_save = False
    workflow.enable_learning = False
    workflow.file_pipeline_skip_learning = True
    started = time.perf_counter()
    html = ""
    try:
        html = await workflow._fetch_html_static(url, timeout_sec=fetch_timeout_sec) or ""
        if not html:
            return {
                "status": "error",
                "error": "failed to fetch detail html",
                "url": url,
                "source_url": url,
                "attachments": [],
                "html_length": 0,
                "fetch_method": "fast_static",
                "elapsed_ms": int((time.perf_counter() - started) * 1000),
            }
        try:
            selector_profile = await workflow._get_selector_profile_for_detail(url=url, board_url=url)
        except Exception:
            selector_profile = None
        try:
            author_info = _extract_file_author_info(
                html,
                url=url,
                selector_profile=selector_profile,
            )
        except Exception as author_exc:
            logger.debug(
                "[FileDashboardAttachProbe] author extraction failed | job_id=%s url=%s err=%s",
                job_id,
                (url or "")[:220],
                author_exc,
            )
            author_info = {}
        if isinstance(author_info, dict):
            content_author = (
                author_info.get("content_author")
                or author_info.get("author")
                or author_info.get("department")
            )
            if content_author:
                author_info["content_author"] = content_author
        else:
            author_info = {}
        logger.warning(
            "[ContentAuthorDebug][file_dashboard.fast_probe_author] job_id=%s url=%s result=%r author=%r content_author=%r department=%r kind=%r raw=%r html_len=%s selector_profile=%s",
            job_id,
            (url or "")[:220],
            str((author_info or {}).get("content_author") or "")[:180],
            str((author_info or {}).get("author") or "")[:180],
            str((author_info or {}).get("content_author") or "")[:180],
            str((author_info or {}).get("department") or "")[:180],
            str((author_info or {}).get("author_kind") or "")[:80],
            str((author_info or {}).get("author_raw") or "")[:180],
            len(html or ""),
            bool(selector_profile),
        )
        if not (author_info or {}).get("content_author"):
            try:
                from bs4 import BeautifulSoup

                soup_for_debug = BeautifulSoup(html or "", "html.parser")
                debug_text = soup_for_debug.get_text(" ", strip=True)
            except Exception:
                debug_text = re.sub(r"\s+", " ", html or "")
            snippets = []
            for token in ("author", "created", "registered", "department", "attachment", "file"):
                pos = debug_text.find(token)
                if pos >= 0:
                    snippets.append(
                        f"{token}@{pos}:{debug_text[max(0, pos - 70):pos + 180]}"
                    )
            logger.warning(
                "[ContentAuthorDebug][file_dashboard.fast_probe_author_empty_context] job_id=%s url=%s snippets=%s",
                job_id,
                (url or "")[:220],
                snippets[:6],
            )

        attachments = workflow._extract_attachment_links_generic(html, base_url=url)
        try:
            ajax_attachments = await workflow._extract_kcohesion_filelist_attachments(html, base_url=url)
        except Exception:
            ajax_attachments = []
        seen = {
            canonicalize_url_for_dedup(str(a.get("href") or "")) or str(a.get("href") or "").strip().lower()
            for a in attachments or []
            if isinstance(a, dict)
        }
        for item in ajax_attachments or []:
            href = str((item or {}).get("href") or "").strip()
            key = canonicalize_url_for_dedup(href) or href.lower()
            if href and key not in seen:
                attachments.append(item)
                seen.add(key)
        return {
            "status": "ok",
            "url": url,
            "source_url": url,
            "attachments": attachments,
            "author_info": author_info,
            "html_length": len(html or ""),
            "fetch_method": "fast_static",
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
        }
    finally:
        try:
            await workflow._close_http_session()
        except Exception:
            pass
        try:
            await workflow._close_playwright()
        except Exception:
            pass


@api_router.options("/backend/file-dashboard/exploration-posts")
async def exploration_posts_options(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True}, headers=build_cors_headers(request))


@api_router.options("/backend/file-dashboard/crawl-status")
async def crawl_status_options(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True}, headers=build_cors_headers(request))


@api_router.options("/backend/file-dashboard/post-attachments")
async def post_attachments_options(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True}, headers=build_cors_headers(request))


@api_router.options("/backend/file-dashboard/crawl-targets")
async def crawl_targets_options(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True}, headers=build_cors_headers(request))


@api_router.options("/backend/file-dashboard/pg-missing-targets")
async def pg_missing_targets_options(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True}, headers=build_cors_headers(request))


@api_router.options("/backend/seed-urls/count")
async def seed_urls_count_options(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True}, headers=build_cors_headers(request))


async def _count_seed_urls_fast(
    *,
    db_name: str,
    chat_bot_id: str,
    contents_url: str,
) -> Dict[str, Any]:
    columns = await _table_columns(db_name, EXPLORATION_TABLE)
    if "url" not in columns or "type" not in columns:
        raise RuntimeError(f"{EXPLORATION_TABLE} requires url and type columns")

    conditions = ["LOWER(TRIM(CAST(`type` AS CHAR))) = 'post'"]
    params: List[Any] = []
    if chat_bot_id and "chat_bot_id" in columns:
        conditions.append("`chat_bot_id` = %s")
        params.append(chat_bot_id)

    url_scope: Dict[str, Any] = {
        "enabled": False,
        "mode": "",
        "target_url": contents_url,
        "exact_found": False,
    }
    if contents_url and _is_domain_root_url(contents_url):
        exact_variants = _domain_root_url_variants(contents_url)
        exact_rows_count = 0
        if exact_variants:
            exact_where = " AND ".join(conditions) + " AND `url` IN (" + ", ".join(["%s"] * len(exact_variants)) + ")"
            exact_count_rows = await maria_execute_query(
                f"SELECT COUNT(*) AS cnt FROM `{EXPLORATION_TABLE}` WHERE {exact_where}",
                tuple(params + exact_variants),
                fetch=True,
                dbname=db_name,
            )
            exact_rows_count = int((exact_count_rows or [{}])[0].get("cnt") or 0)
        if exact_rows_count > 0:
            conditions.append("`url` IN (" + ", ".join(["%s"] * len(exact_variants)) + ")")
            params.extend(exact_variants)
            url_scope.update(
                {
                    "enabled": True,
                    "mode": "domain_exact",
                    "exact_found": True,
                    "exact_variants": exact_variants,
                }
            )
        else:
            like_patterns = _domain_child_url_like_patterns(contents_url)
            if like_patterns:
                conditions.append("(" + " OR ".join(["`url` LIKE %s"] * len(like_patterns)) + ")")
                params.extend(like_patterns)
                url_scope.update(
                    {
                        "enabled": True,
                        "mode": "domain_children_pattern",
                        "exact_found": False,
                        "like_patterns": like_patterns,
                        "exact_variants": exact_variants,
                    }
                )
    elif contents_url:
        url_scope.update({"enabled": False, "mode": "non_domain_root_chatbot_total"})

    rows = await maria_execute_query(
        f"SELECT COUNT(*) AS cnt FROM `{EXPLORATION_TABLE}` WHERE {' AND '.join(conditions)}",
        tuple(params),
        fetch=True,
        dbname=db_name,
    )
    return {
        "total": int((rows or [{}])[0].get("cnt") or 0),
        "columns": columns,
        "url_scope": url_scope,
    }


async def _mariadb_seed_count_preflight(timeout_sec: float = 1.5) -> Dict[str, Any]:
    try:
        from backend.shared.config import Config

        host = str(getattr(Config, "MARIA_DB_HOST", "") or "").strip()
        port = int(getattr(Config, "MARIA_DB_PORT", 3306) or 3306)
    except Exception:
        host = ""
        port = 3306
    if not host:
        return {"ok": True, "host": host, "port": port, "skipped": True}
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout_sec)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return {"ok": True, "host": host, "port": port}
    except Exception as exc:
        return {
            "ok": False,
            "host": host,
            "port": port,
            "error": str(exc) or type(exc).__name__,
            "timeout_sec": timeout_sec,
        }


def _post_seed_count_bridge_sync(url: str, payload: Dict[str, Any], timeout_sec: int) -> Dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=max(1, int(timeout_sec or 20))) as response:
            raw = response.read()
            status_code = int(getattr(response, "status", 0) or 0)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        status_code = int(exc.code or 0)
    text = raw.decode("utf-8", errors="replace") if raw else "{}"
    try:
        data = json.loads(text) if text else {}
    except Exception:
        data = {"raw": text[:1000]}
    if not isinstance(data, dict):
        data = {"data": data}
    data["_bridge_status_code"] = status_code
    return data


def _extract_seed_count_from_bridge(data: Dict[str, Any]) -> int:
    bridge = data.get("bridge") if isinstance(data.get("bridge"), dict) else {}
    candidates = [
        bridge.get("remote_total") if isinstance(bridge, dict) else None,
        data.get("total"),
        data.get("target_count"),
        data.get("count"),
        bridge.get("remote_count") if isinstance(bridge, dict) else None,
    ]
    for value in candidates:
        try:
            if value is not None and str(value).strip() != "":
                return int(value)
        except Exception:
            continue
    targets = data.get("targets")
    if isinstance(targets, list):
        return len(targets)
    return 0


@api_router.post("/backend/seed-urls/count")
async def seed_urls_count(request: Request) -> JSONResponse:
    payload = await _request_json_object(request)
    db_name = str(
        payload.get("db_name") or payload.get("dbname") or payload.get("account_name") or ""
    ).strip()
    chat_bot_id = str(payload.get("chat_bot_id") or payload.get("chatbotid") or "").strip()
    raw_contents_url = payload.get("contents_url") or payload.get("contents") or payload.get("target_url") or ""
    contents_url = raw_contents_url[0] if isinstance(raw_contents_url, list) and raw_contents_url else raw_contents_url
    contents_url = str(contents_url or "").strip()
    if not db_name:
        return JSONResponse(
            {"ok": False, "error": "db_name is required"},
            status_code=400,
            headers=build_cors_headers(request),
        )
    if not chat_bot_id:
        return JSONResponse(
            {"ok": False, "error": "chat_bot_id is required"},
            status_code=400,
            headers=build_cors_headers(request),
        )
    if not contents_url:
        return JSONResponse(
            {"ok": False, "error": "contents_url is required"},
            status_code=400,
            headers=build_cors_headers(request),
        )
    try:
        timeout_sec = _safe_int(payload.get("timeout_sec"), 20, minimum=1, maximum=120)
        bridge_base_url = str(payload.get("bridge_base_url") or "http://127.0.0.1:8031").strip().rstrip("/")
        bridge_path = str(payload.get("bridge_path") or "/api/local-board-crawler/targets/preview").strip()
        if not bridge_path.startswith("/"):
            bridge_path = "/" + bridge_path
        bridge_url = bridge_base_url + bridge_path
        bridge_payload = {
            "db_name": db_name,
            "dbname": db_name,
            "account_name": db_name,
            "chat_bot_id": chat_bot_id,
            "chatbotid": chat_bot_id,
            "contents_url": contents_url,
            "contents": [contents_url],
            "table": EXPLORATION_TABLE,
        }
        for key in ("method", "target_date", "start_urls_target_date", "limit", "db_url_limit", "offset"):
            if key in payload:
                bridge_payload[key] = payload.get(key)
        bridge_data = await asyncio.wait_for(
            asyncio.to_thread(_post_seed_count_bridge_sync, bridge_url, bridge_payload, timeout_sec),
            timeout=timeout_sec,
        )
        status_code = int(bridge_data.get("_bridge_status_code") or 0)
        if status_code >= 400 or bridge_data.get("ok") is False:
            return JSONResponse(
                {
                    "ok": False,
                    "error": bridge_data.get("error") or bridge_data.get("message") or f"bridge HTTP {status_code}",
                    "bridge_url": bridge_url,
                    "bridge_status_code": status_code,
                    "bridge": {
                        "source": bridge_data.get("source"),
                        "status": bridge_data.get("status"),
                    },
                },
                status_code=502,
                headers=build_cors_headers(request),
            )
        total = _extract_seed_count_from_bridge(bridge_data)
        return JSONResponse(
            {
                "ok": True,
                "status": "success",
                "table": EXPLORATION_TABLE,
                "db_name": db_name,
                "chat_bot_id": chat_bot_id,
                "contents_url": contents_url,
                "total": int(total or 0),
                "source": "bridge",
                "bridge_url": bridge_url,
                "bridge_status_code": status_code,
                "bridge_count": bridge_data.get("count"),
                "bridge_remote_total": (bridge_data.get("bridge") or {}).get("remote_total") if isinstance(bridge_data.get("bridge"), dict) else None,
                "bridge_remote_count": (bridge_data.get("bridge") or {}).get("remote_count") if isinstance(bridge_data.get("bridge"), dict) else None,
                "timeout_sec": timeout_sec,
            },
            headers=build_cors_headers(request),
        )
    except asyncio.TimeoutError:
        return JSONResponse(
            {
                "ok": False,
                "error": "seed count timed out",
                "table": EXPLORATION_TABLE,
                "timeout_sec": _safe_int(payload.get("timeout_sec"), 12, minimum=1, maximum=120),
            },
            status_code=504,
            headers=build_cors_headers(request),
        )
    except Exception as exc:
        logger.exception("[SeedUrlsCount] failed | db=%s chat_bot_id=%s", db_name, chat_bot_id)
        return JSONResponse(
            {"ok": False, "error": str(exc), "table": EXPLORATION_TABLE},
            status_code=500,
            headers=build_cors_headers(request),
        )


async def _file_dashboard_status_snapshot(job_id: str) -> Dict[str, Any]:
    from backend.shared.crawler_state import crawler_state

    history = dict(crawler_state.job_history.get(job_id) or {})
    task = crawler_state.workflow_tasks.get(job_id)
    active_worker_task = getattr(crawler_state, "active_worker_tasks", {}).get(job_id)
    workflow = crawler_state.workflows.get(job_id)
    try:
        memory_state = dict(getattr(crawler_state, "accelerated_job_stats", {}).get(job_id) or {})
    except Exception:
        memory_state = {}
    try:
        stats = await asyncio.wait_for(
            asyncio.to_thread(workflow.get_stats),
            timeout=STATUS_SNAPSHOT_TIMEOUT_SEC,
        ) if workflow is not None and hasattr(workflow, "get_stats") else {}
    except Exception:
        stats = {}
    history_status = str(history.get("status") or "").strip().lower()
    workflow_status = str(getattr(workflow, "final_status", "") or "").strip().lower() if workflow is not None else ""
    running = bool(
        (task and not task.done())
        or (active_worker_task and not active_worker_task.done())
        or str(memory_state.get("status") or "").strip().lower() in {"running", "start", "starting"}
        or history_status in {"running", "start", "starting", "dispatch_start_urls"}
        or workflow_status in {"running", "start", "starting"}
    )
    return {
        "job_id": job_id,
        "running": running,
        "history": history,
        "memory_state": memory_state,
        "workflow": {
            "type": type(workflow).__name__ if workflow is not None else "",
            "final_status": str(getattr(workflow, "final_status", "") or "") if workflow is not None else "",
            "is_running": bool(getattr(workflow, "is_running", False)) if workflow is not None else False,
        },
        "stats": stats or {},
    }


@api_router.post("/backend/file-dashboard/pg-missing-targets")
async def pg_missing_targets(request: Request) -> JSONResponse:
    payload = await _request_json_object(request)
    db_name = str(payload.get("db_name") or payload.get("dbname") or payload.get("account_name") or "").strip()
    chat_bot_id = str(payload.get("chat_bot_id") or "").strip()
    limit = _safe_int(
        payload.get("content_relearn_limit")
        or payload.get("db_url_limit")
        or payload.get("limit"),
        10000,
        minimum=1,
        maximum=20000,
    )
    if not db_name:
        return JSONResponse({"ok": False, "error": "db_name is required"}, status_code=400, headers=build_cors_headers(request))
    if not chat_bot_id:
        return JSONResponse({"ok": False, "error": "chat_bot_id is required"}, status_code=400, headers=build_cors_headers(request))

    try:
        from backend.shared.partial_content_relearn import load_partial_content_relearn_targets

        target_payload = dict(payload)
        target_payload["content_relearn_limit"] = limit
        target_payload["content_relearn_only_missing_pg"] = True
        target_payload["content_relearn_match_pg_content_metadata"] = True
        result = await load_partial_content_relearn_targets(
            target_payload,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
        )
        raw_rows = list((result or {}).get("rows") or [])
        rows, duplicate_removed = _dedupe_target_rows(raw_rows)
        return JSONResponse(
            jsonable_encoder(
                {
                    "ok": bool((result or {}).get("ok")),
                    "status": "success" if (result or {}).get("ok") else "error",
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "table": (result or {}).get("table"),
                    "count": len(rows),
                    "returned": len(rows),
                    "raw_count": len(raw_rows),
                    "duplicate_removed": duplicate_removed,
                    "limit": limit,
                    "rows": rows,
                    "pg_filter": (result or {}).get("pg_filter") or {},
                    "filter_source": (result or {}).get("filter_source"),
                    "reason": (result or {}).get("reason"),
                }
            ),
            headers=build_cors_headers(request),
        )
    except Exception as exc:
        logger.exception("[FileDashboard] pg missing targets failed | db=%s chat_bot_id=%s", db_name, chat_bot_id)
        return JSONResponse(
            {"ok": False, "status": "error", "error": str(exc), "db_name": db_name, "chat_bot_id": chat_bot_id},
            status_code=500,
            headers=build_cors_headers(request),
        )


@api_router.post("/backend/file-dashboard/crawl-status")
async def crawl_status(request: Request) -> JSONResponse:
    payload = await _request_json_object(request)
    job_id = str(payload.get("job_id") or "").strip()
    if not job_id:
        return JSONResponse(
            {"ok": False, "error": "job_id is required"},
            status_code=400,
            headers=build_cors_headers(request),
        )
    try:
        snapshot = await _file_dashboard_status_snapshot(job_id)
        snapshot = dict(snapshot or {})
        snapshot["stats"] = _stabilize_status_stats(job_id, snapshot)
        from backend.shared.crawler_state import crawler_state

        events = crawler_state.job_history_events.get(job_id) or []
        return JSONResponse(
            jsonable_encoder(
                {
                    "ok": True,
                    "job_id": job_id,
                    "running": snapshot.get("running"),
                    "history": snapshot.get("history") or {},
                    "events": events[-10:],
                    "memory_state": snapshot.get("memory_state") or {},
                    "workflow": snapshot.get("workflow") or {},
                    "stats": snapshot.get("stats") or {},
                    "status_probe_timeout": bool(snapshot.get("status_probe_timeout")),
                }
            ),
            headers=build_cors_headers(request),
        )
    except Exception as exc:
        logger.exception("[FileDashboard] crawl status failed | job_id=%s", job_id)
        return JSONResponse(
            {"ok": False, "error": str(exc), "job_id": job_id},
            status_code=500,
            headers=build_cors_headers(request),
        )


@api_router.post("/backend/file-dashboard/post-attachments")
async def post_attachments(request: Request) -> JSONResponse:
    payload = await _request_json_object(request)
    db_name = str(payload.get("db_name") or payload.get("dbname") or "").strip()
    chat_bot_id = str(payload.get("chat_bot_id") or "").strip()
    raw_urls = payload.get("urls")
    if isinstance(raw_urls, str):
        raw_urls = [raw_urls]
    urls = []
    seen = set()
    for raw in raw_urls or []:
        try:
            url = ensure_url_scheme(str(raw or "").strip())
        except Exception:
            url = str(raw or "").strip()
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    limit = _safe_int(payload.get("limit"), 50, minimum=1, maximum=200)
    urls = urls[:limit]
    if not db_name:
        return JSONResponse({"ok": False, "error": "db_name is required"}, status_code=400, headers=build_cors_headers(request))
    if not chat_bot_id:
        return JSONResponse({"ok": False, "error": "chat_bot_id is required"}, status_code=400, headers=build_cors_headers(request))
    if not urls:
        return JSONResponse({"ok": False, "error": "urls are required"}, status_code=400, headers=build_cors_headers(request))

    try:
        include_learning_status = str(payload.get("include_learning_status", "1")).strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        learn_rows_task = (
            asyncio.create_task(
                _load_file_learn_rows_for_sources(
                    db_name=db_name,
                    chat_bot_id=chat_bot_id,
                    source_urls=urls,
                )
            )
            if include_learning_status
            else None
        )
        sem = asyncio.Semaphore(_safe_int(payload.get("concurrency"), 8, minimum=1, maximum=24))
        probe_timeout_sec = _safe_int(payload.get("probe_timeout_sec"), 6, minimum=2, maximum=30)
        probe_fetch_timeout_sec = _safe_int(payload.get("probe_fetch_timeout_sec"), 4, minimum=1, maximum=20)
        fast_probe = str(payload.get("fast_probe", "1")).strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        job_id = str(payload.get("job_id") or "file-dashboard-probe")

        async def probe_one(url: str) -> Dict[str, Any]:
            async with sem:
                try:
                    if fast_probe:
                        probe = await asyncio.wait_for(
                            _probe_file_attachments_fast(
                                url=url,
                                db_name=db_name,
                                chat_bot_id=chat_bot_id,
                                job_id=job_id,
                                fetch_timeout_sec=probe_fetch_timeout_sec,
                            ),
                            timeout=probe_timeout_sec,
                        )
                    else:
                        from backend.board.board_probe_endpoints import run_file_crawl_probe_readonly

                        probe = await asyncio.wait_for(
                            run_file_crawl_probe_readonly(
                                {
                                    "url": url,
                                    "db_name": db_name,
                                    "chat_bot_id": chat_bot_id,
                                    "job_id": job_id,
                                    "probe_fetch_timeout_sec": probe_fetch_timeout_sec,
                                    "probe_playwright_timeout_sec": max(1, probe_timeout_sec - 1),
                                    "disable_playwright": bool(payload.get("disable_playwright", True)),
                                }
                            ),
                            timeout=probe_timeout_sec,
                        )
                except asyncio.TimeoutError:
                    return {
                        "url": url,
                        "status": "timeout",
                        "error": f"probe timeout after {probe_timeout_sec}s",
                        "attachments": [],
                        "attachment_count": 0,
                        "learned_count": 0,
                    }
                except Exception as exc:
                    return {
                        "url": url,
                        "status": "error",
                        "error": str(exc),
                        "attachments": [],
                        "attachment_count": 0,
                        "learned_count": 0,
                    }
                attachments = list((probe or {}).get("attachments") or [])
                try:
                    learn_rows_by_source = await learn_rows_task if learn_rows_task is not None else {}
                except Exception as learn_exc:
                    logger.warning(
                        "[FileDashboardAttachProbe] learn row match load failed | db=%s chat_bot_id=%s err=%s",
                        db_name,
                        chat_bot_id,
                        learn_exc,
                    )
                    learn_rows_by_source = {}
                learn_rows = learn_rows_by_source.get(url) or []
                enriched = []
                learned_count = 0
                for idx, item in enumerate(attachments):
                    item = dict(item or {})
                    href = str(item.get("href") or item.get("url") or "").strip()
                    metadata_date = (
                        ((probe or {}).get("metadata") or {}).get("created_at")
                        or ((probe or {}).get("metadata") or {}).get("content_created_at")
                        or (probe or {}).get("reg_date")
                        or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )
                    file_content_metadata = {
                        "source_url": href,
                        "created_at": ((probe or {}).get("metadata") or {}).get("created_at")
                        or metadata_date,
                        "updated_at": ((probe or {}).get("metadata") or {}).get("updated_at")
                        or metadata_date,
                        "content_created_at": ((probe or {}).get("metadata") or {}).get("content_created_at")
                        or metadata_date,
                        "content_updated_at": ((probe or {}).get("metadata") or {}).get("content_updated_at")
                        or metadata_date,
                        "date_rerank_target": True,
                        "source_category": "file",
                        "update_frequency": "1_day",
                    }
                    author_info = (probe or {}).get("author_info") or {}
                    content_author = (
                        author_info.get("content_author")
                        or author_info.get("author")
                        or author_info.get("department")
                    ) if isinstance(author_info, dict) else ""
                    if content_author:
                        file_content_metadata["content_author"] = content_author
                    if idx == 0:
                        logger.warning(
                            "[ContentAuthorDebug][file_dashboard.post_attachments_author] job_id=%s url=%s first_href=%s result=%r author=%r content_author=%r department=%r attachment_count=%s",
                            job_id,
                            (url or "")[:220],
                            href[:220],
                            str(content_author or "")[:180],
                            str((author_info or {}).get("author") or "")[:180] if isinstance(author_info, dict) else "",
                            str((author_info or {}).get("content_author") or "")[:180] if isinstance(author_info, dict) else "",
                            str((author_info or {}).get("department") or "")[:180] if isinstance(author_info, dict) else "",
                            len(attachments or []),
                        )
                    file_content_metadata = {
                        key: value
                        for key, value in file_content_metadata.items()
                        if value is not None and value != ""
                    }
                    matched_rows = [row for row in learn_rows if _learn_row_matches_attachment(row, href)]
                    match_method = "url" if matched_rows else ""
                    if not matched_rows and learn_rows:
                        name = str(item.get("name") or item.get("title") or "").strip()
                        if name:
                            matched_rows = [
                                row for row in learn_rows
                                if name and name in str(row.get("subject") or "")
                            ]
                            if matched_rows:
                                match_method = "name"
                    learned = any(str(row.get("status") or "").strip().upper() == "Y" for row in matched_rows)
                    if learned:
                        learned_count += 1
                    item.update(
                        {
                            "index": idx + 1,
                            "learned": learned,
                            "learn_status": "Y" if learned else (str((matched_rows[0] or {}).get("status") or "") if matched_rows else ""),
                            "learn_list_id": (matched_rows[0] or {}).get("id") if matched_rows else None,
                            "match_count": len(matched_rows),
                            "match_method": match_method,
                            "matched": bool(matched_rows),
                            "content_metadata": file_content_metadata,
                        }
                    )
                    enriched.append(item)
                return {
                    "url": url,
                    "source_url": (probe or {}).get("source_url") or url,
                    "status": (probe or {}).get("status") or "ok",
                    "title": (probe or {}).get("title") or "",
                    "reg_date": (probe or {}).get("reg_date") or "",
                    "author_info": (probe or {}).get("author_info") or {},
                    "metadata": (probe or {}).get("metadata") or {},
                    "fetch_method": (probe or {}).get("fetch_method") or "",
                    "html_length": (probe or {}).get("html_length") or 0,
                    "attachment_count": len(enriched),
                    "learned_count": learned_count,
                    "learn_rows_count": len(learn_rows),
                    "matched_count": sum(1 for item in enriched if item.get("matched")),
                    "attachments": enriched,
                }

        posts = await asyncio.gather(*(probe_one(url) for url in urls))
        if learn_rows_task is not None and not learn_rows_task.done():
            try:
                await learn_rows_task
            except Exception:
                pass
        return JSONResponse(
            jsonable_encoder(
                {
                    "ok": True,
                    "status": "success",
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "count": len(posts),
                    "fast_probe": fast_probe,
                    "include_learning_status": include_learning_status,
                    "posts": posts,
                }
            ),
            headers=build_cors_headers(request),
        )
    except Exception as exc:
        logger.exception("[FileDashboard] post attachment probe failed | db=%s chat_bot_id=%s", db_name, chat_bot_id)
        return JSONResponse(
            {"ok": False, "error": str(exc)},
            status_code=500,
            headers=build_cors_headers(request),
        )


@api_router.post("/backend/file-dashboard/crawl-targets")
async def crawl_targets(request: Request, background_tasks: BackgroundTasks) -> JSONResponse:
    payload = await _request_json_object(request)
    db_name = str(payload.get("db_name") or payload.get("dbname") or "").strip()
    chat_bot_id = str(payload.get("chat_bot_id") or "").strip()
    raw_targets = payload.get("targets") or payload.get("urls") or []
    if isinstance(raw_targets, str):
        raw_targets = [raw_targets]
    targets: List[Dict[str, Any]] = []
    seen_target_keys: set[str] = set()
    for item in raw_targets or []:
        if isinstance(item, dict):
            raw_url = item.get("url") or item.get("content") or item.get("source_page")
            target = dict(item)
        else:
            raw_url = item
            target = {}
        try:
            url = ensure_url_scheme(str(raw_url or "").strip())
        except Exception:
            url = str(raw_url or "").strip()
        if not url:
            continue
        dedupe_key = (canonicalize_url_for_dedup(url) or url).strip().lower()
        if dedupe_key in seen_target_keys:
            continue
        seen_target_keys.add(dedupe_key)
        target["url"] = url
        target["type"] = target.get("type") or "post"
        targets.append(target)
    if not db_name:
        return JSONResponse({"ok": False, "error": "db_name is required"}, status_code=400, headers=build_cors_headers(request))
    if not chat_bot_id:
        return JSONResponse({"ok": False, "error": "chat_bot_id is required"}, status_code=400, headers=build_cors_headers(request))
    if not targets:
        return JSONResponse({"ok": False, "error": "No target URLs selected"}, status_code=400, headers=build_cors_headers(request))

    job_id = str(payload.get("job_id") or f"file-dashboard-{uuid4().hex[:12]}").strip()
    urls = [str(item["url"]) for item in targets]
    attachment_target_count = sum(
        len(item.get("attachments") or [])
        for item in targets
        if isinstance(item, dict) and isinstance(item.get("attachments"), list)
    )
    display_target_count = attachment_target_count or len(targets)
    is_content_relearn = bool(payload.get("content_relearn_mode")) or any(
        str(item.get("type") or "").strip() == "partial_content_relearn"
        or bool(item.get("force_relearn"))
        for item in targets
    )
    crawl_payload: Dict[str, Any] = {
        "job_id": job_id,
        "db_name": db_name,
        "dbname": db_name,
        "account_name": db_name,
        "chat_bot_id": chat_bot_id,
        "colle": "content" if is_content_relearn else "file",
        "content_type": "url" if is_content_relearn else "file",
        "ui_colle": "content" if is_content_relearn else "file",
        "method": str(payload.get("method") or "period"),
        "contents": [urls[0]],
        "contents_url": urls[0],
        "target_domains": payload.get("target_domains") or _target_domains_from_urls(urls),
        "start_urls_override": targets,
        "start_urls_override_source": "partial_content_relearn" if is_content_relearn else "file_crawl_post_db",
        "pre_explored_start_urls_count": display_target_count,
        "exploration_display_count_fixed": True,
        "exploration_display_max_count": display_target_count,
        "enable_learning": True,
        "file_dashboard": True,
        "learn_list_duplicate_exclude_enabled": bool(payload.get("learn_list_duplicate_exclude_enabled", True)),
    }
    if is_content_relearn:
        crawl_payload.update(
            {
                "blank_chunk_relearn": True,
                "blankChunkRelearn": True,
                "learn_list_blank_chunk_relearn": True,
                "content_relearn_mode": True,
                "partial_update_fields": ["content"],
                "content_relearn_only_missing_pg": True,
                "content_relearn_match_pg_content_metadata": True,
                "content_relearn_limit": len(targets),
                "duplicate_repair_mode": "category",
            }
        )
    try:
        from backend.shared.crawl_dispatcher import dispatch_and_schedule_workflow

        response = await dispatch_and_schedule_workflow(crawl_payload, background_tasks, header_response=None)
        try:
            response_body = response.body.decode("utf-8")
            response_data = json.loads(response_body) if response_body else {}
        except Exception:
            response_data = {}
        if response.status_code >= 400:
            return JSONResponse(
                {
                    "ok": False,
                    "error": response_data.get("message") or response_data.get("reason") or "Failed to start crawl",
                    "job_id": job_id,
                    "response": response_data,
                },
                status_code=response.status_code,
                headers=build_cors_headers(request),
            )
        return JSONResponse(
            jsonable_encoder(
                {
                    "ok": True,
                    "job_id": job_id,
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "target_count": display_target_count,
                    "post_target_count": len(targets),
                    "attachment_target_count": attachment_target_count,
                    "targets": targets,
                    "response": response_data,
                }
            ),
            headers=build_cors_headers(request),
        )
    except Exception as exc:
        logger.exception("[FileDashboard] crawl targets failed | job_id=%s db=%s", job_id, db_name)
        return JSONResponse(
            {"ok": False, "error": str(exc), "job_id": job_id},
            status_code=500,
            headers=build_cors_headers(request),
        )


@api_router.post("/backend/file-dashboard/exploration-posts")
async def exploration_posts(request: Request) -> JSONResponse:
    payload = await _request_json_object(request)
    db_name = str(
        payload.get("db_name") or payload.get("dbname") or payload.get("account_name") or ""
    ).strip()
    chat_bot_id = str(payload.get("chat_bot_id") or "").strip()
    limit = _safe_int(
        payload.get("db_url_limit")
        or payload.get("exploration_limit")
        or payload.get("url_limit")
        or payload.get("limit"),
        5000,
        minimum=1,
        maximum=50000,
    )
    offset = _safe_int(payload.get("offset"), 0, minimum=0, maximum=10000000)
    active_only = bool(payload.get("active_only"))
    count_only = bool(payload.get("count_only"))
    include_duplicates = bool(payload.get("include_duplicates"))
    scope_by_contents_learn_list_id = bool(payload.get("scope_by_contents_learn_list_id"))
    first_learn_list_match = bool(payload.get("first_learn_list_match"))
    method = str(payload.get("method") or "").strip().lower()
    exploration_type = str(payload.get("exploration_type") or payload.get("type_filter") or "post").strip().lower()
    target_url = _target_url_from_payload(payload)
    raw_target_date = payload.get("target_date") or payload.get("start_urls_target_date")
    start_date = end_date = ""
    if isinstance(raw_target_date, list) and len(raw_target_date) >= 2:
        start_date = str(raw_target_date[0] or "").strip()
        end_date = str(raw_target_date[1] or "").strip()

    if not db_name:
        return JSONResponse(
            {"ok": False, "error": "db_name is required"},
            status_code=400,
            headers=build_cors_headers(request),
        )

    try:
        columns = await _table_columns(db_name, EXPLORATION_TABLE)
        if "url" not in columns or "type" not in columns:
            return JSONResponse(
                {
                    "ok": False,
                    "error": f"{EXPLORATION_TABLE} requires url and type columns",
                    "db_name": db_name,
                    "columns": columns,
                },
                status_code=400,
                headers=build_cors_headers(request),
            )

        select_columns = [col for col in EXPLORATION_SELECT_CANDIDATES if col in columns]
        if "url" not in select_columns:
            select_columns.insert(0, "url")

        conditions: List[str] = []
        params: List[Any] = []
        if exploration_type in {"", "post"}:
            conditions.append("LOWER(TRIM(CAST(`type` AS CHAR))) = 'post'")
        elif exploration_type in {"empty_or_post", "post_or_empty"}:
            conditions.append("(LOWER(TRIM(CAST(`type` AS CHAR))) = 'post' OR COALESCE(TRIM(CAST(`type` AS CHAR)), '') = '')")
        elif exploration_type in {"all", "any", "*"}:
            pass
        else:
            conditions.append("LOWER(TRIM(CAST(`type` AS CHAR))) = %s")
            params.append(exploration_type)
        if chat_bot_id and "chat_bot_id" in columns:
            conditions.append("`chat_bot_id` = %s")
            params.append(chat_bot_id)
        if active_only and "is_active" in columns:
            conditions.append("COALESCE(`is_active`, 1) = 1")
        if (not include_duplicates) and "merge_status" in columns:
            conditions.append("(COALESCE(`merge_status`, '') = '' OR `merge_status` != 'duplicate')")
        learn_list_ids: List[int] = []
        learn_list_scope = {"enabled": False, "contents_url": target_url, "learn_list_ids": []}
        if scope_by_contents_learn_list_id:
            if "learn_list_id" not in columns:
                return JSONResponse(
                    {
                        "ok": False,
                        "error": f"{EXPLORATION_TABLE} requires learn_list_id column for contents scope",
                        "db_name": db_name,
                        "columns": columns,
                    },
                    status_code=400,
                    headers=build_cors_headers(request),
                )
            if not target_url:
                return JSONResponse(
                    {"ok": False, "error": "contents_url is required for contents learn_list scope", "db_name": db_name},
                    status_code=400,
                    headers=build_cors_headers(request),
                )
            learn_columns = await _table_columns(db_name, LEARN_LIST_TABLE)
            if "content" not in learn_columns:
                return JSONResponse(
                    {
                        "ok": False,
                        "error": f"{LEARN_LIST_TABLE} requires content column",
                        "db_name": db_name,
                        "columns": learn_columns,
                    },
                    status_code=400,
                    headers=build_cors_headers(request),
                )
            candidates: List[str] = []
            seen_candidates: set[str] = set()
            for candidate in (target_url, ensure_url_scheme(target_url)):
                candidate_text = str(candidate or "").strip()
                if candidate_text and candidate_text not in seen_candidates:
                    seen_candidates.add(candidate_text)
                    candidates.append(candidate_text)
            placeholders = ", ".join(["%s"] * len(candidates))
            learn_conditions = [f"`content` IN ({placeholders})"]
            learn_params: List[Any] = list(candidates)
            if chat_bot_id and "chat_bot_id" in learn_columns:
                learn_conditions.append("`chat_bot_id` = %s")
                learn_params.append(chat_bot_id)
            learn_limit_sql = " LIMIT 1" if first_learn_list_match else ""
            learn_rows = await maria_execute_query(
                f"""
                SELECT `id`, `content`
                FROM `{LEARN_LIST_TABLE}`
                WHERE {' AND '.join(learn_conditions)}
                ORDER BY `id` ASC{learn_limit_sql}
                """,
                tuple(learn_params),
                fetch=True,
                dbname=db_name,
            )
            for row in learn_rows or []:
                try:
                    learn_id = int((row or {}).get("id") or 0)
                except Exception:
                    learn_id = 0
                if learn_id > 0 and learn_id not in learn_list_ids:
                    learn_list_ids.append(learn_id)
            learn_list_scope.update({"enabled": True, "learn_list_ids": list(learn_list_ids)})
            if learn_list_ids:
                conditions.append("`learn_list_id` IN (" + ", ".join(["%s"] * len(learn_list_ids)) + ")")
                params.extend(learn_list_ids)
            else:
                conditions.append("1 = 0")

        date_filter = {"enabled": False, "column": "", "start": start_date, "end": end_date}
        if method != "all" and start_date and end_date:
            date_column = next(
                (
                    col
                    for col in ("content_created_at", "reg_date", "created_at", "updated_at")
                    if col in columns
                ),
                "",
            )
            if date_column:
                conditions.append(f"DATE(`{date_column}`) BETWEEN %s AND %s")
                params.extend([start_date, end_date])
                date_filter.update({"enabled": True, "column": date_column})

        where_sql = " AND ".join(conditions) if conditions else "1 = 1"
        scoped_conditions = list(conditions)
        scoped_params = list(params)
        url_scope: Dict[str, Any] = {
            "enabled": False,
            "mode": "",
            "target_url": target_url,
            "exact_found": False,
        }
        if target_url and _is_domain_root_url(target_url):
            exact_variants = _domain_root_url_variants(target_url)
            exact_rows_count = 0
            if exact_variants:
                exact_where = where_sql + " AND `url` IN (" + ", ".join(["%s"] * len(exact_variants)) + ")"
                exact_count_rows = await maria_execute_query(
                    f"SELECT COUNT(*) AS cnt FROM `{EXPLORATION_TABLE}` WHERE {exact_where}",
                    tuple(params + exact_variants),
                    fetch=True,
                    dbname=db_name,
                )
                exact_rows_count = int((exact_count_rows or [{}])[0].get("cnt") or 0)
            if exact_rows_count > 0:
                scoped_conditions.append("`url` IN (" + ", ".join(["%s"] * len(exact_variants)) + ")")
                scoped_params.extend(exact_variants)
                url_scope.update(
                    {
                        "enabled": True,
                        "mode": "domain_exact",
                        "exact_found": True,
                        "exact_variants": exact_variants,
                    }
                )
            else:
                like_patterns = _domain_child_url_like_patterns(target_url)
                if like_patterns:
                    scoped_conditions.append(
                        "(" + " OR ".join(["`url` LIKE %s"] * len(like_patterns)) + ")"
                    )
                    scoped_params.extend(like_patterns)
                    url_scope.update(
                        {
                            "enabled": True,
                            "mode": "domain_children_pattern",
                            "exact_found": False,
                            "like_patterns": like_patterns,
                            "exact_variants": exact_variants,
                        }
                    )
        elif target_url:
            url_scope.update({"enabled": False, "mode": "non_domain_root_unchanged"})

        scoped_where_sql = " AND ".join(scoped_conditions) if scoped_conditions else "1 = 1"
        count_rows = await maria_execute_query(
            f"SELECT COUNT(*) AS cnt FROM `{EXPLORATION_TABLE}` WHERE {scoped_where_sql}",
            tuple(scoped_params),
            fetch=True,
            dbname=db_name,
        )
        total = int((count_rows or [{}])[0].get("cnt") or 0)
        if count_only:
            result = {
                "ok": True,
                "status": "success",
                "db_name": db_name,
                "chat_bot_id": chat_bot_id,
                "table": EXPLORATION_TABLE,
                "exploration_type": exploration_type,
                "total": total,
                "returned": 0,
                "offset": offset,
                "limit": limit,
                "date_filter": date_filter,
                "url_scope": url_scope,
                "learn_list_scope": learn_list_scope,
                "learn_list_ids": learn_list_ids,
                "rows": [],
            }
            return JSONResponse(jsonable_encoder(result), headers=build_cors_headers(request))

        order_column = "id" if "id" in columns else "url"
        rows = await maria_execute_query(
            f"""
            SELECT {", ".join(f"`{col}`" for col in select_columns)}
            FROM `{EXPLORATION_TABLE}`
            WHERE {scoped_where_sql}
            ORDER BY `{order_column}` DESC
            LIMIT %s OFFSET %s
            """,
            tuple(scoped_params + [limit, offset]),
            fetch=True,
            dbname=db_name,
        )
        result = {
            "ok": True,
            "status": "success",
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "table": EXPLORATION_TABLE,
            "exploration_type": exploration_type,
            "columns": select_columns,
            "total": total,
            "returned": len(rows or []),
            "offset": offset,
            "limit": limit,
            "date_filter": date_filter,
            "url_scope": url_scope,
            "learn_list_scope": learn_list_scope,
            "learn_list_ids": learn_list_ids,
            "rows": rows or [],
        }
        logger.info(
            "[FileDashboard] exploration post urls loaded | db=%s chat_bot_id=%s total=%s returned=%s offset=%s limit=%s url_scope=%s",
            db_name,
            chat_bot_id,
            total,
            len(rows or []),
            offset,
            limit,
            url_scope,
        )
        return JSONResponse(jsonable_encoder(result), headers=build_cors_headers(request))
    except Exception as exc:
        logger.exception(
            "[FileDashboard] exploration post url load failed | db=%s chat_bot_id=%s",
            db_name,
            chat_bot_id,
        )
        return JSONResponse(
            {"ok": False, "error": str(exc), "db_name": db_name, "chat_bot_id": chat_bot_id},
            status_code=500,
            headers=build_cors_headers(request),
        )


