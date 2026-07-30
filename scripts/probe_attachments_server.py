import argparse
import asyncio
import os
import sys
from typing import Any, Dict, List, Optional
from uuid import uuid4

try:
    import requests
except Exception:
    requests = None  # type: ignore[assignment]

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import text

from backend.file.file_download_workflow import FileDownloadWorkflow
from db.db_postgres import get_session_factory
from scripts.probe_attachments import (
    DEFAULT_BRIDGE_BASE,
    DEFAULT_POST_ROWS_PATH,
    load_urls_from_local_db,
    load_urls_from_bridge,
    probe_url,
)


class ProbeRequest(BaseModel):
    urls: List[str] = Field(default_factory=list)
    use_bridge: bool = False
    bridge_base: str = DEFAULT_BRIDGE_BASE
    bridge_path: str = DEFAULT_POST_ROWS_PATH
    db_name: str = ""
    chat_bot_id: str = ""
    status: str = ""
    limit: int = 100
    max_urls: int = 100
    concurrency: int = 3
    timeout: float = 30.0
    playwright: bool = False


class HarnessRequest(BaseModel):
    pg_db_name: str = ""
    pg_table: str = ""
    pg_schema: str = "public"
    db_name: str = ""
    chat_bot_id: str = ""
    limit: int = 20
    max_urls: int = 20
    concurrency: int = 3
    timeout: float = 30.0
    playwright: bool = False
    learn: bool = False


class ExplorationAttachmentRequest(BaseModel):
    bridge_base: str = DEFAULT_BRIDGE_BASE
    db_name: str = ""
    chat_bot_id: str = ""
    limit: int = 50000
    offset: int = 0
    page_size: int = 5000
    max_urls: int = 50000
    concurrency: int = 8
    timeout: float = 8.0
    use_remote_probe: bool = True
    active_only: bool = False


class LearnFoundAttachmentsRequest(BaseModel):
    bridge_base: str = DEFAULT_BRIDGE_BASE
    db_name: str = ""
    chat_bot_id: str = ""
    posts: List[Dict[str, Any]] = Field(default_factory=list)
    only_missing: bool = True
    max_attachments: int = 50000
    learn_list_duplicate_exclude_enabled: bool = True
    job_id: str = ""


app = FastAPI(title="Attachment Probe")
JOBS: Dict[str, Dict[str, Any]] = {}


def _dedupe_urls(values: List[Any], max_urls: int) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    if max_urls and max_urls > 0:
        out = out[:max_urls]
    return out


def _quote_pg_ident(value: str) -> str:
    return '"' + str(value or "").replace('"', '""') + '"'


def _bridge_post_json(*, bridge_base: str, path: str, payload: Dict[str, Any], timeout: float = 90.0) -> Dict[str, Any]:
    if requests is None:
        raise RuntimeError("requests is required for f1_dev bridge mode")
    endpoint = bridge_base.rstrip("/") + "/" + path.lstrip("/")
    session = requests.Session()
    session.trust_env = False
    response = session.post(endpoint, json=payload, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected bridge response type: {type(data).__name__}")
    return data


def _row_url(row: Any) -> str:
    if not isinstance(row, dict):
        return str(row or "").strip()
    for key in ("url", "content", "contents_url", "source_url", "source_page", "href"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _attachment_href(row: Any) -> str:
    if not isinstance(row, dict):
        return str(row or "").strip()
    for key in ("href", "url", "file_url", "download_url"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _attachment_name(row: Any) -> str:
    if not isinstance(row, dict):
        return "attachment"
    for key in ("name", "filename", "file_name", "title", "text"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return "attachment"


def _post_title(row: Dict[str, Any]) -> str:
    for source in (row, row.get("exploration_row") if isinstance(row.get("exploration_row"), dict) else {}):
        if not isinstance(source, dict):
            continue
        for key in ("title", "subject", "name"):
            value = str(source.get(key) or "").strip()
            if value:
                return value
    return ""


def _post_reg_date(row: Dict[str, Any]) -> str:
    for source in (row, row.get("exploration_row") if isinstance(row.get("exploration_row"), dict) else {}):
        if not isinstance(source, dict):
            continue
        for key in ("reg_date", "reg_date_str", "published_at", "post_date"):
            value = str(source.get(key) or "").strip()
            if value:
                return value
    return ""


def _build_crawl_targets_from_posts(payload: LearnFoundAttachmentsRequest) -> Dict[str, Any]:
    max_attachments = max(1, min(int(payload.max_attachments or 50000), 200000))
    targets: List[Dict[str, Any]] = []
    seen_attachment_keys: set[str] = set()
    attachment_count = 0

    for post in payload.posts or []:
        if not isinstance(post, dict):
            continue
        post_url = _row_url(post)
        if not post_url:
            continue
        attachments: List[Dict[str, Any]] = []
        for attachment in post.get("attachments") or []:
            if not isinstance(attachment, dict):
                continue
            if payload.only_missing and bool(attachment.get("learned")):
                continue
            href = _attachment_href(attachment)
            if not href:
                continue
            key = href.strip().lower()
            if key in seen_attachment_keys:
                continue
            seen_attachment_keys.add(key)
            item = dict(attachment)
            item["href"] = href
            item["url"] = item.get("url") or href
            item["name"] = _attachment_name(attachment)
            item["method"] = str(item.get("method") or "GET").upper()
            attachments.append(item)
            attachment_count += 1
            if attachment_count >= max_attachments:
                break
        if attachments:
            target: Dict[str, Any] = {
                "url": post_url,
                "type": "post",
                "attachments": attachments,
                "direct_attachments": attachments,
            }
            title = _post_title(post)
            if title:
                target["title"] = title
                target["subject"] = title
            reg_date = _post_reg_date(post)
            if reg_date:
                target["reg_date"] = reg_date
            exploration_row = post.get("exploration_row")
            if isinstance(exploration_row, dict):
                author_info = {
                    key: str(exploration_row.get(key) or "").strip()
                    for key in ("content_author", "author", "department")
                    if str(exploration_row.get(key) or "").strip()
                }
                if author_info:
                    target["author_info"] = author_info
            targets.append(target)
        if attachment_count >= max_attachments:
            break

    return {"targets": targets, "attachment_count": attachment_count}


def _compact_rows(rows: List[Any]) -> List[Dict[str, Any]]:
    compact: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, 1):
        if isinstance(row, dict):
            item = dict(row)
        else:
            item = {"url": str(row or "").strip()}
        item.setdefault("_index", idx)
        compact.append(item)
    return compact


async def _load_exploration_rows_via_bridge(payload: ExplorationAttachmentRequest) -> Dict[str, Any]:
    limit = max(1, min(int(payload.limit or 50000), 200000))
    page_size = max(1, min(int(payload.page_size or 5000), 50000))
    offset = max(0, int(payload.offset or 0))
    rows: List[Dict[str, Any]] = []
    total: Optional[int] = None
    bridge_pages: List[Dict[str, Any]] = []

    while len(rows) < limit:
        current_limit = min(page_size, limit - len(rows))
        request_payload = {
            "db_name": payload.db_name,
            "chat_bot_id": payload.chat_bot_id,
            "limit": current_limit,
            "offset": offset + len(rows),
            "active_only": payload.active_only,
            "method": "all",
        }
        data = await asyncio.to_thread(
            _bridge_post_json,
            bridge_base=payload.bridge_base,
            path="/backend/file-dashboard/exploration-posts",
            payload=request_payload,
        )
        page_rows = _compact_rows(list(data.get("rows") or []))
        if total is None:
            try:
                total = int(data.get("total") or 0)
            except Exception:
                total = 0
        bridge_pages.append(
            {
                "offset": request_payload["offset"],
                "limit": current_limit,
                "returned": len(page_rows),
            }
        )
        rows.extend(page_rows)
        if not page_rows or len(page_rows) < current_limit:
            break
        if total is not None and offset + len(rows) >= total:
            break

    return {
        "rows": rows,
        "total": int(total or len(rows)),
        "returned": len(rows),
        "offset": offset,
        "limit": limit,
        "pages": bridge_pages,
    }


async def _probe_attachments_via_remote_dashboard(
    *,
    payload: ExplorationAttachmentRequest,
    urls: List[str],
) -> List[Dict[str, Any]]:
    posts: List[Dict[str, Any]] = []
    batch_size = 200
    for start in range(0, len(urls), batch_size):
        batch = urls[start:start + batch_size]
        data = await asyncio.to_thread(
            _bridge_post_json,
            bridge_base=payload.bridge_base,
            path="/backend/file-dashboard/post-attachments",
            payload={
                "db_name": payload.db_name,
                "chat_bot_id": payload.chat_bot_id,
                "urls": batch,
                "limit": len(batch),
                "concurrency": max(1, min(int(payload.concurrency or 8), 24)),
                "probe_timeout_sec": max(2, min(int(payload.timeout or 8), 30)),
                "probe_fetch_timeout_sec": max(1, min(int(payload.timeout or 8), 20)),
                "include_learning_status": "1",
                "fast_probe": "1",
                "job_id": "local-exploration-attachment-check",
            },
            timeout=max(90.0, float(payload.timeout or 8) * max(2, len(batch))),
        )
        posts.extend([dict(row or {}) for row in data.get("posts") or [] if isinstance(row, dict)])
    return posts


async def _probe_attachments_locally(
    *,
    payload: ExplorationAttachmentRequest,
    urls: List[str],
) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(max(1, min(int(payload.concurrency or 8), 20)))

    async def one(url: str) -> Dict[str, Any]:
        async with sem:
            return await probe_url(
                url,
                use_playwright=False,
                timeout_sec=max(1.0, float(payload.timeout or 8.0)),
            )

    return await asyncio.gather(*(one(url) for url in urls))


async def load_postgres_type_post_urls(
    *,
    pg_db_name: str,
    pg_table: str = "",
    pg_schema: str = "public",
    limit: int = 20,
) -> Dict[str, Any]:
    url_candidates = ("url", "content", "source_url", "contents_url", "href", "file_source")
    type_candidates = ("type", "content_type")
    factory = get_session_factory(pg_db_name or None)
    async with factory() as session:
        if pg_table:
            table_rows = [{"table_schema": pg_schema or "public", "table_name": pg_table}]
        else:
            result = await session.execute(
                text(
                    """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_schema, table_name
                    """
                )
            )
            table_rows = [dict(row._mapping) for row in result.fetchall()]

        inspected: List[Dict[str, Any]] = []
        for table_row in table_rows:
            schema = str(table_row.get("table_schema") or "public")
            table = str(table_row.get("table_name") or "")
            if not table:
                continue
            col_result = await session.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table
                    """
                ),
                {"schema": schema, "table": table},
            )
            cols = {str(row[0]) for row in col_result.fetchall()}
            type_col = next((c for c in type_candidates if c in cols), "")
            url_col = next((c for c in url_candidates if c in cols), "")
            if not type_col or not url_col:
                inspected.append({"table": f"{schema}.{table}", "reason": "missing_type_or_url_column"})
                continue
            order_col = "id" if "id" in cols else ""
            order_sql = f"ORDER BY {_quote_pg_ident(order_col)} DESC" if order_col else ""
            sql = f"""
                SELECT {_quote_pg_ident(url_col)} AS url
                FROM {_quote_pg_ident(schema)}.{_quote_pg_ident(table)}
                WHERE LOWER(COALESCE(CAST({_quote_pg_ident(type_col)} AS TEXT), '')) = 'post'
                  AND {_quote_pg_ident(url_col)} IS NOT NULL
                  AND TRIM(CAST({_quote_pg_ident(url_col)} AS TEXT)) <> ''
                {order_sql}
                LIMIT :limit
            """
            rows = await session.execute(text(sql), {"limit": max(1, min(int(limit or 20), 5000))})
            urls = _dedupe_urls([row._mapping.get("url") for row in rows.fetchall()], max(1, min(int(limit or 20), 5000)))
            inspected.append(
                {
                    "table": f"{schema}.{table}",
                    "type_col": type_col,
                    "url_col": url_col,
                    "count": len(urls),
                }
            )
            if urls:
                return {
                    "urls": urls,
                    "source_table": f"{schema}.{table}",
                    "type_col": type_col,
                    "url_col": url_col,
                    "inspected": inspected,
                }
        return {"urls": [], "source_table": "", "type_col": "", "url_col": "", "inspected": inspected}


async def _run_file_learning_job(
    *,
    job_id: str,
    db_name: str,
    chat_bot_id: str,
    results: List[Dict[str, Any]],
) -> None:
    JOBS[job_id]["status"] = "running"
    workflow = FileDownloadWorkflow()
    workflow.job_id = job_id
    workflow.db_name = db_name
    workflow.chat_bot_id = chat_bot_id
    workflow.enable_db_save = True
    workflow.file_pipeline_skip_learning = False
    workflow.sync_after_download = True
    workflow.start_urls_override_source = "attachment_probe_harness"

    start_items: List[Dict[str, Any]] = []
    for row in results:
        attachments = row.get("attachments") or []
        if not attachments:
            continue
        start_items.append(
            {
                "url": row.get("url"),
                "type": "post",
                "direct_attachments": attachments,
                "title": row.get("title") or "",
            }
        )

    def progress(stats: Dict[str, Any]) -> None:
        JOBS[job_id]["stats"] = dict(stats or {})

    try:
        JOBS[job_id]["start_items"] = len(start_items)
        await workflow.start_workflow(
            start_urls=start_items,
            progress_callback=progress,
            start_urls_override_source="attachment_probe_harness",
            content_type="file",
            colle="file",
            db_name=db_name,
            chat_bot_id=chat_bot_id,
        )
        JOBS[job_id]["status"] = "completed"
        JOBS[job_id]["stats"] = workflow.get_stats()
    except Exception as exc:
        JOBS[job_id]["status"] = "failed"
        JOBS[job_id]["error"] = repr(exc)
    finally:
        try:
            await workflow._close_playwright()
        except Exception:
            pass


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return HTML


@app.post("/api/probe")
async def api_probe(payload: ProbeRequest):
    urls = list(payload.urls or [])
    bridge_error = ""
    bridge_count = 0
    if payload.use_bridge:
        if not payload.db_name or not payload.chat_bot_id:
            return JSONResponse(
                {"ok": False, "message": "Bridge mode requires db_name and chat_bot_id."},
                status_code=400,
            )
        try:
            bridge_urls = await asyncio.to_thread(
                load_urls_from_bridge,
                bridge_base=payload.bridge_base,
                bridge_path=payload.bridge_path,
                db_name=payload.db_name,
                chat_bot_id=payload.chat_bot_id,
                limit=max(1, min(int(payload.limit or 100), 5000)),
                status=payload.status,
            )
            bridge_count = len(bridge_urls)
            urls.extend(bridge_urls)
        except Exception as exc:
            bridge_error = repr(exc)
            try:
                bridge_urls = await load_urls_from_local_db(
                    db_name=payload.db_name,
                    chat_bot_id=payload.chat_bot_id,
                    limit=max(1, min(int(payload.limit or 100), 5000)),
                    status=payload.status,
                )
                bridge_count = len(bridge_urls)
                urls.extend(bridge_urls)
                bridge_error = f"remote bridge failed; used local DB fallback: {bridge_error}"
            except Exception as local_exc:
                bridge_error = f"{bridge_error}; local DB fallback failed: {repr(local_exc)}"

    targets = _dedupe_urls(urls, max(1, min(int(payload.max_urls or 100), 5000)))
    if not targets and bridge_error:
        return JSONResponse(
            {"ok": False, "message": "Bridge URL load failed.", "bridge_error": bridge_error},
            status_code=502,
        )
    if not targets:
        return JSONResponse({"ok": False, "message": "No URLs to probe."}, status_code=400)

    sem = asyncio.Semaphore(max(1, min(int(payload.concurrency or 3), 20)))

    async def one(url: str) -> Dict[str, Any]:
        async with sem:
            return await probe_url(
                url,
                use_playwright=bool(payload.playwright),
                timeout_sec=max(1.0, float(payload.timeout or 30.0)),
            )

    results = await asyncio.gather(*(one(url) for url in targets))
    pages_with_attachments = sum(1 for row in results if int(row.get("attachment_count") or 0) > 0)
    total_attachments = sum(int(row.get("attachment_count") or 0) for row in results)
    return {
        "ok": True,
        "target_count": len(targets),
        "bridge_count": bridge_count,
        "bridge_error": bridge_error,
        "with_attachments": pages_with_attachments,
        "total_attachments": total_attachments,
        "results": results,
    }


@app.post("/api/exploration-attachments")
async def api_exploration_attachments(payload: ExplorationAttachmentRequest):
    if not payload.db_name:
        return JSONResponse({"ok": False, "message": "db_name is required."}, status_code=400)
    if not payload.chat_bot_id:
        return JSONResponse({"ok": False, "message": "chat_bot_id is required."}, status_code=400)

    try:
        exploration = await _load_exploration_rows_via_bridge(payload)
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "message": "f1_dev exploration-posts bridge failed.",
                "error": repr(exc),
            },
            status_code=502,
        )

    max_urls = max(1, min(int(payload.max_urls or payload.limit or 50000), 200000))
    urls = _dedupe_urls([_row_url(row) for row in exploration.get("rows") or []], max_urls)
    if not urls:
        return JSONResponse(
            {
                "ok": False,
                "message": "No type=post URLs found in ASADAL_CRAWLING_EXPLORATION.",
                "exploration": exploration,
            },
            status_code=404,
        )

    try:
        if payload.use_remote_probe:
            posts = await _probe_attachments_via_remote_dashboard(payload=payload, urls=urls)
            probe_mode = "f1_dev_post_attachments"
        else:
            posts = await _probe_attachments_locally(payload=payload, urls=urls)
            probe_mode = "local_probe_url"
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "message": "Attachment extraction failed.",
                "error": repr(exc),
                "exploration": exploration,
                "target_count": len(urls),
            },
            status_code=502,
        )

    attachment_total = sum(int(row.get("attachment_count") or len(row.get("attachments") or []) or 0) for row in posts)
    learned_total = sum(int(row.get("learned_count") or 0) for row in posts)
    matched_total = sum(int(row.get("matched_count") or 0) for row in posts)
    rows_by_url = {_row_url(row): row for row in exploration.get("rows") or []}
    enriched_posts = []
    for idx, post in enumerate(posts, 1):
        item = dict(post or {})
        url = _row_url(item)
        item["exploration_row"] = rows_by_url.get(url) or {}
        item["_index"] = idx
        enriched_posts.append(item)

    return {
        "ok": True,
        "db_name": payload.db_name,
        "chat_bot_id": payload.chat_bot_id,
        "table": "ASADAL_CRAWLING_EXPLORATION",
        "probe_mode": probe_mode,
        "exploration_total": int(exploration.get("total") or 0),
        "exploration_returned": int(exploration.get("returned") or 0),
        "target_count": len(urls),
        "with_attachments": sum(1 for row in enriched_posts if int(row.get("attachment_count") or 0) > 0),
        "total_attachments": attachment_total,
        "learned_attachments": learned_total,
        "matched_attachments": matched_total,
        "missing_attachments": max(0, attachment_total - learned_total),
        "bridge_pages": exploration.get("pages") or [],
        "rows": exploration.get("rows") or [],
        "posts": enriched_posts,
        "results": enriched_posts,
    }


@app.post("/api/exploration-attachments/learn")
async def api_learn_found_attachments(payload: LearnFoundAttachmentsRequest):
    if not payload.db_name:
        return JSONResponse({"ok": False, "message": "db_name is required."}, status_code=400)
    if not payload.chat_bot_id:
        return JSONResponse({"ok": False, "message": "chat_bot_id is required."}, status_code=400)

    built = _build_crawl_targets_from_posts(payload)
    targets = built["targets"]
    attachment_count = int(built["attachment_count"] or 0)
    if not targets or not attachment_count:
        return JSONResponse(
            {"ok": False, "message": "No attachment URLs to save/learn.", "target_count": 0},
            status_code=400,
        )

    job_id = str(payload.job_id or f"probe-learn-{uuid4().hex[:12]}").strip()
    try:
        data = await asyncio.to_thread(
            _bridge_post_json,
            bridge_base=payload.bridge_base,
            path="/backend/file-dashboard/crawl-targets",
            payload={
                "job_id": job_id,
                "db_name": payload.db_name,
                "chat_bot_id": payload.chat_bot_id,
                "targets": targets,
                "learn_list_duplicate_exclude_enabled": bool(payload.learn_list_duplicate_exclude_enabled),
                "method": "period",
            },
            timeout=120.0,
        )
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "message": "File crawler batch save/learn endpoint failed.",
                "error": repr(exc),
                "job_id": job_id,
                "target_count": len(targets),
                "attachment_count": attachment_count,
            },
            status_code=502,
        )

    return {
        "ok": True,
        "job_id": data.get("job_id") or job_id,
        "db_name": payload.db_name,
        "chat_bot_id": payload.chat_bot_id,
        "post_target_count": len(targets),
        "attachment_target_count": attachment_count,
        "bridge_response": data,
    }


@app.post("/api/harness/run")
async def api_harness_run(payload: HarnessRequest):
    if not payload.pg_db_name and not payload.db_name:
        return JSONResponse({"ok": False, "message": "pg_db_name or db_name is required."}, status_code=400)
    if payload.learn and (not payload.db_name or not payload.chat_bot_id):
        return JSONResponse({"ok": False, "message": "Learning requires db_name and chat_bot_id."}, status_code=400)

    try:
        pg_info = await load_postgres_type_post_urls(
            pg_db_name=payload.pg_db_name or payload.db_name,
            pg_table=payload.pg_table,
            pg_schema=payload.pg_schema or "public",
            limit=max(1, min(int(payload.limit or 20), 5000)),
        )
    except Exception as exc:
        return JSONResponse(
            {
                "ok": False,
                "message": "PostgreSQL type=post URL load failed.",
                "error": repr(exc),
            },
            status_code=502,
        )
    targets = _dedupe_urls(pg_info.get("urls") or [], max(1, min(int(payload.max_urls or 20), 5000)))
    if not targets:
        return JSONResponse(
            {"ok": False, "message": "No PostgreSQL type=post URLs found.", "postgres": pg_info},
            status_code=404,
        )

    sem = asyncio.Semaphore(max(1, min(int(payload.concurrency or 3), 20)))

    async def one(url: str) -> Dict[str, Any]:
        async with sem:
            return await probe_url(
                url,
                use_playwright=bool(payload.playwright),
                timeout_sec=max(1.0, float(payload.timeout or 30.0)),
            )

    results = await asyncio.gather(*(one(url) for url in targets))
    total_attachments = sum(int(row.get("attachment_count") or 0) for row in results)
    pages_with_attachments = sum(1 for row in results if int(row.get("attachment_count") or 0) > 0)

    learn_job_id: Optional[str] = None
    if payload.learn and total_attachments:
        learn_job_id = f"probe-{uuid4().hex[:12]}"
        JOBS[learn_job_id] = {
            "job_id": learn_job_id,
            "status": "queued",
            "db_name": payload.db_name,
            "chat_bot_id": payload.chat_bot_id,
            "target_count": len(targets),
            "total_attachments": total_attachments,
            "stats": {},
        }
        asyncio.create_task(
            _run_file_learning_job(
                job_id=learn_job_id,
                db_name=payload.db_name,
                chat_bot_id=payload.chat_bot_id,
                results=results,
            )
        )

    return {
        "ok": True,
        "postgres": pg_info,
        "target_count": len(targets),
        "with_attachments": pages_with_attachments,
        "total_attachments": total_attachments,
        "learn_job_id": learn_job_id,
        "results": results,
    }


@app.get("/api/harness/jobs/{job_id}")
async def api_harness_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        return JSONResponse({"ok": False, "message": "job not found", "job_id": job_id}, status_code=404)
    return {"ok": True, "job": job}


HTML = r"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Attachment Probe</title>
  <style>
    :root {
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #1d2430;
      --muted: #667085;
      --line: #d8dee8;
      --accent: #1f7a5a;
      --accent-strong: #145f45;
      --danger: #b42318;
      --chip: #edf7f3;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      height: 56px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 22px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }
    h1 { margin: 0; font-size: 18px; font-weight: 700; }
    main {
      display: grid;
      grid-template-columns: minmax(320px, 420px) 1fr;
      min-height: calc(100vh - 56px);
    }
    aside {
      border-right: 1px solid var(--line);
      background: var(--panel);
      padding: 18px;
      overflow: auto;
    }
    section {
      padding: 18px;
      overflow: auto;
    }
    label {
      display: block;
      font-weight: 650;
      margin: 14px 0 6px;
    }
    input, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 10px;
      font: inherit;
      background: #fff;
    }
    textarea {
      min-height: 135px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
    }
    .row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .check {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-top: 12px;
      color: var(--text);
      font-weight: 600;
    }
    .check input {
      width: 16px;
      height: 16px;
    }
    button {
      width: 100%;
      margin-top: 18px;
      border: 0;
      border-radius: 6px;
      background: var(--accent);
      color: #fff;
      font-weight: 750;
      padding: 11px 12px;
      cursor: pointer;
    }
    button:hover { background: var(--accent-strong); }
    button:disabled { opacity: .6; cursor: wait; }
    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric, .result {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 13px;
    }
    .metric b {
      display: block;
      font-size: 22px;
      margin-bottom: 2px;
    }
    .metric span { color: var(--muted); }
    .result {
      margin-bottom: 10px;
    }
    .result-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }
    .url {
      overflow-wrap: anywhere;
      font-family: ui-monospace, SFMono-Regular, Consolas, monospace;
      font-size: 13px;
    }
    .badge {
      flex: 0 0 auto;
      padding: 3px 8px;
      border-radius: 999px;
      background: var(--chip);
      color: var(--accent-strong);
      font-weight: 750;
    }
    .error { color: var(--danger); margin-top: 8px; }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
      table-layout: fixed;
    }
    th, td {
      border-top: 1px solid var(--line);
      padding: 8px 6px;
      text-align: left;
      vertical-align: top;
      overflow-wrap: anywhere;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      font-weight: 750;
    }
    td:nth-child(1), th:nth-child(1) { width: 72px; }
    td:nth-child(2), th:nth-child(2) { width: 24%; }
    td:nth-child(4), th:nth-child(4) { width: 80px; }
    .muted { color: var(--muted); }
    .hidden { display: none; }
    @media (max-width: 860px) {
      main { grid-template-columns: 1fr; }
      aside { border-right: 0; border-bottom: 1px solid var(--line); }
      .summary { grid-template-columns: repeat(2, 1fr); }
    }
  </style>
</head>
<body>
  <header>
    <h1>Attachment Probe</h1>
    <span class="muted" id="state">Ready</span>
  </header>
  <main>
    <aside>
      <label for="urls">URL</label>
      <textarea id="urls" placeholder="https://example.go.kr/board/view.do?id=1"></textarea>

      <label class="check"><input id="useBridge" type="checkbox"> f1_dev bridge에서 type=post URL 가져오기</label>

      <div id="bridgePanel">
        <label for="bridgeBase">Bridge base</label>
        <input id="bridgeBase" value="https://api-aipro.chatbaram.com/api-aipro/f1_dev/Ai_Pro_filecrawler">
        <label for="bridgePath">Bridge path</label>
        <input id="bridgePath" value="/backend/file-dashboard/exploration-posts">
        <label for="dbName">DB name</label>
        <input id="dbName" placeholder="dev_user">
        <label for="chatBotId">chat_bot_id</label>
        <input id="chatBotId" placeholder="AS...">
      </div>

      <div class="row">
        <div>
          <label for="limit">Exploration limit</label>
          <input id="limit" type="number" value="50000" min="1" max="200000">
        </div>
        <div>
          <label for="maxUrls">Max probe</label>
          <input id="maxUrls" type="number" value="50000" min="1" max="200000">
        </div>
      </div>

      <div class="row">
        <div>
          <label for="concurrency">Concurrency</label>
          <input id="concurrency" type="number" value="3" min="1" max="20">
        </div>
        <div>
          <label for="timeout">Timeout</label>
          <input id="timeout" type="number" value="30" min="1">
        </div>
      </div>

      <label for="status">Status filter</label>
      <input id="status" placeholder="optional, e.g. Y">

      <label class="check"><input id="playwright" type="checkbox"> static 실패 시 Playwright 재시도</label>
      <button id="runBtn">Run probe</button>
      <label class="check"><input id="remoteProbe" type="checkbox" checked> 첨부 추출도 f1_dev 대시보드 API 사용</label>
      <button id="explorationBtn">Run exploration attachment check</button>
      <label class="check"><input id="onlyMissingAttachments" type="checkbox" checked> 이미 학습된 첨부 제외</label>
      <button id="learnFoundBtn">현재 파일크롤링 저장/학습 시작</button>

      <label for="pgDbName">PostgreSQL DB</label>
      <input id="pgDbName" placeholder="ne">
      <div class="row">
        <div>
          <label for="pgSchema">PG schema</label>
          <input id="pgSchema" value="public">
        </div>
        <div>
          <label for="pgTable">PG table</label>
          <input id="pgTable" placeholder="auto">
        </div>
      </div>
      <label class="check"><input id="learnAfterProbe" type="checkbox"> Learn found attachments</label>
      <button id="harnessBtn">Run PG harness</button>
    </aside>
    <section>
      <div class="summary">
        <div class="metric"><b id="mTargets">0</b><span>Targets</span></div>
        <div class="metric"><b id="mFound">0</b><span>Attachments</span></div>
        <div class="metric"><b id="mBridge">0</b><span>Exploration rows</span></div>
        <div class="metric"><b id="mElapsed">0s</b><span>Elapsed</span></div>
      </div>
      <div id="message" class="muted">결과가 여기에 표시됩니다.</div>
      <div id="results"></div>
    </section>
  </main>
  <script>
    const $ = (id) => document.getElementById(id);
    let lastProbeData = null;
    function lines(value) {
      return String(value || "").split(/\r?\n/).map(v => v.trim()).filter(Boolean);
    }
    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    }
    function payload() {
      return {
        urls: lines($("urls").value),
        use_bridge: $("useBridge").checked,
        bridge_base: $("bridgeBase").value.trim(),
        bridge_path: $("bridgePath").value.trim(),
        db_name: $("dbName").value.trim(),
        chat_bot_id: $("chatBotId").value.trim(),
        status: $("status").value.trim(),
        limit: Number($("limit").value || 100),
        max_urls: Number($("maxUrls").value || 100),
        concurrency: Number($("concurrency").value || 3),
        timeout: Number($("timeout").value || 30),
        playwright: $("playwright").checked
      };
    }
    function harnessPayload() {
      return {
        pg_db_name: $("pgDbName").value.trim() || $("dbName").value.trim(),
        pg_schema: $("pgSchema").value.trim() || "public",
        pg_table: $("pgTable").value.trim(),
        db_name: $("dbName").value.trim(),
        chat_bot_id: $("chatBotId").value.trim(),
        limit: Number($("limit").value || 20),
        max_urls: Number($("maxUrls").value || 20),
        concurrency: Number($("concurrency").value || 3),
        timeout: Number($("timeout").value || 30),
        playwright: $("playwright").checked,
        learn: $("learnAfterProbe").checked
      };
    }
    function explorationPayload() {
      return {
        bridge_base: $("bridgeBase").value.trim(),
        db_name: $("dbName").value.trim(),
        chat_bot_id: $("chatBotId").value.trim(),
        limit: Number($("limit").value || 50000),
        max_urls: Number($("maxUrls").value || $("limit").value || 50000),
        page_size: Math.min(5000, Math.max(1, Number($("limit").value || 5000))),
        concurrency: Number($("concurrency").value || 8),
        timeout: Number($("timeout").value || 8),
        use_remote_probe: $("remoteProbe").checked
      };
    }
    function learnFoundPayload() {
      const source = lastProbeData || {};
      return {
        bridge_base: $("bridgeBase").value.trim(),
        db_name: $("dbName").value.trim(),
        chat_bot_id: $("chatBotId").value.trim(),
        posts: source.posts || source.results || [],
        only_missing: $("onlyMissingAttachments").checked,
        max_attachments: Number($("maxUrls").value || $("limit").value || 50000),
        learn_list_duplicate_exclude_enabled: true
      };
    }
    function render(data, elapsed) {
      lastProbeData = data;
      $("mTargets").textContent = data.target_count || 0;
      $("mFound").textContent = data.total_attachments || 0;
      $("mBridge").textContent = data.exploration_returned || data.bridge_count || 0;
      $("mElapsed").textContent = `${elapsed.toFixed(1)}s`;
      const pg = data.postgres && data.postgres.source_table ? ` | PG ${data.postgres.source_table}` : "";
      const learn = data.learn_job_id ? ` | learn job ${data.learn_job_id}` : "";
      const exploration = data.exploration_total != null
        ? ` | exploration ${Number(data.exploration_returned || 0).toLocaleString()} / ${Number(data.exploration_total || 0).toLocaleString()}`
        : "";
      const learned = data.learned_attachments != null
        ? ` | learned ${Number(data.learned_attachments || 0).toLocaleString()} / missing ${Number(data.missing_attachments || 0).toLocaleString()}`
        : "";
      $("message").textContent = data.bridge_error ? `Bridge warning: ${data.bridge_error}` : `Done${pg}${learn}${exploration}${learned}`;
      const html = (data.results || data.posts || []).map(row => {
        const atts = row.attachments || [];
        const rows = atts.map(a => `
          <tr>
            <td>${esc(a.kind || "url")}</td>
            <td>${esc(a.name || "attachment")}</td>
            <td>${esc(a.href || a.url || "")}${a.learned ? " <b>[learned]</b>" : ""}</td>
            <td>${esc(a.method || "GET")}</td>
          </tr>`).join("");
        const rowOk = row.ok !== false && row.status !== "error" && row.status !== "timeout";
        return `
          <article class="result">
            <div class="result-head">
              <div class="url">${esc(row.url)}</div>
              <div class="badge">${Number(row.attachment_count || 0)}</div>
            </div>
            ${rowOk ? "" : `<div class="error">${esc(row.error || "failed")}</div>`}
            ${atts.length ? `<table><thead><tr><th>Kind</th><th>Name</th><th>Href</th><th>Method</th></tr></thead><tbody>${rows}</tbody></table>` : `<div class="muted" style="margin-top:8px;">첨부 없음</div>`}
          </article>`;
      }).join("");
      $("results").innerHTML = html;
    }
    $("runBtn").addEventListener("click", async () => {
      const started = performance.now();
      $("runBtn").disabled = true;
      $("state").textContent = "Running";
      $("message").textContent = "탐색 중...";
      $("results").innerHTML = "";
      try {
        const res = await fetch("/api/probe", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(payload())
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error(data.message || data.bridge_error || `HTTP ${res.status}`);
        }
        render(data, (performance.now() - started) / 1000);
      } catch (err) {
        $("message").innerHTML = `<span class="error">${esc(err.message || err)}</span>`;
      } finally {
        $("runBtn").disabled = false;
        $("state").textContent = "Ready";
      }
    });
    $("explorationBtn").addEventListener("click", async () => {
      const started = performance.now();
      $("explorationBtn").disabled = true;
      $("state").textContent = "Running";
      $("message").textContent = "f1_dev로 exploration post row를 가져오고 첨부 URL을 추출 중입니다...";
      $("results").innerHTML = "";
      try {
        const res = await fetch("/api/exploration-attachments", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(explorationPayload())
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error(data.message || data.error || `HTTP ${res.status}`);
        }
        render(data, (performance.now() - started) / 1000);
      } catch (err) {
        $("message").innerHTML = `<span class="error">${esc(err.message || err)}</span>`;
      } finally {
        $("explorationBtn").disabled = false;
        $("state").textContent = "Ready";
      }
    });
    $("learnFoundBtn").addEventListener("click", async () => {
      $("learnFoundBtn").disabled = true;
      $("state").textContent = "Starting learn";
      $("message").textContent = "Sending found attachment URLs to the file crawler save/learn endpoint...";
      try {
        const res = await fetch("/api/exploration-attachments/learn", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(learnFoundPayload())
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error(data.message || data.error || `HTTP ${res.status}`);
        }
        $("message").textContent = `File crawl save/learn started: ${data.job_id} | posts ${Number(data.post_target_count || 0).toLocaleString()} | attachments ${Number(data.attachment_target_count || 0).toLocaleString()}`;
        $("state").textContent = `Learn ${data.job_id}`;
      } catch (err) {
        $("message").innerHTML = `<span class="error">${esc(err.message || err)}</span>`;
        $("state").textContent = "Ready";
      } finally {
        $("learnFoundBtn").disabled = false;
      }
    });
    $("harnessBtn").addEventListener("click", async () => {
      const started = performance.now();
      $("harnessBtn").disabled = true;
      $("state").textContent = "Running";
      $("message").textContent = "Loading PostgreSQL type=post URLs...";
      $("results").innerHTML = "";
      try {
        const res = await fetch("/api/harness/run", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify(harnessPayload())
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error(data.message || `HTTP ${res.status}`);
        }
        render(data, (performance.now() - started) / 1000);
        if (data.learn_job_id) {
          pollJob(data.learn_job_id);
        }
      } catch (err) {
        $("message").innerHTML = `<span class="error">${esc(err.message || err)}</span>`;
      } finally {
        $("harnessBtn").disabled = false;
        $("state").textContent = "Ready";
      }
    });
    async function pollJob(jobId) {
      for (let i = 0; i < 120; i++) {
        await new Promise(r => setTimeout(r, 2000));
        try {
          const res = await fetch(`/api/harness/jobs/${encodeURIComponent(jobId)}`);
          const data = await res.json();
          if (!res.ok || !data.ok) continue;
          const job = data.job || {};
          $("state").textContent = `Learn ${job.status || "unknown"}`;
          if (["completed", "failed"].includes(job.status)) {
            $("message").textContent = `Learn job ${jobId}: ${job.status}${job.error ? " | " + job.error : ""}`;
            break;
          }
        } catch (_) {}
      }
    }
  </script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the attachment probe browser UI.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
