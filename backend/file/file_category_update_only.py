import asyncio
import json
import logging
import os
import re
import ssl
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.request import Request as UrlRequest, urlopen

from backend.board.board_meta_extractor import extract_attachment_summary_from_html
from backend.file.file_category_apply import (
    apply_file_categories_by_source_pages,
    apply_file_category_by_source_page,
    apply_file_category_by_subject_names,
)
from backend.shared.file_crawl_post_urls import load_file_crawl_post_url_strings
from backend.shared.pre_explored_url import _load_category_url_pattern_object, resolve_cate_for_detail_url
from backend.shared.sub_cate_mode import get_sub_cate_mode_from_config, is_sub_cate_overwrite
from utils.db_name import resolve_db_name
from utils.file import strip_fallback_download_label
from utils.http_client import requests_get
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.file.file_category_update_only")


def _pg_safe_ident(value: str) -> str:
    text = str(value or "").strip().lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", text):
        raise ValueError(f"invalid_pg_identifier:{value!r}")
    return text


def _metadata_text_contains_url(metadata: Any, detail_url: str) -> bool:
    target = str(detail_url or "").strip()
    if not target:
        return False
    if isinstance(metadata, str):
        text = metadata
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = None
        if target in text:
            return True
    if isinstance(metadata, dict):
        stack: list[Any] = [metadata]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)
            elif isinstance(item, str) and item.strip() == target:
                return True
    return False


async def _pg_training_table_candidates(*, db_name: str, chat_bot_id: str) -> List[str]:
    candidates: List[str] = []
    try:
        from db.mariadb_save_update import get_account_identifier_from_chatbot_setup

        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
    except Exception:
        account_identifier = None
    for raw in (account_identifier, chat_bot_id, str(chat_bot_id or "").replace("-", "")):
        text = str(raw or "").strip().lower()
        if not text:
            continue
        table = f"td_{text}_training_data"
        try:
            _pg_safe_ident(table)
        except Exception:
            continue
        if table not in candidates:
            candidates.append(table)
    return candidates


async def _pg_table_columns(*, db_name: str, table_name: str) -> Set[str]:
    from sqlalchemy import text
    from db.db_postgres import get_session_factory

    session_factory = get_session_factory(db_name)
    async with session_factory() as session:
        result = await session.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = :table_name
                """
            ),
            {"table_name": table_name},
        )
        return {str(row[0]) for row in result.fetchall() if row and row[0]}


async def _load_pg_subjects_by_metadata_url(
    *,
    db_name: str,
    chat_bot_id: str,
    assignments: Iterable[Dict[str, str]],
    limit_per_query: int = 200,
) -> Tuple[Dict[str, Set[str]], Dict[str, Any]]:
    from sqlalchemy import text
    from db.db_postgres import get_session_factory

    normalized = [
        {
            "source_page": str((item or {}).get("source_page") or "").strip(),
            "board_cate2": str((item or {}).get("board_cate2") or "").strip(),
        }
        for item in assignments or []
        if isinstance(item, dict)
        and str((item or {}).get("source_page") or "").strip()
        and str((item or {}).get("board_cate2") or "").strip()
    ]
    if not normalized:
        return {}, {"reason": "no_assignments"}

    table_name = ""
    cols: Set[str] = set()
    for candidate in await _pg_training_table_candidates(db_name=db_name, chat_bot_id=chat_bot_id):
        candidate_cols = await _pg_table_columns(db_name=db_name, table_name=candidate)
        if {"content_metadata", "content"}.issubset(candidate_cols):
            table_name = candidate
            cols = candidate_cols
            break
    if not table_name:
        return {}, {"reason": "pg_table_or_columns_not_found"}

    url_to_cate2 = {item["source_page"]: item["board_cate2"] for item in normalized}
    urls = list(url_to_cate2.keys())
    subjects_by_cate2: Dict[str, Set[str]] = {}
    sample: List[Dict[str, str]] = []
    quoted_table = f'public."{_pg_safe_ident(table_name)}"'
    selected_cols = ["content", "content_metadata"]
    if "subject" in cols:
        selected_cols.insert(0, "subject")
    if "content_type" in cols:
        type_filter = "AND LOWER(COALESCE(content_type, '')) = 'file'"
    else:
        type_filter = ""

    session_factory = get_session_factory(db_name)
    async with session_factory() as session:
        for offset in range(0, len(urls), limit_per_query):
            chunk = urls[offset : offset + limit_per_query]
            like_parts = []
            params: Dict[str, Any] = {}
            for idx, url in enumerate(chunk):
                key = f"url_{idx}"
                like_parts.append(f"content_metadata::text LIKE :{key}")
                params[key] = f"%{url}%"
            sql = (
                f"SELECT {', '.join(selected_cols)} FROM {quoted_table} "
                f"WHERE content_metadata IS NOT NULL {type_filter} "
                f"AND ({' OR '.join(like_parts)}) "
                "LIMIT 5000"
            )
            result = await session.execute(text(sql), params)
            for row in result.mappings().all():
                metadata = row.get("content_metadata")
                matched_url = ""
                for url in chunk:
                    if _metadata_text_contains_url(metadata, url):
                        matched_url = url
                        break
                if not matched_url:
                    continue
                subject = str(row.get("subject") or row.get("content") or "").strip()
                if not subject:
                    continue
                cate2 = url_to_cate2.get(matched_url, "")
                if not cate2:
                    continue
                subjects_by_cate2.setdefault(cate2, set()).add(subject)
                if len(sample) < 10:
                    sample.append({"url": matched_url[:180], "board_cate2": cate2, "subject": subject[:160]})

    return subjects_by_cate2, {
        "reason": "pg_content_metadata",
        "pg_table": table_name,
        "assignment_count": len(normalized),
        "matched_subject_count": sum(len(v) for v in subjects_by_cate2.values()),
        "sample": sample,
    }


def _first_request_url(data: Dict[str, Any]) -> str:
    for key in ("contents_url", "target_url", "url", "detail_url"):
        text = str((data or {}).get(key) or "").strip()
        if text:
            return ensure_url_scheme(text)
    contents = (data or {}).get("contents")
    if isinstance(contents, list) and contents:
        text = str(contents[0] or "").strip()
        if text:
            return ensure_url_scheme(text)
    return ""


def _looks_like_root_or_index_url(url: str) -> bool:
    text = str(url or "").strip().lower()
    if not text:
        return False
    try:
        from urllib.parse import urlparse

        parsed = urlparse(text)
        path = (parsed.path or "").strip().rstrip("/")
    except Exception:
        path = ""
    if not path or path == "/":
        return True
    return path.endswith("/index.do") or path.endswith("/main.do")


def _fetch_text_sync(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    req = UrlRequest(url, headers=headers)
    timeout = _fetch_timeout_seconds()
    ssl_context = None
    if str(url or "").lower().startswith("https://"):
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.set_ciphers("DEFAULT@SECLEVEL=0")
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            try:
                ssl_context.options |= ssl.OP_LEGACY_SERVER_CONNECT
            except AttributeError:
                pass
        except Exception:
            ssl_context = None
    try:
        open_kwargs: Dict[str, Any] = {"timeout": timeout}
        if ssl_context is not None:
            open_kwargs["context"] = ssl_context
        with urlopen(req, **open_kwargs) as resp:
            raw = resp.read(5 * 1024 * 1024)
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except Exception as first_exc:
        logger.info(
            "[FileCategoryUpdateOnly] urlopen fetch failed; trying requests fallback | url=%s err=%s",
            str(url or "")[:220],
            first_exc,
        )

    resp = requests_get(
        url,
        headers=headers,
        timeout=timeout,
        verify=False,
    )
    resp.raise_for_status()
    if not resp.encoding:
        resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text[: 5 * 1024 * 1024]


def _fetch_timeout_seconds() -> float:
    try:
        raw = float(os.getenv("FILE_CATEGORY_UPDATE_ONLY_FETCH_TIMEOUT", "6") or "6")
    except Exception:
        raw = 6.0
    return max(2.0, min(raw, 20.0))


def _cate_pair_from_item_type(item_type: Any) -> Tuple[str, str]:
    text = str(item_type or "").strip()
    if not text.startswith("cate_match|"):
        return "", ""
    parts = text.split("|")
    if len(parts) >= 3:
        return str(parts[1] or "").strip(), str(parts[2] or "").strip()
    return "", ""


def _cate2_from_item_type(item_type: Any) -> str:
    return _cate_pair_from_item_type(item_type)[1]


def _attachment_subject_candidates(name: str) -> List[str]:
    text = str(name or "").strip()
    if not text:
        return []
    candidates = [text]
    cleaned = (strip_fallback_download_label(text) or text).strip()
    if cleaned and cleaned not in candidates:
        candidates.append(cleaned)
    return candidates


def _update_only_max_posts() -> int:
    try:
        raw = int(os.getenv("FILE_CATEGORY_UPDATE_ONLY_MAX_POSTS", "500") or "500")
    except Exception:
        raw = 500
    return max(1, min(raw, 5000))


async def _file_category_blank_only_from_config(
    *,
    data: Dict[str, Any],
    db_name: str,
    chat_bot_id: str,
    job_id: str,
) -> bool:
    try:
        sub_cate_mode = await get_sub_cate_mode_from_config(chat_bot_id, dbname=db_name)
    except Exception as exc:
        sub_cate_mode = "emp"
        logger.warning(
            "[FileCategoryUpdateOnly] sub_cate lookup failed; fallback emp | job_id=%s db=%s chat_bot_id=%s err=%s",
            job_id,
            db_name,
            chat_bot_id,
            exc,
        )
    blank_only = not is_sub_cate_overwrite(sub_cate_mode)
    try:
        data["_sub_cate_mode"] = sub_cate_mode
        data["_file_category_blank_only"] = blank_only
    except Exception:
        pass
    logger.info(
        "[FileCategoryUpdateOnly] sub_cate mode | job_id=%s db=%s chat_bot_id=%s sub_cate=%s blank_only=%s",
        job_id,
        db_name,
        chat_bot_id,
        sub_cate_mode,
        blank_only,
    )
    return blank_only


async def _update_one_detail(
    *,
    data: Dict[str, Any],
    db_name: str,
    chat_bot_id: str,
    job_id: str,
    detail_url: str,
    board_cate2: str,
) -> Dict[str, Any]:
    blank_only = await _file_category_blank_only_from_config(
        data=data,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        job_id=job_id,
    )
    source_result = await apply_file_category_by_source_page(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        source_page=detail_url,
        board_cate2=board_cate2,
        access_url=str((data or {}).get("access_url") or "").strip() or None,
        request_cookies=dict((data or {}).get("_category_sync_request_cookies") or {}),
        blank_only=blank_only,
    )
    if int(source_result.get("updated") or 0) > 0:
        source_result.update(
            {
                "detail_url": detail_url,
                "board_cate2": board_cate2,
                "attachment_count": 0,
                "subject_sample": [],
                "used_source_page_fast_path": True,
            }
        )
        return source_result

    try:
        html = await asyncio.to_thread(_fetch_text_sync, detail_url)
        logger.info(
            "[FileCategoryUpdateOnly] fetched | job_id=%s detail_url=%s html_len=%s board_cate2=%s",
            job_id,
            detail_url[:220],
            len(html or ""),
            board_cate2,
        )
        summary = extract_attachment_summary_from_html(
            html,
            url=detail_url,
            use_full_html=True,
            same_article_only=True,
        )
    except Exception as exc:
        logger.info(
            "[FileCategoryUpdateOnly] attachment extract skipped/failed | job_id=%s url=%s err=%s",
            job_id,
            detail_url[:180],
            exc,
        )
        return {
            "ok": True,
            "updated": 0,
            "reason": f"attachment_extract_failed:{exc}",
            "detail_url": detail_url,
            "board_cate2": board_cate2,
        }

    subject_names: List[str] = []
    for item in list((summary or {}).get("attachments") or []):
        if not isinstance(item, dict):
            continue
        for key in ("name", "filename", "title", "text"):
            name = str(item.get(key) or "").strip()
            for candidate in _attachment_subject_candidates(name):
                if candidate and candidate not in subject_names:
                    subject_names.append(candidate)
    logger.info(
        "[FileCategoryUpdateOnly] attachments | job_id=%s board_cate2=%s count=%s sample=%s detail_url=%s",
        job_id,
        board_cate2,
        len(subject_names),
        subject_names[:10],
        detail_url[:180],
    )

    logger.info(
        "[FileCategoryUpdateOnly] db-update call | job_id=%s db=%s chat_bot_id=%s board_cate2=%s subject_count=%s sample=%s",
        job_id,
        db_name,
        chat_bot_id,
        board_cate2,
        len(subject_names),
        subject_names[:10],
    )
    try:
        result = await apply_file_category_by_subject_names(
            chat_bot_id=chat_bot_id,
            db_name=db_name,
            subject_names=subject_names,
            board_cate2=board_cate2,
            access_url=str((data or {}).get("access_url") or "").strip() or None,
            request_cookies=dict((data or {}).get("_category_sync_request_cookies") or {}),
            blank_only=blank_only,
        )
    except Exception as exc:
        logger.warning(
            "[FileCategoryUpdateOnly] db-update failed | job_id=%s db=%s chat_bot_id=%s board_cate2=%s err=%s",
            job_id,
            db_name,
            chat_bot_id,
            board_cate2,
            exc,
            exc_info=True,
        )
        return {
            "ok": False,
            "updated": 0,
            "reason": f"db_update_failed:{exc}",
            "detail_url": detail_url,
            "board_cate2": board_cate2,
            "attachment_count": len(subject_names),
            "subject_sample": subject_names[:10],
        }
    result.update(
        {
            "detail_url": detail_url,
            "board_cate2": board_cate2,
            "attachment_count": len(subject_names),
            "subject_sample": subject_names[:10],
        }
    )
    return result


async def _run_update_from_post_db(
    *,
    data: Dict[str, Any],
    db_name: str,
    chat_bot_id: str,
    job_id: str,
    contents_url: str,
) -> Dict[str, Any]:
    category_obj = None
    logger.info(
        "[FileCategoryUpdateOnly] post-db category load start | job_id=%s contents_url=%s",
        job_id,
        contents_url[:220],
    )
    try:
        category_obj = await _load_category_url_pattern_object(
            chat_bot_id,
            db_name,
            contents_url=contents_url,
            require_nonempty_rules=True,
        )
        logger.info(
            "[FileCategoryUpdateOnly] post-db category load done | job_id=%s contents_url=%s category_obj=%s",
            job_id,
            contents_url[:220],
            bool(category_obj),
        )
    except Exception as exc:
        logger.warning(
            "[FileCategoryUpdateOnly] post-db category object load failed | job_id=%s contents_url=%s err=%s",
            job_id,
            contents_url[:180],
            exc,
        )
    max_posts = _update_only_max_posts()
    logger.info(
        "[FileCategoryUpdateOnly] post-db posts load start | job_id=%s contents_url=%s method=%s target_date=%s scope_path_prefix=%s",
        job_id,
        contents_url[:220],
        str((data or {}).get("method") or "period"),
        (data or {}).get("start_urls_target_date") or (data or {}).get("target_date"),
        (data or {}).get("scope_path_prefix"),
    )
    posts = await load_file_crawl_post_url_strings(
        db_name=db_name,
        contents_url=contents_url,
        chat_bot_id=chat_bot_id,
        method=str((data or {}).get("method") or "period"),
        target_date=(data or {}).get("start_urls_target_date") or (data or {}).get("target_date"),
        exploration_date_filter_enabled=False,
        scope_path_prefix=(data or {}).get("scope_path_prefix"),
        start_urls_order=(data or {}).get("start_urls_order"),
        use_category_rules=True,
        limit=max_posts,
    )
    logger.info(
        "[FileCategoryUpdateOnly] post-db posts load done | job_id=%s contents_url=%s posts=%s",
        job_id,
        contents_url[:220],
        len(posts or []),
    )
    total_posts = len(posts or [])
    updated = 0
    processed = 0
    skipped_no_cate = 0
    resolved_in_update_only = 0
    samples: List[Dict[str, Any]] = []
    assignments: List[Dict[str, str]] = []
    blank_only = await _file_category_blank_only_from_config(
        data=data,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        job_id=job_id,
    )
    logger.info(
        "[FileCategoryUpdateOnly] post-db start | job_id=%s contents_url=%s posts=%s max_posts=%s category_obj=%s",
        job_id,
        contents_url[:220],
        total_posts,
        max_posts,
        bool(category_obj),
    )
    for item in list(posts or [])[:max_posts]:
        if not isinstance(item, dict):
            continue
        detail_url = ensure_url_scheme(str(item.get("url") or "").strip()) if item.get("url") else ""
        board_cate1, board_cate2 = _cate_pair_from_item_type(item.get("type"))
        if detail_url and not board_cate2 and category_obj:
            try:
                pair = resolve_cate_for_detail_url(detail_url, category_obj)
                if pair:
                    board_cate1 = str(pair[0] or "").strip()
                    board_cate2 = str(pair[1] or "").strip()
                    if board_cate2:
                        resolved_in_update_only += 1
            except Exception:
                board_cate2 = ""
        if not (detail_url and board_cate2):
            skipped_no_cate += 1
            continue
        processed += 1
        assignments.append({"source_page": detail_url, "board_cate1": board_cate1, "board_cate2": board_cate2})
        if len(samples) < 10:
            samples.append(
                {
                    "detail_url": detail_url[:180],
                    "board_cate2": board_cate2,
                    "mode": "bulk_source_page_candidate",
                }
            )

    bulk_result = await apply_file_categories_by_source_pages(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        assignments=assignments,
        blank_only=blank_only,
    )
    updated += int(bulk_result.get("updated") or 0)
    missing_cate2 = {
        str((item or {}).get("board_cate2") or "").strip()
        for item in list(bulk_result.get("missing") or [])
        if isinstance(item, dict) and str((item or {}).get("board_cate2") or "").strip()
    }
    if missing_cate2 and int(bulk_result.get("mapped_category_count") or 0) == 0:
        logger.warning(
            "[FileCategoryUpdateOnly] fetch fallback skipped: all board categories lack file-learning mapping | job_id=%s missing_cate2=%s processed=%s",
            job_id,
            sorted(missing_cate2)[:20],
            processed,
        )
        return {
            "ok": True,
            "updated": updated,
            "reason": "file_learning_category_mapping_empty",
            "post_count": total_posts,
            "processed": processed,
            "skipped_no_cate": skipped_no_cate,
            "resolved_in_update_only": resolved_in_update_only,
            "max_posts": max_posts,
            "samples": samples,
            "bulk": bulk_result,
            "missing_cate2": sorted(missing_cate2),
        }

    zero_update_diagnostics = list(bulk_result.get("zero_update_diagnostics") or [])
    source_page_link_unavailable = any(
        isinstance(item, dict)
        and int(item.get("source_only") or 0) > 0
        and int(item.get("file_type") or 0) == 0
        for item in zero_update_diagnostics
    )
    fallback_fetch_enabled = str(os.getenv("FILE_CATEGORY_UPDATE_ONLY_FETCH_FALLBACK", "0") or "0").strip().lower() in {
        "1",
        "true",
        "y",
        "yes",
        "on",
    }
    auto_fallback_enabled = str(os.getenv("FILE_CATEGORY_UPDATE_ONLY_AUTO_FETCH_FALLBACK", "1") or "1").strip().lower() in {
        "1",
        "true",
        "y",
        "yes",
        "on",
    }
    if (
        updated == 0
        and processed > 0
        and source_page_link_unavailable
        and auto_fallback_enabled
    ):
        pg_subjects_by_cate2, pg_meta = await _load_pg_subjects_by_metadata_url(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            assignments=assignments,
        )
        pg_updated = 0
        pg_results: List[Dict[str, Any]] = []
        for board_cate2, subject_set in pg_subjects_by_cate2.items():
            subject_names = sorted(subject_set)
            if not subject_names:
                continue
            result = await apply_file_category_by_subject_names(
                chat_bot_id=chat_bot_id,
                db_name=db_name,
                subject_names=subject_names,
                board_cate2=board_cate2,
                access_url=str((data or {}).get("access_url") or "").strip() or None,
                request_cookies=dict((data or {}).get("_category_sync_request_cookies") or {}),
                blank_only=blank_only,
            )
            pg_updated += int(result.get("updated") or 0)
            if len(pg_results) < 10:
                pg_results.append(
                    {
                        "board_cate2": board_cate2,
                        "subject_count": len(subject_names),
                        "updated": int(result.get("updated") or 0),
                        "reason": result.get("reason", ""),
                    }
                )
        if pg_updated > 0:
            updated += pg_updated
            logger.info(
                "[FileCategoryUpdateOnly] pg metadata fallback updated | job_id=%s db=%s chat_bot_id=%s updated=%s meta=%s results=%s",
                job_id,
                db_name,
                chat_bot_id,
                pg_updated,
                pg_meta,
                pg_results,
            )
            return {
                "ok": True,
                "updated": updated,
                "reason": "post_db_bulk_source_page_update_with_pg_metadata_fallback",
                "post_count": total_posts,
                "processed": processed,
                "skipped_no_cate": skipped_no_cate,
                "resolved_in_update_only": resolved_in_update_only,
                "max_posts": max_posts,
                "samples": samples,
                "bulk": bulk_result,
                "pg_metadata": pg_meta,
                "pg_results": pg_results,
            }
        logger.warning(
            "[FileCategoryUpdateOnly] pg metadata fallback yielded no updates | job_id=%s db=%s chat_bot_id=%s meta=%s results=%s",
            job_id,
            db_name,
            chat_bot_id,
            pg_meta,
            pg_results,
        )
        fallback_fetch_enabled = True
        logger.warning(
            "[FileCategoryUpdateOnly] auto fetch fallback enabled | job_id=%s db=%s chat_bot_id=%s reason=source_page_link_unavailable processed=%s diag_sample=%s",
            job_id,
            db_name,
            chat_bot_id,
            processed,
            zero_update_diagnostics[:2],
        )
    if not fallback_fetch_enabled:
        return {
            "ok": True,
            "updated": updated,
            "reason": "post_db_bulk_source_page_update",
            "post_count": total_posts,
            "processed": processed,
            "skipped_no_cate": skipped_no_cate,
            "resolved_in_update_only": resolved_in_update_only,
            "max_posts": max_posts,
            "samples": samples,
            "bulk": bulk_result,
        }

    fallback_processed = 0
    fallback_skipped_missing_mapping = 0
    for item in assignments:
        detail_url = item["source_page"]
        board_cate2 = item["board_cate2"]
        if board_cate2 in missing_cate2:
            fallback_skipped_missing_mapping += 1
            continue
        result = await _update_one_detail(
            data=data,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            job_id=job_id,
            detail_url=detail_url,
            board_cate2=board_cate2,
        )
        fallback_processed += 1
        updated += int(result.get("updated") or 0)
        if len(samples) < 10:
            samples.append(
                {
                    "detail_url": detail_url[:180],
                    "board_cate2": board_cate2,
                    "attachments": result.get("attachment_count"),
                    "updated": result.get("updated"),
                    "reason": result.get("reason", ""),
                }
            )
    if fallback_skipped_missing_mapping:
        logger.warning(
            "[FileCategoryUpdateOnly] fetch fallback skipped missing mappings | job_id=%s skipped=%s processed=%s missing_cate2=%s",
            job_id,
            fallback_skipped_missing_mapping,
            fallback_processed,
            sorted(missing_cate2)[:20],
        )
    return {
        "ok": True,
        "updated": updated,
        "reason": "post_db_bulk_source_page_update_with_fetch_fallback",
        "post_count": total_posts,
        "processed": processed,
        "skipped_no_cate": skipped_no_cate,
        "resolved_in_update_only": resolved_in_update_only,
        "max_posts": max_posts,
        "samples": samples,
        "bulk": bulk_result,
        "fallback_processed": fallback_processed,
        "fallback_skipped_missing_mapping": fallback_skipped_missing_mapping,
    }


async def run_file_category_update_only(data: Dict[str, Any]) -> Dict[str, Any]:
    db_name = resolve_db_name(data, default="dev_user")
    job_id = str((data or {}).get("job_id") or "").strip()
    chat_bot_id = str((data or {}).get("chat_bot_id") or ((data or {}).get("metadata") or {}).get("chat_bot_id") or "").strip()
    detail_url = _first_request_url(data)
    logger.info(
        "[FileCategoryUpdateOnly] start | job_id=%s db=%s chat_bot_id=%s detail_url=%s cate1=%s cate2=%s keys=%s",
        job_id,
        db_name,
        chat_bot_id,
        detail_url[:220],
        (data or {}).get("cate1"),
        (data or {}).get("cate2"),
        sorted(str(k) for k in (data or {}).keys())[:50],
    )
    if not (chat_bot_id and detail_url):
        logger.warning(
            "[FileCategoryUpdateOnly] skip | job_id=%s reason=missing_chat_bot_id_or_detail_url chat_bot_id=%s detail_url=%s",
            job_id,
            chat_bot_id,
            detail_url,
        )
        return {"ok": True, "updated": 0, "reason": "missing_chat_bot_id_or_detail_url"}

    if _looks_like_root_or_index_url(detail_url) and not str((data or {}).get("cate2") or "").strip():
        logger.info(
            "[FileCategoryUpdateOnly] root/index request -> post DB fallback | job_id=%s detail_url=%s",
            job_id,
            detail_url[:220],
        )
        result = await _run_update_from_post_db(
            data=data,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            job_id=job_id,
            contents_url=detail_url,
        )
        result.update({"detail_url": detail_url, "resolved": None})
        logger.info(
            "[FileCategoryUpdateOnly] completed | job_id=%s db=%s chat_bot_id=%s mode=post_db_root posts=%s processed=%s resolved_in_update_only=%s updated=%s reason=%s",
            job_id,
            db_name,
            chat_bot_id,
            result.get("post_count"),
            result.get("processed"),
            result.get("resolved_in_update_only"),
            result.get("updated"),
            result.get("reason", ""),
        )
        return result

    board_cate2 = ""
    resolved = None
    try:
        category_obj = await _load_category_url_pattern_object(
            chat_bot_id,
            db_name,
            contents_url=detail_url,
            require_nonempty_rules=True,
        )
        resolved = resolve_cate_for_detail_url(detail_url, category_obj or {}) if category_obj else None
        if resolved:
            board_cate2 = str(resolved[1] or "").strip()
    except Exception as exc:
        logger.warning(
            "[FileCategoryUpdateOnly] category resolve failed | job_id=%s url=%s err=%s",
            job_id,
            detail_url[:180],
            exc,
        )

    if not board_cate2:
        board_cate2 = str((data or {}).get("cate2") or ((data or {}).get("metadata") or {}).get("cate2") or "").strip()
        logger.info(
            "[FileCategoryUpdateOnly] category fallback | job_id=%s payload_cate2=%s resolved=%s",
            job_id,
            board_cate2,
            resolved,
        )
    if not board_cate2:
        logger.warning(
            "[FileCategoryUpdateOnly] detail cate2 not found; fallback to post DB | job_id=%s detail_url=%s resolved=%s",
            job_id,
            detail_url[:220],
            resolved,
        )
        result = await _run_update_from_post_db(
            data=data,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            job_id=job_id,
            contents_url=detail_url,
        )
        result.update({"detail_url": detail_url, "resolved": resolved})
        logger.info(
            "[FileCategoryUpdateOnly] completed | job_id=%s db=%s chat_bot_id=%s mode=post_db posts=%s processed=%s resolved_in_update_only=%s updated=%s reason=%s",
            job_id,
            db_name,
            chat_bot_id,
            result.get("post_count"),
            result.get("processed"),
            result.get("resolved_in_update_only"),
            result.get("updated"),
            result.get("reason", ""),
        )
        return result

    result = await _update_one_detail(
        data=data,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        job_id=job_id,
        detail_url=detail_url,
        board_cate2=board_cate2,
    )
    logger.info(
        "[FileCategoryUpdateOnly] completed | job_id=%s db=%s chat_bot_id=%s board_cate2=%s attachments=%s updated=%s reason=%s",
        job_id,
        db_name,
        chat_bot_id,
        board_cate2,
        result.get("attachment_count"),
        result.get("updated"),
        result.get("reason", ""),
    )
    return result


