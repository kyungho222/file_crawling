from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Set
from urllib.parse import parse_qsl, urlparse

from backend.shared.redis_sse_service import send_message_to_redis_sse, update_state_only
from db.crawl_db_manager import update_crawling_log_counters
from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot
from db.mysql_db_config import mysql_execute_query
from utils.db_name import resolve_db_name
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.shared.summary_only_mode")

SUMMARY_MODE_DEFAULT = "off"
SUMMARY_MODE_ALIASES = {
    "": SUMMARY_MODE_DEFAULT,
    "0": "off",
    "false": "off",
    "no": "off",
    "none": "off",
    "disable": "off",
    "disabled": "off",
    "off": "off",
    "1": "on",
    "true": "on",
    "yes": "on",
    "enable": "on",
    "enabled": "on",
    "on": "on",
    "summary": "on",
    "summarize": "on",
    "only": "only",
    "summary_only": "only",
    "summarize_only": "only",
}

DUPLICATE_SUMMARY_MODE_DEFAULT = "off"
DUPLICATE_SUMMARY_MODE_ALIASES = {
    "": DUPLICATE_SUMMARY_MODE_DEFAULT,
    "0": "off",
    "false": "off",
    "no": "off",
    "none": "off",
    "disable": "off",
    "disabled": "off",
    "off": "off",
    "1": "on",
    "true": "on",
    "yes": "on",
    "enable": "on",
    "enabled": "on",
    "on": "on",
    "summary": "summary",
    "summarize": "summary",
    "only": "summary",
    "summary_only": "summary",
    "summarize_only": "summary",
}


def normalize_summary_request_mode(raw_value: Optional[Any], default: str = SUMMARY_MODE_DEFAULT) -> str:
    value = str(raw_value if raw_value is not None else default or "").strip().lower()
    if value in SUMMARY_MODE_ALIASES:
        return SUMMARY_MODE_ALIASES[value]
    return SUMMARY_MODE_ALIASES.get(str(default or "").strip().lower(), SUMMARY_MODE_DEFAULT)


def normalize_duplicate_summary_request_mode(
    raw_value: Optional[Any],
    default: str = DUPLICATE_SUMMARY_MODE_DEFAULT,
) -> str:
    value = str(raw_value if raw_value is not None else default or "").strip().lower()
    if value in DUPLICATE_SUMMARY_MODE_ALIASES:
        return DUPLICATE_SUMMARY_MODE_ALIASES[value]
    return DUPLICATE_SUMMARY_MODE_ALIASES.get(
        str(default or "").strip().lower(),
        DUPLICATE_SUMMARY_MODE_DEFAULT,
    )


def is_summary_only_request(data: Dict[str, Any]) -> bool:
    mode = str((data or {}).get("crawl_mode") or "").strip().lower()
    if mode in {"summary_only", "summarize_only"}:
        return True
    fields = (data or {}).get("partial_update_fields")
    if isinstance(fields, list) and any(str(item or "").strip().lower() in {"symmary", "summary"} for item in fields):
        return True
    duplicate_summary_mode = normalize_duplicate_summary_request_mode(
        (data or {}).get("duplicate_summary_mode")
        or (data or {}).get("duplicateSummaryMode")
        or (data or {}).get("board_duplicate_summary")
        or (data or {}).get("duplicate_summary")
    )
    if duplicate_summary_mode == "summary":
        return True
    summary_mode = normalize_summary_request_mode(
        (data or {}).get("summary_mode")
        or (data or {}).get("summaryMode")
        or (data or {}).get("board_summary")
        or (data or {}).get("summary_only")
    )
    return summary_mode in {"on", "only"} or bool((data or {}).get("summary_only_enabled") is True)


def _bool_from_payload(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _compact(value: Any, *, limit: int = 220) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[:limit] + "..."


def _field_save_counts(summary_count: int = 0) -> Dict[str, int]:
    count = max(0, _safe_int(summary_count, 0))
    return {
        "title": 0,
        "content": 0,
        "cate": 0,
        "symmary": count,
        "type": 0,
        "url": 0,
        "web_de": 0,
    }


def _list_from_filter(value: Any) -> List[str]:
    if isinstance(value, list):
        raw_items = value
    elif value in (None, ""):
        raw_items = []
    else:
        raw_items = str(value).split(",")
    out: List[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _partial_target_terms(data: Dict[str, Any]) -> tuple[List[str], str]:
    target = (data or {}).get("partial_target_filter")
    if not isinstance(target, dict):
        return [], "any"
    url_terms = _list_from_filter(target.get("url_contains"))
    query_terms = _list_from_filter(target.get("query_contains"))
    terms: List[str] = []
    for item in [*url_terms, *query_terms]:
        lowered = str(item or "").strip().lower()
        if lowered and lowered not in terms:
            terms.append(lowered)
    match_mode = str(target.get("match_mode") or "any").strip().lower()
    if match_mode not in {"any", "all"}:
        match_mode = "any"
    return terms, match_mode


def _partial_target_sql_condition(content_col: str, data: Dict[str, Any]) -> tuple[str, List[Any]]:
    terms, match_mode = _partial_target_terms(data)
    if not terms:
        return "", []
    clauses = [f"LOWER(CAST(`{content_col}` AS CHAR)) LIKE %s" for _ in terms]
    joiner = " AND " if match_mode == "all" else " OR "
    return "(" + joiner.join(clauses) + ")", [f"%{term}%" for term in terms]


async def _get_table_columns_lower(db_name: str, table_name: str) -> Set[str]:
    rows = await mysql_execute_query(
        """
        SELECT LOWER(column_name) AS column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND LOWER(table_name) = LOWER(%s)
        """,
        (db_name, table_name),
        fetch=True,
        dbname=db_name,
    )
    columns: Set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        name = str(row.get("column_name") or "").strip().lower()
        if name:
            columns.add(name)
    return columns


def _learn_content_column(columns: Set[str]) -> str:
    return "content" if "content" in columns else ""


def _normalize_target_domains(value: Any) -> List[str]:
    if isinstance(value, list):
        raw_items = value
    elif value in (None, ""):
        raw_items = []
    else:
        raw_items = str(value).split(",")
    out: List[str] = []
    for item in raw_items:
        text = str(item or "").strip().lower()
        if text and text not in out:
            out.append(text)
    return out


def _first_payload_content(data: Dict[str, Any]) -> str:
    for payload_key in ("contents", "base_url"):
        contents = (data or {}).get(payload_key)
        if isinstance(contents, list):
            for item in contents:
                text = str(item or "").strip()
                if text:
                    return text
        text = str(contents or "").strip()
        if text:
            return text
    for key in ("content", "url"):
        text = str((data or {}).get(key) or "").strip()
        if text:
            return text
    return ""


def _base_urls_override(data: Dict[str, Any]) -> List[str]:
    raw = (data or {}).get("base_url")
    if not isinstance(raw, list):
        return []
    urls: List[str] = []
    seen: Set[str] = set()
    for item in raw:
        text = str(item or "").strip()
        if not text:
            continue
        normalized = ensure_url_scheme(text) if "://" in text or text.startswith("www.") else text
        if normalized and normalized not in seen:
            seen.add(normalized)
            urls.append(normalized)
    return urls


def _content_tokens(value: Any) -> Set[str]:
    text = str(value or "").strip()
    if not text:
        return set()
    tokens = {text.lower()}
    try:
        parsed = urlparse(ensure_url_scheme(text))
        if parsed.path:
            path = parsed.path.strip("/")
            tokens.add(parsed.path.lower())
            tokens.add(path.lower())
            for part in path.split("/"):
                if part:
                    tokens.add(part.lower())
        for key, val in parse_qsl(parsed.query or "", keep_blank_values=True):
            if key:
                tokens.add(str(key).lower())
            if val:
                tokens.add(str(val).lower())
            if key or val:
                tokens.add(f"{key}={val}".lower())
    except Exception:
        pass
    return {token for token in tokens if token}


def _partial_filter_matches_content(content_value: str, data: Dict[str, Any]) -> bool:
    terms, match_mode = _partial_target_terms(data)
    if not terms:
        return True
    haystack = _content_tokens(content_value)
    def _matched(term: str) -> bool:
        needle = str(term or "").strip().lower()
        return bool(needle) and any(needle in token for token in haystack)
    if match_mode == "all":
        return all(_matched(term) for term in terms)
    return any(_matched(term) for term in terms)


def _extract_start_url(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("url", "content", "contents_url", "href", "source_url"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
        return ""
    return str(value or "").strip()


def _start_urls_override(data: Dict[str, Any]) -> List[str]:
    raw = (data or {}).get("start_urls_override")
    if not isinstance(raw, list):
        return []
    urls: List[str] = []
    seen: Set[str] = set()
    for item in raw:
        text = _extract_start_url(item)
        if not text:
            continue
        normalized = ensure_url_scheme(text) if "://" in text or text.startswith("www.") else text
        key = normalized.strip()
        if key and key not in seen:
            seen.add(key)
            urls.append(key)
    return urls


def _url_in_scope(url: str, *, target_domains: List[str], path_prefix: str) -> bool:
    if not target_domains and not path_prefix:
        return True
    try:
        from urllib.parse import urlparse

        parsed = urlparse(ensure_url_scheme(url))
        host = (parsed.netloc or "").split("@")[-1].split(":")[0].lower()
        path = parsed.path or ""
    except Exception:
        return False
    if target_domains and not any(host == d or host.endswith("." + d) for d in target_domains):
        return False
    if path_prefix:
        prefix = "/" + str(path_prefix or "").strip().strip("/")
        if prefix != "/" and not path.startswith(prefix):
            return False
    return True


async def _load_summary_target_rows(
    *,
    db_name: str,
    learn_table: str,
    columns: Set[str],
    data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    content_col = _learn_content_column(columns)
    if not content_col or "id" not in columns:
        return []

    only_empty = _bool_from_payload(data.get("summary_only_empty"), default=True)
    metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    requested_id = _safe_int(
        data.get("learn_list_id")
        or data.get("learnListId")
        or data.get("learn_list")
        or metadata.get("learn_list_id"),
        0,
    )
    payload_content = _first_payload_content(data)
    desired_status = str(
        data.get("summary_learn_list_status")
        or os.getenv("SUMMARY_ONLY_LEARN_LIST_STATUS", "Y")
        or ""
    ).strip()
    max_rows = max(1, min(_safe_int(data.get("summary_limit"), _safe_int(os.getenv("SUMMARY_ONLY_MAX_ROWS"), 50000)), 200000))
    page_size = max(50, min(_safe_int(os.getenv("SUMMARY_ONLY_PAGE_SIZE"), 300), 2000))

    select_cols = ["id", f"`{content_col}` AS content_value"]
    for col in ("subject", "web_title", "content_type", "memo1", "keyword1"):
        if col in columns:
            select_cols.append(f"`{col}`")

    override_urls = _start_urls_override(data)
    if override_urls:
        override_urls = [url for url in override_urls if _partial_filter_matches_content(url, data)]
        rows_by_content: Dict[str, Dict[str, Any]] = {}
        chunk_size = 300
        for start in range(0, len(override_urls), chunk_size):
            chunk = override_urls[start:start + chunk_size]
            placeholders = ", ".join(["%s"] * len(chunk))
            use_type_content_index = content_col == "content" and "content_type" in columns
            where_sql = (
                f"`content_type` = %s AND `{content_col}` IN ({placeholders})"
                if use_type_content_index
                else f"`{content_col}` IN ({placeholders})"
            )
            query_params = ("url", *chunk) if use_type_content_index else tuple(chunk)
            rows = await mysql_execute_query(
                (
                    f"SELECT {', '.join(select_cols)} FROM `{learn_table}` "
                    f"WHERE {where_sql} "
                    f"ORDER BY `id` ASC"
                ),
                query_params,
                fetch=True,
                dbname=db_name,
            )
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                content_value = str(row.get("content_value") or "").strip()
                if content_value and content_value not in rows_by_content:
                    rows_by_content[content_value] = row
        rows_out: List[Dict[str, Any]] = []
        matched = 0
        for url in override_urls[:max_rows]:
            row = rows_by_content.get(url)
            if row:
                rows_out.append(dict(row))
                matched += 1
            else:
                rows_out.append(
                    {
                        "id": 0,
                        "content_value": url,
                        "content_type": "url",
                        "_summary_skip_reason": "learn_list_missing",
                    }
                )
        logger.debug(
            "[SummaryOnly] targets loaded from start_urls_override | db=%s table=%s rows=%s matched=%s missing=%s override_count=%s content_col=%s source=%s sample=%s",
            db_name,
            learn_table,
            len(rows_out),
            matched,
            max(0, len(rows_out) - matched),
            len(override_urls),
            content_col,
            data.get("start_urls_override_source"),
            override_urls[:3],
        )
        return rows_out

    conditions: List[str] = []
    params: List[Any] = []
    if requested_id > 0:
        conditions.append("`id` = %s")
        params.append(requested_id)
    else:
        conditions.extend([f"`{content_col}` IS NOT NULL", f"TRIM(CAST(`{content_col}` AS CHAR)) <> ''"])
    if requested_id <= 0 and desired_status and "status" in columns:
        conditions.append("UPPER(COALESCE(`status`, '')) = %s")
        params.append(desired_status.upper())
    if requested_id <= 0 and "content_type" in columns:
        conditions.append("LOWER(COALESCE(`content_type`, '')) NOT IN ('', 'folder')")
    if requested_id <= 0 and only_empty:
        empty_clauses = []
        if "memo1" in columns:
            empty_clauses.append("(`memo1` IS NULL OR TRIM(`memo1`) = '')")
        if "keyword1" in columns:
            empty_clauses.append("(`keyword1` IS NULL OR TRIM(`keyword1`) = '')")
        if empty_clauses:
            conditions.append("(" + " OR ".join(empty_clauses) + ")")
    if requested_id <= 0:
        target_sql, target_params = _partial_target_sql_condition(content_col, data)
        if target_sql:
            conditions.append(target_sql)
            params.extend(target_params)

    base_where = " AND ".join(conditions)
    target_domains = _normalize_target_domains(data.get("target_domains"))
    path_prefix = str(data.get("scope_path_prefix") or "").strip()

    async def _query_rows(
        *,
        where_sql: str,
        where_params: List[Any],
        scope_domains: List[str],
        scope_path_prefix: str,
    ) -> List[Dict[str, Any]]:
        rows_out: List[Dict[str, Any]] = []
        last_id = 0
        while len(rows_out) < max_rows:
            limit = min(page_size, max_rows - len(rows_out))
            rows = await mysql_execute_query(
                (
                    f"SELECT {', '.join(select_cols)} FROM `{learn_table}` "
                    f"WHERE {where_sql} AND `id` > %s "
                    f"ORDER BY `id` ASC LIMIT {limit}"
                ),
                tuple(where_params + [last_id]),
                fetch=True,
                dbname=db_name,
            )
            if not rows:
                break
            for row in rows:
                if not isinstance(row, dict):
                    continue
                row_id = _safe_int(row.get("id"), 0)
                if row_id > last_id:
                    last_id = row_id
                content_value = str(row.get("content_value") or "").strip() or payload_content
                if not content_value:
                    continue
                if not str(row.get("content_value") or "").strip():
                    row["content_value"] = content_value
                if not _url_in_scope(
                    content_value,
                    target_domains=scope_domains,
                    path_prefix=scope_path_prefix,
                ):
                    continue
                rows_out.append(row)
                if len(rows_out) >= max_rows:
                    break
            if len(rows) < limit:
                break
        return rows_out

    rows_out = await _query_rows(
        where_sql=base_where,
        where_params=params,
        scope_domains=target_domains,
        scope_path_prefix=path_prefix,
    )
    logger.debug(
        "[SummaryOnly] targets loaded | db=%s table=%s requested_id=%s rows=%s content_col=%s payload_content=%s scope_domains=%s path_prefix=%s partial_filter=%s where=%s",
        db_name,
        learn_table,
        requested_id,
        len(rows_out),
        content_col,
        bool(payload_content),
        target_domains,
        path_prefix,
        data.get("partial_target_filter") if isinstance(data.get("partial_target_filter"), dict) else {},
        base_where,
    )
    return rows_out


async def _summarize_and_apply_row(
    *,
    db_name: str,
    chat_bot_id: str,
    learn_table: str,
    row: Dict[str, Any],
) -> Dict[str, Any]:
    from backend.shared.summarize_keywords_client import (
        post_summarize_keywords,
        summarize_keywords_endpoint,
        summarize_keywords_payload_concurrency,
        summarize_keywords_timeout_sec,
    )
    from utils.learn_list_keyword import (
        _has_body,
        _normalize_text,
        _resolve_training_table,
        fetch_chunks_text,
    )

    row_id = _safe_int(row.get("id"), 0)
    content_value = str(row.get("content_value") or "").strip()
    content_type = str(row.get("content_type") or "url").strip().lower() or "url"
    if row_id <= 0:
        return {
            "ok": False,
            "skipped": True,
            "id": row_id,
            "reason": str(row.get("_summary_skip_reason") or "learn_list_missing"),
            "content": _compact(content_value),
        }
    subject = str(row.get("subject") or row.get("web_title") or "").strip()
    normalized_text = ""
    training_table = await _resolve_training_table(str(chat_bot_id), str(db_name))
    if training_table:
        try:
            raw_text = await fetch_chunks_text(
                training_table,
                str(db_name),
                content_type,
                subject,
                content_value,
            )
            normalized_text = _normalize_text(raw_text)
        except Exception as exc:
            logger.warning(
                "[SummaryOnly] training text lookup failed | db=%s chat_bot_id=%s row_id=%s table=%s content_type=%s content=%s err=%s",
                db_name,
                chat_bot_id,
                row_id,
                training_table,
                content_type,
                _compact(content_value),
                exc,
            )
    logger.debug(
        "[SummaryOnly] source resolved | db=%s chat_bot_id=%s row_id=%s content_type=%s training_table=%s normalized_text_len=%s has_body=%s content=%s",
        db_name,
        chat_bot_id,
        row_id,
        content_type,
        training_table or "-",
        len(normalized_text),
        _has_body(normalized_text),
        _compact(content_value),
    )
    payload = {
        "chat_bot_id": str(chat_bot_id),
        "db_name": str(db_name),
        "learn_table": str(learn_table),
        "target_table": str(learn_table),
        "target_db": str(db_name),
        "target": "learn_list",
        "contents": [content_value],
        "content_type": content_type,
        "concurrency": summarize_keywords_payload_concurrency(),
        "learn_list_id": row_id,
        "summary_only": True,
    }
    if subject:
        payload["subject"] = subject
    if _has_body(normalized_text):
        payload["normalized_text"] = normalized_text
        payload["normalized_contents"] = [normalized_text]
        payload["source_url"] = content_value
    status, body = await post_summarize_keywords(
        summarize_keywords_endpoint(),
        payload,
        timeout_sec=summarize_keywords_timeout_sec(),
    )
    if status != 200:
        return {"ok": False, "id": row_id, "status": status, "detail": _compact(body)}
    try:
        parsed = json.loads(body or "{}")
    except Exception as exc:
        return {"ok": False, "id": row_id, "status": status, "detail": f"json_parse_failed: {exc}"}
    results = parsed.get("results") if isinstance(parsed, dict) else None
    res_info = results[0] if isinstance(results, list) and results and isinstance(results[0], dict) else None
    try:
        api_updated = int(parsed.get("updated") or 0) if isinstance(parsed, dict) else 0
    except Exception:
        api_updated = 0
    if not res_info:
        if api_updated > 0:
            return {
                "ok": True,
                "id": row_id,
                "api_updated": api_updated,
                "content": _compact(content_value),
            }
        logger.warning(
            "[SummaryOnly] summarize returned no row result | db=%s chat_bot_id=%s row_id=%s content_type=%s normalized_text_len=%s has_body=%s total=%s updated=%s skipped=%s errors=%s message=%s content=%s",
            db_name,
            chat_bot_id,
            row_id,
            content_type,
            len(normalized_text),
            _has_body(normalized_text),
            parsed.get("total") if isinstance(parsed, dict) else None,
            parsed.get("updated") if isinstance(parsed, dict) else None,
            parsed.get("skipped") if isinstance(parsed, dict) else None,
            parsed.get("errors") if isinstance(parsed, dict) else None,
            parsed.get("message") if isinstance(parsed, dict) else None,
            _compact(content_value),
        )
        return {
            "ok": False,
            "id": row_id,
            "status": status,
            "detail": "empty_results",
            "api_total": parsed.get("total") if isinstance(parsed, dict) else None,
            "api_updated": parsed.get("updated") if isinstance(parsed, dict) else None,
            "api_skipped": parsed.get("skipped") if isinstance(parsed, dict) else None,
            "api_errors": parsed.get("errors") if isinstance(parsed, dict) else None,
            "api_message": parsed.get("message") if isinstance(parsed, dict) else None,
        }
    return {
        "ok": True,
        "id": row_id,
        "api_updated": api_updated,
        "summary_len": len(str(res_info.get("summary") or "")),
        "keywords_count": len(res_info.get("keywords") or []) if isinstance(res_info.get("keywords"), list) else 0,
        "content": _compact(content_value),
    }


async def run_summary_only(data: Dict[str, Any]) -> Dict[str, Any]:
    db_name = resolve_db_name(data, default="dev_user") or "dev_user"
    job_id = str((data or {}).get("job_id") or "").strip()
    chat_bot_id = str((data or {}).get("chat_bot_id") or "").strip()
    suppress_terminal_sse = bool((data or {}).get("_suppress_terminal_sse"))
    if not chat_bot_id:
        meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
        chat_bot_id = str(meta.get("chat_bot_id") or "").strip()
    if not chat_bot_id:
        raise RuntimeError("summary_only requires chat_bot_id")

    started_payload = {
        "status": "running",
        "event": "summary_only_started",
        "job_id": job_id,
        "account_name": db_name,
        "total_count": 0,
        "collection_count": 0,
        "updated_count": 0,
        "save_count": 0,
        "study_count": 0,
        "field_save_counts": _field_save_counts(0),
        "h3": "요약 적용",
        "message": "요약 API 적용 중",
        "source": "summary_only",
    }
    if job_id:
        await update_state_only(job_id=job_id, account_name=db_name, payload=started_payload)
        await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=started_payload)

    learn_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    columns = await _get_table_columns_lower(db_name, learn_table) if learn_table else set()
    rows = await _load_summary_target_rows(
        db_name=db_name,
        learn_table=str(learn_table or ""),
        columns=columns,
        data=data,
    )
    total = len(rows)
    updated = 0
    errors = 0
    skipped = 0
    samples: List[Dict[str, Any]] = []
    error_samples: List[Dict[str, Any]] = []
    skip_samples: List[Dict[str, Any]] = []

    if job_id:
        await send_message_to_redis_sse(
            job_id=job_id,
            dbname=db_name,
            message={
                **started_payload,
                "total_count": total,
                "scan_count": total,
                "updated_count": 0,
                "field_save_counts": _field_save_counts(0),
                "message": f"요약 대상 {total}건 확인",
            },
        )

    for index, row in enumerate(rows, start=1):
        try:
            result = await _summarize_and_apply_row(
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                learn_table=str(learn_table),
                row=row,
            )
            if result.get("ok"):
                updated += 1
                if len(samples) < 10:
                    samples.append(result)
            elif result.get("skipped"):
                skipped += 1
                if len(skip_samples) < 10:
                    skip_samples.append(result)
            else:
                errors += 1
                if len(error_samples) < 10:
                    error_samples.append(result)
        except Exception as exc:
            errors += 1
            logger.exception("[SummaryOnly] row failed | job_id=%s row_id=%s err=%s", job_id, row.get("id"), exc)
            if len(error_samples) < 10:
                error_samples.append({"id": row.get("id"), "detail": str(exc)})

        if job_id:
            await send_message_to_redis_sse(
                job_id=job_id,
                dbname=db_name,
                message={
                    "status": "running",
                    "event": "summary_only_progress",
                    "job_id": job_id,
                    "total_count": total,
                    "scan_count": total,
                    "collection_count": updated,
                    "updated_count": updated,
                    "save_count": 0,
                    "study_count": 0,
                    "field_save_counts": _field_save_counts(updated),
                    "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
                    "partial_sequence_running": data.get("_partial_sequence_running"),
                    "message": f"요약 적용 중 {index}/{total}",
                    "source": "summary_only",
                },
            )

    result_payload = {
        "status": "completed",
        "event": "workflow_completed",
        "job_id": job_id,
        "account_name": db_name,
        "total_count": total,
        "scan_count": total,
        "collection_count": updated,
        "save_count": 0,
        "study_count": 0,
        "updated_count": updated,
        "field_save_counts": _field_save_counts(updated),
        "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
        "partial_sequence_running": data.get("_partial_sequence_running"),
        "skipped_count": skipped,
        "error_count": errors,
        "learn_list_table": learn_table,
        "summary_samples": samples,
        "summary_skip_samples": skip_samples,
        "summary_error_samples": error_samples,
        "message": f"요약 적용 완료: {updated}건 반영, 스킵 {skipped}건, 오류 {errors}건",
        "source": "summary_only",
    }
    if job_id:
        await update_crawling_log_counters(
            job_id=job_id,
            status="completed",
            scan=total,
            collection=updated,
            saved=0,
            study=0,
            dbname=db_name,
        )
        if not suppress_terminal_sse:
            await update_state_only(job_id=job_id, account_name=db_name, payload=result_payload)
            await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=result_payload)
    logger.debug(
        "[SummaryOnly] completed | job_id=%s db=%s chat_bot_id=%s table=%s total=%s updated=%s skipped=%s errors=%s",
        job_id,
        db_name,
        chat_bot_id,
        learn_table,
        total,
        updated,
        skipped,
        errors,
    )
    return result_payload

__all__ = [
    "normalize_duplicate_summary_request_mode",
    "normalize_summary_request_mode",
    "is_summary_only_request",
    "run_summary_only",
]
