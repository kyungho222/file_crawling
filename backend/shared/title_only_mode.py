from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse, urlunparse

from backend.shared.redis_sse_service import send_message_to_redis_sse, update_state_only
from backend.shared.sub_change_mode import is_partial_title_change_request
from db.crawl_db_manager import update_crawling_log_counters
from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot
from db.mysql_db_config import mysql_execute_query
from utils.db_name import resolve_db_name
from utils.url import canonicalize_url_for_dedup, ensure_url_scheme, urls_match_for_dedup

logger = logging.getLogger("backend.shared.title_only_mode")

TITLE_MODE_DEFAULT = "off"
TITLE_MODE_ALIASES = {
    "": TITLE_MODE_DEFAULT,
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
    "title": "title",
    "subject": "title",
    "web_title": "title",
    "only": "title",
    "title_only": "title",
}


def _field_save_counts(title_count: int = 0) -> Dict[str, int]:
    return {
        "title": int(title_count or 0),
        "content": 0,
        "cate": 0,
        "symmary": 0,
        "type": 0,
        "url": 0,
        "web_de": 0,
    }


def normalize_duplicate_title_request_mode(
    raw_value: Optional[Any],
    default: str = TITLE_MODE_DEFAULT,
) -> str:
    value = str(raw_value if raw_value is not None else default or "").strip().lower()
    if value in TITLE_MODE_ALIASES:
        return TITLE_MODE_ALIASES[value]
    return TITLE_MODE_ALIASES.get(str(default or "").strip().lower(), TITLE_MODE_DEFAULT)


def is_title_only_request(data: Dict[str, Any]) -> bool:
    if is_partial_title_change_request(data or {}):
        return True
    mode = str((data or {}).get("crawl_mode") or "").strip().lower()
    if mode in {"title_only", "title_postprocess", "title_repair_only"}:
        return True
    duplicate_title_mode = normalize_duplicate_title_request_mode(
        (data or {}).get("duplicate_title_mode")
        or (data or {}).get("duplicateTitleMode")
        or (data or {}).get("board_duplicate_title")
        or (data or {}).get("duplicate_title")
    )
    if duplicate_title_mode == "title":
        return True
    title_mode = normalize_duplicate_title_request_mode(
        (data or {}).get("title_mode")
        or (data or {}).get("titleMode")
        or (data or {}).get("title_only")
    )
    return title_mode == "title" or bool((data or {}).get("title_only_enabled") is True)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y"}


def _compact(value: Any, *, limit: int = 220) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[:limit] + "..."


def _normalize_compare_title(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).strip().lower()


def _is_weak_title(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    if re.match(r"^https?://", text, flags=re.IGNORECASE):
        return True
    low = text.lower()
    if any(token in low for token in ("error", "err_", "error_title", "error_title_signature")):
        return True
    if text.count(" < ") >= 1:
        return True
    compact = text.replace(" ", "")
    return compact.lower() in {"attachment", "attachments", "content", "contents", "file"}


def _clean_head_title_candidate(value: Any, *, source: str = "") -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        return ""

    # Some public sites expose "site,title" in meta description.
    if "," in text:
        left, right = [part.strip() for part in text.split(",", 1)]
        if right and re.search(r"(구청|시청|군청|공단|공사|센터|청)$", left):
            text = right

    if source in {"title", "og:title", "meta:title", "name"}:
        for sep in (" | ", " ｜ ", " :: ", " > ", " - ", " – ", " — "):
            if sep in text:
                parts = [part.strip() for part in text.split(sep) if part.strip()]
                if parts:
                    text = parts[0]
                    break

    text = re.sub(r"\s+", " ", text).strip(" \t\r\n·|｜-–—:：")
    return text


def _score_head_title_candidate(title: str, *, source: str = "") -> int:
    if _is_weak_title(title):
        return -1
    if len(title) < 3 or len(title) > 180:
        return -1
    compact = title.replace(" ", "")
    if compact in {"강동구청", "공지사항", "상세보기", "목록", "본문"}:
        return -1
    if title.count("|") >= 1 or title.count(" > ") >= 1:
        return -1

    score = 0
    if source in {"og:title", "meta:title", "title", "name"}:
        score += 40
    elif source in {"og:description", "description"}:
        score += 25
        # Description is often a summary; use it only when it looks title-like.
        if len(title) > 120 and not re.match(r"^\[[^\]]{2,20}\]", title):
            return -1
    if re.match(r"^\[[^\]]{2,20}\]", title):
        score += 30
    if re.search(r"(모집|공고|안내|사업|용역|결과|채용|행사|교육|신청|접수)", title):
        score += 5
    score += max(0, 30 - abs(len(title) - 45) // 5)
    return score


def _extract_title_from_head_first(soup: Any) -> str:
    candidates: List[tuple[int, str, str]] = []

    def add(source: str, value: Any) -> None:
        title = _clean_head_title_candidate(value, source=source)
        score = _score_head_title_candidate(title, source=source)
        if score >= 0:
            candidates.append((score, title, source))

    try:
        for source, selector, attr in (
            ("og:description", "meta[property='og:description']", "content"),
            ("description", "meta[name='description']", "content"),
            ("og:title", "meta[property='og:title']", "content"),
            ("meta:title", "meta[name='title']", "content"),
            ("name", "meta[name='name']", "content"),
        ):
            node = soup.select_one(selector)
            if node is not None:
                add(source, node.get(attr))
        if getattr(soup, "title", None) is not None:
            add("title", soup.title.string)
    except Exception:
        return ""

    if not candidates:
        return ""
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


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


def _title_select_column_expr(col: str) -> str:
    if col in {"created_at", "content_created_at", "content_at"}:
        return f"CAST(`{col}` AS CHAR) AS `{col}`"
    return f"`{col}`"


def _extract_start_url(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("url", "content", "contents_url", "href", "source_url"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
        return ""
    return str(value or "").strip()


def _extract_title_hint(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    for key in ("title", "subject", "web_title", "clean_title", "post_title", "text"):
        text = re.sub(r"\s+", " ", str(value.get(key) or "")).strip()
        if text and not _is_weak_title(text):
            return text
    return ""


def _start_url_items(data: Dict[str, Any]) -> List[Dict[str, str]]:
    raw = (data or {}).get("start_urls_override")
    if not isinstance(raw, list):
        return []
    items: List[Dict[str, str]] = []
    seen: Set[str] = set()
    for item in raw:
        text = _extract_start_url(item)
        if not text:
            continue
        normalized = ensure_url_scheme(text) if "://" in text or text.startswith("www.") else text
        key = normalized.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        items.append({"url": key, "title_hint": _extract_title_hint(item)})
    return items


def _auto_exploration_override_source(data: Dict[str, Any]) -> bool:
    source = str((data or {}).get("start_urls_override_source") or "").strip().lower()
    return source in {"pre_explored_db", "file_crawl_post_db", "file_crawl_post_db_stream"}


def _blank_title_conditions(columns: Set[str]) -> List[str]:
    conditions: List[str] = []
    if "subject" in columns:
        conditions.append("(`subject` IS NULL OR TRIM(`subject`) = '')")
    if "web_title" in columns:
        conditions.append("(`web_title` IS NULL OR TRIM(`web_title`) = '')")
    return conditions


def _title_lookup_candidates(url: str) -> List[str]:
    candidates: List[str] = []

    def _push(value: Any) -> None:
        text = str(value or "").strip()
        if text and text not in candidates:
            candidates.append(text)

    _push(url)
    try:
        ensured = ensure_url_scheme(url)
        _push(ensured)
        canonical = canonicalize_url_for_dedup(ensured)
        _push(canonical)
        parsed = urlparse(canonical)
        if parsed.netloc and not parsed.netloc.startswith("www."):
            _push(urlunparse(parsed._replace(netloc=f"www.{parsed.netloc}")))
    except Exception:
        pass
    return candidates


def _find_title_row_for_url(
    url: str,
    rows_by_content: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    for candidate in _title_lookup_candidates(url):
        row = rows_by_content.get(candidate)
        if row:
            return row

    for content_value, row in rows_by_content.items():
        try:
            if urls_match_for_dedup(url, content_value):
                return row
        except Exception:
            continue
    return None


async def _load_title_target_rows(
    *,
    db_name: str,
    learn_table: str,
    columns: Set[str],
    data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    content_col = _learn_content_column(columns)
    if not content_col or "id" not in columns:
        return []

    items = _start_url_items(data)
    if _auto_exploration_override_source(data):
        return await _load_blank_title_target_rows(
            db_name=db_name,
            learn_table=learn_table,
            columns=columns,
            content_col=content_col,
            data=data,
        )
    if not items and is_partial_title_change_request(data):
        return await _load_partial_title_target_rows(
            db_name=db_name,
            learn_table=learn_table,
            columns=columns,
            content_col=content_col,
            data=data,
        )
    max_rows = max(1, min(_safe_int(data.get("title_limit"), _safe_int(os.getenv("TITLE_ONLY_MAX_ROWS"), 50000)), 200000))
    if not items:
        logger.info(
            "[TitleOnly] targets loaded | db=%s table=%s rows=0 reason=start_urls_override_empty",
            db_name,
            learn_table,
        )
        return []

    urls = [item["url"] for item in items[:max_rows]]
    lookup_values: List[str] = []
    seen_lookup_values: Set[str] = set()
    for url in urls:
        for candidate in _title_lookup_candidates(url):
            if candidate and candidate not in seen_lookup_values:
                seen_lookup_values.add(candidate)
                lookup_values.append(candidate)
    select_cols = ["id", f"`{content_col}` AS content_value"]
    for col in ("subject", "web_title", "content_type", "content_at", "content_created_at", "created_at"):
        if col in columns:
            select_cols.append(_title_select_column_expr(col))

    rows_by_content: Dict[str, Dict[str, Any]] = {}
    chunk_size = 300
    for start in range(0, len(lookup_values), chunk_size):
        chunk = lookup_values[start:start + chunk_size]
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
    date_filtered = 0
    start_date, end_date = _parse_partial_target_date(data)
    date_col = _title_target_date_column(columns)
    for item in items[:max_rows]:
        url = item["url"]
        row = _find_title_row_for_url(url, rows_by_content)
        if row:
            if not _row_matches_title_target_date(row, columns=columns, data=data):
                rows_out.append(
                    {
                        "id": 0,
                        "content_value": url,
                        "content_type": "url",
                        "_title_hint": item.get("title_hint") or "",
                        "_source_url": url,
                        "_title_skip_reason": "target_date_out_of_range",
                    }
                )
                date_filtered += 1
                continue
            merged = dict(row)
            merged["_title_hint"] = item.get("title_hint") or ""
            merged["_source_url"] = url
            rows_out.append(merged)
            matched += 1
        else:
            rows_out.append(
                {
                    "id": 0,
                    "content_value": url,
                    "content_type": "url",
                    "_title_hint": item.get("title_hint") or "",
                    "_source_url": url,
                    "_title_skip_reason": "learn_list_missing",
                }
            )

    logger.info(
        "[TitleOnly] targets loaded from start_urls_override | db=%s table=%s rows=%s matched=%s missing=%s date_filtered=%s override_count=%s content_col=%s source=%s sample=%s",
        db_name,
        learn_table,
        len(rows_out),
        matched,
        max(0, len(rows_out) - matched - date_filtered),
        date_filtered,
        len(items),
        content_col,
        data.get("start_urls_override_source"),
        urls[:3],
    )
    logger.info(
        "[TitleOnly][FlowDebug][date_filter] stage=start_urls_override job_id=%s db=%s table=%s applied=%s date_col=%s start=%s end=%s raw_target_date=%s filtered=%s reason=%s",
        str((data or {}).get("job_id") or ""),
        db_name,
        learn_table,
        bool(start_date and end_date and date_col),
        date_col,
        start_date,
        end_date,
        (data or {}).get("target_date") or (data or {}).get("start_urls_target_date"),
        date_filtered,
        "post_filter" if start_date and end_date and date_col else ("date_column_missing" if start_date and end_date else "target_date_empty"),
    )
    return rows_out


async def _load_blank_title_target_rows(
    *,
    db_name: str,
    learn_table: str,
    columns: Set[str],
    content_col: str,
    data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    max_rows = max(1, min(_safe_int(data.get("title_limit"), _safe_int(os.getenv("TITLE_ONLY_MAX_ROWS"), 50000)), 200000))
    select_cols = ["id", f"`{content_col}` AS content_value"]
    for col in ("subject", "web_title", "content_type", "content_at", "content_created_at", "created_at"):
        if col in columns:
            select_cols.append(_title_select_column_expr(col))

    conditions = _blank_title_conditions(columns)
    if not conditions:
        logger.info(
            "[TitleOnly] blank title targets skipped | db=%s table=%s reason=title_columns_missing",
            db_name,
            learn_table,
        )
        return []
    where_parts = ["(" + " OR ".join(conditions) + ")"]
    params: List[Any] = []
    if "content_type" in columns:
        where_parts.append("LOWER(COALESCE(`content_type`, '')) = 'url'")
    if "type" in columns:
        where_parts.append("LOWER(COALESCE(`type`, '')) <> 'file'")
    _append_title_target_date_filter(
        where_parts=where_parts,
        params=params,
        columns=columns,
        data=data,
        stage="blank_title_targets",
        db_name=db_name,
        learn_table=learn_table,
    )

    rows = await mysql_execute_query(
        f"""
        SELECT {', '.join(select_cols)}
        FROM `{learn_table}`
        WHERE {' AND '.join(where_parts)}
        ORDER BY `id` ASC
        LIMIT %s
        """,
        tuple(params + [max_rows]),
        fetch=True,
        dbname=db_name,
    )
    rows_out: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        merged = dict(row)
        merged["_source_url"] = str(row.get("content_value") or "").strip()
        rows_out.append(merged)
    logger.info(
        "[TitleOnly] blank title targets loaded from learn_list | db=%s table=%s rows=%s content_col=%s source=%s",
        db_name,
        learn_table,
        len(rows_out),
        content_col,
        data.get("start_urls_override_source"),
    )
    return rows_out


def _normalize_text_list(value: Any) -> List[str]:
    if isinstance(value, list):
        raw = value
    elif isinstance(value, str):
        raw = [value]
    else:
        raw = []
    return [str(item or "").strip() for item in raw if str(item or "").strip()]


def _target_filter_url_terms(target_filter: Dict[str, Any]) -> List[str]:
    terms: List[str] = []
    for key in ("url_contains", "url", "urls"):
        for item in _normalize_text_list((target_filter or {}).get(key)):
            if item not in terms:
                terms.append(item)
    return terms


def _target_filter_query_terms(target_filter: Dict[str, Any]) -> List[str]:
    terms: List[str] = []
    for key in ("query_contains", "query", "queries", "query_params", "query_param"):
        for item in _normalize_text_list((target_filter or {}).get(key)):
            if item not in terms:
                terms.append(item)
    return terms


def _parse_partial_target_date(data: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    raw = (data or {}).get("target_date") or (data or {}).get("start_urls_target_date")
    if not isinstance(raw, list) or len(raw) < 2:
        return None, None
    start = str(raw[0] or "").strip()[:10]
    end = str(raw[1] or "").strip()[:10]
    if not start and not end:
        return None, None
    return start or end, end or start


def _parse_title_subject_filter(data: Dict[str, Any]) -> tuple[str, str]:
    raw = (data or {}).get("subject_filter")
    if raw in (None, ""):
        raw = (data or {}).get("title_subject_filter")
    if isinstance(raw, dict):
        value = str(raw.get("value") or raw.get("subject") or raw.get("text") or "").strip()
        match_mode = str(raw.get("match_mode") or raw.get("mode") or "exact").strip().lower()
    else:
        value = str(raw or "").strip()
        match_mode = str((data or {}).get("subject_match_mode") or "exact").strip().lower()
    if match_mode not in {"contains", "like"}:
        match_mode = "exact"
    if match_mode == "like":
        match_mode = "contains"
    return value, match_mode


def _append_title_subject_filter(
    *,
    where_parts: List[str],
    params: List[Any],
    columns: Set[str],
    data: Dict[str, Any],
    stage: str,
    db_name: str,
    learn_table: str,
) -> tuple[str, str, bool]:
    subject_filter, match_mode = _parse_title_subject_filter(data)
    applied = False
    reason = "subject_filter_empty"
    if subject_filter:
        if "subject" in columns:
            if match_mode == "contains":
                where_parts.append("CAST(COALESCE(`subject`, '') AS CHAR) LIKE %s")
                params.append(f"%{subject_filter}%")
            else:
                where_parts.append("TRIM(CAST(COALESCE(`subject`, '') AS CHAR)) = %s")
                params.append(subject_filter)
            applied = True
            reason = "applied"
        else:
            reason = "subject_column_missing"
    logger.info(
        "[TitleOnly][FlowDebug][subject_filter] stage=%s job_id=%s db=%s table=%s applied=%s mode=%s value=%r reason=%s",
        stage,
        str((data or {}).get("job_id") or ""),
        db_name,
        learn_table,
        applied,
        match_mode,
        subject_filter[:160],
        reason,
    )
    return subject_filter, match_mode, applied


def _title_target_date_column(columns: Set[str]) -> str:
    for col in ("created_at", "content_created_at", "content_at"):
        if col in columns:
            return col
    return ""


def _normalize_date_text(value: Any) -> str:
    text = str(value or "").strip()
    match = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", text)
    if not match:
        return ""
    year, month, day = match.groups()
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def _append_title_target_date_filter(
    *,
    where_parts: List[str],
    params: List[Any],
    columns: Set[str],
    data: Dict[str, Any],
    stage: str,
    db_name: str,
    learn_table: str,
) -> tuple[Optional[str], Optional[str], str, bool]:
    start_date, end_date = _parse_partial_target_date(data)
    date_col = _title_target_date_column(columns)
    applied = False
    reason = "target_date_empty"
    if _truthy((data or {}).get("title_ignore_date_filter")):
        logger.info(
            "[TitleOnly][FlowDebug][date_filter] stage=%s job_id=%s db=%s table=%s applied=%s date_col=%s start=%s end=%s raw_target_date=%s reason=%s",
            stage,
            str((data or {}).get("job_id") or ""),
            db_name,
            learn_table,
            False,
            date_col,
            start_date,
            end_date,
            (data or {}).get("target_date") or (data or {}).get("start_urls_target_date"),
            "ignored_by_title_ignore_date_filter",
        )
        return start_date, end_date, date_col, False
    if start_date and end_date:
        if date_col:
            where_parts.append(
                f"LEFT(NULLIF(CAST(`{date_col}` AS CHAR), '0000-00-00 00:00:00'), 10) BETWEEN %s AND %s"
            )
            params.extend([start_date, end_date])
            applied = True
            reason = "applied"
        else:
            reason = "date_column_missing"
    logger.info(
        "[TitleOnly][FlowDebug][date_filter] stage=%s job_id=%s db=%s table=%s applied=%s date_col=%s start=%s end=%s raw_target_date=%s reason=%s",
        stage,
        str((data or {}).get("job_id") or ""),
        db_name,
        learn_table,
        applied,
        date_col,
        start_date,
        end_date,
        (data or {}).get("target_date") or (data or {}).get("start_urls_target_date"),
        reason,
    )
    return start_date, end_date, date_col, applied


def _row_matches_title_target_date(
    row: Dict[str, Any],
    *,
    columns: Set[str],
    data: Dict[str, Any],
) -> bool:
    start_date, end_date = _parse_partial_target_date(data)
    if not start_date or not end_date:
        return True
    date_col = _title_target_date_column(columns)
    if not date_col:
        return True
    row_date = _normalize_date_text(row.get(date_col))
    return bool(row_date and start_date <= row_date <= end_date)


def _partial_filter_matches_content(content: Any, target_filter: Dict[str, Any]) -> bool:
    if not target_filter:
        return True
    text = str(content or "").lower()
    url_terms = [item.lower() for item in _target_filter_url_terms(target_filter)]
    query_terms = [item.lower() for item in _target_filter_query_terms(target_filter)]
    match_mode = str((target_filter or {}).get("match_mode") or "any").strip().lower()
    url_ok = not url_terms or any(term in text for term in url_terms)
    query_ok = not query_terms or any(term in text for term in query_terms)
    if url_terms and query_terms:
        return url_ok and query_ok if match_mode == "all" else (url_ok or query_ok)
    return url_ok and query_ok


async def _load_partial_title_target_rows(
    *,
    db_name: str,
    learn_table: str,
    columns: Set[str],
    content_col: str,
    data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    target_filter = data.get("partial_target_filter") if isinstance(data.get("partial_target_filter"), dict) else {}
    start_date, end_date = _parse_partial_target_date(data)
    limit_disabled = _truthy((data or {}).get("title_limit_disabled"))
    max_rows = None if limit_disabled else max(1, min(_safe_int(data.get("title_limit"), _safe_int(os.getenv("TITLE_ONLY_MAX_ROWS"), 50000)), 200000))
    select_cols = ["id", f"`{content_col}` AS content_value"]
    for col in ("subject", "web_title", "content_type", "content_at", "content_created_at", "created_at"):
        if col in columns:
            select_cols.append(_title_select_column_expr(col))

    conditions = []
    params: List[Any] = []
    if "content_type" in columns:
        conditions.append("LOWER(COALESCE(`content_type`, '')) = 'url'")
    if "type" in columns:
        conditions.append("LOWER(COALESCE(`type`, '')) <> 'file'")
    start_date, end_date, _, _ = _append_title_target_date_filter(
        where_parts=conditions,
        params=params,
        columns=columns,
        data=data,
        stage="partial_title_targets",
        db_name=db_name,
        learn_table=learn_table,
    )
    _append_title_subject_filter(
        where_parts=conditions,
        params=params,
        columns=columns,
        data=data,
        stage="partial_title_targets",
        db_name=db_name,
        learn_table=learn_table,
    )
    url_terms = _target_filter_url_terms(target_filter)
    query_terms = _target_filter_query_terms(target_filter)
    match_mode = str((target_filter or {}).get("match_mode") or "any").strip().lower()
    like_terms = url_terms + [term for term in query_terms if term not in url_terms]
    if like_terms:
        joiner = " AND " if match_mode == "all" else " OR "
        conditions.append("(" + joiner.join([f"`{content_col}` LIKE %s"] * len(like_terms)) + ")")
        params.extend([f"%{term}%" for term in like_terms])
    where_sql = " AND ".join(conditions) if conditions else "1=1"

    limit_sql = "" if limit_disabled else "LIMIT %s"
    query_params = list(params)
    if not limit_disabled:
        query_params.append(max_rows)
    rows = await mysql_execute_query(
        f"""
        SELECT {', '.join(select_cols)}
        FROM `{learn_table}`
        WHERE {where_sql}
        ORDER BY `id` ASC
        {limit_sql}
        """,
        tuple(query_params),
        fetch=True,
        dbname=db_name,
    )
    rows_out: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if not _partial_filter_matches_content(row.get("content_value"), target_filter):
            continue
        merged = dict(row)
        merged["_source_url"] = str(row.get("content_value") or "").strip()
        rows_out.append(merged)
    logger.info(
        "[TitleOnly] partial targets loaded | db=%s table=%s rows=%s filter=%s target_date=%s",
        db_name,
        learn_table,
        len(rows_out),
        target_filter,
        [start_date, end_date] if start_date and end_date else None,
    )
    return rows_out


def _row_needs_title_update(row: Dict[str, Any], new_title: str) -> bool:
    if _is_weak_title(new_title):
        return False
    subject = str(row.get("subject") or "").strip()
    web_title = str(row.get("web_title") or "").strip()
    if _is_weak_title(subject) or _is_weak_title(web_title):
        return True
    return False


async def _fetch_and_parse_title(url: str, *, timeout_sec: float) -> str:
    if not url:
        return ""
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
        from backend.board.board_content_workflow import BoardContentWorkflow
    except Exception as exc:
        logger.info("[TitleOnly] parser unavailable | url=%s err=%s", _compact(url), exc)
        return ""

    workflow = BoardContentWorkflow()
    try:
        html = await workflow._fetch_html_static(url, timeout_sec=timeout_sec)
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        title = _extract_title_from_head_first(soup)
        if not title:
            title = str(workflow._extract_board_title(soup, url=url, html=html) or "").strip()
        if not title and soup.title and soup.title.string:
            title = str(soup.title.string or "").strip()
        return "" if _is_weak_title(title) else re.sub(r"\s+", " ", title).strip()
    except Exception as exc:
        logger.info("[TitleOnly] title parse failed | url=%s err=%s", _compact(url), exc)
        return ""
    finally:
        try:
            await workflow._close_http_session()
        except Exception:
            pass


async def _resolve_candidate_title(
    row: Dict[str, Any],
    *,
    fetch_missing: bool,
    timeout_sec: float,
) -> tuple[str, str]:
    hint = str(row.get("_title_hint") or "").strip()
    if hint and not _is_weak_title(hint):
        return hint, "hint"
    if not fetch_missing:
        return "", "title_missing_no_fetch"
    url = str(row.get("_source_url") or row.get("content_value") or "").strip()
    title = await _fetch_and_parse_title(url, timeout_sec=timeout_sec)
    if title:
        return title, "parsed"
    return "", "title_parse_empty"


async def _apply_title_row(
    *,
    db_name: str,
    learn_table: str,
    columns: Set[str],
    row: Dict[str, Any],
    fetch_missing: bool,
    timeout_sec: float,
) -> Dict[str, Any]:
    row_id = _safe_int(row.get("id"), 0)
    content_value = str(row.get("content_value") or "").strip()
    if row_id <= 0:
        return {
            "ok": False,
            "skipped": True,
            "id": row_id,
            "reason": str(row.get("_title_skip_reason") or "learn_list_missing"),
            "content": _compact(content_value),
        }

    title, source = await _resolve_candidate_title(
        row,
        fetch_missing=fetch_missing,
        timeout_sec=timeout_sec,
    )
    if not title:
        return {
            "ok": False,
            "skipped": True,
            "id": row_id,
            "reason": source,
            "content": _compact(content_value),
        }
    if not _row_needs_title_update(row, title):
        return {
            "ok": False,
            "unchanged": True,
            "id": row_id,
            "title": _compact(title),
            "source": source,
            "content": _compact(content_value),
        }

    update_sets: List[str] = []
    update_params: List[Any] = []
    if "subject" in columns:
        update_sets.append("`subject` = %s")
        update_params.append(title)
    if "web_title" in columns:
        update_sets.append("`web_title` = %s")
        update_params.append(title)
    if not update_sets:
        return {
            "ok": False,
            "skipped": True,
            "id": row_id,
            "reason": "title_columns_missing",
            "content": _compact(content_value),
        }

    update_params.append(row_id)
    from backend.shared.db_write_queue import run_db_write

    await run_db_write(
        "postprocess.title_only_update",
        lambda: mysql_execute_query(
            f"UPDATE `{learn_table}` SET {', '.join(update_sets)} WHERE id = %s",
            tuple(update_params),
            dbname=db_name,
        ),
    )
    return {
        "ok": True,
        "id": row_id,
        "title": _compact(title),
        "source": source,
        "content": _compact(content_value),
    }


async def run_title_only(data: Dict[str, Any]) -> Dict[str, Any]:
    db_name = resolve_db_name(data, default="dev_user") or "dev_user"
    job_id = str((data or {}).get("job_id") or "").strip()
    chat_bot_id = str((data or {}).get("chat_bot_id") or "").strip()
    suppress_terminal_sse = bool((data or {}).get("_suppress_terminal_sse"))
    if not chat_bot_id:
        meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
        chat_bot_id = str(meta.get("chat_bot_id") or "").strip()
    if not chat_bot_id:
        raise RuntimeError("title_only requires chat_bot_id")

    started_payload = {
        "status": "running",
        "event": "title_only_started",
        "job_id": job_id,
        "account_name": db_name,
        "total_count": 0,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
        "field_save_counts": _field_save_counts(0),
        "h3": "제목 보정",
        "message": "제목 보정 준비 중",
        "source": "title_only",
    }
    if job_id:
        await update_state_only(job_id=job_id, account_name=db_name, payload=started_payload)
        await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=started_payload)

    learn_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    columns = await _get_table_columns_lower(db_name, learn_table) if learn_table else set()
    rows = await _load_title_target_rows(
        db_name=db_name,
        learn_table=str(learn_table or ""),
        columns=columns,
        data=data,
    )
    total = len(rows)
    updated = 0
    unchanged = 0
    skipped = 0
    errors = 0
    samples: List[Dict[str, Any]] = []
    skip_samples: List[Dict[str, Any]] = []
    error_samples: List[Dict[str, Any]] = []

    fetch_missing = str(
        data.get("title_only_fetch_missing")
        or os.getenv("TITLE_ONLY_FETCH_MISSING", "1")
        or "1"
    ).strip().lower() in {"1", "true", "yes", "on", "y"}
    try:
        timeout_raw = float(os.getenv("TITLE_ONLY_FETCH_TIMEOUT_SEC", "10") or "10")
    except Exception:
        timeout_raw = 10.0
    timeout_sec = max(3.0, min(timeout_raw, 30.0))
    concurrency = max(1, min(_safe_int(os.getenv("TITLE_ONLY_CONCURRENCY"), 5), 20))
    semaphore = asyncio.Semaphore(concurrency)

    if job_id:
        await send_message_to_redis_sse(
            job_id=job_id,
            dbname=db_name,
            message={
                **started_payload,
                "total_count": total,
                "scan_count": total,
                "message": f"제목 보정 대상 {total}건 확인",
            },
        )

    async def _run_one(row: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            return await _apply_title_row(
                db_name=db_name,
                learn_table=str(learn_table),
                columns=columns,
                row=row,
                fetch_missing=fetch_missing,
                timeout_sec=timeout_sec,
            )

    processed = 0
    chunk_size = 1
    logger.info(
        "[TitleOnly][UnitProgressDebug] start | job_id=%s total=%s concurrency=%s chunk_size=%s rows=%s",
        job_id,
        total,
        concurrency,
        chunk_size,
        len(rows),
    )
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        logger.info(
            "[TitleOnly][UnitProgressDebug] chunk_start | job_id=%s start=%s chunk_len=%s processed=%s updated=%s",
            job_id,
            start,
            len(chunk),
            processed,
            updated,
        )
        results = await asyncio.gather(
            *[_run_one(row) for row in chunk],
            return_exceptions=True,
        )
        for offset, result in enumerate(results):
            row = chunk[offset]
            processed += 1
            if isinstance(result, Exception):
                errors += 1
                logger.error(
                    "[TitleOnly] row failed | job_id=%s row_id=%s err=%s",
                    job_id,
                    row.get("id"),
                    result,
                )
                if len(error_samples) < 10:
                    error_samples.append({"id": row.get("id"), "detail": str(result)})
                continue
            if result.get("ok"):
                updated += 1
                if len(samples) < 10:
                    samples.append(result)
            elif result.get("unchanged"):
                unchanged += 1
            elif result.get("skipped"):
                skipped += 1
                if len(skip_samples) < 10:
                    skip_samples.append(result)
            else:
                errors += 1
                if len(error_samples) < 10:
                    error_samples.append(result)
            logger.info(
                "[TitleOnly][UnitProgressDebug] row_done | job_id=%s processed=%s total=%s updated=%s unchanged=%s skipped=%s errors=%s row_id=%s result=%s",
                job_id,
                processed,
                total,
                updated,
                unchanged,
                skipped,
                errors,
                row.get("id"),
                (
                    "exception" if isinstance(result, Exception)
                    else "ok" if result.get("ok")
                    else "unchanged" if result.get("unchanged")
                    else "skipped" if result.get("skipped")
                    else "error"
                ),
            )
            if job_id:
                logger.info(
                    "[TitleOnly][UnitProgressDebug] redis_send | job_id=%s processed=%s collection=%s field_title=%s",
                    job_id,
                    processed,
                    updated,
                    _field_save_counts(updated).get("title"),
                )
                await send_message_to_redis_sse(
                    job_id=job_id,
                    dbname=db_name,
                    message={
                        "status": "running",
                        "event": "title_only_progress",
                        "job_id": job_id,
                        "total_count": total,
                        "scan_count": total,
                        "collection_count": updated,
                        "save_count": 0,
                        "study_count": 0,
                        "updated_count": updated,
                        "field_save_counts": _field_save_counts(updated),
                        "message": f"제목 보정 중 {processed}/{total}",
                        "source": "title_only",
                    },
                )

        if False and job_id:
            await send_message_to_redis_sse(
                job_id=job_id,
                dbname=db_name,
                message={
                    "status": "running",
                    "event": "title_only_progress",
                    "job_id": job_id,
                    "total_count": total,
                    "scan_count": total,
                    "collection_count": updated,
                    "save_count": 0,
                    "study_count": 0,
                    "message": f"제목 보정 중 {processed}/{total}",
                    "updated_count": updated,
                        "field_save_counts": _field_save_counts(updated),
                        "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
                        "partial_sequence_running": data.get("_partial_sequence_running"),
                        "source": "title_only",
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
        "unchanged_count": unchanged,
        "skipped_count": skipped,
        "error_count": errors,
        "learn_list_table": learn_table,
        "title_samples": samples,
        "title_skip_samples": skip_samples,
        "title_error_samples": error_samples,
        "message": f"제목 보정 완료: {updated}건 반영, 동일 {unchanged}건, 스킵 {skipped}건, 오류 {errors}건",
        "source": "title_only",
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
    logger.info(
        "[TitleOnly] completed | job_id=%s db=%s chat_bot_id=%s table=%s total=%s updated=%s unchanged=%s skipped=%s errors=%s",
        job_id,
        db_name,
        chat_bot_id,
        learn_table,
        total,
        updated,
        unchanged,
        skipped,
        errors,
    )
    return result_payload


__all__ = [
    "normalize_duplicate_title_request_mode",
    "is_title_only_request",
    "run_title_only",
]
