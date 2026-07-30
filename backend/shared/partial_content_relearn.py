from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Set
from urllib.parse import urlparse

from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot
from db.mysql_db_config import mysql_execute_query
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.shared.partial_content_relearn")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _string_list(value: Any) -> List[str]:
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


def _first_content_url(data: Dict[str, Any]) -> str:
    for key in ("contents", "contents_url", "content", "url"):
        value = (data or {}).get(key)
        if isinstance(value, list):
            for item in value:
                text = str(item or "").strip()
                if text:
                    return text
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _target_filter_terms(data: Dict[str, Any]) -> tuple[List[str], str]:
    target = (data or {}).get("partial_target_filter")
    if not isinstance(target, dict):
        return [], "any"
    terms: List[str] = []
    for item in [*_string_list(target.get("url_contains")), *_string_list(target.get("query_contains"))]:
        lowered = str(item or "").strip().lower()
        if lowered and lowered not in terms:
            terms.append(lowered)
    match_mode = str(target.get("match_mode") or "any").strip().lower()
    if match_mode not in {"any", "all"}:
        match_mode = "any"
    return terms, match_mode


async def _get_columns(db_name: str, table_name: str) -> Set[str]:
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
    cols: Set[str] = set()
    for row in rows or []:
        if isinstance(row, dict):
            name = str(row.get("column_name") or "").strip().lower()
            if name:
                cols.add(name)
    return cols


def _fallback_scope_condition(content_col: str, data: Dict[str, Any]) -> tuple[str, List[Any]]:
    raw_url = _first_content_url(data)
    if not raw_url:
        return "", []
    try:
        parsed = urlparse(ensure_url_scheme(raw_url))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").strip()
    except Exception:
        return "", []
    clauses: List[str] = []
    params: List[Any] = []
    if host:
        clauses.append(f"LOWER(CAST(`{content_col}` AS CHAR)) LIKE %s")
        params.append(f"%{host}%")
    # Main/home URLs should not force a path prefix, but a board/list URL can.
    path_lc = path.lower().strip("/")
    if path_lc and not path_lc.endswith(("main.do", "index.do", "default.do")):
        first = path_lc.split("/", 1)[0]
        if first:
            clauses.append(f"LOWER(CAST(`{content_col}` AS CHAR)) LIKE %s")
            params.append(f"%/{first}/%")
    if not clauses:
        return "", []
    return "(" + " AND ".join(clauses) + ")", params


def _target_sql(content_col: str, data: Dict[str, Any]) -> tuple[str, List[Any], str]:
    terms, match_mode = _target_filter_terms(data)
    if terms:
        joiner = " AND " if match_mode == "all" else " OR "
        return (
            "(" + joiner.join([f"LOWER(CAST(`{content_col}` AS CHAR)) LIKE %s" for _ in terms]) + ")",
            [f"%{term}%" for term in terms],
            "partial_target_filter",
        )
    sql, params = _fallback_scope_condition(content_col, data)
    return sql, params, "contents_scope" if sql else "none"


def _parse_target_date(data: Dict[str, Any]) -> tuple[str, str]:
    raw = (data or {}).get("target_date") or (data or {}).get("start_urls_target_date")
    if isinstance(raw, list) and len(raw) >= 2:
        return str(raw[0] or "").strip(), str(raw[1] or "").strip()
    return "", ""


def _bool_from_payload(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_pg_training_table_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if re.fullmatch(r"td_[a-z0-9_]+_training_data", text or ""):
        return text
    return ""


def _url_match_candidates(*values: Any) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        candidates = [text]
        if text.startswith("https://"):
            candidates.append("http://" + text[len("https://") :])
        elif text.startswith("http://"):
            candidates.append("https://" + text[len("http://") :])
        for candidate in candidates:
            if candidate and candidate not in seen:
                seen.add(candidate)
                out.append(candidate)
    return out


def _metadata_like_pattern(value: Any) -> str:
    text = str(value or "").strip()
    return f"%{text}%" if text else ""


async def _resolve_pg_training_table(*, db_name: str, chat_bot_id: str) -> str:
    try:
        from utils.whoami import get_chat_id_from_db

        chat_id = await get_chat_id_from_db(db_name, chat_bot_id)
    except Exception as exc:
        logger.info(
            "[PartialContent] pg table chat_id lookup failed | db=%s chat_bot_id=%s err=%s",
            db_name,
            chat_bot_id,
            exc,
        )
        chat_id = None
    return _safe_pg_training_table_name(f"td_{chat_id}_training_data") if chat_id else ""


async def _pg_table_columns(*, db_name: str, table_name: str) -> Set[str]:
    if not table_name:
        return set()
    try:
        from db.db_operations import execute_query as pg_execute_query

        rows = await pg_execute_query(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = $1
            """,
            (table_name,),
            fetch=True,
            dbname=db_name,
        )
    except Exception as exc:
        logger.info(
            "[PartialContent] pg table columns lookup failed | db=%s table=%s err=%s",
            db_name,
            table_name,
            exc,
        )
        return set()
    return {
        str(dict(row).get("column_name") or "").strip()
        for row in rows or []
        if row and dict(row).get("column_name")
    }


async def _filter_rows_missing_pg_learning(
    *,
    db_name: str,
    chat_bot_id: str,
    rows: List[Dict[str, Any]],
    match_content_metadata: bool,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not rows:
        return rows, {"enabled": True, "reason": "no_rows", "matched": 0}

    training_table = await _resolve_pg_training_table(db_name=db_name, chat_bot_id=chat_bot_id)
    columns = await _pg_table_columns(db_name=db_name, table_name=training_table)
    if not training_table or "content" not in columns:
        return rows, {
            "enabled": True,
            "reason": "pg_table_or_content_column_missing",
            "training_table": training_table,
            "matched": 0,
        }

    has_content_metadata = "content_metadata" in columns
    row_ids_by_key: Dict[tuple[str, str], Set[int]] = {}
    for index, row in enumerate(rows):
        for candidate in _url_match_candidates(row.get("content_value")):
            row_ids_by_key.setdefault(("content", candidate), set()).add(index)
            if match_content_metadata and has_content_metadata:
                row_ids_by_key.setdefault(("metadata_source_url", candidate), set()).add(index)

    content_values = sorted({value for key, value in row_ids_by_key if key == "content"})
    metadata_values = sorted({value for key, value in row_ids_by_key if key == "metadata_source_url"})
    conditions: List[str] = []
    params: List[Any] = []
    if content_values:
        params.append(content_values)
        conditions.append(f"content = ANY(${len(params)}::text[])")
    if metadata_values and has_content_metadata:
        patterns = [_metadata_like_pattern(value) for value in metadata_values if _metadata_like_pattern(value)]
        if patterns:
            params.append(patterns)
            conditions.append(f"content_metadata::text LIKE ANY(${len(params)}::text[])")
    if not conditions:
        return rows, {
            "enabled": True,
            "reason": "no_pg_lookup_values",
            "training_table": training_table,
            "matched": 0,
        }

    text_filter = "text_data IS NOT NULL AND TRIM(CAST(text_data AS TEXT)) <> ''" if "text_data" in columns else "1=1"
    select_cols = ["content"]
    if has_content_metadata:
        select_cols.append("content_metadata")
    try:
        from db.db_operations import execute_query as pg_execute_query

        pg_rows = await pg_execute_query(
            f"""
            SELECT {", ".join(select_cols)}
            FROM public.{training_table}
            WHERE {text_filter}
              AND ({" OR ".join(conditions)})
            """,
            tuple(params),
            fetch=True,
            dbname=db_name,
        )
    except Exception as exc:
        logger.info(
            "[PartialContent] pg learned-row lookup failed open | db=%s table=%s err=%s",
            db_name,
            training_table,
            exc,
        )
        return rows, {
            "enabled": True,
            "reason": "pg_lookup_failed_open",
            "training_table": training_table,
            "matched": 0,
            "error": str(exc),
        }

    learned_indexes: Set[int] = set()
    for pg_row in pg_rows or []:
        if not pg_row:
            continue
        pg_dict = dict(pg_row)
        for candidate in _url_match_candidates(pg_dict.get("content")):
            learned_indexes.update(row_ids_by_key.get(("content", candidate), set()))
        if match_content_metadata and has_content_metadata:
            metadata_text = str(pg_dict.get("content_metadata") or "")
            for candidate in metadata_values:
                if candidate and candidate in metadata_text:
                    learned_indexes.update(row_ids_by_key.get(("metadata_source_url", candidate), set()))

    filtered = [row for index, row in enumerate(rows) if index not in learned_indexes]
    return filtered, {
        "enabled": True,
        "reason": "ok",
        "training_table": training_table,
        "input_rows": len(rows),
        "matched": len(learned_indexes),
        "remaining": len(filtered),
        "match_content_metadata": bool(match_content_metadata and has_content_metadata),
    }


async def load_partial_content_relearn_targets(
    data: Dict[str, Any],
    *,
    db_name: str,
    chat_bot_id: str,
) -> Dict[str, Any]:
    learn_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    columns = await _get_columns(db_name, str(learn_table or "")) if learn_table else set()
    if not learn_table or "id" not in columns or "content" not in columns:
        return {"ok": False, "reason": "learn_list_table_or_columns_missing", "rows": [], "table": learn_table}

    max_rows = max(
        1,
        min(
            _safe_int((data or {}).get("content_relearn_limit"), _safe_int(os.getenv("PARTIAL_CONTENT_RELEARN_MAX_ROWS"), 1000)),
            20000,
        ),
    )
    select_cols = ["id", "`content` AS content_value"]
    for col in ("subject", "web_title", "content_type", "content_created_at", "created_at"):
        if col in columns:
            select_cols.append(f"`{col}`")

    conditions = ["`content` IS NOT NULL", "TRIM(CAST(`content` AS CHAR)) <> ''"]
    params: List[Any] = []
    if "content_type" in columns:
        conditions.append("LOWER(COALESCE(`content_type`, '')) = 'url'")
    if "status" in columns:
        conditions.append("UPPER(COALESCE(`status`, '')) = %s")
        params.append(str((data or {}).get("content_relearn_status") or "Y").upper())

    target_condition, target_params, filter_source = _target_sql("content", data)
    if not target_condition:
        return {
            "ok": False,
            "reason": "target_filter_required",
            "rows": [],
            "table": learn_table,
            "filter_source": filter_source,
        }
    conditions.append(target_condition)
    params.extend(target_params)

    start_date, end_date = _parse_target_date(data)
    date_col = "content_created_at" if "content_created_at" in columns else ("created_at" if "created_at" in columns else "")
    if date_col and start_date and end_date and str((data or {}).get("content_relearn_ignore_date") or "").lower() not in {"1", "true", "yes", "on"}:
        conditions.append(f"DATE(`{date_col}`) BETWEEN %s AND %s")
        params.extend([start_date, end_date])

    where_sql = " AND ".join(conditions)
    rows = await mysql_execute_query(
        (
            f"SELECT {', '.join(select_cols)} FROM `{learn_table}` "
            f"WHERE {where_sql} ORDER BY `id` ASC LIMIT {max_rows}"
        ),
        tuple(params),
        fetch=True,
        dbname=db_name,
    )

    out_rows: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        url = str(row.get("content_value") or "").strip()
        if not url:
            continue
        title = str(row.get("web_title") or row.get("subject") or "").strip()
        out_rows.append(
            {
                "url": url,
                "title": title,
                "subject": title,
                "type": "partial_content_relearn",
                "force_detail": True,
                "force_relearn": True,
                "learn_list_id": _safe_int(row.get("id"), 0),
            }
        )

    pg_filter_meta: Dict[str, Any] = {"enabled": False}
    if _bool_from_payload((data or {}).get("content_relearn_only_missing_pg"), True):
        out_rows, pg_filter_meta = await _filter_rows_missing_pg_learning(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            rows=out_rows,
            match_content_metadata=_bool_from_payload(
                (data or {}).get("content_relearn_match_pg_content_metadata"),
                True,
            ),
        )

    logger.info(
        "[PartialContent] targets loaded | db=%s table=%s rows=%s filter_source=%s filter=%s pg_filter=%s where=%s params=%s sample=%s",
        db_name,
        learn_table,
        len(out_rows),
        filter_source,
        (data or {}).get("partial_target_filter") if isinstance((data or {}).get("partial_target_filter"), dict) else {},
        pg_filter_meta,
        where_sql,
        params,
        out_rows[:3],
    )
    return {
        "ok": True,
        "reason": "ok",
        "rows": out_rows,
        "table": learn_table,
        "filter_source": filter_source,
        "pg_filter": pg_filter_meta,
        "where": where_sql,
        "params": params,
    }
