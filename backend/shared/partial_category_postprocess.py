import logging
import re
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlparse, unquote

from backend.shared.pre_explored_url import (
    _extract_query_keys_from_rule_item,
    _category_table_name_from_chat_bot_id,
    get_category_url_pattern_raw,
    _get_rule_entries,
    resolve_cate_for_detail_url,
)
from backend.shared.redis_sse_service import send_message_to_redis_sse, update_state_only
from backend.shared.sub_cate_mode import get_sub_cate_mode_from_config, merge_category_pair
from db.crawl_db_manager import update_crawling_log_counters
from db.maria_operations import maria_execute_query
from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot
from utils.db_name import resolve_db_name
from utils.timezone_utils import get_local_now
from utils.url import canonicalize_url_for_dedup, ensure_url_scheme

logger = logging.getLogger("backend.shared.partial_category_postprocess")
CRAWLING_CATEGORY_TABLE = "ASADAL_CRAWLING_CATEGORY"


def _field_save_counts(cate_count: int = 0) -> Dict[str, int]:
    return {
        "title": 0,
        "content": 0,
        "cate": int(cate_count or 0),
        "symmary": 0,
        "type": 0,
        "url": 0,
        "web_de": 0,
    }


def is_partial_category_postprocess_request(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    colle = str(data.get("colle") or "").strip().lower()
    if colle != "content":
        return False
    fields = data.get("partial_update_fields")
    if isinstance(fields, str):
        fields = [fields]
    if not isinstance(fields, list) or "cate" not in {str(v or "").strip().lower() for v in fields}:
        return False
    return bool(data.get("partial_target_filter") is not None)


def partial_category_debug_reason(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return {"ok": False, "reason": "payload_not_dict"}
    fields = data.get("partial_update_fields")
    if isinstance(fields, str):
        fields = [fields]
    normalized_fields = (
        {str(v or "").strip().lower() for v in fields}
        if isinstance(fields, list)
        else set()
    )
    out = {
        "ok": False,
        "colle": str(data.get("colle") or "").strip().lower(),
        "partial_update_fields": sorted(normalized_fields),
        "has_partial_target_filter": data.get("partial_target_filter") is not None,
        "partial_target_filter_type": type(data.get("partial_target_filter")).__name__,
        "content_type": data.get("content_type"),
        "crawl_mode": data.get("crawl_mode"),
    }
    if out["colle"] != "content":
        out["reason"] = "colle_not_content"
    elif "cate" not in normalized_fields:
        out["reason"] = "cate_not_in_partial_update_fields"
    elif data.get("partial_target_filter") is None:
        out["reason"] = "partial_target_filter_missing"
    else:
        out["ok"] = True
        out["reason"] = "matched"
    return out


def _partial_update_fields(data: Dict[str, Any]) -> Set[str]:
    fields = (data or {}).get("partial_update_fields")
    if isinstance(fields, str):
        fields = [fields]
    if not isinstance(fields, list):
        return set()
    return {
        str(item or "").strip().lower()
        for item in fields
        if str(item or "").strip()
    }


def _is_cate_only_partial_request(data: Dict[str, Any]) -> bool:
    fields = _partial_update_fields(data)
    return bool(fields) and fields == {"cate"}


def _first_list_value(value: Any) -> str:
    if isinstance(value, list) and value:
        return str(value[0] or "").strip()
    return str(value or "").strip()


def _normalize_text_list(value: Any) -> List[str]:
    if isinstance(value, list):
        raw = value
    elif isinstance(value, str):
        raw = [value]
    else:
        raw = []
    out: List[str] = []
    for item in raw:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _has_target_filter_terms(target_filter: Dict[str, Any]) -> bool:
    if not isinstance(target_filter, dict):
        return False
    return bool(
        _target_filter_url_terms(target_filter)
        or _target_filter_query_terms(target_filter)
    )


def _target_filter_url_terms(target_filter: Dict[str, Any]) -> List[str]:
    if not isinstance(target_filter, dict):
        return []
    terms: List[str] = []
    for key in ("url_contains", "url", "urls"):
        for item in _normalize_text_list(target_filter.get(key)):
            if item not in terms:
                terms.append(item)
    return terms


def _target_filter_query_terms(target_filter: Dict[str, Any]) -> List[str]:
    if not isinstance(target_filter, dict):
        return []
    terms: List[str] = []
    for key in ("query_contains", "query", "queries", "query_params", "query_param"):
        for item in _normalize_text_list(target_filter.get(key)):
            if item not in terms:
                terms.append(item)
    return terms


def _parse_target_date(data: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    raw = data.get("target_date") or data.get("start_urls_target_date")
    if not isinstance(raw, list) or len(raw) < 2:
        return None, None
    start = str(raw[0] or "").strip()
    end = str(raw[1] or "").strip()
    if not start and not end:
        return None, None
    if not start:
        start = end
    if not end:
        end = start
    return start[:10], end[:10]


def _default_current_month_date_range() -> Tuple[str, str]:
    today = get_local_now().date()
    start = today.replace(day=1)
    return start.isoformat(), today.isoformat()


async def _get_columns(db_name: str, table_name: str) -> Set[str]:
    rows = await maria_execute_query(
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
    return {
        str((row or {}).get("column_name") or "").strip().lower()
        for row in (rows or [])
        if isinstance(row, dict) and str((row or {}).get("column_name") or "").strip()
    }


async def _get_text_columns(db_name: str, table_name: str) -> List[str]:
    rows = await maria_execute_query(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND LOWER(table_name) = LOWER(%s)
        ORDER BY ordinal_position ASC
        """,
        (db_name, table_name),
        fetch=True,
        dbname=db_name,
    )
    text_types = {"char", "varchar", "tinytext", "text", "mediumtext", "longtext", "json"}
    out: List[str] = []
    seen: Set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        col = str(row.get("column_name") or "").strip()
        dtype = str(row.get("data_type") or "").strip().lower()
        if not col or col.lower() in seen or dtype not in text_types:
            continue
        seen.add(col.lower())
        out.append(col)
    return out


def _query_tokens_from_url_or_text(value: str) -> Set[str]:
    original_text = str(value or "").strip()
    if not original_text:
        return set()
    tokens: Set[str] = set()

    try:
        parsed = urlparse(ensure_url_scheme(original_text))
    except Exception:
        parsed = urlparse(original_text)

    path_text = unquote(parsed.path or "")
    for part in re.split(r"[/?#&=\s,|]+", path_text):
        part = str(part or "").strip().lower()
        if part:
            tokens.add(part)
    if path_text:
        tokens.add(path_text.strip().lower())
        tokens.add(path_text.strip().lstrip("/").lower())

    text = original_text
    if "?" in text:
        try:
            text = urlparse(ensure_url_scheme(text)).query or text.split("?", 1)[1]
        except Exception:
            text = text.split("?", 1)[1]
    if text.startswith("?"):
        text = text[1:]
    if "=" in text or "&" in text:
        try:
            for key, val in parse_qsl(text, keep_blank_values=True):
                key_text = str(key or "").strip()
                val_text = str(val or "").strip()
                if key_text:
                    tokens.add(f"{key_text}={val_text}".lower() if val_text else key_text.lower())
                    tokens.add(key_text.lower())
                    if val_text:
                        tokens.add(val_text.lower())
        except Exception:
            pass
    if not tokens or "?" not in original_text:
        for part in text.replace(",", " ").split():
            part = part.strip()
            if part:
                tokens.add(part.lower())
    return tokens


def _target_filter_query_match_terms(value: str) -> Set[str]:
    text = str(value or "").strip()
    if not text:
        return set()
    terms: Set[str] = {text.lower()}
    try:
        parsed = urlparse(ensure_url_scheme(text))
    except Exception:
        parsed = urlparse(text)

    path_text = unquote(parsed.path or "").strip()
    for part in re.split(r"[/?#&=\s,|]+", path_text):
        part = str(part or "").strip().lower()
        if part:
            terms.add(part)
    if path_text:
        terms.add(path_text.lower())
        terms.add(path_text.lstrip("/").lower())

    query_text = parsed.query or ""
    if not query_text and "?" in text:
        query_text = text.split("?", 1)[1]
    if query_text.startswith("?"):
        query_text = query_text[1:]
    if query_text:
        for key, val in parse_qsl(query_text, keep_blank_values=True):
            key_text = str(key or "").strip().lower()
            val_text = str(val or "").strip().lower()
            if key_text and val_text:
                terms.add(f"{key_text}={val_text}")
                terms.add(val_text)
            elif key_text:
                terms.add(key_text)
    elif "=" in text or "&" in text:
        for key, val in parse_qsl(text, keep_blank_values=True):
            key_text = str(key or "").strip().lower()
            val_text = str(val or "").strip().lower()
            if key_text and val_text:
                terms.add(f"{key_text}={val_text}")
                terms.add(val_text)
            elif key_text:
                terms.add(key_text)
    return {term for term in terms if term}


def _expanded_target_filter_query_terms(query_terms: List[str]) -> Set[str]:
    expanded: Set[str] = set()
    for term in query_terms:
        expanded.update(_target_filter_query_match_terms(term))
    return expanded


def _rule_matches_partial_filter(rule: Dict[str, Any], target_filter: Dict[str, Any]) -> bool:
    url_terms = [v.lower() for v in _target_filter_url_terms(target_filter)]
    query_terms = [v.lower() for v in _target_filter_query_terms(target_filter)]
    match_mode = str(target_filter.get("match_mode") or "any").strip().lower()
    rule_url = str(rule.get("url") or "").strip().lower()
    rule_query_tokens = {
        str(token or "").strip().lower()
        for token in (rule.get("query") or [])
        if str(token or "").strip()
    }
    expanded_query_terms: Set[str] = set()
    expanded_query_terms.update(_expanded_target_filter_query_terms(query_terms))

    url_ok = not url_terms or any(term in rule_url or (rule_url and rule_url in term) for term in url_terms)
    query_ok = not expanded_query_terms or any(
        any(term in token or token in term for token in rule_query_tokens)
        for term in expanded_query_terms
    )
    if url_terms and expanded_query_terms:
        return url_ok and query_ok if match_mode == "all" else (url_ok or query_ok)
    return url_ok and query_ok


def _filtered_category_object(category_obj: Optional[Dict[str, Any]], target_filter: Dict[str, Any]) -> Dict[str, Any]:
    rules = [r for r in _get_rule_entries(category_obj or {}) if isinstance(r, dict)]
    if not target_filter:
        return {"mode": "rule", "rules": rules}
    filtered = [rule for rule in rules if _rule_matches_partial_filter(rule, target_filter)]
    return {"mode": "rule", "rules": filtered}


def _target_filter_sql_condition(target_filter: Dict[str, Any]) -> Tuple[str, List[Any]]:
    if not isinstance(target_filter, dict):
        return "", []
    url_terms = [str(v or "").strip().lower() for v in _target_filter_url_terms(target_filter) if str(v or "").strip()]
    query_terms = sorted(_expanded_target_filter_query_terms(_target_filter_query_terms(target_filter)))
    match_mode = str(target_filter.get("match_mode") or "any").strip().lower()

    def _like_clause(terms: List[str]) -> Tuple[str, List[Any]]:
        clean_terms = [term for term in terms if term]
        if not clean_terms:
            return "", []
        clauses = ["LOWER(COALESCE(`content`, '')) LIKE %s" for _ in clean_terms]
        return "(" + " OR ".join(clauses) + ")", [f"%{term}%" for term in clean_terms]

    url_sql, url_params = _like_clause(url_terms)
    query_sql, query_params = _like_clause(query_terms)
    if url_sql and query_sql:
        joiner = " AND " if match_mode == "all" else " OR "
        return f"({url_sql}{joiner}{query_sql})", url_params + query_params
    if url_sql:
        return url_sql, url_params
    if query_sql:
        return query_sql, query_params
    return "", []


def _row_url_values(row: Dict[str, Any]) -> List[str]:
    values: List[str] = []
    for key in ("content",):
        text = str((row or {}).get(key) or "").strip()
        if text and text not in values:
            values.append(text)
    return values


def _url_key(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = ensure_url_scheme(text)
    return canonicalize_url_for_dedup(normalized) or normalized


def _target_filter_matches_row(row: Dict[str, Any], target_filter: Dict[str, Any]) -> bool:
    if not target_filter:
        return True
    url_terms = [v.lower() for v in _target_filter_url_terms(target_filter)]
    query_terms = [v.lower() for v in _target_filter_query_terms(target_filter)]
    match_mode = str(target_filter.get("match_mode") or "any").strip().lower()
    values = _row_url_values(row)
    value_text = " ".join(values).lower()
    row_query_tokens: Set[str] = set()
    for value in values:
        row_query_tokens.update(_query_tokens_from_url_or_text(value))
    expanded_query_terms: Set[str] = set()
    expanded_query_terms.update(_expanded_target_filter_query_terms(query_terms))
    url_ok = not url_terms or any(term in value_text for term in url_terms)
    query_ok = not expanded_query_terms or any(
        any(term in token or token in term for token in row_query_tokens)
        for term in expanded_query_terms
    )
    if url_terms and expanded_query_terms:
        return url_ok and query_ok if match_mode == "all" else (url_ok or query_ok)
    return url_ok and query_ok


def _rule_code_tokens_from_values(values: List[Any]) -> List[str]:
    tokens: List[str] = []
    seen: Set[str] = set()
    for raw in values or []:
        for match in re.findall(r"(?i)\bB\d{6,}\b", str(raw or "")):
            token = match.upper()
            if token in seen:
                continue
            seen.add(token)
            tokens.append(token)
    return tokens


def _rule_query_code_tokens(rules: List[Dict[str, Any]]) -> List[str]:
    values: List[Any] = []
    for rule in rules or []:
        if not isinstance(rule, dict):
            continue
        values.extend(_extract_query_keys_from_rule_item(rule))
    return _rule_code_tokens_from_values(values)


def _rule_raw_code_tokens(rules: List[Dict[str, Any]]) -> List[str]:
    values: List[Any] = []
    for rule in rules or []:
        try:
            values.append(json.dumps(rule, ensure_ascii=False, default=str))
        except Exception:
            values.append(str(rule or ""))
    return _rule_code_tokens_from_values(values)


async def _log_rule_token_row_probe(
    *,
    db_name: str,
    job_id: str,
    learn_table: str,
    columns: Set[str],
    where_sql: str,
    params: List[Any],
    rules: List[Dict[str, Any]],
) -> None:
    if "content" not in columns:
        logger.info(
            "[PartialCategory][RuleTokenProbe] job_id=%s skipped=no_content_column table=%s columns=%s",
            job_id,
            learn_table,
            sorted(columns),
        )
        return

    query_tokens = _rule_query_code_tokens(rules)
    raw_tokens = _rule_raw_code_tokens(rules)
    tokens = []
    seen: Set[str] = set()
    for token in query_tokens + raw_tokens:
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    logger.info(
        "[PartialCategory][RuleTokenProbe] job_id=%s table=%s query_code_count=%s query_code_tokens=%s raw_code_count=%s raw_code_tokens=%s probe_tokens=%s",
        job_id,
        learn_table,
        len(query_tokens),
        query_tokens[:80],
        len(raw_tokens),
        raw_tokens[:80],
        tokens[:80],
    )
    for token in tokens[:80]:
        like_param = f"%{token.lower()}%"
        sample_cols = ["id", "content"]
        for col in ("content_type", "content_created_at", "cate1", "cate2"):
            if col in columns:
                sample_cols.append(col)
        try:
            total_rows = await maria_execute_query(
                f"""
                SELECT COUNT(*) AS cnt
                FROM `{learn_table}`
                WHERE LOWER(COALESCE(`content`, '')) LIKE %s
                """,
                (like_param,),
                fetch=True,
                dbname=db_name,
            )
            scoped_rows = await maria_execute_query(
                f"""
                SELECT COUNT(*) AS cnt
                FROM `{learn_table}`
                WHERE {where_sql}
                  AND LOWER(COALESCE(`content`, '')) LIKE %s
                """,
                tuple(params + [like_param]),
                fetch=True,
                dbname=db_name,
            )
            sample_rows = await maria_execute_query(
                f"""
                SELECT {', '.join(f'`{col}`' for col in sample_cols)}
                FROM `{learn_table}`
                WHERE LOWER(COALESCE(`content`, '')) LIKE %s
                ORDER BY `id` DESC
                LIMIT 3
                """,
                (like_param,),
                fetch=True,
                dbname=db_name,
            )
            total_count = int(((total_rows or [{}])[0] or {}).get("cnt") or 0)
            scoped_count = int(((scoped_rows or [{}])[0] or {}).get("cnt") or 0)
            samples = []
            for row in sample_rows or []:
                if not isinstance(row, dict):
                    continue
                samples.append(
                    {
                        "id": row.get("id"),
                        "content": str(row.get("content") or "")[:180],
                        "content_type": row.get("content_type"),
                        "content_created_at": str(row.get("content_created_at") or ""),
                        "cate1": row.get("cate1"),
                        "cate2": row.get("cate2") if "cate2" in columns else "",
                    }
                )
            logger.info(
                "[PartialCategory][RuleTokenProbe] job_id=%s token=%s total_rows=%s scoped_rows=%s where=%s params=%s samples=%s",
                job_id,
                token,
                total_count,
                scoped_count,
                where_sql,
                params,
                samples,
            )
        except Exception as ex:
            logger.warning(
                "[PartialCategory][RuleTokenProbe] job_id=%s token=%s failed=%s",
                job_id,
                token,
                ex,
            )


async def _log_category_table_raw_code_probe(
    *,
    db_name: str,
    job_id: str,
    category_table: str,
) -> None:
    if not str(category_table or "").strip():
        logger.info(
            "[PartialCategory][CategoryRawProbe] job_id=%s skipped=missing_category_table",
            job_id,
        )
        return
    try:
        columns = await _get_columns(db_name, category_table)
        text_cols = await _get_text_columns(db_name, category_table)
        if not text_cols:
            logger.info(
                "[PartialCategory][CategoryRawProbe] job_id=%s table=%s skipped=no_text_columns columns=%s",
                job_id,
                category_table,
                sorted(columns),
            )
            return
        scan_cols = text_cols[:40]
        concat_expr = "CONCAT_WS(' ', " + ", ".join(f"COALESCE(`{col}`, '')" for col in scan_cols) + ")"
        base_where = f"{concat_expr} REGEXP 'B[0-9]{{6,}}'"
        total_rows = await maria_execute_query(
            f"SELECT COUNT(*) AS cnt FROM `{category_table}` WHERE {base_where}",
            fetch=True,
            dbname=db_name,
        )
        active_count = None
        if "cate_use" in columns:
            active_rows = await maria_execute_query(
                f"SELECT COUNT(*) AS cnt FROM `{category_table}` WHERE {base_where} AND LOWER(COALESCE(`cate_use`, '')) = 'y'",
                fetch=True,
                dbname=db_name,
            )
            active_count = int(((active_rows or [{}])[0] or {}).get("cnt") or 0)
        select_cols = []
        for col in ("cate_code", "cate_treecode", "cate_name", "cate_use", "url", "query"):
            if col in columns and col not in select_cols:
                select_cols.append(col)
        for col in scan_cols:
            if col.lower() not in {v.lower() for v in select_cols}:
                select_cols.append(col)
        sample_rows = await maria_execute_query(
            f"""
            SELECT {', '.join(f'`{col}`' for col in select_cols[:45])}
            FROM `{category_table}`
            WHERE {base_where}
            ORDER BY {('`cate_treecode` ASC' if 'cate_treecode' in columns else '1')}
            LIMIT 10
            """,
            fetch=True,
            dbname=db_name,
        )
        samples: List[Dict[str, Any]] = []
        raw_tokens: List[str] = []
        for row in sample_rows or []:
            if not isinstance(row, dict):
                continue
            hit_fields: Dict[str, str] = {}
            for col, value in row.items():
                text = str(value or "")
                tokens = _rule_code_tokens_from_values([text])
                if not tokens:
                    continue
                raw_tokens.extend(tokens)
                hit_fields[str(col)] = text[:220]
            samples.append(
                {
                    "cate_code": row.get("cate_code"),
                    "cate_treecode": row.get("cate_treecode"),
                    "cate_name": row.get("cate_name"),
                    "cate_use": row.get("cate_use"),
                    "hit_fields": hit_fields,
                }
            )
        logger.info(
            "[PartialCategory][CategoryRawProbe] job_id=%s table=%s columns=%s text_cols=%s b_code_total_rows=%s b_code_active_rows=%s sample_tokens=%s samples=%s",
            job_id,
            category_table,
            sorted(columns),
            scan_cols,
            int(((total_rows or [{}])[0] or {}).get("cnt") or 0),
            active_count,
            _rule_code_tokens_from_values(raw_tokens)[:80],
            samples,
        )
    except Exception as ex:
        logger.warning(
            "[PartialCategory][CategoryRawProbe] job_id=%s table=%s failed=%s",
            job_id,
            category_table,
            ex,
        )


async def run_partial_category_postprocess(data: Dict[str, Any]) -> Dict[str, Any]:
    db_name = resolve_db_name(data, default="dev_user")
    chat_bot_id = str(data.get("chat_bot_id") or "").strip()
    job_id = str(data.get("job_id") or "").strip()
    suppress_terminal_sse = bool((data or {}).get("_suppress_terminal_sse"))
    contents_url = _first_list_value(data.get("contents"))
    has_explicit_target_filter = "partial_target_filter" in data
    target_filter = data.get("partial_target_filter") if isinstance(data.get("partial_target_filter"), dict) else {}
    raw_url_terms = _target_filter_url_terms(target_filter)
    raw_query_terms = _target_filter_query_terms(target_filter)
    logger.info(
        "[PartialCategory][RequestFilter] job_id=%s db=%s has_filter=%s url_terms=%s query_terms=%s match_mode=%s payload_keys=%s",
        job_id,
        db_name,
        has_explicit_target_filter,
        raw_url_terms,
        raw_query_terms,
        str(target_filter.get("match_mode") or "") if isinstance(target_filter, dict) else "",
        sorted(str(k) for k in data.keys()),
    )
    if has_explicit_target_filter and not _has_target_filter_terms(target_filter):
        logger.info(
            "[PartialCategory] empty explicit target filter; applying all CATEGORY url/query rules | job_id=%s db=%s filter=%s",
            job_id,
            db_name,
            target_filter,
        )
        target_filter = {}
    if not has_explicit_target_filter:
        target_filter = {}
    start_date, end_date = _parse_target_date(data)
    date_source = "payload"
    if not (start_date and end_date):
        start_date, end_date = None, None
        date_source = "not_provided"
        logger.info(
            "[PartialCategory] target_date not provided; date filter disabled | job_id=%s payload_keys=%s",
            job_id,
            sorted(str(k) for k in data.keys()),
        )

    started = {
        "status": "running",
        "event": "partial_category_started",
        "job_id": job_id,
        "account_name": db_name,
        "total_count": 0,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
        "field_save_counts": _field_save_counts(0),
        "h3": "분류 후보정",
        "message": "분류 후보정을 시작했습니다.",
    }
    if job_id:
        await update_state_only(job_id=job_id, account_name=db_name, payload=started)
        await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=started)

    if not chat_bot_id:
        raise ValueError("chat_bot_id is required")
    sub_cate_mode = await get_sub_cate_mode_from_config(chat_bot_id, dbname=db_name)

    learn_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    if not learn_table:
        raise RuntimeError("LEARN_LIST table not found")
    columns = await _get_columns(db_name, learn_table)
    logger.info(
        "[PartialCategory][Table] job_id=%s db=%s chat_bot_id=%s learn_table=%s columns=%s sub_cate_mode=%s contents_url=%s",
        job_id,
        db_name,
        chat_bot_id,
        learn_table,
        sorted(columns),
        sub_cate_mode,
        contents_url,
    )
    if "id" not in columns or "cate1" not in columns:
        raise RuntimeError(f"{learn_table} requires id and cate1 columns")

    selected_category_table = _category_table_name_from_chat_bot_id(chat_bot_id) or ""
    selected_table_source = "get_category_url_pattern_raw"
    category_obj: Optional[Dict[str, Any]] = None
    try:
        raw_category_obj = await get_category_url_pattern_raw(
            chat_bot_id,
            "period",
            db_name,
            contents_url=contents_url,
            require_nonempty_rules=True,
        )
        if isinstance(raw_category_obj, str) and raw_category_obj.strip():
            category_obj = json.loads(raw_category_obj)
        elif isinstance(raw_category_obj, dict):
            category_obj = raw_category_obj
    except Exception as ex:
        logger.warning(
            "[PartialCategory] category rules load failed via crawling flow | job_id=%s db=%s chat_bot_id=%s contents_url=%s err=%s",
            job_id,
            db_name,
            chat_bot_id,
            contents_url,
            ex,
        )
        category_obj = None

    rule_obj: Dict[str, Any] = category_obj or {"mode": "rule", "rules": []}
    all_rules: List[Any] = _get_rule_entries(rule_obj)
    filtered_obj: Dict[str, Any] = _filtered_category_object(rule_obj, target_filter)
    rules: List[Any] = _get_rule_entries(filtered_obj)
    table_attempts: List[Dict[str, Any]] = [
        {
            "table": selected_category_table,
            "source": selected_table_source,
            "obj_present": bool(category_obj),
            "all_rules": len(all_rules),
            "filtered_rules": len(rules),
            "sample": rules[:3],
        }
    ]
    logger.info(
        "[PartialCategory] category rules loaded | job_id=%s db=%s category_table=%s table_source=%s attempts=%s rules=%s filtered_rules=%s apply_mode=%s target_filter=%s contents_url=%s date_source=%s rule_sample=%s",
        job_id,
        db_name,
        selected_category_table,
        selected_table_source,
        table_attempts,
        len(all_rules),
        len(rules),
        "all_rules" if not target_filter else "filtered_rules",
        target_filter,
        contents_url,
        date_source,
        rules[:5],
    )
    if not rules:
        logger.warning(
            "[PartialCategory][NoRules] job_id=%s db=%s category_table=%s chat_bot_id=%s contents_url=%s category_obj_present=%s all_rules=%s target_filter=%s",
            job_id,
            db_name,
            selected_category_table,
            chat_bot_id,
            contents_url,
            bool(category_obj),
            len(all_rules),
            target_filter,
        )
    await _log_category_table_raw_code_probe(
        db_name=db_name,
        job_id=job_id,
        category_table=selected_category_table,
    )

    select_cols = ["id", "cate1"]
    for col in ("cate2", "content", "content_type", "content_created_at", "created_at"):
        if col in columns and col not in select_cols:
            select_cols.append(col)

    conditions = []
    if "content_type" in columns:
        conditions.append("LOWER(COALESCE(`content_type`, '')) = 'url'")
    if "type" in columns:
        conditions.append("LOWER(COALESCE(`type`, '')) <> 'file'")
    if start_date and end_date:
        date_col = "created_at" if "created_at" in columns else ("content_created_at" if "content_created_at" in columns else "")
        if date_col:
            conditions.append(f"DATE(`{date_col}`) BETWEEN %s AND %s")
    filter_sql, filter_params = _target_filter_sql_condition(target_filter) if "content" in columns else ("", [])
    if filter_sql:
        conditions.append(filter_sql)
    where_sql = " AND ".join(conditions) if conditions else "1=1"
    params: List[Any] = []
    if start_date and end_date and ("content_created_at" in columns or "created_at" in columns):
        params.extend([start_date, end_date])
    params.extend(filter_params)

    page_size = 1000
    last_id = 0
    scanned = 0
    matched = 0
    updated = 0
    samples: List[Dict[str, Any]] = []
    debug_counters = {
        "target_filter_skipped": 0,
        "no_row_url": 0,
        "no_rule_match": 0,
        "empty_resolved_code": 0,
        "unchanged": 0,
        "update_attempts": 0,
    }
    debug_samples: List[Dict[str, Any]] = []
    logger.info(
        "[PartialCategory][Select] job_id=%s table=%s select_cols=%s where=%s params=%s",
        job_id,
        learn_table,
        select_cols,
        where_sql,
        params,
    )
    progress_total = 0
    try:
        count_rows = await maria_execute_query(
            f"SELECT COUNT(*) AS cnt FROM `{learn_table}` WHERE {where_sql}",
            tuple(params),
            fetch=True,
            dbname=db_name,
        )
        progress_total = int(((count_rows or [{}])[0] or {}).get("cnt") or 0)
    except Exception as ex:
        logger.warning(
            "[PartialCategory][ProgressCount] failed | job_id=%s table=%s where=%s err=%s",
            job_id,
            learn_table,
            where_sql,
            ex,
        )
    if job_id:
        await send_message_to_redis_sse(
            job_id=job_id,
            dbname=db_name,
            message={
                "status": "running",
                "event": "partial_category_progress",
                "job_id": job_id,
                "account_name": db_name,
                "total_count": 0,
                "scan_count": 0,
                "collection_count": 0,
                "save_count": 0,
                "study_count": 0,
                "inspected_count": 0,
                "candidate_count": progress_total,
                "field_save_counts": _field_save_counts(0),
                "source": "partial_category_postprocess",
                "message": f"분류 보정 대상 {progress_total}건 확인",
            },
        )
    await _log_rule_token_row_probe(
        db_name=db_name,
        job_id=job_id,
        learn_table=learn_table,
        columns=columns,
        where_sql=where_sql,
        params=params,
        rules=rules,
    )

    async def _send_category_progress(reason: str = "") -> None:
        if not job_id:
            return
        await send_message_to_redis_sse(
            job_id=job_id,
            dbname=db_name,
            message={
                "status": "running",
                "event": "partial_category_progress",
                "job_id": job_id,
                "account_name": db_name,
                "total_count": matched,
                "scan_count": matched,
                "collection_count": matched,
                "save_count": updated,
                "study_count": 0,
                "inspected_count": scanned,
                "candidate_count": progress_total,
                "field_save_counts": _field_save_counts(updated),
                "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
                "partial_sequence_running": data.get("_partial_sequence_running"),
                "source": "partial_category_postprocess",
                "progress_reason": reason,
                "message": f"분류 보정 진행 중 {scanned}/{progress_total or scanned}",
            },
        )

    while True:
        rows = await maria_execute_query(
            f"""
            SELECT {', '.join(f'`{c}`' for c in select_cols)}
            FROM `{learn_table}`
            WHERE {where_sql}
              AND `id` > %s
            ORDER BY `id` ASC
            LIMIT {page_size}
            """,
            tuple(params + [last_id]),
            fetch=True,
            dbname=db_name,
        )
        if not rows:
            logger.info(
                "[PartialCategory][Page] job_id=%s last_id=%s rows=0 scanned=%s matched=%s updated=%s counters=%s",
                job_id,
                last_id,
                scanned,
                matched,
                updated,
                debug_counters,
            )
            break
        logger.info(
            "[PartialCategory][Page] job_id=%s last_id=%s rows=%s",
            job_id,
            last_id,
            len(rows),
        )
        for row in rows:
            row = row or {}
            try:
                row_id = int(row.get("id") or 0)
            except Exception:
                row_id = 0
            if row_id > last_id:
                last_id = row_id
            scanned += 1
            if not _target_filter_matches_row(row, target_filter):
                debug_counters["target_filter_skipped"] += 1
                if len(debug_samples) < 10:
                    debug_samples.append({"id": row_id, "reason": "target_filter_skipped", "content": _row_url_values(row)[:2]})
                await _send_category_progress("target_filter_skipped")
                continue

            resolved: Optional[Tuple[str, str]] = None
            resolved_url = ""
            row_urls = _row_url_values(row)
            if not row_urls:
                debug_counters["no_row_url"] += 1
                if len(debug_samples) < 10:
                    debug_samples.append({"id": row_id, "reason": "no_row_url"})
                await _send_category_progress("no_row_url")
                continue
            for url_value in row_urls:
                resolved = resolve_cate_for_detail_url(url_value, filtered_obj)
                if resolved:
                    resolved_url = url_value
                    break
            if not resolved:
                debug_counters["no_rule_match"] += 1
                if len(debug_samples) < 10:
                    debug_samples.append({"id": row_id, "reason": "no_rule_match", "content": row_urls[:2]})
                await _send_category_progress("no_rule_match")
                continue
            new_cate1, new_cate2 = str(resolved[0] or "").strip(), str(resolved[1] or "").strip()
            if not new_cate1 and not new_cate2:
                debug_counters["empty_resolved_code"] += 1
                if len(debug_samples) < 10:
                    debug_samples.append({"id": row_id, "reason": "empty_resolved_code", "content": resolved_url})
                await _send_category_progress("empty_resolved_code")
                continue
            matched += 1
            current_cate1 = str(row.get("cate1") or "").strip()
            current_cate2 = str(row.get("cate2") or "").strip() if "cate2" in columns else ""
            target_cate1, target_cate2 = merge_category_pair(
                sub_cate_mode,
                current_cate1,
                current_cate2,
                new_cate1,
                new_cate2,
                has_cate2="cate2" in columns,
            )

            if current_cate1 == target_cate1 and ("cate2" not in columns or current_cate2 == target_cate2):
                debug_counters["unchanged"] += 1
                if len(debug_samples) < 10:
                    debug_samples.append({
                        "id": row_id,
                        "reason": "unchanged",
                        "content": _url_key(resolved_url)[:180],
                        "current": [current_cate1, current_cate2],
                        "resolved": [new_cate1, new_cate2],
                    })
                await _send_category_progress("unchanged")
                continue
            debug_counters["update_attempts"] += 1
            if "cate2" in columns:
                affected = await maria_execute_query(
                    f"UPDATE `{learn_table}` SET `cate1` = %s, `cate2` = %s WHERE `id` = %s",
                    (target_cate1, target_cate2, row_id),
                    fetch=False,
                    dbname=db_name,
                )
            else:
                affected = await maria_execute_query(
                    f"UPDATE `{learn_table}` SET `cate1` = %s WHERE `id` = %s",
                    (target_cate1, row_id),
                    fetch=False,
                    dbname=db_name,
                )
            try:
                updated += int(affected or 0)
            except Exception:
                updated += 1
            logger.info(
                "[PartialCategory][Update] job_id=%s row_id=%s affected=%s url=%s before=%s resolved=%s target=%s",
                job_id,
                row_id,
                affected,
                _url_key(resolved_url)[:220],
                [current_cate1, current_cate2],
                [new_cate1, new_cate2],
                [target_cate1, target_cate2],
            )
            if len(samples) < 5:
                samples.append(
                    {
                        "id": row_id,
                        "url": _url_key(resolved_url)[:180],
                        "before": [current_cate1, current_cate2],
                        "after": [target_cate1, target_cate2],
                    }
                )
            if job_id:
                await send_message_to_redis_sse(
                    job_id=job_id,
                    dbname=db_name,
                    message={
                        "status": "running",
                        "event": "partial_category_progress",
                        "job_id": job_id,
                        "account_name": db_name,
                        "total_count": matched,
                        "scan_count": matched,
                        "collection_count": matched,
                        "save_count": updated,
                        "study_count": 0,
                        "inspected_count": scanned,
                        "candidate_count": progress_total,
                        "field_save_counts": _field_save_counts(updated),
                        "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
                        "partial_sequence_running": data.get("_partial_sequence_running"),
                        "source": "partial_category_postprocess",
                        "message": f"분류 보정 진행 중 {scanned}/{progress_total or scanned}",
                    },
                )
        if len(rows) < page_size:
            break
    logger.info(
        "[PartialCategory][SummaryDebug] job_id=%s scanned=%s matched=%s updated=%s counters=%s samples=%s",
        job_id,
        scanned,
        matched,
        updated,
        debug_counters,
        debug_samples,
    )

    result = {
        "status": "completed",
        "event": "workflow_completed",
        "job_id": job_id,
        "account_name": db_name,
        "total_count": matched,
        "scan_count": matched,
        "collection_count": matched,
        "save_count": updated,
        "study_count": 0,
        "updated_count": updated,
        "matched_count": matched,
        "inspected_count": scanned,
        "candidate_count": progress_total,
        "debug_counters": debug_counters,
        "debug_samples": debug_samples,
        "field_save_counts": _field_save_counts(updated),
        "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
        "partial_sequence_running": data.get("_partial_sequence_running"),
        "category_code_source": selected_category_table,
        "category_table_source": selected_table_source,
        "category_table_attempts": table_attempts,
        "category_apply_mode": "url_query_rule_code",
        "rule_count": len(rules),
        "learn_list_table": learn_table,
        "target_date": [start_date, end_date] if start_date and end_date else None,
        "date_source": date_source,
        "samples": samples,
        "source": "partial_category_postprocess",
        "message": f"분류 후보정 완료: {updated}건 업데이트",
    }
    if job_id:
        await update_crawling_log_counters(
            job_id=job_id,
            status="completed",
            scan=matched,
            collection=matched,
            saved=updated,
            study=0,
            dbname=db_name,
        )
        if not suppress_terminal_sse:
            await update_state_only(job_id=job_id, account_name=db_name, payload=result)
            await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=result)
    logger.info(
        "[PartialCategory] completed | job_id=%s db=%s table=%s scanned=%s matched=%s updated=%s rules=%s target_date=%s filter=%s samples=%s",
        job_id,
        db_name,
        learn_table,
        scanned,
        matched,
        updated,
        len(rules),
        result.get("target_date"),
        target_filter,
        samples,
    )
    return result
