import logging
import os
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlparse

from backend.shared.pre_explored_url import (
    _build_exploration_rule_sql_condition,
    _get_rule_entries,
    _load_category_url_pattern_object,
    _resolve_preexplored_scope,
    _sql_single_quoted_literal,
    get_url_rule_filters,
    resolve_cate_for_detail_url,
)
from backend.shared.redis_sse_service import send_message_to_redis_sse, update_state_only
from backend.shared.url_scope import build_sql_scope_condition
from db.crawl_db_manager import update_crawling_log_counters
from db.maria_operations import maria_execute_query
from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot
from utils.db_name import resolve_db_name
from utils.url import canonicalize_url_for_dedup, ensure_url_scheme

logger = logging.getLogger("backend.shared.type_postprocess")

_EXPLORATION_TABLE = "ASADAL_CRAWLING_EXPLORATION"
_DEFAULT_BOARD_PARAM_KEYS = (
    "menuNo",
    "mid",
    "bbsno",
    "bbs",
    "bbsNo",
    "bbsId",
    "nttId",
    "nttNo",
    "boardNo",
    "articleNo",
    "q_sn",
    "q_bbscttSn",
)


def is_type_postprocess_request(data: Dict[str, Any]) -> bool:
    mode = str((data or {}).get("crawl_mode") or "").strip().lower()
    if mode == "type_postprocess":
        return True
    raw_enabled = (data or {}).get("type_postprocess_enabled")
    if isinstance(raw_enabled, bool):
        return raw_enabled
    if str(raw_enabled or "").strip().lower() in {"1", "true", "yes", "on"}:
        return True
    fields = _partial_update_fields(data or {})
    return (
        str((data or {}).get("colle") or "").strip().lower() == "content"
        and "type" in fields
        and not bool(fields & {"title", "content", "cate", "symmary", "summary"})
    )


def _partial_update_fields(data: Dict[str, Any]) -> Set[str]:
    fields = (data or {}).get("partial_update_fields")
    if isinstance(fields, str):
        fields = [fields]
    if not isinstance(fields, list):
        return set()
    return {str(item or "").strip().lower() for item in fields if str(item or "").strip()}


def _list_from_filter(value: Any) -> List[str]:
    if isinstance(value, list):
        raw_items = value
    elif value in (None, ""):
        raw_items = []
    else:
        raw_items = [value]
    out: List[str] = []
    for item in raw_items:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _partial_target_terms(data: Dict[str, Any]) -> Tuple[List[str], str]:
    raw_filter = (data or {}).get("partial_target_filter")
    if not isinstance(raw_filter, dict):
        return [], "any"
    terms = _list_from_filter(raw_filter.get("url_contains")) + _list_from_filter(raw_filter.get("query_contains"))
    match_mode = str(raw_filter.get("match_mode") or "any").strip().lower()
    if match_mode not in {"any", "all"}:
        match_mode = "any"
    seen: Set[str] = set()
    unique: List[str] = []
    for term in terms:
        lowered = term.lower()
        if lowered and lowered not in seen:
            seen.add(lowered)
            unique.append(term)
    return unique, match_mode


def _partial_target_sql_condition(column_name: str, data: Dict[str, Any]) -> Tuple[str, List[Any]]:
    terms, match_mode = _partial_target_terms(data)
    if not terms:
        return "", []
    pieces: List[str] = []
    params: List[Any] = []
    for term in terms:
        pieces.append(f"LOWER(CAST(`{column_name}` AS CHAR)) LIKE %s")
        params.append(f"%{term.lower()}%")
    joiner = " AND " if match_mode == "all" else " OR "
    return "(" + joiner.join(pieces) + ")", params


def _bool_from_payload(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _normalize_param_keys(raw: Any) -> List[str]:
    items = raw if isinstance(raw, list) else []
    items = [*list(_DEFAULT_BOARD_PARAM_KEYS), *items]
    out: List[str] = []
    for item in items:
        key = str(item or "").strip().lower()
        if key and key not in out:
            out.append(key)
    return out


def _has_board_param(url: str, param_keys: List[str]) -> bool:
    try:
        parsed = urlparse(ensure_url_scheme(str(url or "").strip()))
        pairs = parse_qsl(parsed.query or "", keep_blank_values=True)
    except Exception:
        pairs = []
    if not pairs:
        return False
    key_set = {str(key or "").strip().lower() for key in param_keys if str(key or "").strip()}
    return any(str(key or "").strip().lower() in key_set for key, _ in pairs)


async def _get_table_columns_lower(db_name: str, table_name: str) -> set[str]:
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
    columns: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        name = str(row.get("column_name") or "").strip().lower()
        if name:
            columns.add(name)
    return columns


def _iter_chunks(items: List[int], size: int = 500):
    chunk_size = max(1, int(size or 500))
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def _url_key(url: Any) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    normalized = ensure_url_scheme(raw)
    return (canonicalize_url_for_dedup(normalized) or normalized).strip()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _should_seed_type_as_post(url: str, param_keys: List[str]) -> bool:
    text = str(url or "").strip()
    if not text:
        return False
    try:
        parsed = urlparse(ensure_url_scheme(text))
        path = (parsed.path or "").lower()
        pairs = parse_qsl(parsed.query or "", keep_blank_values=True)
    except Exception:
        return False

    keys = {str(k or "").strip().lower() for k, _ in pairs if str(k or "").strip()}
    if not keys:
        return False

    configured = {str(k or "").strip().lower() for k in param_keys if str(k or "").strip()}
    strong_keys = {
        "bbsno",
        "bbs",
        "bbsid",
        "discussionid",
        "nttid",
        "nttno",
        "boardno",
        "articleno",
        "q_sn",
        "q_bbscttsn",
    }
    if keys & strong_keys:
        logger.debug(
            "[FileUrlTrace][type_postprocess.post_detect] strong query key matched | url=%s keys=%s matched=%s",
            text[:300],
            sorted(keys),
            sorted(keys & strong_keys),
        )
        return True

    path_hints = ("bbs", "board", "view", "detail", "select", "read")
    if keys & configured and any(hint in path for hint in path_hints):
        logger.debug(
            "[FileUrlTrace][type_postprocess.post_detect] configured query key matched | url=%s path=%s keys=%s configured=%s",
            text[:300],
            path,
            sorted(keys),
            sorted(keys & configured),
        )
        return True
    if "bd_pblcdiscussionview.do" in path:
        logger.warning(
            "[FileUrlTrace][type_postprocess.query_missing_or_unmatched] NE discussion detail URL not typed as post | url=%s path=%s keys=%s required=discussionId",
            text[:300],
            path,
            sorted(keys),
        )
    return False


def _learn_url_column(columns: Set[str]) -> str:
    return "content" if "content" in columns else ""


async def _load_existing_exploration_url_keys(
    *,
    db_name: str,
    chat_bot_id: str,
    learn_list_id: Optional[int],
    columns: Set[str],
    scope_condition: str,
    max_rows: int,
) -> Set[str]:
    if "url" not in columns:
        return set()
    conditions = ["`url` IS NOT NULL", "TRIM(CAST(`url` AS CHAR)) <> ''"]
    params: List[Any] = []
    if chat_bot_id and "chat_bot_id" in columns:
        conditions.append("chat_bot_id = %s")
        params.append(chat_bot_id)
    if learn_list_id is not None and "learn_list_id" in columns:
        conditions.append("learn_list_id = %s")
        params.append(learn_list_id)
    if "is_active" in columns:
        conditions.append("COALESCE(is_active, 0) = 1")
    if "merge_status" in columns:
        conditions.append("COALESCE(LOWER(merge_status), '') <> 'duplicate'")
    if scope_condition:
        conditions.append(scope_condition)
    where_sql = " AND ".join(conditions)
    page_size = max(100, min(int(os.getenv("TYPE_POSTPROCESS_EXISTING_PAGE_SIZE", "2000") or "2000"), 10000))
    limit_total = max(1, int(max_rows or 100000))
    last_id = 0
    loaded = 0
    keys: Set[str] = set()

    while loaded < limit_total:
        limit = min(page_size, limit_total - loaded)
        rows = await maria_execute_query(
            f"""
            SELECT id, url
            FROM `{_EXPLORATION_TABLE}`
            WHERE {where_sql}
              AND id > %s
            ORDER BY id ASC
            LIMIT {limit}
            """,
            tuple(params + [last_id]),
            fetch=True,
            dbname=db_name,
        )
        if not rows:
            break
        for row in rows:
            row_id = _safe_int((row or {}).get("id"), 0)
            if row_id > last_id:
                last_id = row_id
            loaded += 1
            key = _url_key((row or {}).get("url"))
            if key:
                keys.add(key)
        if len(rows) < limit:
            break
    return keys


async def _seed_missing_learn_list_urls_to_exploration(
    *,
    data: Dict[str, Any],
    db_name: str,
    chat_bot_id: str,
    job_id: str,
    learn_list_id: Optional[int],
    exploration_columns: Set[str],
    scope_condition: str,
    param_keys: List[str],
) -> Dict[str, Any]:
    if not chat_bot_id or "url" not in exploration_columns:
        return {
            "learn_list_table": "",
            "learn_scanned": 0,
            "learn_inserted": 0,
            "learn_post_inserted": 0,
            "learn_type_applied": 0,
        }

    learn_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    learn_columns = await _get_table_columns_lower(db_name, learn_table) if learn_table else set()
    url_col = _learn_url_column(learn_columns)
    if not learn_table or not url_col:
        return {
            "learn_list_table": learn_table or "",
            "learn_scanned": 0,
            "learn_inserted": 0,
            "learn_post_inserted": 0,
            "learn_type_applied": 0,
        }

    logger.info(
        "[TypePostprocess][seed] start | job_id=%s db=%s chat_bot_id=%s learn_table=%s learn_list_id=%s",
        job_id,
        db_name,
        chat_bot_id,
        learn_table,
        learn_list_id,
    )

    max_scan = max(1, int(os.getenv("TYPE_POSTPROCESS_LEARN_LIST_MAX_SCAN_ROWS", "100000") or "100000"))
    existing_keys = await _load_existing_exploration_url_keys(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        learn_list_id=learn_list_id,
        columns=exploration_columns,
        scope_condition=scope_condition,
        max_rows=max_scan,
    )

    learn_conditions = [f"`{url_col}` IS NOT NULL", f"TRIM(CAST(`{url_col}` AS CHAR)) <> ''"]
    learn_params: List[Any] = []
    if "content_type" in learn_columns:
        learn_conditions.append("LOWER(COALESCE(`content_type`, '')) = 'url'")
    if "status" in learn_columns:
        desired_status = str(data.get("type_postprocess_learn_list_status") or os.getenv("TYPE_POSTPROCESS_LEARN_LIST_STATUS", "Y")).strip()
        if desired_status:
            learn_conditions.append("UPPER(COALESCE(`status`, '')) = %s")
            learn_params.append(desired_status.upper())
    learn_scope = build_sql_scope_condition(url_col, data.get("target_domains") if isinstance(data.get("target_domains"), list) else None)
    if learn_scope:
        learn_conditions.append(learn_scope)
    partial_condition, partial_params = _partial_target_sql_condition(url_col, data)
    if partial_condition:
        learn_conditions.append(partial_condition)
        learn_params.extend(partial_params)
    learn_where = " AND ".join(learn_conditions)

    insertable = [
        "chat_bot_id",
        "learn_list_id",
        "url",
        "created_at",
        "status",
        "job_id",
        "type",
        "is_active",
        "merge_status",
        "merge_source",
        "crawl_state",
        "suspect_count",
        "change_state",
        "discovery_state",
    ]
    insert_cols = [col for col in insertable if col in exploration_columns]
    if "url" not in insert_cols:
        return {
            "learn_list_table": learn_table,
            "learn_scanned": 0,
            "learn_inserted": 0,
            "learn_post_inserted": 0,
            "learn_type_applied": 0,
        }

    page_size = max(100, min(int(os.getenv("TYPE_POSTPROCESS_LEARN_LIST_PAGE_SIZE", "1000") or "1000"), 5000))
    scanned = 0
    inserted = 0
    inserted_new = 0
    restored_existing = 0
    refreshed_existing = 0
    post_inserted = 0
    type_applied = 0
    type_unchanged = 0
    duplicate_skipped = 0
    last_id = 0
    samples: List[str] = []
    inserted_samples: List[str] = []
    restored_samples: List[Dict[str, Any]] = []
    refreshed_samples: List[str] = []
    sample_limit = max(1, min(int(os.getenv("TYPE_POSTPROCESS_SEED_SAMPLE_LIMIT", "20") or "20"), 200))

    while scanned < max_scan:
        limit = min(page_size, max_scan - scanned)
        rows = await maria_execute_query(
            f"""
            SELECT `id`, `{url_col}` AS url
            FROM `{learn_table}`
            WHERE {learn_where}
              AND id > %s
            ORDER BY id ASC
            LIMIT {limit}
            """,
            tuple(learn_params + [last_id]),
            fetch=True,
            dbname=db_name,
        )
        if not rows:
            break
        for row in rows:
            row_id = _safe_int((row or {}).get("id"), 0)
            if row_id > last_id:
                last_id = row_id
            scanned += 1
            raw_url = str((row or {}).get("url") or "").strip()
            if not raw_url:
                continue
            # Keep the URL used for DB/request paths separate from the canonical key.
            request_url = ensure_url_scheme(raw_url)
            if raw_url != request_url:
                logger.debug(
                    "[FileUrlTrace][type_postprocess.request_url_prepared] job_id=%s raw_url=%s request_url=%s",
                    job_id,
                    raw_url[:300],
                    request_url[:300],
                )
            normalized_key = _url_key(request_url)
            if not normalized_key:
                duplicate_skipped += 1
                logger.debug(
                    "[FileUrlTrace][type_postprocess.skip_empty_key] job_id=%s raw_url=%s request_url=%s",
                    job_id,
                    raw_url[:300],
                    request_url[:300],
                )
                continue

            existing_row: Optional[Dict[str, Any]] = None
            existing_rows = await maria_execute_query(
                f"""
                SELECT id, url, learn_list_id, is_active, type, status, merge_status
                FROM `{_EXPLORATION_TABLE}`
                WHERE chat_bot_id = %s
                  AND url = %s
                LIMIT 1
                """,
                (chat_bot_id, request_url),
                fetch=True,
                dbname=db_name,
            )
            if existing_rows and isinstance(existing_rows[0], dict):
                existing_row = existing_rows[0]

            inferred_type = "post" if _should_seed_type_as_post(request_url, param_keys) else ""
            if "BD_pblcDiscussionView.do" in request_url or "bd_pblcdiscussionview.do" in request_url.lower():
                logger.warning(
                    "[FileUrlTrace][type_postprocess.ne_discussion_url] job_id=%s inferred_type=%s raw_url=%s request_url=%s normalized_key=%s",
                    job_id,
                    inferred_type or "-",
                    raw_url[:300],
                    request_url[:300],
                    normalized_key[:300],
                )
            values_by_col: Dict[str, Any] = {
                "chat_bot_id": chat_bot_id,
                "learn_list_id": learn_list_id,
                "url": request_url,
                "created_at": None,
                "status": "N",
                "job_id": job_id,
                "type": inferred_type,
                "is_active": 1,
                "merge_status": "none",
                "merge_source": "auto",
                "crawl_state": "normal",
                "suspect_count": 0,
                "change_state": "none",
                "discovery_state": "known",
            }
            sql_cols = [col for col in insert_cols if not (col == "learn_list_id" and learn_list_id is None)]
            placeholders = ["NOW(3)" if col == "created_at" else "%s" for col in sql_cols]
            params = [values_by_col[col] for col in sql_cols if col != "created_at"]
            update_cols = [
                col
                for col in sql_cols
                if col not in {"chat_bot_id", "url", "created_at"}
            ]
            update_assignments = []
            for col in update_cols:
                if col == "type":
                    update_assignments.append("`type` = CASE WHEN COALESCE(TRIM(CAST(`type` AS CHAR)), '') = '' THEN VALUES(`type`) ELSE `type` END")
                else:
                    update_assignments.append(f"`{col}` = VALUES(`{col}`)")
            update_clause = ", ".join(update_assignments)
            if not update_clause:
                update_clause = "`url` = VALUES(`url`)"
            from backend.shared.db_write_queue import run_db_write

            affected = await run_db_write(
                "postprocess.type_seed_upsert",
                lambda: maria_execute_query(
                    f"""
                    INSERT INTO `{_EXPLORATION_TABLE}`
                    ({', '.join(f'`{col}`' for col in sql_cols)})
                    VALUES ({', '.join(placeholders)})
                    ON DUPLICATE KEY UPDATE {update_clause}
                    """,
                    tuple(params),
                    fetch=False,
                    dbname=db_name,
                ),
            )
            existing_keys.add(normalized_key)
            inserted += 1
            type_will_be_applied = False
            if existing_row is None:
                inserted_new += 1
                type_will_be_applied = bool(inferred_type)
                if len(inserted_samples) < sample_limit:
                    inserted_samples.append(request_url[:240])
            else:
                before_active = _safe_int(existing_row.get("is_active"), 0)
                before_learn_list_id = _safe_int(existing_row.get("learn_list_id"), 0) or None
                before_type = str(existing_row.get("type") or "").strip()
                before_status = str(existing_row.get("status") or "").strip()
                before_merge_status = str(existing_row.get("merge_status") or "").strip()
                type_will_be_applied = bool(inferred_type and not before_type)
                changed_for_restore = (
                    before_active != 1
                    or before_learn_list_id != learn_list_id
                    or (not before_type and bool(inferred_type))
                    or before_status != "N"
                    or before_merge_status.lower() != "none"
                )
                if changed_for_restore:
                    restored_existing += 1
                    if len(restored_samples) < sample_limit:
                        restored_samples.append({
                            "id": _safe_int(existing_row.get("id"), 0),
                            "url": request_url[:240],
                            "before": {
                                "learn_list_id": before_learn_list_id,
                                "is_active": before_active,
                                "type": before_type,
                                "status": before_status,
                                "merge_status": before_merge_status,
                            },
                            "after": {
                                "learn_list_id": learn_list_id,
                                "is_active": 1,
                                "type": inferred_type,
                                "status": "N",
                                "merge_status": "none",
                            },
                        })
                else:
                    refreshed_existing += 1
                    if len(refreshed_samples) < sample_limit:
                        refreshed_samples.append(request_url[:240])
            if inferred_type == "post":
                post_inserted += 1
            if type_will_be_applied:
                type_applied += 1
            elif inferred_type:
                type_unchanged += 1
            if len(samples) < 5:
                samples.append(request_url[:240])
        if len(rows) < limit:
            break

    logger.info(
        "[TypePostprocess][seed] done | job_id=%s db=%s learn_table=%s scanned=%s upserted=%s inserted_new=%s restored_existing=%s refreshed_existing=%s post_candidates=%s type_applied=%s type_unchanged=%s duplicate_skipped=%s partial_filter=%s samples=%s",
        job_id,
        db_name,
        learn_table,
        scanned,
        inserted,
        inserted_new,
        restored_existing,
        refreshed_existing,
        post_inserted,
        type_applied,
        type_unchanged,
        duplicate_skipped,
        data.get("partial_target_filter") if isinstance(data.get("partial_target_filter"), dict) else {},
        samples,
    )
    logger.info(
        "[TypePostprocess][seed] changed_urls | job_id=%s inserted_new_samples=%s restored_samples=%s refreshed_samples=%s",
        job_id,
        inserted_samples,
        restored_samples,
        refreshed_samples,
    )

    verify_count = 0
    verify_samples: List[Dict[str, Any]] = []
    verify_by_learn_join = 0
    try:
        if learn_list_id is not None and "learn_list_id" in exploration_columns:
            verify_rows = await maria_execute_query(
                f"""
                SELECT COUNT(*) AS cnt
                FROM `{_EXPLORATION_TABLE}`
                WHERE chat_bot_id = %s
                  AND learn_list_id = %s
                  AND COALESCE(is_active, 0) = 1
                """,
                (chat_bot_id, learn_list_id),
                fetch=True,
                dbname=db_name,
            )
            if verify_rows:
                verify_count = _safe_int((verify_rows[0] or {}).get("cnt"), 0)
            verify_sample_rows = await maria_execute_query(
                f"""
                SELECT id, learn_list_id, url, type, status, is_active, merge_status
                FROM `{_EXPLORATION_TABLE}`
                WHERE chat_bot_id = %s
                  AND learn_list_id = %s
                  AND COALESCE(is_active, 0) = 1
                ORDER BY id DESC
                LIMIT 10
                """,
                (chat_bot_id, learn_list_id),
                fetch=True,
                dbname=db_name,
            )
            for item in verify_sample_rows or []:
                if isinstance(item, dict):
                    verify_samples.append({
                        "id": _safe_int(item.get("id"), 0),
                        "learn_list_id": _safe_int(item.get("learn_list_id"), 0),
                        "url": str(item.get("url") or "")[:240],
                        "type": str(item.get("type") or ""),
                        "status": str(item.get("status") or ""),
                        "is_active": _safe_int(item.get("is_active"), 0),
                        "merge_status": str(item.get("merge_status") or ""),
                    })
        verify_learn_conditions = [f"l.`{url_col}` IS NOT NULL", f"TRIM(CAST(l.`{url_col}` AS CHAR)) <> ''"]
        verify_learn_params: List[Any] = []
        if "content_type" in learn_columns:
            verify_learn_conditions.append("LOWER(COALESCE(l.`content_type`, '')) = 'url'")
        if "status" in learn_columns:
            desired_status = str(data.get("type_postprocess_learn_list_status") or os.getenv("TYPE_POSTPROCESS_LEARN_LIST_STATUS", "Y")).strip()
            if desired_status:
                verify_learn_conditions.append("UPPER(COALESCE(l.`status`, '')) = %s")
                verify_learn_params.append(desired_status.upper())
        verify_join_rows = await maria_execute_query(
            f"""
            SELECT COUNT(*) AS cnt
            FROM `{_EXPLORATION_TABLE}` e
            INNER JOIN `{learn_table}` l
              ON e.url = l.`{url_col}`
            WHERE e.chat_bot_id = %s
              AND COALESCE(e.is_active, 0) = 1
              AND {" AND ".join(verify_learn_conditions)}
            """,
            tuple([chat_bot_id] + verify_learn_params),
            fetch=True,
            dbname=db_name,
        )
        if verify_join_rows:
            verify_by_learn_join = _safe_int((verify_join_rows[0] or {}).get("cnt"), 0)
    except Exception as verify_exc:
        logger.warning(
            "[TypePostprocess][seed] verify failed | job_id=%s db=%s learn_table=%s err=%s",
            job_id,
            db_name,
            learn_table,
            verify_exc,
        )
    logger.info(
        "[TypePostprocess][seed] verify | job_id=%s db=%s table=%s learn_list_id=%s active_by_learn_list_id=%s active_joined_to_learn_list=%s samples=%s",
        job_id,
        db_name,
        _EXPLORATION_TABLE,
        learn_list_id,
        verify_count,
        verify_by_learn_join,
        verify_samples,
    )

    return {
        "learn_list_table": learn_table,
        "learn_scanned": scanned,
        "learn_inserted": inserted,
        "learn_inserted_new": inserted_new,
        "learn_restored_existing": restored_existing,
        "learn_refreshed_existing": refreshed_existing,
        "learn_post_inserted": post_inserted,
        "learn_type_applied": type_applied,
        "learn_type_unchanged": type_unchanged,
        "learn_duplicate_skipped": duplicate_skipped,
        "learn_insert_samples": samples,
        "learn_inserted_new_samples": inserted_samples,
        "learn_restored_samples": restored_samples,
        "learn_refreshed_samples": refreshed_samples,
    }


async def run_type_postprocess(data: Dict[str, Any]) -> Dict[str, Any]:
    db_name = resolve_db_name(data, default="dev_user") or "dev_user"
    job_id = str((data or {}).get("job_id") or "").strip()
    chat_bot_id = str((data or {}).get("chat_bot_id") or "").strip()
    suppress_terminal_sse = bool((data or {}).get("_suppress_terminal_sse"))
    contents = data.get("contents")
    contents_url = ""
    if isinstance(contents, list) and contents:
        contents_url = str(contents[0] or "").strip()
    elif isinstance(contents, str):
        contents_url = contents.strip()
    contents_url = str(data.get("contents_url") or data.get("target_url") or contents_url or "").strip()
    target = str(data.get("type_postprocess_target") or "post").strip().lower() or "post"
    param_keys = _normalize_param_keys(data.get("type_postprocess_board_param_keys"))
    request_learn_list_id = _safe_int((data or {}).get("learn_list_id"), 0) or None
    logger.info(
        "[TypePostprocess][StartRequest] job_id=%s db=%s chat_bot_id=%s fields=%s filter=%s contents_url=%s",
        job_id,
        db_name,
        chat_bot_id,
        sorted(_partial_update_fields(data or {})),
        data.get("partial_target_filter") if isinstance(data.get("partial_target_filter"), dict) else {},
        contents_url,
    )

    started_payload = {
        "status": "running",
        "event": "type_postprocess_started",
        "job_id": job_id,
        "account_name": db_name,
        "total_count": 0,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
        "field_save_counts": {
            "title": 0,
            "content": 0,
            "cate": 0,
            "symmary": 0,
            "type": 0,
            "url": 0,
            "web_de": 0,
        },
        "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
        "partial_sequence_running": data.get("_partial_sequence_running"),
        "source": "type_postprocess",
        "h3": "type 후보정",
        "message": "type 후보정 실행 중",
    }
    if job_id:
        await update_state_only(job_id=job_id, account_name=db_name, payload=started_payload)
        await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=started_payload)

    category_obj = await _load_category_url_pattern_object(
        chat_bot_id,
        db_name,
        contents_url=contents_url,
        require_nonempty_rules=True,
    )
    rule_count = len(_get_rule_entries(category_obj)) if isinstance(category_obj, dict) else 0
    if False and (not category_obj or rule_count <= 0):
        result = {
            "status": "completed",
            "event": "workflow_completed",
            "job_id": job_id,
            "account_name": db_name,
            "total_count": 0,
            "collection_count": 0,
            "save_count": 0,
            "study_count": 0,
            "updated_count": 0,
            "field_save_counts": {
                "title": 0,
                "content": 0,
                "cate": 0,
                "symmary": 0,
                "type": 0,
                "url": 0,
                "web_de": 0,
            },
            "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
            "partial_sequence_running": data.get("_partial_sequence_running"),
            "rule_count": 0,
            "message": "적용할 url/query 패턴이 없어 type 후보정을 건너뜁니다.",
            "source": "type_postprocess",
        }
        if job_id:
            await update_state_only(job_id=job_id, account_name=db_name, payload=result)
            if not suppress_terminal_sse:
                await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=result)
        return result

    columns = await _get_table_columns_lower(db_name, _EXPLORATION_TABLE)
    if "id" not in columns or "url" not in columns or "type" not in columns:
        raise RuntimeError(f"{_EXPLORATION_TABLE} requires id, url, type columns")

    url_rule_patterns = await get_url_rule_filters(chat_bot_id, db_name, contents_url=contents_url)
    final_domains, scope_path_prefix = _resolve_preexplored_scope(
        target_domains=data.get("target_domains") if isinstance(data.get("target_domains"), list) else None,
        contents_url=contents_url,
        use_rule_scope=True,
        rule_patterns=url_rule_patterns,
        explicit_path_prefix=data.get("scope_path_prefix"),
    )

    conditions = ["COALESCE(TRIM(CAST(`type` AS CHAR)), '') = ''"]
    if chat_bot_id:
        conditions.append(f"chat_bot_id = '{_sql_single_quoted_literal(chat_bot_id)}'")
    if "merge_status" in columns:
        conditions.append("COALESCE(LOWER(merge_status), '') <> 'duplicate'")
    if "is_active" in columns:
        conditions.append("COALESCE(is_active, 0) = 1")
    scope_condition = build_sql_scope_condition("url", final_domains, path_prefix=scope_path_prefix)
    if scope_condition:
        conditions.append(scope_condition)
    partial_exploration_condition, partial_exploration_params = _partial_target_sql_condition("url", data)
    if partial_exploration_condition:
        conditions.append(partial_exploration_condition)

    seed_missing_learn_urls = _bool_from_payload(
        data.get("type_postprocess_seed_missing_learn_urls"),
        default=str(os.getenv("TYPE_POSTPROCESS_SEED_MISSING_LEARN_URLS", "0")).strip().lower() in {"1", "true", "yes", "on", "y"},
    )
    if seed_missing_learn_urls:
        seed_stats = await _seed_missing_learn_list_urls_to_exploration(
            data=data,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            job_id=job_id,
            learn_list_id=request_learn_list_id,
            exploration_columns=columns,
            scope_condition=scope_condition,
            param_keys=param_keys,
        )
    else:
        seed_stats = {
            "learn_list_table": "",
            "learn_scanned": 0,
            "learn_inserted": 0,
            "learn_post_inserted": 0,
            "learn_type_applied": 0,
            "learn_type_unchanged": 0,
            "learn_duplicate_skipped": 0,
            "learn_insert_samples": [],
            "learn_seed_skipped": True,
        }

    if not category_obj or rule_count <= 0:
        inserted_count = int(seed_stats.get("learn_inserted") or 0)
        seed_type_applied = int(seed_stats.get("learn_type_applied") or 0)
        scanned_count = int(seed_stats.get("learn_scanned") or 0)
        result = {
            "status": "completed",
            "event": "workflow_completed",
            "job_id": job_id,
            "account_name": db_name,
            "total_count": scanned_count,
            "collection_count": seed_type_applied,
            "save_count": seed_type_applied,
            "study_count": 0,
            "updated_count": 0,
            "inserted_count": inserted_count,
            "field_save_counts": {
                "title": 0,
                "content": 0,
                "cate": 0,
                "symmary": 0,
                "type": seed_type_applied,
                "url": 0,
                "web_de": 0,
            },
            "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
            "partial_sequence_running": data.get("_partial_sequence_running"),
            "rule_count": 0,
            **seed_stats,
            "message": f"type 후보정 완료: exploration 누락 URL {inserted_count}건 보강, 신규 type 적용 {seed_type_applied}건입니다. 적용 가능한 url/query 패턴이 없어 기존 type 업데이트는 건너뛰었습니다.",
            "source": "type_postprocess",
        }
        if job_id:
            await update_crawling_log_counters(
                job_id=job_id,
                status="completed",
                scan=scanned_count,
                collection=seed_type_applied,
                saved=seed_type_applied,
                study=0,
                dbname=db_name,
            )
            if not suppress_terminal_sse:
                await update_state_only(job_id=job_id, account_name=db_name, payload=result)
                await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=result)
        return result

    sql_rule_condition, sql_rule_meta = _build_exploration_rule_sql_condition(category_obj, column_name="url")
    if sql_rule_condition:
        conditions.append(sql_rule_condition)

    base_where = " AND ".join(conditions)
    page_size = max(100, min(int(os.getenv("TYPE_POSTPROCESS_SCAN_PAGE_SIZE", "1000") or "1000"), 5000))
    max_scan = max(1, int(os.getenv("TYPE_POSTPROCESS_MAX_SCAN_ROWS", "50000") or "50000"))
    last_id = 0
    scanned = 0
    board_param_count = 0
    matched_count = 0
    matched_ids: List[int] = []
    matched_samples: List[str] = []

    while scanned < max_scan:
        limit = min(page_size, max_scan - scanned)
        rows = await maria_execute_query(
            f"""
            SELECT id, url
            FROM `{_EXPLORATION_TABLE}`
            WHERE {base_where}
              AND id > %s
            ORDER BY id ASC
            LIMIT {limit}
            """,
            tuple(partial_exploration_params + [last_id]),
            fetch=True,
            dbname=db_name,
        )
        if not rows:
            break
        for row in rows:
            try:
                row_id = int((row or {}).get("id") or 0)
            except Exception:
                row_id = 0
            if row_id > last_id:
                last_id = row_id
            raw_url = str((row or {}).get("url") or "").strip()
            if not raw_url:
                continue
            scanned += 1
            url = ensure_url_scheme(raw_url)
            if not _has_board_param(url, param_keys):
                continue
            board_param_count += 1
            if resolve_cate_for_detail_url(url, category_obj) is None:
                continue
            matched_count += 1
            if row_id:
                matched_ids.append(row_id)
                if len(matched_samples) < 5:
                    matched_samples.append(url[:240])
        if len(rows) < limit:
            break

    updated_count = 0
    for chunk in _iter_chunks(matched_ids, 500):
        placeholders = ", ".join(["%s"] * len(chunk))
        params = [target, *chunk]
        from backend.shared.db_write_queue import run_db_write

        affected = await run_db_write(
            "postprocess.type_batch_update",
            lambda: maria_execute_query(
                f"""
                UPDATE `{_EXPLORATION_TABLE}`
                SET `type` = %s
                WHERE id IN ({placeholders})
                  AND COALESCE(TRIM(CAST(`type` AS CHAR)), '') = ''
                """,
                tuple(params),
                fetch=False,
                dbname=db_name,
            ),
        )
        try:
            updated_count += int(affected or 0)
        except Exception:
            updated_count += len(chunk)

    inserted_count = int(seed_stats.get("learn_inserted") or 0)
    seed_type_applied = int(seed_stats.get("learn_type_applied") or 0)
    scanned_total = scanned + int(seed_stats.get("learn_scanned") or 0)
    saved_total = updated_count + seed_type_applied
    collection_total = updated_count + seed_type_applied

    result = {
        "status": "completed",
        "event": "workflow_completed",
        "job_id": job_id,
        "account_name": db_name,
        "total_count": scanned_total,
        "collection_count": collection_total,
        "save_count": saved_total,
        "study_count": 0,
        "updated_count": updated_count,
        "inserted_count": inserted_count,
        "field_save_counts": {
            "title": 0,
            "content": 0,
            "cate": 0,
            "symmary": 0,
            "type": saved_total,
            "url": 0,
            "web_de": 0,
        },
        "_partial_sequence_aggregate_counts": data.get("_partial_sequence_aggregate_counts"),
        "partial_sequence_running": data.get("_partial_sequence_running"),
        "rule_count": rule_count,
        "sql_rule_count": int((sql_rule_meta or {}).get("sql_rules") or 0),
        "board_param_count": board_param_count,
        "matched_samples": matched_samples,
        **seed_stats,
        "message": f"type 후보정 완료: {updated_count}건을 {target}로 업데이트했습니다.",
        "source": "type_postprocess",
    }
    result["message"] = f"type 후보정 완료: exploration 누락 URL {inserted_count}건 보강, 신규 type 적용 {seed_type_applied}건, 기존 URL type 업데이트 {updated_count}건입니다."
    logger.info(
        "[TypePostprocess] completed | job_id=%s db=%s chat_bot_id=%s scanned=%s learn_inserted=%s seed_type_applied=%s board_param=%s matched=%s updated=%s rules=%s sample=%s",
        job_id,
        db_name,
        chat_bot_id,
        scanned,
        inserted_count,
        seed_type_applied,
        board_param_count,
        matched_count,
        updated_count,
        rule_count,
        matched_samples,
    )
    if job_id:
        await update_crawling_log_counters(
            job_id=job_id,
            status="completed",
            scan=scanned_total,
            collection=collection_total,
            saved=saved_total,
            study=0,
            dbname=db_name,
        )
        if not suppress_terminal_sse:
            await update_state_only(job_id=job_id, account_name=db_name, payload=result)
            await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=result)
    return result
