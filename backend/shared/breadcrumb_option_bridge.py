import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


async def fetch_breadcrumb_option_bridge_payload(
    *,
    db_name: str,
    learn_list_id: Optional[int] = None,
    contents_url: str = "",
    limit: int = 10,
) -> Dict[str, Any]:
    from db.mysql_db_config import mysql_execute_query

    option_table = "ASADAL_CRAWLING_BREADCRUMB_OPTION"
    learn_table = "ASADAL_CRAWLING_LEARN_LIST"
    exploration_table = "ASADAL_CRAWLING_EXPLORATION"
    selector_filter = "breadcrumb_selector IS NOT NULL AND LENGTH(TRIM(breadcrumb_selector)) != 0"

    columns = await mysql_execute_query(
        f"SHOW COLUMNS FROM {option_table}",
        fetch=True,
        dbname=db_name,
    )
    column_names = [
        str((row or {}).get("Field") or "")
        for row in (columns or [])
        if isinstance(row, dict) and (row or {}).get("Field")
    ]
    order_col = ""
    for candidate in ("id", "updated_at", "created_at", "learn_list_id"):
        if candidate in column_names:
            order_col = candidate
            break
    order_clause = f" ORDER BY {order_col} DESC" if order_col else ""
    option_learn_list_expr = "b.learn_list_id" if "learn_list_id" in column_names else "NULL"
    learn_join_clause = (
        f"LEFT JOIN {learn_table} l ON l.id = b.learn_list_id "
        if "learn_list_id" in column_names
        else f"LEFT JOIN {learn_table} l ON 1 = 0 "
    )

    option_lookup_learn_list_id = learn_list_id
    contents_learn_rows = []
    if contents_url:
        try:
            contents_learn_rows = await mysql_execute_query(
                (
                    f"SELECT id, LEFT(content, 500) AS content, LEFT(subject, 300) AS subject, status "
                    f"FROM {learn_table} "
                    "WHERE TRIM(content) = %s "
                    "ORDER BY id ASC "
                    "LIMIT 1"
                ),
                (contents_url,),
                fetch=True,
                dbname=db_name,
            )
            if contents_learn_rows and isinstance(contents_learn_rows[0], dict):
                raw_option_id = (contents_learn_rows[0] or {}).get("id")
                if raw_option_id not in (None, ""):
                    option_lookup_learn_list_id = int(raw_option_id)
        except Exception as contents_lookup_exc:
            logger.info(
                "[BreadcrumbOptionBridge] contents learn_list lookup failed | db=%s contents_url=%s err=%s",
                db_name,
                contents_url[:240],
                contents_lookup_exc,
            )

    scoped_filter = selector_filter
    scoped_params: list[Any] = []
    if option_lookup_learn_list_id is not None and "learn_list_id" in column_names:
        scoped_filter = f"learn_list_id = %s AND {selector_filter}"
        scoped_params.append(option_lookup_learn_list_id)

    try:
        exploration_columns = await mysql_execute_query(
            f"SHOW COLUMNS FROM {exploration_table}",
            fetch=True,
            dbname=db_name,
        )
    except Exception as exploration_cols_exc:
        logger.info(
            "[BreadcrumbOptionBridge] exploration columns unavailable | db=%s err=%s",
            db_name,
            exploration_cols_exc,
        )
        exploration_columns = []
    exploration_column_names = [
        str((row or {}).get("Field") or "")
        for row in (exploration_columns or [])
        if isinstance(row, dict) and (row or {}).get("Field")
    ]
    exploration_select_cols = [
        col
        for col in ("id", "url", "type", "status", "study_status", "chat_bot_id", "is_active", "created_at", "updated_at")
        if col in exploration_column_names
    ]
    exploration_select_sql = ", ".join(
        f"e.{col} AS exploration_{col}" for col in exploration_select_cols
    )
    if exploration_select_sql:
        exploration_select_sql = ", " + exploration_select_sql

    total_rows = await mysql_execute_query(
        f"SELECT COUNT(*) AS cnt FROM {option_table}",
        fetch=True,
        dbname=db_name,
    )
    non_empty_rows = await mysql_execute_query(
        f"SELECT COUNT(*) AS cnt FROM {option_table} WHERE {selector_filter}",
        fetch=True,
        dbname=db_name,
    )
    scoped_count_rows = await mysql_execute_query(
        f"SELECT COUNT(*) AS cnt FROM {option_table} WHERE {scoped_filter}",
        tuple(scoped_params),
        fetch=True,
        dbname=db_name,
    )
    samples = await mysql_execute_query(
        f"SELECT * FROM {option_table} WHERE {scoped_filter}{order_clause} LIMIT {limit}",
        tuple(scoped_params),
        fetch=True,
        dbname=db_name,
    )

    joined_filter = "b.breadcrumb_selector IS NOT NULL AND LENGTH(TRIM(b.breadcrumb_selector)) != 0"
    joined_params = list(scoped_params)
    if option_lookup_learn_list_id is not None and "learn_list_id" in column_names:
        joined_filter = f"b.learn_list_id = %s AND {joined_filter}"
    joined_order_clause = f" ORDER BY b.{order_col} DESC" if order_col else ""
    joined = await mysql_execute_query(
        (
            "SELECT "
            f"{option_learn_list_expr} AS learn_list_id, "
            "b.breadcrumb_selector, "
            "l.id AS learn_id, "
            "LEFT(l.content, 500) AS content, "
            "LEFT(l.subject, 300) AS subject, "
            "l.status "
            f"FROM {option_table} b "
            f"{learn_join_clause}"
            f"WHERE {joined_filter} "
            f"{joined_order_clause} "
            f"LIMIT {limit}"
        ),
        tuple(joined_params),
        fetch=True,
        dbname=db_name,
    )

    exploration_joined = []
    exploration_exact_rows = []
    if "url" in exploration_column_names:
        exploration_joined = await mysql_execute_query(
            (
                "SELECT "
                f"{option_learn_list_expr} AS learn_list_id, "
                "b.breadcrumb_selector, "
                "l.id AS learn_id, "
                "LEFT(l.content, 500) AS learn_content, "
                "LEFT(l.subject, 300) AS learn_subject, "
                "l.status AS learn_status "
                f"{exploration_select_sql} "
                f"FROM {option_table} b "
                f"{learn_join_clause}"
                f"LEFT JOIN {exploration_table} e ON e.url = l.content "
                f"WHERE {joined_filter} "
                f"{joined_order_clause} "
                f"LIMIT {limit}"
            ),
            tuple(joined_params),
            fetch=True,
            dbname=db_name,
        )
        if learn_list_id is not None:
            learn_url_rows = await mysql_execute_query(
                f"SELECT content FROM {learn_table} WHERE id = %s LIMIT 1",
                (learn_list_id,),
                fetch=True,
                dbname=db_name,
            )
            learn_url = ""
            if learn_url_rows and isinstance(learn_url_rows[0], dict):
                learn_url = str((learn_url_rows[0] or {}).get("content") or "").strip()
            if learn_url:
                exact_select = ", ".join(f"`{col}`" for col in exploration_select_cols) or "`url`"
                exploration_exact_rows = await mysql_execute_query(
                    (
                        f"SELECT {exact_select} "
                        f"FROM {exploration_table} "
                        "WHERE url = %s "
                        "LIMIT 20"
                    ),
                    (learn_url,),
                    fetch=True,
                    dbname=db_name,
                )

    return {
        "ok": True,
        "db_name": db_name,
        "learn_list_id": learn_list_id,
        "option_lookup_learn_list_id": option_lookup_learn_list_id,
        "contents_url": contents_url,
        "limit": limit,
        "option_table": option_table,
        "learn_table": learn_table,
        "exploration_table": exploration_table,
        "columns": columns or [],
        "column_names": column_names,
        "contents_learn_rows": contents_learn_rows or [],
        "exploration_columns": exploration_columns or [],
        "exploration_column_names": exploration_column_names,
        "total_count": int(((total_rows or [{}])[0] or {}).get("cnt") or 0),
        "non_empty_selector_count": int(((non_empty_rows or [{}])[0] or {}).get("cnt") or 0),
        "scoped_non_empty_selector_count": int(((scoped_count_rows or [{}])[0] or {}).get("cnt") or 0),
        "samples": samples or [],
        "joined_samples": joined or [],
        "exploration_joined_samples": exploration_joined or [],
        "exploration_exact_rows_for_learn_list_id": exploration_exact_rows or [],
    }
