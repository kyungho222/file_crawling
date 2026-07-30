import logging
import re
import time
from typing import Any, Dict, Optional, Tuple

from asyncmy.cursors import DictCursor

from db.mariadb_pool import _run_mariadb_operation_with_retry, mariadb_execute, mariadb_wait_for_query
from db.query_debug import record_db_query
from utils.logging_util import LoggerSingleton

logger = LoggerSingleton.get_logger(logger_name="utils.get_mariadb", level=logging.INFO)


def _short_sql(query: Any, limit: int = 120) -> str:
    try:
        s = " ".join(str(query or "").split())
    except Exception:
        return ""
    if len(s) <= limit:
        return s
    return s[:limit] + "..."


def _is_insert_like(query: Any) -> bool:
    try:
        normalized = str(query or "").lstrip().lower()
    except Exception:
        return False
    return normalized.startswith("insert") or normalized.startswith("replace")


def _short_value_for_warning(value: Any, limit: int = 240) -> str:
    try:
        text = str(value or "")
    except Exception:
        text = repr(value)
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _extract_insert_columns_for_warning(query: Any) -> list[str]:
    try:
        sql = str(query or "")
    except Exception:
        return []
    match = re.search(r"\binsert\s+into\s+[^()]+\((?P<cols>[^)]{1,4000})\)\s+values\s*\(", sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    cols: list[str] = []
    for raw in match.group("cols").split(","):
        col = raw.strip().strip("`").strip()
        if col:
            cols.append(col)
    return cols


def _param_context_for_warning(query: Any, params: Any, column_name: str, extra_context: Optional[Dict[str, Any]] = None) -> dict[str, Any]:
    cols = _extract_insert_columns_for_warning(query)
    values = list(params or ()) if isinstance(params, (list, tuple)) else []
    out: dict[str, Any] = {"column": column_name}
    if column_name in cols:
        idx = cols.index(column_name)
        out["param_index"] = idx
        if idx < len(values):
            value = values[idx]
            try:
                out["value_len"] = len(str(value or ""))
            except Exception:
                out["value_len"] = None
            out["value_preview"] = _short_value_for_warning(value)
    if isinstance(extra_context, dict):
        for key, value in extra_context.items():
            if value is not None and str(value).strip():
                out[str(key)] = _short_value_for_warning(value, 500)
    return out


async def _log_mariadb_warnings_if_needed(cursor: Any, *, query: Any, params: Any, dbname: Any, op_name: str, warning_context: Optional[Dict[str, Any]] = None) -> None:
    query_text = _short_sql(query, 500)
    if "content_author" not in str(query or "").lower():
        return
    try:
        warning_count = int(getattr(cursor, "warning_count", 0) or 0)
    except Exception:
        warning_count = 0
    # Some asyncmy versions log server warnings without exposing warning_count reliably.
    try:
        await mariadb_wait_for_query(cursor.execute("SHOW WARNINGS"))
        rows = await mariadb_wait_for_query(cursor.fetchall())
    except Exception as exc:
        logger.debug(
            "[MariaDB][warning_detail] SHOW WARNINGS failed | db=%s op=%s err=%s query=%s",
            dbname,
            op_name,
            exc,
            query_text,
        )
        return
    if not rows and warning_count <= 0:
        return
    for row in rows or []:
        try:
            level = row[0] if len(row) > 0 else ""
            code = row[1] if len(row) > 1 else ""
            message = row[2] if len(row) > 2 else row
        except Exception:
            level, code, message = "", "", row
        message_text = str(message or "")
        if "content_author" not in message_text.lower() and "data truncated" not in message_text.lower():
            continue
        context = _param_context_for_warning(query, params, "content_author", warning_context)
        logger.warning(
            "[MariaDB][warning_detail] db=%s op=%s level=%s code=%s message=%s context=%s query=%s",
            dbname,
            op_name,
            level,
            code,
            message_text,
            context,
            query_text,
        )


async def maria_execute_query(query, params=None, fetch=False, dbname=None, op_name=None, warning_context: Optional[Dict[str, Any]] = None):
    """
    MariaDB SQL 荑쇰━瑜??ㅽ뻾?섎뒗 踰붿슜 ?⑥닔.
    - SELECT 怨꾩뿴? dict rows 諛섑솚
    - INSERT/REPLACE 怨꾩뿴? lastrowid 諛섑솚
    - 洹???鍮꾩“??荑쇰━??rowcount 諛섑솚
    """
    if not fetch:
        try:
            from backend.shared.db_write_queue import in_db_write_worker, run_db_write
            if not in_db_write_worker():
                label = op_name or f"maria_write:{_short_sql(query)}"
                return await run_db_write(
                    label,
                    lambda: maria_execute_query(query, params=params, fetch=fetch, dbname=dbname, op_name=op_name, warning_context=warning_context),
                )
        except ImportError:
            pass
    q_t0 = time.perf_counter()
    try:
        if fetch:
            result = await mariadb_execute(query, params=params, fetch=True, dbname=dbname, op_name=op_name)
        elif _is_insert_like(query):
            async def _executor(conn):
                async with conn.cursor() as cursor:
                    await mariadb_wait_for_query(cursor.execute(query, params or ()))
                    lastrowid = int(getattr(cursor, "lastrowid", 0) or 0)
                    await _log_mariadb_warnings_if_needed(
                        cursor,
                        query=query,
                        params=params or (),
                        dbname=dbname,
                        op_name="insert",
                        warning_context=warning_context,
                    )
                    return lastrowid

            result = await _run_mariadb_operation_with_retry(
                dbname,
                f"insert:{_short_sql(query)}",
                _executor,
            )
        else:
            result = await mariadb_execute(query, params=params, fetch=False, dbname=dbname, op_name=op_name)

        record_db_query(
            query=str(query),
            dbname=dbname,
            elapsed_ms=(time.perf_counter() - q_t0) * 1000.0,
            ok=True,
            fetch=bool(fetch),
            error=None,
        )
        return result
    except Exception as e:
        try:
            record_db_query(
                query=str(query),
                dbname=dbname,
                elapsed_ms=(time.perf_counter() - q_t0) * 1000.0,
                ok=False,
                fetch=bool(fetch),
                error=e,
            )
        except Exception:
            pass
        logger.error("Database operation failed: %s", e)
        raise


async def maria_insert_data(table, data, dbname=None, warning_context: Optional[Dict[str, Any]] = None):
    columns = ", ".join(data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    return await maria_execute_query(query, tuple(data.values()), dbname=dbname, warning_context=warning_context)


async def maria_upsert_then_last_insert_id(
    query: str,
    params: Optional[Tuple[Any, ...]] = None,
    dbname: Optional[str] = None,
) -> Tuple[Optional[int], int]:
    """
    INSERT ... ON DUPLICATE KEY UPDATE 瑜??숈씪 ?곌껐?먯꽌 ?ㅽ뻾????LAST_INSERT_ID() 瑜?議고쉶?쒕떎.
    Returns:
        (last_insert_id, rowcount)
    """
    try:
        from backend.shared.db_write_queue import in_db_write_worker, run_db_write
        if not in_db_write_worker():
            return await run_db_write(
                f"maria_upsert:{_short_sql(query)}",
                lambda: maria_upsert_then_last_insert_id(query, params=params, dbname=dbname),
            )
    except ImportError:
        pass
    q_t0 = time.perf_counter()
    try:
        async def _executor(conn):
            async with conn.cursor() as cursor:
                await mariadb_wait_for_query(cursor.execute(query, params or ()))
                rc = int(getattr(cursor, "rowcount", 0) or 0)
                await _log_mariadb_warnings_if_needed(
                    cursor,
                    query=query,
                    params=params or (),
                    dbname=dbname,
                    op_name="upsert",
                )
                await mariadb_wait_for_query(cursor.execute("SELECT LAST_INSERT_ID()"))
                row = await mariadb_wait_for_query(cursor.fetchone())
                lid_raw = row[0] if row else None
                try:
                    lid = int(lid_raw) if lid_raw is not None else None
                except (TypeError, ValueError):
                    lid = None
                return (lid, rc)

        result = await _run_mariadb_operation_with_retry(
            dbname,
            f"upsert:{_short_sql(query)}",
            _executor,
        )
        record_db_query(
            query=str(query),
            dbname=dbname,
            elapsed_ms=(time.perf_counter() - q_t0) * 1000.0,
            ok=True,
            fetch=False,
            error=None,
        )
        return result
    except Exception as e:
        try:
            record_db_query(
                query=str(query),
                dbname=dbname,
                elapsed_ms=(time.perf_counter() - q_t0) * 1000.0,
                ok=False,
                fetch=False,
                error=e,
            )
        except Exception:
            pass
        logger.error("[MariaDB] upsert failed: %s", e)
        raise


async def maria_update_data(table, data, condition, dbname=None, op_name=None):
    set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
    query = f"UPDATE {table} SET {set_clause} WHERE {condition}"
    await maria_execute_query(query, tuple(data.values()), dbname=dbname, op_name=op_name)


async def maria_delete_data(table, condition, dbname=None):
    query = f"DELETE FROM {table} WHERE {condition}"
    await maria_execute_query(query, dbname=dbname)


async def maria_select_data(table, columns="*", condition=None, dbname=None, order_by=None):
    query = f"SELECT {columns} FROM {table}"
    if condition:
        query += f" WHERE {condition}"
    if order_by:
        query += f" ORDER BY {order_by}"
    return await maria_execute_query(query, fetch=True, dbname=dbname)


__all__ = [
    "maria_execute_query",
    "maria_insert_data",
    "maria_upsert_then_last_insert_id",
    "maria_update_data",
    "maria_delete_data",
    "maria_select_data",
    "DictCursor",
]

