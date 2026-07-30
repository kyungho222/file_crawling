"""Compatibility wrapper for MySQL/MariaDB routing.

This module preserves the historic public API while delegating actual pool and
execution logic to `db.mysql_pool`, `db.mariadb_pool`, and `db.rdbms_router`.
"""

import logging
import time
import inspect
from typing import Optional, Tuple, Any

from db.maria_operations import maria_upsert_then_last_insert_id
from db.mysql_pool import _run_mysql_operation_with_retry
from db.query_debug import record_db_query
from db.rdbms_router import (
    MYSQL_DatabasePool,
    rdbms_connect,
    rdbms_execute_query,
    rdbms_executemany,
    rdbms_release,
    resolve_rdbms_engine,
)

logger = logging.getLogger("db.mysql_db_config")


def _short_sql(query: Any, limit: int = 120) -> str:
    try:
        s = " ".join(str(query or "").split())
    except Exception:
        return ""
    if len(s) <= limit:
        return s
    return s[:limit] + "..."


async def mysql_execute_query(query, params=None, fetch=False, dbname=None, op_name=None):
    """
    SQL 荑쇰━瑜??ㅽ뻾?섎뒗 ?명솚 ?섑띁.
    ?ㅼ젣 ?붿쭊 ?좏깮? rdbms_router媛 ?대떦?쒕떎.
    """
    if not fetch:
        try:
            from backend.shared.db_write_queue import in_db_write_worker, run_db_write
            if not in_db_write_worker():
                label = op_name or f"mysql_write:{_short_sql(query)}"
                return await run_db_write(
                    label,
                    lambda: mysql_execute_query(query, params=params, fetch=fetch, dbname=dbname, op_name=op_name),
                )
        except ImportError:
            pass
    q_t0 = time.perf_counter()
    try:
        result = await rdbms_execute_query(query, params=params, fetch=fetch, dbname=dbname, op_name=op_name)
        record_db_query(
            query=str(query),
            dbname=dbname,
            elapsed_ms=(time.perf_counter() - q_t0) * 1000.0,
            ok=True,
            fetch=bool(fetch),
            error=None,
        )
        if fetch:
            return result
        return None
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
        logger.error("[MySQL] query failed(db=%s): %s", dbname, e)
        raise


async def mysql_upsert_then_last_insert_id(
    query: str,
    params: Optional[Tuple[Any, ...]] = None,
    dbname: Optional[str] = None,
) -> Tuple[Optional[int], int]:
    """
    INSERT ... ON DUPLICATE KEY UPDATE ???숈씪 ?곌껐?먯꽌 LAST_INSERT_ID() 議고쉶.
    MariaDB濡??쇱슦?낅릺??DB??maria_operations 援ы쁽??洹몃?濡??ъ슜?쒕떎.
    """
    try:
        from backend.shared.db_write_queue import in_db_write_worker, run_db_write
        if not in_db_write_worker():
            return await run_db_write(
                f"mysql_upsert:{_short_sql(query)}",
                lambda: mysql_upsert_then_last_insert_id(query, params=params, dbname=dbname),
            )
    except ImportError:
        pass
    if resolve_rdbms_engine(dbname) != "mysql":
        return await maria_upsert_then_last_insert_id(query, params=params, dbname=dbname)

    q_t0 = time.perf_counter()
    try:
        async def _executor(conn):
            async with conn.cursor() as cursor:
                await cursor.execute(query, params or ())
                rc = int(getattr(cursor, "rowcount", 0) or 0)
                await cursor.execute("SELECT LAST_INSERT_ID()")
                row = await cursor.fetchone()
                lid_raw = row[0] if row else None
                try:
                    lid = int(lid_raw) if lid_raw is not None else None
                except (TypeError, ValueError):
                    lid = None
                return (lid, rc)

        result = await _run_mysql_operation_with_retry(
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
        logger.error("[MySQL] upsert failed(db=%s): %s", dbname, e)
        raise


async def mysql_connect_db(dbname=None):
    return await rdbms_connect(dbname)


async def mysql_return_connection(conn, dbname=None):
    await rdbms_release(conn, dbname)


async def mysql_executemany(query: str, params_list, dbname: Optional[str] = None) -> int:
    return await rdbms_executemany(query, params_list, dbname)


async def mysql_cleanup_on_shutdown():
    await MYSQL_DatabasePool.close_all_pools()


async def mysql_user_lock_run(
    dbname: Optional[str],
    lock_name: str,
    timeout_sec: int,
    callback,
):
    """
    Run a callback while holding a named DB lock on a dedicated session.

    The callback may use separate pooled connections; the lock remains held by
    this session until the callback completes and RELEASE_LOCK is issued.
    """
    conn = None
    lock_acquired = False
    safe_timeout = max(0, int(timeout_sec or 0))
    safe_lock_name = str(lock_name or "").strip()
    if not safe_lock_name:
        raise ValueError("lock_name cannot be empty")

    try:
        conn = await rdbms_connect(dbname)
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT GET_LOCK(%s, %s)", (safe_lock_name, safe_timeout))
            row = await cursor.fetchone()
            if isinstance(row, dict):
                lock_value = next(iter(row.values()), None)
            elif isinstance(row, (list, tuple)):
                lock_value = row[0] if row else None
            else:
                lock_value = row

        try:
            lock_acquired = int(lock_value or 0) == 1
        except Exception:
            lock_acquired = False

        if not lock_acquired:
            raise TimeoutError(
                f"Failed to acquire DB named lock '{safe_lock_name}' within {safe_timeout}s"
            )

        result = callback()
        if inspect.isawaitable(result):
            return await result
        return result
    finally:
        if conn is not None:
            if lock_acquired:
                try:
                    async with conn.cursor() as cursor:
                        await cursor.execute("DO RELEASE_LOCK(%s)", (safe_lock_name,))
                except Exception as release_exc:
                    logger.warning(
                        "[MySQL] named lock release failed(db=%s, lock=%s): %s",
                        dbname,
                        safe_lock_name,
                        release_exc,
                    )
            await rdbms_release(conn, dbname)


__all__ = [
    "MYSQL_DatabasePool",
    "mysql_execute_query",
    "mysql_upsert_then_last_insert_id",
    "mysql_connect_db",
    "mysql_return_connection",
    "mysql_executemany",
    "mysql_cleanup_on_shutdown",
    "mysql_user_lock_run",
]

