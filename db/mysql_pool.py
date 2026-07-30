"""Pure MySQL connection pool management using aiomysql.

Handles MySQL-specific databases: chatty, naraone
"""
import asyncio
import logging
from typing import Dict, Tuple, Optional, Any, Callable, Awaitable, TypeVar

import aiomysql
from backend.shared.config import Config
from utils.logging_util import LoggerSingleton

logger = LoggerSingleton.get_logger(logger_name="db.mysql_pool", level=logging.INFO)

T = TypeVar("T")

# Retry configuration
DB_MAX_RETRY_ATTEMPTS = max(1, int(getattr(Config, "DB_RETRY_ATTEMPTS", 3) or 3))
DB_RETRY_INITIAL_DELAY_SEC = float(getattr(Config, "DB_RETRY_INITIAL_DELAY_SEC", 0.2) or 0.2)
DB_RETRY_BACKOFF_MULTIPLIER = float(getattr(Config, "DB_RETRY_BACKOFF_MULTIPLIER", 2.0) or 2.0)

TRANSIENT_ERROR_CODES = {1040, 1205, 1213, 2006, 2013, 2014}
TRANSIENT_ERROR_KEYWORDS = (
    "packet sequence number wrong",
    "lost connection to mysql server",
    "server has gone away",
    "gone away",
    "lock wait timeout exceeded",
    "deadlock found when trying to get lock",
    "can't connect to mysql server",
)


def _extract_error_code(exc: Exception) -> Optional[int]:
    """Extract MySQL error code from exception."""
    for attr in ("errno",):
        code = getattr(exc, attr, None)
        if isinstance(code, int):
            return code
    args = getattr(exc, "args", None)
    if args and isinstance(args, tuple) and isinstance(args[0], int):
        return args[0]
    return None


def _should_retry_mysql_error(exc: Exception) -> bool:
    """Determine if MySQL error should trigger retry."""
    if isinstance(exc, asyncio.TimeoutError):
        return True
    code = _extract_error_code(exc)
    if code is not None and code in TRANSIENT_ERROR_CODES:
        return True
    message = str(exc).lower()
    return any(keyword in message for keyword in TRANSIENT_ERROR_KEYWORDS)


def _get_query_timeout_sec() -> float:
    try:
        value = float(getattr(Config, "MYSQL_QUERY_TIMEOUT_SEC", 20.0) or 20.0)
    except Exception:
        value = 20.0
    return max(1.0, min(value, 300.0))


def _decode_bytes_safely(value: bytes) -> str:
    """Safely decode bytes to string with Korean fallback."""
    for enc in ("utf-8", "cp949", "euc-kr", "latin1"):
        try:
            return value.decode(enc)
        except Exception:
            continue
    # Final fallback: allow data loss
    return value.decode("utf-8", errors="replace")


def _normalize_row_unicode(row: Any) -> Any:
    """Convert DictCursor byte fields to strings."""
    if row is None:
        return row
    if isinstance(row, dict):
        return {k: (_decode_bytes_safely(v) if isinstance(v, (bytes, bytearray)) else v) for k, v in row.items()}
    if isinstance(row, (list, tuple)):
        converted: list[Any] = []
        for v in row:
            if isinstance(v, (bytes, bytearray)):
                converted.append(_decode_bytes_safely(v))
            else:
                converted.append(v)
        return type(row)(converted)
    if isinstance(row, (bytes, bytearray)):
        return _decode_bytes_safely(row)
    return row


def _get_pool_metrics(pool: Any) -> dict[str, Any]:
    try:
        pool_size = int(getattr(pool, "size", 0) or 0)
    except Exception:
        pool_size = 0
    try:
        free_size = int(getattr(pool, "freesize", 0) or 0)
    except Exception:
        free_size = 0
    try:
        used_size = int(len(getattr(pool, "_used", []) or []))
    except Exception:
        used_size = max(0, pool_size - free_size)
    try:
        acquiring = int(getattr(pool, "_acquiring", 0) or 0)
    except Exception:
        acquiring = 0
    return {
        "pool_size": pool_size,
        "free_size": free_size,
        "used_size": used_size,
        "acquiring": acquiring,
        "min_size": getattr(pool, "minsize", None),
        "max_size": getattr(pool, "maxsize", None),
        "closing": bool(getattr(pool, "_closing", False)),
        "closed": bool(getattr(pool, "_closed", False)),
    }


def _format_pool_snapshot(dbname: str, pool: Any, last_used: float) -> str:
    metrics = _get_pool_metrics(pool)
    idle_s = max(0.0, asyncio.get_event_loop().time() - float(last_used or 0.0))
    return (
        f"{dbname}(used={metrics['used_size']},free={metrics['free_size']},"
        f"size={metrics['pool_size']},max={metrics['max_size']},"
        f"acquiring={metrics['acquiring']},idle={idle_s:.1f}s,"
        f"closing={int(metrics['closing'])},closed={int(metrics['closed'])})"
    )


def _resolve_mysql_dbname(dbname: Optional[str]) -> str:
    """Map logical dbname to actual MySQL schema name.

    - naraone ??Asadal_Chatbot (or Config.GWI_MYSQL_DBNAME)
    - chatty  ??chatty (or Config.CHATTY_MYSQL_DBNAME)
    """
    name = (dbname or "").strip().lower()
    if name == "naraone":
        return getattr(Config, 'GWI_MYSQL_DBNAME', 'Asadal_Chatbot')
    if name == "chatty":
        return getattr(Config, 'CHATTY_MYSQL_DBNAME', 'chatty')
    raise ValueError(f"Unsupported MySQL dbname: {dbname}")


def _get_mysql_credentials(dbname: Optional[str]):
    """Select MySQL credentials based on dbname.

    - naraone ??GWI_* credentials
    - chatty  ??CHATTY_* credentials
    """
    name = (dbname or "").strip().lower()

    def pick(prefix: str):
        user = getattr(Config, f"{prefix}_MYSQL_USER", None)
        password = getattr(Config, f"{prefix}_MYSQL_PASSWORD", None)
        host = getattr(Config, f"{prefix}_MYSQL_HOST", None)
        port_val = getattr(Config, f"{prefix}_MYSQL_PORT", None)
        try:
            port = int(port_val)
        except Exception:
            port = 3306
        return user, password, host, port

    if name == "naraone":
        return pick("GWI")
    elif name == "chatty":
        return pick("CHATTY")
    else:
        return None, None, None, 3306


class MySQLPool:
    """MySQL connection pool manager (chatty, naraone only)."""

    _pools: Dict[str, Tuple[aiomysql.Pool, float]] = {}
    _lock = asyncio.Lock()
    _cleanup_task: Optional[asyncio.Task] = None

    @classmethod
    def _log_pool_status(cls, tag: str, level: int = logging.INFO) -> None:
        snapshots = [
            _format_pool_snapshot(dbname, pool, last_used)
            for dbname, (pool, last_used) in cls._pools.items()
        ]
        if not snapshots:
            return
        logger.log(level, "[MySQL][%s] %s", tag, " | ".join(snapshots))

    @classmethod
    def _touch_pool(cls, dbname: Optional[str]) -> None:
        """Update pool last-used timestamp."""
        try:
            if dbname is None:
                return
            pool_tuple = cls._pools.get(dbname)
            if not pool_tuple:
                return
            pool, _ = pool_tuple
            cls._pools[dbname] = (pool, asyncio.get_event_loop().time())
        except Exception:
            pass

    @classmethod
    async def get_pool(cls, dbname=None) -> aiomysql.Pool:
        """Get or create MySQL connection pool."""
        if dbname is None:
            logger.error("Database name cannot be None")
            raise ValueError("Database name cannot be None")

        db_name = dbname

        async with cls._lock:
            if db_name not in cls._pools:
                try:
                    actual_db = _resolve_mysql_dbname(db_name)
                    user, password, host, port = _get_mysql_credentials(db_name)

                    if not all([user, password, host]):
                        raise ValueError(f"Unsupported dbname or missing credentials: {db_name}")

                    # Character set configuration (MySQL 5.0.x ??utf8 recommended)
                    charset = getattr(Config, 'MYSQL_CHARSET', None) or 'utf8'
                    collation = getattr(Config, 'MYSQL_COLLATION', None) or 'utf8_general_ci'

                    pool = await aiomysql.create_pool(
                        db=actual_db,
                        user=user,
                        password=password,
                        host=host,
                        port=port,
                        minsize=int(getattr(Config, "MYSQL_POOL_MIN", Config.DB_POOL_MIN) or Config.DB_POOL_MIN),
                        maxsize=int(getattr(Config, "MYSQL_POOL_MAX", Config.DB_POOL_MAX) or Config.DB_POOL_MAX),
                        charset=charset,
                        use_unicode=True,
                        init_command=f"SET NAMES {charset} COLLATE {collation}",
                        autocommit=True,
                    )
                    cls._pools[db_name] = (pool, asyncio.get_event_loop().time())
                    logger.info(f"??MySQL pool created logical='{db_name}' ??actual='{actual_db}' ({host}:{port})")

                    if cls._cleanup_task is None or cls._cleanup_task.done():
                        cls._cleanup_task = asyncio.create_task(cls._auto_cleanup())

                except Exception as e:
                    logger.error(f"??MySQL pool creation failed {db_name} ({host}:{port}): {e}")
                    raise
            else:
                pool, _ = cls._pools[db_name]
                cls._pools[db_name] = (pool, asyncio.get_event_loop().time())

        return cls._pools[db_name][0]

    @classmethod
    async def _auto_cleanup(cls):
        """Periodically cleanup unused pools."""
        while True:
            try:
                await asyncio.sleep(Config.DB_POOL_CHCK)
                cls._log_pool_status("periodic.status.before_cleanup")
                await cls.release_unused_pools()
                cls._log_pool_status("periodic.status.after_cleanup")
            except Exception as e:
                logger.error(f"Auto cleanup error: {e}")
            except asyncio.CancelledError:
                logger.info("Auto cleanup task terminated")
                break

    @classmethod
    async def release_unused_pools(cls, timeout=None):
        """Release pools unused for timeout period."""
        if timeout is None:
            timeout = Config.DB_POOL_CHCK

        async with cls._lock:
            current_time = asyncio.get_event_loop().time()
            to_remove = []

            for dbname, (pool, last_used) in cls._pools.items():
                # Don't close pools with active connections
                metrics = _get_pool_metrics(pool)
                pool_size = metrics["pool_size"]
                free_size = metrics["free_size"]
                if pool_size > 0 and free_size < pool_size:
                    continue
                if current_time - last_used > timeout:
                    try:
                        pool.close()
                        await pool.wait_closed()
                        to_remove.append(dbname)
                        logger.info(
                            "[MySQL][idle_pool_released] db=%s snapshot=%s",
                            dbname,
                            _format_pool_snapshot(dbname, pool, last_used),
                        )
                    except Exception as e:
                        logger.error(f"MySQL pool release failed {dbname}: {e}")

            for dbname in to_remove:
                del cls._pools[dbname]

    @classmethod
    async def close_all_pools(cls):
        """Close all pools (application shutdown)."""
        logger.info(f"Closing {len(cls._pools)} MySQL connection pools")

        if cls._cleanup_task and not cls._cleanup_task.done():
            cls._cleanup_task.cancel()
            try:
                await cls._cleanup_task
            except asyncio.CancelledError:
                pass

        async with cls._lock:
            for dbname, (pool, _) in cls._pools.items():
                logger.info(f"Closing MySQL pool: {dbname}")
                try:
                    pool.close()
                    await pool.wait_closed()
                    logger.info(f"??MySQL pool closed {dbname}")
                except Exception as e:
                    logger.error(f"??MySQL pool close failed: {dbname} - {e}")

            cls._pools.clear()
            logger.info("All MySQL connection pools closed")

    @classmethod
    def get_pool_status(cls):
        """Get current pool status information."""
        current_time = asyncio.get_event_loop().time()
        status = {}

        for dbname, (pool, last_used) in cls._pools.items():
            age = current_time - last_used
            metrics = _get_pool_metrics(pool)
            status[dbname] = {
                "pool_id": hex(id(pool)),
                "min_size": metrics["min_size"],
                "max_size": metrics["max_size"],
                "pool_size": metrics["pool_size"],
                "free_size": metrics["free_size"],
                "used_size": metrics["used_size"],
                "acquiring": metrics["acquiring"],
                "closing": metrics["closing"],
                "closed": metrics["closed"],
                "last_used_seconds_ago": age,
                "is_idle": age > Config.DB_POOL_CHCK,
            }

        return status


async def _run_mysql_operation_with_retry(
    dbname: str,
    op_name: str,
    executor: Callable[[Any], Awaitable[T]],
) -> T:
    """Execute MySQL operation with retry logic."""
    delay = DB_RETRY_INITIAL_DELAY_SEC
    last_exc: Optional[Exception] = None
    for attempt in range(1, DB_MAX_RETRY_ATTEMPTS + 1):
        connected = None
        discard_connected = False
        try:
            loop = asyncio.get_event_loop()
            start_time = loop.time()
            connected = await mysql_connect(dbname)
            time_connected = loop.time()
            result = await executor(connected)
            time_done = loop.time()
            try:
                slow_warn_sec = float(getattr(Config, "MYSQL_SLOW_OP_WARN_SEC", 4.0) or 4.0)
            except Exception:
                slow_warn_sec = 4.0
            delay_total = time_done - start_time
            delay_connected = time_connected - start_time
            delay_executor = time_done - time_connected
            if delay_total >= max(0.5, float(slow_warn_sec)):
                logger.warning(
                    "[MySQL] slow op db=%s total=%.3fs connected_wait=%.3fs executor=%.3fs op=%s",
                    dbname,
                    delay_total,
                    delay_connected,
                    delay_executor,
                    op_name,
                )
            return result
        except asyncio.CancelledError:
            discard_connected = True
            raise
        except Exception as exc:
            last_exc = exc
            discard_connected = True
            should_retry = _should_retry_mysql_error(exc) or isinstance(exc, asyncio.TimeoutError)
            if not should_retry or attempt >= DB_MAX_RETRY_ATTEMPTS:
                logger.error(
                    "[MySQL] %s failed(db=%s, attempt=%s/%s): %s",
                    op_name,
                    dbname,
                    attempt,
                    DB_MAX_RETRY_ATTEMPTS,
                    exc,
                )
                raise
            logger.warning(
                "[MySQL] %s retry scheduled(db=%s, attempt=%s/%s, wait=%.2fs): %s",
                op_name,
                dbname,
                attempt,
                DB_MAX_RETRY_ATTEMPTS,
                delay,
                exc,
            )
            await asyncio.sleep(delay)
            delay *= DB_RETRY_BACKOFF_MULTIPLIER
        finally:
            if connected:
                try:
                    await mysql_release(connected, dbname, discard=discard_connected)
                except Exception as return_exc:
                    logger.error(
                        "[MySQL] Connection return failed db=%s discard=%s error=%s",
                        dbname,
                        discard_connected,
                        return_exc,
                        exc_info=True,
                    )
                    if connected:
                        try:
                            connected.close()
                            await asyncio.wait_for(connected.wait_closed(), timeout=10.0)
                            logger.warning("[MySQL] Force close succeeded after return failure db=%s", dbname)
                        except asyncio.TimeoutError:
                            logger.error("[MySQL] Force close timeout - pool leak possible db=%s", dbname)
                        except Exception as close_exc:
                            logger.critical("[MySQL] Force close failed - pool leak confirmed! db=%s error=%s", dbname, close_exc)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"[MySQL] {op_name} failed: unknown error")


async def mysql_connect(dbname=None) -> aiomysql.Connection:
    """Acquire MySQL database connection."""
    try:
        pool = await MySQLPool.get_pool(dbname)
        try:
            acquire_timeout = float(getattr(Config, "MYSQL_POOL_ACQUIRE_TIMEOUT_SEC", 5.0) or 5.0)
        except Exception:
            acquire_timeout = 5.0
        acquire_timeout = max(0.2, float(acquire_timeout))
        try:
            conn = await asyncio.wait_for(pool.acquire(), timeout=acquire_timeout)
            conn._pool = pool  # ensure release uses the exact origin pool
            conn._pool_dbname = dbname
            return conn
        except asyncio.TimeoutError:
            try:
                logger.error(
                    "[MySQL] pool acquire timeout db=%s timeout=%.2fs pool_size=%s free_size=%s",
                    str(dbname),
                    acquire_timeout,
                    getattr(pool, "size", None),
                    getattr(pool, "freesize", None),
                )
            except Exception:
                pass
            raise
    except Exception as e:
        logger.error(f"MySQL connection failed: {e}")
        raise


async def mysql_release(conn, dbname=None, discard: bool = False):
    """Return connection to pool."""
    if not conn:
        return

    pool = getattr(conn, "_pool", None)
    resolved_dbname = dbname

    if pool is None and dbname is not None:
        try:
            pool = await MySQLPool.get_pool(dbname)
        except Exception as e:
            logger.error(f"MySQL pool resolve failed during release(db={dbname}): {e}")

    if resolved_dbname is None and pool is not None:
        try:
            for logical_name, (known_pool, _) in MySQLPool._pools.items():
                if known_pool is pool:
                    resolved_dbname = logical_name
                    break
        except Exception:
            pass

    try:
        if discard:
            # Close socket first, but still release to reclaim pool._used slot.
            try:
                conn.close()
                await conn.wait_closed()
            except Exception:
                pass
    finally:
        if pool is not None:
            try:
                pool.release(conn)
                MySQLPool._touch_pool(resolved_dbname)
            except Exception as e:
                logger.error(f"MySQL connection return failed(db={resolved_dbname}): {e}")
        else:
            logger.error(
                "MySQL connection return failed: pool reference not found "
                f"(db={dbname}, discard={discard})"
            )
            try:
                conn.close()
                await conn.wait_closed()
            except Exception:
                pass

async def mysql_execute(query, params=None, fetch=False, dbname=None, op_name: Optional[str] = None):
    """Execute SQL query on MySQL database.

    Args:
        query: SQL query to execute
        params: Query parameters (tuple/list/dict)
        fetch: If True, return results (SELECT queries)
        dbname: Database name (chatty or naraone)

    Returns:
        list[dict] if fetch=True, else int(rowcount)
    """
    if dbname is None:
        logger.error("Database name cannot be None")
        raise ValueError("Database name cannot be None")

    async def _executor(conn):
        if fetch:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await asyncio.wait_for(cursor.execute(query, params or ()), timeout=_get_query_timeout_sec())
                result = await asyncio.wait_for(cursor.fetchall(), timeout=_get_query_timeout_sec())
                return [_normalize_row_unicode(row) for row in (result or [])]
        try:
            await conn.autocommit(False)  # type: ignore[attr-defined]
        except Exception:
            pass
        try:
            async with conn.cursor() as cursor:
                await asyncio.wait_for(cursor.execute(query, params or ()), timeout=_get_query_timeout_sec())
                rowcount = int(getattr(cursor, "rowcount", 0) or 0)
            try:
                await asyncio.wait_for(conn.commit(), timeout=_get_query_timeout_sec())
            except Exception:
                pass
            return rowcount
        except Exception:
            try:
                await asyncio.wait_for(conn.rollback(), timeout=_get_query_timeout_sec())
            except Exception:
                pass
            raise
        finally:
            try:
                await conn.autocommit(True)  # type: ignore[attr-defined]
            except Exception:
                pass

    resolved_op_name = op_name or f"{'fetch' if fetch else 'exec'}:{query.strip()[:60]}"
    return await _run_mysql_operation_with_retry(dbname, resolved_op_name, _executor)


async def rdbms_executemany(query: str, params_list, dbname: Optional[str] = None) -> int:
    """Batch execute helper (non-SELECT only)."""
    if not dbname:
        raise ValueError("Database name cannot be None")

    async def _executor(conn):
        try:
            await conn.autocommit(False)  # type: ignore[attr-defined]
        except Exception:
            pass
        try:
            items = list(params_list or [])
            if not items:
                return 0
            chunk_size = int(getattr(Config, "rdbms_executemany_CHUNK_SIZE", 100) or 100)
            chunk_size = max(1, chunk_size)
            async with conn.cursor() as cursor:
                for i in range(0, len(items), chunk_size):
                    chunk = items[i : i + chunk_size]
                    await cursor.executemany(query, chunk)
            try:
                await conn.commit()
            except Exception:
                pass
        except Exception:
            try:
                await conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                await conn.autocommit(True)  # type: ignore[attr-defined]
            except Exception:
                pass
        return len(params_list or [])

    op_name = f"executemany:{query.strip()[:60]}"
    return await _run_mysql_operation_with_retry(dbname, op_name, _executor)


async def mysql_executemany(query: str, params_list, dbname: Optional[str] = None) -> int:
    """Compatibility alias for existing mysql_db_config API."""
    return await rdbms_executemany(query, params_list, dbname)


async def mysql_cleanup_on_shutdown():
    """Application shutdown: cleanup MySQL pools."""
    await MySQLPool.close_all_pools()

