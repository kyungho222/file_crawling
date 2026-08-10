"""Pure MariaDB connection pool management using asyncmy.

Handles all logical databases except chatty and naraone.
"""

import asyncio
import logging
import math
import os
import re
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, TypeVar

import asyncmy
from asyncmy.cursors import DictCursor

from backend.shared.config import Config
from utils.logging_util import LoggerSingleton

logger = LoggerSingleton.get_logger(logger_name="db.mariadb_pool", level=logging.INFO)

T = TypeVar("T")


def _short_warning_value(value: Any, limit: int = 240) -> str:
    try:
        text = str(value or "")
    except Exception:
        text = repr(value)
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _format_running_task_snapshot(*, limit: int = 64) -> str:
    """Capture suspended asyncio tasks after an unexpectedly slow pool acquire."""
    try:
        current = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if not task.done()]
    except RuntimeError:
        return "unavailable:no_running_loop"

    entries = []
    for task in sorted(tasks, key=lambda item: item.get_name())[:limit]:
        try:
            stack = task.get_stack(limit=1)
            if stack:
                frame = stack[-1]
                location = f"{os.path.basename(frame.f_code.co_filename)}:{frame.f_lineno}:{frame.f_code.co_name}"
            else:
                location = "no_python_frame"
            entries.append(
                f"{task.get_name()}:{'current' if task is current else 'waiting'}:{location}"
            )
        except Exception:
            entries.append("task_snapshot_error")
    remaining = max(0, len(tasks) - len(entries))
    if remaining:
        entries.append(f"more={remaining}")
    return " | ".join(entries) or "none"


def _content_author_warning_context(query: Any, params: Any) -> Dict[str, Any]:
    values = list(params or ()) if isinstance(params, (list, tuple)) else []
    context: Dict[str, Any] = {"param_count": len(values)}
    try:
        sql = str(query or "")
    except Exception:
        sql = ""
    insert_match = re.search(r"\binsert\s+into\s+[^()]+\((?P<cols>[^)]{1,4000})\)\s+values\s*\(", sql, re.IGNORECASE | re.DOTALL)
    if insert_match:
        cols = [raw.strip().strip("`").strip() for raw in insert_match.group("cols").split(",")]
        if "content_author" in cols:
            idx = cols.index("content_author")
            context["param_index"] = idx
            if idx < len(values):
                value = values[idx]
                context["value_len"] = len(str(value or ""))
                context["value_preview"] = _short_warning_value(value)
            return context
    update_match = re.search(r"\bcontent_author\s*=\s*%s", sql, re.IGNORECASE)
    if update_match and values:
        before = sql[: update_match.start()]
        idx = before.count("%s")
        context["param_index"] = idx
        if idx < len(values):
            value = values[idx]
            context["value_len"] = len(str(value or ""))
            context["value_preview"] = _short_warning_value(value)
    return context


async def _log_content_author_warnings(cursor: Any, *, query: Any, params: Any, dbname: Any, op_name: str) -> None:
    query_raw = str(query or "")
    if "content_author" not in query_raw.lower():
        return
    try:
        await mariadb_wait_for_query(cursor.execute("SHOW WARNINGS"))
        rows = await mariadb_wait_for_query(cursor.fetchall())
    except Exception as exc:
        logger.debug(
            "[MariaDB][warning_detail] SHOW WARNINGS failed | db=%s op=%s err=%s query=%s",
            dbname,
            op_name,
            exc,
            " ".join(query_raw.split())[:500],
        )
        return
    context = _content_author_warning_context(query, params)
    for row in rows or []:
        try:
            level = row[0] if len(row) > 0 else ""
            code = row[1] if len(row) > 1 else ""
            message = row[2] if len(row) > 2 else row
        except Exception:
            level, code, message = "", "", row
        message_text = str(message or "")
        if "data truncated" not in message_text.lower() and "content_author" not in message_text.lower():
            continue
        logger.warning(
            "[MariaDB][warning_detail] db=%s op=%s level=%s code=%s message=%s context=%s query=%s",
            dbname,
            op_name,
            level,
            code,
            message_text,
            context,
            " ".join(query_raw.split())[:500],
        )

_TRANSIENT_WARNING_STATE: Dict[Tuple[str, str], Tuple[float, int]] = {}
_ASYNCMY_SOCKET_PATCHED = False
_MARIADB_JOB_SHARE_COUNTS: Dict[str, int] = {}
_MARIADB_JOB_SHARE_COND: Optional[asyncio.Condition] = None
_MARIADB_JOB_SHARE_COND_LOOP: Optional[asyncio.AbstractEventLoop] = None
_MARIADB_RECREATE_LOCKS: Dict[str, asyncio.Lock] = {}
_MARIADB_RECREATE_LOCKS_LOOP: Optional[asyncio.AbstractEventLoop] = None
_MARIADB_RECENT_RECREATE_TS: Dict[str, float] = {}
_MARIADB_ACTIVE_HOLDERS: Dict[int, Dict[str, Any]] = {}


def _patch_asyncmy_transport_socket_guards() -> None:
    global _ASYNCMY_SOCKET_PATCHED
    if _ASYNCMY_SOCKET_PATCHED:
        return

    try:
        from asyncmy.connection import Connection as AsyncMyConnection
    except Exception as exc:
        logger.debug("[MariaDB][asyncmy_patch_skipped] reason=import_failed err=%s", exc)
        return

    def _wrap_socket_option_method(method_name: str) -> None:
        original = getattr(AsyncMyConnection, method_name, None)
        if not callable(original):
            return

        def _safe_socket_option(self, *args, **kwargs):
            try:
                return original(self, *args, **kwargs)
            except RuntimeError as exc:
                if "Transport does not expose socket instance" not in str(exc):
                    raise
                logger.debug(
                    "[MariaDB][asyncmy_socket_option_skipped] method=%s host=%s port=%s err=%s",
                    method_name,
                    getattr(self, "_host", None),
                    getattr(self, "_port", None),
                    exc,
                )
                return None

        setattr(AsyncMyConnection, method_name, _safe_socket_option)

    _wrap_socket_option_method("_set_keep_alive")
    _wrap_socket_option_method("_set_nodelay")
    _ASYNCMY_SOCKET_PATCHED = True


_patch_asyncmy_transport_socket_guards()


def _get_job_share_condition() -> asyncio.Condition:
    global _MARIADB_JOB_SHARE_COND
    global _MARIADB_JOB_SHARE_COND_LOOP

    loop = asyncio.get_running_loop()
    if _MARIADB_JOB_SHARE_COND is None or _MARIADB_JOB_SHARE_COND_LOOP is not loop:
        _MARIADB_JOB_SHARE_COND = asyncio.Condition()
        _MARIADB_JOB_SHARE_COND_LOOP = loop
    return _MARIADB_JOB_SHARE_COND


def _get_pool_recreate_lock(dbname: Optional[str]) -> asyncio.Lock:
    global _MARIADB_RECREATE_LOCKS_LOOP

    loop = asyncio.get_running_loop()
    if _MARIADB_RECREATE_LOCKS_LOOP is not loop:
        _MARIADB_RECREATE_LOCKS.clear()
        _MARIADB_RECREATE_LOCKS_LOOP = loop

    key = str(dbname or "").strip() or "__default__"
    lock = _MARIADB_RECREATE_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _MARIADB_RECREATE_LOCKS[key] = lock
    return lock


def _get_pool_acquire_timeout_sec() -> float:
    try:
        value = float(getattr(Config, "MARIADB_POOL_ACQUIRE_TIMEOUT_SEC", 12.0) or 12.0)
    except Exception:
        value = 12.0
    return max(0.2, value)


def _get_pre_ping_timeout_sec() -> float:
    try:
        value = float(
            getattr(
                Config,
                "MARIADB_PRE_PING_TIMEOUT_SEC",
                min(5.0, _get_pool_acquire_timeout_sec()),
            )
            or min(5.0, _get_pool_acquire_timeout_sec())
        )
    except Exception:
        value = min(5.0, _get_pool_acquire_timeout_sec())
    return max(0.2, value)



def _get_connection_hold_warn_ms(op_name: str = "") -> float:
    op_text = str(op_name or "").strip().lower()
    env_name = "MARIADB_CONN_HOLD_WARN_MS"
    default_value = "5000"
    if "learn_status_update" in op_text:
        env_name = "MARIADB_LEARN_STATUS_UPDATE_HOLD_WARN_MS"
        default_value = "7000"
    elif "file_learn_list_row_debug" in op_text:
        env_name = "MARIADB_FILE_DEBUG_HOLD_WARN_MS"
        default_value = "5000"
    elif "crawling_log" in op_text:
        env_name = "MARIADB_CRAWLING_LOG_HOLD_WARN_MS"
        default_value = "3000"
    elif "information_schema" in op_text or "schema_index_check" in op_text:
        env_name = "MARIADB_SCHEMA_INDEX_CHECK_HOLD_WARN_MS"
        default_value = "5000"
    try:
        value = float(os.getenv(env_name, default_value) or default_value)
    except Exception:
        value = float(default_value or 5000)
    return max(0.0, min(value, 600000.0))


def _mariadb_pool_worker_summary() -> Dict[str, Any]:
    summary: Dict[str, Any] = {}
    try:
        from backend.shared.db_write_queue import db_write_queue_status

        status = db_write_queue_status()
        summary.update(
            {
                "db_write_workers": status.get("workers_configured"),
                "db_write_alive": status.get("workers_alive"),
                "db_write_active": status.get("active"),
                "db_write_pending": status.get("pending"),
                "log_workers": status.get("log_workers_configured"),
                "default_workers": status.get("default_workers_configured"),
            }
        )
    except Exception:
        pass
    try:
        summary["pg_learn_write_limit"] = int(getattr(Config, "POSTGRES_LEARN_WRITE_MAX_CONCURRENCY", 2) or 2)
    except Exception:
        pass
    return summary

def _get_query_timeout_sec() -> float:
    try:
        value = float(getattr(Config, "MARIADB_QUERY_TIMEOUT_SEC", 20.0) or 20.0)
    except Exception:
        value = 20.0
    return max(0.2, value)


def _get_operation_slow_log_ms() -> float:
    try:
        value = float(getattr(Config, "MARIADB_OPERATION_SLOW_LOG_MS", 1000.0) or 1000.0)
    except Exception:
        value = 1000.0
    return max(0.0, value)



def _get_pool_recreate_close_grace_sec() -> float:
    try:
        value = float(getattr(Config, "MARIADB_POOL_RECREATE_CLOSE_GRACE_SEC", 0.25) or 0.25)
    except Exception:
        value = 0.25
    return max(0.0, min(5.0, value))


def _get_pool_recreate_cooldown_sec() -> float:
    try:
        value = float(getattr(Config, "MARIADB_POOL_RECREATE_COOLDOWN_SEC", 0.75) or 0.75)
    except Exception:
        value = 0.75
    return max(0.0, min(10.0, value))


def _get_pool_soft_refresh_timeout_sec() -> float:
    try:
        value = float(
            os.getenv(
                "MARIADB_POOL_SOFT_REFRESH_TIMEOUT_SEC",
                str(getattr(Config, "MARIADB_POOL_SOFT_REFRESH_TIMEOUT_SEC", 1.0) or 1.0),
            )
            or "1.0"
        )
    except Exception:
        value = 1.0
    return max(0.1, min(10.0, value))


def _get_connect_validation_retry_count() -> int:
    try:
        value = int(getattr(Config, "MARIADB_CONNECT_VALIDATION_RETRIES", 2) or 2)
    except Exception:
        value = 2
    return max(1, min(5, value))


def _get_transient_warning_interval_sec() -> float:
    try:
        value = float(getattr(Config, "MARIADB_TRANSIENT_WARNING_INTERVAL_SEC", 30.0) or 30.0)
    except Exception:
        value = 30.0
    return max(1.0, min(300.0, value))


def _get_transient_warning_log_level() -> int:
    try:
        raw = str(getattr(Config, "MARIADB_TRANSIENT_WARNING_LOG_LEVEL", "DEBUG") or "DEBUG").strip().upper()
    except Exception:
        raw = "DEBUG"
    return {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "ERROR": logging.ERROR,
    }.get(raw, logging.DEBUG)


def _dynamic_job_share_enabled() -> bool:
    try:
        value = bool(getattr(Config, "MARIADB_DYNAMIC_JOB_SHARE", True))
    except Exception:
        value = True
    return value


def _get_mariadb_total_connection_cap() -> int:
    try:
        value = int(getattr(Config, "MARIADB_POOL_MAX", Config.DB_POOL_MAX) or Config.DB_POOL_MAX)
    except Exception:
        value = int(getattr(Config, "DB_POOL_MAX", 16) or 16)
    return max(1, min(value, 256))


def _get_job_share_db_cap(dbname: Optional[str], *, total_cap: int, active_jobs: int) -> int:
    base_cap = max(1, math.ceil(int(total_cap or 1) / max(1, int(active_jobs or 1))))
    shared_db_floor = max(base_cap, math.ceil(int(total_cap or 1) / 2))
    cap = shared_db_floor

    try:
        raw_min = str(os.getenv("MARIADB_JOB_SHARE_MIN_DB_CAP", "") or "").strip()
        if raw_min:
            cap = max(cap, int(raw_min))
    except Exception:
        pass

    db_name = _normalize_dbname(dbname) if dbname else ""
    if db_name:
        try:
            db_key = re.sub(r"[^A-Za-z0-9]+", "_", db_name.strip().upper()).strip("_")
            raw_db_cap = str(os.getenv(f"MARIADB_JOB_SHARE_DB_CAP_{db_key}", "") or "").strip()
            if raw_db_cap:
                cap = int(raw_db_cap)
        except Exception:
            pass

    return max(1, min(int(cap or base_cap), int(total_cap or base_cap)))


def _is_mysql_router_dbname(dbname: Optional[str]) -> bool:
    name = str(dbname or "").strip().lower()
    return name in {"chatty", "naraone"}


def _get_active_mariadb_workflow_count() -> int:
    try:
        from backend.shared.crawler_state import crawler_state

        workflows = getattr(crawler_state, "workflows", None)
        if not isinstance(workflows, dict):
            return 1

        count = 0
        for job_id, workflow in workflows.items():
            try:
                if hasattr(crawler_state, "has_active_workflow_slot") and not crawler_state.has_active_workflow_slot(job_id):
                    continue
            except Exception:
                pass
            try:
                db_name = str(getattr(workflow, "db_name", "") or "").strip()
            except Exception:
                db_name = ""
            if not db_name or _is_mysql_router_dbname(db_name):
                continue
            count += 1
        return max(1, count)
    except Exception:
        return 1


def _reconcile_job_share_counts() -> None:
    """
    Clamp stale share counters down to the live pool usage estimate.

    The share counter should track checked-out connections, but legacy close
    paths or interrupted releases can leave it above the real pool usage. When
    that happens, new acquires can be blocked even though the pool still has
    free connections.
    """
    try:
        known_dbnames = set(_MARIADB_JOB_SHARE_COUNTS.keys())
        known_dbnames.update(MariaDBPool._pools.keys())
    except Exception:
        known_dbnames = set(_MARIADB_JOB_SHARE_COUNTS.keys())

    for db_name in list(known_dbnames):
        if not db_name:
            continue
        try:
            current = int(_MARIADB_JOB_SHARE_COUNTS.get(db_name, 0) or 0)
        except Exception:
            current = 0
        if current <= 0:
            _MARIADB_JOB_SHARE_COUNTS.pop(db_name, None)
            continue

        live_estimate: Optional[int] = None
        try:
            pool_tuple = MariaDBPool._pools.get(db_name)
            if pool_tuple is not None:
                pool, _ = pool_tuple
                metrics = _get_pool_metrics(pool)
                live_estimate = max(
                    0,
                    int(metrics.get("used_size", 0) or 0) + int(metrics.get("acquiring", 0) or 0),
                )
            else:
                live_estimate = 0
        except Exception:
            live_estimate = None

        if live_estimate is None or current <= live_estimate:
            continue

        if live_estimate <= 0:
            _MARIADB_JOB_SHARE_COUNTS.pop(db_name, None)
        else:
            _MARIADB_JOB_SHARE_COUNTS[db_name] = live_estimate

        reconcile_level = (
            logging.INFO
            if str(os.getenv("MARIADB_JOB_SHARE_RECONCILE_INFO", "0") or "0").strip().lower()
            in ("1", "true", "yes", "on")
            else logging.DEBUG
        )
        logger.log(
            reconcile_level,
            "[MariaDB][job_share_reconciled] db=%s stale=%s live=%s",
            db_name,
            current,
            live_estimate,
        )


def _get_job_share_snapshot(dbname: Optional[str]) -> dict[str, int]:
    _reconcile_job_share_counts()
    total_cap = _get_mariadb_total_connection_cap()
    active_jobs = _get_active_mariadb_workflow_count()
    db_name = _normalize_dbname(dbname) if dbname else ""
    per_job_cap = _get_job_share_db_cap(db_name, total_cap=total_cap, active_jobs=active_jobs)
    db_in_use = int(_MARIADB_JOB_SHARE_COUNTS.get(db_name, 0) or 0) if db_name else 0
    global_in_use = int(sum(_MARIADB_JOB_SHARE_COUNTS.values()) or 0)
    return {
        "total_cap": total_cap,
        "active_jobs": active_jobs,
        "per_job_cap": per_job_cap,
        "db_in_use": db_in_use,
        "global_in_use": global_in_use,
    }


async def _acquire_job_share_slot(dbname: str, timeout_sec: float) -> tuple[dict[str, int], bool]:
    if not _dynamic_job_share_enabled():
        return _get_job_share_snapshot(dbname), False

    deadline = time.monotonic() + max(0.2, float(timeout_sec or 0.2))
    job_share_cond = _get_job_share_condition()
    async with job_share_cond:
        while True:
            snapshot = _get_job_share_snapshot(dbname)
            if snapshot["active_jobs"] <= 1:
                return snapshot, False
            if (
                snapshot["db_in_use"] < snapshot["per_job_cap"]
                and snapshot["global_in_use"] < snapshot["total_cap"]
            ):
                _MARIADB_JOB_SHARE_COUNTS[dbname] = snapshot["db_in_use"] + 1
                return _get_job_share_snapshot(dbname), True

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError(
                    f"MariaDB job share slot timeout(db={dbname}, active_jobs={snapshot['active_jobs']}, "
                    f"per_job_cap={snapshot['per_job_cap']}, db_in_use={snapshot['db_in_use']}, "
                    f"global_in_use={snapshot['global_in_use']}, total_cap={snapshot['total_cap']})"
                )
            await asyncio.wait_for(job_share_cond.wait(), timeout=remaining)


async def _release_job_share_slot(dbname: Optional[str]) -> None:
    if not _dynamic_job_share_enabled():
        return
    db_name = _normalize_dbname(dbname) if dbname else ""
    if not db_name:
        return
    job_share_cond = _get_job_share_condition()
    async with job_share_cond:
        current = int(_MARIADB_JOB_SHARE_COUNTS.get(db_name, 0) or 0)
        if current <= 1:
            _MARIADB_JOB_SHARE_COUNTS.pop(db_name, None)
        else:
            _MARIADB_JOB_SHARE_COUNTS[db_name] = current - 1
        job_share_cond.notify_all()


def _consume_suppressed_warning_count(tag: str, dbname: str) -> int:
    key = (tag, dbname)
    now = time.time()
    interval = _get_transient_warning_interval_sec()
    last_ts, suppressed = _TRANSIENT_WARNING_STATE.get(key, (0.0, 0))
    if (now - last_ts) >= interval:
        _TRANSIENT_WARNING_STATE[key] = (now, 0)
        return suppressed
    _TRANSIENT_WARNING_STATE[key] = (last_ts, suppressed + 1)
    return -1


DB_MAX_RETRY_ATTEMPTS = max(1, int(getattr(Config, "DB_RETRY_ATTEMPTS", 3) or 3))
DB_RETRY_INITIAL_DELAY_SEC = float(getattr(Config, "DB_RETRY_INITIAL_DELAY_SEC", 0.2) or 0.2)
DB_RETRY_BACKOFF_MULTIPLIER = float(getattr(Config, "DB_RETRY_BACKOFF_MULTIPLIER", 2.0) or 2.0)

TRANSIENT_ERROR_CODES = {1040, 1205, 1213, 2003, 2006, 2013, 2014}
TRANSIENT_ERROR_KEYWORDS = (
    "packet sequence number wrong",
    "lost connection to mysql server",
    "server has gone away",
    "gone away",
    "lock wait timeout exceeded",
    "deadlock found when trying to get lock",
    "can't connect to mysql server",
    "connection reset by peer",
    "cannot acquire connection after closing pool",
    "transport does not expose socket instance",
    "stale mariadb pool reference",
    "no usable mariadb pool before acquire",
    "no usable mariadb pool after acquire",
)


def _normalize_dbname(dbname: Optional[str]) -> str:
    name = str(dbname or "").strip()
    if not name:
        raise ValueError("Database name cannot be None")
    return name


def _release_job_share_slot_sync(dbname: Optional[str]) -> bool:
    db_name = str(dbname or "").strip()
    if not db_name:
        return False
    current = int(_MARIADB_JOB_SHARE_COUNTS.get(db_name, 0) or 0)
    if current <= 1:
        _MARIADB_JOB_SHARE_COUNTS.pop(db_name, None)
    else:
        _MARIADB_JOB_SHARE_COUNTS[db_name] = current - 1
    return True


def _schedule_job_share_release_from_close(conn: Any) -> None:
    _unregister_active_holder(conn)
    if not bool(getattr(conn, "_job_share_slot_granted", False)):
        return
    if bool(getattr(conn, "_skip_job_share_release_on_close", False)):
        return
    if bool(getattr(conn, "_job_share_slot_release_started", False)):
        return

    db_name = str(getattr(conn, "_pool_dbname", "") or "").strip()
    if not db_name:
        return

    try:
        setattr(conn, "_job_share_slot_granted", False)
        setattr(conn, "_job_share_slot_release_started", True)
    except Exception:
        pass

    async def _release_task() -> None:
        try:
            await _release_job_share_slot(db_name)
        except Exception:
            _release_job_share_slot_sync(db_name)
        finally:
            try:
                setattr(conn, "_job_share_slot_release_started", False)
            except Exception:
                pass

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _release_job_share_slot_sync(db_name)
        try:
            setattr(conn, "_job_share_slot_release_started", False)
        except Exception:
            pass
        return

    loop.create_task(_release_task())


def _install_job_share_close_guard(conn: Any) -> None:
    if conn is None or bool(getattr(conn, "_job_share_close_guard_installed", False)):
        return

    original_close = getattr(conn, "close", None)
    if callable(original_close):
        def _guarded_close(*args, **kwargs):
            try:
                return original_close(*args, **kwargs)
            finally:
                _schedule_job_share_release_from_close(conn)

        setattr(conn, "close", _guarded_close)

    original_ensure_closed = getattr(conn, "ensure_closed", None)
    if callable(original_ensure_closed):
        async def _guarded_ensure_closed(*args, **kwargs):
            try:
                return await original_ensure_closed(*args, **kwargs)
            finally:
                _schedule_job_share_release_from_close(conn)

        setattr(conn, "ensure_closed", _guarded_ensure_closed)

    try:
        setattr(conn, "_job_share_close_guard_installed", True)
    except Exception:
        pass


async def mariadb_wait_for_query(awaitable: Awaitable[T]) -> T:
    return await asyncio.wait_for(awaitable, timeout=_get_query_timeout_sec())


def _extract_error_code(exc: Exception) -> Optional[int]:
    for attr in ("errno",):
        code = getattr(exc, attr, None)
        if isinstance(code, int):
            return code
    args = getattr(exc, "args", None)
    if args and isinstance(args, tuple) and isinstance(args[0], int):
        return args[0]
    return None


def _should_retry_maria_error(exc: Exception) -> bool:
    code = _extract_error_code(exc)
    if code is not None and code in TRANSIENT_ERROR_CODES:
        return True
    if isinstance(exc, (ConnectionError, asyncio.TimeoutError)):
        return True
    message = str(exc).lower()
    return any(keyword in message for keyword in TRANSIENT_ERROR_KEYWORDS)


def _is_disconnect_like_error(exc: Exception) -> bool:
    code = _extract_error_code(exc)
    if code in {2003, 2006, 2013, 2014}:
        return True
    message = str(exc).lower()
    return any(
        token in message
        for token in (
            "lost connection to mysql server",
            "server has gone away",
            "connection reset by peer",
            "can't connect to mysql server",
            "cannot acquire connection after closing pool",
            "unhealthy maria connection",
            "transport does not expose socket instance",
            "stale mariadb pool reference",
            "no usable mariadb pool before acquire",
            "no usable mariadb pool after acquire",
        )
    )


def _should_refresh_pool_after_connect_failure(exc: Exception, stage: str) -> bool:
    if _is_disconnect_like_error(exc):
        return True
    if stage == "empty_pool":
        return True
    if stage in {"acquire", "health_check", "pre_ping"} and isinstance(exc, asyncio.TimeoutError):
        return True
    return False


async def _refresh_pool_for_retry(dbname: Optional[str], reason: str) -> bool:
    if not dbname:
        return False
    refreshed = await _soft_refresh_pool(dbname, reason)
    if refreshed:
        return True
    return await _full_recreate_pool(dbname, reason)


def _is_connection_healthy(conn: Optional[Any]) -> bool:
    if conn is None:
        return False
    try:
        reader = getattr(conn, "_reader", None)
        writer = getattr(conn, "_writer", None)
        if reader is None or writer is None:
            return False
        if hasattr(reader, "at_eof") and callable(reader.at_eof) and reader.at_eof():
            return False
        if hasattr(writer, "is_closing") and callable(writer.is_closing) and writer.is_closing():
            return False
    except Exception:
        return False
    return True


async def _close_connection_safely(conn: Any) -> None:
    if conn is None:
        return
    previous_skip_release = bool(getattr(conn, "_skip_job_share_release_on_close", False))
    try:
        setattr(conn, "_skip_job_share_release_on_close", True)
    except Exception:
        previous_skip_release = False
    try:
        try:
            ensure_closed = getattr(conn, "ensure_closed", None)
            if callable(ensure_closed):
                await ensure_closed()
                return
        except Exception:
            pass

        try:
            conn.close()
        except Exception:
            pass
        try:
            wait_closed = getattr(conn, "wait_closed", None)
            if callable(wait_closed):
                await wait_closed()
        except Exception:
            pass
        try:
            setattr(conn, "_connected", False)
        except Exception:
            pass
    finally:
        try:
            setattr(conn, "_skip_job_share_release_on_close", previous_skip_release)
        except Exception:
            pass


async def _ping_connection(conn: Any) -> None:
    if not bool(getattr(Config, "DB_POOL_PRE_PING", True)):
        return

    ping = getattr(conn, "ping", None)
    if callable(ping):
        await asyncio.wait_for(ping(False), timeout=_get_pre_ping_timeout_sec())
        return

    async def _do_ping() -> None:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            await cursor.fetchone()

    await asyncio.wait_for(_do_ping(), timeout=_get_pre_ping_timeout_sec())


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
    try:
        terminated = int(len(getattr(pool, "_terminated", []) or []))
    except Exception:
        terminated = 0
    return {
        "pool_size": pool_size,
        "free_size": free_size,
        "used_size": used_size,
        "acquiring": acquiring,
        "terminated": terminated,
        "min_size": getattr(pool, "minsize", None),
        "max_size": getattr(pool, "maxsize", None),
        "closing": bool(getattr(pool, "_closing", False)),
        "closed": bool(getattr(pool, "_closed", False)),
    }


def _format_pool_snapshot(dbname: str, pool: Any, last_used: Optional[float]) -> str:
    metrics = _get_pool_metrics(pool)
    share = _get_job_share_snapshot(dbname)
    idle_s = max(0.0, time.time() - float(last_used or 0.0)) if last_used is not None else 0.0
    return (
        f"{dbname}(used={metrics['used_size']},free={metrics['free_size']},"
        f"size={metrics['pool_size']},max={metrics['max_size']},"
        f"acquiring={metrics['acquiring']},share={share['db_in_use']}/{share['per_job_cap']},"
        f"global={share['global_in_use']}/{share['total_cap']},idle={idle_s:.1f}s,"
        f"closing={int(metrics['closing'])},closed={int(metrics['closed'])})"
    )


def _current_pool_snapshot(dbname: Optional[str]) -> str:
    try:
        db_name = _normalize_dbname(dbname)
        pool_tuple = MariaDBPool._pools.get(db_name)
        if not pool_tuple:
            return "pool=missing"
        pool, last_used = pool_tuple
        return _format_pool_snapshot(db_name, pool, last_used)
    except Exception as exc:
        return f"pool_snapshot_error={exc}"



def _register_active_holder(conn: Any, dbname: str) -> None:
    if conn is None:
        return
    try:
        task = asyncio.current_task()
        task_name = task.get_name() if task else ""
    except Exception:
        task_name = ""
    _MARIADB_ACTIVE_HOLDERS[id(conn)] = {
        "db": dbname,
        "task": task_name,
        "op": getattr(conn, "_mariadb_op_name", "") or "",
        "acquired_perf": float(getattr(conn, "_mariadb_acquired_perf", 0.0) or 0.0),
        "acquired_at": float(getattr(conn, "_mariadb_acquired_at", 0.0) or 0.0),
    }


def _update_active_holder_op(conn: Any, op_name: str) -> None:
    if conn is None:
        return
    holder = _MARIADB_ACTIVE_HOLDERS.get(id(conn))
    if holder is not None:
        holder["op"] = op_name


def _unregister_active_holder(conn: Any) -> None:
    if conn is None:
        return
    _MARIADB_ACTIVE_HOLDERS.pop(id(conn), None)


def _format_active_holder_snapshot(dbname: Optional[str], limit: int = 8) -> str:
    try:
        db_name = _normalize_dbname(dbname)
    except Exception:
        db_name = str(dbname or "")
    now_perf = time.perf_counter()
    rows = []
    for holder in list(_MARIADB_ACTIVE_HOLDERS.values()):
        if db_name and holder.get("db") != db_name:
            continue
        acquired_perf = float(holder.get("acquired_perf") or 0.0)
        hold_ms = (now_perf - acquired_perf) * 1000.0 if acquired_perf > 0 else -1.0
        rows.append(
            {
                "hold_ms": hold_ms,
                "task": holder.get("task") or "-",
                "op": _short_warning_value(holder.get("op") or "-", 120),
            }
        )
    rows.sort(key=lambda item: float(item.get("hold_ms") or 0.0), reverse=True)
    if not rows:
        return "none"
    return "; ".join(
        f"hold_ms={row['hold_ms']:.1f} task={row['task']} op={row['op']}"
        for row in rows[: max(1, int(limit or 1))]
    )


def _classify_mariadb_slow(
    *,
    connect_ms: Optional[float],
    executor_ms: Optional[float],
    release_ms: Optional[float],
) -> str:
    parts = {
        "pool_wait_or_connect": float(connect_ms or 0.0),
        "query_exec": float(executor_ms or 0.0),
        "release": float(release_ms or 0.0),
    }
    bottleneck = max(parts, key=parts.get)
    return bottleneck if parts[bottleneck] > 0 else "unknown"


def _pool_is_usable(pool: Any) -> bool:
    if pool is None:
        return False
    try:
        metrics = _get_pool_metrics(pool)
    except Exception:
        return False
    return not bool(metrics.get("closing") or metrics.get("closed"))


def _pool_has_no_connections(pool: Any) -> bool:
    try:
        metrics = _get_pool_metrics(pool)
    except Exception:
        return False
    try:
        return (
            int(metrics.get("pool_size", 0) or 0) <= 0
            and int(metrics.get("free_size", 0) or 0) <= 0
            and int(metrics.get("used_size", 0) or 0) <= 0
            and int(metrics.get("acquiring", 0) or 0) <= 0
            and not bool(metrics.get("closing") or metrics.get("closed"))
        )
    except Exception:
        return False


class MariaDBPool:
    """MariaDB connection pool manager (all databases except chatty/naraone)."""

    _pools: Dict[str, Tuple[Any, float]] = {}
    _lock: Optional[asyncio.Lock] = None
    _lock_loop: Optional[asyncio.AbstractEventLoop] = None
    _cleanup_task: Optional[asyncio.Task] = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if cls._lock is None or cls._lock_loop is not loop:
            cls._lock = asyncio.Lock()
            cls._lock_loop = loop
        return cls._lock

    @classmethod
    def _touch_pool(cls, dbname: Optional[str]) -> None:
        try:
            if dbname is None:
                return
            pool_tuple = cls._pools.get(dbname)
            if not pool_tuple:
                return
            pool, _ = pool_tuple
            cls._pools[dbname] = (pool, time.time())
        except Exception:
            pass

    @classmethod
    def _build_pool_kwargs(cls, db_name: str) -> dict[str, Any]:
        min_size = int(
            getattr(Config, "MARIADB_POOL_MIN", Config.DB_POOL_MIN) or Config.DB_POOL_MIN
        )
        max_size = int(
            getattr(Config, "MARIADB_POOL_MAX", Config.DB_POOL_MAX) or Config.DB_POOL_MAX
        )
        return {
            "db": db_name,
            "user": Config.MARIA_DB_USER,
            "password": Config.MARIA_DB_PASSWORD,
            "host": Config.MARIA_DB_HOST,
            "port": Config.MARIA_DB_PORT,
            "minsize": min_size,
            "maxsize": max_size,
            "charset": "utf8mb4",
            "pool_recycle": int(getattr(Config, "DB_POOL_RECYCLE", 300) or 300),
            "connect_timeout": int(getattr(Config, "MARIA_CONNECT_TIMEOUT", 5) or 5),
            "autocommit": True,
        }

    @classmethod
    async def _create_pool_instance(cls, db_name: str) -> Any:
        return await asyncmy.create_pool(**cls._build_pool_kwargs(db_name))

    @classmethod
    def _is_current_pool(cls, dbname: Optional[str], pool: Any) -> bool:
        if not dbname or pool is None:
            return False
        try:
            current = cls._pools.get(dbname)
            return bool(current and current[0] is pool)
        except Exception:
            return False

    @classmethod
    def _log_pool_status(cls, tag: str, level: int = logging.INFO) -> None:
        snapshots = [
            _format_pool_snapshot(dbname, pool, last_used)
            for dbname, (pool, last_used) in cls._pools.items()
        ]
        if not snapshots:
            return
        logger.log(level, "[MariaDB][%s] %s", tag, " | ".join(snapshots))

    @classmethod
    async def get_pool(cls, dbname=None):
        db_name = _normalize_dbname(dbname)

        async with cls._get_lock():
            pool_tuple = cls._pools.get(db_name)
            if pool_tuple is not None:
                pool, _ = pool_tuple
                metrics = _get_pool_metrics(pool)
                if metrics["closing"] or metrics["closed"]:
                    cls._pools.pop(db_name, None)
                else:
                    cls._pools[db_name] = (pool, time.time())
                    return pool

            try:
                pool_kwargs = cls._build_pool_kwargs(db_name)
                pool = await cls._create_pool_instance(db_name)
                cls._pools[db_name] = (pool, time.time())
                logger.info(
                    "[MariaDB][pool_create] db=%s host=%s port=%s min=%s max=%s recycle=%s snapshot=%s",
                    db_name,
                    pool_kwargs.get("host"),
                    pool_kwargs.get("port"),
                    pool_kwargs.get("minsize"),
                    pool_kwargs.get("maxsize"),
                    pool_kwargs.get("pool_recycle"),
                    _format_pool_snapshot(db_name, pool, cls._pools[db_name][1]),
                )

                if cls._cleanup_task is None or cls._cleanup_task.done():
                    cls._cleanup_task = asyncio.create_task(cls._auto_cleanup())
                return pool
            except Exception as exc:
                logger.error(
                    "[MariaDB][pool_create_failed] db=%s host=%s port=%s min=%s max=%s recycle=%s err=%s",
                    db_name,
                    getattr(Config, "MARIA_DB_HOST", None),
                    getattr(Config, "MARIA_DB_PORT", None),
                    getattr(Config, "MARIADB_POOL_MIN", Config.DB_POOL_MIN),
                    getattr(Config, "MARIADB_POOL_MAX", Config.DB_POOL_MAX),
                    getattr(Config, "DB_POOL_RECYCLE", None),
                    exc,
                )
                raise

    @classmethod
    async def _auto_cleanup(cls):
        while True:
            try:
                await asyncio.sleep(Config.DB_POOL_CHCK)
                cls._log_pool_status("periodic.status.before_cleanup")
                await cls.release_unused_pools()
                cls._log_pool_status("periodic.status.after_cleanup")
            except asyncio.CancelledError:
                logger.info("Auto cleanup task terminated")
                break
            except Exception as exc:
                logger.error("Auto cleanup error: %s", exc)

    @classmethod
    async def release_unused_pools(cls, timeout=None):
        if timeout is None:
            timeout = Config.DB_POOL_CHCK

        idle_pools: list[tuple[str, Any, float]] = []
        async with cls._get_lock():
            current_time = time.time()
            for dbname, (pool, last_used) in list(cls._pools.items()):
                metrics = _get_pool_metrics(pool)
                if metrics["used_size"] > 0 or metrics["acquiring"] > 0:
                    continue
                if current_time - last_used <= timeout:
                    continue
                cls._pools.pop(dbname, None)
                idle_pools.append((dbname, pool, last_used))

        for dbname, pool, last_used in idle_pools:
            try:
                pool.close()
                await pool.wait_closed()
                logger.info(
                    "[MariaDB][idle_pool_released] db=%s snapshot=%s",
                    dbname,
                    _format_pool_snapshot(dbname, pool, last_used),
                )
            except Exception as exc:
                logger.error("MariaDB pool release failed %s: %s", dbname, exc)

    @classmethod
    def get_pool_status(cls):
        current_time = time.time()
        status = {}

        for dbname, (pool, last_used) in cls._pools.items():
            metrics = _get_pool_metrics(pool)
            share = _get_job_share_snapshot(dbname)
            status[dbname] = {
                "pool_id": hex(id(pool)),
                "min_size": metrics["min_size"],
                "max_size": metrics["max_size"],
                "pool_size": metrics["pool_size"],
                "free_size": metrics["free_size"],
                "used_size": metrics["used_size"],
                "acquiring": metrics["acquiring"],
                "terminated": metrics["terminated"],
                "closing": metrics["closing"],
                "closed": metrics["closed"],
                "last_used_seconds_ago": current_time - last_used,
                "is_idle": (current_time - last_used) > Config.DB_POOL_CHCK,
                "job_share_active_jobs": share["active_jobs"],
                "job_share_per_job_cap": share["per_job_cap"],
                "job_share_db_in_use": share["db_in_use"],
                "job_share_global_in_use": share["global_in_use"],
                "job_share_total_cap": share["total_cap"],
            }

        return status

    @classmethod
    async def close_all_pools(cls) -> None:
        logger.info("Closing %s MariaDB connection pools", len(cls._pools))

        if cls._cleanup_task and not cls._cleanup_task.done():
            cls._cleanup_task.cancel()
            try:
                await cls._cleanup_task
            except asyncio.CancelledError:
                pass

        pools_to_close: list[tuple[str, Any]] = []
        async with cls._get_lock():
            pools_to_close = list(cls._pools.items())
            cls._pools.clear()
            _MARIADB_JOB_SHARE_COUNTS.clear()

        for dbname, (pool, _) in pools_to_close:
            logger.info("Closing MariaDB pool: %s", dbname)
            try:
                pool.close()
                await pool.wait_closed()
                logger.info("MariaDB pool closed %s", dbname)
            except Exception as exc:
                logger.error("MariaDB pool close failed: %s - %s", dbname, exc)

        logger.info("All MariaDB connection pools closed")


async def _drain_closed_pool(
    pool: Any,
    dbname: str,
    reason: str,
    *,
    close_delay_sec: float = 0.0,
    log_tag: str = "pool_drain_done",
) -> None:
    try:
        if close_delay_sec > 0:
            await asyncio.sleep(close_delay_sec)
        try:
            pool.close()
        except Exception:
            pass
        await pool.wait_closed()
        logger.info("[MariaDB][%s] db=%s reason=%s", log_tag, dbname, reason)
    except Exception as exc:
        logger.debug("[MariaDB][%s] db=%s reason=%s err=%s", log_tag.replace("done", "failed"), dbname, reason, exc)


async def _soft_refresh_pool(dbname: Optional[str], reason: str) -> bool:
    if not dbname:
        return False

    pool = None
    snapshot = ""
    async with MariaDBPool._get_lock():
        pool_tuple = MariaDBPool._pools.get(dbname)
        if pool_tuple is None:
            return False
        pool, last_used = pool_tuple
        metrics = _get_pool_metrics(pool)
        if metrics["closing"] or metrics["closed"]:
            return False
        snapshot = _format_pool_snapshot(dbname, pool, last_used)

    try:
        timeout_sec = _get_pool_soft_refresh_timeout_sec()
        await asyncio.wait_for(pool.clear(), timeout=timeout_sec)
        MariaDBPool._touch_pool(dbname)
        return True
    except asyncio.TimeoutError:
        logger.log(
            _get_transient_warning_log_level(),
            "[MariaDB][soft_refresh_timeout] db=%s reason=%s timeout=%.2fs pool=%s",
            dbname,
            reason,
            _get_pool_soft_refresh_timeout_sec(),
            snapshot,
        )
        return False
    except asyncio.CancelledError:
        logger.debug(
            "[MariaDB][soft_refresh_cancelled] db=%s reason=%s pool=%s",
            dbname,
            reason,
            snapshot,
        )
        raise
    except Exception as exc:
        logger.log(
            _get_transient_warning_log_level(),
            "[MariaDB][soft_refresh_failed] db=%s reason=%s err=%s",
            dbname,
            reason,
            exc,
        )
        return False


async def _full_recreate_pool(dbname: Optional[str], reason: str) -> bool:
    if not dbname:
        return False
    recreate_lock = _get_pool_recreate_lock(dbname)

    async with recreate_lock:
        cooldown_sec = _get_pool_recreate_cooldown_sec()
        now = time.time()
        recent_recreate_ts = float(_MARIADB_RECENT_RECREATE_TS.get(dbname, 0.0) or 0.0)
        async with MariaDBPool._get_lock():
            current_tuple = MariaDBPool._pools.get(dbname)
            current_pool = current_tuple[0] if current_tuple else None
            current_last_used = current_tuple[1] if current_tuple else None
        if (
            cooldown_sec > 0
            and recent_recreate_ts > 0
            and now - recent_recreate_ts <= cooldown_sec
            and _pool_is_usable(current_pool)
            and not _pool_has_no_connections(current_pool)
        ):
            return True

        old_pool = None
        old_metrics = None
        old_snapshot = ""
        async with MariaDBPool._get_lock():
            pool_tuple = MariaDBPool._pools.get(dbname)
            if pool_tuple is not None:
                old_pool, last_used = pool_tuple
                old_metrics = _get_pool_metrics(old_pool)
                old_snapshot = _format_pool_snapshot(dbname, old_pool, last_used)

        try:
            if old_pool is None or old_metrics is None:
                replacement_pool = await MariaDBPool._create_pool_instance(dbname)
                async with MariaDBPool._get_lock():
                    current_tuple = MariaDBPool._pools.get(dbname)
                    current_pool = current_tuple[0] if current_tuple else None
                    current_metrics = _get_pool_metrics(current_pool) if current_pool is not None else {}
                    if current_pool is None or current_metrics.get("closing") or current_metrics.get("closed"):
                        MariaDBPool._pools[dbname] = (replacement_pool, time.time())
                        _MARIADB_RECENT_RECREATE_TS[dbname] = time.time()
                        if MariaDBPool._cleanup_task is None or MariaDBPool._cleanup_task.done():
                            MariaDBPool._cleanup_task = asyncio.create_task(MariaDBPool._auto_cleanup())
                        logger.log(
                            _get_transient_warning_log_level(),
                            "[MariaDB][full_recreate_from_empty] db=%s reason=%s snapshot=%s",
                            dbname,
                            reason,
                            _format_pool_snapshot(dbname, replacement_pool, time.time()),
                        )
                        return True
                try:
                    replacement_pool.close()
                    await replacement_pool.wait_closed()
                except Exception:
                    pass
                return True

            replacement_pool = await MariaDBPool._create_pool_instance(dbname)
            replaced = False
            async with MariaDBPool._get_lock():
                current_tuple = MariaDBPool._pools.get(dbname)
                current_pool = current_tuple[0] if current_tuple else None
                current_metrics = _get_pool_metrics(current_pool) if current_pool is not None else {}
                if (
                    current_pool is old_pool
                    or current_pool is None
                    or current_metrics.get("closing")
                    or current_metrics.get("closed")
                ):
                    MariaDBPool._pools[dbname] = (replacement_pool, time.time())
                    _MARIADB_RECENT_RECREATE_TS[dbname] = time.time()
                    replaced = True
                    if MariaDBPool._cleanup_task is None or MariaDBPool._cleanup_task.done():
                        MariaDBPool._cleanup_task = asyncio.create_task(MariaDBPool._auto_cleanup())

            if not replaced:
                try:
                    replacement_pool.close()
                    await replacement_pool.wait_closed()
                except Exception:
                    pass
                return True

            asyncio.create_task(
                _drain_closed_pool(
                    old_pool,
                    dbname,
                    reason,
                    close_delay_sec=_get_pool_recreate_close_grace_sec(),
                    log_tag="full_recreate.drain_done",
                )
            )
            logger.log(
                _get_transient_warning_log_level(),
                "[MariaDB][full_recreate] db=%s reason=%s old_snapshot=%s new_snapshot=%s",
                dbname,
                reason,
                old_snapshot,
                _format_pool_snapshot(dbname, replacement_pool, time.time()),
            )
            return True
        except Exception as exc:
            logger.error(
                "[MariaDB][full_recreate_failed] db=%s reason=%s snapshot=%s err=%s",
                dbname,
                reason,
                old_snapshot,
                exc,
            )
            return False


async def _run_mariadb_operation_with_retry(
    dbname: Optional[str],
    op_name: str,
    executor: Callable[[Any], Awaitable[T]],
) -> T:
    delay = DB_RETRY_INITIAL_DELAY_SEC
    last_exc: Optional[Exception] = None

    for attempt in range(1, DB_MAX_RETRY_ATTEMPTS + 1):
        conn = None
        discard_conn = False
        attempt_t0 = time.perf_counter()
        connect_ms: Optional[float] = None
        executor_ms: Optional[float] = None
        release_ms: Optional[float] = None
        op_ok = False
        try:
            connect_t0 = time.perf_counter()
            conn = await mariadb_connect(dbname)
            try:
                setattr(conn, "_mariadb_op_name", op_name)
                _update_active_holder_op(conn, op_name)
            except Exception:
                pass
            connect_ms = (time.perf_counter() - connect_t0) * 1000.0
            if not _is_connection_healthy(conn):
                discard_conn = True
                raise ConnectionError("unhealthy maria connection")
            executor_t0 = time.perf_counter()
            result = await executor(conn)
            executor_ms = (time.perf_counter() - executor_t0) * 1000.0
            op_ok = True
            return result
        except Exception as exc:
            last_exc = exc
            discard_conn = True
            should_retry = _should_retry_maria_error(exc)
            if _is_disconnect_like_error(exc):
                logger.log(
                    _get_transient_warning_log_level(),
                    "[MariaDB][query_disconnect] db=%s attempt=%s/%s op=%s err=%s",
                    dbname,
                    attempt,
                    DB_MAX_RETRY_ATTEMPTS,
                    op_name,
                    exc,
                )
            if not should_retry or attempt >= DB_MAX_RETRY_ATTEMPTS:
                logger.error(
                    "[MariaDB] %s failed(db=%s, attempt=%s/%s): %s",
                    op_name,
                    dbname,
                    attempt,
                    DB_MAX_RETRY_ATTEMPTS,
                    exc,
                )
                raise

            if dbname and _is_disconnect_like_error(exc):
                if attempt == 1:
                    refreshed = await _soft_refresh_pool(
                        dbname, f"{op_name}:attempt={attempt}:disconnect"
                    )
                    if not refreshed:
                        await _full_recreate_pool(dbname, f"{op_name}:attempt={attempt}:disconnect")
                else:
                    await _full_recreate_pool(dbname, f"{op_name}:attempt={attempt}:disconnect")

            logger.log(
                _get_transient_warning_log_level(),
                "[MariaDB] %s retry scheduled(db=%s, attempt=%s/%s, wait=%.2fs): %s",
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
            if conn is not None:
                release_t0 = time.perf_counter()
                try:
                    await mariadb_release(conn, dbname, discard=discard_conn)
                    release_ms = (time.perf_counter() - release_t0) * 1000.0
                except Exception as return_exc:
                    release_ms = (time.perf_counter() - release_t0) * 1000.0
                    logger.error(
                        "[MariaDB] Connection return failed db=%s discard=%s error=%s",
                        dbname,
                        discard_conn,
                        return_exc,
                        exc_info=True,
                    )
                    try:
                        await asyncio.wait_for(_close_connection_safely(conn), timeout=10.0)
                        logger.log(
                            _get_transient_warning_log_level(),
                            "[MariaDB] Force close succeeded after return failure db=%s",
                            dbname,
                        )
                    except asyncio.TimeoutError:
                        logger.error("[MariaDB] Force close timeout - pool leak possible db=%s", dbname)
                    except Exception as close_exc:
                        logger.critical(
                            "[MariaDB] Force close failed - pool leak confirmed! db=%s error=%s",
                            dbname,
                            close_exc,
                        )
            total_ms = (time.perf_counter() - attempt_t0) * 1000.0
            slow_ms = _get_operation_slow_log_ms()
            op_name_text = str(op_name or "")
            is_file_subject_size_lookup = op_name_text.startswith(
                "file_duplicate_subject_size_lookup"
            )
            if op_ok and is_file_subject_size_lookup:
                bottleneck = _classify_mariadb_slow(
                    connect_ms=connect_ms,
                    executor_ms=executor_ms,
                    release_ms=release_ms,
                )
                logger.info(
                    "[MariaDB][file_duplicate_lookup] db=%s op=%s attempt=%s/%s total_ms=%.1f connect_ms=%s executor_ms=%s release_ms=%s bottleneck=%s pool=%s discard=%s",
                    dbname,
                    op_name_text,
                    attempt,
                    DB_MAX_RETRY_ATTEMPTS,
                    total_ms,
                    f"{connect_ms:.1f}" if connect_ms is not None else "-",
                    f"{executor_ms:.1f}" if executor_ms is not None else "-",
                    f"{release_ms:.1f}" if release_ms is not None else "-",
                    bottleneck,
                    _current_pool_snapshot(dbname),
                    discard_conn,
                )
            if op_ok and slow_ms >= 0 and total_ms >= slow_ms:
                bottleneck = _classify_mariadb_slow(
                    connect_ms=connect_ms,
                    executor_ms=executor_ms,
                    release_ms=release_ms,
                )
                pool_snapshot = _current_pool_snapshot(dbname)
                # Slow DB operations need their full timing breakdown in
                # operational logs; conn_hold_slow alone cannot distinguish
                # pool acquisition from SQL execution.
                logger.warning(
                    "[MariaDB][op_slow] db=%s op=%s attempt=%s/%s total_ms=%.1f connect_ms=%s executor_ms=%s release_ms=%s bottleneck=%s pool=%s discard=%s",
                    dbname,
                    op_name,
                    attempt,
                    DB_MAX_RETRY_ATTEMPTS,
                    total_ms,
                    f"{connect_ms:.1f}" if connect_ms is not None else "-",
                    f"{executor_ms:.1f}" if executor_ms is not None else "-",
                    f"{release_ms:.1f}" if release_ms is not None else "-",
                    bottleneck,
                    pool_snapshot,
                    discard_conn,
                )

    if last_exc:
        raise last_exc
    raise RuntimeError(f"[MariaDB] {op_name} failed: unknown error")


async def mariadb_connect(dbname=None):
    db_name = _normalize_dbname(dbname)
    last_exc: Optional[Exception] = None
    recovery_reason: Optional[str] = None
    connect_started = time.perf_counter()
    pool_get_ms = 0.0
    job_share_ms = 0.0
    pool_acquire_ms = 0.0
    pre_ping_ms = 0.0
    validation_failed_stage = ""
    validation_failed_stage_ms = 0.0
    validation_refresh_ms = 0.0
    acquire_before_pool = "pool=unknown"
    acquire_before_holders = "none"

    for attempt in range(1, 3):
        pool = None
        stage = "get_pool"
        refreshed_during_attempt = False
        try:
            pool_get_started = time.perf_counter()
            pool = await MariaDBPool.get_pool(db_name)
            pool_get_ms += (time.perf_counter() - pool_get_started) * 1000.0
            if pool is None:
                stage = "no_usable_pool"
                raise RuntimeError("No usable MariaDB pool before acquire")
            metrics = _get_pool_metrics(pool)
            if metrics["closing"] or metrics["closed"]:
                raise RuntimeError("Cannot acquire connection after closing pool")

            acquire_timeout = _get_pool_acquire_timeout_sec()
            validation_retries = _get_connect_validation_retry_count()

            for validation_attempt in range(1, validation_retries + 1):
                conn = None
                slot_granted = False
                stage = "job_share"
                stage_started = time.perf_counter()
                try:
                    job_share_started = time.perf_counter()
                    share_snapshot, slot_granted = await _acquire_job_share_slot(db_name, acquire_timeout)
                    job_share_ms += (time.perf_counter() - job_share_started) * 1000.0

                    latest_pool = await MariaDBPool.get_pool(db_name)
                    if latest_pool is not pool:
                        logger.info(
                            "[MariaDB][pool_refreshed_before_acquire] db=%s attempt=%s/2 validate=%s/%s old=%s new=%s",
                            db_name,
                            attempt,
                            validation_attempt,
                            validation_retries,
                            _format_pool_snapshot(db_name, pool, time.time()) if pool is not None else "missing",
                            _format_pool_snapshot(db_name, latest_pool, time.time()) if latest_pool is not None else "missing",
                        )
                        pool = latest_pool
                        recovery_reason = recovery_reason or "stale_pool"

                    if not _pool_is_usable(pool):
                        stage = "no_usable_pool"
                        refreshed_during_attempt = await _full_recreate_pool(
                            db_name,
                            f"connect:attempt={attempt}:validate={validation_attempt}:stage=pre_acquire_no_usable_pool",
                        )
                        pool = await MariaDBPool.get_pool(db_name)
                        if refreshed_during_attempt:
                            recovery_reason = recovery_reason or "no_usable_pool"
                        if not _pool_is_usable(pool):
                            raise RuntimeError("No usable MariaDB pool before acquire")
                    if not MariaDBPool._is_current_pool(db_name, pool):
                        stage = "stale_pool"
                        pool = await MariaDBPool.get_pool(db_name)
                        if not MariaDBPool._is_current_pool(db_name, pool):
                            raise RuntimeError("Stale MariaDB pool reference")
                        recovery_reason = recovery_reason or "stale_pool"
                    if _pool_has_no_connections(pool):
                        stage = "empty_pool"
                        logger.warning(
                            "[MariaDB][empty_pool_before_acquire] db=%s attempt=%s/2 validate=%s/%s snapshot=%s",
                            db_name,
                            attempt,
                            validation_attempt,
                            validation_retries,
                            _format_pool_snapshot(db_name, pool, time.time()),
                        )
                        refreshed_during_attempt = await _full_recreate_pool(
                            db_name,
                            f"connect:attempt={attempt}:validate={validation_attempt}:stage=empty_pool",
                        )
                        pool = await MariaDBPool.get_pool(db_name)
                        if not refreshed_during_attempt:
                            logger.warning(
                                "[MariaDB][empty_pool_recreate_not_applied] db=%s attempt=%s/2 validate=%s/%s snapshot=%s",
                                db_name,
                                attempt,
                                validation_attempt,
                                validation_retries,
                                _format_pool_snapshot(db_name, pool, time.time()),
                            )

                    stage = "acquire"
                    acquire_before_pool = _format_pool_snapshot(db_name, pool, time.time())
                    acquire_before_holders = _format_active_holder_snapshot(db_name)
                    stage_started = time.perf_counter()
                    pool_acquire_started = time.perf_counter()
                    conn = await asyncio.wait_for(pool.acquire(), timeout=acquire_timeout)
                    acquire_elapsed_ms = (time.perf_counter() - pool_acquire_started) * 1000.0
                    pool_acquire_ms += acquire_elapsed_ms
                    if acquire_elapsed_ms >= 1000.0 and logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "[MariaDB][acquire_task_snapshot] db=%s acquire_ms=%.1f attempt=%s/2 validate=%s/%s before_pool=%s before_holders=%s tasks=%s",
                            db_name,
                            acquire_elapsed_ms,
                            attempt,
                            validation_attempt,
                            validation_retries,
                            acquire_before_pool,
                            acquire_before_holders,
                            _format_running_task_snapshot(),
                        )
                    setattr(conn, "_pool", pool)
                    setattr(conn, "_pool_dbname", db_name)
                    setattr(conn, "_job_share_slot_granted", slot_granted)
                    setattr(conn, "_mariadb_acquired_perf", time.perf_counter())
                    setattr(conn, "_mariadb_acquired_at", time.time())
                    _register_active_holder(conn, db_name)
                    try:
                        task = asyncio.current_task()
                        setattr(conn, "_mariadb_task_name", task.get_name() if task else "")
                    except Exception:
                        setattr(conn, "_mariadb_task_name", "")
                    _install_job_share_close_guard(conn)

                    if not _pool_is_usable(pool) or not MariaDBPool._is_current_pool(db_name, pool):
                        stale_stage = "no_usable_pool" if not _pool_is_usable(pool) else "stale_pool"
                        logger.warning(
                            "[MariaDB][discard_stale_acquired_connection] db=%s attempt=%s/2 validate=%s/%s stage=%s snapshot=%s",
                            db_name,
                            attempt,
                            validation_attempt,
                            validation_retries,
                            stale_stage,
                            _format_pool_snapshot(db_name, pool, time.time()) if pool is not None else "missing",
                        )
                        try:
                            await mariadb_release(conn, db_name, discard=True)
                        except Exception:
                            await _close_connection_safely(conn)
                        conn = None
                        slot_granted = False
                        pool = await MariaDBPool.get_pool(db_name)
                        recovery_reason = recovery_reason or stale_stage
                        if validation_attempt < validation_retries:
                            continue
                        stage = stale_stage
                        raise RuntimeError(
                            "No usable MariaDB pool after acquire"
                            if stale_stage == "no_usable_pool"
                            else "Stale MariaDB pool reference"
                        )

                    stage = "health_check"
                    stage_started = time.perf_counter()
                    if not _is_connection_healthy(conn):
                        raise ConnectionError("unhealthy maria connection")

                    stage = "pre_ping"
                    stage_started = time.perf_counter()
                    pre_ping_started = time.perf_counter()
                    await _ping_connection(conn)
                    pre_ping_ms += (time.perf_counter() - pre_ping_started) * 1000.0
                    stage = "ready"
                    MariaDBPool._touch_pool(db_name)
                    connect_total_ms = (time.perf_counter() - connect_started) * 1000.0
                    if connect_total_ms >= _get_operation_slow_log_ms():
                        logger.warning(
                            "[MariaDB][connect_slow] db=%s total_ms=%.1f get_pool_ms=%.1f job_share_ms=%.1f acquire_ms=%.1f pre_ping_ms=%.1f validation_failed_stage=%s validation_failed_stage_ms=%.1f refresh_ms=%.1f attempt=%s/2 validate=%s/%s acquire_before_pool=%s acquire_before_holders=%s pool=%s",
                            db_name,
                            connect_total_ms,
                            pool_get_ms,
                            job_share_ms,
                            pool_acquire_ms,
                            pre_ping_ms,
                            validation_failed_stage or "-",
                            validation_failed_stage_ms,
                            validation_refresh_ms,
                            attempt,
                            validation_attempt,
                            validation_retries,
                            acquire_before_pool,
                            acquire_before_holders,
                            _format_pool_snapshot(db_name, pool, time.time()),
                        )
                    if recovery_reason:
                        logger.info(
                            "[MariaDB][recovered_after_%s] db=%s attempt=%s/2 validate=%s/%s snapshot=%s",
                            recovery_reason,
                            db_name,
                            attempt,
                            validation_attempt,
                            validation_retries,
                            _format_pool_snapshot(db_name, pool, time.time()) if pool is not None else "",
                        )
                        recovery_reason = None
                    return conn
                except Exception as exc:
                    last_exc = exc if isinstance(exc, Exception) else Exception(str(exc))
                    failed_stage_elapsed_ms = max(
                        0.0,
                        (time.perf_counter() - stage_started) * 1000.0,
                    )
                    metrics = _get_pool_metrics(pool) if pool is not None else {}
                    share_metrics = _get_job_share_snapshot(db_name)
                    err_code = _extract_error_code(last_exc)
                    is_validation_stage = stage in {"health_check", "pre_ping"}
                    is_retriable_acquire_stage = (
                        stage in {"acquire", "stale_pool", "no_usable_pool", "empty_pool"}
                        and validation_attempt < validation_retries
                        and _should_retry_maria_error(last_exc)
                        and _should_refresh_pool_after_connect_failure(last_exc, stage)
                    )
                    log_tag = (
                        "pre_ping_failed"
                        if stage == "pre_ping"
                        else "health_check_failed"
                        if stage == "health_check"
                        else "acquire_failed"
                    )
                    if is_validation_stage:
                        validation_failed_stage = stage
                        validation_failed_stage_ms += failed_stage_elapsed_ms
                        if validation_attempt < validation_retries:
                            logger.warning(
                                "[MariaDB][connect_validation_retry] db=%s attempt=%s/2 validate=%s/%s stage=%s stage_elapsed_ms=%.1f error_type=%s error=%s pool=%s",
                                db_name,
                                attempt,
                                validation_attempt,
                                validation_retries,
                                stage,
                                failed_stage_elapsed_ms,
                                type(last_exc).__name__,
                                _short_warning_value(last_exc, 240),
                                _format_pool_snapshot(db_name, pool, time.time()) if pool is not None else "missing",
                            )
                            logger.debug(
                                "[MariaDB][%s] MariaDB ??寃?寃?????? DB=%s ????%s/2 寃??%s/%s ??怨?%s "
                                "??寃??????湲곗떆??%.2f?????????湲곗떆??%.2f??"
                                "??_?????%s ??_????%s ??_??湲?%s ??_理??=%s "
                                "???낃났?????????%s ???낃났??DB蹂꾪븳??%s "
                                "???낃났??DB?????%s ???낃났????泥?????%s ???낃났????泥????%s "
                                "??瑜????%s ??瑜섏퐫??%s ??瑜?%s ??瑜????%r",
                                log_tag,
                                db_name,
                                attempt,
                                validation_attempt,
                                validation_retries,
                                stage,
                                _get_pool_acquire_timeout_sec(),
                                _get_pre_ping_timeout_sec(),
                                metrics.get("used_size"),
                                metrics.get("free_size"),
                                metrics.get("pool_size"),
                                metrics.get("max_size"),
                                share_metrics.get("active_jobs"),
                                share_metrics.get("per_job_cap"),
                                share_metrics.get("db_in_use"),
                                share_metrics.get("global_in_use"),
                                share_metrics.get("total_cap"),
                                type(last_exc).__name__,
                                err_code,
                                exc,
                                exc,
                            )
                        else:
                            suppressed = _consume_suppressed_warning_count(log_tag, db_name)
                            if suppressed >= 0:
                                logger.log(
                                    _get_transient_warning_log_level(),
                                    "[MariaDB][%s] MariaDB ??寃?寃?????? DB=%s ????%s/2 寃??%s/%s ??怨?%s "
                                    "??寃??????湲곗떆??%.2f?????????湲곗떆??%.2f??"
                                    "??_?????%s ??_????%s ??_??湲?%s ??_理??=%s "
                                    "???낃났?????????%s ???낃났??DB蹂꾪븳??%s "
                                    "???낃났??DB?????%s ???낃났????泥?????%s ???낃났????泥????%s "
                                    "??瑜????%s ??瑜섏퐫??%s ??瑜?%s ??瑜????%r ????????쇨꼍怨?%s",
                                    log_tag,
                                    db_name,
                                    attempt,
                                    validation_attempt,
                                    validation_retries,
                                    stage,
                                    _get_pool_acquire_timeout_sec(),
                                    _get_pre_ping_timeout_sec(),
                                    metrics.get("used_size"),
                                    metrics.get("free_size"),
                                    metrics.get("pool_size"),
                                    metrics.get("max_size"),
                                    share_metrics.get("active_jobs"),
                                    share_metrics.get("per_job_cap"),
                                    share_metrics.get("db_in_use"),
                                    share_metrics.get("global_in_use"),
                                    share_metrics.get("total_cap"),
                                    type(last_exc).__name__,
                                    err_code,
                                    exc,
                                    exc,
                                    suppressed,
                                )
                    elif is_retriable_acquire_stage:
                        logger.debug(
                            "[MariaDB][%s] MariaDB ??寃??????????????????: DB=%s ????%s/2 寃??%s/%s ??怨?%s "
                            "??寃??????湲곗떆??%.2f?????????湲곗떆??%.2f??"
                            "??_?????%s ??_????%s ??_??湲?%s ??_理??=%s "
                            "???낃났?????????%s ???낃났??DB蹂꾪븳??%s "
                            "???낃났??DB?????%s ???낃났????泥?????%s ???낃났????泥????%s "
                            "??瑜????%s ??瑜섏퐫??%s ??瑜?%s ??瑜????%r",
                            log_tag,
                            db_name,
                            attempt,
                            validation_attempt,
                            validation_retries,
                            stage,
                            _get_pool_acquire_timeout_sec(),
                            _get_pre_ping_timeout_sec(),
                            metrics.get("used_size"),
                            metrics.get("free_size"),
                            metrics.get("pool_size"),
                            metrics.get("max_size"),
                            share_metrics.get("active_jobs"),
                            share_metrics.get("per_job_cap"),
                            share_metrics.get("db_in_use"),
                            share_metrics.get("global_in_use"),
                            share_metrics.get("total_cap"),
                            type(last_exc).__name__,
                            err_code,
                            exc,
                            exc,
                        )
                    else:
                        logger.error(
                            "[MariaDB][%s] MariaDB ??寃????????? DB=%s ????%s/2 寃??%s/%s ??怨?%s "
                            "??寃??????湲곗떆??%.2f?????????湲곗떆??%.2f??"
                            "??_?????%s ??_????%s ??_??湲?%s ??_理??=%s "
                            "???낃났?????????%s ???낃났??DB蹂꾪븳??%s "
                            "???낃났??DB?????%s ???낃났????泥?????%s ???낃났????泥????%s "
                            "??瑜????%s ??瑜섏퐫??%s ??瑜?%s ??瑜????%r",
                            log_tag,
                            db_name,
                            attempt,
                            validation_attempt,
                            validation_retries,
                            stage,
                            _get_pool_acquire_timeout_sec(),
                            _get_pre_ping_timeout_sec(),
                            metrics.get("used_size"),
                            metrics.get("free_size"),
                            metrics.get("pool_size"),
                            metrics.get("max_size"),
                            share_metrics.get("active_jobs"),
                            share_metrics.get("per_job_cap"),
                            share_metrics.get("db_in_use"),
                            share_metrics.get("global_in_use"),
                            share_metrics.get("total_cap"),
                            type(last_exc).__name__,
                            err_code,
                            exc,
                            exc,
                        )

                    if conn is not None:
                        try:
                            await mariadb_release(conn, db_name, discard=True)
                        except Exception:
                            await _close_connection_safely(conn)
                    elif slot_granted:
                        await _release_job_share_slot(db_name)

                    if (
                        validation_attempt < validation_retries
                        and (is_validation_stage or is_retriable_acquire_stage)
                    ):
                        if db_name and _should_refresh_pool_after_connect_failure(last_exc, stage):
                            refresh_reason = (
                                f"connect:attempt={attempt}:validate={validation_attempt}:"
                                f"stage={stage}:{type(last_exc).__name__}"
                            )
                            refresh_started = time.perf_counter()
                            refreshed_during_attempt = await _refresh_pool_for_retry(
                                db_name,
                                refresh_reason,
                            )
                            refresh_elapsed_ms = (time.perf_counter() - refresh_started) * 1000.0
                            validation_refresh_ms += refresh_elapsed_ms
                            logger.warning(
                                "[MariaDB][connect_validation_refresh] db=%s stage=%s refresh_ms=%.1f refreshed=%s reason=%s",
                                db_name,
                                stage,
                                refresh_elapsed_ms,
                                refreshed_during_attempt,
                                refresh_reason,
                            )
                            if refreshed_during_attempt and stage in {"no_usable_pool", "stale_pool", "empty_pool", "acquire", "health_check", "pre_ping"}:
                                recovery_reason = stage
                        logger.debug(
                            "[MariaDB][reacquire_after_%s] db=%s attempt=%s/2 next_validate=%s/%s",
                            stage,
                            db_name,
                            attempt,
                            validation_attempt + 1,
                            validation_retries,
                        )
                        continue
                    break

            if last_exc is not None:
                raise last_exc
            raise RuntimeError("MariaDB connection validation failed without exception")
        except Exception as exc:
            last_exc = exc if isinstance(exc, Exception) else Exception(str(exc))
            metrics = _get_pool_metrics(pool) if pool is not None else {}
            share_metrics = _get_job_share_snapshot(db_name)
            err_code = _extract_error_code(last_exc)
            connect_failed_log = logger.error
            if attempt < 2 and stage in {"no_usable_pool", "stale_pool"}:
                connect_failed_log = lambda msg, *args, **kwargs: logger.log(
                    _get_transient_warning_log_level(),
                    msg,
                    *args,
                    **kwargs,
                )
            connect_failed_log(
                "[MariaDB][connect_failed] acquire failed | db=%s attempt=%s/2 stage=%s "
                "acquire_timeout=%.2fs pre_ping_timeout=%.2fs "
                "pool_used=%s pool_free=%s pool_size=%s pool_max=%s "
                "active_jobs=%s per_job_cap=%s db_in_use=%s global_in_use=%s total_cap=%s "
                "error_type=%s error_code=%s error=%s error_repr=%r holders=%s",
                db_name,
                attempt,
                stage,
                _get_pool_acquire_timeout_sec(),
                _get_pre_ping_timeout_sec(),
                metrics.get("used_size"),
                metrics.get("free_size"),
                metrics.get("pool_size"),
                metrics.get("max_size"),
                share_metrics.get("active_jobs"),
                share_metrics.get("per_job_cap"),
                share_metrics.get("db_in_use"),
                share_metrics.get("global_in_use"),
                share_metrics.get("total_cap"),
                type(last_exc).__name__,
                err_code,
                exc,
                exc,
                _format_active_holder_snapshot(db_name),
            )

            if (
                stage == "no_usable_pool"
                and attempt < 2
            ):
                refreshed_during_attempt = await _full_recreate_pool(
                    db_name,
                    f"connect:attempt={attempt}:stage={stage}:{type(last_exc).__name__}",
                )
                logger.log(
                    _get_transient_warning_log_level(),
                    "[MariaDB][retry_after_no_usable_pool] db=%s attempt=%s/2 refreshed=%s",
                    db_name,
                    attempt,
                    refreshed_during_attempt,
                )
                if refreshed_during_attempt:
                    recovery_reason = "no_usable_pool"
                    continue

            if attempt >= 2 or not _should_retry_maria_error(last_exc):
                break

            if (
                not refreshed_during_attempt
                and _should_refresh_pool_after_connect_failure(last_exc, stage)
            ):
                await _refresh_pool_for_retry(
                    db_name,
                    f"connect:attempt={attempt}:stage={stage}:{type(last_exc).__name__}",
                )

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("MariaDB connection failed: unknown error")


async def mariadb_release(conn, dbname=None, discard: bool = False):
    if not conn:
        return

    _unregister_active_holder(conn)
    pool = getattr(conn, "_pool", None)
    resolved_dbname = getattr(conn, "_pool_dbname", None) or dbname
    share_slot_granted = bool(getattr(conn, "_job_share_slot_granted", False))

    if pool is None and resolved_dbname is not None:
        try:
            pool = await MariaDBPool.get_pool(resolved_dbname)
        except Exception as exc:
            logger.error("MariaDB pool resolve failed during release(db=%s): %s", resolved_dbname, exc)

    if resolved_dbname is None and pool is not None:
        try:
            for logical_name, (known_pool, _) in MariaDBPool._pools.items():
                if known_pool is pool:
                    resolved_dbname = logical_name
                    break
        except Exception:
            pass

    try:
        try:
            acquired_perf = float(getattr(conn, "_mariadb_acquired_perf", 0.0) or 0.0)
        except Exception:
            acquired_perf = 0.0
        hold_ms = (time.perf_counter() - acquired_perf) * 1000.0 if acquired_perf > 0 else -1.0
        op_name_for_warn = getattr(conn, "_mariadb_op_name", "") or "direct"
        warn_ms = _get_connection_hold_warn_ms(op_name_for_warn)
        if hold_ms >= warn_ms and warn_ms >= 0:
            try:
                share_metrics = _get_job_share_snapshot(resolved_dbname)
            except Exception:
                share_metrics = {}
            logger.warning(
                "[MariaDB][conn_hold_slow] db=%s hold_ms=%.1f warn_ms=%.1f discard=%s op=%s task=%s pool=%s share=%s workers=%s",
                resolved_dbname,
                hold_ms,
                warn_ms,
                discard,
                op_name_for_warn,
                getattr(conn, "_mariadb_task_name", "") or "-",
                _current_pool_snapshot(resolved_dbname),
                share_metrics,
                _mariadb_pool_worker_summary(),
            )
        if discard:
            try:
                await _close_connection_safely(conn)
            except Exception:
                pass
            return
        if pool is not None:
            try:
                pool.release(conn)
                MariaDBPool._touch_pool(resolved_dbname)
            except Exception as exc:
                logger.error("MariaDB connection return failed(db=%s): %s", resolved_dbname, exc)
                try:
                    await _close_connection_safely(conn)
                except Exception:
                    pass
        else:
            logger.error(
                "MariaDB connection return failed: pool reference not found (db=%s, discard=%s)",
                dbname,
                discard,
            )
            try:
                await _close_connection_safely(conn)
            except Exception:
                pass
    finally:
        if share_slot_granted:
            try:
                await _release_job_share_slot(resolved_dbname)
            finally:
                try:
                    setattr(conn, "_job_share_slot_granted", False)
                except Exception:
                    pass

async def mariadb_execute(query, params=None, fetch=False, dbname=None, op_name: Optional[str] = None):
    async def _executor(conn):
        if fetch:
            async with conn.cursor(DictCursor) as cursor:
                await mariadb_wait_for_query(cursor.execute(query, params or ()))
                return await mariadb_wait_for_query(cursor.fetchall())
        try:
            await conn.autocommit(False)
        except Exception:
            pass
        try:
            async with conn.cursor() as cursor:
                await mariadb_wait_for_query(cursor.execute(query, params or ()))
                rowcount = int(getattr(cursor, "rowcount", 0) or 0)
                await _log_content_author_warnings(
                    cursor,
                    query=query,
                    params=params or (),
                    dbname=dbname,
                    op_name=op_name or "exec",
                )
            try:
                await mariadb_wait_for_query(conn.commit())
            except Exception:
                pass
            return rowcount
        except Exception:
            try:
                await mariadb_wait_for_query(conn.rollback())
            except Exception:
                pass
            raise
        finally:
            try:
                await conn.autocommit(True)
            except Exception:
                pass

    resolved_op_name = op_name or f"{'fetch' if fetch else 'exec'}:{str(query or '').strip()[:60]}"
    return await _run_mariadb_operation_with_retry(dbname, resolved_op_name, _executor)


async def mariadb_executemany(query: str, params_list, dbname: Optional[str] = None) -> int:
    db_name = _normalize_dbname(dbname)

    async def _executor(conn):
        try:
            await conn.autocommit(False)
        except Exception:
            pass
        try:
            async with conn.cursor() as cursor:
                await mariadb_wait_for_query(cursor.executemany(query, params_list or []))
            try:
                await mariadb_wait_for_query(conn.commit())
            except Exception:
                pass
        except Exception:
            try:
                await mariadb_wait_for_query(conn.rollback())
            except Exception:
                pass
            raise
        finally:
            try:
                await conn.autocommit(True)
            except Exception:
                pass
        return len(params_list or [])

    op_name = f"executemany:{str(query or '').strip()[:60]}"
    return await _run_mariadb_operation_with_retry(db_name, op_name, _executor)


async def get_global_connection_diagnostics(dbname: Optional[str] = None) -> dict[str, Any]:
    target_db = (str(dbname or "").strip()) or next(iter(MariaDBPool._pools.keys()), "")
    if not target_db:
        return {}

    conn = None
    try:
        conn = await mariadb_connect(target_db)
        async with conn.cursor(DictCursor) as cursor:
            await cursor.execute("SHOW VARIABLES LIKE 'max_connections'")
            max_rows = await cursor.fetchall()

            await cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
            connected_rows = await cursor.fetchall()

            await cursor.execute("SHOW STATUS LIKE 'Threads_running'")
            running_rows = await cursor.fetchall()

            db_connections = None
            try:
                await cursor.execute(
                    "SELECT COUNT(*) AS db_connections FROM information_schema.PROCESSLIST WHERE DB = %s",
                    (target_db,),
                )
                processlist_rows = await cursor.fetchall()
                if processlist_rows:
                    db_connections = int((processlist_rows[0] or {}).get("db_connections", 0) or 0)
            except Exception as exc:
                logger.debug("[MariaDB] processlist diagnostic unavailable db=%s err=%s", target_db, exc)

        def _parse_show_like(rows: list[dict[str, Any]] | None) -> Optional[int]:
            if not rows:
                return None
            try:
                return int((rows[0] or {}).get("Value"))
            except Exception:
                return None

        return {
            "db": target_db,
            "max_connections": _parse_show_like(max_rows),
            "threads_connected": _parse_show_like(connected_rows),
            "threads_running": _parse_show_like(running_rows),
            "db_connections": db_connections,
        }
    finally:
        if conn is not None:
            try:
                await mariadb_release(conn, target_db, discard=False)
            except Exception as exc:
                logger.debug("[MariaDB] diagnostic connection release failed db=%s err=%s", target_db, exc)


async def mariadb_cleanup_on_shutdown():
    await MariaDBPool.close_all_pools()


