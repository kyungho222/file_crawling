"""Bounded async queue for DB writes that should not fan out uncontrolled."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class _DBWriteJob:
    label: str
    callback: Callable[[], Any]
    future: asyncio.Future
    enqueued_at: float


_queue: Optional[asyncio.Queue[_DBWriteJob]] = None
_queue_loop: Optional[asyncio.AbstractEventLoop] = None
_workers: list[asyncio.Task] = []
_log_queue: Optional[asyncio.Queue[_DBWriteJob]] = None
_log_queue_loop: Optional[asyncio.AbstractEventLoop] = None
_log_workers: list[asyncio.Task] = []
_ensure_lock = asyncio.Lock()
_active = 0
_completed = 0
_failed = 0
_worker_id_seq = 0
_active_jobs: Dict[int, Dict[str, Any]] = {}
_IN_DB_WRITE_WORKER: contextvars.ContextVar[bool] = contextvars.ContextVar("in_db_write_worker", default=False)


def in_db_write_worker() -> bool:
    try:
        return bool(_IN_DB_WRITE_WORKER.get())
    except Exception:
        return False


def db_write_queue_enabled() -> bool:
    raw = str(os.getenv("DB_WRITE_QUEUE_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def db_write_queue_workers() -> int:
    try:
        value = int((os.getenv("DB_WRITE_QUEUE_WORKERS") or "2").strip())
    except Exception:
        value = 2
    return max(1, min(value, 64))

def db_write_log_queue_enabled() -> bool:
    raw = str(os.getenv("DB_WRITE_LOG_QUEUE_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def db_write_log_queue_workers() -> int:
    try:
        value = int((os.getenv("DB_WRITE_LOG_QUEUE_WORKERS") or "1").strip())
    except Exception:
        value = 1
    return max(1, min(value, 32))

def db_write_queue_maxsize() -> int:
    try:
        value = int((os.getenv("DB_WRITE_QUEUE_MAXSIZE") or "5000").strip())
    except Exception:
        value = 5000
    return max(1, min(value, 100000))


def db_write_log_queue_maxsize() -> int:
    try:
        value = int((os.getenv("DB_WRITE_LOG_QUEUE_MAXSIZE") or "1000").strip())
    except Exception:
        value = 1000
    return max(1, min(value, 100000))


def db_write_queue_timeout_sec() -> float:
    try:
        value = float((os.getenv("DB_WRITE_QUEUE_TIMEOUT_SEC") or "120").strip())
    except Exception:
        value = 120.0
    return max(1.0, value)


def _is_log_db_write(label: str) -> bool:
    if not db_write_log_queue_enabled():
        return False
    normalized = str(label or "").strip().lower()
    return normalized.startswith("crawling_log.")


async def run_db_write(
    label: str,
    callback: Callable[[], Any],
    *,
    timeout_sec: Optional[float] = None,
) -> Any:
    if not db_write_queue_enabled() or in_db_write_worker():
        result = callback()
        if inspect.isawaitable(result):
            return await result
        return result

    loop = asyncio.get_running_loop()
    is_log_write = _is_log_db_write(label)
    queue = await _ensure_queue(loop, log_queue=is_log_write)
    future = loop.create_future()
    enqueued_at = time.monotonic()
    await queue.put(_DBWriteJob(label=str(label or "db_write"), callback=callback, future=future, enqueued_at=enqueued_at))
    timeout = db_write_queue_timeout_sec() if timeout_sec is None else timeout_sec
    if timeout and timeout > 0:
        wait_started_at = time.monotonic()
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        except asyncio.TimeoutError:
            wait_elapsed = time.monotonic() - wait_started_at
            status = db_write_queue_status()
            logger.warning(
                "[DBWriteQueue] wait timeout | label=%s queue=%s timeout=%.1fs wait_elapsed=%.3fs timeout_overrun=%.3fs queue_wait_before_wait=%.3fs pending=%s active=%s completed=%s failed=%s workers=%s/%s maxsize=%s active_jobs=%s",
                label,
                "log" if is_log_write else "default",
                timeout,
                wait_elapsed,
                max(0.0, wait_elapsed - timeout),
                max(0.0, wait_started_at - enqueued_at),
                status.get("pending"),
                status.get("active"),
                status.get("completed"),
                status.get("failed"),
                status.get("workers_alive"),
                status.get("workers_configured"),
                status.get("maxsize"),
                status.get("active_jobs"),
            )
            raise
    return await future


def _next_worker_id() -> int:
    global _worker_id_seq
    _worker_id_seq += 1
    return _worker_id_seq


async def _ensure_queue(loop: asyncio.AbstractEventLoop, *, log_queue: bool = False) -> asyncio.Queue[_DBWriteJob]:
    global _queue, _queue_loop, _workers, _log_queue, _log_queue_loop, _log_workers
    async with _ensure_lock:
        if log_queue:
            if _log_queue_loop is not loop:
                _log_queue = asyncio.Queue(maxsize=db_write_log_queue_maxsize())
                _log_queue_loop = loop
                _log_workers = []
            if _log_queue is None:
                _log_queue = asyncio.Queue(maxsize=db_write_log_queue_maxsize())
            alive_workers = [task for task in _log_workers if not task.done()]
            _log_workers[:] = alive_workers
            while len(_log_workers) < db_write_log_queue_workers():
                _log_workers.append(loop.create_task(_db_write_worker(_next_worker_id(), log_queue=True)))
            return _log_queue

        if _queue_loop is not loop:
            _queue = asyncio.Queue(maxsize=db_write_queue_maxsize())
            _queue_loop = loop
            _workers = []
        if _queue is None:
            _queue = asyncio.Queue(maxsize=db_write_queue_maxsize())
        alive_workers = [task for task in _workers if not task.done()]
        _workers[:] = alive_workers
        while len(_workers) < db_write_queue_workers():
            _workers.append(loop.create_task(_db_write_worker(_next_worker_id())))
        return _queue


async def _db_write_worker(worker_id: int, *, log_queue: bool = False) -> None:
    global _active, _completed, _failed
    while True:
        queue = _log_queue if log_queue else _queue
        if queue is None:
            await asyncio.sleep(0.05)
            continue
        job = await queue.get()
        _active += 1
        started_at = time.monotonic()
        queue_wait_sec = max(0.0, started_at - job.enqueued_at)
        _active_jobs[worker_id] = {
            "label": job.label,
            "queue": "log" if log_queue else "default",
            "wait_sec": round(max(0.0, started_at - job.enqueued_at), 3),
            "started_at": started_at,
            "started_ago_sec": 0.0,
        }
        token = _IN_DB_WRITE_WORKER.set(True)
        slow_sec = _db_write_queue_job_slow_sec()
        if queue_wait_sec >= slow_sec:
            logger.warning(
                "[DBWriteQueue] slow queue wait | worker=%s label=%s queue=%s wait=%.3fs slow=%.3fs pending=%s",
                worker_id,
                job.label,
                "log" if log_queue else "default",
                queue_wait_sec,
                slow_sec,
                queue.qsize(),
            )
        try:
            result = job.callback()
            if inspect.isawaitable(result):
                result = await result
            run_sec = time.monotonic() - started_at
            _completed += 1
            if not job.future.cancelled():
                job.future.set_result(result)
            else:
                logger.warning(
                    "[DBWriteQueue] job completed after waiter timeout | worker=%s label=%s queue_wait=%.3fs run=%.3fs",
                    worker_id,
                    job.label,
                    started_at - job.enqueued_at,
                    run_sec,
                )
            if run_sec >= slow_sec:
                logger.warning(
                    "[DBWriteQueue] slow job completed | worker=%s label=%s queue_wait=%.3fs run=%.3fs slow=%.3fs",
                    worker_id,
                    job.label,
                    started_at - job.enqueued_at,
                    run_sec,
                    slow_sec,
                )
        except Exception as exc:
            run_sec = time.monotonic() - started_at
            _failed += 1
            logger.warning(
                "[DBWriteQueue] job failed | worker=%s label=%s queue_wait=%.3fs run=%.3fs future_cancelled=%s err=%s",
                worker_id,
                job.label,
                started_at - job.enqueued_at,
                run_sec,
                job.future.cancelled(),
                exc,
            )
            if not job.future.cancelled():
                job.future.set_exception(exc)
        finally:
            try:
                _IN_DB_WRITE_WORKER.reset(token)
            except Exception:
                pass
            _active = max(0, _active - 1)
            _active_jobs.pop(worker_id, None)
            queue.task_done()


def db_write_queue_status() -> Dict[str, Any]:
    queue = _queue
    log_queue = _log_queue
    workers = list(_workers)
    log_workers = list(_log_workers)
    pending = queue.qsize() if queue is not None else 0
    log_pending = log_queue.qsize() if log_queue is not None else 0
    default_workers_configured = db_write_queue_workers()
    log_workers_configured = db_write_log_queue_workers() if db_write_log_queue_enabled() else 0
    default_workers_alive = sum(1 for task in workers if not task.done())
    log_workers_alive = sum(1 for task in log_workers if not task.done())
    now = time.monotonic()
    active_jobs = []
    for worker_id, meta in sorted(_active_jobs.items()):
        item = {k: v for k, v in meta.items() if k != "started_at"}
        item["worker"] = worker_id
        try:
            item["started_ago_sec"] = round(max(0.0, now - float(meta.get("started_at", now))), 3)
        except Exception:
            pass
        active_jobs.append(item)
    return {
        "enabled": db_write_queue_enabled(),
        "log_queue_enabled": db_write_log_queue_enabled(),
        "pending": pending + log_pending,
        "default_pending": pending,
        "log_pending": log_pending,
        "active": _active,
        "completed": _completed,
        "failed": _failed,
        "workers_configured": default_workers_configured + log_workers_configured,
        "workers_alive": default_workers_alive + log_workers_alive,
        "default_workers_configured": default_workers_configured,
        "default_workers_alive": default_workers_alive,
        "log_workers_configured": log_workers_configured,
        "log_workers_alive": log_workers_alive,
        "maxsize": db_write_queue_maxsize() + (db_write_log_queue_maxsize() if db_write_log_queue_enabled() else 0),
        "default_maxsize": db_write_queue_maxsize(),
        "log_maxsize": db_write_log_queue_maxsize() if db_write_log_queue_enabled() else 0,
        "active_jobs": active_jobs,
    }


def _db_write_queue_job_slow_sec() -> float:
    try:
        value = float(os.getenv("DB_WRITE_QUEUE_JOB_SLOW_SEC", "3.0") or "3.0")
    except Exception:
        value = 3.0
    return max(0.1, value)


