import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional

from backend.shared.crawl_shared import send_sse_message
from backend.shared.crawl_trace import crawl_trace, elapsed_ms as trace_elapsed_ms

logger = logging.getLogger("backend.shared.sse_publish_queue")

# ==================== SSE Publish Priority Queue ====================
#
# Purpose:
# - Publish SSE/Redis PubSub progress events through a dedicated worker so
#   latency-sensitive UI updates do not compete directly with other workflow work.
# - Coalesce pending payloads by job_id so only the latest progress message is
#   published while preserving urgent terminal/stop events with higher priority.
#

_sse_publish_queue: "asyncio.PriorityQueue[tuple[int, float, str]]" = asyncio.PriorityQueue()
_sse_latest_by_job: Dict[str, Dict[str, Any]] = {}
_sse_latest_db_by_job: Dict[str, str] = {}
_sse_latest_source_by_job: Dict[str, str] = {}
_sse_latest_priority_by_job: Dict[str, int] = {}
_sse_job_enqueued: set[str] = set()
_sse_worker_task: Optional[asyncio.Task] = None
_sse_worker_lock = asyncio.Lock()
_sse_worker_heartbeat_ts: float = 0.0
_sse_last_published_ts_by_job: Dict[str, float] = {}
_sse_last_db_update_ts_by_job: Dict[str, float] = {}
_crawling_log_update_tasks: Dict[str, asyncio.Task] = {}
_crawling_log_latest_update_by_job: Dict[str, tuple[Dict[str, Any], str, Any]] = {}
_redis_publish_tasks: set[asyncio.Task] = set()
_redis_publish_tasks_by_job: Dict[str, asyncio.Task] = {}
_redis_publish_latest_by_job: Dict[str, tuple[Dict[str, Any], str, str]] = {}


_db_update_in_progress: set[str] = set()
_db_update_lock = asyncio.Lock()

_MONOTONIC_COUNT_KEYS = {
    "scan_count",
    "total_count",
    "collection_count",
    "save_count",
    "save_success_count",
    "save_done_count",
    "save_failed_count",
    "study_count",
    "study_done_count",
    "study_success_count",
    "file_study_count",
    "file_study_done_count",
    "file_study_success_count",
    "pending_collection_count",
    "pending_save_count",
    "error_count",
    "actual_scan_count",
    "actual_start_urls_count",
    "pre_explored_start_urls_count",
    "exploration_post_total_count",
    "exploration_display_max_count",
}


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _merge_counter_snapshot(existing: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    if not existing:
        return dict(payload or {})

    merged = dict(payload or {})
    existing_revision = _to_int(existing.get("stats_revision"), -1)
    payload_revision = _to_int(merged.get("stats_revision"), -1)
    if existing_revision >= 0 and payload_revision >= 0 and payload_revision < existing_revision:
        for key in _MONOTONIC_COUNT_KEYS:
            if key in existing:
                merged[key] = existing.get(key)
        if "progress_percentage" in existing:
            merged["progress_percentage"] = existing.get("progress_percentage")
        merged["stats_revision"] = existing.get("stats_revision")

    return merged


def _sse_db_update_interval_seconds() -> float:
    try:
        sec = float(os.getenv("SSE_DB_UPDATE_INTERVAL_SECONDS", "0.0") or "0.0")
    except Exception:
        sec = 0.0
    return max(0.0, min(sec, 60.0))


def _sse_db_update_before_publish_timeout_seconds() -> float:
    try:
        sec = float(os.getenv("SSE_DB_UPDATE_BEFORE_PUBLISH_TIMEOUT_SECONDS", "2.0") or "2.0")
    except Exception:
        sec = 2.0
    return max(0.0, min(sec, 30.0))

def _sse_redis_publish_timeout_seconds() -> float:
    try:
        sec = float(os.getenv("SSE_REDIS_PUBLISH_TIMEOUT_SECONDS", "3.0") or "3.0")
    except Exception:
        sec = 3.0
    return max(0.1, min(sec, 30.0))


def _sse_redis_publish_slow_ms() -> float:
    try:
        ms = float(os.getenv("SSE_REDIS_PUBLISH_SLOW_MS", "3000") or "3000")
    except Exception:
        ms = 3000.0
    return max(0.0, min(ms, 30000.0))


def _sse_redis_publish_task_limit() -> int:
    try:
        limit = int(os.getenv("SSE_REDIS_PUBLISH_TASK_LIMIT", "100") or "100")
    except Exception:
        limit = 100
    return max(1, min(limit, 1000))


def _is_terminal_status(status: Any) -> bool:
    return str(status or "").strip().lower() in {"completed", "cancelled", "error"}


def _sse_enqueue_slow_ms() -> float:
    try:
        return max(0.0, float(os.getenv("SSE_ENQUEUE_SLOW_MS", "50") or "50"))
    except Exception:
        return 50.0


def _trace_enqueue_result(
    *,
    state: str,
    job_id: str,
    elapsed_ms: float,
    payload: Dict[str, Any],
    source: str,
    priority: int,
    queue_priority: int,
    merge_ms: float,
) -> None:
    status = (payload or {}).get("status")
    slow_ms = _sse_enqueue_slow_ms()
    if elapsed_ms >= slow_ms:
        level = logging.WARNING
    else:
        level = logging.DEBUG
    if not logger.isEnabledFor(level):
        return
    crawl_trace(
        logger,
        phase="sse",
        action="enqueue_sse_message",
        state=state,
        job_id=job_id,
        level=level,
        elapsed_ms=elapsed_ms,
        counts={
            "queue_size": _sse_publish_queue.qsize(),
            "latest_jobs": len(_sse_latest_by_job),
            "payload_keys": len(payload or {}),
        },
        source=source,
        priority=priority,
        queue_priority=queue_priority,
        status=status,
        merge_ms=merge_ms,
        slow_ms=slow_ms,
    )


async def _run_redis_publish_task(
    *,
    job_id: str,
    payload: Dict[str, Any],
    db_name: str,
    source: str,
) -> None:
    redis_published = False
    redis_t0 = time.perf_counter()
    publish_task: Optional[asyncio.Task] = None
    channel = f"crawl:{db_name}:{job_id}:progress"
    crawl_trace(
        logger,
        phase="redis",
        action="queued_publish",
        state="start",
        job_id=job_id,
        source=source,
        status=(payload or {}).get("status"),
        channel=channel,
        timeout_sec=_sse_redis_publish_timeout_seconds(),
        task_count=len(_redis_publish_tasks),
    )
    try:
        publish_task = asyncio.create_task(
            send_sse_message(job_id, payload, db_name, source),
            name=f"sse-send:{job_id}",
        )
        await asyncio.wait_for(publish_task, timeout=_sse_redis_publish_timeout_seconds())
        redis_published = True
        _sse_last_published_ts_by_job[job_id] = time.time()
    except asyncio.TimeoutError:
        if publish_task is not None and not publish_task.done():
            publish_task.cancel()
        crawl_trace(
            logger,
            phase="redis",
            action="queued_publish",
            state="timeout",
            job_id=job_id,
            level=logging.WARNING,
            elapsed_ms=trace_elapsed_ms(redis_t0),
            source=source,
            status=(payload or {}).get("status"),
            channel=channel,
            timeout_sec=_sse_redis_publish_timeout_seconds(),
            task_count=len(_redis_publish_tasks),
        )
    except Exception as exc:
        crawl_trace(
            logger,
            phase="redis",
            action="queued_publish",
            state="error",
            job_id=job_id,
            level=logging.DEBUG,
            elapsed_ms=trace_elapsed_ms(redis_t0),
            source=source,
            status=(payload or {}).get("status"),
            channel=channel,
            error=str(exc),
        )
    finally:
        elapsed_ms = (time.perf_counter() - redis_t0) * 1000.0
        slow_ms = _sse_redis_publish_slow_ms()
        if elapsed_ms >= max(0.0, slow_ms):
            crawl_trace(
                logger,
                phase="redis",
                action="queued_publish",
                state="slow",
                job_id=job_id,
                level=logging.DEBUG,
                elapsed_ms=elapsed_ms,
                source=source,
                status=(payload or {}).get("status"),
                channel=channel,
                published=redis_published,
                slow_ms=slow_ms,
            )
        else:
            crawl_trace(
                logger,
                phase="redis",
                action="queued_publish",
                state="end",
                job_id=job_id,
                elapsed_ms=elapsed_ms,
                source=source,
                status=(payload or {}).get("status"),
                channel=channel,
                published=redis_published,
            )
        task = _redis_publish_tasks_by_job.get(job_id)
        if task is asyncio.current_task():
            _redis_publish_tasks_by_job.pop(job_id, None)
        latest = _redis_publish_latest_by_job.pop(job_id, None)
        if latest:
            latest_payload, latest_db_name, latest_source = latest
            _schedule_redis_publish(
                job_id=job_id,
                payload=latest_payload,
                db_name=latest_db_name,
                source=latest_source,
            )


def _schedule_redis_publish(
    *,
    job_id: str,
    payload: Dict[str, Any],
    db_name: str,
    source: str,
) -> bool:
    active = {task for task in _redis_publish_tasks if task and not task.done()}
    _redis_publish_tasks.clear()
    _redis_publish_tasks.update(active)
    active_by_job = {
        active_job_id: task
        for active_job_id, task in _redis_publish_tasks_by_job.items()
        if task and not task.done()
    }
    _redis_publish_tasks_by_job.clear()
    _redis_publish_tasks_by_job.update(active_by_job)
    existing_job_task = _redis_publish_tasks_by_job.get(job_id)
    if existing_job_task and not existing_job_task.done():
        _redis_publish_latest_by_job[job_id] = (dict(payload or {}), db_name, source)
        crawl_trace(
            logger,
            phase="redis",
            action="schedule_publish",
            state="coalesce",
            job_id=job_id,
            level=logging.DEBUG,
            counts={"active_tasks": len(_redis_publish_tasks), "pending_jobs": len(_redis_publish_latest_by_job)},
            status=(payload or {}).get("status"),
        )
        return True
    limit = _sse_redis_publish_task_limit()
    if len(_redis_publish_tasks) >= limit:
        crawl_trace(
            logger,
            phase="redis",
            action="schedule_publish",
            state="drop",
            job_id=job_id,
            level=logging.WARNING,
            counts={"active_tasks": len(_redis_publish_tasks), "limit": limit},
            status=(payload or {}).get("status"),
        )
        return False
    task = asyncio.create_task(
        _run_redis_publish_task(
            job_id=job_id,
            payload=dict(payload or {}),
            db_name=db_name,
            source=source,
        ),
        name=f"sse-redis-publish:{job_id}",
    )
    _redis_publish_tasks.add(task)
    _redis_publish_tasks_by_job[job_id] = task

    def _cleanup(done: asyncio.Task) -> None:
        _redis_publish_tasks.discard(done)
        if _redis_publish_tasks_by_job.get(job_id) is done:
            _redis_publish_tasks_by_job.pop(job_id, None)
        try:
            if not done.cancelled():
                done.exception()
        except Exception:
            pass

    task.add_done_callback(_cleanup)
    return True


async def _apply_crawling_log_update(
    job_id: str,
    payload: Dict[str, Any],
    db_name: str,
    status_val: Any,
) -> None:
    from db.crawl_db_manager import update_crawling_log_counters

    db_t0 = time.perf_counter()
    crawl_trace(
        logger,
        phase="db",
        action="queued_crawling_log_update",
        state="start",
        job_id=job_id,
        status=status_val,
        counts={
            "scan": payload.get("scan_count"),
            "collection": payload.get("collection_count"),
            "save": payload.get("save_count"),
            "study": payload.get("study_count"),
        },
    )
    await update_crawling_log_counters(
        job_id,
        scan=payload.get("scan_count"),
        collection=payload.get("collection_count"),
        saved=payload.get("save_count"),
        study=payload.get("study_count"),
        pages=payload.get("pages"),
        status=status_val,
        colle=payload.get("colle"),
        dbname=db_name,
        force=True,
    )
    _sse_last_db_update_ts_by_job[job_id] = time.time()
    crawl_trace(
        logger,
        phase="db",
        action="queued_crawling_log_update",
        state="end",
        job_id=job_id,
        elapsed_ms=trace_elapsed_ms(db_t0),
        status=status_val,
    )


async def _run_crawling_log_update_worker(job_id: str) -> None:
    try:
        while True:
            item = _crawling_log_latest_update_by_job.pop(job_id, None)
            if not item:
                return
            payload, db_name, status_val = item
            try:
                await _apply_crawling_log_update(job_id, payload, db_name, status_val)
            except Exception as exc:
                crawl_trace(
                    logger,
                    phase="db",
                    action="queued_crawling_log_update",
                    state="error",
                    job_id=job_id,
                    level=logging.DEBUG,
                    status=status_val,
                    error=str(exc),
                )
    finally:
        task = _crawling_log_update_tasks.get(job_id)
        if task is asyncio.current_task():
            _crawling_log_update_tasks.pop(job_id, None)
        if job_id in _crawling_log_latest_update_by_job:
            try:
                task = asyncio.create_task(
                    _run_crawling_log_update_worker(job_id),
                    name=f"sse-crawling-log-update:{job_id}",
                )
                _crawling_log_update_tasks[job_id] = task
                task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
            except Exception:
                pass


def _schedule_crawling_log_update(job_id: str, payload: Dict[str, Any], db_name: str, status_val: Any) -> None:
    _crawling_log_latest_update_by_job[job_id] = (dict(payload or {}), db_name, status_val)
    task = _crawling_log_update_tasks.get(job_id)
    if task and not task.done():
        return
    task = asyncio.create_task(
        _run_crawling_log_update_worker(job_id),
        name=f"sse-crawling-log-update:{job_id}",
    )
    _crawling_log_update_tasks[job_id] = task
    task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)


async def _await_crawling_log_update_idle(job_id: str) -> None:
    task = _crawling_log_update_tasks.get(job_id)
    if task and not task.done():
        await asyncio.shield(task)


async def await_sse_publish_idle(job_id: str, *, timeout_sec: float = 5.0) -> bool:
    """Wait briefly until this job's queued SSE payload and Redis publish task are flushed."""
    if not job_id:
        return True
    try:
        timeout = max(0.1, min(float(timeout_sec or 5.0), 30.0))
    except Exception:
        timeout = 5.0
    deadline = time.monotonic() + timeout
    while True:
        pending_queue = job_id in _sse_job_enqueued or job_id in _sse_latest_by_job
        redis_task = _redis_publish_tasks_by_job.get(job_id)
        pending_redis = bool(redis_task and not redis_task.done()) or job_id in _redis_publish_latest_by_job
        db_task = _crawling_log_update_tasks.get(job_id)
        pending_db = bool(db_task and not db_task.done()) or job_id in _crawling_log_latest_update_by_job
        if not pending_queue and not pending_redis and not pending_db:
            return True
        if time.monotonic() >= deadline:
            crawl_trace(
                logger,
                phase="sse",
                action="await_publish_idle",
                state="timeout",
                job_id=job_id,
                level=logging.WARNING,
                timeout_sec=timeout,
                counts={
                    "pending_queue": int(bool(pending_queue)),
                    "pending_redis": int(bool(pending_redis)),
                    "pending_db": int(bool(pending_db)),
                },
            )
            return False
        await asyncio.sleep(0.05)

async def ensure_worker_started() -> None:
    """Start the SSE publish worker during startup."""
    global _sse_worker_task
    async with _sse_worker_lock:
        if _sse_worker_task and not _sse_worker_task.done():
            return
        _sse_worker_task = asyncio.create_task(_sse_publish_worker(), name="sse-publish-worker")


def enqueue_sse_message(job_id: str, payload: Dict[str, Any], db_name: str, source: str, priority: int = 0) -> None:
    """Queue an SSE publish request and coalesce pending payloads by job_id.

    Lower priority values are published first. Stop/terminal events should use a
    negative priority so they can bypass ordinary progress updates.
    """
    if not job_id:
        return
    enqueue_t0 = time.perf_counter()
    try:
        if not (_sse_worker_task and not _sse_worker_task.done()):
            asyncio.get_running_loop().create_task(ensure_worker_started())
    except Exception:
        pass
    merge_t0 = time.perf_counter()
    existing = _sse_latest_by_job.get(job_id) or {}
    merged = _merge_counter_snapshot(existing, payload or {})
    merge_ms = (time.perf_counter() - merge_t0) * 1000.0
    existing_priority = int(_sse_latest_priority_by_job.get(job_id, 0))
    next_priority = min(int(priority), existing_priority) if existing else int(priority)

    existing_status = str(existing.get("status") or "").strip().lower()
    new_status = str(merged.get("status") or "").strip().lower()
    existing_stop_requested = bool(existing.get("stop_requested"))
    new_stop_requested = bool(merged.get("stop_requested"))
    existing_is_terminalish = existing_status in {"cancelled", "error", "completed"} or existing_stop_requested
    new_is_terminalish = new_status in {"cancelled", "error", "completed"} or new_stop_requested

    if existing_is_terminalish and not new_is_terminalish:
        preserved = dict(existing)
        preserved.update(merged)
        preserved["status"] = existing.get("status", merged.get("status"))
        preserved["stop_requested"] = existing.get("stop_requested", merged.get("stop_requested"))
        if existing.get("status_hint"):
            preserved["status_hint"] = existing.get("status_hint")
        if existing.get("event"):
            preserved["event"] = existing.get("event")
        if existing.get("message"):
            preserved["message"] = existing.get("message")
        merged = preserved

    _sse_latest_by_job[job_id] = merged
    _sse_latest_db_by_job[job_id] = db_name
    _sse_latest_source_by_job[job_id] = source
    _sse_latest_priority_by_job[job_id] = next_priority
    if job_id in _sse_job_enqueued:
        elapsed = (time.perf_counter() - enqueue_t0) * 1000.0
        _trace_enqueue_result(
            state="coalesce",
            job_id=job_id,
            elapsed_ms=elapsed,
            payload=merged,
            source=source,
            priority=priority,
            queue_priority=next_priority,
            merge_ms=merge_ms,
        )
        if int(priority) < existing_priority:
            _sse_publish_queue.put_nowait((int(priority), time.time(), job_id))
        return
    _sse_job_enqueued.add(job_id)
    _sse_publish_queue.put_nowait((next_priority, time.time(), job_id))
    elapsed = (time.perf_counter() - enqueue_t0) * 1000.0
    _trace_enqueue_result(
        state="queued",
        job_id=job_id,
        elapsed_ms=elapsed,
        payload=merged,
        source=source,
        priority=next_priority,
        queue_priority=next_priority,
        merge_ms=merge_ms,
    )


async def mark_db_update_start(job_id: str) -> None:
    """Mark that a crawling_log DB counter update is in progress for a job."""
    if not job_id:
        return
    async with _db_update_lock:
        _db_update_in_progress.add(job_id)


async def mark_db_update_end(job_id: str) -> None:
    """Mark that the crawling_log DB counter update has finished for a job."""
    if not job_id:
        return
    async with _db_update_lock:
        try:
            _db_update_in_progress.discard(job_id)
        except Exception:
            pass


async def _sse_publish_worker() -> None:
    """Dedicated SSE publish worker that prioritizes terminal and urgent events."""
    while True:
        try:
            global _sse_worker_heartbeat_ts
            _sse_worker_heartbeat_ts = time.time()
            _priority, _ts, job_id = await _sse_publish_queue.get()
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.01)
            continue

        try:
            payload = _sse_latest_by_job.pop(job_id, None)
            db_name = _sse_latest_db_by_job.pop(job_id, None)
            source = _sse_latest_source_by_job.pop(job_id, None) or "queued"
            _sse_latest_priority_by_job.pop(job_id, None)
            _sse_job_enqueued.discard(job_id)
            if payload and db_name:
                try:
                    _payload_status = (payload or {}).get("status")
                    _terminal = _is_terminal_status(_payload_status)
                    status_val = _payload_status if _terminal else "start"
                    now_ts = time.time()
                    last_db_ts = _sse_last_db_update_ts_by_job.get(job_id, 0.0)
                    should_update_db = bool(_terminal) or ((now_ts - last_db_ts) >= _sse_db_update_interval_seconds())
                    if should_update_db:
                        _schedule_crawling_log_update(job_id, payload, db_name, status_val)
                        timeout_sec = _sse_db_update_before_publish_timeout_seconds()
                        if timeout_sec > 0:
                            try:
                                await asyncio.wait_for(
                                    asyncio.shield(_await_crawling_log_update_idle(job_id)),
                                    timeout=timeout_sec,
                                )
                            except asyncio.TimeoutError:
                                crawl_trace(
                                    logger,
                                    phase="db",
                                    action="queued_crawling_log_update",
                                    state="timeout_before_publish",
                                    job_id=job_id,
                                    level=logging.WARNING,
                                    status=status_val,
                                    timeout_sec=timeout_sec,
                                )
                except Exception as e:
                    crawl_trace(
                        logger,
                        phase="db",
                        action="schedule_crawling_log_update",
                        state="error",
                        job_id=job_id,
                        level=logging.DEBUG,
                        status=(payload or {}).get("status"),
                        error=str(e),
                    )

                try:
                    _schedule_redis_publish(
                        job_id=job_id,
                        payload=payload,
                        db_name=db_name,
                        source=source,
                    )
                except Exception as exc:
                    crawl_trace(
                        logger,
                        phase="redis",
                        action="schedule_publish",
                        state="error",
                        job_id=job_id,
                        level=logging.DEBUG,
                        status=(payload or {}).get("status"),
                        error=str(exc),
                    )
                continue
        except Exception as exc:
            crawl_trace(
                logger,
                phase="sse",
                action="publish_worker",
                state="error",
                job_id=job_id,
                level=logging.DEBUG,
                error=str(exc),
            )
        finally:
            try:
                _sse_publish_queue.task_done()
            except Exception:
                pass


async def debug_sse_publish_queue():
    """Return diagnostic state for the SSE publish queue.

    Includes publish backlog, worker heartbeat, pending DB updates, and recent
    per-job publish timestamps.
    """
    try:
        worker_alive = bool(_sse_worker_task and not _sse_worker_task.done())
    except Exception:
        worker_alive = False
    try:
        qsize = int(_sse_publish_queue.qsize())
    except Exception:
        qsize = -1
    return {
        "worker_alive": worker_alive,
        "worker_task": str(_sse_worker_task),
        "worker_heartbeat_ts": _sse_worker_heartbeat_ts,
        "queue_size": qsize,
        "enqueued_jobs": len(_sse_job_enqueued),
        "latest_payload_jobs": len(_sse_latest_by_job),
        "crawling_log_update_tasks": len([t for t in _crawling_log_update_tasks.values() if t and not t.done()]),
        "crawling_log_pending_updates": len(_crawling_log_latest_update_by_job),
        "redis_publish_tasks": len([t for t in _redis_publish_tasks if t and not t.done()]),
        "last_published_ts_by_job": _sse_last_published_ts_by_job,
    }







