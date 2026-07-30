"""POST /summarize_keywords with proxy-safe requests and bounded retries."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
import logging
import os
import re
import threading
import time
from typing import Any, Dict, Optional, Tuple

import requests

from utils.http_client import get_requests_session

logger = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "https://dev.chatbaram.com:9001/summarize_keywords"
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
_COOLDOWN_TRIGGER_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
COOLDOWN_ACTIVE_STATUS = -2
_summarize_keywords_gate: Optional[asyncio.Semaphore] = None
_summarize_keywords_gate_size: Optional[int] = None
_summarize_keywords_cooldown_until = 0.0
_summarize_keywords_cooldown_lock = threading.Lock()
_summarize_keywords_queue: Optional[asyncio.Queue] = None
_summarize_keywords_queue_loop: Optional[asyncio.AbstractEventLoop] = None
_summarize_keywords_queue_workers: list[asyncio.Task] = []
_summarize_keywords_queue_active = 0
_summarize_keywords_queue_delayed = 0
_summarize_keywords_queue_completed = 0
_summarize_keywords_queue_dropped = 0
_SUMMARIZE_DEBUG_LOG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "logs",
    "summarize_keywords.log",
)


def _write_summarize_debug_log(message: str) -> None:
    """요약 큐/전송 흐름을 별도 파일에 남겨 성공 여부를 추적한다."""
    try:
        os.makedirs(os.path.dirname(_SUMMARIZE_DEBUG_LOG_PATH), exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(_SUMMARIZE_DEBUG_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(f"[{timestamp}] {message}\n")
    except Exception:
        pass


def summarize_keywords_endpoint() -> str:
    return (os.getenv("SUMMARIZE_KEYWORDS_URL") or "").strip() or DEFAULT_ENDPOINT


def summarize_keywords_timeout_sec() -> float:
    try:
        return float((os.getenv("SUMMARIZE_KEYWORDS_TIMEOUT_SEC") or "120").strip() or "120")
    except ValueError:
        return 120.0


def summarize_keywords_retry_count() -> int:
    try:
        value = int((os.getenv("SUMMARIZE_KEYWORDS_RETRY_COUNT") or "2").strip() or "2")
    except ValueError:
        value = 2
    return max(0, min(value, 5))


def summarize_keywords_retry_backoff_sec() -> float:
    try:
        value = float((os.getenv("SUMMARIZE_KEYWORDS_RETRY_BACKOFF_SEC") or "1").strip() or "1")
    except ValueError:
        value = 1.0
    return max(0.1, min(value, 10.0))


def summarize_keywords_payload_concurrency() -> int:
    try:
        value = int((os.getenv("SUMMARIZE_KEYWORDS_PAYLOAD_CONCURRENCY") or "3").strip() or "3")
    except ValueError:
        value = 3
    return max(1, min(value, 10))


def summarize_keywords_max_inflight() -> int:
    try:
        value = int((os.getenv("SUMMARIZE_KEYWORDS_MAX_INFLIGHT") or "4").strip() or "4")
    except ValueError:
        value = 4
    return max(1, min(value, 32))


def summarize_keywords_cooldown_sec() -> float:
    try:
        value = float((os.getenv("SUMMARIZE_KEYWORDS_COOLDOWN_SEC") or "90").strip() or "90")
    except ValueError:
        value = 90.0
    return max(5.0, min(value, 1800.0))


def summarize_keywords_use_queue() -> bool:
    raw = str(os.getenv("SUMMARIZE_KEYWORDS_USE_QUEUE", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def summarize_keywords_queue_workers() -> int:
    try:
        default_workers = max(2, summarize_keywords_max_inflight())
        value = int((os.getenv("SUMMARIZE_KEYWORDS_QUEUE_WORKERS") or str(default_workers)).strip() or str(default_workers))
    except ValueError:
        value = max(2, summarize_keywords_max_inflight())
    return max(1, min(value, 16))


def summarize_keywords_queue_maxsize() -> int:
    try:
        value = int((os.getenv("SUMMARIZE_KEYWORDS_QUEUE_MAXSIZE") or "1000").strip() or "1000")
    except ValueError:
        value = 1000
    return max(10, min(value, 100000))


def summarize_keywords_queue_retry_count() -> int:
    try:
        value = int((os.getenv("SUMMARIZE_KEYWORDS_QUEUE_RETRY_COUNT") or "2").strip() or "2")
    except ValueError:
        value = 2
    return max(0, min(value, 20))


def _compact_debug_text(value: Any, *, limit: int = 180) -> str:
    if value is None:
        return ""
    try:
        compact = " ".join(str(value).split())
    except Exception:
        compact = str(value)
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "..."


def summarize_keywords_body_preview_limit() -> int:
    try:
        value = int((os.getenv("SUMMARIZE_KEYWORDS_BODY_PREVIEW_LIMIT") or "1000").strip() or "1000")
    except ValueError:
        value = 1000
    return max(80, min(value, 10000))


def _payload_debug_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    contents = payload.get("contents")
    if not isinstance(contents, list):
        contents = []
    first_content = ""
    if contents:
        try:
            first_content = _compact_debug_text(contents[0], limit=180)
        except Exception:
            first_content = ""
    return {
        "chat_bot_id": _compact_debug_text(payload.get("chat_bot_id"), limit=80),
        "db_name": _compact_debug_text(payload.get("db_name"), limit=80),
        "learn_list_id": _compact_debug_text(payload.get("learn_list_id"), limit=40),
        "target": _compact_debug_text(payload.get("target"), limit=80),
        "target_db": _compact_debug_text(payload.get("target_db"), limit=80),
        "learn_table": _compact_debug_text(payload.get("learn_table"), limit=120),
        "target_table": _compact_debug_text(payload.get("target_table"), limit=120),
        "source": _compact_debug_text(payload.get("source"), limit=80),
        "summary_only": _compact_debug_text(payload.get("summary_only"), limit=20),
        "file_crawl": _compact_debug_text(payload.get("file_crawl"), limit=20),
        "concurrency": _compact_debug_text(payload.get("concurrency"), limit=20),
        "content_type": _compact_debug_text(payload.get("content_type"), limit=40),
        "contents_count": len(contents),
        "first_content": first_content or "-",
    }


def _payload_debug_preview(payload: Dict[str, Any]) -> str:
    ctx = _payload_debug_context(payload)
    fields = [
        f"chat_bot_id={ctx['chat_bot_id']}",
        f"db_name={ctx['db_name']}",
        f"learn_list_id={ctx['learn_list_id'] or '-'}",
        f"content_type={ctx['content_type']}",
        f"contents_count={ctx['contents_count']}",
        f"first_content={ctx['first_content']}",
    ]
    for key in ("target", "target_db", "learn_table", "target_table", "source", "summary_only", "file_crawl", "concurrency"):
        if ctx.get(key):
            fields.append(f"{key}={ctx[key]}")
    for key in ("source_url", "subject"):
        if payload.get(key):
            fields.append(f"{key}={_compact_debug_text(payload.get(key), limit=180)}")
    normalized_text = str(payload.get("normalized_text") or "").strip()
    normalized_contents = payload.get("normalized_contents")
    normalized_count = len(normalized_contents) if isinstance(normalized_contents, list) else 0
    if normalized_text or normalized_count:
        fields.append(f"normalized_text_len={len(normalized_text)}")
        fields.append(f"normalized_contents_count={normalized_count}")
    return " ".join(fields)


def _extract_wait_seconds(text: str, key: str) -> Optional[int]:
    try:
        match = re.search(rf"{re.escape(key)}=(\d+)", text or "")
    except Exception:
        return None
    if not match:
        return None
    try:
        return max(0, int(match.group(1)))
    except Exception:
        return None



def _derive_requeue_delay_sec(status: int, body: str) -> float:
    remaining = _extract_wait_seconds(body or "", "remaining_sec")
    if remaining is not None:
        return max(1.0, float(remaining))
    cooldown = _extract_wait_seconds(body or "", "cooldown_sec")
    if cooldown is not None:
        return max(1.0, float(cooldown))
    if status == COOLDOWN_ACTIVE_STATUS:
        return max(1.0, summarize_keywords_cooldown_sec())
    return max(1.0, summarize_keywords_retry_backoff_sec())


@dataclass
class _QueuedSummarizeJob:
    endpoint: str
    payload: Dict[str, Any]
    timeout_sec: float
    queue_attempt: int = 0
    enqueued_at: float = 0.0


def _get_summarize_keywords_gate() -> asyncio.Semaphore:
    global _summarize_keywords_gate, _summarize_keywords_gate_size
    size = summarize_keywords_max_inflight()
    if _summarize_keywords_gate is None or _summarize_keywords_gate_size != size:
        _summarize_keywords_gate = asyncio.Semaphore(size)
        _summarize_keywords_gate_size = size
    return _summarize_keywords_gate


def _remaining_cooldown_sec(now: Optional[float] = None) -> float:
    current = time.monotonic() if now is None else now
    with _summarize_keywords_cooldown_lock:
        remaining = _summarize_keywords_cooldown_until - current
    return max(0.0, remaining)


def _activate_cooldown() -> float:
    until = time.monotonic() + summarize_keywords_cooldown_sec()
    with _summarize_keywords_cooldown_lock:
        global _summarize_keywords_cooldown_until
        if until > _summarize_keywords_cooldown_until:
            _summarize_keywords_cooldown_until = until
        return _summarize_keywords_cooldown_until


def _clear_cooldown() -> None:
    with _summarize_keywords_cooldown_lock:
        global _summarize_keywords_cooldown_until
        _summarize_keywords_cooldown_until = 0.0


def post_summarize_keywords_sync(
    endpoint: str,
    payload: Dict[str, Any],
    *,
    timeout_sec: float,
) -> Tuple[int, str]:
    debug_ctx = _payload_debug_context(payload)
    remaining = _remaining_cooldown_sec()
    if remaining > 0:
        logger.debug(
            "[SummarizeClient] cooldown_active endpoint=%s remaining_sec=%s chat_bot_id=%s db_name=%s content_type=%s contents_count=%s first_content=%s",
            endpoint,
            int(remaining),
            debug_ctx["chat_bot_id"],
            debug_ctx["db_name"],
            debug_ctx["content_type"],
            debug_ctx["contents_count"],
            debug_ctx["first_content"],
        )
        _write_summarize_debug_log(
            "[SummarizeClient] cooldown_active "
            f"endpoint={endpoint} remaining_sec={int(remaining)} "
            f"chat_bot_id={debug_ctx['chat_bot_id']} db_name={debug_ctx['db_name']} "
            f"content_type={debug_ctx['content_type']} contents_count={debug_ctx['contents_count']} "
            f"first_content={debug_ctx['first_content']}"
        )
        return COOLDOWN_ACTIVE_STATUS, f"cooldown_active remaining_sec={int(remaining)}"

    attempts = summarize_keywords_retry_count() + 1
    backoff_sec = summarize_keywords_retry_backoff_sec()
    last_status = 0
    last_body = ""
    dispatch_started = time.monotonic()
    payload_preview = _payload_debug_preview(payload)
    logger.debug(
        "[SummarizeClient] dispatch_start endpoint=%s attempts=%s timeout_sec=%s max_inflight=%s payload=%s",
        endpoint,
        attempts,
        timeout_sec,
        summarize_keywords_max_inflight(),
        payload_preview,
    )
    _write_summarize_debug_log(
        "[SummarizeClient] dispatch_start "
        f"endpoint={endpoint} attempts={attempts} timeout_sec={timeout_sec} "
        f"max_inflight={summarize_keywords_max_inflight()} "
        f"payload={payload_preview}"
    )

    with get_requests_session() as session:
        for attempt in range(1, attempts + 1):
            attempt_started = time.monotonic()
            logger.debug(
                "[SummarizeClient] attempt_start endpoint=%s attempt=%s/%s timeout_sec=%s first_content=%s",
                endpoint,
                attempt,
                attempts,
                timeout_sec,
                debug_ctx["first_content"],
            )
            _write_summarize_debug_log(
                "[SummarizeClient] attempt_start "
                f"endpoint={endpoint} attempt={attempt}/{attempts} timeout_sec={timeout_sec} "
                f"first_content={debug_ctx['first_content']}"
            )
            try:
                resp = session.post(
                    endpoint,
                    json=payload,
                    timeout=timeout_sec,
                    headers={"Content-Type": "application/json; charset=utf-8"},
                )
                last_status = int(resp.status_code or 0)
                last_body = resp.text or ""
                elapsed_ms = int((time.monotonic() - attempt_started) * 1000)
                content_type = resp.headers.get("content-type", "")
                body_preview = _compact_debug_text(last_body, limit=summarize_keywords_body_preview_limit())
                logger.debug(
                    "[SummarizeClient] attempt_response endpoint=%s attempt=%s/%s status=%s elapsed_ms=%s content_type=%s body_len=%s body_preview=%s first_content=%s",
                    endpoint,
                    attempt,
                    attempts,
                    last_status,
                    elapsed_ms,
                    _compact_debug_text(content_type, limit=80),
                    len(last_body),
                    body_preview,
                    debug_ctx["first_content"],
                )
                _write_summarize_debug_log(
                    "[SummarizeClient] attempt_response "
                    f"endpoint={endpoint} attempt={attempt}/{attempts} status={last_status} "
                    f"elapsed_ms={elapsed_ms} content_type={_compact_debug_text(content_type, limit=80)} "
                    f"body_len={len(last_body)} "
                    f"body_preview={body_preview} "
                    f"first_content={debug_ctx['first_content']}"
                )
                if last_status == 200:
                    _clear_cooldown()
                    total_elapsed_ms = int((time.monotonic() - dispatch_started) * 1000)
                    logger.debug(
                        "[SummarizeClient] dispatch_success endpoint=%s attempt=%s/%s status=%s elapsed_ms=%s contents_count=%s learn_list_id=%s first_content=%s",
                        endpoint,
                        attempt,
                        attempts,
                        last_status,
                        total_elapsed_ms,
                        debug_ctx["contents_count"],
                        debug_ctx["learn_list_id"] or "-",
                        debug_ctx["first_content"],
                    )
                    _write_summarize_debug_log(
                        "[SummarizeClient] dispatch_success "
                        f"endpoint={endpoint} attempt={attempt}/{attempts} status={last_status} "
                        f"elapsed_ms={total_elapsed_ms} contents_count={debug_ctx['contents_count']} "
                        f"learn_list_id={debug_ctx['learn_list_id'] or '-'} "
                        f"first_content={debug_ctx['first_content']}"
                    )
                if last_status not in _RETRYABLE_STATUS_CODES or attempt >= attempts:
                    if last_status in _COOLDOWN_TRIGGER_STATUS_CODES:
                        until = _activate_cooldown()
                        remaining = max(0, int(until - time.monotonic()))
                        last_body = (
                            f"{last_body} | cooldown_sec={remaining}"
                            if last_body
                            else f"cooldown_sec={remaining}"
                        )
                        logger.warning(
                            "[SummarizeClient] cooldown_set endpoint=%s trigger_status=%s attempt=%s/%s cooldown_sec=%s detail=%s first_content=%s",
                            endpoint,
                            last_status,
                            attempt,
                            attempts,
                            remaining,
                            _compact_debug_text(last_body, limit=240),
                            debug_ctx["first_content"],
                        )
                        _write_summarize_debug_log(
                            "[SummarizeClient] cooldown_set "
                            f"endpoint={endpoint} trigger_status={last_status} attempt={attempt}/{attempts} "
                            f"cooldown_sec={remaining} detail={_compact_debug_text(last_body, limit=240)} "
                            f"first_content={debug_ctx['first_content']}"
                        )
                    elif last_status != 200:
                        logger.warning(
                            "[SummarizeClient] dispatch_non200 endpoint=%s status=%s attempt=%s/%s detail=%s first_content=%s",
                            endpoint,
                            last_status,
                            attempt,
                            attempts,
                            _compact_debug_text(last_body, limit=240),
                            debug_ctx["first_content"],
                        )
                        _write_summarize_debug_log(
                            "[SummarizeClient] dispatch_non200 "
                            f"endpoint={endpoint} status={last_status} attempt={attempt}/{attempts} "
                            f"detail={_compact_debug_text(last_body, limit=240)} "
                            f"first_content={debug_ctx['first_content']}"
                        )
                    return last_status, last_body
                logger.warning(
                    "[SummarizeClient] retryable_status endpoint=%s status=%s attempt=%s/%s backoff_sec=%s detail=%s first_content=%s",
                    endpoint,
                    last_status,
                    attempt,
                    attempts,
                    backoff_sec * attempt,
                    _compact_debug_text(last_body, limit=240),
                    debug_ctx["first_content"],
                )
                _write_summarize_debug_log(
                    "[SummarizeClient] retryable_status "
                    f"endpoint={endpoint} status={last_status} attempt={attempt}/{attempts} "
                    f"backoff_sec={backoff_sec * attempt} detail={_compact_debug_text(last_body, limit=240)} "
                    f"first_content={debug_ctx['first_content']}"
                )
            except requests.RequestException as exc:
                elapsed_ms = int((time.monotonic() - attempt_started) * 1000)
                last_status = 0
                last_body = str(exc)
                is_final_attempt = attempt >= attempts
                log_fn = logger.warning if is_final_attempt else logger.debug
                log_fn(
                    "[SummarizeClient] request_exception endpoint=%s attempt=%s/%s elapsed_ms=%s err_type=%s err=%s first_content=%s",
                    endpoint,
                    attempt,
                    attempts,
                    elapsed_ms,
                    type(exc).__name__,
                    _compact_debug_text(last_body, limit=240),
                    debug_ctx["first_content"],
                )
                _write_summarize_debug_log(
                    "[SummarizeClient] request_exception "
                    f"endpoint={endpoint} attempt={attempt}/{attempts} elapsed_ms={elapsed_ms} "
                    f"err_type={type(exc).__name__} "
                    f"err={_compact_debug_text(last_body, limit=240)} "
                    f"first_content={debug_ctx['first_content']}"
                )
                if is_final_attempt:
                    until = _activate_cooldown()
                    remaining = max(0, int(until - time.monotonic()))
                    last_body = (
                        f"{last_body} | cooldown_sec={remaining}"
                        if last_body
                        else f"cooldown_sec={remaining}"
                    )
                    logger.warning(
                        "[SummarizeClient] cooldown_set endpoint=%s trigger=request_exception attempt=%s/%s cooldown_sec=%s err=%s first_content=%s",
                        endpoint,
                        attempt,
                        attempts,
                        remaining,
                        _compact_debug_text(last_body, limit=240),
                        debug_ctx["first_content"],
                    )
                    _write_summarize_debug_log(
                        "[SummarizeClient] cooldown_set "
                        f"endpoint={endpoint} trigger=request_exception attempt={attempt}/{attempts} "
                        f"cooldown_sec={remaining} err={_compact_debug_text(last_body, limit=240)} "
                        f"first_content={debug_ctx['first_content']}"
                    )
                    return last_status, last_body

            time.sleep(backoff_sec * attempt)

    return last_status, last_body


async def post_summarize_keywords(
    endpoint: str,
    payload: Dict[str, Any],
    *,
    timeout_sec: float,
) -> Tuple[int, str]:
    async with _get_summarize_keywords_gate():
        return await asyncio.to_thread(
            post_summarize_keywords_sync,
            endpoint,
            payload,
            timeout_sec=timeout_sec,
        )


def _reset_summarize_queue_state(*, loop: asyncio.AbstractEventLoop) -> asyncio.Queue:
    global _summarize_keywords_queue, _summarize_keywords_queue_loop, _summarize_keywords_queue_workers
    if _summarize_keywords_queue_loop is not loop:
        _summarize_keywords_queue = asyncio.Queue(maxsize=summarize_keywords_queue_maxsize())
        _summarize_keywords_queue_loop = loop
        _summarize_keywords_queue_workers = []
    if _summarize_keywords_queue is None:
        _summarize_keywords_queue = asyncio.Queue(maxsize=summarize_keywords_queue_maxsize())
    return _summarize_keywords_queue


def _ensure_summarize_queue_workers(*, loop: asyncio.AbstractEventLoop) -> asyncio.Queue:
    queue = _reset_summarize_queue_state(loop=loop)
    alive_workers = [task for task in _summarize_keywords_queue_workers if not task.done()]
    _summarize_keywords_queue_workers[:] = alive_workers
    target_workers = summarize_keywords_queue_workers()
    while len(_summarize_keywords_queue_workers) < target_workers:
        worker_id = len(_summarize_keywords_queue_workers) + 1
        task = loop.create_task(_summarize_keywords_queue_worker(worker_id))
        _summarize_keywords_queue_workers.append(task)
        logger.debug(
            "[SummarizeQueue] worker_started worker=%s/%s loop_id=%s",
            worker_id,
            target_workers,
            id(loop),
        )
        _write_summarize_debug_log(
            f"[SummarizeQueue] worker_started worker={worker_id}/{target_workers} loop_id={id(loop)}"
        )
    return queue


def summarize_keywords_queue_status() -> Dict[str, Any]:
    """Return local in-process summarize queue state for diagnostics."""
    queue = _summarize_keywords_queue
    workers = list(_summarize_keywords_queue_workers)
    pending = queue.qsize() if queue is not None else 0
    alive_workers = [task for task in workers if not task.done()]
    unfinished = pending + _summarize_keywords_queue_active + _summarize_keywords_queue_delayed
    maxsize = summarize_keywords_queue_maxsize()
    return {
        "use_queue": summarize_keywords_use_queue(),
        "idle": unfinished == 0,
        "unfinished": unfinished,
        "pending": pending,
        "active": _summarize_keywords_queue_active,
        "delayed_retry": _summarize_keywords_queue_delayed,
        "completed": _summarize_keywords_queue_completed,
        "dropped": _summarize_keywords_queue_dropped,
        "workers_configured": summarize_keywords_queue_workers(),
        "workers_alive": len(alive_workers),
        "workers_total": len(workers),
        "queue_initialized": queue is not None,
        "queue_maxsize": maxsize,
        "queue_full": bool(queue is not None and pending >= maxsize),
        "cooldown_remaining_sec": int(_remaining_cooldown_sec()),
    }


async def enqueue_summarize_keywords(
    endpoint: str,
    payload: Dict[str, Any],
    *,
    timeout_sec: float,
) -> None:
    loop = asyncio.get_running_loop()
    queue = _ensure_summarize_queue_workers(loop=loop)
    job = _QueuedSummarizeJob(
        endpoint=endpoint,
        payload=dict(payload),
        timeout_sec=timeout_sec,
        queue_attempt=0,
        enqueued_at=time.monotonic(),
    )
    debug_ctx = _payload_debug_context(payload)
    payload_preview = _payload_debug_preview(payload)
    await queue.put(job)
    logger.debug(
        "[SummarizeQueue] enqueued queue_size=%s workers=%s payload=%s",
        queue.qsize(),
        summarize_keywords_queue_workers(),
        payload_preview,
    )
    _write_summarize_debug_log(
        "[SummarizeQueue] enqueued "
        f"queue_size={queue.qsize()} workers={summarize_keywords_queue_workers()} "
        f"payload={payload_preview}"
    )


async def _summarize_keywords_queue_worker(worker_id: int) -> None:
    global _summarize_keywords_queue_active
    global _summarize_keywords_queue_completed
    global _summarize_keywords_queue_dropped
    loop = asyncio.get_running_loop()
    while True:
        queue = _reset_summarize_queue_state(loop=loop)
        job = await queue.get()
        debug_ctx = _payload_debug_context(job.payload)
        _summarize_keywords_queue_active += 1
        try:
            logger.debug(
                "[SummarizeQueue] worker_pick worker=%s queue_attempt=%s queue_size=%s first_content=%s",
                worker_id,
                job.queue_attempt,
                queue.qsize(),
                debug_ctx["first_content"],
            )
            _write_summarize_debug_log(
                "[SummarizeQueue] worker_pick "
                f"worker={worker_id} queue_attempt={job.queue_attempt} "
                f"queue_size={queue.qsize()} first_content={debug_ctx['first_content']}"
            )
            status, body = await post_summarize_keywords(
                job.endpoint,
                job.payload,
                timeout_sec=job.timeout_sec,
            )
            should_requeue = (
                status == COOLDOWN_ACTIVE_STATUS
                or status <= 0
                or status in _COOLDOWN_TRIGGER_STATUS_CODES
            )
            if should_requeue and job.queue_attempt < summarize_keywords_queue_retry_count():
                delay_sec = _derive_requeue_delay_sec(status, body)
                retry_job = _QueuedSummarizeJob(
                    endpoint=job.endpoint,
                    payload=dict(job.payload),
                    timeout_sec=job.timeout_sec,
                    queue_attempt=job.queue_attempt + 1,
                    enqueued_at=time.monotonic(),
                )
                logger.warning(
                    "[SummarizeQueue] requeue worker=%s status=%s next_queue_attempt=%s delay_sec=%s queue_size=%s first_content=%s detail=%s",
                    worker_id,
                    status,
                    retry_job.queue_attempt,
                    int(delay_sec),
                    queue.qsize(),
                    debug_ctx["first_content"],
                    _compact_debug_text(body, limit=240),
                )
                _write_summarize_debug_log(
                    "[SummarizeQueue] requeue "
                    f"worker={worker_id} status={status} next_queue_attempt={retry_job.queue_attempt} "
                    f"delay_sec={int(delay_sec)} queue_size={queue.qsize()} "
                    f"first_content={debug_ctx['first_content']} detail={_compact_debug_text(body, limit=240)}"
                )
                _schedule_requeue_summarize_job(retry_job, delay_sec, loop=loop)
            elif should_requeue:
                _summarize_keywords_queue_dropped += 1
                logger.warning(
                    "[SummarizeQueue] drop_after_retries worker=%s status=%s queue_attempt=%s first_content=%s detail=%s",
                    worker_id,
                    status,
                    job.queue_attempt,
                    debug_ctx["first_content"],
                    _compact_debug_text(body, limit=240),
                )
                _write_summarize_debug_log(
                    "[SummarizeQueue] drop_after_retries "
                    f"worker={worker_id} status={status} queue_attempt={job.queue_attempt} "
                    f"first_content={debug_ctx['first_content']} detail={_compact_debug_text(body, limit=240)}"
                )
            elif status == 200:
                _summarize_keywords_queue_completed += 1
                logger.debug(
                    "[SummarizeQueue] completed worker=%s queue_attempt=%s first_content=%s",
                    worker_id,
                    job.queue_attempt,
                    debug_ctx["first_content"],
                )
                _write_summarize_debug_log(
                    "[SummarizeQueue] completed "
                    f"worker={worker_id} queue_attempt={job.queue_attempt} "
                    f"first_content={debug_ctx['first_content']}"
                )
            else:
                _summarize_keywords_queue_dropped += 1
                logger.warning(
                    "[SummarizeQueue] nonretryable_drop worker=%s status=%s queue_attempt=%s first_content=%s detail=%s",
                    worker_id,
                    status,
                    job.queue_attempt,
                    debug_ctx["first_content"],
                    _compact_debug_text(body, limit=240),
                )
                _write_summarize_debug_log(
                    "[SummarizeQueue] nonretryable_drop "
                    f"worker={worker_id} status={status} queue_attempt={job.queue_attempt} "
                    f"first_content={debug_ctx['first_content']} detail={_compact_debug_text(body, limit=240)}"
                )
        except Exception as exc:
            logger.exception(
                "[SummarizeQueue] worker_error worker=%s queue_attempt=%s first_content=%s err=%s",
                worker_id,
                job.queue_attempt,
                debug_ctx["first_content"],
                exc,
            )
            _write_summarize_debug_log(
                "[SummarizeQueue] worker_error "
                f"worker={worker_id} queue_attempt={job.queue_attempt} "
                f"first_content={debug_ctx['first_content']} err={_compact_debug_text(str(exc), limit=240)}"
            )
        finally:
            _summarize_keywords_queue_active = max(0, _summarize_keywords_queue_active - 1)
            queue.task_done()


def _schedule_requeue_summarize_job(
    job: _QueuedSummarizeJob,
    delay_sec: float,
    *,
    loop: asyncio.AbstractEventLoop,
) -> None:
    global _summarize_keywords_queue_delayed
    _summarize_keywords_queue_delayed += 1
    loop.create_task(_requeue_summarize_job(job, delay_sec))


async def _requeue_summarize_job(job: _QueuedSummarizeJob, delay_sec: float) -> None:
    global _summarize_keywords_queue_delayed
    try:
        await asyncio.sleep(max(0.0, delay_sec))
        loop = asyncio.get_running_loop()
        queue = _ensure_summarize_queue_workers(loop=loop)
        await queue.put(job)
        debug_ctx = _payload_debug_context(job.payload)
        logger.debug(
            "[SummarizeQueue] requeued queue_attempt=%s queue_size=%s first_content=%s",
            job.queue_attempt,
            queue.qsize(),
            debug_ctx["first_content"],
        )
        _write_summarize_debug_log(
            "[SummarizeQueue] requeued "
            f"queue_attempt={job.queue_attempt} queue_size={queue.qsize()} "
            f"first_content={debug_ctx['first_content']}"
        )
    finally:
        _summarize_keywords_queue_delayed = max(0, _summarize_keywords_queue_delayed - 1)
