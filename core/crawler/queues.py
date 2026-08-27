# core/crawler/queues.py
"""
Queue helpers for crawler pipelines.

Historically the project relied on process-global queues, which prevented
concurrent jobs and left residue after stop.  This module now exposes
`JobQueues` so every crawl owns an isolated pipeline, while still providing a
legacy default instance for modules that haven't migrated yet.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import asyncio
import os
import random
import logging
from typing import Any, Dict, Optional

from asyncio import Queue
from core.crawler.batch_queue import BatchQueue
from utils.file import parse_display_file_size_bytes

logger = logging.getLogger(__name__)


def is_large_download_item(item: Any) -> bool:
    """Classify only declared-size attachments; unknown sizes stay in the normal lane."""
    try:
        threshold_mb = float(os.getenv("DOWNLOAD_LARGE_FILE_THRESHOLD_MB", "20") or "20")
    except Exception:
        threshold_mb = 20.0
    threshold_bytes = int(max(1.0, min(threshold_mb, 1024.0)) * 1024 * 1024)
    meta = item if isinstance(item, dict) else {}
    original_meta = meta.get("original_meta") if isinstance(meta.get("original_meta"), dict) else {}
    candidates = (
        meta.get("declared_file_size_bytes"),
        parse_display_file_size_bytes(meta.get("name")),
        parse_display_file_size_bytes(meta.get("subject")),
        parse_display_file_size_bytes(original_meta.get("attachment_name")),
    )
    return max((int(size) for size in candidates if size), default=0) >= threshold_bytes


def _env_queue_maxsize(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)) or str(default))
    except Exception:
        value = default
    return max(1, min(value, 5000))


def _env_batch_size(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)) or str(default))
    except Exception:
        value = default
    return max(1, min(value, 500))


def _download_queue_maxsize() -> int:
    """Keep pending download candidates in memory until a worker consumes them."""
    # asyncio.Queue treats zero as unbounded. Download throughput is governed by
    # workers and host slots; a full ingress queue must not stall attachment
    # extraction or make a crawl appear to stop before candidates are queued.
    return 0


_job_pause_flags: Dict[str, Dict[str, bool]] = {}
_job_pause_events: Dict[str, asyncio.Event] = {}


def get_job_pause_flags(job_id: str) -> Dict[str, bool]:
    try:
        return _job_pause_flags.get(str(job_id), {"scan": False, "collection": False})
    except Exception:
        return {"scan": False, "collection": False}


def get_job_pause_event(job_id: str) -> asyncio.Event:
    """Return an asyncio.Event for the job pause state.

    The event is SET when scanning/collection is allowed, and CLEARED when paused.
    This allows workers to await on the event instead of polling.
    """
    jid = str(job_id)
    ev = _job_pause_events.get(jid)
    if ev is None:
        ev = asyncio.Event()
        # default: not paused => set
        ev.set()
        _job_pause_events[jid] = ev
    return ev


def set_job_pause_flags(job_id: str, *, pause_scan: Optional[bool] = None, pause_collection: Optional[bool] = None) -> Dict[str, bool]:
    jid = str(job_id)
    cur = _job_pause_flags.get(jid, {"scan": False, "collection": False})
    if pause_scan is not None:
        cur["scan"] = bool(pause_scan)
    if pause_collection is not None:
        cur["collection"] = bool(pause_collection)
    _job_pause_flags[jid] = cur
    # ensure pause event exists and update it
    try:
        ev = _job_pause_events.get(jid)
        if ev is None:
            ev = asyncio.Event()
            _job_pause_events[jid] = ev
        # if scan paused -> clear event (workers will block), else set
        if cur.get("scan", False):
            ev.clear()
        else:
            ev.set()
    except Exception:
        # best-effort: non-blocking
        pass
    return dict(cur)


def _get_item_url(item: Any) -> str:
    """Best-effort URL extractor from queue items."""
    try:
        if isinstance(item, dict):
            u = item.get("url")
            return str(u or "")
        return str(item or "")
    except Exception:
        return ""


def _is_high_priority_url(url: str) -> bool:
    """
    Heuristic: detail/view URLs should be processed earlier than list/menu pages.
    This reduces 'discovered late' feeling caused by FIFO scan_queue backlog.
    """
    try:
        lu = (url or "").lower()
    except Exception:
        lu = str(url).lower()
    if not lu:
        return False
    # view/detail hints
    if any(h in lu for h in ("view.do", "detail.do", "read.do", "brdview", "brddetail")):
        return True
    # common board identifiers
    if any(h in lu for h in ("nttid=", "num=")):
        return True
    return False


class PriorityScanQueue:
    """
    Two-level async queue (high/low) with a compatible subset of asyncio.Queue API.

    - put/put_nowait routes by URL heuristics: view/detail => high, others => low
    - get prefers high but enforces basic fairness (after N high, take one low if available)
    - task_done/join emulate asyncio.Queue semantics across both internal queues
    """

    def __init__(self, *, high_ratio: int = 10):
        total_maxsize = _env_queue_maxsize("CRAWLER_SCAN_QUEUE_MAXSIZE", 500)
        lane_maxsize = max(1, (int(total_maxsize) + 1) // 2)
        self._high: Queue = Queue(maxsize=lane_maxsize)
        self._low: Queue = Queue(maxsize=lane_maxsize)
        # 환경변수로 동작 튜닝 가능
        try:
            high_ratio = int(os.getenv("SCAN_QUEUE_HIGH_RATIO", str(high_ratio)))
        except Exception:
            pass
        self._high_ratio = max(1, int(high_ratio))
        try:
            self._randomize = os.getenv("SCAN_QUEUE_RANDOMIZE", "1") == "1"
        except Exception:
            self._randomize = True
        try:
            self._randomize_prob = float(os.getenv("SCAN_QUEUE_RANDOMIZE_PROB", "0.2"))
        except Exception:
            self._randomize_prob = 0.2
        self._randomize_prob = max(0.0, min(1.0, self._randomize_prob))
        self._high_streak = 0
        self._unfinished_tasks = 0
        self._finished = asyncio.Event()
        self._finished.set()

    def _maybe_promote_last_to_front(self, lane: Queue) -> None:
        """
        FIFO 큐의 순서를 완전히 깨지 않으면서, 일정 확률로 '방금 들어온' 아이템을
        앞쪽에 끼워 넣어 탐색의 다양성을 높인다.

        asyncio.Queue의 내부 deque(_queue)를 사용한다(Private API이므로 best-effort).
        """
        if not self._randomize:
            return
        if self._randomize_prob <= 0:
            return
        if random.random() >= self._randomize_prob:
            return
        try:
            dq = getattr(lane, "_queue", None)
            if dq is None:
                return
            # 마지막 요소를 앞으로 이동
            if len(dq) >= 2 and hasattr(dq, "appendleft"):
                dq.appendleft(dq.pop())
        except Exception:
            return

    def _choose_lane(self, item: Any) -> Queue:
        url = _get_item_url(item)
        return self._high if _is_high_priority_url(url) else self._low

    async def put(self, item: Any) -> None:
        # unbounded queue라 put_nowait로도 충분하며, 랜덤화/프로모션을 원자적으로 적용하기 쉽다.
        lane = self._choose_lane(item)
        await lane.put(item)
        self._maybe_promote_last_to_front(lane)
        self._unfinished_tasks += 1
        self._finished.clear()

    def put_nowait(self, item: Any) -> None:
        lane = self._choose_lane(item)
        lane.put_nowait(item)
        self._maybe_promote_last_to_front(lane)
        self._unfinished_tasks += 1
        self._finished.clear()

    async def get(self) -> Any:
        # Prefer high, but keep fairness to avoid starving low.
        while True:
            if (not self._high.empty()) and (self._high_streak < self._high_ratio or self._low.empty()):
                item = await self._high.get()
                self._high_streak += 1
                return item
            if not self._low.empty():
                item = await self._low.get()
                self._high_streak = 0
                return item

            # both empty -> wait for whichever arrives first
            get_high = asyncio.create_task(self._high.get())
            get_low = asyncio.create_task(self._low.get())
            done, pending = await asyncio.wait(
                {get_high, get_low}, return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
            item = next(iter(done)).result()
            if get_high in done:
                self._high_streak += 1
            else:
                self._high_streak = 0
            return item

    def get_nowait(self) -> Any:
        if (not self._high.empty()) and (self._high_streak < self._high_ratio or self._low.empty()):
            self._high_streak += 1
            return self._high.get_nowait()
        if not self._low.empty():
            self._high_streak = 0
            return self._low.get_nowait()
        raise asyncio.QueueEmpty

    def task_done(self) -> None:
        if self._unfinished_tasks <= 0:
            raise ValueError("task_done() called too many times")
        self._unfinished_tasks -= 1
        if self._unfinished_tasks == 0:
            self._finished.set()

    async def join(self) -> None:
        await self._finished.wait()

    def qsize(self) -> int:
        return self._high.qsize() + self._low.qsize()

    def empty(self) -> bool:
        return self._high.empty() and self._low.empty()

    def snapshot(self) -> Dict[str, int]:
        return {"high": self._high.qsize(), "low": self._low.qsize(), "total": self.qsize()}


@dataclass
class JobQueues:
    """Per-job queue container."""

    scan_queue: Any = field(default_factory=PriorityScanQueue)
    scan_batch_queue: BatchQueue = field(default_factory=lambda: BatchQueue(batch_size=_env_batch_size("CRAWLER_SCAN_BATCH_SIZE", 3)))
    collection_batch_queue: BatchQueue = field(
        default_factory=lambda: BatchQueue(
            batch_size=_env_batch_size("CRAWLER_COLLECTION_BATCH_SIZE", 1),
            queue_maxsize=_download_queue_maxsize(),
        )
    )
    large_collection_batch_queue: BatchQueue = field(
        default_factory=lambda: BatchQueue(
            batch_size=_env_batch_size("CRAWLER_COLLECTION_BATCH_SIZE", 1),
            queue_maxsize=_download_queue_maxsize(),
        )
    )
    source_prewarm_batch_queue: BatchQueue = field(
        default_factory=lambda: BatchQueue(
            batch_size=1,
            queue_maxsize=_download_queue_maxsize(),
        )
    )
    save_batch_queue: BatchQueue = field(default_factory=lambda: BatchQueue(batch_size=_env_batch_size("CRAWLER_SAVE_BATCH_SIZE", 3)))
    study_batch_queue: BatchQueue = field(default_factory=lambda: BatchQueue(batch_size=_env_batch_size("CRAWLER_STUDY_BATCH_SIZE", 3)))
    retry_batch_queue: BatchQueue = field(
        default_factory=lambda: BatchQueue(
            batch_size=1,
            queue_maxsize=_env_queue_maxsize("DOWNLOAD_FAILED_RETRY_MAXSIZE", 500),
        )
    )
    progress_queue: Queue = field(default_factory=lambda: Queue(maxsize=_env_queue_maxsize("CRAWLER_PROGRESS_QUEUE_MAXSIZE", 500)))

    async def drain(self) -> Dict[str, int]:
        """Drain all queues, returning drained counts for diagnostics."""
        return {
            "scan_queue": await _drain_asyncio_queue_nowait(self.scan_queue),
            "progress_queue": await _drain_asyncio_queue_nowait(self.progress_queue),
            "scan_batch_queue": await _clear_batch_queue_nowait(self.scan_batch_queue),
            "collection_batch_queue": await _clear_batch_queue_nowait(self.collection_batch_queue),
            "large_collection_batch_queue": await _clear_batch_queue_nowait(self.large_collection_batch_queue),
            "source_prewarm_batch_queue": await _clear_batch_queue_nowait(self.source_prewarm_batch_queue),
            "save_batch_queue": await _clear_batch_queue_nowait(self.save_batch_queue),
            "study_batch_queue": await _clear_batch_queue_nowait(self.study_batch_queue),
            "retry_batch_queue": await _clear_batch_queue_nowait(self.retry_batch_queue),
        }

    def snapshot(self) -> Dict[str, int]:
        """Return current queue depths for debugging."""
        try:
            scan_batch_size = self.scan_batch_queue.queue.qsize()
        except Exception:
            scan_batch_size = -1
        try:
            collection_batch_size = self.collection_batch_queue.queue.qsize()
        except Exception:
            collection_batch_size = -1
        try:
            save_batch_size = self.save_batch_queue.queue.qsize()
        except Exception:
            save_batch_size = -1
        try:
            study_batch_size = self.study_batch_queue.queue.qsize()
        except Exception:
            study_batch_size = -1

        try:
            scan_queue_size = self.scan_queue.qsize()
        except Exception:
            scan_queue_size = -1
        return {
            "scan_queue": scan_queue_size,
            "progress_queue": self.progress_queue.qsize(),
            "scan_batch_queue": scan_batch_size,
            "collection_batch_queue": collection_batch_size,
            "large_collection_batch_queue": self.large_collection_batch_queue.queue.qsize(),
            "source_prewarm_batch_queue": self.source_prewarm_batch_queue.queue.qsize(),
            "save_batch_queue": save_batch_size,
            "study_batch_queue": study_batch_size,
            "retry_batch_queue": self.retry_batch_queue.queue.qsize(),
        }

    def debug_snapshot(self) -> Dict[str, Any]:
        """Include queued, buffered, and in-flight batch counts for stall diagnosis."""
        result: Dict[str, Any] = dict(self.snapshot())
        for name in (
            "scan_batch_queue",
            "collection_batch_queue",
            "large_collection_batch_queue",
            "source_prewarm_batch_queue",
            "save_batch_queue",
            "study_batch_queue",
            "retry_batch_queue",
        ):
            batch_queue = getattr(self, name, None)
            raw_queue = getattr(batch_queue, "queue", None)
            try:
                result[f"{name}_buffer"] = len(getattr(batch_queue, "buffer", []) or [])
            except Exception:
                result[f"{name}_buffer"] = -1
            try:
                result[f"{name}_unfinished"] = int(getattr(raw_queue, "_unfinished_tasks", -1) or 0)
            except Exception:
                result[f"{name}_unfinished"] = -1
        try:
            result["progress_queue_unfinished"] = int(
                getattr(self.progress_queue, "_unfinished_tasks", -1) or 0
            )
        except Exception:
            result["progress_queue_unfinished"] = -1
        return result

_job_queue_registry: Dict[str, JobQueues] = {}


def create_job_queues(
    job_id: str,
    *,
    collection_batch_size: Optional[int] = None,
    collection_queue_maxsize: Optional[int] = None,
) -> JobQueues:
    """Create and register queues for a job.

    collection_batch_size:
        >1 이면 collection→download 구간에서 항목을 소배치로 묶어 전달한다.
        파일 크롤링 등에서 batch_size=1이면 다운로드 워커가 건당 1개씩만 처리해
        전체 병렬도가 download_workers 수에 묶이므로, 기본은 파일 파이프라인에서만 상향한다.
    """
    bs = _env_batch_size("CRAWLER_COLLECTION_BATCH_SIZE", 1)
    if collection_batch_size is not None:
        try:
            bs = int(collection_batch_size)
        except Exception:
            bs = _env_batch_size("CRAWLER_COLLECTION_BATCH_SIZE", 1)
        bs = max(1, min(bs, 500))
    if collection_queue_maxsize is None:
        queue_maxsize = _collection_queue_maxsize()
    else:
        queue_maxsize = max(1, min(int(collection_queue_maxsize), 5000))
    cbq = BatchQueue(
        batch_size=bs,
        queue_maxsize=max(1, queue_maxsize // 2),
    )
    large_cbq = BatchQueue(
        batch_size=bs,
        queue_maxsize=max(1, queue_maxsize // 2),
    )
    queues = JobQueues(
        collection_batch_queue=cbq,
        large_collection_batch_queue=large_cbq,
    )
    _job_queue_registry[job_id] = queues
    logger.debug(
        "[Queues] Created job queues | job_id=%s collection_batch_size=%s",
        job_id,
        getattr(cbq, "batch_size", bs),
    )
    return queues


def get_job_queues(job_id: str) -> Optional[JobQueues]:
    return _job_queue_registry.get(job_id)


async def dispose_job_queues(job_id: str) -> Dict[str, int]:
    """Drain and remove queues for the given job_id."""
    queues = _job_queue_registry.pop(job_id, None)
    if not queues:
        return {}
    drained = await queues.drain()
    logger.debug("[Queues] Disposed job queues | job_id=%s drained=%s", job_id, drained)
    return drained


# --- Legacy compatibility --------------------------------------------------
# Existing modules import global queues directly.  Provide a default instance so
# they keep working while the refactor migrates them to JobQueues.
_legacy_queues = JobQueues()

scan_queue = _legacy_queues.scan_queue
scan_batch_queue = _legacy_queues.scan_batch_queue
collection_batch_queue = _legacy_queues.collection_batch_queue
save_batch_queue = _legacy_queues.save_batch_queue
study_batch_queue = _legacy_queues.study_batch_queue
progress_queue = _legacy_queues.progress_queue


async def reset_global_queues() -> Dict[str, int]:
    """Drain legacy global queues (used by old single-job flow)."""
    return await _legacy_queues.drain()


async def _drain_asyncio_queue_nowait(q) -> int:
    """Drain an asyncio.Queue without awaiting. Returns number of drained items."""
    drained = 0
    while True:
        try:
            q.get_nowait()
        except Exception:
            break
        else:
            drained += 1
            try:
                q.task_done()
            except Exception:
                pass
    return drained

async def _clear_batch_queue_nowait(bq: BatchQueue) -> int:
    """Clear a BatchQueue (buffer + underlying queue). Returns approx drained item count."""
    drained = 0
    # buffer is protected by internal lock
    async with bq._lock:
        drained += len(getattr(bq, 'buffer', []) or [])
        bq.buffer = []
        while True:
            try:
                batch = bq.queue.get_nowait()
            except Exception:
                break
            else:
                try:
                    drained += len(batch) if isinstance(batch, list) else 1
                except Exception:
                    drained += 1
                try:
                    bq.queue.task_done()
                except Exception:
                    pass
    return drained

async def reset_crawler_queues() -> dict:
    """Reset global crawler queues. Use at workflow start/end to avoid leftover work leaking.

    NOTE: This project uses process-global queues, so concurrent multi-job crawling is not supported.
    """
    stats = {}
    stats['scan_queue'] = await _drain_asyncio_queue_nowait(scan_queue)
    stats['progress_queue'] = await _drain_asyncio_queue_nowait(progress_queue)
    stats['scan_batch_queue'] = await _clear_batch_queue_nowait(scan_batch_queue)
    stats['collection_batch_queue'] = await _clear_batch_queue_nowait(collection_batch_queue)
    stats['save_batch_queue'] = await _clear_batch_queue_nowait(save_batch_queue)
    stats['study_batch_queue'] = await _clear_batch_queue_nowait(study_batch_queue)
    return stats
