"""
Global (job_id-aware) worker pool with fair-share scheduling.

Design goals:
- Multiple job_id runs should share a single set of scan/collection/download workers.
- job_id isolation must be preserved (no cross-job dedup/visited bleed).
- Existing worker functions are reused by providing multiplexed queue wrappers.
- 브라우저 재사용: 단일 Playwright 브라우저 인스턴스를 공유하여 리소스 절약.
  - get_browser() / ensure_started() 후 _browser 사용 권장.

NOTE:
- This module intentionally keeps the implementation small and conservative.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import os
import signal
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional
import json
import time

from playwright.async_api import async_playwright, Browser

from config.settings import settings
from core.crawler.queues import JobQueues
from core.crawler.workers.scan import scan_worker
from core.crawler.workers.collection import collection_worker
from core.crawler.workers.download import (
    cancel_download_worker_activity,
    download_worker,
    get_download_worker_activity_snapshot,
)
from utils.runtime_flags import is_no_limits_mode
from core.crawler.workers.study import study_worker
from core.crawler.dedup import CollectionDeduplicator
from core.crawler.browser_launch import (
    BROWSER_LAUNCH_SEMAPHORE,
    get_default_launch_args,
    filter_launch_args,
    get_default_navigation_timeout_ms,
    get_default_timeout_ms,
)
from db.repository import DBRepository

logger = logging.getLogger(__name__)

_current_job_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("current_job_id", default=None)


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)) or default)
    except Exception:
        return float(default)


def _queue_has_pending(q: Any) -> bool:
    """asyncio.Queue / BatchQueue / PriorityScanQueue 등에 대기 항목이 있는지 best-effort."""
    if q is None:
        return False
    try:
        if not q.empty():
            return True
    except Exception:
        pass
    try:
        if int(q.qsize()) > 0:
            return True
    except Exception:
        pass
    try:
        buf = getattr(q, "buffer", None)
        if buf is not None and len(buf) > 0:
            return True
    except Exception:
        pass
    try:
        inner = getattr(q, "queue", None)
        if inner is not None and not inner.empty():
            return True
    except Exception:
        pass
    return False


async def _terminate_browser_process_os_best_effort(browser: Any) -> None:
    """close() 이후에도 남는 Chromium 자식 프로세스가 있으면 OS 레벨에서 정리."""
    if browser is None:
        return
    proc = getattr(browser, "process", None)
    pid = getattr(proc, "pid", None) if proc is not None else None
    if not pid:
        return
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                timeout=15,
                check=False,
            )
        else:
            try:
                os.kill(int(pid), signal.SIGTERM)
            except Exception:
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except Exception:
                    pass
    except Exception as exc:
        logger.debug("[GlobalPool] OS-level browser process cleanup skipped | pid=%s err=%s", pid, exc)


@dataclass
class JobContext:
    job_id: str
    queues: JobQueues
    # job_id별 deduplicators (다른 job_id와 혼선 방지)
    file_dedup: CollectionDeduplicator
    collection_dedup: CollectionDeduplicator
    # job_id별 stage gating (stop semantics)
    scan_enabled: bool = True
    # job_id별 전용 BrowserContext (해당 job_id 해제 시만 close)
    browser_context: Optional[Any] = None


class _RoundRobin:
    def __init__(self) -> None:
        self._job_ids: list[str] = []
        self._idx: int = 0

    def set_jobs(self, ids: list[str]) -> None:
        self._job_ids = list(ids)
        if self._idx >= len(self._job_ids):
            self._idx = 0

    def next_ids(self) -> list[str]:
        if not self._job_ids:
            return []
        n = len(self._job_ids)
        start = self._idx % n
        ordered = self._job_ids[start:] + self._job_ids[:start]
        self._idx = (start + 1) % n
        return ordered

    def note_consumed(self, jid: str) -> None:
        """성공적으로 jid에서 한 건 소비한 뒤 공정성을 위해 다음 라운드 시작점을 맞춘다."""
        try:
            i = self._job_ids.index(str(jid))
            self._idx = (i + 1) % max(len(self._job_ids), 1)
        except ValueError:
            pass

    def rotation_order(self) -> list[str]:
        """next_ids()와 달리 _idx를 진행하지 않고, 현재 인덱스 기준 순서만 반환."""
        if not self._job_ids:
            return []
        n = len(self._job_ids)
        start = self._idx % n
        return self._job_ids[start:] + self._job_ids[:start]


def _nonempty_job_ids_for_pool(
    pool: "GlobalWorkerPool",
    selector: Callable[[JobQueues], Any],
    stage: str,
) -> list[str]:
    """데이터가 있는 job 큐만 골라 스케줄링 오버헤드를 줄인다."""
    out: list[str] = []
    for jid in pool._rr.rotation_order():
        ctx = pool._jobs.get(jid)
        if not ctx:
            continue
        if stage == "scan" and not ctx.scan_enabled:
            continue
        q = selector(ctx.queues)
        if _queue_has_pending(q):
            out.append(jid)
    return out


class MultiplexQueue:
    """
    Multiplex multiple per-job queues into a single asyncio.Queue-like interface.
    Workers call get()/task_done() without knowing job_id; we track job_id via ContextVar.
    """

    def __init__(self, pool: "GlobalWorkerPool", selector: Callable[[JobQueues], Any], *, stage: str):
        self._pool = pool
        self._selector = selector
        self._stage = stage

    def qsize(self) -> int:
        total = 0
        for ctx in self._pool._jobs.values():
            q = self._selector(ctx.queues)
            # asyncio.Queue has qsize(); BatchQueue does not (has .queue/.buffer)
            try:
                total += int(q.qsize())
                continue
            except Exception:
                pass
            try:
                total += int(getattr(q, "queue").qsize())
            except Exception:
                pass
            try:
                total += int(len(getattr(q, "buffer")))
            except Exception:
                pass
        return total

    def empty(self) -> bool:
        for ctx in self._pool._jobs.values():
            q = self._selector(ctx.queues)
            try:
                if not q.empty():
                    return False
            except Exception:
                pass
        return True

    async def put(self, item: Any) -> None:
        job_id = (item or {}).get("job_id") if isinstance(item, dict) else None
        if not job_id:
            raise ValueError("MultiplexQueue.put requires item['job_id']")
        ctx = self._pool._jobs.get(str(job_id))
        if not ctx:
            # 운영 안정성:
            # - job 컨텍스트가 unregister 된 직후에도(완료/취소 등) 워커 결과가 늦게 도착할 수 있다.
            # - 이 경우 예외를 던지면 해당 워커(stage)가 에러 로그를 남기고 흐름이 흔들릴 수 있으므로,
            #   best-effort로 조용히 드롭한다.
            logger.debug("[GlobalPool] drop put (job not registered) | stage=%s job_id=%s", self._stage, job_id)
            return
        if self._stage == "scan" and not ctx.scan_enabled:
            # stopping semantics: drop new scan items for this job
            return
        q = self._selector(ctx.queues)
        await q.put(item)

    def put_nowait(self, item: Any) -> None:
        job_id = (item or {}).get("job_id") if isinstance(item, dict) else None
        if not job_id:
            raise ValueError("MultiplexQueue.put_nowait requires item['job_id']")
        ctx = self._pool._jobs.get(str(job_id))
        if not ctx:
            logger.debug("[GlobalPool] drop put_nowait (job not registered) | stage=%s job_id=%s", self._stage, job_id)
            return
        if self._stage == "scan" and not ctx.scan_enabled:
            return
        q = self._selector(ctx.queues)
        q.put_nowait(item)

    async def get(self) -> Any:
        while True:
            jids = _nonempty_job_ids_for_pool(self._pool, self._selector, self._stage)
            if not jids:
                await asyncio.sleep(0.01)
                continue
            for jid in jids:
                ctx = self._pool._jobs.get(jid)
                if not ctx:
                    continue
                if self._stage == "scan" and not ctx.scan_enabled:
                    continue
                q = self._selector(ctx.queues)
                try:
                    if q.empty():
                        continue
                except Exception:
                    continue
                try:
                    item = await q.get()
                except Exception:
                    continue
                _current_job_id.set(jid)
                self._pool._rr.note_consumed(jid)
                return item
            await asyncio.sleep(0.01)

    def get_nowait(self) -> Any:
        for jid in _nonempty_job_ids_for_pool(self._pool, self._selector, self._stage):
            ctx = self._pool._jobs.get(jid)
            if not ctx:
                continue
            if self._stage == "scan" and not ctx.scan_enabled:
                continue
            q = self._selector(ctx.queues)
            try:
                item = q.get_nowait()
            except Exception:
                continue
            _current_job_id.set(jid)
            self._pool._rr.note_consumed(jid)
            return item
        raise asyncio.QueueEmpty()

    def task_done(self) -> None:
        jid = _current_job_id.get()
        if not jid:
            return
        ctx = self._pool._jobs.get(jid)
        if not ctx:
            return
        q = self._selector(ctx.queues)
        try:
            q.task_done()
        except Exception:
            pass


class MultiplexBatchQueue(MultiplexQueue):
    async def flush(self) -> None:
        # Flush all job queues for this stage (best-effort)
        for ctx in self._pool._jobs.values():
            q = self._selector(ctx.queues)
            try:
                await q.flush()
            except Exception:
                pass


class MultiplexProgressQueue:
    """
    Progress events are emitted without job_id in many places.
    We attach current job_id (ContextVar set by MultiplexQueue.get()) when missing.
    """

    def __init__(self, pool: "GlobalWorkerPool"):
        self._pool = pool

    def _drop_loglevel(self) -> int:
        """작업이 없거나 풀이 내려가는 중이면 DEBUG/INFO, 활성 작업 중 이상 징후는 INFO."""
        try:
            if not getattr(self._pool, "_started", False) or not self._pool._jobs:
                return logging.DEBUG
            return logging.INFO
        except Exception:
            return logging.INFO

    @staticmethod
    def _is_lossy_telemetry(item: Dict[str, Any]) -> bool:
        return str(item.get("type") or "").strip().lower() in {
            "scan",
            "collection",
            "post",
            "in_flight",
        }

    async def put(self, item: Any) -> None:
        if not isinstance(item, dict):
            return
        if "job_id" not in item:
            jid = _current_job_id.get()
            if jid:
                item = dict(item)
                item["job_id"] = jid
        jid = item.get("job_id")
        if not jid:
            nested = item.get("file_info") if isinstance(item, dict) else None
            nested_jid = (nested or {}).get("job_id") if isinstance(nested, dict) else None
            _nj = str(nested_jid).strip() if nested_jid is not None else ""
            if _nj and _nj in self._pool._jobs:
                item = dict(item)
                item["job_id"] = _nj
                jid = _nj
        if not jid:
            ctx_hint = _current_job_id.get()
            nested = item.get("file_info") if isinstance(item, dict) else None
            nested_jid = (nested or {}).get("job_id") if isinstance(nested, dict) else None
            logger.log(
                self._drop_loglevel(),
                "[GlobalPool] progress_queue.put dropped: no top-level job_id | "
                "type=%s ctxvar_job_id=%s nested_file_info.job_id=%s registered=%s",
                item.get("type"),
                ctx_hint,
                nested_jid,
                list(self._pool._jobs.keys()),
            )
            return
        ctx = self._pool._jobs.get(str(jid))
        if not ctx:
            nested = item.get("file_info") if isinstance(item, dict) else None
            nested_jid = (nested or {}).get("job_id") if isinstance(nested, dict) else None
            logger.log(
                self._drop_loglevel(),
                "[GlobalPool] progress_queue.put dropped: job_id not registered | "
                "job_id=%s type=%s nested_file_info.job_id=%s registered=%s",
                jid,
                item.get("type"),
                nested_jid,
                list(self._pool._jobs.keys()),
            )
            return
        progress_queue = ctx.queues.progress_queue
        maxsize = int(getattr(progress_queue, "maxsize", 0) or 0)
        queue_size = progress_queue.qsize()
        reserved_terminal_slots = max(10, maxsize // 10) if maxsize > 0 else 0
        if (
            maxsize > 0
            and self._is_lossy_telemetry(item)
            and queue_size >= max(1, maxsize - reserved_terminal_slots)
        ):
            logger.debug(
                "[GlobalPool][progress_backpressure] telemetry dropped | job_id=%s type=%s queue=%s/%s",
                jid,
                item.get("type"),
                queue_size,
                maxsize,
            )
            return
        wait_started = time.monotonic()
        await progress_queue.put(item)
        wait_sec = time.monotonic() - wait_started
        if wait_sec >= 1.0:
            logger.warning(
                "[GlobalPool][progress_backpressure] progress enqueue delayed | job_id=%s type=%s wait_sec=%.1f queue=%s/%s",
                jid,
                item.get("type"),
                wait_sec,
                queue_size,
                maxsize,
            )

    def put_nowait(self, item: Any) -> None:
        if not isinstance(item, dict):
            return
        if "job_id" not in item:
            jid = _current_job_id.get()
            if jid:
                item = dict(item)
                item["job_id"] = jid
        jid = item.get("job_id")
        if not jid:
            nested = item.get("file_info") if isinstance(item, dict) else None
            nested_jid = (nested or {}).get("job_id") if isinstance(nested, dict) else None
            _nj = str(nested_jid).strip() if nested_jid is not None else ""
            if _nj and _nj in self._pool._jobs:
                item = dict(item)
                item["job_id"] = _nj
                jid = _nj
        if not jid:
            ctx_hint = _current_job_id.get()
            nested = item.get("file_info") if isinstance(item, dict) else None
            nested_jid = (nested or {}).get("job_id") if isinstance(nested, dict) else None
            logger.log(
                self._drop_loglevel(),
                "[GlobalPool] progress_queue.put_nowait dropped: no top-level job_id | "
                "type=%s ctxvar_job_id=%s nested_file_info.job_id=%s registered=%s",
                item.get("type"),
                ctx_hint,
                nested_jid,
                list(self._pool._jobs.keys()),
            )
            return
        ctx = self._pool._jobs.get(str(jid))
        if not ctx:
            nested = item.get("file_info") if isinstance(item, dict) else None
            nested_jid = (nested or {}).get("job_id") if isinstance(nested, dict) else None
            logger.log(
                self._drop_loglevel(),
                "[GlobalPool] progress_queue.put_nowait dropped: job_id not registered | "
                "job_id=%s type=%s nested_file_info.job_id=%s registered=%s",
                jid,
                item.get("type"),
                nested_jid,
                list(self._pool._jobs.keys()),
            )
            return
        try:
            ctx.queues.progress_queue.put_nowait(item)
        except Exception:
            pass


class GlobalWorkerPool:
    def __init__(self) -> None:
        self._jobs: Dict[str, JobContext] = {}
        self._rr = _RoundRobin()
        self._started = False
        self._start_lock = asyncio.Lock()
        self._ready_event = asyncio.Event()
        self._idle_grace_task: Optional[asyncio.Task] = None
        self._idle_grace_lock = asyncio.Lock()

        self._playwright = None
        self._browser: Optional[Browser] = None
        self._relaunch_lock = asyncio.Lock()
        self._semaphore_acquired = False  # BROWSER_LAUNCH_SEMAPHORE acquire 시 True, close 시 release
        self._browser_use_count: Dict[int, int] = {}
        self._retired_browsers: Dict[int, Browser] = {}
        self._retired_browser_cleanup_tasks: set[asyncio.Task] = set()

        self._tasks: list[asyncio.Task] = []
        self._flush_task: Optional[asyncio.Task] = None
        self._db_repo = DBRepository()

        # Multiplexed queues (constructed after started)
        self.scan_queue = MultiplexQueue(self, lambda q: q.scan_queue, stage="scan")
        self.scan_batch_queue = MultiplexBatchQueue(self, lambda q: q.scan_batch_queue, stage="scan_batch")
        self.collection_batch_queue = MultiplexBatchQueue(self, lambda q: q.collection_batch_queue, stage="collection_batch")
        self.large_collection_batch_queue = MultiplexBatchQueue(self, lambda q: q.large_collection_batch_queue, stage="large_collection_batch")
        self.save_batch_queue = MultiplexBatchQueue(self, lambda q: q.save_batch_queue, stage="save_batch")
        self.progress_queue = MultiplexProgressQueue(self)

    @property
    def registered_jobs(self) -> list[str]:
        """등록된 crawl job_id 목록(디버깅·이중 확인용)."""
        return list(self._jobs.keys())

    def worker_health_snapshot(self, *, job_id: Optional[str] = None) -> Dict[str, Any]:
        """Expose download worker liveness and active item context for queue diagnosis."""
        download_tasks = [t for t in self._tasks if t.get_name().startswith("global-download-")]
        return {
            "download_total": len(download_tasks),
            "download_alive": sum(1 for t in download_tasks if not t.done()),
            "download_done": sum(1 for t in download_tasks if t.done()),
            "download_active": get_download_worker_activity_snapshot(job_id=job_id),
        }
    def _track_worker_task(self, task: asyncio.Task) -> None:
        self._tasks.append(task)

        def _report_done(done_task: asyncio.Task) -> None:
            if done_task.cancelled():
                return
            try:
                exc = done_task.exception()
            except Exception as callback_exc:
                logger.error("[GlobalPool] worker state check failed | task=%s err=%r", done_task.get_name(), callback_exc)
                return
            if exc is not None:
                logger.error(
                    "[GlobalPool] worker stopped with error | task=%s err=%r",
                    done_task.get_name(),
                    exc,
                    exc_info=(type(exc), exc, getattr(exc, "__traceback__", None)),
                )
            else:
                logger.warning("[GlobalPool] worker stopped unexpectedly | task=%s", done_task.get_name())

        task.add_done_callback(_report_done)

    def register_job(self, job_id: str, queues: JobQueues) -> None:
        jid = str(job_id).strip() or "unknown"
        if jid in self._jobs:
            return
        self._cancel_idle_grace_sync()
        self._jobs[jid] = JobContext(
            job_id=jid,
            queues=queues,
            file_dedup=CollectionDeduplicator(),
            collection_dedup=CollectionDeduplicator(),
        )
        self._rr.set_jobs(list(self._jobs.keys()))
        if jid not in self._jobs:
            logger.error(
                "[GlobalPool] register_job invariant failed | job_id=%s registered=%s",
                jid,
                self.registered_jobs,
            )

    def _cancel_idle_grace_sync(self) -> None:
        t = self._idle_grace_task
        if t is not None and not t.done():
            try:
                t.cancel()
            except Exception:
                pass

    async def register_job_and_ensure_started(self, job_id: str, queues: JobQueues) -> None:
        """
        워커 기동 직전에 job_id가 풀에 등록됐는지 확인한 뒤 ensure_started 한다.
        (진행률 이벤트가 MultiplexProgressQueue에서 드롭되지 않도록)
        """
        self.register_job(job_id, queues)
        jid = str(job_id).strip() or "unknown"
        if jid not in self._jobs:
            raise RuntimeError(
                f"[GlobalPool] register_job failed: job_id={jid!r} registered={self.registered_jobs!r}"
            )
        await self.ensure_started()
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=120.0)
        except asyncio.TimeoutError:
            logger.warning(
                "[GlobalPool] pool readiness wait timeout | job_id=%s started=%s browser=%s",
                jid,
                self._started,
                self._browser is not None,
            )
        if jid not in self._jobs:
            logger.error(
                "[GlobalPool] job_id missing after ensure_started | job_id=%s registered=%s",
                jid,
                self.registered_jobs,
            )

    async def unregister_job(self, job_id: str) -> None:
        """
        job_id 해제 시 browser_context를 먼저 닫고 등록을 제거한다.
        (외부에서 close_context_for_job_id를 반복 호출할 필요 없음)
        """
        jid = str(job_id).strip() or "unknown"
        cancelled_downloads = await cancel_download_worker_activity(jid)
        await self.close_context_for_job_id(jid)
        self._jobs.pop(jid, None)
        self._rr.set_jobs(list(self._jobs.keys()))
        logger.info(
            "[FileCrawlTrace][stop_cancel_downloads] job_id=%s active_cancelled=%s remaining_jobs=%s",
            jid,
            cancelled_downloads,
            len(self._jobs),
        )
        await self._maybe_schedule_idle_shutdown()

    async def close_context_for_job_id(self, job_id: str) -> None:
        """
        해당 job_id에 묶인 context만 종료. (job_id별 정리, 다른 job_id에는 영향 없음)
        일반적으로는 unregister_job(job_id)가 내부에서 호출하므로 직접 부를 필요는 없다.
        """
        jid = str(job_id).strip() or "unknown"
        ctx = self._jobs.get(jid)
        if not ctx:
            return
        bc = getattr(ctx, "browser_context", None)
        if not bc:
            return
        try:
            await bc.close()
        except Exception:
            pass
        try:
            ctx.browser_context = None
        except Exception:
            pass
        logger.debug("[GlobalPool] job_id별 context 종료 | job_id=%s", jid)

    async def _maybe_schedule_idle_shutdown(self) -> None:
        """
        등록 job이 0건이 되었을 때 즉시 stop하지 않고 유예 시간 후 stop(워커 재기동 비용 절감).
        환경변수 GLOBAL_POOL_IDLE_GRACE_SECONDS (기본 120, 권장 60~180).
        """
        async with self._idle_grace_lock:
            if self._jobs:
                return
            if not self._started:
                return
            if self._idle_grace_task and not self._idle_grace_task.done():
                return
            delay = max(60.0, min(_env_float("GLOBAL_POOL_IDLE_GRACE_SECONDS", 120.0), 180.0))

            async def _grace() -> None:
                try:
                    await asyncio.sleep(delay)
                    async with self._start_lock:
                        if self._jobs:
                            return
                        if not self._started:
                            return
                        logger.info(
                            "[GlobalPool] idle grace %.0fs elapsed — stopping pool (no registered jobs)",
                            delay,
                        )
                        await self.stop()
                except asyncio.CancelledError:
                    pass
                except Exception as exc:
                    logger.debug("[GlobalPool] idle grace worker: %s", exc)

            self._idle_grace_task = asyncio.create_task(_grace(), name="global-pool-idle-grace")

    async def close_resources_if_no_jobs(self) -> None:
        """
        등록 job이 0건일 때: 유예 시간 후 글로벌 풀 stop (즉시 stop 아님).
        """
        if len(self._jobs) > 0:
            return
        try:
            await self._maybe_schedule_idle_shutdown()
        except Exception as e:
            logger.debug("[GlobalPool] close_resources_if_no_jobs: %s", e)

    def disable_scan(self, job_id: str) -> None:
        """Stop semantics: 해당 job_id의 scan 단계만 중단."""
        ctx = self._jobs.get(str(job_id).strip() or "unknown")
        if not ctx:
            return
        ctx.scan_enabled = False

    def enable_scan(self, job_id: str) -> None:
        ctx = self._jobs.get(str(job_id).strip() or "unknown")
        if not ctx:
            return
        ctx.scan_enabled = True

    def has_enabled_scan_job(self) -> bool:
        """Return True if at least one registered job still allows scan work."""
        for ctx in self._jobs.values():
            if getattr(ctx, "scan_enabled", True):
                return True
        return False

    def get_job_context(self, job_id: str) -> Optional[JobContext]:
        """job_id별 JobContext 반환 (queues, dedup, browser_context 등)."""
        return self._jobs.get(str(job_id).strip() or "unknown")

    async def ensure_started(self) -> None:
        async with self._start_lock:
            if self._started:
                return
            if not self._jobs:
                logger.error(
                    "[GlobalPool] ensure_started aborted: no register_job yet | registered_jobs=[]"
                )
                return
            self._ready_event.clear()
            self._started = True

            # 동시 브라우저 수 제한: 세마포어 획득 후 launch (꽉 차면 대기)
            await BROWSER_LAUNCH_SEMAPHORE.acquire()
            self._semaphore_acquired = True
            try:
                # Start Playwright once
                self._playwright = await async_playwright().start()
                self._browser = await self._launch_browser()
            except Exception:
                if self._semaphore_acquired:
                    BROWSER_LAUNCH_SEMAPHORE.release()
                    self._semaphore_acquired = False
                raise
            finally:
                # 예외 시 반드시 롤백: 브라우저 미생성이면 세마포어 release + started 해제
                if not getattr(self, "_browser", None):
                    try:
                        if getattr(self, "_semaphore_acquired", False):
                            BROWSER_LAUNCH_SEMAPHORE.release()
                            self._semaphore_acquired = False
                    except Exception:
                        pass
                    self._started = False

            # Global worker counts (tunable by existing settings/env)
            cfg = settings.worker_config
            scan_workers = int(cfg.get("scan_workers") or 1)
            collection_workers = int(cfg.get("collection_workers") or cfg.get("scan_workers") or 1)
            download_workers = int(cfg.get("download_workers") or 1)
            study_workers = int(cfg.get("study_workers") or 1)

            # max_depth mirrors WorkerManager behavior
            # - 기본 모드: cap=2 (단, 0은 그대로)
            # - 무제한 모드: cap 제거 (0은 그대로)
            effective_max_depth = settings.crawl_settings.get("max_depth", 1)
            try:
                eff = int(effective_max_depth)
                if eff == 0:
                    effective_max_depth = 0
                else:
                    effective_max_depth = max(eff, 0) if is_no_limits_mode() else min(max(eff, 0), 2)
            except Exception:
                effective_max_depth = 2

            # shared browser context options (same as WorkerManager) + 타임아웃 강제
            context_options = dict(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                default_navigation_timeout=get_default_navigation_timeout_ms(),
                default_timeout=get_default_timeout_ms(),
            )

            # Start scan workers
            for _ in range(max(1, scan_workers)):
                t = asyncio.create_task(
                    scan_worker(
                        in_queue=self.scan_queue,
                        scan_batch_queue=self.scan_batch_queue,
                        collection_batch_queue=self.collection_batch_queue,
                        progress_queue=self.progress_queue,  # multiplex routes to per-job
                        browser=self._browser,
                        max_depth=effective_max_depth,
                        context_options=context_options.copy(),
                        browser_relauncher=self._relaunch_browser,
                        max_concurrent_pages=2,
                        heartbeat_guard=self.has_enabled_scan_job,
                        # These will be overridden per-item in patched workers
                        start_date=None,
                        end_date=None,
                        chat_bot_id=None,
                        db_name=None,
                        # Per-job dedup is handled inside patched workers
                        file_deduplicator=None,
                    ),
                    name="global-scan-worker",
                )
                self._track_worker_task(t)

            # Start collection workers
            for _ in range(max(1, collection_workers)):
                t = asyncio.create_task(
                    collection_worker(
                        self.scan_batch_queue,
                        self.collection_batch_queue,
                        progress_queue=self.progress_queue,
                        db_repo=self._db_repo,
                        deduplicator=None,
                        on_valid_batch=None,
                        forward_to_queue=True,
                        chat_bot_id=None,
                        db_name=None,
                        start_date=None,
                        end_date=None,
                    ),
                    name="global-collection-worker",
                )
                self._track_worker_task(t)

            # Start download workers
            try:
                max_concurrent = int(
                    os.getenv(
                        "DOWNLOAD_MAX_CONCURRENT",
                        str(getattr(settings, "DOWNLOAD_MAX_CONCURRENT", 4)),
                    )
                )
            except Exception:
                max_concurrent = int(getattr(settings, "DOWNLOAD_MAX_CONCURRENT", 4) or 4)
            max_concurrent = max(1, max_concurrent)

            normal_download_workers = int(getattr(settings, "FILE_CRAWL_NORMAL_DOWNLOAD_WORKERS", 4) or 4)
            normal_download_workers = max(1, normal_download_workers)
            for i in range(normal_download_workers):
                worker_lane = "normal"
                worker_queue = self.collection_batch_queue
                t = asyncio.create_task(
                    download_worker(
                        worker_queue,
                        self.save_batch_queue,
                        progress_queue=self.progress_queue,
                        max_concurrent=max_concurrent,
                        browser=self._browser,
                        browser_getter=self.acquire_browser,
                        browser_releaser=self.release_browser,
                        worker_id=i + 1,
                        worker_lane=worker_lane,
                        large_download_queue=None,
                        browser_relauncher=self._relaunch_browser,
                    ),
                    name=f"global-download-{worker_lane}-worker-{i+1}",
                )
                self._track_worker_task(t)

            # Start study workers (consume save_batch_queue)
            for i in range(max(1, study_workers)):
                t = asyncio.create_task(
                    study_worker(self.save_batch_queue),
                    name=f"global-study-worker-{i+1}",
                )
                self._track_worker_task(t)

            # Global batch flush loop (mirrors WorkerManager._periodic_flush but across all jobs)
            self._flush_task = asyncio.create_task(self._periodic_flush(), name="global-batch-flush")

            logger.info(
                "[GlobalPool] started | scan=%s collection=%s download=%s study=%s",
                scan_workers,
                collection_workers,
                download_workers,
                study_workers,
            )
            self._ready_event.set()

    async def _launch_browser(self) -> Browser:
        # [근본 해결] Playwright 객체가 소멸되었을 경우 재초기화 로직 추가
        if self._playwright is None:
            logger.info("[GlobalPool] Playwright object is None. Re-initializing...")
            self._playwright = await async_playwright().start()

        # 실행 인자 제한: 화이트리스트만 사용 (browser_launch 모듈)
        launch_args = filter_launch_args(get_default_launch_args())
        return await self._playwright.chromium.launch(
            headless=settings.HEADLESS, 
            args=launch_args
        ) # type: ignore[attr-defined]

    def acquire_browser(self) -> Optional[Browser]:
        browser = self._browser
        if browser is None:
            return None
        key = id(browser)
        self._browser_use_count[key] = self._browser_use_count.get(key, 0) + 1
        return browser

    def release_browser(self, browser: Optional[Browser]) -> None:
        if browser is None:
            return
        key = id(browser)
        current = self._browser_use_count.get(key, 0)
        if current <= 1:
            self._browser_use_count.pop(key, None)
        else:
            self._browser_use_count[key] = current - 1
            return
        if browser is not self._browser and key in self._retired_browsers:
            self._schedule_retired_browser_cleanup(browser)

    def _schedule_retired_browser_cleanup(self, browser: Optional[Browser]) -> None:
        if browser is None:
            return
        key = id(browser)
        if key not in self._retired_browsers:
            return
        if self._browser_use_count.get(key, 0) > 0:
            return
        retired = self._retired_browsers.pop(key, None)
        if retired is None:
            return
        task = asyncio.create_task(self._close_browser_instance(retired))
        self._retired_browser_cleanup_tasks.add(task)
        task.add_done_callback(self._retired_browser_cleanup_tasks.discard)

    async def _close_browser_instance(self, browser: Optional[Browser]) -> None:
        if browser is None:
            return
        try:
            for ctx in getattr(browser, "contexts", []) or []:
                try:
                    await ctx.close()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await browser.close()
        except Exception:
            pass
        await _terminate_browser_process_os_best_effort(browser)

    async def _relaunch_browser(self) -> Browser:
        async with self._relaunch_lock:
            old_browser = self._browser
            try:
                new_browser = await self._launch_browser()
            except Exception:
                raise
            self._browser = new_browser
            if old_browser and old_browser is not new_browser:
                self._retired_browsers[id(old_browser)] = old_browser
                self._schedule_retired_browser_cleanup(old_browser)
            return new_browser
                    
    async def _periodic_flush(self) -> None:
        try:
            flush_interval = float(os.getenv("BATCH_FLUSH_INTERVAL_SECONDS", "0.15") or "0.15")
        except Exception:
            flush_interval = 0.15
        flush_interval = max(0.05, min(flush_interval, 2.0))

        while True:
            try:
                await asyncio.sleep(flush_interval)
                # best-effort flush across all registered jobs
                for ctx in list(self._jobs.values()):
                    try:
                        await ctx.queues.scan_batch_queue.flush()
                        await ctx.queues.collection_batch_queue.flush()
                        await ctx.queues.large_collection_batch_queue.flush()
                        await ctx.queues.save_batch_queue.flush()
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.5)

    def get_browser(self) -> Optional[Browser]:
        """브라우저 재사용: 풀에 이미 시작된 브라우저가 있으면 반환. 없으면 None."""
        return getattr(self, "_browser", None)

    async def stop(self) -> None:
        """
        Gracefully stop the GlobalWorkerPool:
        - cancel worker tasks and periodic flush task
        - close Playwright browser and stop Playwright (finally에서 반드시 close 호출)
        - mark pool as not started
        """
        started_at = time.monotonic()
        logger.info(
            "[GlobalPool] stop_start | started=%s jobs=%s tasks=%s browser=%s playwright=%s",
            self._started,
            self.registered_jobs,
            len(self._tasks),
            bool(self._browser),
            bool(self._playwright),
        )
        self._cancel_idle_grace_sync()
        try:
            self._ready_event.clear()
        except Exception:
            pass
        # 1) Cancel worker tasks
        try:
            for t in list(self._tasks):
                try:
                    if not t.done():
                        t.cancel()
                except Exception:
                    pass
        except Exception:
            pass

        # 2) Cancel flush task
        try:
            if self._flush_task and not self._flush_task.done():
                try:
                    self._flush_task.cancel()
                    await self._flush_task
                except Exception:
                    pass
        except Exception:
            pass

        # 3) & 4) finally 블록에서 반드시 close() 호출 (열려 있는 context 먼저 종료 후 브라우저 종료)
        try:
            pass
        finally:
            try:
                cleanup_tasks = list(self._retired_browser_cleanup_tasks)
                if cleanup_tasks:
                    await asyncio.gather(*cleanup_tasks, return_exceptions=True)
                retired = list(self._retired_browsers.values())
                self._retired_browsers.clear()
                self._browser_use_count.clear()
                for retired_browser in retired:
                    await self._close_browser_instance(retired_browser)
                if self._browser:
                    br = self._browser
                    await self._close_browser_instance(br)
                    self._browser = None
                    if getattr(self, "_semaphore_acquired", False):
                        BROWSER_LAUNCH_SEMAPHORE.release()
                        self._semaphore_acquired = False
            except Exception:
                pass
            try:
                if self._playwright:
                    try:
                        await self._playwright.stop()
                    except Exception:
                        pass
                    self._playwright = None
            except Exception:
                pass
            # 5) finally: 반드시 stopped 표시
            try:
                self._started = False
            except Exception:
                pass
        logger.info(
            "[GlobalPool] stop_done | elapsed_ms=%s jobs=%s tasks=%s browser=%s playwright=%s",
            int((time.monotonic() - started_at) * 1000),
            self.registered_jobs,
            len(self._tasks),
            bool(self._browser),
            bool(self._playwright),
        )


_global_pool: Optional[GlobalWorkerPool] = None


def get_global_worker_pool() -> GlobalWorkerPool:
    global _global_pool
    if _global_pool is None:
        _global_pool = GlobalWorkerPool()
    return _global_pool
