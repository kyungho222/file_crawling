import asyncio
import os
import shutil
from typing import List, Dict, Any, Callable, Optional, Set
from collections import deque
from datetime import datetime
from urllib.parse import urlparse
import logging
import json
from enum import Enum
import random
import re
import time
import uuid

from core.crawler.queues import JobQueues, create_job_queues, dispose_job_queues
from db.mysql_db_config import mysql_execute_query
from config.settings import settings
from db.mariadb_save_update import insert_into_learn_list
from utils.whoami import get_chat_id_from_db
from utils.runtime_flags import is_no_limits_mode
from utils.url import canonicalize_url_for_dedup, ensure_url_scheme
try:
    from backend.shared.stage_url_report import append_stage_urls  # type: ignore
except Exception:
    def append_stage_urls(*, stage, urls, job_id=None, db_name=None, output_dir=None, extra_meta=None, entry_extra=None):
        return None
from backend.shared.runtime_tab_view import (
    resolve_start_urls_to_list_pages,
    extract_views_from_list_pages,
    resolve_runtime_start_urls,
)
from config.settings import (
    normalize_access_url,
    get_storage_domain_for_db_name,
    get_file_download_path,
    get_uploaded_files_local_dir,
)
from utils.web_sync import sync_file_to_webserver
import utils.web_sync as web_sync

from db.db_job_managers import AsyncJobManager, AsyncJobProgress
from db.db_redis import get_redis

logger = logging.getLogger("backend.file.integrated_workflow")

if os.getenv("CRAWL_DEBUG_FLOW", "0") == "1":
    try:
        logger.setLevel(logging.INFO)
    except Exception:
        pass

try:
    import requests
except Exception:
    requests = None

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None

# ---------------------------------------------------------
# ---------------------------------------------------------
def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert a value to int with a fallback default."""
    try: return int(value) if value is not None else default
    except (ValueError, TypeError, AttributeError): return default

def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert a value to float with a fallback default."""
    try: return float(value) if value is not None else default
    except (ValueError, TypeError, AttributeError): return default

def safe_bool(value: Any, default: bool = False) -> bool:
    """Safely parse common truthy values."""
    if value is None: return default
    if isinstance(value, bool): return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")

def _debug_log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    """Emit structured debug logs when crawl progress debugging is enabled."""
    if safe_bool(os.getenv("CRAWL_PROGRESS_DEBUG", "0")):
        try:
            payload = {"hypothesis_id": hypothesis_id, "location": location, "message": message, "data": data, "ts": int(time.time() * 1000)}
            logger.info("[ProgressDebug] %s", json.dumps(payload, ensure_ascii=False))
        except Exception:
            pass

def extract_cate2_from_web_title(web_title: str) -> str:
    """Extract a clean category label from a web title string."""
    try:
        if not web_title: return ""
        part = str(web_title).split("<")[0].strip()
        return re.sub(r"\s*\([^)]*\)\s*", "", part).strip()
    except Exception:
        return ""

class WorkflowState(Enum):
    INIT = "init"
    RUNNING = "running"
    STOPPING = "stopping"
    COMPLETED = "completed"
    ERROR = "error"
    CANCELLED = "cancelled"


class IntegratedWorkflow:
    """Integrated file crawling workflow for scan, collection, save, and learning stages."""
    
    def __init__(self):
        self.scan_buffer = []
        self.crawler_file_buffer = []
        self.colle_mode = ""
        self.file_mode = False
        self.collection_queue = deque()
        self.save_queue = deque()
        self.study_queue = deque()
        
        self.seen_scan_urls = set()
        self.seen_collection_urls = set()
        self.seen_save_urls = set()
        self.pending_collection_urls = set()
        self.pending_save_urls = set()
        self.saved_urls = set()
        self.saved_db_ids = []
        self._save_claimed_urls: Set[str] = set()
        
        self._post_download_tasks: Set[asyncio.Task] = set()
        self._trigger_tasks: Set[asyncio.Task] = set()

        limit_val = 1000000 if is_no_limits_mode() else safe_int(os.getenv("POST_DOWNLOAD_CONCURRENCY", "10"), 10)
        self._post_download_sem = asyncio.Semaphore(max(1, min(limit_val, 24)))
        
        tlimit_val = 1000000 if is_no_limits_mode() else safe_int(os.getenv("TRIGGER_LEARNING_CONCURRENCY", "5"), 5)
        self._trigger_sem = asyncio.Semaphore(max(1, min(tlimit_val, 12)))

        self._learn_runtime_lock = asyncio.Lock()
        self._learn_redis = None
        self._learn_job_manager = None
        self._learn_job_progress = None

        self._last_crawling_log_update_at: float = 0.0
        self._crawling_log_update_lock = asyncio.Lock()
        self._crawling_log_update_interval = safe_float(os.getenv("CRAWLING_LOG_UPDATE_INTERVAL_SECONDS", "5.0"), 5.0)
        
        self.batch_size = 10
        self.save_interval = 3
        self.study_delay = 3
        
        self.state = WorkflowState.INIT
        self.is_running = False
        self.stop_event = asyncio.Event()
        self.worker_manager = None
        self.final_status: Optional[str] = None
        
        self.job_id: Optional[str] = None
        self.chat_bot_id: Optional[str] = None
        self.db_name: Optional[str] = None
        self.domain: Optional[str] = None
        self.server_domain: Optional[str] = None
        self.unique_id: Optional[str] = None
        self.cate1: Optional[str] = None
        self.cate2: Optional[str] = None
        self.memo: Optional[str] = None

        self._pg_chat_id: Optional[str] = None
        self._pg_table_name: Optional[str] = None
        self.job_queues: Optional[JobQueues] = None
        self._job_queue_key: Optional[str] = None
        
        self.in_flight = {'scan': 0, 'collection': 0, 'download': 0}
        
        self.stats = {
            'scan_count': 0,
            'collection_count': 0,
            'save_count': 0,
            'study_count': 0,
            'study_success_count': 0,
            'study_failed_count': 0,
            'save_reasons': {'new': 0, 'duplicate': 0, 'skipped': 0}
        }
        self._stats_lock = asyncio.Lock()
        self._counted_study_keys: set[str] = set()
        self._pending_study_keys: Set[str] = set()
        self._pending_study_success_keys: Set[str] = set()
        self._processed_study_keys: Set[str] = set()
        
        self.start_date = None
        self.end_date = None
        self._last_in_range_ts: Optional[float] = None
        self._auto_out_of_range_task: Optional[asyncio.Task] = None
        self._observed_reg_date: bool = False
        
        self.progress_callback = None
        self.start_url_profile: Optional[Dict[str, Any]] = None
        self.collection_queue_lock = asyncio.Lock()
        self.save_flush_lock = asyncio.Lock()
        self._worker_manager_start_task: Optional[asyncio.Task] = None
        
        self._stop_requested: bool = False
        self._stop_requested_at: Optional[float] = None
        self._stop_grace_seconds: float = safe_float(os.getenv("STOP_GRACE_SECONDS", "10"), 10.0)
        self._stop_quiet_seconds: float = safe_float(os.getenv("STOP_QUIET_SECONDS", "3"), 3.0)
        self._hard_stop: bool = False
        self._hard_stop_reason: Optional[str] = None
        self._hard_stop_at: Optional[float] = None
        self._stop_grace_enforcer_task: Optional[asyncio.Task] = None
        
        self.learning_service = None
        self.enable_learning = safe_bool(os.getenv("FILE_ENABLE_LEARNING", "1"), True)
        self._cate2_name_cache: Dict[str, Optional[str]] = {}
        self._cate2_name_lock = asyncio.Lock()
        self.enable_cate_filter: bool = safe_bool(os.getenv("FILE_ENABLE_CATE_FILTER", "0"), False)
        
        self._dummy_scan_count = 0
        self._scan_attempt_count: int = 0

    @staticmethod
    def _normalize_count_url(url: Optional[str]) -> Optional[str]:
        """Normalize a URL for count and duplicate tracking."""
        if not url: return None
        raw = str(url).strip()
        if not raw: return None
        try: canon = canonicalize_url_for_dedup(raw) or raw
        except Exception: canon = raw
        try:
            p = urlparse(canon)
            host = (p.netloc or "").removeprefix("www.")
            canon = p._replace(netloc=host, fragment="").geturl()
        except Exception: pass
        return canon

    # ---------------------------------------------------------
    # ---------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        """Return a normalized workflow progress snapshot for UI, SSE, and DB updates."""
        stats = dict(getattr(self, 'stats', {}) or {})
        
        real_count = len(getattr(self, 'seen_scan_urls', set()) or set())
        attempt_count = safe_int(getattr(self, "_scan_attempt_count", 0), 0)
        base_count = max(real_count, attempt_count)

        dummy_max = safe_int(getattr(self, "_dummy_scan_max", 0), 0)
        dummy_floor = safe_int(getattr(self, "_dummy_scan_floor", 0), 0)
        dummy_completed = getattr(self, "_dummy_scan_completed", False)
        dummy_in_progress = getattr(self, "_dummy_scan_in_progress", False)

        if dummy_completed and dummy_max > 0:
            display_scan = dummy_max + base_count
        elif (dummy_in_progress and dummy_floor > 0) or (dummy_floor > 0):
            display_scan = dummy_floor + base_count
        else:
            display_scan = base_count

        stats["real_scan_count"] = base_count
        stats["display_scan_count"] = display_scan
        stats["scan_count"] = base_count
        stats["total_count"] = base_count
        
        current_coll = safe_int(stats.get('collection_count', 0), 0)
        seen_coll = len(getattr(self, 'seen_collection_urls', set()) or set())
        stats['collection_count'] = max(current_coll, seen_coll)
        
        db_saved = len(getattr(self, 'saved_db_ids', []) or [])
        current_save = safe_int(stats.get('save_count', 0), 0)
        stats['save_count'] = db_saved if db_saved > 0 else current_save
        
        pending_keys = getattr(self, '_pending_study_keys', set()) or set()
        processed_keys = getattr(self, '_processed_study_keys', set()) or set()
        unprocessed_count = len(pending_keys - processed_keys)
        stats['study_count'] = safe_int(stats.get('study_count', 0), 0) + unprocessed_count
        
        pending_success_keys = getattr(self, '_pending_study_success_keys', set()) or set()
        unprocessed_success = len(pending_success_keys - processed_keys)
        stats['study_success_count'] = safe_int(stats.get('study_success_count', 0), 0) + unprocessed_success

        stats["collection_count"] = min(stats["collection_count"], stats["scan_count"])
        stats["save_count"] = min(stats["save_count"], stats["collection_count"])
        stats["study_count"] = min(stats["study_count"], stats["save_count"])
        stats["save_success_count"] = min(safe_int(stats.get("save_success_count", 0)), stats["save_count"])
        stats["study_success_count"] = min(stats["study_success_count"], stats["study_count"])

        stats['pending_collection_count'] = len(getattr(self, 'pending_collection_urls', set()) or set())
        stats['pending_save_count'] = len(getattr(self, 'pending_save_urls', set()) or set())
        inflight = getattr(self, "in_flight", {}) or {}
        stats["in_flight"] = {
            "scan": safe_int(inflight.get("scan")),
            "collection": safe_int(inflight.get("collection")),
            "download": safe_int(inflight.get("download")),
        }

        stats["stop_requested"] = getattr(self, "_stop_requested", False) or self.stop_event.is_set()
        stats["stop_grace_seconds"] = safe_float(getattr(self, "_stop_grace_seconds", 0))
        stats["stop_elapsed_seconds"] = safe_float(self._stop_grace_elapsed_seconds())
        stats["hard_stop"] = getattr(self, "_hard_stop", False)
        stats["stop_level"] = "hard" if stats["hard_stop"] else ("soft" if stats["stop_requested"] else "")
        stats["stop_reason"] = str(getattr(self, "_hard_stop_reason", ""))

        def _fmt(v): return v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else str(v) if v else None
        stats["date_filter_active"] = bool(getattr(self, "start_date", None) or getattr(self, "end_date", None))
        stats["start_date"] = _fmt(getattr(self, "start_date", None))
        stats["end_date"] = _fmt(getattr(self, "end_date", None))

        return stats

    # ---------------------------------------------------------
    # ---------------------------------------------------------
    async def start_workflow(self, start_urls: List[str], progress_callback=None, stop_check=None, start_date=None, end_date=None, use_query_links_only: bool = False):
        """Start the integrated file crawling workflow."""
        self.seen_scan_urls.clear()
        self.seen_collection_urls.clear()
        self.seen_save_urls.clear()
        self.in_flight = {'scan': 0, 'collection': 0, 'download': 0}
        await self._initialize_job_queues()
        
        self.start_date = start_date
        self.end_date = end_date
        self.state = WorkflowState.RUNNING
        self.is_running = True
        self.progress_callback = progress_callback

        if getattr(self, "file_mode", False) and start_urls:
            try:
                from backend.board.board_content_workflow import BoardContentWorkflow
                normalized_seed = [ensure_url_scheme(u) for u in (start_urls or []) if u]
                bwf = BoardContentWorkflow()
                bwf.job_id = self.job_id
                bwf.chat_bot_id = self.chat_bot_id
                bwf.db_name = self.db_name
                bwf.server_domain = self.server_domain
                detail_urls = await bwf.discover_detail_urls_only(
                    normalized_seed or list(start_urls),
                    start_date=start_date,
                    end_date=end_date,
                    use_query_links_only=use_query_links_only,
                )
                logger.info(
                    "[FileDiscoverDebug][preexpand] job_id=%s seed_count=%s detail_count=%s first_seed=%s",
                    self.job_id,
                    len(normalized_seed or list(start_urls) or []),
                    len(detail_urls or []),
                    (normalized_seed or list(start_urls) or [None])[0],
                )
                if detail_urls:
                    start_urls = list(detail_urls)
                    use_query_links_only = True
                else:
                    logger.warning(
                        "[FileDiscoverDebug][preexpand_empty] job_id=%s seed_count=%s first_seed=%s use_query_links_only=%s",
                        self.job_id,
                        len(normalized_seed or list(start_urls) or []),
                        (normalized_seed or list(start_urls) or [None])[0],
                        use_query_links_only,
                    )
            except Exception as exc:
                logger.warning("[Workflow] file preexpand(detail urls) failed (continue): %s", exc)
        
        if start_urls:
            self.domain = urlparse(start_urls[0]).netloc or "unknown"
        
        if self.progress_callback:
            self.progress_callback(self.get_stats())

        max_depth = 0 if use_query_links_only else max(0, min(safe_int(os.getenv("CRAWL_MAX_DEPTH", "2"), 2), 10))
        scan_task = asyncio.create_task(self.stream_scan_results(start_urls, stop_check, None, max_depth=max_depth))
        
        try:
            await scan_task
        except asyncio.CancelledError:
            self.state = WorkflowState.CANCELLED
            self.final_status = "cancelled"
        finally:
            await self.finalize_workflow()
            await self._cleanup_job_queues()
            self.is_running = False
            if self.state in [WorkflowState.RUNNING, WorkflowState.STOPPING]:
                self.state = WorkflowState.COMPLETED
            self.final_status = self.state.value

    async def stream_scan_results(self, start_urls: List[str], stop_check: Optional[Callable], page_profile: Optional[Dict[str, Any]], max_depth: int):
        """Stream scan results through worker queues and process progress events."""
        from core.crawler.manager import WorkerManager
        queues = self.queues
        scan_queue = queues.scan_queue
        progress_queue = queues.progress_queue
        
        self.worker_manager = WorkerManager(
            on_collection_batch=self._handle_collection_batch,
            chat_bot_id=self.chat_bot_id,
            db_name=self.db_name,
            start_date=self.start_date,
            end_date=self.end_date,
            max_depth=max_depth,
            job_queues=queues,
        )
        self._worker_manager_start_task = asyncio.create_task(self.worker_manager.start())
        
        progress_queue.put_nowait({'type': 'in_flight', 'stage': 'scan', 'delta': len(start_urls)})
        for url in start_urls:
            try:
                eff_colle = str(getattr(self, "colle_mode", "") or getattr(self, "ui_colle", "") or "").strip().lower()
            except Exception:
                eff_colle = ""
            await scan_queue.put({
                'url': url, 'depth': 0, 'job_id': self.job_id, 'chat_bot_id': self.chat_bot_id,
                'db_name': self.db_name, 'start_date': self.start_date, 'end_date': self.end_date,
                'page_profile': {'is_dynamic': False, 'static_only': True},
                'colle': eff_colle,
                'memo': getattr(self, "memo", "") or "",
                'defer_save_batch_until_learn_list': True,
            })

        last_worker_check = asyncio.get_event_loop().time()
        
        try:
            while True:
                current_time = asyncio.get_event_loop().time()
                stopping = (self.state == WorkflowState.STOPPING) or self.stop_event.is_set() or getattr(self, "_stop_requested", False)
                
                if stopping and not getattr(self, "_hard_stop", False) and self._stop_grace_exceeded():
                    await self._force_hard_stop(reason="stop_grace_timeout")
                    break

                if not stopping and (current_time - last_worker_check >= 2.0):
                    last_worker_check = current_time
                    if self.worker_manager and hasattr(self.worker_manager, 'tasks'):
                        dead_tasks = [t for t in self.worker_manager.tasks if t.done()]
                        if dead_tasks and self.is_running:
                            logger.warning("[Workflow] %s worker tasks stopped; restarting worker manager", len(dead_tasks))
                            await self.worker_manager.stop(graceful=False)
                            await asyncio.sleep(2)
                            self._worker_manager_start_task = asyncio.create_task(self.worker_manager.start())

                if not stopping and self.state != WorkflowState.RUNNING:
                    break

                await self._process_progress_updates(progress_queue, stopping)

                in_flight_total = sum(self.in_flight.values())
                all_queues_empty = (
                    scan_queue.empty()
                    and queues.collection_batch_queue.empty()
                    and queues.large_collection_batch_queue.empty()
                    and queues.save_batch_queue.empty()
                    and queues.study_batch_queue.empty()
                )
                pending_save_done = len(getattr(self, "pending_save_urls", set()) or set()) <= 0
                if (
                    in_flight_total <= 0
                    and all_queues_empty
                    and progress_queue.empty()
                    and pending_save_done
                    and not self._trigger_tasks
                    and not self._post_download_tasks
                ):
                    break

                await asyncio.sleep(0.1)
                
        except asyncio.CancelledError:
            self.state = WorkflowState.CANCELLED
        finally:
            if not getattr(self, "_hard_stop", False):
                await self._await_post_download_tasks()
                await self._await_trigger_tasks()
            if self.worker_manager:
                await self.worker_manager.stop(graceful=True)

    async def _process_progress_updates(self, progress_queue, stopping: bool):
        """Drain progress queue updates and synchronize workflow counters."""
        try:
            update = await asyncio.wait_for(progress_queue.get(), timeout=0.1)
            updates = [update]
            while len(updates) < 200:
                try: updates.append(progress_queue.get_nowait())
                except asyncio.QueueEmpty: break

            updates.sort(key=lambda u: 0 if u.get('type') in ('file_saved', 'download_skipped') else 1)

            for item in updates:
                u_type = item.get('type')
                
                if u_type == 'in_flight':
                    stage, delta = item.get('stage'), item.get('delta', 0)
                    if stage in self.in_flight:
                        self.in_flight[stage] = max(0, self.in_flight[stage] + delta)

                elif u_type in ('scan', 'file_found'):
                    scan_items = item.get('items', []) or []
                    self.seen_scan_urls.update(scan_items)
                    try:
                        append_stage_urls(
                            stage="scan",
                            urls=scan_items,
                            job_id=self.job_id,
                            db_name=self.db_name,
                        )
                    except Exception:
                        pass
                elif u_type == 'collection':
                    urls = item.get('items', [])
                    normed = [self._normalize_count_url(u) for u in urls if u]
                    self.seen_collection_urls.update(normed)
                    for u in normed: self.pending_save_urls.add(u)
                    try:
                        append_stage_urls(
                            stage="collection",
                            urls=normed,
                            job_id=self.job_id,
                            db_name=self.db_name,
                        )
                    except Exception:
                        pass

                elif u_type == 'file_saved':
                    file_info = item.get('file_info', {})
                    norm_url = self._normalize_count_url(file_info.get('url'))
                    if norm_url in self.pending_save_urls:
                        self.pending_save_urls.discard(norm_url)
                    try:
                        if norm_url:
                            append_stage_urls(
                                stage="save",
                                urls=[{"url": norm_url, "file_path": file_info.get("file_path") or file_info.get("local_path")}],
                                job_id=self.job_id,
                                db_name=self.db_name,
                            )
                    except Exception:
                        pass
                    self._schedule_post_download_processing(file_info)
                
                elif u_type == 'download_skipped':
                    norm_url = self._normalize_count_url(item.get('url'))
                    if norm_url in self.pending_save_urls:
                        self.pending_save_urls.discard(norm_url)

                if self.progress_callback:
                    self.progress_callback(self.get_stats())
                progress_queue.task_done()
            await self._update_crawling_log_stats_async(reason="progress_queue")
                
        except asyncio.TimeoutError:
            pass

    def _schedule_post_download_processing(self, file_info: Dict[str, Any]) -> None:
        """Schedule post-download DB save and learning handoff."""
        if not file_info: return
        async def _runner(info: Dict[str, Any]) -> None:
            async with self._post_download_sem:
                await self._process_post_download(info)
        task = asyncio.create_task(_runner(file_info))
        self._post_download_tasks.add(task)
        task.add_done_callback(lambda t: self._post_download_tasks.discard(t))

    async def _get_learning_runtime(self):
        async with self._learn_runtime_lock:
            if self._learn_redis and self._learn_job_manager and self._learn_job_progress:
                return self._learn_redis, self._learn_job_manager, self._learn_job_progress
            redis = await get_redis()
            job_man = AsyncJobManager(redis)
            job_prog = AsyncJobProgress(redis)
            self._learn_redis = redis
            self._learn_job_manager = job_man
            self._learn_job_progress = job_prog
            return redis, job_man, job_prog

    def _schedule_immediate_learning(self, study_item: Dict[str, Any]) -> None:
        async def _runner(item: Dict[str, Any]) -> None:
            async with self._trigger_sem:
                try:
                    from core.crawler.workers.study import study_process_batch_items

                    redis, job_man, job_prog = await self._get_learning_runtime()
                    logger.debug(
                        "[LearningTrace][integrated.immediate_learning.start] job_id=%s db=%s db_id=%s file=%s",
                        item.get("job_id"),
                        item.get("db_name"),
                        item.get("db_id"),
                        os.path.basename(item.get("file_path") or item.get("local_path") or ""),
                    )
                    await study_process_batch_items([item], redis, job_man, job_prog)
                    logger.debug(
                        "[LearningTrace][integrated.immediate_learning.done] job_id=%s db=%s db_id=%s file=%s",
                        item.get("job_id"),
                        item.get("db_name"),
                        item.get("db_id"),
                        os.path.basename(item.get("file_path") or item.get("local_path") or ""),
                    )
                except Exception as exc:
                    logger.error(
                        "[LearningError][integrated.immediate_learning.error] job_id=%s db=%s db_id=%s err=%s",
                        item.get("job_id"),
                        item.get("db_name"),
                        item.get("db_id"),
                        exc,
                        exc_info=True,
                    )
                    if self.job_queues:
                        await self.job_queues.save_batch_queue.put(item)
                        try:
                            await self.job_queues.save_batch_queue.flush()
                        except Exception:
                            pass
                        logger.debug(
                            "[LearningTrace][integrated.immediate_learning.fallback_queue] job_id=%s db=%s db_id=%s",
                            item.get("job_id"),
                            item.get("db_name"),
                            item.get("db_id"),
                        )

        task = asyncio.create_task(_runner(dict(study_item or {})))
        self._trigger_tasks.add(task)
        task.add_done_callback(lambda t: self._trigger_tasks.discard(t))

    async def _await_post_download_tasks(self) -> None:
        tasks = list(self._post_download_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _await_trigger_tasks(self) -> None:
        tasks = list(self._trigger_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _update_crawling_log_save_immediate(self) -> None:
        # Save progress is published through Redis/SSE immediately.  Do not
        # synchronously contend on the crawling-log row for every file.
        logger.debug(
            "[Workflow][CrawlingLog] deferred save progress | job_id=%s db=%s",
            getattr(self, "job_id", ""),
            getattr(self, "db_name", ""),
        )

    async def _update_crawling_log_stats_async(
        self,
        *,
        force: bool = False,
        status: Optional[str] = None,
        reason: str = "progress",
    ) -> None:
        if not (getattr(self, "job_id", None) and getattr(self, "db_name", None)):
            return
        if status is None:
            # Redis/SSE owns live counters.  ASADAL_CRAWLING_LOG is refreshed
            # explicitly by the dashboard or once at a terminal state.
            return
        async with self._crawling_log_update_lock:
            now = time.monotonic()
            interval = max(1.0, safe_float(getattr(self, "_crawling_log_update_interval", 5.0), 5.0))
            if not force and status is None and now - self._last_crawling_log_update_at < interval:
                return
            try:
                from db.crawl_db_manager import update_crawling_log_counters

                snapshot = self.get_stats()
                await update_crawling_log_counters(
                    job_id=str(self.job_id),
                    scan=safe_int(snapshot.get("scan_count", 0), 0),
                    collection=safe_int(snapshot.get("collection_count", 0), 0),
                    saved=safe_int(snapshot.get("save_count", 0), 0),
                    study=safe_int(snapshot.get("study_count", 0), 0),
                    status=status,
                    dbname=str(self.db_name),
                    log_id=getattr(self, "craw_id", None),
                    colle=str(getattr(self, "colle", None) or getattr(self, "colle_mode", None) or "file"),
                )
                self._last_crawling_log_update_at = now
                logger.debug(
                    "[Workflow][CrawlingLog] stats update | job_id=%s reason=%s status=%s scan=%s collection=%s save=%s study=%s",
                    self.job_id,
                    reason,
                    status,
                    snapshot.get("scan_count", 0),
                    snapshot.get("collection_count", 0),
                    snapshot.get("save_count", 0),
                    snapshot.get("study_count", 0),
                )
            except Exception as exc:
                logger.debug(
                    "[Workflow][CrawlingLog] stats update failed | job_id=%s reason=%s err=%s",
                    getattr(self, "job_id", ""),
                    reason,
                    exc,
                )
    async def _process_post_download(self, file_info: Dict[str, Any]) -> None:
        """Save a downloaded file to MariaDB and enqueue learning when enabled."""
        if getattr(self, "_hard_stop", False) or (self.stop_event.is_set() and self._stop_grace_exceeded()):
            return

        file_path = file_info.get('file_path') or file_info.get('local_path')
        if not file_path or not os.path.exists(file_path):
            return

        norm_url = self._normalize_count_url(file_info.get("url"))
        if norm_url:
            async with self._stats_lock:
                if norm_url in self.saved_urls or norm_url in self._save_claimed_urls:
                    return
                self._save_claimed_urls.add(norm_url)

        if "cate1" not in file_info and self.cate1: file_info["cate1"] = self.cate1
        # cate2_extracted = extract_cate2_from_web_title(file_info.get("web_title", ""))
        # if cate2_extracted: file_info["cate2"] = cate2_extracted

        file_info["type"] = "file"
        file_info["content_type"] = "file"

        db_id = None
        try:
            db_id = await insert_into_learn_list(chat_bot_id=self.chat_bot_id, db_name=self.db_name, file_info=file_info)
            if db_id:
                self.saved_db_ids.append(str(db_id))
                if norm_url:
                    async with self._stats_lock:
                        self.saved_urls.add(norm_url)
                await self._update_crawling_log_save_immediate()
                try:
                    if norm_url:
                        append_stage_urls(
                            stage="save_db",
                            urls=[{"url": norm_url, "db_id": str(db_id)}],
                            job_id=self.job_id,
                            db_name=self.db_name,
                        )
                except Exception:
                    pass
                if self.progress_callback:
                    self.progress_callback(self.get_stats())

                if getattr(self, "enable_learning", True) and self.job_queues:
                    count_key = f"db:{db_id}"
                    async with self._stats_lock:
                        if count_key not in self._counted_study_keys:
                            self._counted_study_keys.add(count_key)
                            self._pending_study_keys.add(count_key)
                    study_item = dict(file_info)
                    study_item["chat_bot_id"] = self.chat_bot_id
                    study_item["db_name"] = self.db_name
                    study_item["job_id"] = self.job_id
                    if "content_type" not in study_item and study_item.get("type"):
                        study_item["content_type"] = study_item.get("type")
                    study_item["db_id"] = db_id
                    study_item["_count_key"] = count_key
                    self._schedule_immediate_learning(study_item)
        finally:
            if norm_url:
                async with self._stats_lock:
                    self._save_claimed_urls.discard(norm_url)

        await self._update_crawling_log_stats_async()


def run_save_stage(items_to_save: List[Dict[str, Any]], domain: Optional[str], start_date=None, end_date=None) -> List[Dict[str, Any]]:
    """Build enriched file metadata for the save stage."""
    processed = []
    domain_value = domain or "unknown"
    for item in items_to_save:
        url = item.get("url")
        if not url: continue
        
        # Build enriched file metadata.
        enriched = {
            "url": url,
            "name": item.get("name", url.split("/")[-1].split("?")[0]),
            "source_page": item.get("source_page", ""),
            "type": item.get("type", "file"),
            "domain": domain_value,
            "reg_date": item.get("reg_date"),
            "title": item.get("title", ""),
            "web_title": item.get("web_title", ""),
            "cate1": item.get("cate1"),
            "cate2": item.get("cate2"),
            "original_meta": item.get("original_meta", {})
        }
        processed.append(enriched)
    return processed


def save_worker_log_json(job_id: str, normed_urls: list, names: list, collection_count: Optional[int] = None):
    """Persist collected URL metadata for save worker diagnostics."""
    if not job_id or not normed_urls: return
    try:
        target_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "core", "crawler", "workers")
        os.makedirs(target_dir, exist_ok=True)
        filepath = os.path.join(target_dir, f"workflow_collection_{job_id}.json")
        
        data = []
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                
        for i, u in enumerate(normed_urls):
            if u and not any(e.get("url") == u for e in data):
                data.append({
                    "url": u,
                    "filename": names[i] if i < len(names) else u.split("/")[-1],
                    "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "collection_count": collection_count if collection_count else (len(data) + 1)
                })
                
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.getLogger(__name__).warning(f"[JSON_SAVE_ERR] {e}")        

