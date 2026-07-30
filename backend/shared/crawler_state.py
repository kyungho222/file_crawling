import asyncio
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

TERMINAL_WORKFLOW_STATUSES = {
    "stop",
    "stopped",
    "cancelled",
    "canceled",
    "cancel",
    "coll_stop",
    "completed",
    "complete",
    "finished",
    "ok",
    "error",
    "failed",
    "fail",
    "exception",
}

logger = logging.getLogger("backend.shared.crawler_state")
CONCURRENT_CRAWL_LOG_PREFIX = "[ConcurrentCrawlStartDebug]"


class CrawlerState:
    """
    job_id ?⑥쐞 ?꾩뿭 ?곹깭(硫붾え由? 愿由?
    - workflows: job_id -> workflow ?몄뒪?댁뒪
    - workflow_tasks: job_id -> asyncio.Task(run_workflow_task)
    - job_history/job_history_events: 吏꾪뻾/醫낅즺 ?곹깭 異붿쟻(?붾쾭源?蹂듦뎄??
    """

    def __init__(self) -> None:
        self.workflows: Dict[str, Any] = {}
        self.workflow_tasks: Dict[str, asyncio.Task] = {}
        self.active_clients: set[Any] = set()
        self.admitted_workflow_jobs: set[str] = set()
        self.waiting_workflow_jobs: set[str] = set()
        self._workflow_slot_cond: Optional[asyncio.Condition] = None
        self._workflow_slot_cond_loop: Optional[asyncio.AbstractEventLoop] = None
        # job_id ?⑥쐞濡?"援щ룆 ?뺤씤"??湲곕줉 (?대씪?댁뼵?멸? 援щ룆 ?꾨즺 ??POST濡??뚮젮二쇰㈃ 異붽?)
        # ?쒕쾭 硫붾え由?湲곕컲?대?濡?硫???쒕쾭 ?섍꼍?먯꽌???대떦 ?쒕쾭???곌껐???대씪?댁뼵?몃쭔 諛섏쁺?⑸땲??
        self.confirmed_subscriptions: set[str] = set()
        self.job_history: Dict[str, Dict[str, Any]] = {}
        self.job_history_events: Dict[str, List[Dict[str, Any]]] = {}

    def _prune_history(self) -> None:
        try:
            max_jobs = int(os.getenv("CRAWLER_JOB_HISTORY_MAX_JOBS", "300") or "300")
        except Exception:
            max_jobs = 300
        try:
            max_events = int(os.getenv("CRAWLER_JOB_HISTORY_EVENT_LIMIT", "50") or "50")
        except Exception:
            max_events = 50
        max_jobs = max(10, min(max_jobs, 5000))
        max_events = max(5, min(max_events, 500))

        for job_id, events in list(self.job_history_events.items()):
            try:
                if isinstance(events, list) and len(events) > max_events:
                    self.job_history_events[job_id] = events[-max_events:]
            except Exception:
                continue

        while len(self.job_history) > max_jobs:
            try:
                oldest_job_id = next(iter(self.job_history))
            except StopIteration:
                break
            self.job_history.pop(oldest_job_id, None)
            self.job_history_events.pop(oldest_job_id, None)
            try:
                self.confirmed_subscriptions.discard(oldest_job_id)
            except Exception:
                pass

    def record_history(
        self,
        job_id: str,
        status: str,
        detail: str = "",
        db_name: Optional[str] = None,
        chat_bot_id: Optional[str] = None,
    ) -> None:
        if not chat_bot_id and job_id in self.job_history:
            try:
                chat_bot_id = self.job_history[job_id].get("chat_bot_id")
            except Exception:
                chat_bot_id = chat_bot_id
        entry = {
            "status": status,
            "detail": detail,
            "db_name": db_name,
            "timestamp": datetime.now().isoformat(),
            "chat_bot_id": chat_bot_id,
        }
        self.job_history[job_id] = entry
        self.job_history_events.setdefault(job_id, []).append(entry)
        self._prune_history()

    def _get_workflow_slot_condition(self) -> asyncio.Condition:
        loop = asyncio.get_running_loop()
        if self._workflow_slot_cond is None or self._workflow_slot_cond_loop is not loop:
            self._workflow_slot_cond = asyncio.Condition()
            self._workflow_slot_cond_loop = loop
        return self._workflow_slot_cond

    def _is_file_crawl_workflow(self, workflow: Any = None) -> bool:
        if workflow is None:
            return False
        try:
            if bool(getattr(workflow, "is_attachment_file_crawl_workflow", False)):
                return True
        except Exception:
            pass
        for attr in ("colle", "colle_mode", "ui_colle", "content_type"):
            try:
                value = str(getattr(workflow, attr, "") or "").strip().lower()
            except Exception:
                value = ""
            if value in {"file", "attach", "attachment"}:
                return True
        try:
            if bool(getattr(workflow, "file_dashboard", False)):
                return True
        except Exception:
            pass
        try:
            source = str(getattr(workflow, "start_urls_override_source", "") or "").strip()
            if source in {"file_crawl_post_db", "file_crawl_post_db_stream"}:
                return True
        except Exception:
            pass
        return False

    def _parse_workflow_slot_limit(self, raw: Any, *, default: int, max_value: int = 64) -> int:
        raw_text = str(raw if raw is not None else "").strip()
        if not raw_text:
            return int(default or 0)
        try:
            parsed = int(raw_text)
        except Exception:
            parsed = int(default or 0)
        if parsed <= 0:
            return 0
        return max(1, min(parsed, max_value))

    def get_workflow_slot_limit(self, workflow: Any = None) -> int:
        if self._is_file_crawl_workflow(workflow):
            raw = os.getenv("FILE_CRAWL_MAX_ACTIVE_WORKFLOWS")
            if raw is None:
                raw = os.getenv("FILE_CRAWL_MAX_ACTIVE_JOBS")
            # File crawling must scale at the workflow admission layer. Target-domain
            # concurrency and MariaDB job-share caps remain the real backpressure.
            return self._parse_workflow_slot_limit(raw, default=0, max_value=256)

        raw = str(os.getenv("CRAWL_MAX_ACTIVE_WORKFLOWS", "") or "").strip()
        if raw:
            return self._parse_workflow_slot_limit(raw, default=0, max_value=64)

        share_raw = str(os.getenv("CRAWL_WORKFLOW_DB_POOL_SHARE", "8") or "8").strip()
        try:
            pool_share = int(share_raw)
        except Exception:
            pool_share = 4
        pool_share = max(1, min(pool_share, 32))

        try:
            from backend.shared.config import Config

            total_cap = int(getattr(Config, "DB_POOL_MAX", 16) or 16)
        except Exception:
            total_cap = 16

        derived = total_cap // pool_share
        if derived <= 0:
            derived = 1
        return max(1, min(derived, 32))

    def get_workflow_slot_snapshot(self, workflow: Any = None) -> Dict[str, int]:
        return {
            "limit": int(self.get_workflow_slot_limit(workflow=workflow) or 0),
            "active": len(self.admitted_workflow_jobs),
            "waiting": len(self.waiting_workflow_jobs),
        }

    def get_workflow_debug_snapshot(self) -> Dict[str, Any]:
        active_tasks: List[str] = []
        done_tasks: List[str] = []
        try:
            for jid, task in list(self.workflow_tasks.items()):
                if task is not None and not task.done():
                    active_tasks.append(str(jid))
                else:
                    done_tasks.append(str(jid))
        except Exception:
            active_tasks = []
            done_tasks = []

        active_workers: List[str] = []
        try:
            for jid, task in list(getattr(self, "active_worker_tasks", {}) or {}).items():
                if task is not None and not task.done():
                    active_workers.append(str(jid))
        except Exception:
            active_workers = []

        return {
            "slot_limit": int(self.get_workflow_slot_limit() or 0),
            "admitted": sorted(str(jid) for jid in self.admitted_workflow_jobs),
            "waiting": sorted(str(jid) for jid in self.waiting_workflow_jobs),
            "workflow_tasks_active": sorted(active_tasks),
            "workflow_tasks_done": sorted(done_tasks),
            "active_worker_tasks": sorted(active_workers),
            "workflows": sorted(str(jid) for jid in self.workflows.keys()),
        }

    def has_active_workflow_slot(self, job_id: Optional[str]) -> bool:
        return bool(job_id and str(job_id) in self.admitted_workflow_jobs)

    def _workflow_stop_requested(self, workflow: Any) -> bool:
        if workflow is None:
            return False
        try:
            stop_event = getattr(workflow, "stop_event", None)
            if stop_event is not None and bool(stop_event.is_set()):
                return True
        except Exception:
            pass
        try:
            final_status = str(getattr(workflow, "final_status", "") or "").strip().lower()
            if final_status in TERMINAL_WORKFLOW_STATUSES:
                return True
        except Exception:
            pass
        try:
            if bool(getattr(workflow, "_stop_requested", False)):
                return True
        except Exception:
            pass
        return False

    async def acquire_workflow_slot(self, job_id: str, workflow: Any = None) -> Dict[str, Any]:
        jid = str(job_id or "").strip()
        limit = self.get_workflow_slot_limit(workflow=workflow)
        if not jid:
            return {"granted": False, "waited": False, "cancelled": False, **self.get_workflow_slot_snapshot(workflow=workflow)}
        if limit <= 0:
            self.admitted_workflow_jobs.add(jid)
            return {"granted": True, "waited": False, "cancelled": False, **self.get_workflow_slot_snapshot(workflow=workflow)}

        cond = self._get_workflow_slot_condition()
        waited = False
        wait_started_at = time.monotonic()
        last_wait_log_at = 0.0
        try:
            wait_log_sec = float(os.getenv("WORKFLOW_SLOT_WAIT_LOG_SEC", "30") or "30")
        except Exception:
            wait_log_sec = 30.0
        wait_log_sec = max(5.0, min(wait_log_sec, 300.0))
        async with cond:
            self.waiting_workflow_jobs.add(jid)
            try:
                while True:
                    if self._workflow_stop_requested(workflow):
                        return {"granted": False, "waited": waited, "cancelled": True, **self.get_workflow_slot_snapshot(workflow=workflow)}
                    if jid in self.admitted_workflow_jobs:
                        return {"granted": True, "waited": waited, "cancelled": False, **self.get_workflow_slot_snapshot(workflow=workflow)}
                    if len(self.admitted_workflow_jobs) < limit:
                        self.admitted_workflow_jobs.add(jid)
                        logger.info(
                            "%s[slot_granted] job_id=%s limit=%s active=%s waiting=%s admitted=%s",
                            CONCURRENT_CRAWL_LOG_PREFIX,
                            jid,
                            limit,
                            len(self.admitted_workflow_jobs),
                            len(self.waiting_workflow_jobs),
                            sorted(self.admitted_workflow_jobs),
                        )
                        return {"granted": True, "waited": waited, "cancelled": False, **self.get_workflow_slot_snapshot(workflow=workflow)}
                    waited = True
                    now = time.monotonic()
                    if now - last_wait_log_at >= wait_log_sec:
                        last_wait_log_at = now
                        logger.info(
                            "%s[slot_waiting] job_id=%s wait_sec=%.1f limit=%s active=%s waiting=%s admitted=%s",
                            CONCURRENT_CRAWL_LOG_PREFIX,
                            jid,
                            now - wait_started_at,
                            limit,
                            len(self.admitted_workflow_jobs),
                            len(self.waiting_workflow_jobs),
                            sorted(self.admitted_workflow_jobs),
                        )
                    try:
                        await asyncio.wait_for(cond.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
            finally:
                self.waiting_workflow_jobs.discard(jid)

    async def release_workflow_slot(self, job_id: Optional[str]) -> None:
        jid = str(job_id or "").strip()
        if not jid:
            return
        cond = self._get_workflow_slot_condition()
        async with cond:
            self.waiting_workflow_jobs.discard(jid)
            if jid in self.admitted_workflow_jobs:
                self.admitted_workflow_jobs.discard(jid)
                cond.notify_all()
                logger.info(
                    "%s[slot_released] job_id=%s limit=%s active=%s waiting=%s admitted=%s",
                    CONCURRENT_CRAWL_LOG_PREFIX,
                    jid,
                    self.get_workflow_slot_limit(),
                    len(self.admitted_workflow_jobs),
                    len(self.waiting_workflow_jobs),
                    sorted(self.admitted_workflow_jobs),
                )

    def release_workflow_slot_sync(self, job_id: Optional[str]) -> bool:
        jid = str(job_id or "").strip()
        if not jid:
            return False
        self.waiting_workflow_jobs.discard(jid)
        if jid not in self.admitted_workflow_jobs:
            return False
        self.admitted_workflow_jobs.discard(jid)
        return True


crawler_state = CrawlerState()






