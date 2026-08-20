# core/crawler/manager.py
"""
워커 매니저 - Playwright 브라우저 관리 및 워커 생성
"""
import asyncio
import logging
import time
import os
from typing import Awaitable, Callable, Optional, List, Dict, Any
from playwright.async_api import async_playwright, Browser
from config.settings import settings
from core.crawler.browser_launch import (
    BROWSER_LAUNCH_SEMAPHORE,
    MAX_RETIRED_BROWSERS,
    RETIRED_BROWSER_FORCE_CLOSE_SECONDS,
    get_default_launch_args,
    filter_launch_args,
    get_default_navigation_timeout_ms,
    get_default_timeout_ms,
)
from core.crawler.queues import JobQueues
from core.crawler.workers.scan import scan_worker
from core.crawler.workers.download import download_worker
from core.crawler.workers.study import study_worker
from core.crawler.workers.collection import collection_worker
from core.crawler.dedup import CollectionDeduplicator
from db.repository import DBRepository
from utils.runtime_flags import is_no_limits_mode

logger = logging.getLogger(__name__)

class WorkerManager:
    """워커 풀 관리 및 Playwright 브라우저 라이프사이클 관리"""
    
    def __init__(
        self,
        progress=None,
        on_collection_batch: Optional[Callable[[List[Dict[str, Any]]], Awaitable[None]]] = None,
        chat_bot_id: Optional[str] = None,
        db_name: Optional[str] = None,
        start_date=None,
        end_date=None,
        max_depth: Optional[int] = None,
        job_queues: Optional[JobQueues] = None,
        worker_config_override: Optional[Dict[str, Any]] = None,
        defer_browser_launch: bool = False,
    ):
        self.tasks: List[asyncio.Task] = []
        # 역할별 task (부분 중단/그레이스풀 중단 제어용)
        self.scan_tasks: List[asyncio.Task] = []
        self.collection_tasks: List[asyncio.Task] = []
        self.download_tasks: List[asyncio.Task] = []
        self.study_tasks: List[asyncio.Task] = []
        self.flush_task: Optional[asyncio.Task] = None
        self.playwright = None
        self.browser = None
        self.db_repo = DBRepository()
        self._last_batch_wait_log = 0.0
        self._batch_wait_log_interval = 60.0
        self.collection_deduplicator = CollectionDeduplicator()
        # scan 단계에서 fileDown 같은 파일 URL이 여러 scan worker에 의해 중복 enqueue 되는 것을 방지
        self.file_deduplicator = CollectionDeduplicator()
        self._scan_heartbeat_enabled = False
        self.progress = progress
        self.on_collection_batch = on_collection_batch
        self.job_queues = job_queues or JobQueues()
        self.chat_bot_id = chat_bot_id
        self.db_name = db_name
        self.start_date = start_date
        self.end_date = end_date
        self.max_depth = max_depth
        self.worker_config_override = dict(worker_config_override or {})
        self.defer_browser_launch = bool(defer_browser_launch)
        self.on_collection_batch = on_collection_batch
        self._semaphore_acquired = False  # 동시 브라우저 수 제한용
        self._browser_use_count: Dict[int, int] = {}
        self._retired_browsers: Dict[int, Browser] = {}
        self._retired_browser_cleanup_tasks: set[asyncio.Task] = set()
        self._retired_browser_force_cleanup_tasks: set[asyncio.Task] = set()
        self._normal_download_semaphore: Optional[asyncio.Semaphore] = None
        self._download_runtime_config: Dict[str, Any] = {}
        self._elastic_download_tasks: set[asyncio.Task] = set()
        self._elastic_download_sequence = 0

    async def start(self):
        """워커 풀 시작 및 Playwright 초기화"""
        try:
            logger.info(
                "[WorkerManager] start | db=%s chat_bot_id=%s max_depth=%s",
                self.db_name,
                (str(self.chat_bot_id)[:8] + "..." if self.chat_bot_id else None),
                self.max_depth,
            )
        except Exception:
            pass
        # Windows에서 이벤트 루프 정책 확인 (이미 main.py에서 설정됨)
        import sys
        if sys.platform == 'win32':
            policy = asyncio.get_event_loop_policy()
            if isinstance(policy, asyncio.WindowsProactorEventLoopPolicy):
                logger.debug("[WorkerManager] WindowsProactorEventLoopPolicy confirmed")
            else:
                logger.warning(
                    "[WorkerManager] Event loop policy is %s, not WindowsProactorEventLoopPolicy",
                    type(policy).__name__,
                )
                logger.warning("[WorkerManager] Playwright may fail on Windows. Update loop policy before startup.")
        
        # Start Playwright
        self.playwright = await async_playwright().start() if not self.defer_browser_launch else None
        # 동시 브라우저 수 제한: 세마포어 획득 후 launch (꽉 차면 대기)
        if not self.defer_browser_launch:
            await BROWSER_LAUNCH_SEMAPHORE.acquire()
        self._semaphore_acquired = not self.defer_browser_launch
        try:
            if not self.defer_browser_launch:
                self.browser = await self._launch_browser()
        except Exception:
            if self._semaphore_acquired:
                BROWSER_LAUNCH_SEMAPHORE.release()
                self._semaphore_acquired = False
            raise
        finally:
            if not getattr(self, "browser", None) and getattr(self, "_semaphore_acquired", False):
                try:
                    BROWSER_LAUNCH_SEMAPHORE.release()
                    self._semaphore_acquired = False
                except Exception:
                    pass
        if self.defer_browser_launch:
            logger.info(
                "[WorkerManager] browser launch deferred; direct HTTP workers start first | db=%s",
                self.db_name,
            )
        context_options = dict(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            default_navigation_timeout=get_default_navigation_timeout_ms(),
            default_timeout=get_default_timeout_ms(),
        )

        cfg = dict(getattr(settings, "worker_config", {}) or {})
        if self.worker_config_override:
            cfg.update(self.worker_config_override)

        # Serialize browser relaunch across workers (prevents TargetClosedError races)
        self._relaunch_lock = asyncio.Lock()

        # max_depth 설정: 인스턴스 변수가 있으면 사용, 없으면 설정값 사용
        effective_max_depth = (
            self.max_depth
            if self.max_depth is not None
            else cfg.get("max_depth", settings.crawl_settings.get("max_depth", 1))
        )
        # ✅ 정책:
        # - 기본 모드: depth는 2로 제한 (단, 0을 명시한 경우는 그대로 유지)
        # - 무제한 모드(CRAWL_NO_LIMITS=1): depth 상한을 두지 않는다(0은 그대로 유지)
        try:
            if effective_max_depth is None:
                effective_max_depth = 2
            else:
                eff = int(effective_max_depth)
                if eff == 0:
                    effective_max_depth = 0
                else:
                    effective_max_depth = max(eff, 0) if is_no_limits_mode() else min(max(eff, 0), 2)
        except Exception:
            effective_max_depth = 2

        # Create Workers 순차 기동 (과도한 초기 부하 방지)
        # NOTE:
        # - on_collection_batch 콜백을 사용하더라도(IntegratedWorkflow), 다운로드/학습 워커는 필요하다.
        #   IntegratedWorkflow는 collection을 콜백으로 처리한 뒤, download 큐(collection_batch_queue)에 직접 투입한다.
        #   따라서 여기서 download/study 워커를 조건부로 끄면 save~study 카운트가 진행되지 않는다.
        forward_to_download = self.on_collection_batch is None
        try:
            stage_boot_wait_sec = float(os.getenv("WORKER_MANAGER_STAGE_BOOT_WAIT_SEC", "0.1") or "0.1")
        except Exception:
            stage_boot_wait_sec = 0.1
        stage_boot_wait_sec = max(0.0, min(stage_boot_wait_sec, 1.0))

        self._scan_heartbeat_enabled = True
        await self._start_scan_workers(cfg, context_options, effective_max_depth)
        logger.info("[WorkerManager] Scan workers ready (%s)", len(self.scan_tasks))
        # 초기 탐색이 실제로 진행되도록 짧은 대기
        if stage_boot_wait_sec:
            await asyncio.sleep(stage_boot_wait_sec)

        await self._start_collection_workers(cfg, forward_to_download)
        logger.info("[WorkerManager] Collection workers ready (%s)", len(self.collection_tasks))
        if stage_boot_wait_sec:
            await asyncio.sleep(stage_boot_wait_sec)

        # download/study는 항상 시작 (queue 기반 파이프라인 유지)
        await self._start_download_workers(cfg)
        logger.info("[WorkerManager] Download workers ready (%s)", len(self.download_tasks))
        if stage_boot_wait_sec:
            await asyncio.sleep(stage_boot_wait_sec)

        await self._start_study_workers(cfg)
        logger.info("[WorkerManager] Study workers ready (%s)", len(self.study_tasks))

        logger.info("[WorkerManager] Started %s workers.", len(self.tasks))
        
        # Start periodic flush task
        self.flush_task = asyncio.create_task(self._periodic_flush())

    def _scan_heartbeat_guard(self) -> bool:
        return bool(self._scan_heartbeat_enabled)

    async def _start_scan_workers(self, cfg, context_options, effective_max_depth):
        """scan worker 기동"""
        for _ in range(cfg["scan_workers"]):
            t = asyncio.create_task(
                scan_worker(
                    in_queue=self.job_queues.scan_queue,
                    scan_batch_queue=self.job_queues.scan_batch_queue,
                    collection_batch_queue=self.job_queues.collection_batch_queue,
                    progress_queue=self.job_queues.progress_queue,
                    browser=self.browser,
                    max_depth=effective_max_depth,
                    context_options=context_options.copy(),
                    browser_relauncher=self._relaunch_browser,
                    max_concurrent_pages=2,
                    heartbeat_guard=self._scan_heartbeat_guard,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    chat_bot_id=self.chat_bot_id,
                    db_name=self.db_name,
                    file_deduplicator=self.file_deduplicator,
                )
            )
            self.scan_tasks.append(t)
            self.tasks.append(t)

    async def _start_collection_workers(self, cfg, forward_to_download: bool):
        """collection worker 기동"""
        # ✅ 병목 완화:
        # collection 워커 수는 scan과 동일일 필요가 없다. (HEAD 검증/DB 중복 체크가 느린 경우가 많음)
        # 설정값(collection_workers)이 있으면 그 값을 우선 사용한다.
        try:
            n_workers = int(cfg.get("collection_workers") or cfg.get("scan_workers") or 1)
        except Exception:
            n_workers = int(cfg.get("scan_workers") or 1)
        n_workers = max(1, n_workers)

        for _ in range(n_workers):
            t = asyncio.create_task(
                collection_worker(
                    self.job_queues.scan_batch_queue,
                    self.job_queues.collection_batch_queue if forward_to_download else None,
                    progress_queue=self.job_queues.progress_queue,
                    db_repo=self.db_repo,
                    deduplicator=self.collection_deduplicator,
                    on_valid_batch=self.on_collection_batch,
                    forward_to_queue=forward_to_download,
                    chat_bot_id=self.chat_bot_id,
                    db_name=self.db_name,
                    start_date=self.start_date,
                    end_date=self.end_date,
                )
            )
            self.collection_tasks.append(t)
            self.tasks.append(t)

    async def _start_download_workers(self, cfg):
        """download worker 기동"""
        # 다운로드 병렬성(워커 1개가 동시에 처리할 다운로드 개수)
        # - 기본값은 5로 상향(이전 2)하여 저장(다운로드) 단계 병목 완화
        # - 운영에서는 환경변수로 조절 가능
        try:
            # ✅ 워커 재배치(저장=학습):
            # 저장(다운로드)이 학습보다 너무 빨라지면 save_count가 먼저 증가하고 study가 밀린다.
            # 기본 동시 다운로드 수를 소폭 낮추고, 필요 시 환경변수로 올리도록 한다.
            if is_no_limits_mode():
                max_concurrent = 1_000_000
            elif cfg.get("download_max_concurrent") is not None:
                max_concurrent = int(cfg["download_max_concurrent"])
            else:
                # 기본 10: batch_size=1일 때 워커당 1건씩만 돌아가 저장이 선별보다 크게 밀리는 현상 완화
                max_concurrent = int(
                    os.getenv(
                        "DOWNLOAD_MAX_CONCURRENT",
                        str(getattr(settings, "DOWNLOAD_MAX_CONCURRENT", 5)),
                    )
                )
        except Exception:
            max_concurrent = int(getattr(settings, "DOWNLOAD_MAX_CONCURRENT", 5) or 5)
        max_concurrent = max(1, max_concurrent)

        total_workers = max(1, int(getattr(settings, "DOWNLOAD_WORKERS", 5) or 5))
        requested_large_workers = max(
            0,
            int(getattr(settings, "FILE_CRAWL_LARGE_DOWNLOAD_WORKERS", 2) or 0),
        )
        large_workers = min(requested_large_workers, max(0, total_workers - 1))
        requested_normal_workers = max(
            1,
            int(getattr(settings, "FILE_CRAWL_NORMAL_DOWNLOAD_WORKERS", total_workers) or total_workers),
        )
        normal_workers = min(requested_normal_workers, total_workers - large_workers)
        normal_workers = max(1, normal_workers)

        logger.info(
            "[WorkerManager][download_lanes] total=%s normal=%s large=%s max_concurrent=%s",
            total_workers,
            normal_workers,
            large_workers,
            max_concurrent,
        )
        shared_download_semaphore = asyncio.Semaphore(normal_workers)
        self._normal_download_semaphore = shared_download_semaphore
        self._download_runtime_config = {
            "max_concurrent": max_concurrent,
            "normal_workers": normal_workers,
            "large_workers": large_workers,
        }
        for i in range(normal_workers):
            worker_lane = "normal"
            worker_queue = self.job_queues.collection_batch_queue
            t = asyncio.create_task(
                download_worker(
                    worker_queue,
                    self.job_queues.save_batch_queue,
                    progress_queue=self.job_queues.progress_queue,
                    max_concurrent=max_concurrent,
                    browser=self.browser,
                    browser_getter=self.acquire_browser,
                    browser_releaser=self.release_browser,
                    browser_relauncher=self._relaunch_browser,
                    worker_id=i + 1,
                    worker_lane=worker_lane,
                    large_download_queue=(
                        self.job_queues.large_collection_batch_queue if large_workers else None
                    ),
                    shared_download_semaphore=shared_download_semaphore,
                )
            )
            self.download_tasks.append(t)
            self.tasks.append(t)
            self._log_download_task_lifecycle(t, worker_id=i + 1, lane=worker_lane)

        if large_workers:
            large_download_semaphore = asyncio.Semaphore(large_workers)
            for i in range(large_workers):
                t = asyncio.create_task(
                    download_worker(
                        self.job_queues.large_collection_batch_queue,
                        self.job_queues.save_batch_queue,
                        progress_queue=self.job_queues.progress_queue,
                        max_concurrent=max_concurrent,
                        browser=self.browser,
                        browser_getter=self.acquire_browser,
                        browser_releaser=self.release_browser,
                        browser_relauncher=self._relaunch_browser,
                        worker_id=normal_workers + i + 1,
                        worker_lane="large",
                        fallback_in_queue=self.job_queues.collection_batch_queue,
                        shared_download_semaphore=large_download_semaphore,
                    )
                )
                self.download_tasks.append(t)
                self.tasks.append(t)
                self._log_download_task_lifecycle(
                    t,
                    worker_id=normal_workers + i + 1,
                    lane="large",
                )

    async def ensure_elastic_normal_download_worker(
        self,
        *,
        reason: str,
        max_elastic_workers: int = 2,
        lifetime_sec: float = 600.0,
    ) -> bool:
        """Start one bounded normal-lane worker when large downloads block the base pool."""
        runtime = dict(getattr(self, "_download_runtime_config", {}) or {})
        normal_workers = int(runtime.get("normal_workers") or 0)
        if normal_workers < 1:
            return False

        elastic_tasks = getattr(self, "_elastic_download_tasks", None)
        if not isinstance(elastic_tasks, set):
            elastic_tasks = set()
            self._elastic_download_tasks = elastic_tasks
        elastic_tasks.intersection_update(
            task
            for task in elastic_tasks
            if isinstance(task, asyncio.Task) and not task.done()
        )
        max_elastic_workers = max(0, min(int(max_elastic_workers or 0), 4))
        if len(elastic_tasks) >= max_elastic_workers:
            return False

        semaphore = getattr(self, "_normal_download_semaphore", None)
        if semaphore is None:
            return False

        self._elastic_download_sequence += 1
        worker_id = (
            normal_workers
            + int(runtime.get("large_workers") or 0)
            + self._elastic_download_sequence
        )
        max_concurrent = max(1, int(runtime.get("max_concurrent") or 1))
        lifetime_sec = max(60.0, min(float(lifetime_sec or 600.0), 600.0))

        # The base semaphore is sized to the initial normal workers. One permit
        # makes this elastic worker real concurrency instead of a parked task.
        semaphore.release()

        async def _run_elastic_worker() -> None:
            try:
                await asyncio.wait_for(
                    download_worker(
                        self.job_queues.collection_batch_queue,
                        self.job_queues.save_batch_queue,
                        progress_queue=self.job_queues.progress_queue,
                        max_concurrent=max_concurrent,
                        browser=self.browser,
                        browser_getter=self.acquire_browser,
                        browser_releaser=self.release_browser,
                        browser_relauncher=self._relaunch_browser,
                        worker_id=worker_id,
                        worker_lane="normal",
                        large_download_queue=(
                            self.job_queues.large_collection_batch_queue
                            if int(runtime.get("large_workers") or 0) > 0
                            else None
                        ),
                        shared_download_semaphore=semaphore,
                    ),
                    timeout=lifetime_sec,
                )
            except asyncio.TimeoutError:
                logger.info(
                    "[WorkerManager][elastic_download_worker_expired] db=%s worker=%s lifetime_sec=%.0f reason=%s",
                    self.db_name,
                    worker_id,
                    lifetime_sec,
                    reason,
                )
            finally:
                # A completed elastic task is removed before the next scale check.
                self._elastic_download_tasks.discard(asyncio.current_task())

        task = asyncio.create_task(
            _run_elastic_worker(),
            name=f"download-elastic-normal-worker-{worker_id}",
        )
        self._elastic_download_tasks.add(task)
        task.add_done_callback(self._elastic_download_tasks.discard)
        self.download_tasks.append(task)
        self.tasks.append(task)
        self._log_download_task_lifecycle(task, worker_id=worker_id, lane="normal-elastic")
        logger.info(
            "[WorkerManager][elastic_download_worker_created] db=%s worker=%s active_elastic=%s max_elastic=%s lifetime_sec=%.0f reason=%s",
            self.db_name,
            worker_id,
            len(elastic_tasks),
            max_elastic_workers,
            lifetime_sec,
            reason,
        )
        return True

    def _log_download_task_lifecycle(self, task: asyncio.Task, *, worker_id: int, lane: str) -> None:
        logger.info(
            "[WorkerManager][download_worker_created] db=%s worker=%s lane=%s task=%s",
            self.db_name,
            worker_id,
            lane,
            task.get_name(),
        )

        def _done(completed: asyncio.Task) -> None:
            if completed.cancelled():
                logger.warning(
                    "[WorkerManager][download_worker_stopped] db=%s worker=%s lane=%s reason=cancelled",
                    self.db_name,
                    worker_id,
                    lane,
                )
                return
            try:
                exc = completed.exception()
            except Exception as exc:
                logger.error(
                    "[WorkerManager][download_worker_stopped] db=%s worker=%s lane=%s reason=exception_read err=%r",
                    self.db_name,
                    worker_id,
                    lane,
                    exc,
                )
                return
            if exc is None:
                logger.warning(
                    "[WorkerManager][download_worker_stopped] db=%s worker=%s lane=%s reason=returned",
                    self.db_name,
                    worker_id,
                    lane,
                )
            else:
                logger.error(
                    "[WorkerManager][download_worker_stopped] db=%s worker=%s lane=%s reason=exception err=%r",
                    self.db_name,
                    worker_id,
                    lane,
                    exc,
                )

        task.add_done_callback(_done)

    async def _start_study_workers(self, cfg):
        """study worker 기동

        download_worker 출력은 save_batch_queue 단일 큐로 넣는다(GlobalWorkerPool과 동일).
        study_worker가 study_batch_queue를 소비하면 다운로드 결과가 DB/학습으로 가지 않는다.
        """
        for _ in range(cfg["study_workers"]):
            t = asyncio.create_task(study_worker(self.job_queues.save_batch_queue))
            self.study_tasks.append(t)
            self.tasks.append(t)

    async def _launch_browser(self):
        """브라우저 실행 시 크래시 방지 옵션을 강제로 추가하여 실행합니다."""
        if self.playwright is None:
            self.playwright = await async_playwright().start()
        if not self._semaphore_acquired:
            await BROWSER_LAUNCH_SEMAPHORE.acquire()
            self._semaphore_acquired = True
        # 1. 기존 설정된 실행 인자를 가져옵니다.
        launch_args = filter_launch_args(get_default_launch_args())
        
        # 2. [추가] 리눅스 환경 크래시 방지 및 루트 권한 실행을 위한 필수 옵션들
        # --no-sandbox: root 권한 실행 시 필수
        # --disable-dev-shm-usage: 공유 메모리 부족으로 인한 int3 trap 크래시 해결의 핵심
        # --disable-gpu: 서버 환경(GUI 없음)에서의 안정성 확보
        extra_args = [
            "--no-sandbox", 
            "--disable-setuid-sandbox", 
            "--disable-dev-shm-usage", 
            "--disable-gpu"
        ]
        
        # 3. 중복되지 않게 인자를 병합합니다.
        for arg in extra_args:
            if arg not in launch_args:
                launch_args.append(arg)
                
        logger.info(f"[WorkerManager] Browser launching with crash-prevention args: {extra_args}")
        
        # 4. 강화된 인자로 브라우저를 실행합니다.
        return await self.playwright.chromium.launch(
            headless=settings.HEADLESS, 
            args=launch_args
        )

    def get_browser(self) -> Optional[Browser]:
        return self.browser

    def acquire_browser(self) -> Optional[Browser]:
        browser = self.browser
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
        if browser is not self.browser and key in self._retired_browsers:
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

    def _schedule_retired_browser_force_close(self, browser: Optional[Browser]) -> None:
        if browser is None:
            return
        key = id(browser)

        async def _force_close() -> None:
            try:
                await asyncio.sleep(RETIRED_BROWSER_FORCE_CLOSE_SECONDS)
                retired = self._retired_browsers.pop(key, None)
                if retired is None:
                    return
                logger.warning(
                    "[PlaywrightDiag][retired_force_close] scope=local db=%s chat_bot_id=%s browser_id=%s lease_count=%s grace_sec=%.1f",
                    self.db_name,
                    self.chat_bot_id,
                    key,
                    self._browser_use_count.get(key, 0),
                    RETIRED_BROWSER_FORCE_CLOSE_SECONDS,
                )
                self._browser_use_count.pop(key, None)
                await self._close_browser_instance(retired)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("[PlaywrightDiag] retired force-close failed | browser_id=%s err=%s", key, exc)

        task = asyncio.create_task(_force_close(), name=f"retired-browser-force-close-{key}")
        self._retired_browser_force_cleanup_tasks.add(task)
        task.add_done_callback(self._retired_browser_force_cleanup_tasks.discard)

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

    async def _relaunch_browser(self):
        # Prevent concurrent relaunch.
        # 새 브라우저를 먼저 띄우고 old browser는 lease가 모두 해제된 뒤 정리한다.
        if not hasattr(self, "_relaunch_lock") or self._relaunch_lock is None:
            self._relaunch_lock = asyncio.Lock()
        async with self._relaunch_lock:
            old_browser = self.browser
            if len(self._retired_browsers) >= MAX_RETIRED_BROWSERS:
                retired = list(self._retired_browsers.values())
                self._retired_browsers.clear()
                logger.warning(
                    "[PlaywrightDiag][retired_limit_reached] scope=local db=%s retired=%s limit=%s action=force_close_before_relaunch",
                    self.db_name,
                    len(retired),
                    MAX_RETIRED_BROWSERS,
                )
                for stale_browser in retired:
                    self._browser_use_count.pop(id(stale_browser), None)
                    await self._close_browser_instance(stale_browser)
            logger.warning(
                "[PlaywrightDiag][relaunch] scope=local db=%s current_browser_id=%s connected=%s current_leases=%s retired=%s",
                self.db_name,
                id(old_browser) if old_browser else None,
                bool(old_browser and old_browser.is_connected()),
                self._browser_use_count.get(id(old_browser), 0) if old_browser else 0,
                len(self._retired_browsers),
            )
            try:
                new_browser = await self._launch_browser()
            except Exception:
                raise
            self.browser = new_browser
            if old_browser and old_browser is not new_browser:
                self._retired_browsers[id(old_browser)] = old_browser
                self._schedule_retired_browser_cleanup(old_browser)
                if id(old_browser) in self._retired_browsers:
                    self._schedule_retired_browser_force_close(old_browser)
            return new_browser

    async def _periodic_flush(self):
        """실시간 처리를 보장하기 위해 0.1초마다 남은 아이템 강제 전달 (안전장치)"""
        scan_batch_queue = self.job_queues.scan_batch_queue
        collection_batch_queue = self.job_queues.collection_batch_queue
        large_collection_batch_queue = self.job_queues.large_collection_batch_queue
        save_batch_queue = self.job_queues.save_batch_queue
        study_batch_queue = self.job_queues.study_batch_queue
        # flush 주기 단축(기본 50ms): batch buffer로 인해 다음 단계가 지연되는 현상 완화
        try:
            flush_interval = float(os.getenv("BATCH_FLUSH_INTERVAL_SECONDS", "0.05"))
        except Exception:
            flush_interval = 0.05
        flush_interval = max(0.01, flush_interval)
        while True:
            try:
                await asyncio.sleep(flush_interval)
                pending_before_flush = (
                    not scan_batch_queue.empty()
                    or not collection_batch_queue.empty()
                    or not large_collection_batch_queue.empty()
                    or not save_batch_queue.empty()
                    or not study_batch_queue.empty()
                )
                # 무조건 플러시 시도 (empty 체크 제거하여 확실한 처리 보장)
                # 각 단계별로 데이터가 흐를 수 있도록 순차적으로 처리
                # 실시간 모드에서는 buffer가 거의 0이므로 로그 생략
                await scan_batch_queue.flush()
                await asyncio.sleep(0.01)
                await collection_batch_queue.flush()
                await large_collection_batch_queue.flush()
                await asyncio.sleep(0.01)
                await save_batch_queue.flush()
                await asyncio.sleep(0.01)
                await study_batch_queue.flush()
                if not pending_before_flush:
                    now = time.monotonic()
                    if now - self._last_batch_wait_log >= self._batch_wait_log_interval:
                        logger.debug("[WorkerManager] Batch queues idle; waiting for new work.")
                        self._last_batch_wait_log = now
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("[WorkerManager] Flush error: %s", e)
                await asyncio.sleep(1)

    async def stop(
        self,
        graceful: bool = True,
        *,
        stop_scan: bool = True,
        stop_collection: bool = True,
        stop_download: bool = True,
        stop_study: bool = True,
        stop_flush_task: bool = True,
        close_browser: bool = True,
        stop_playwright: bool = True,
        reset_deduplicator: bool = True,
    ):
        """
        모든 워커 중지 및 Playwright 종료
        :param graceful: True면 큐를 비우고 대기, False면 즉시 중단
        """
        started_at = time.monotonic()
        logger.info(
            "[WorkerManager] stop_start | graceful=%s tasks=%s scan=%s collection=%s download=%s study=%s close_browser=%s stop_playwright=%s browser=%s playwright=%s",
            graceful,
            len(getattr(self, "tasks", []) or []),
            len(getattr(self, "scan_tasks", []) or []),
            len(getattr(self, "collection_tasks", []) or []),
            len(getattr(self, "download_tasks", []) or []),
            len(getattr(self, "study_tasks", []) or []),
            close_browser,
            stop_playwright,
            bool(getattr(self, "browser", None)),
            bool(getattr(self, "playwright", None)),
        )
        self._scan_heartbeat_enabled = False
        if graceful:
            # Flush all batch queues (탐색·선별·저장·학습) before stopping — 저장·학습 job 완료까지 대기
            logger.info("[WorkerManager] Flushing batch queues (scan/collection/save/study)...")
            await self.job_queues.scan_batch_queue.flush()
            await self.job_queues.collection_batch_queue.flush()
            await self.job_queues.large_collection_batch_queue.flush()
            await self.job_queues.save_batch_queue.flush()
            await self.job_queues.study_batch_queue.flush()
            logger.info("[WorkerManager] Batch queues flushed.")
            # 저장·학습 워커가 flush된 항목을 처리할 때까지 대기 (학습 완료 후 종료)
            try:
                wait_sec = float(os.getenv("WORKFLOW_GRACEFUL_STOP_WAIT_SEC", "60"))
            except Exception:
                wait_sec = 60.0
            wait_sec = max(2.0, min(wait_sec, 600.0))
            logger.info("[WorkerManager] Waiting up to %.0fs for save/study workers to finish...", wait_sec)
            await asyncio.sleep(wait_sec)
        
        # Stop periodic flush task
        if stop_flush_task and getattr(self, "flush_task", None):
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass

        tasks_to_cancel: List[asyncio.Task] = []
        if stop_scan:
            tasks_to_cancel.extend(self.scan_tasks)
        if stop_collection:
            tasks_to_cancel.extend(self.collection_tasks)
        if stop_download:
            tasks_to_cancel.extend(self.download_tasks)
        if stop_study:
            tasks_to_cancel.extend(self.study_tasks)

        if stop_download:
            logger.info("[WorkerManager] 파일 다운로드 작업중지 요청 (graceful=%s).", graceful)

        # Cancel selected workers
        for task in tasks_to_cancel:
            try:
                task.cancel()
            except Exception:
                pass

        # Wait for selected tasks to finish
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

        # Close Playwright / resources (optional for partial stop)
        if reset_deduplicator and self.collection_deduplicator:
            await self.collection_deduplicator.reset()
        if reset_deduplicator and getattr(self, "file_deduplicator", None):
            await self.file_deduplicator.reset()

        # finally 블록에서 반드시 close() 호출 (열려 있는 context 먼저 종료 후 브라우저 종료)
        try:
            pass
        finally:
            force_cleanup_tasks = list(self._retired_browser_force_cleanup_tasks)
            for task in force_cleanup_tasks:
                task.cancel()
            if force_cleanup_tasks:
                await asyncio.gather(*force_cleanup_tasks, return_exceptions=True)
            cleanup_tasks = list(self._retired_browser_cleanup_tasks)
            if cleanup_tasks:
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            retired = list(self._retired_browsers.values())
            self._retired_browsers.clear()
            self._browser_use_count.clear()
            for retired_browser in retired:
                await self._close_browser_instance(retired_browser)
            if close_browser and self.browser:
                await self._close_browser_instance(self.browser)
                self.browser = None
                if getattr(self, "_semaphore_acquired", False):
                    BROWSER_LAUNCH_SEMAPHORE.release()
                    self._semaphore_acquired = False
            if stop_playwright and self.playwright:
                try:
                    await self.playwright.stop()
                except Exception:
                    pass
                self.playwright = None

        logger.info(
            "[WorkerManager] stop_done | elapsed_ms=%s browser=%s playwright=%s semaphore_acquired=%s",
            int((time.monotonic() - started_at) * 1000),
            bool(getattr(self, "browser", None)),
            bool(getattr(self, "playwright", None)),
            bool(getattr(self, "_semaphore_acquired", False)),
        )
        logger.info("[WorkerManager] All workers stopped.")
