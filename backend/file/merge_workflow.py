import asyncio
import os
import shutil
import logging
import re
import time
import uuid
from pathlib import Path
from datetime import datetime
from enum import Enum
from typing import List, Dict, Any, Callable, Optional, Set
from collections import deque
from urllib.parse import urlparse

from backend.file.integrated_workflow import save_worker_log_json

# 핵심 크롤링 엔진 및 유틸리티 임포트
from core.crawler.queues import JobQueues, create_job_queues, dispose_job_queues
from db.mysql_db_config import mysql_execute_query
from db.mariadb_save_update import insert_into_learn_list, get_account_identifier_from_chatbot_setup, get_learn_list_table_name
from db.db_job_managers import AsyncJobManager, AsyncJobProgress
from db.db_redis import get_redis
from utils.url import canonicalize_url_for_dedup, ensure_url_scheme, extract_download_url_from_js
from config.settings import settings, get_fileupload_root, get_storage_domain_for_db_name

# 로거 설정
logger = logging.getLogger("backend.file.merge_workflow")


def _merge_content_duplicate_check_enabled() -> bool:
    return str(os.getenv("MERGE_WORKFLOW_CONTENT_DUPLICATE_CHECK", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

try:
    from backend.shared.stage_url_report import append_stage_urls  # type: ignore
except Exception:
    def append_stage_urls(*, stage, urls, job_id=None, db_name=None, output_dir=None, extra_meta=None, entry_extra=None):
        try:
            import sys as _sys
            import os as _os
            project_root = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", ".."))
            if project_root not in _sys.path:
                _sys.path.insert(0, project_root)
            from backend.shared.stage_url_report import append_stage_urls as _impl  # type: ignore
            return _impl(stage=stage, urls=urls, job_id=job_id, db_name=db_name, output_dir=output_dir, extra_meta=extra_meta, entry_extra=entry_extra)
        except Exception:
            return None

def _board_project_root() -> str:
    return str(Path(__file__).resolve().parent.parent.parent)

class BoardContentFilePipelineMixin:
    """file_content_workflow 대체용 최소 믹스인(merge_workflow 기준)."""
    async def _ensure_file_pipeline(self) -> None: return None
    async def _enqueue_file_downloads(self, **kwargs) -> int: return 0
    async def _shutdown_file_pipeline(self, *, graceful: bool = False) -> None: return None
    async def _wait_for_save_done(self, from_count: int, needed: int, *, timeout_sec: float = 60.0) -> None: return None
    async def _run_file_progress_loop(self) -> None: return None

class WorkflowState(Enum):
    INIT = "init"
    RUNNING = "running"
    STOPPING = "stopping"
    COMPLETED = "completed"
    ERROR = "error"

class IntegratedWorkflow:
    """
    탐색을 생략하고 수집부터 파일 학습까지 전담하며, 선별 수치를 즉시 반영하는 통합 엔진입니다.
    """
    
    def __init__(self):
        # 큐 및 작업 상태를 관리하기 위한 변수들을 초기화합니다.
        self.is_running = False
        self.job_queues = None
        self.worker_manager = None
        self.saved_urls = set()
        self._save_claimed_urls = set()
        self.in_flight = {'collection': 0, 'download': 0}
        
        # 선별(Scan) 카운트 추적 및 중복 제거를 위한 변수를 초기화합니다.
        self._seen_scan = set() 
        
        # UI에서 요구하는 모든 단계별 카운트 항목을 초기화합니다.
        self.stats = {
            'scan_count': 0, 
            'total_count': 0, 
            'collection_count': 0, 
            'save_count': 0, 
            'study_count': 0,
            'study_success_count': 0,
            'study_failed_count': 0
        }
        self._stats_lock = asyncio.Lock()
        self._file_enqueue_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        
        # app.py 및 workflow_dispatch_assembly 호환성 속성 초기화
        self.crawler_file_buffer = []
        self.progress_callback = None
        self.job_id = None
        self.chat_bot_id = None
        self.db_name = None
        self.server_domain = None
        
        # UI 및 추가 메타데이터 속성
        self.ui_colle = None
        self.ui_subject = None
        self.ui_h3 = None
        self.ui_details = None
        self.cate1 = None
        self.cate2 = None
        self.memo = ""
        self.unique_id = None
        self.board_list_urls = None
        self.access_url = None
        self._trigger_tasks: Set[asyncio.Task] = set()
        self._learn_runtime_lock = asyncio.Lock()
        self._learn_redis = None
        self._learn_job_manager = None
        self._learn_job_progress = None
        self._trigger_sem = asyncio.Semaphore(4)

    # 기능별 1줄 주석설명: 입력된 URL을 선별 통계에 즉시 반영한 후 수집 큐에 투입하여 진행 상황을 노출합니다.
    async def start_workflow(self, start_urls: List[str] = None, progress_callback=None, start_url: str = None, start_date=None, end_date=None, **kwargs):
        # app.py에서 start_url(단수)로 보낼 경우 start_urls(복수)로 변환
        if start_urls is None:
            start_urls = [start_url] if start_url else []
        
        self.is_running = True
        self.progress_callback = progress_callback
        self.job_queues = create_job_queues(self.job_id or "merged-workflow")
        
        # 1. 입력된 URL을 선별(Scan) 수치로 먼저 등록하여 UI에 즉시 반영합니다.
        for u in start_urls:
            url_with_scheme = ensure_url_scheme(u)
            norm_u = canonicalize_url_for_dedup(url_with_scheme)
            if norm_u not in self._seen_scan:
                self._seen_scan.add(norm_u)
        
        # 2. 선별 개수를 업데이트하고 첫 번째 통계를 콜백으로 보고합니다.
        self.stats['scan_count'] = len(self._seen_scan)
        self.stats['total_count'] = self.stats['scan_count']
        if self.progress_callback:
            self.progress_callback(self.get_stats())

        # 파일 수집 및 다운로드 일꾼(워커) 매니저를 기동합니다.
        from core.crawler.manager import WorkerManager
        self.worker_manager = WorkerManager(
            on_collection_batch=None, 
            job_queues=self.job_queues,
            chat_bot_id=self.chat_bot_id,
            db_name=self.db_name
        )
        asyncio.create_task(self.worker_manager.start())

        # 3. 입력된 URL을 수집 단계로 즉시 밀어넣어 작업을 시작합니다.
        items = [{'url': ensure_url_scheme(u), 'colle': 'file', 'job_id': self.job_id} for u in start_urls]
        self.in_flight['collection'] = len(items)
        await self.job_queues.collection_batch_queue.put(items)
        
        # 이벤트 모니터링 루프를 시작합니다.
        await self._main_monitoring_loop()

    # 기능별 1줄 주석설명: 진행 상태 큐를 실시간 감시하며 파일 저장 완료 시 후처리와 통계 갱신을 실행합니다.
    async def _main_monitoring_loop(self):
        progress_queue = self.job_queues.progress_queue
        while self.is_running:
            try:
                item = await asyncio.wait_for(progress_queue.get(), timeout=1.0)
                # 파일 다운로드가 완료되었을 때 실행되는 핵심 핸들러입니다.
                if item.get('type') == 'file_saved':
                    await self._process_post_download(item.get('file_info', {}))
                
                # 통계 수치를 갱신하고 콜백으로 프론트에 전달합니다.
                await self._update_internal_stats(item)
                progress_queue.task_done()
            except asyncio.TimeoutError:
                if self.stop_event.is_set(): break

    # 기능별 1줄 주석설명: 외부(app.py 등)에서 직접 전송한 파일 목록을 수집 엔진의 큐로 안전하게 전달합니다.
    async def process_scan_batch(self, items: List[Dict[str, Any]]):
        """app.py/crawler_file_callback 호출 대응용"""
        if not self.job_queues or not items:
            return
        
        # 수집 큐 형태에 맞춰 데이터 정규화 [ {url:..., colle:file, ...} ]
        collection_items = []
        for it in items:
            u = it.get('url') or it.get('href')
            if u:
                collection_items.append({
                    'url': ensure_url_scheme(u),
                    'colle': 'file',
                    'job_id': self.job_id,
                    'file_info': it # 원본 정보 보존
                })
        
        if collection_items:
            async with self._stats_lock:
                self.in_flight['collection'] += len(collection_items)
            await self.job_queues.collection_batch_queue.put(collection_items)

    # 기능별 1줄 주석설명: 동일한 제목과 용량을 가진 파일이 이미 학습되어 있는지 DB를 조회합니다.
    async def _check_content_duplicate(self, file_name: str, file_path: str) -> bool:
        if not _merge_content_duplicate_check_enabled():
            return False
        try:
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            acc_id = await get_account_identifier_from_chatbot_setup(self.chat_bot_id, self.db_name)
            table = get_learn_list_table_name(acc_id)
            sql = f"SELECT id FROM `{table}` WHERE subject = %s AND size = %s AND status = 'Y' LIMIT 1"
            rows = await mysql_execute_query(sql, (file_name, file_size), fetch=True, dbname=self.db_name)
            return bool(rows)
        except Exception: return False

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
                        "[LearningTrace][merge.immediate_learning.start] job_id=%s db=%s db_id=%s file=%s",
                        item.get("job_id"),
                        item.get("db_name"),
                        item.get("db_id"),
                        os.path.basename(item.get("file_path") or item.get("local_path") or ""),
                    )
                    await study_process_batch_items([item], redis, job_man, job_prog)
                    logger.debug(
                        "[LearningTrace][merge.immediate_learning.done] job_id=%s db=%s db_id=%s file=%s",
                        item.get("job_id"),
                        item.get("db_name"),
                        item.get("db_id"),
                        os.path.basename(item.get("file_path") or item.get("local_path") or ""),
                    )
                except Exception as exc:
                    logger.error(
                        "[LearningError][merge.immediate_learning.error] job_id=%s db=%s db_id=%s err=%s",
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
                            "[LearningTrace][merge.immediate_learning.fallback_queue] job_id=%s db=%s db_id=%s",
                            item.get("job_id"),
                            item.get("db_name"),
                            item.get("db_id"),
                        )

        task = asyncio.create_task(_runner(dict(study_item or {})))
        self._trigger_tasks.add(task)
        task.add_done_callback(lambda t: self._trigger_tasks.discard(t))

    # 기능별 1줄 주석설명: 다운로드된 파일을 전용 경로로 복사하고 텍스트를 추출하여 AI 학습을 트리거합니다.
    async def _process_post_download(self, file_info: Dict[str, Any]):
        url = file_info.get('url')
        file_path = file_info.get('file_path') or file_info.get('local_path')
        file_name = file_info.get('name') or os.path.basename(file_path)

        # 1. 파일 내용 기준 중복 방어 로직을 실행합니다.
        if await self._check_content_duplicate(file_name, file_path):
            logger.info(f"[병합엔진] 중복 파일 학습 스킵: {file_name}")
            return

        # 2. 파일을 챗봇 전용 업로드 경로(fileupload/...)로 물리적 복사를 수행합니다.
        upload_path = await self._copy_file_to_upload_path(file_path)
        if not upload_path: return

        # 3. 문서 파일(PDF, HWP 등)에서 학습에 필요한 텍스트 내용을 추출합니다.
        extracted_text = await self._extract_text_from_file(upload_path)
        
        # 4. 정제된 정보를 바탕으로 MariaDB에 등록하고 AI 학습 큐에 전달합니다.
        file_info.update({'local_path': upload_path, 'content': extracted_text, 'subject': file_name})
        file_info["type"] = "file"
        file_info["content_type"] = "file"
        db_id = await insert_into_learn_list(chat_bot_id=self.chat_bot_id, db_name=self.db_name, file_info=file_info)
        if db_id and self.job_queues:
            self._schedule_immediate_learning(
                {
                    **file_info,
                    "db_id": db_id,
                    "chat_bot_id": self.chat_bot_id,
                    "db_name": self.db_name,
                    "job_id": self.job_id,
                }
            )

    # 기능별 1줄 주석설명: 임시 파일을 시스템의 최종 업로드 루트 폴더로 안전하게 복사합니다.
    async def _copy_file_to_upload_path(self, source_path: str) -> Optional[str]:
        try:
            uuid_part = str(self.chat_bot_id).split("-")[-1]
            storage_domain = get_storage_domain_for_db_name(getattr(self, "db_name", None))
            target_dir = os.path.normpath(os.path.join(get_fileupload_root(), storage_domain, uuid_part))
            os.makedirs(target_dir, exist_ok=True)
            target_path = os.path.join(target_dir, os.path.basename(source_path))
            await asyncio.to_thread(shutil.copy2, source_path, target_path)
            return target_path
        except Exception as e:
            logger.error(f"[병합엔진] 파일 복사 실패: {e}")
            return None

    # 기능별 1줄 주석설명: 파일 확장자에 따라 적절한 추출기를 사용하여 텍스트 데이터를 확보합니다.
    async def _extract_text_from_file(self, path: str) -> str:
        ext = os.path.splitext(path)[1].lower()
        try:
            if ext == ".pdf":
                from edu.pdf_edu import extract_text_with_tables_sync
                return await asyncio.to_thread(lambda: "\n".join(p.get("text", "") for p in extract_text_with_tables_sync(path)))
            elif ext in (".hwp", ".hwpx"):
                from edu.hwp_edu import hwp_to_text
                return await asyncio.to_thread(hwp_to_text, path)
            elif ext == ".txt":
                with open(path, 'r', encoding='utf-8', errors='replace') as f: return f.read()
        except Exception as e:
            logger.warning(f"[병합엔진] 텍스트 추출 에러: {e}")
        return ""

    # 기능별 1줄 주석설명: 워커로부터 받은 이벤트를 바탕으로 선별, 수집, 저장, 학습 통계를 실시간 업데이트합니다.
    async def _update_internal_stats(self, item: Dict[str, Any]):
        u_type = item.get('type')
        async with self._stats_lock:
            if u_type == 'in_flight':
                stage, delta = item.get('stage'), item.get('delta', 0)
                if stage in self.in_flight:
                    self.in_flight[stage] = max(0, self.in_flight[stage] + delta)
            elif u_type == 'collection':
                self.stats['collection_count'] += len(item.get('items', []))
            elif u_type == 'file_saved':
                self.stats['save_count'] += 1
            elif u_type == 'study_done':
                # 학습 시도와 성공/실패 여부를 세분화하여 기록합니다.
                self.stats['study_count'] += 1
                if item.get('success'):
                    self.stats['study_success_count'] += 1
                else:
                    self.stats['study_failed_count'] += 1
        
        if self.progress_callback:
            self.progress_callback(self.get_stats())

    # 기능별 1줄 주석설명: 현재까지의 누적 통계 데이터를 딕셔너리 형태로 반환합니다.
    def get_stats(self) -> Dict[str, Any]:
        return {**self.stats, "in_flight": self.in_flight, "is_running": self.is_running}

    # 기능별 1줄 주석설명: 실행 중인 워크플로우를 중단하고 모든 워커를 안전하게 종료합니다.
    async def stop(self):
        self.is_running = False
        self.stop_event.set()
        if self.worker_manager:
            await self.worker_manager.stop(graceful=True)
