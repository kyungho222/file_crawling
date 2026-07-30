import sys
import os

# 프로젝트 루트를 sys.path에 추가 (core/crawler/workers -> ../../../)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import asyncio
import json
import time
import multiprocessing
import logging
from typing import List, Dict, Optional, Any
import redis.asyncio as aioredis
from core.crawler.batch_queue import BatchQueue

# Schemas
from schemas.request import EduRequest, VideoSegment

# DB & Managers
from db.db_job_managers import AsyncJobManager, AsyncJobProgress
from db.db_redis import get_redis
from db.db_operations import delete_data

# Utils
from utils.whoami import get_chat_id_from_db, check_duplicate_contents
from utils.url import canonicalize_url_for_dedup
from utils.db_name import resolve_db_name
# learn_del is missing, implemented locally
from config.settings import Config
from db.mariadb_save_update import get_account_identifier_from_chatbot_setup, get_learn_list_table_name

# Services
try:
    from services.milvus_service import (
        MilvusSyncContext,
        activate_milvus_sync_context,
        reset_milvus_sync_context,
        get_milvus_context,
        sync_deleted_contents,
    )
except ImportError:
    MilvusSyncContext = None
    activate_milvus_sync_context = None
    reset_milvus_sync_context = None
get_milvus_context = None
sync_deleted_contents = None

# Try multiple import paths to locate the Celery app across different runtimes.
# Preferred canonical import used by CLI: backend.src.tasks.celery_app
try:
    from backend.src.tasks.celery_app import celery_app  # type: ignore
except Exception:
    try:
        from src.tasks.celery_app import celery_app  # when backend/src is on PYTHONPATH
    except Exception:
        try:
            from celery_app import celery_app  # legacy / top-level module
        except Exception:
            celery_app = None

# Learn Modules (PTContext 방식)
from edu.learn_modules import PTContext, process_and_store

# Safe import: avoid ModuleNotFoundError in different runtime environments.
try:
    from backend.shared.stage_url_report import append_stage_urls  # type: ignore
except Exception:
    # Fallback: no-op implementation to avoid breaking runtime if module isn't available.
    def append_stage_urls(*, stage, urls, job_id=None, db_name=None, output_dir=None, extra_meta=None, entry_extra=None):
        return None

logger = logging.getLogger(__name__)

_completed_jobs = set()
_completed_jobs_timestamps = {}
MILVUS_ACCOUNT_NAME = Config.MILVUS_ACCOUNT_NAME if hasattr(Config, 'MILVUS_ACCOUNT_NAME') else "chatty"


def _clip_text(value: Any, limit: int = 160) -> str:
    try:
        text = " ".join(str(value or "").split())
    except Exception:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _result_chunk_count(result: Any) -> int:
    if not isinstance(result, dict):
        return 0
    chunk_list = result.get("chunk_count")
    if isinstance(chunk_list, list):
        total = 0
        for item in chunk_list:
            try:
                total += int(item or 0)
            except Exception:
                continue
        if total > 0:
            return total
    try:
        return int(result.get("chunks", 0) or 0)
    except Exception:
        return 0


def _batch_queue_depth(batch_queue: Any) -> Optional[int]:
    try:
        return int(batch_queue.queue.qsize())
    except Exception:
        return None


def _resolve_learning_title(item: Optional[Dict[str, Any]], file_path: str = "") -> str:
    if not isinstance(item, dict):
        return os.path.basename(file_path or "")
    original_meta = item.get("original_meta")
    candidates = [
        item.get("display_name"),
        item.get("attachment_name"),
        original_meta.get("attachment_name") if isinstance(original_meta, dict) else None,
        item.get("title"),
        original_meta.get("title") if isinstance(original_meta, dict) else None,
        item.get("subject"),
        item.get("name"),
        original_meta.get("name") if isinstance(original_meta, dict) else None,
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text
    return os.path.basename(file_path or "")

async def cleanup_disconnected_job(job_id: str):
    logger.warning(f"[StudyWorker] cleanup_disconnected_job not implemented for {job_id}")

async def get_chatbot_config(chat_bot_id: str, db_name: str) -> dict:
    # Dummy implementation since utils.chatbot_config is missing
    return {}

async def learn_del(request: EduRequest):
    """중복 데이터 삭제"""
    learn_started = time.perf_counter()
    job_id = getattr(request, "job_id", None)
    try:
        chat_id = await get_chat_id_from_db(request.db_name, request.chat_bot_id)
        if not chat_id:
            return
        table_name = f"td_{chat_id}_training_data"
        for content in request.contents:
            await delete_data(table_name, {"content": content}, dbname=request.db_name)
            # ✅ (이식) Milvus 삭제 동기화는 옵션 (컨텍스트 활성 시에만)
            try:
                if get_milvus_context and sync_deleted_contents:
                    ctx = get_milvus_context()
                    await sync_deleted_contents([content], ctx)
            except Exception:
                pass
        logger.error(f"Deleted {len(request.contents)} items from {table_name}")
    except Exception as e:
        logger.error(
            "[LearningError][learn.error] job_id=%s db=%s elapsed_ms=%s err=%s",
            job_id,
            getattr(request, "db_name", None),
            int((time.perf_counter() - learn_started) * 1000),
            e,
            exc_info=True,
        )
        logger.error(f"Error in learn_del: {e}")

async def learn(
    request: EduRequest,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
    redis_client: aioredis.Redis,
):
    learn_started = time.perf_counter()
    job_id = getattr(request, "job_id", None)
    try:
        logger.info(f"\n###########################################: {request.memo} ###########################################\n")
        if not job_id:
            raise ValueError("job_id is required.")
        logger.debug(
            "[LearningTrace][learn.start] job_id=%s db=%s chat_bot_id=%s type=%s subject=%s file_path=%s content=%s",
            job_id,
            getattr(request, "db_name", None),
            getattr(request, "chat_bot_id", None),
            getattr(request, "content_type", None),
            _clip_text((getattr(request, "subjects", None) or [None])[0]),
            _clip_text((getattr(request, "file_paths", None) or [None])[0]),
            _clip_text((getattr(request, "contents", None) or [None])[0]),
        )
        
        await job_manager.start_job(job_id)
        logger.info(
            f"job_manager.start_job: {job_id}, chat_bot_id={request.chat_bot_id}"
        )

        if request.content_type not in ["text", "url", "image", "video", "sound"]:
            raise ValueError("Only 'text', 'url', 'image', 'video', or 'sound' is allowed for content_type.")

        chat_id = await get_chat_id_from_db(request.db_name, request.chat_bot_id)
        if not chat_id:
            raise ValueError("The chat_bot_id is invalid.")

        logger.info(f"chat_id={chat_id} 조회 성공")

        table_name = f"td_{chat_id}_training_data"

        # 조합 테이블명 로깅 (MariaDB + PG) - 학습 시작 시점에 확정된 테이블을 한 줄로 남긴다.
        try:
            account_identifier = await get_account_identifier_from_chatbot_setup(request.chat_bot_id, request.db_name)
            learn_list_table = get_learn_list_table_name(account_identifier)
            training_process_table = (
                learn_list_table.replace("_LEARN_LIST", "_TRAINING_PROCESS")
                if learn_list_table and learn_list_table.endswith("_LEARN_LIST")
                else None
            )
            logger.info(
                "[Study] Tables | db=%s chat_bot_id=%s pg=%s maria.learn_list=%s maria.training_process=%s",
                request.db_name,
                request.chat_bot_id,
                table_name,
                learn_list_table,
                training_process_table,
            )
        except Exception:
            pass

        if not request.contents:
            raise ValueError("No training data was provided.")

        logger.info("학습 데이터 확인 완료")

        if (request.content_type in ["text", "image", "video", "sound"]) and not request.subjects:
            raise ValueError(f"'subject' is required for {request.content_type} learning.")

        # 중복 체크 기준 설정: 항상 contents(URL 등) 기준으로 수행 (사용자 요청 반영)
        check_contents = request.contents
            
        is_duplicate, duplicate_contents = await check_duplicate_contents(
            check_contents, table_name, request.db_name
        )
        
        if is_duplicate:
            logger.info(f"{len(duplicate_contents)}개의 데이터 삭제 후 학습진행")
            delete_request = EduRequest(
                chat_bot_id=request.chat_bot_id,
                db_name=request.db_name,
                contents=duplicate_contents,
                job_id=f"{job_id}_del" # dummy job id
            )
            await learn_del(delete_request)

        job_data = {
            "chat_bot_id": request.chat_bot_id,
            "dbname": request.db_name,
            "pending_contents": check_contents,
        }
        await redis_client.set(f"job_data:{job_id}", json.dumps(job_data))
    except Exception as e:
        logger.error(f"learn 실행 중 오류 발생: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "message": f"learn execution failed: {str(e)}",
        }

    crawl_mode = request.crawl_mode
    chatbot_config = await get_chatbot_config(request.chat_bot_id, request.db_name)
    logger.info(f"learn_file 함수 chatbot_config 설정 불러오기: {chatbot_config}")

    # 학습 완료까지 기다리고 결과 반환
    result = await process_learn(
        request,
        job_manager,
        job_progress_manager,
        job_id,
        redis_client,
        table_name,
        chat_id,
        crawl_mode,
        request.sitemap,
        request.chat_bot_id,
        chatbot_config,
    )
    logger.info(f"\n = = = = = = = = = = 최종 반환 되는 crawl_pages: {result.get('crawl_pages')}\n = = = = = = = = = =")

    logger.debug(
        "[LearningTrace][learn.done] job_id=%s db=%s elapsed_ms=%s status=%s chunks=%s crawl_pages=%s first_source=%s",
        job_id,
        getattr(request, "db_name", None),
        int((time.perf_counter() - learn_started) * 1000),
        result.get("status") if isinstance(result, dict) else None,
        _result_chunk_count(result),
        result.get("crawl_pages") if isinstance(result, dict) else None,
        _clip_text((result.get("use_source", []) or [None])[0] if isinstance(result, dict) else None),
    )
    if result and "chunk_count" in result:
        return {
            "status": "OK", 
            "message": "Start learning", 
            "chunk_count": result["chunk_count"],
            "use_source": result.get("use_source", []),
            "source_size": result.get("source_size", []),
            "web_title": result.get("web_title", []),
            "crawl_pages" : result.get("crawl_pages")
        }
    else:
        return {
            "status": "OK", 
            "message": "Start learning", 
            "chunk_count": [],
            "use_source": [],
            "source_size": [],
            "web_title": [],
            "crawl_pages" : 0
        }


async def process_learn(
    request,
    job_manager,
    job_progress_manager,
    job_id,
    redis_client,
    table_name,
    chat_id,
    crawl_mode,
    sitemap: str = "N",
    chat_bot_id: str = None,
    chatbot_config: dict = None,
):
    milvus_token = None
    # ✅ 정책: Milvus 연동은 기본 OFF. (MILVUS_ENABLED=true일 때만 services.milvus_service에서 활성화됨)
    if chat_bot_id and request.db_name == "chatty" and MilvusSyncContext:
        milvus_ctx = MilvusSyncContext(
            enabled=True,
            dbname=request.db_name,
            chat_bot_id=chat_bot_id,
            account_name=MILVUS_ACCOUNT_NAME,
        )
        milvus_token = activate_milvus_sync_context(milvus_ctx)
    try:
        total_tasks = len(request.contents)
        each_progress = round(100 / total_tasks, 2)
        logger.info(f"job_id:{job_id} 학습 시작, total_tasks:{total_tasks}")
        
        await job_progress_manager.set_job_progress(job_id, 0)
        
        # URL 배치 최적화 로직 - 생략하거나 backend/edu/url_edu.py 구조 확인 후 적용
        # 현재 url_edu 모듈 import가 없으므로 일단 스킵하거나 개별 처리로 유도
        
        max_concurrent = min(multiprocessing.cpu_count(), 8)
        semaphore = asyncio.Semaphore(max_concurrent)
        
        total_chunks = 0
        chunk_count_list = []
        use_source_list = []
        source_size_list = []
        web_title_list = []
        
        tasks = []
        # Normalize memo input: accept None, a single string, or a list.
        memo_list = getattr(request, "memo", None)
        if isinstance(memo_list, str):
            memo_list = [memo_list]
        elif memo_list is None:
            memo_list = []
        elif not isinstance(memo_list, list):
            try:
                memo_list = list(memo_list)
            except Exception:
                memo_list = [str(memo_list)]

        for i, content in enumerate(request.contents):
            subject = ""
            if request.subjects and i < len(request.subjects):
                subject = request.subjects[i]

            # 파일 학습의 경우: contents는 "식별자(URL)"이고 실제 로컬 파일 경로는 file_paths로 전달될 수 있다.
            file_path_override = None
            try:
                if getattr(request, "file_paths", None) and i < len(request.file_paths or []):
                    file_path_override = request.file_paths[i] or None
            except Exception:
                file_path_override = None

            current_memo = ""
            if request.memo and i < len(request.memo):
                current_memo = request.memo[i] if request.memo[i] else ""
            
            # ... (영상/음성 파일 이름 처리 로직 생략 또는 간소화) ...
            
            current_image_file_name = None
            current_video_file_name = None
            current_sound_file_name = None
            current_video_segments = None
            current_content_created_at = None
            current_content_updated_at = None

            task = asyncio.create_task(
                process_single_content(
                    semaphore=semaphore,
                    content=content,
                    subject=subject,
                    file_path_override=file_path_override,
                    request=request,
                    table_name=table_name,
                    job_id=job_id,
                    each_progress=each_progress,
                    job_manager=job_manager,
                    job_progress_manager=job_progress_manager,
                    memo=current_memo,
                    crawl_mode=crawl_mode,
                    sitemap=sitemap,
                    task_index=i,
                    total_tasks=total_tasks,
                    chat_bot_id=chat_bot_id,
                    chatbot_config=chatbot_config,
                    video_file_name=current_video_file_name,
                    image_file_name=current_image_file_name,
                    sound_file_name=current_sound_file_name,
                    content_created_at=current_content_created_at,
                    content_updated_at=current_content_updated_at,
                    video_segments=current_video_segments,
                )
            )
            tasks.append(task)
        
        completed_count = 0
        full_filtered_texts = []
        task_results = {}
        video_segment_debug_info = []
        
        for coro in asyncio.as_completed(tasks):
            status = await job_manager.get_job_status(job_id)
            if status == "cancel":
                logger.info(f"작업이 취소되었습니다: job_id={job_id}")
                await cleanup_disconnected_job(job_id)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                return {"status": "cancelled", "message": "job is cancelled.", "chunk_count": [], "use_source": [], "source_size": [], "web_title": []}
            
            result = await coro
            completed_count += 1
            
            if result and isinstance(result, dict):
                task_index = result.get("task_index", completed_count - 1)
                task_results[task_index] = result
                # 결과 집계 로직 수행
            
            progress = min(int((completed_count / total_tasks) * 95), 95)
            await job_progress_manager.set_job_progress(job_id, progress)

        await job_manager.complete_job(job_id)
        
        # 결과 정렬 및 집계 (생략된 로직 복원 필요)
        # 중요: for loop로 task_results 순회하며 chunk_count_list 등 채우기
        for i in range(total_tasks):
            if i in task_results:
                result = task_results[i]
                chunk_count = result.get("chunk_count", [])
                if isinstance(chunk_count, list) and len(chunk_count) > 0:
                    chunk_count_list.extend(chunk_count)
                else:
                    chunk_count_list.append(result.get("chunks", 0))
                
                # use_source 등 채우기
                use_source_list.append(request.subjects[i] if request.subjects else result.get("content"))
                
                # full_filtered_text 수집
                full_filtered_texts.append({
                    "content": result.get("content"),
                    "full_filtered_text": result.get("full_filtered_text", "")
                })

        completion_message = {
            "status": "completed",
            "progress": 100,
            "chunk_count": chunk_count_list,
            "use_source": use_source_list,
            "source_size": source_size_list,
            "web_title": web_title_list,
            "crawl_pages": len(request.contents),
            "full_filtered_text": full_filtered_texts
        }
        
        # FAISS 인덱스 갱신: backend/src/tasks task_sender 경유 발행 (연결 통일)
        try:
            from backend.src.tasks.task_sender import send_faiss_refresh
            task_id = send_faiss_refresh(
                request.db_name,
                chat_id,
                queue="faiss_index_chatty_9000",
            )
            if not task_id and celery_app:
                celery_app.send_task(
                    "chat.faiss_process.refresh_index_task",
                    args=[request.db_name, chat_id],
                    queue="faiss_index_chatty_9000",
                )
        except Exception as e:
            if celery_app:
                try:
                    celery_app.send_task(
                        "chat.faiss_process.refresh_index_task",
                        args=[request.db_name, chat_id],
                        queue="faiss_index_chatty_9000",
                    )
                except Exception as send_e:
                    logger.error("FAISS refresh failed: %s", send_e)
            else:
                logger.debug("FAISS refresh skipped (no Celery): %s", e)

        _completed_jobs.add(job_id)
        _completed_jobs_timestamps[job_id] = time.time()
        
        return completion_message

    except Exception as e:
        await job_manager.cancel_job(job_id)
        logger.error(f"학습 처리 중 오류 발생: {str(e)}")
        return {"status": "error", "message": str(e), "chunk_count": [], "use_source": [], "source_size": [], "web_title": []}
    finally:
        if milvus_token:
            reset_milvus_sync_context(milvus_token)
        await redis_client.delete(f"job_data:{job_id}")


async def process_single_content(
    semaphore: asyncio.Semaphore,
    content: str,
    subject: str,
    request,
    table_name: str,
    job_id: str,
    each_progress: float,
    job_manager,
    job_progress_manager,
    memo: str,
    crawl_mode: str,
    sitemap: str,
    task_index: int,
    total_tasks: int,
    chat_bot_id: str = None,
    chatbot_config: dict = None,
    video_file_name: str = None,
    image_file_name: str = None,
    sound_file_name: str = None,
    content_created_at: Optional[str] = None,
    content_updated_at: Optional[str] = None,
    video_segments: Optional[List[Dict[str, str]]] = None,
    file_path_override: Optional[str] = None,
):
    """개별 컨텐츠를 처리하고 결과를 반환합니다."""
    async with semaphore: 
        start_time = time.time()
        try:
            logger.info(f"작업 시작: job_id={job_id}, task={task_index+1}/{total_tasks}, content={content[:50]}...")
            
            # process_and_store 호출 (PTContext 사용)
            inferred_file_path = (
                file_path_override
                if file_path_override
                else (
                    subject
                    if request.content_type == "url"
                    else content
                )
            )
            pt_ctx = PTContext(
                content=content,
                file_path=inferred_file_path,
                content_type=request.content_type,
                table_name=table_name,
                dbname=request.db_name,
                job_id=job_id,
                job_manager=job_manager,
                job_progress_manager=job_progress_manager,
                subject=subject,
                each_progress=each_progress,
                memo=memo,
                crawl_mode=crawl_mode,
                sitemap=sitemap,
                chat_bot_id=chat_bot_id,
                url_filter=(request.url_filter if request.url_filter is not None else "B"),
                chatbot_config=chatbot_config,
                video_file_name=video_file_name,
                image_file_name=image_file_name,
                sound_file_name=sound_file_name,
                content_created_at=content_created_at,
                content_updated_at=content_updated_at,
                video_segments=video_segments,
            )
            result = await process_and_store(pt_ctx)
            
            # 결과 가공 및 반환 (기존 코드 단순화해서 사용)
            processing_time = round(time.time() - start_time, 2)

            if not isinstance(result, dict):
                raise RuntimeError(f"process_and_store returned invalid type: {type(result)}")

            # ✅ status는 process_and_store 결과를 그대로 존중한다.
            # - 특히 chunks=0인데 success로 처리되면 이후 MariaDB status=Y 업데이트 판단이 꼬인다.
            status = (result.get("status") or "success")
            chunks = 0
            try:
                chunks = int(result.get("chunks", 0) or 0)
            except Exception:
                chunks = 0

            if status in ("cancel", "cancelled"):
                return {
                    "status": "cancelled",
                    "content": content,
                    "processing_time": processing_time,
                    "chunks": 0,
                    "chunk_count": [],
                    "use_source": [],
                    "source_size": [],
                    "web_title": "",
                    "task_index": task_index,
                    "total_crawled_urls": result.get("total_crawled_urls", 0),
                    "message": result.get("message", "작업이 취소되었습니다."),
                }

            # ✅ 청크가 0이면 학습 완료로 취급하지 않는다.
            # (PG에 청크가 없으니 MariaDB status=Y 업데이트도 되면 안 됨)
            if status in ("success", "ok", "completed") and chunks <= 0:
                status = "error"
                result_msg = result.get("message") or "no_chunks_extracted"
            else:
                result_msg = result.get("message") or ""

            ret_data = {
                "status": status,
                "content": content,
                "processing_time": processing_time,
                "chunks": chunks,
                "chunk_count": result.get("chunk_count", []),
                "use_source": result.get("use_source", []),
                "source_size": result.get("source_size", []),
                "web_title": result.get("web_title", ""),
                "task_index": task_index,
                "total_crawled_urls": result.get("total_crawled_urls", 0),
                "message": result_msg,
            }
            if "full_filtered_text" in result:
                ret_data["full_filtered_text"] = result["full_filtered_text"]
                
            return ret_data
            
        except Exception as e:
            processing_time = round(time.time() - start_time, 2)
            logger.error(f"개별 작업 실패: job_id={job_id}, task={task_index+1}, error={e}")
            return {
                "status": "error",
                "content": content,
                "error": str(e),
                "processing_time": processing_time,
                "task_index": task_index
            }
async def study_process_batch_items(
    batch: List[Dict[str, Any]],
    redis_client,
    job_manager: AsyncJobManager,
    job_progress_manager: AsyncJobProgress,
) -> None:
    """Process one study batch (shared with universal download worker)."""
    if not batch:
        return
    try:
        batch_conc = int(os.getenv("STUDY_BATCH_CONCURRENCY", "3") or "3")
    except Exception:
        batch_conc = 3
    batch_conc = max(1, min(batch_conc, 10))
    sem = asyncio.Semaphore(batch_conc)

    async def _process_item(item: Dict[str, Any]) -> None:
        async with sem:
            item_started = time.perf_counter()
            try:
                file_path = item.get('file_path') or item.get('local_path')
                if not file_path:
                    return

                # IntegratedWorkflow는 다운로드 완료 후 learn()을 직접 호출한다.
                # 해당 경로에서는 중복 학습을 방지하기 위해 skip 플래그를 사용한다.
                if item.get("skip_study_worker"):
                    logger.info(
                        f"[StudyWorker] ⏭️ Skipping learning for {os.path.basename(file_path)} (skip_study_worker=True)"
                    )
                    return

                chat_bot_id = item.get("chat_bot_id")
                db_name = resolve_db_name(item)
                name = _resolve_learning_title(item, file_path)
                item_job_id = item.get("job_id") or item.get("jobId")
                if not (chat_bot_id and db_name):
                    logger.warning(
                        "[StudyWorker] missing chat_bot_id/db_name; cannot learn | file=%s",
                        os.path.basename(file_path),
                    )
                    return
                logger.debug(
                    "[LearningTrace][study_item.start] job_id=%s db=%s chat_bot_id=%s file=%s url=%s type=%s batch_size=%s",
                    item_job_id,
                    db_name,
                    chat_bot_id,
                    os.path.basename(file_path),
                    _clip_text(item.get("url")),
                    item.get("content_type") or item.get("type") or "text",
                    len(batch),
                )

                # learn()이 허용하는 타입으로 매핑
                # IntegratedWorkflow는 file_info에 type 키를 주로 사용한다.
                c_type = (item.get("content_type") or item.get("type") or "text")
                allowed_types = ["text", "url", "image", "video", "sound"]
                target_type = "text"
                try:
                    if c_type in allowed_types:
                        target_type = c_type
                    elif isinstance(c_type, str) and c_type.startswith("image/"):
                        target_type = "image"
                    elif isinstance(c_type, str) and c_type.startswith("video/"):
                        target_type = "video"
                    elif isinstance(c_type, str) and c_type.startswith("audio/"):
                        target_type = "sound"
                    else:
                        # 문서 파일 등은 text로 보내고, process_and_store가 확장자로 재매핑 가능
                        target_type = "text"
                except Exception:
                    target_type = "text"

                # 개별 서브 job_id 생성
                sub_job_id = f"study_{int(time.time())}_{os.getpid()}_{os.path.basename(file_path)[:20]}"

                # 1) MariaDB LEARN_LIST 등록 (학습 트리거용 ID 확보)
                learn_list_id = None
                learn_list_duplicate = None
                logger.debug(
                    "[LearningTrace][study_item.before_learn_list] job_id=%s db=%s file=%s url=%s provided_db_id=%s",
                    item_job_id,
                    db_name,
                    os.path.basename(file_path),
                    _clip_text(item.get("url")),
                    item.get("db_id") or item.get("learn_list_id") or item.get("learn_list"),
                )
                try:
                    from db.mariadb_save_update import (
                        coalesce_learn_list_cates,
                        insert_into_learn_list,
                    )
                    # 상위 워크플로우(예: IntegratedWorkflow)가 이미 LEARN_LIST에 저장하고 db_id를 전달한 경우 재사용
                    provided_db_id = item.get("db_id") or item.get("learn_list_id") or item.get("learn_list")
                    if provided_db_id:
                        learn_list_id = provided_db_id
                        learn_list_duplicate = item.get("learn_list_duplicate")
                        logger.info(
                            "[StudyWorker] Using provided LEARN_LIST id | id=%s file=%s url=%s",
                            learn_list_id,
                            os.path.basename(file_path),
                            item.get("url"),
                        )
                    else:
                        file_info = {
                            "url": item.get("url"),
                            "name": name,
                            "type": item.get("content_type") or target_type,
                            "size": item.get("size"),
                            "file_path": file_path,
                            "local_path": item.get("local_path") or file_path,
                            "author": item.get("author"),
                            "department": item.get("department"),
                            "author_kind": item.get("author_kind"),
                            "author_raw": item.get("author_raw"),
                            "department_raw": item.get("department_raw"),
                            "source_page": item.get("source_page"),
                            "reg_date": item.get("reg_date"),
                            "original_meta": item.get("original_meta"),
                            "job_id": item.get("job_id"),
                        }
                        try:
                            _sc1, _sc2 = coalesce_learn_list_cates(item)
                            file_info["cate1"] = _sc1
                            file_info["cate2"] = _sc2
                        except Exception:
                            pass
                        if file_info.get("url"):
                            learn_list_id = await insert_into_learn_list(
                                chat_bot_id=chat_bot_id,
                                db_name=db_name,
                                file_info=file_info,
                            )
                            learn_list_duplicate = file_info.get("learn_list_duplicate")
                            if learn_list_id:
                                logger.info(
                                    "[StudyWorker] LEARN_LIST registered | id=%s file=%s url=%s disk=%s",
                                    learn_list_id,
                                    os.path.basename(file_path),
                                    item.get("url"),
                                    os.path.isfile(file_path),
                                )
                except Exception as db_exc:
                    logger.warning(
                        "[StudyWorker] LEARN_LIST insert failed | file=%s err=%s",
                        os.path.basename(file_path),
                        db_exc,
                    )
                logger.debug(
                    "[LearningTrace][study_item.after_learn_list] job_id=%s db=%s file=%s learn_list_id=%s duplicate=%s elapsed_ms=%s",
                    item_job_id,
                    db_name,
                    os.path.basename(file_path),
                    learn_list_id,
                    learn_list_duplicate,
                    int((time.perf_counter() - item_started) * 1000),
                )

                memo_val = item.get("memo")
                if isinstance(memo_val, list):
                    memo_val = memo_val[0] if memo_val else ""
                memo_text = str(memo_val).strip() if memo_val is not None else ""
                if not memo_text:
                    memo_text = f"Auto-learned from crawl queue: {item.get('url', '')}"
                # no-op placeholders removed; memo_text already prepared

                req = EduRequest(
                    job_id=sub_job_id,
                    chat_bot_id=chat_bot_id,
                    db_name=db_name,
                    content_type=target_type,
                    # contents는 "PG 저장/중복 처리 식별자"로 사용한다.
                    # 파일 학습은 식별자를 URL(정규화)로 통일하고, 실제 로컬 경로는 file_paths로 별도 전달한다.
                    contents=[canonicalize_url_for_dedup(item.get("url") or "") or (item.get("url") or file_path)],
                    file_paths=[file_path],
                    subjects=[name],
                    crawl_mode="Y",
                    memo=[memo_text],
                )

                # ✅ 중복이면 삭제 후 재학습 (learn() 내부 + process_and_store 내부 정책으로 보장)
                learn_call_started = time.perf_counter()
                logger.debug(
                    "[LearningTrace][study_item.before_learn] item_job_id=%s sub_job_id=%s db=%s learn_list_id=%s file=%s url=%s",
                    item_job_id,
                    sub_job_id,
                    db_name,
                    learn_list_id,
                    os.path.basename(file_path),
                    _clip_text(item.get("url")),
                )
                result = await learn(
                    req,
                    job_manager=job_manager,
                    job_progress_manager=job_progress_manager,
                    redis_client=redis_client,
                )
                learn_elapsed_ms = int((time.perf_counter() - learn_call_started) * 1000)
                chunks_val = _result_chunk_count(result)
                logger.debug(
                    "[LearningTrace][study_item.after_learn] item_job_id=%s sub_job_id=%s db=%s learn_list_id=%s elapsed_ms=%s status=%s chunks=%s",
                    item_job_id,
                    sub_job_id,
                    db_name,
                    learn_list_id,
                    learn_elapsed_ms,
                    (result or {}).get("status") if isinstance(result, dict) else None,
                    chunks_val,
                )

                # 2) MariaDB 학습 상태/이력 반영 (청크가 실제로 저장된 경우에만)
                try:
                    if learn_list_id and isinstance(result, dict):
                        if chunks_val > 0:
                            # ✅ 게시판/파일 공용 학습 완료 반영(상태 업데이트 + TRAINING_PROCESS 기록)
                            from backend.shared.learning_finalize import finalize_learning_to_mariadb

                            # PG 저장 식별자는 URL(정규화)로 통일한다.
                            pg_content_value = canonicalize_url_for_dedup(item.get("url") or "") or (item.get("url") or "").strip()
                            if not pg_content_value:
                                pg_content_value = item.get("url") or file_path

                            finalize_started = time.perf_counter()
                            logger.debug(
                                "[LearningTrace][study_item.before_finalize] item_job_id=%s sub_job_id=%s db=%s learn_list_id=%s chunks=%s pg_content=%s",
                                item_job_id,
                                sub_job_id,
                                db_name,
                                learn_list_id,
                                chunks_val,
                                _clip_text(pg_content_value),
                            )
                            trigger_ok = await finalize_learning_to_mariadb(
                                chat_bot_id=chat_bot_id,
                                db_name=db_name,
                                learn_list_id=str(learn_list_id),
                                display_name=(name or os.path.basename(file_path)),
                                actual_chunks=chunks_val,
                                pg_content_value=pg_content_value,
                                learning_service=None,
                                pg_wait_timeout_seconds=None,
                                job_id_for_count=item.get("job_id") or item.get("jobId"),
                                crawling_log_id=item.get("craw_id") if isinstance(item, dict) else None,
                            )
                            logger.debug(
                                "[LearningTrace][study_item.after_finalize] item_job_id=%s sub_job_id=%s db=%s learn_list_id=%s elapsed_ms=%s ok=%s",
                                item_job_id,
                                sub_job_id,
                                db_name,
                                learn_list_id,
                                int((time.perf_counter() - finalize_started) * 1000),
                                trigger_ok,
                            )
                            # ✅ status=Y 업데이트 완료 시점에 study 카운트 증가 (DB 반영)
                            if trigger_ok:
                                try:
                                    job_id_for_log = item.get("job_id") or item.get("jobId")
                                except Exception:
                                    job_id_for_log = None
                                if job_id_for_log:
                                    # ✅ 단계별 결과 URL 저장 (study 성공)
                                    try:
                                        url_to_log = (item.get("url") or "").strip()
                                        if not url_to_log:
                                            # URL이 없으면 count_key/db_id로라도 추적 가능하게 남김
                                            url_to_log = (item.get("_count_key") or "").strip() or f"db:{learn_list_id}"
                                        append_stage_urls(
                                            stage="study",
                                            urls=[{"url": url_to_log, "db_id": str(learn_list_id) if learn_list_id else None}],
                                            job_id=job_id_for_log,
                                            db_name=db_name,
                                        )
                                    except Exception:
                                        pass
                                    try:
                                        await asyncio.sleep(0)
                                    except Exception:
                                        pass
                                    # 학습 카운트 증가 시 Redis SSE로 전체 카운트도 함께 발행하여
                                    # scan/collection/save 값이 UI에 반영되도록 보장
                                    try:
                                        # 가능한 경우 workflow의 get_stats()를 사용해 현재 통계 취득
                                        from backend.shared.crawler_state import crawler_state
                                        try:
                                            workflow = crawler_state.workflows.get(job_id_for_log)
                                        except Exception:
                                            workflow = None

                                        payload = None
                                        if workflow and hasattr(workflow, "get_stats"):
                                            try:
                                                payload = workflow.get_stats()
                                            except Exception:
                                                payload = None

                                        # workflow 통계를 못 얻었으면 DB에서 최종 집계 조회
                                        if payload is None:
                                            try:
                                                from db.crawl_db_manager import get_crawling_log_summary
                                                summary = await get_crawling_log_summary(job_id_for_log, dbname=db_name)
                                                payload = {
                                                    "scan_count": summary.get("scan", 0),
                                                    "total_count": summary.get("scan", 0),
                                                    "collection_count": summary.get("collection", 0),
                                                    "save_count": summary.get("save", 0),
                                                    "study_count": summary.get("study", 0),
                                                    "study_success_count": summary.get("study", 0),
                                                }
                                            except Exception:
                                                payload = {}

                                        # 발행 시도
                                        try:
                                            if payload:
                                                from backend.shared.crawl_shared import send_sse_message
                                                await send_sse_message(job_id_for_log, payload, db_name, source="study_worker")
                                        except Exception as pub_exc:
                                            logger.debug("[StudyWorker] SSE publish failed: %s", pub_exc)
                                    except Exception:
                                        pass
                                
                                # ✅ workflow의 _pending_study_success_keys에 count_key 추가
                                try:
                                    count_key = item.get("_count_key")
                                    if not count_key:
                                        try:
                                            fallback_db_id = item.get("db_id")
                                        except Exception:
                                            fallback_db_id = None
                                        count_key = (
                                            f"db:{fallback_db_id}" if fallback_db_id else None
                                        ) or (item.get("url") or "").strip() or file_path or name
                                        if not count_key:
                                            count_key = f"study_fallback_{int(time.time() * 1000)}_{id(item)}"
                                        # 보정된 키를 다시 주입 (후속 디버깅/추적 용)
                                        try:
                                            item["_count_key"] = count_key
                                        except Exception:
                                            pass
                                        logger.info(
                                            "[StudyWorker] ⚠️ _count_key missing; fallback generated | job_id=%s count_key=%s",
                                            job_id_for_log,
                                            count_key,
                                        )

                                    if count_key and job_id_for_log:
                                        from backend.shared.crawler_state import crawler_state
                                        workflow = crawler_state.workflows.get(job_id_for_log)
                                        if workflow and hasattr(workflow, "_pending_study_success_keys"):
                                            stats_lock = getattr(workflow, "_stats_lock", None)
                                            if stats_lock:
                                                async with stats_lock:
                                                    if hasattr(workflow, "_pending_study_success_keys"):
                                                        workflow._pending_study_success_keys.add(count_key)
                                                        logger.info(
                                                            "[StudyWorker] ✅ Added to _pending_study_success_keys | job_id=%s count_key=%s pending_success_count=%s",
                                                            job_id_for_log,
                                                            count_key,
                                                            len(workflow._pending_study_success_keys),
                                                        )
                                                        # ✅ progress_callback 호출하여 UI 즉시 갱신
                                                        if hasattr(workflow, "progress_callback") and workflow.progress_callback:
                                                            try:
                                                                workflow.progress_callback(workflow.get_stats())
                                                            except Exception:
                                                                pass
                                            else:
                                                # lock이 없으면 직접 추가 (동시성 문제 가능하지만 예외 처리)
                                                if hasattr(workflow, "_pending_study_success_keys"):
                                                    workflow._pending_study_success_keys.add(count_key)
                                                    logger.info(
                                                        "[StudyWorker] ✅ Added to _pending_study_success_keys (no lock) | job_id=%s count_key=%s pending_success_count=%s",
                                                        job_id_for_log,
                                                        count_key,
                                                        len(workflow._pending_study_success_keys),
                                                    )
                                                    # ✅ progress_callback 호출하여 UI 즉시 갱신
                                                    if hasattr(workflow, "progress_callback") and workflow.progress_callback:
                                                        try:
                                                            workflow.progress_callback(workflow.get_stats())
                                                        except Exception:
                                                            pass
                                        elif workflow and hasattr(workflow, "_counted_study_keys") and hasattr(workflow, "stats"):
                                            # BoardContentWorkflow 등 pending 키가 없는 워크플로우도
                                            # 카운트 증가는 workflow._mark_study_done로 일원화하여
                                            # 중복 가산을 방지한다.
                                            try:
                                                # Use workflow's _mark_study_done to ensure single place에서 증가
                                                outcome_norm = "success"
                                                try:
                                                    # call central marker which already guards with _counted_study_keys
                                                    await workflow._mark_study_done(url=count_key, outcome=outcome_norm)
                                                except Exception:
                                                    # As fallback, fall back to guarded increment (best-effort)
                                                    stats_lock = getattr(workflow, "_stats_lock", None)
                                                    if stats_lock:
                                                        async with stats_lock:
                                                            if count_key not in workflow._counted_study_keys:
                                                                workflow._counted_study_keys.add(count_key)
                                                                _file_ns = bool(
                                                                    getattr(
                                                                        workflow,
                                                                        "is_attachment_file_crawl_workflow",
                                                                        False,
                                                                    )
                                                                )
                                                                if _file_ns:
                                                                    workflow.stats["file_study_count"] = int(
                                                                        workflow.stats.get("file_study_count", 0) or 0
                                                                    ) + 1
                                                                    workflow.stats["file_study_success_count"] = int(
                                                                        workflow.stats.get("file_study_success_count", 0) or 0
                                                                    ) + 1
                                                                    workflow.stats["file_study_done_count"] = int(
                                                                        workflow.stats.get("file_study_done_count", 0) or 0
                                                                    ) + 1
                                                                else:
                                                                    workflow.stats["study_count"] = int(
                                                                        workflow.stats.get("study_count", 0) or 0
                                                                    ) + 1
                                                                    workflow.stats["study_success_count"] = int(
                                                                        workflow.stats.get("study_success_count", 0) or 0
                                                                    ) + 1
                                                                    workflow.stats["study_done_count"] = int(
                                                                        workflow.stats.get("study_done_count", 0) or 0
                                                                    ) + 1
                                                # Ensure UI update via progress_callback
                                                if hasattr(workflow, "progress_callback") and workflow.progress_callback:
                                                    try:
                                                        workflow.progress_callback(workflow.get_stats())
                                                    except Exception:
                                                        pass
                                            except Exception as exc:
                                                logger.debug("[StudyWorker] fallback mark_study_done failed | err=%s", exc)
                                        elif not workflow:
                                            logger.warning(
                                                "[StudyWorker] ⚠️ Workflow not found in crawler_state | job_id=%s count_key=%s",
                                                job_id_for_log,
                                                count_key,
                                            )
                                        elif not hasattr(workflow, "_pending_study_success_keys"):
                                            logger.warning(
                                                "[StudyWorker] ⚠️ Workflow has no _pending_study_success_keys | job_id=%s workflow_type=%s",
                                                job_id_for_log,
                                                type(workflow).__name__,
                                            )
                                    elif not count_key:
                                        logger.warning(
                                            "[StudyWorker] ⚠️ _count_key not found in item | job_id=%s item_keys=%s",
                                            job_id_for_log,
                                            list(item.keys()) if isinstance(item, dict) else "not_dict",
                                        )
                                except Exception as pending_exc:
                                    logger.error(
                                        "[StudyWorker] ❌ Failed to add to _pending_study_success_keys | job_id=%s err=%s",
                                        job_id_for_log,
                                        pending_exc,
                                        exc_info=True,
                                    )
                        else:
                            logger.error(
                                "[LearningError][study_item.finalize_skipped] item_job_id=%s sub_job_id=%s db=%s learn_list_id=%s chunks=%s reason=no_chunks",
                                item_job_id,
                                sub_job_id,
                                db_name,
                                learn_list_id,
                                chunks_val,
                            )
                    else:
                        logger.debug(
                            "[LearningTrace][study_item.finalize_skipped] item_job_id=%s sub_job_id=%s db=%s learn_list_id=%s chunks=%s reason=%s",
                            item_job_id,
                            sub_job_id,
                            db_name,
                            learn_list_id,
                            chunks_val,
                            "missing_learn_list_id" if not learn_list_id else "non_dict_result",
                        )
                except Exception as trig_exc:
                    logger.warning(
                        "[StudyWorker] trigger_learning failed | id=%s file=%s err=%s",
                        learn_list_id,
                        os.path.basename(file_path),
                        trig_exc,
                    )

                detail_url = (item.get("source_page") or item.get("post_url") or item.get("referer") or "").strip()
                download_url = (item.get("url") or "").strip()
                logger.info(
                    "[StudyWorker][test090] learn finished | status=%s file=%s detail_url=%s download_url=%s",
                    (result or {}).get("status"),
                    os.path.basename(file_path),
                    detail_url,
                    download_url,
                )
                logger.debug(
                    "[LearningTrace][study_item.done] item_job_id=%s sub_job_id=%s db=%s file=%s elapsed_ms=%s status=%s chunks=%s",
                    item_job_id,
                    sub_job_id,
                    db_name,
                    os.path.basename(file_path),
                    int((time.perf_counter() - item_started) * 1000),
                    (result or {}).get("status") if isinstance(result, dict) else None,
                    chunks_val,
                )
                # 자동 원본 삭제 (학습 성공 후)
                try:
                    delete_enabled = str(os.getenv("WEB_SYNC_DELETE_AFTER_STUDY", "1") or "1").strip().lower() in (
                        "1",
                        "true",
                        "yes",
                        "on",
                    )
                    chunks_val_calc = 0
                    try:
                        chunks_val_calc = int((result or {}).get("chunks", 0) or 0)
                    except Exception:
                        chunks_val_calc = 0
                    learn_status = (result or {}).get("status")
                    # 조건: 삭제 기능 활성화, 청크가 존재하고 학습 상태가 성공 계열일 때 삭제
                    if delete_enabled and chunks_val_calc > 0 and learn_status in ("success", "ok", "completed"):
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                logger.info("[StudyWorker] original file removed after study | file=%s", file_path)
                        except Exception as del_err:
                            logger.error("[StudyWorker] failed to remove original file after study | file=%s err=%s", file_path, del_err)
                except Exception:
                    pass
            except Exception as item_err:
                logger.error(
                    "[LearningError][study_item.error] job_id=%s db=%s file=%s elapsed_ms=%s err=%s",
                    item.get("job_id") if isinstance(item, dict) else None,
                    resolve_db_name(item) if isinstance(item, dict) else None,
                    os.path.basename(item.get("file_path") or item.get("local_path") or "")
                    if isinstance(item, dict)
                    else None,
                    int((time.perf_counter() - item_started) * 1000),
                    item_err,
                    exc_info=True,
                )

    tasks = [asyncio.create_task(_process_item(item)) for item in batch]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# Study Worker implementation for queue-based processing
async def study_worker(in_queue: BatchQueue):
    """
    Study Worker:
    - SaveBatchQueue(in_queue)에서 저장 완료된 파일 정보를 가져옴
    - learn() 함수를 호출하여 실제 학습(벡터화 등) 프로세스 진행
    """
    # 의존성 객체 미리 생성 (워커 레벨에서 재사용)
    try:
        redis_client = await get_redis()
        job_manager = AsyncJobManager(redis_client)
        job_progress_manager = AsyncJobProgress(redis_client)
    except Exception as e:
        logger.error(f"[StudyWorker] Failed to initialize managers: {e}")
        return

    # 배치 내부 병렬 처리 수 (과도한 병렬은 DB/벡터스토어 부하 증가)
    try:
        batch_conc = int(os.getenv("STUDY_BATCH_CONCURRENCY", "3") or "3")
    except Exception:
        batch_conc = 3
    batch_conc = max(1, min(batch_conc, 10))

    while True:
        try:
            # 배치 가져오기
            batch = await in_queue.get()
            if not batch:
                in_queue.task_done()
                continue
            batch_started = time.perf_counter()
            first_item = batch[0] if isinstance(batch, list) and batch else {}
            logger.debug(
                "[LearningTrace][study_worker.batch.start] size=%s queue_depth=%s first_job_id=%s first_file=%s",
                len(batch) if isinstance(batch, list) else None,
                _batch_queue_depth(in_queue),
                first_item.get("job_id") if isinstance(first_item, dict) else None,
                os.path.basename((first_item.get("file_path") or first_item.get("local_path") or ""))
                if isinstance(first_item, dict)
                else None,
            )
            await study_process_batch_items(
                batch, redis_client, job_manager, job_progress_manager
            )
            logger.debug(
                "[LearningTrace][study_worker.batch.done] size=%s queue_depth=%s elapsed_ms=%s",
                len(batch) if isinstance(batch, list) else None,
                _batch_queue_depth(in_queue),
                int((time.perf_counter() - batch_started) * 1000),
            )
            in_queue.task_done()
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[StudyWorker] Unexpected error: {e}")
            await asyncio.sleep(1)

# Backwards compatibility alias if needed
# study_worker = learn # Removed in favor of proper implementation above
