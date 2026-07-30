"""
Celery 태스크 발행 공통 진입점.
backend·core 등 어디서나 동일한 방식으로 Celery 작업을 넣을 수 있도록 연결합니다.
"""

import logging
import os
from typing import Optional, Any

logger = logging.getLogger(__name__)

_celery_app = None


def _warn_if_no_celery_workers(app, context: str, queue_name: str) -> None:
    """
    브로커에는 메시지가 들어갔지만 worker가 없으면 작업이 영원히 대기한다.
    운영에서 흔한 원인: uvicorn만 띄우고 celery worker systemd/unit을 안 띄운 경우.
    """
    if str(os.getenv("CELERY_SKIP_WORKER_PING", "") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return
    try:
        ping = app.control.inspect(timeout=2.0).ping()
        if not ping:
            logger.warning(
                "[TaskSender] %s | queue=%s | 브로커에 태스크는 넣었지만 "
                "응답하는 Celery worker가 없습니다. "
                "별도 프로세스로 워커를 실행하세요 (예: 프로젝트 루트에서 "
                "`celery -A run_celery_worker worker -l info -Q %s`). "
                "CELERY_BROKER_URL(또는 .env)이 uvicorn과 워커에서 동일한지 확인하세요.",
                context,
                queue_name,
                queue_name,
            )
        else:
            logger.debug("[TaskSender] Celery worker ping OK | workers=%s", list(ping.keys()))
    except Exception as exc:
        logger.debug("[TaskSender] Celery inspect/ping 실패(무시 가능): %s", exc)


def get_celery_app():
    """Celery 앱 인스턴스 반환. 없으면 None. (backend 실행 / 프로젝트 루트 실행 모두 지원)"""
    global _celery_app
    if _celery_app is not None:
        return _celery_app
    try:
        from src.tasks.celery_app import celery_app
        _celery_app = celery_app
        return celery_app
    except Exception as e:
        logger.debug("Celery app (src.tasks): %s", e)
    try:
        from backend.src.tasks.celery_app import celery_app  # type: ignore
        _celery_app = celery_app
        return celery_app
    except Exception as e:
        logger.debug("Celery app (backend.src.tasks): %s", e)
    return None


def send_crawl_site_background(
    start_url: str,
    max_depth: int = 3,
    max_pages: int = 150,
    save_to_db: bool = True,
    domain: Optional[str] = None,
    **kwargs: Any,
) -> Optional[str]:
    """
    크롤링 백그라운드 태스크 발행.
    Returns:
        task_id 또는 None (Celery 없을 때)
    """
    app = get_celery_app()
    if not app:
        logger.debug("[TaskSender] Celery unavailable, skip send_crawl_site_background")
        return None
    try:
        from src.tasks.crawl_tasks import crawl_site_background
        t = crawl_site_background.apply_async(
            args=[start_url],
            kwargs={
                "max_depth": max_depth,
                "max_pages": max_pages,
                "save_to_db": save_to_db,
                "domain": domain,
                **kwargs,
            },
        )
        logger.info("[TaskSender] crawl_site_background enqueued task_id=%s", t.id)
        return str(t.id)
    except Exception as e:
        logger.warning("[TaskSender] send_crawl_site_background failed: %s", e)
        return None


def send_faiss_refresh(
    db_name: str,
    chat_id: str,
    queue: str = "faiss_index_chatty_9000",
) -> Optional[str]:
    """
    FAISS 인덱스 갱신 태스크 발행 (외부 앱 태스크명 사용).
    Returns:
        task_id 또는 None
    """
    app = get_celery_app()
    if not app:
        logger.debug("[TaskSender] Celery unavailable, skip send_faiss_refresh")
        return None
    try:
        t = app.send_task(
            "chat.faiss_process.refresh_index_task",
            args=[db_name, chat_id],
            queue=queue,
        )
        task_id = getattr(t, "id", None) or str(t)
        logger.info("[TaskSender] faiss refresh enqueued task_id=%s queue=%s", task_id, queue)
        return task_id
    except Exception as e:
        logger.warning("[TaskSender] send_faiss_refresh failed: %s", e)
        return None


def send_download_file_background(
    file_url: str,
    filename: str,
    save_dir: str,
    domain: Optional[str] = None,
    custom_extensions: Optional[list] = None,
    **kwargs: Any,
) -> Optional[str]:
    """
    파일 다운로드 백그라운드 태스크 발행.
    Returns:
        task_id 또는 None
    """
    app = get_celery_app()
    if not app:
        logger.debug("[TaskSender] Celery unavailable, skip send_download_file_background")
        return None
    try:
        from src.tasks.crawl_tasks import download_file_background
        t = download_file_background.apply_async(
            args=[file_url, filename, save_dir],
            kwargs={
                "domain": domain,
                "custom_extensions": custom_extensions,
                **kwargs,
            },
        )
        logger.info("[TaskSender] download_file_background enqueued task_id=%s", t.id)
        return str(t.id)
    except Exception as e:
        logger.warning("[TaskSender] send_download_file_background failed: %s", e)
        return None


def send_save_metadata_to_db(
    files: list,
    domain: str,
    session_id: Optional[int] = None,
    countdown: int = 1,
) -> Optional[str]:
    """
    메타데이터 DB 저장 태스크 발행.
    Returns:
        task_id 또는 None
    """
    app = get_celery_app()
    if not app:
        logger.debug("[TaskSender] Celery unavailable, skip send_save_metadata_to_db")
        return None
    try:
        from src.tasks.metadata_tasks import save_metadata_to_db
        t = save_metadata_to_db.apply_async(
            args=[files, domain],
            kwargs={"session_id": session_id},
            countdown=countdown,
        )
        logger.info("[TaskSender] save_metadata_to_db enqueued task_id=%s", t.id)
        return str(t.id)
    except Exception as e:
        logger.warning("[TaskSender] send_save_metadata_to_db failed: %s", e)
        return None


def send_crawl_workflow_dispatch_job(job_id: str) -> Optional[str]:
    """
    게시판/통합 크롤 워크플로를 Celery에서 실행 (Redis 페이로드 선행 필요).
    큐: 환경변수 CRAWL_CELERY_QUEUE (기본 celery)
    """
    app = get_celery_app()
    if not app:
        logger.debug("[TaskSender] Celery unavailable, skip send_crawl_workflow_dispatch_job")
        return None
    try:
        from src.tasks.crawl_workflow_tasks import crawl_run_workflow_dispatch

        q = (os.getenv("CRAWL_CELERY_QUEUE") or "celery").strip() or "celery"
        t = crawl_run_workflow_dispatch.apply_async(args=[job_id], queue=q)
        logger.info("[TaskSender] crawl_run_workflow_dispatch enqueued job_id=%s task_id=%s queue=%s", job_id, t.id, q)
        _warn_if_no_celery_workers(
            app,
            "crawl_run_workflow_dispatch enqueued",
            q,
        )
        return str(t.id)
    except Exception as e:
        logger.warning("[TaskSender] send_crawl_workflow_dispatch_job failed: %s", e)
        return None
