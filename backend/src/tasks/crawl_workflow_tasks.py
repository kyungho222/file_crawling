"""
게시판/파일 크롤 워크플로를 Celery 워커에서 실행.
- 페이로드는 Redis 키 crawl_wf_payload:{job_id} (JSON)
- 환경변수 CRAWL_WORKFLOW_USE_CELERY=1 일 때 dispatch에서 큐에 넣음
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

current_file = Path(__file__).resolve()
backend_root = current_file.parent.parent.parent
project_root = backend_root.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

from src.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)
CONCURRENT_CRAWL_LOG_PREFIX = "[ConcurrentCrawlStartDebug]"


@celery_app.task(name="crawl.run_workflow_dispatch", bind=True)
def crawl_run_workflow_dispatch(self, job_id: str) -> None:
    """Redis에서 페이로드를 읽어 asyncio 워크플로를 실행한다."""
    logger.debug("%s[celery_task_received] job_id=%s", CONCURRENT_CRAWL_LOG_PREFIX, job_id)
    try:
        asyncio.run(_run_crawl_dispatch_async(job_id))
    except Exception:
        logger.exception("%s[celery_task_failed] job_id=%s", CONCURRENT_CRAWL_LOG_PREFIX, job_id)
        raise


async def _run_crawl_dispatch_async(job_id: str) -> None:
    from db.db_redis import get_redis
    from backend.shared.workflow_dispatch_assembly import assemble_workflow_after_url_resolve
    from backend.shared.workflow_runner import run_workflow_task
    from backend.shared.crawl_monitor import monitor_auto_stop
    from backend.shared.crawler_state import crawler_state
    from utils.timezone_utils import get_local_now

    r = await get_redis()
    raw = await r.get(f"crawl_wf_payload:{job_id}")
    if not raw:
        logger.error("%s[celery_payload_missing] job_id=%s", CONCURRENT_CRAWL_LOG_PREFIX, job_id)
        return
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    try:
        payload = json.loads(raw)
    except Exception as exc:
        logger.error("%s[celery_payload_invalid] job_id=%s err=%s", CONCURRENT_CRAWL_LOG_PREFIX, job_id, exc)
        await r.delete(f"crawl_wf_payload:{job_id}")
        return

    data = payload.get("data") or {}
    start_urls = payload.get("start_urls") or []
    sd = payload.get("start_date_iso")
    ed = payload.get("end_date_iso")
    start_date = None
    end_date = None
    try:
        if sd:
            start_date = datetime.fromisoformat(str(sd))
    except Exception:
        start_date = None
    try:
        if ed:
            end_date = datetime.fromisoformat(str(ed))
    except Exception:
        end_date = None

    job_id = str(payload.get("job_id") or job_id)
    craw_id = str(payload.get("craw_id") or "")
    db_name = str(payload.get("db_name") or "dev_user")
    chat_bot_id = payload.get("chat_bot_id")
    use_query_links_only = bool(payload.get("use_query_links_only"))
    override_source = str(payload.get("override_source") or "")
    primary_content = payload.get("primary_content")
    logger.debug(
        "%s[celery_payload_loaded] job_id=%s db=%s chat_bot_id=%s start_urls=%s override_source=%s",
        CONCURRENT_CRAWL_LOG_PREFIX,
        job_id,
        db_name,
        chat_bot_id,
        len(start_urls or []),
        override_source,
    )

    try:
        pending_stop = await r.get(f"crawl_stop_request:{job_id}")
    except Exception:
        pending_stop = None
    if pending_stop:
        logger.info("%s[celery_stop_before_start] job_id=%s db=%s", CONCURRENT_CRAWL_LOG_PREFIX, job_id, db_name)
        try:
            await r.delete(f"crawl_wf_payload:{job_id}")
            await r.delete(f"crawl_wf_active:{job_id}")
            await r.delete(f"crawl_stop_request:{job_id}")
        except Exception:
            pass
        return

    workflow = assemble_workflow_after_url_resolve(
        data=data,
        start_urls=start_urls,
        start_date=start_date,
        end_date=end_date,
        job_id=job_id,
        craw_id=craw_id,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        use_query_links_only=use_query_links_only,
        override_source=override_source,
        primary_content=primary_content,
    )
    logger.debug(
        "%s[celery_workflow_assembled] job_id=%s workflow=%s start_urls=%s",
        CONCURRENT_CRAWL_LOG_PREFIX,
        job_id,
        type(workflow).__name__,
        len(start_urls or []),
    )

    try:
        crawler_state.workflows[job_id] = workflow
    except Exception:
        pass
    try:
        crawler_state.record_history(job_id, "celery_started", "workflow_worker", db_name, chat_bot_id=chat_bot_id)
    except Exception:
        pass

    try:
        start_local_time = get_local_now()
    except Exception:
        start_local_time = None

    try:
        asyncio.create_task(
            monitor_auto_stop(
                workflow=workflow,
                job_id=job_id,
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                stop_signal=getattr(workflow, "stop_event", None) or asyncio.Event(),
                start_time=start_local_time,
            ),
            name=f"auto_stop_celery:{job_id}",
        )
    except Exception as exc:
        logger.debug("%s[celery_monitor_start_failed] job_id=%s err=%s", CONCURRENT_CRAWL_LOG_PREFIX, job_id, exc)

    try:
        await run_workflow_task(
            workflow,
            start_urls,
            start_date,
            end_date,
            job_id,
            craw_id,
            db_name,
            chat_bot_id,
            use_query_links_only,
        )
    finally:
        try:
            crawler_state.workflows.pop(job_id, None)
            crawler_state.workflow_tasks.pop(job_id, None)
        except Exception:
            pass
        try:
            await r.delete(f"crawl_wf_payload:{job_id}")
            await r.delete(f"crawl_wf_active:{job_id}")
            await r.delete(f"crawl_stop_request:{job_id}")
        except Exception:
            pass
        logger.debug("%s[celery_finished_cleanup] job_id=%s", CONCURRENT_CRAWL_LOG_PREFIX, job_id)
