import asyncio
import logging
from typing import Any, Dict, Optional

from backend import app_state

logger = logging.getLogger(__name__)


async def stop_active_crawl(job_id: Optional[str] = None, source: str = "external") -> Dict[str, Any]:
    """
    통합 중단 처리:
    - 워크플로우/백그라운드 Task 중단
    - 전역 진행 상태 갱신
    """
    crawl_progress = app_state.crawl_progress

    # 이미 중단/대기 상태면 그대로 상태 반환
    if crawl_progress.get("status") in {"idle", "cancelled"} and not app_state.current_crawl_task:
        return {
            "status": crawl_progress.get("status", "idle"),
            "message": crawl_progress.get("message", "대기 중..."),
            "job_id": job_id,
            "source": source,
        }

    print("================중단요청 호출06==================")
    logger.info("[StopService] Stop requested via %s | job_id=%s", source, job_id)

    crawl_progress["status"] = "stopping"
    crawl_progress["message"] = "중단 요청 처리 중..."
    print("================ㅅㅅㅅ1==================")

    if app_state.current_workflow:
        print("[Main] Stopping workflow...", flush=True)
        try:
            ret = app_state.current_workflow.stop()
            import inspect
            if inspect.isawaitable(ret):
                await ret
        except Exception as exc:
            logger.warning("[StopService] Failed to signal workflow stop: %s", exc)
        await asyncio.sleep(0.5)
        app_state.current_workflow = None

    if app_state.current_crawl_task and not app_state.current_crawl_task.done():
        print("================ㅅㅅㅅ2==================")
        app_state.current_crawl_task.cancel()
        try:
            await app_state.current_crawl_task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning("[StopService] Error while awaiting cancelled task: %s", exc)

    app_state.current_crawl_task = None

    app_state.reset_crawl_progress_for_stop("크롤링이 중단되었습니다.")

    return {
        "status": "cancelled",
        "message": "크롤링이 중단되었습니다.",
        "job_id": job_id,
        "source": source,
    }


