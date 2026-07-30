import asyncio
from typing import Any, Dict, Optional


def _default_crawl_progress() -> Dict[str, Any]:
    return {
        "status": "idle",
        "stage": "idle",
        "message": "대기 중...",
        "scan_count": 0,
        "collection_count": 0,
        "save_count": 0,
        "study_count": 0,
        "summary": None,
        "links": [],
        "recent_files": [],
        "error": None,
    }


crawl_progress: Dict[str, Any] = _default_crawl_progress()
current_crawl_task: Optional[asyncio.Task] = None
current_workflow: Optional[Any] = None


def _replace_crawl_progress(progress: Dict[str, Any]) -> None:
    crawl_progress.clear()
    crawl_progress.update(progress)


def reset_crawl_progress() -> None:
    """Reinitialize crawl progress to the default idle state."""
    _replace_crawl_progress(_default_crawl_progress())


def reset_crawl_progress_for_stop(message: str = "크롤링이 중단되었습니다.") -> None:
    """Clear counts and transient lists while preserving a cancelled terminal state."""
    progress = _default_crawl_progress()
    progress["status"] = "cancelled"
    progress["message"] = message
    _replace_crawl_progress(progress)

