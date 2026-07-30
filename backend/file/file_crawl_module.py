"""File crawl workflow assembly helpers."""

from __future__ import annotations

import logging
import os
from typing import Any, List, Optional

from backend.board.board_crawl_module import create_board_crawl_workflow
from backend.shared.crawl_request_config import parse_bool
from backend.file.file_wait_policy import (
    FILE_FETCH_DELAY_PAYLOAD_KEY,
    FILE_FETCH_DELAY_SOURCE_KEY,
    backend_file_fetch_delay_sec,
)

logger = logging.getLogger("backend.file.file_crawl_module")


def _env_skip_learning() -> bool:
    try:
        return str(os.getenv("BOARD_FILE_DOWNLOAD_SKIP_LEARNING", "") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
    except Exception:
        return False


def _payload_bool(value: Any, default: Optional[bool] = None) -> Optional[bool]:
    return parse_bool(value, default)


def _apply_file_db_save_option(workflow: Any, data: dict, *, job_id: str) -> None:
    raw = (
        data.get("enable_db_save")
        if "enable_db_save" in data
        else data.get("enableDbSave")
        if "enableDbSave" in data
        else data.get("file_pipeline_enable_db_save")
    )
    enabled = _payload_bool(raw, None)
    if enabled is None:
        return

    try:
        workflow.enable_db_save = bool(enabled)
    except Exception:
        pass
    if not enabled:
        try:
            workflow.enable_learning = False
        except Exception:
            pass
        try:
            workflow.file_pipeline_skip_learning = True
        except Exception:
            pass

def _apply_file_learning_option(workflow: Any, data: dict, *, job_id: str) -> None:
    if not hasattr(workflow, "file_pipeline_skip_learning"):
        return

    try:
        if getattr(workflow, "enable_db_save", None) is False:
            workflow.file_pipeline_skip_learning = True
            return
    except Exception:
        pass

    skip_raw = data.get("file_pipeline_skip_learning")
    env_skip = _env_skip_learning()
    skip_value = _payload_bool(skip_raw, None)
    if skip_value is True:
        workflow.file_pipeline_skip_learning = True
    elif env_skip:
        workflow.file_pipeline_skip_learning = True
    elif skip_value is False:
        workflow.file_pipeline_skip_learning = False

 


def _apply_file_workflow_boundary(workflow: Any, data: dict) -> None:
    """Keep file crawling mode explicit at the module boundary."""
    data["colle"] = "file"
    data["content_type"] = "file"

    for attr, value in (
        ("colle", "file"),
        ("colle_mode", "file"),
        ("ui_colle", "file"),
        ("file_mode", True),
        ("content_type", "file"),
    ):
        try:
            setattr(workflow, attr, value)
        except Exception:
            pass


def _as_float(value: Any, default: float) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: int) -> int:
    try:
        if value in (None, ""):
            return default
        return int(value)
    except Exception:
        return default


def _apply_file_fetch_policy(workflow: Any, data: dict, *, job_id: str) -> None:
    """File crawling favors completeness over fast failure."""
    fetch_timeout = max(
        5.0,
        min(
            _as_float(
                data.get("file_crawl_fetch_timeout_sec")
                or data.get("fileCrawlFetchTimeoutSec")
                or os.getenv("FILE_CRAWL_DETAIL_FETCH_TIMEOUT_SEC")
                or os.getenv("FILE_CRAWL_ACCELERATED_FETCH_TIMEOUT_SEC"),
                30.0,
            ),
            90.0,
        ),
    )
    retry_timeout = max(
        fetch_timeout,
        min(
            _as_float(
                data.get("file_crawl_retry_fetch_timeout_sec")
                or data.get("fileCrawlRetryFetchTimeoutSec")
                or os.getenv("FILE_CRAWL_RETRY_FETCH_TIMEOUT_SEC"),
                max(fetch_timeout * 1.5, 45.0),
            ),
            120.0,
        ),
    )
    queue_timeout = max(
        5.0,
        min(
            _as_float(
                data.get("file_crawl_fetch_queue_timeout_sec")
                or data.get("fileCrawlFetchQueueTimeoutSec")
                or os.getenv("FILE_CRAWL_FETCH_QUEUE_TIMEOUT_SEC"),
                60.0,
            ),
            300.0,
        ),
    )
    guard_grace = max(
        1.0,
        min(
            _as_float(
                data.get("file_crawl_fetch_guard_grace_sec")
                or data.get("fileCrawlFetchGuardGraceSec")
                or os.getenv("FILE_CRAWL_FETCH_GUARD_GRACE_SEC"),
                5.0,
            ),
            30.0,
        ),
    )
    retry_workers = max(
        1,
        min(
            _as_int(
                data.get("file_crawl_detail_retry_workers")
                or data.get("fileCrawlDetailRetryWorkers")
                or os.getenv("FILE_CRAWL_DETAIL_RETRY_WORKERS"),
                4,
            ),
            8,
        ),
    )
    tail_wait = max(
        60.0,
        min(
            _as_float(
                data.get("file_crawl_tail_wait_sec")
                or data.get("fileCrawlTailWaitSec")
                or os.getenv("FILE_CRAWL_TAIL_WAIT_SEC"),
                1200.0,
            ),
            7200.0,
        ),
    )
    fetch_delay = _as_float(
        data.get(FILE_FETCH_DELAY_PAYLOAD_KEY),
        backend_file_fetch_delay_sec(),
    )
    fetch_delay = max(0.0, min(fetch_delay, 60.0))

    try:
        workflow.accelerated_parse_only = False
        workflow.accelerated_fast_parse_only = False
        workflow.accelerated_fetch_timeout_sec = fetch_timeout
        workflow._file_crawl_static_fetch_timeout_sec = fetch_timeout
        workflow._file_crawl_retry_fetch_timeout_sec = retry_timeout
        workflow._file_crawl_fetch_guard_grace_sec = guard_grace
        workflow._file_crawl_fetch_queue_timeout_sec = queue_timeout
        workflow._file_crawl_detail_retry_workers = retry_workers
        workflow._file_crawl_tail_wait_sec = tail_wait
        workflow._file_crawl_fetch_delay_sec = fetch_delay
        workflow._file_crawl_fetch_delay_source = str(
            data.get(FILE_FETCH_DELAY_SOURCE_KEY) or "backend_default"
        )
    except Exception:
        pass

    for key, value in (
        ("BOARD_DETAIL_STATIC_FETCH_TIMEOUT_SEC", fetch_timeout),
        ("BOARD_DETAIL_PREFETCH_STATIC_FETCH_TIMEOUT_SEC", fetch_timeout),
        ("BOARD_DETAIL_PREFETCH_STATIC_FETCH_MAX_SEC", max(fetch_timeout, 60.0)),
        ("BOARD_DETAIL_PREFETCH_RETRY_STATIC_FETCH_TIMEOUT_SEC", retry_timeout),
        ("BOARD_DETAIL_PREFETCH_GUARD_GRACE_SEC", guard_grace),
        ("BOARD_FETCH_GUARD_QUEUE_TIMEOUT_DEFAULT_SEC", queue_timeout),
        ("BOARD_DETAIL_RETRY_QUEUE_WORKERS", retry_workers),
        ("WORKFLOW_BOARD_TAIL_WAIT_SEC", tail_wait),
    ):
        os.environ.setdefault(key, str(value))

  


def create_file_crawl_workflow(
    *,
    workflow_class: type,
    data: dict,
    start_urls: List[Any],
    primary_target_url: Optional[str],
    job_id: str,
) -> Any:
    """Create and initialize the attachment-file crawling workflow surface."""
    workflow = create_board_crawl_workflow(
        workflow_class=workflow_class,
        data=data,
        start_urls=start_urls,
        primary_target_url=primary_target_url,
        job_id=job_id,
    )
    _apply_file_workflow_boundary(workflow, data)
    _apply_file_fetch_policy(workflow, data, job_id=job_id)
    _apply_file_db_save_option(workflow, data, job_id=job_id)
    _apply_file_learning_option(workflow, data, job_id=job_id)
    try:
        logger.info(
            "[FileLearningPolicy] resolved | job_id=%s db=%s chat_bot_id=%s enable_db_save=%s enable_learning=%s skip_learning=%s content_type=%s",
            job_id,
            getattr(workflow, "db_name", None),
            getattr(workflow, "chat_bot_id", None),
            bool(getattr(workflow, "enable_db_save", False)),
            bool(getattr(workflow, "enable_learning", False)),
            bool(getattr(workflow, "file_pipeline_skip_learning", False)),
            getattr(workflow, "content_type", None),
        )
    except Exception:
        pass
    return workflow
