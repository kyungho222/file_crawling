from __future__ import annotations

import logging
import os
from typing import Iterable


_NOISY_INFO_MARKERS = (
    "[WorkerChain]",
    "[YonginTitleDebug]",
    "[YonginEmpmnTitleDebug]",
    "[DuplicateRepairDebug]",
    "[DuplicateSummaryDebug]",
    "[BoardDuplicate][TitleDebug]",
    "[AutoCategoryDebug]",
    "[DBLoad]",
    "[SSE-COUNT]",
    "[SSE-DBG]",
    "[STUDY-COUNT-DEBUG]",
    "[RedisSSE][UnitProgressDebug]",
    "[ProgressDebug]",
    "[Download][SaveAttempt]",
    "[Download][SaveDone]",
    "[Download][WebSync] start",
    "[Download][WebSync] done",
    "[Download][Worker",
    "[Download][Filtered]",
    "[Download] restored site-specific fileDown url",
    "[Download] source_page HTML fallback found file link",
    "[Download] HTML fallback found file link",
    "[DOWNLOAD] file written",
    "[DOWNLOAD][PathDebug]",
    "[Scan] Worker started",
    "[Scan] skip cross-job claimed url",
    "[Scan] Worker context closed safely",
    "[Validation] HEAD network error forced accepted",
    "[중복선별]",
    "[기간필터]",
    "[test002]",
)

_IMPORTANT_INFO_MARKERS = (
    "작업중지",
    "Finished",
    "Started",
    "workflow returned",
    "failed",
    "timeout",
    "cancelled",
    "canceled",
    "error",
    "Error",
)

_IMPORTANT_TRACE_TOKENS = (
    "state=timeout",
    "state=fail",
    "state=error",
    "state=slow",
    "state=discard_timeout_overrun_result",
    "phase=workflow",
    "action=workflow",
    "action=detail_slow_background",
    "action=background",
)

_NOISY_TRACE_STATES = (
    "state=start",
    "state=end",
    "state=hit",
    "state=skipped",
    "state=queued",
    "state=scheduled",
)

_NOISY_TRACE_ACTIONS = (
    "action=normalize_rule_sample",
)

_NOISY_TRACE_COMBINATIONS = (
    ("phase=redis", "action=publish_sse_event", "state=throttled"),
)


def _enabled() -> bool:
    return str(os.getenv("BOARD_SHARED_CORE_LOG_ONLY", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "y",
    }


def _matches_any(text: str, markers: Iterable[str]) -> bool:
    return any(marker in text for marker in markers)


def _matches_all(text: str, markers: Iterable[str]) -> bool:
    return all(marker in text for marker in markers)


class BoardSharedCoreLogFilter(logging.Filter):
    """Suppress noisy board/shared INFO logs while keeping warnings and key progress."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not _enabled():
            return True
        if record.levelno >= logging.WARNING:
            return True

        name = str(record.name or "")
        if not (
            name.startswith("backend.board")
            or name.startswith("backend.file")
            or name.startswith("backend.shared")
            or name.startswith("core.crawler.workers")
            or name in {"redis_sse_service", "header_crawler", "runtime_tab_view"}
        ):
            return True

        try:
            msg = record.getMessage()
        except Exception:
            msg = str(record.msg or "")

        if "[CrawlTrace]" in msg:
            if _matches_any(msg, _IMPORTANT_TRACE_TOKENS):
                return True
            if any(_matches_all(msg, markers) for markers in _NOISY_TRACE_COMBINATIONS):
                return False
            if _matches_any(msg, _NOISY_TRACE_ACTIONS):
                return False
            if _matches_any(msg, _NOISY_TRACE_STATES):
                return False

        if _matches_any(msg, _NOISY_INFO_MARKERS) and not _matches_any(msg, _IMPORTANT_INFO_MARKERS):
            return False

        return True


def install_board_shared_core_log_filter() -> None:
    root = logging.getLogger()
    for handler in list(root.handlers or []):
        if any(isinstance(f, BoardSharedCoreLogFilter) for f in getattr(handler, "filters", []) or []):
            continue
        handler.addFilter(BoardSharedCoreLogFilter())
