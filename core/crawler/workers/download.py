# core/crawler/workers/download.py
"""
File download worker (real-time path).
- Processes items emitted by the collection stage immediately.
- Runs parallel downloads through a semaphore.
- Detects and blocks HTML responses.
- Parses Content-Disposition filenames.
- Emits detailed logs and handles retryable errors.

Important: this worker does not rely on Playwright by default.
- scan/collection may use Playwright.
- download uses ordinary HTTP via aiohttp first.
- Playwright is used only as a fallback for difficult download pages.
"""
import asyncio
import aiohttp
import os
import re
import logging
import sys
import zipfile
from typing import List, Dict, Optional, Callable, Awaitable, Any, Tuple
from playwright.async_api import Browser
import json
import time
import socket
import random
import threading
from datetime import datetime, timezone

from html import unescape
from urllib.parse import parse_qsl, quote, urlencode, unquote, urljoin, urlparse, urlunparse

# Add project root to sys.path (core/crawler/workers -> ../../../)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from uuid import uuid4
from config.settings import settings, get_uploaded_files_local_dir, normalize_access_url, get_storage_domain_for_db_name, get_file_upload_content_url, get_fileupload_root
from config.constants import DOC_EXTENSIONS
from utils.file import parse_display_file_size_bytes, sanitize_filename, make_safe_storage_filename
from core.crawler.batch_queue import BatchQueue
from utils.download_doc_filter import (
    DOWNLOAD_DOC_ONLY,
    is_blocked_non_document,
    should_skip_attachment_at_scan,
)
from utils.web_sync import sync_file_to_webserver
from utils.db_name import resolve_db_name
from utils.url import canonicalize_url_for_dedup, extract_download_url_from_js
from utils.attachment_url_normalize import extract_attachment_key_candidates
from backend.shared.completed_url_ttl_cache import completed_url_cached
from utils.download_integrity import is_partial_download_path, wait_for_file_ready
from backend.board.anseong_file import (
    extract_anseong_attachment_key_candidates,
    resolve_anseong_yhlib_download_url,
)
from backend.shared.file_name_debug import emit_file_name_debug
from backend.shared.playwright_optimizations import (
    apply_stealth_if_needed,
    configure_context_for_crawl,
)


_ACTIVE_DOWNLOAD_ITEMS: Dict[str, Dict[str, Any]] = {}


def _register_download_activity(worker_id: int, file_meta: Any, worker_lane: str = "normal") -> str:
    meta = file_meta if isinstance(file_meta, dict) else {}
    token = f"{worker_id}:{id(file_meta)}:{time.monotonic_ns()}"
    _ACTIVE_DOWNLOAD_ITEMS[token] = {
        "worker_id": worker_id,
        "worker_lane": worker_lane,
        "job_id": str(meta.get("job_id") or ""),
        "url": str(meta.get("url") or meta.get("_raw_url") or "")[:220],
        "post_url": str(meta.get("source_page") or meta.get("source_url") or "")[:220],
        "name": str(meta.get("name") or meta.get("subject") or "")[:160],
        "started_at": time.monotonic(),
        "task": asyncio.current_task(),
    }
    return token


def _clear_download_activity(token: str) -> None:
    _ACTIVE_DOWNLOAD_ITEMS.pop(token, None)


def _set_download_activity_phase(file_meta: Any, phase: str) -> None:
    """Expose the currently awaited download phase to queue diagnostics."""
    if not isinstance(file_meta, dict):
        return
    now = time.monotonic()
    file_meta["_download_trace_phase"] = str(phase or "unknown")
    file_meta["_download_trace_phase_started_at"] = now
    token = str(file_meta.get("_download_activity_token") or "")
    if token and token in _ACTIVE_DOWNLOAD_ITEMS:
        _ACTIVE_DOWNLOAD_ITEMS[token]["phase"] = file_meta["_download_trace_phase"]
        _ACTIVE_DOWNLOAD_ITEMS[token]["phase_started_at"] = now


def get_download_worker_activity_snapshot(*, job_id: Optional[str] = None) -> List[Dict[str, Any]]:
    expected_job_id = str(job_id or "").strip()
    now = time.monotonic()
    active: List[Dict[str, Any]] = []
    for state in list(_ACTIVE_DOWNLOAD_ITEMS.values()):
        if expected_job_id and state.get("job_id") != expected_job_id:
            continue
        active.append(
            {
                "worker": state.get("worker_id"),
                "lane": state.get("worker_lane"),
                "job_id": state.get("job_id"),
                "elapsed_sec": round(max(0.0, now - float(state.get("started_at") or now)), 1),
                "url": state.get("url"),
                "post_url": state.get("post_url"),
                "name": state.get("name"),
                "phase": state.get("phase") or "unknown",
                "phase_elapsed_sec": round(max(0.0, now - float(state.get("phase_started_at") or state.get("started_at") or now)), 1),
            }
        )
    return sorted(active, key=lambda item: float(item.get("elapsed_sec") or 0.0), reverse=True)


async def cancel_download_worker_activity(job_id: str) -> int:
    """Cancel active item downloads for one job without stopping shared workers."""
    expected_job_id = str(job_id or "").strip()
    if not expected_job_id:
        return 0
    tasks: List[asyncio.Task] = []
    for state in list(_ACTIVE_DOWNLOAD_ITEMS.values()):
        if state.get("job_id") != expected_job_id:
            continue
        task = state.get("task")
        if isinstance(task, asyncio.Task) and not task.done():
            task.cancel()
            tasks.append(task)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    return len(tasks)


def _is_retryable_incomplete_payload_error(exc: BaseException) -> bool:
    """Return whether a response body was interrupted after the request succeeded."""
    current: Optional[BaseException] = exc
    seen = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (aiohttp.ClientPayloadError, ConnectionResetError)):
            return True
        if type(current).__name__ in {"ContentLengthError", "ConnectionResetError"}:
            return True
        current = current.__cause__ or current.__context__
    return False


def _learn_list_ids_from_file_meta(file_meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Propagate LEARN_LIST ids attached at collection stage to study/file_saved."""
    if not isinstance(file_meta, dict):
        return {}
    raw = file_meta.get("db_id")
    if raw is None:
        raw = file_meta.get("learn_list_id")
    if raw is None:
        return {}
    if isinstance(raw, str) and not raw.strip():
        return {}
    did = file_meta.get("db_id")
    if did is None:
        did = raw
    lid = file_meta.get("learn_list_id")
    if lid is None:
        lid = raw
    return {"db_id": did, "learn_list_id": lid}


def _defer_save_batch_flag(file_meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """For board-file pipeline, defer save_batch until LEARN_LIST save completes."""
    if isinstance(file_meta, dict) and file_meta.get("defer_save_batch_until_learn_list"):
        return {"defer_save_batch_until_learn_list": True}
    return {}


# Logger setup
logger = logging.getLogger(__name__)

_URL_TRACE_LOCK = threading.Lock()


def _download_url_trace_enabled() -> bool:
    return str(os.getenv("DOWNLOAD_URL_TRACE_LOG", "1") or "1").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _write_download_url_trace(record: Dict[str, Any]) -> None:
    """Append URL-level crawler and target-response diagnostics outside normal logs."""
    if not _download_url_trace_enabled():
        return
    try:
        trace_dir = os.path.join(project_root, "download")
        os.makedirs(trace_dir, exist_ok=True)
        trace_name = f"url_trace_{datetime.now().strftime('%Y%m%d')}.jsonl"
        trace_path = os.path.join(trace_dir, trace_name)
        payload = dict(record or {})
        payload.setdefault("timestamp_utc", datetime.now(timezone.utc).isoformat())
        for key in ("job_id", "url", "post_url", "event", "outcome", "reason", "error"):
            if key in payload and payload[key] is not None:
                payload[key] = str(payload[key])[:2048]
        with _URL_TRACE_LOCK:
            with open(trace_path, "a", encoding="utf-8") as trace_file:
                trace_file.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    except Exception as exc:
        logger.debug("[DownloadUrlTrace] write failed | err=%s", exc)


async def _append_download_url_trace(record: Dict[str, Any]) -> None:
    task = asyncio.create_task(
        asyncio.to_thread(_write_download_url_trace, record),
        name="download-url-trace-write",
    )
    task.add_done_callback(_consume_download_url_trace_task_result)

def _consume_download_url_trace_task_result(task: asyncio.Task) -> None:
    try:
        task.result()
    except Exception as exc:
        logger.debug("[DownloadUrlTrace] background write failed | err=%s", exc)

FLOW_DEBUG = os.getenv("CRAWL_DEBUG_FLOW", "0") == "1"
if FLOW_DEBUG:
    try:
        logger.setLevel(logging.INFO)
    except Exception:
        pass

# Stage URL report helper
def _flow_debug_print(*args, **kwargs) -> None:
    if not FLOW_DEBUG:
        return
    kwargs.setdefault("flush", True)
    print(*args, **kwargs)

try:
    from backend.shared.stage_url_report import append_stage_urls  # type: ignore
except Exception:
    def append_stage_urls(*, stage, urls, job_id=None, db_name=None, output_dir=None, extra_meta=None, entry_extra=None):
        try:
            import sys as _sys
            if project_root not in _sys.path:
                _sys.path.insert(0, project_root)
            from backend.shared.stage_url_report import append_stage_urls as _impl  # type: ignore
            return _impl(stage=stage, urls=urls, job_id=job_id, db_name=db_name, output_dir=output_dir, extra_meta=extra_meta, entry_extra=entry_extra)
        except Exception:
            return None

def _env_bool(key: str, default: str = "1") -> bool:
    try:
        return str(os.getenv(key, default)).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default == "1"


def _filename_debug_log(location: str, **data: Any) -> None:
    emit_file_name_debug(component="download", location=location, data=data, logger=logger)

def _trace_filename_resolution(file_meta: Optional[Dict[str, Any]], *, worker_id: int, url: str, stage: str, response_filename: Any, selected_filename: Any) -> None:
    """Log a filename decision only when response and source metadata differ."""
    if not isinstance(file_meta, dict):
        return
    source_filename = str(
        file_meta.get("attachment_name")
        or file_meta.get("original_name")
        or file_meta.get("name")
        or ""
    ).strip()
    response_name = str(response_filename or "").strip()
    selected_name = str(selected_filename or "").strip()
    if not source_filename or (source_filename == response_name == selected_name):
        return
    logger.info(
        "[DownloadTrace][filename_resolution] job_id=%s worker=%s stage=%s source_filename=%s response_filename=%s selected_filename=%s url=%s",
        file_meta.get("job_id"), worker_id, stage, _short(source_filename, 180), _short(response_name, 180), _short(selected_name, 180), _short(url, 220),
    )


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return float(default)


def _format_exception_for_log(exc: BaseException) -> str:
    try:
        msg = str(exc or "").strip()
    except Exception:
        msg = ""
    try:
        exc_type = type(exc).__name__
    except Exception:
        exc_type = "Exception"
    if msg:
        return f"{exc_type}: {msg}"
    try:
        rep = repr(exc)
    except Exception:
        rep = ""
    if rep and rep != msg:
        return f"{exc_type}: {rep}"
    return exc_type

# Download path debug log (default OFF)
# - 1?대㈃: server_domain/domain/chat_bot_id/db_name 諛?怨꾩궛??download_dir/filepath瑜?濡쒓렇濡?異쒕젰
DOWNLOAD_PATH_DEBUG = _env_bool("DOWNLOAD_PATH_DEBUG", "0")
# HTML fallback body preview log (default OFF)
DOWNLOAD_HTML_FALLBACK_BODY_LOG = _env_bool("DOWNLOAD_HTML_FALLBACK_BODY_LOG", "0")
# Document metadata extraction toggle and timeout
DOCUMENT_META_ENABLED = _env_bool("DOCUMENT_META_ENABLED", "1")
DOCUMENT_META_TIMEOUT_SEC = max(0.1, min(_env_float("DOCUMENT_META_TIMEOUT_SEC", 2.5), 30.0))


def _download_http_stream_chunk_size() -> int:
    try:
        value = int(os.getenv("DOWNLOAD_HTTP_STREAM_CHUNK_SIZE", str(256 * 1024)) or str(256 * 1024))
    except Exception:
        value = 256 * 1024
    return max(8 * 1024, min(value, 4 * 1024 * 1024))


def _declared_file_size_bytes(file_meta: Dict[str, Any]) -> int:
    """Return the best pre-download attachment size from extractor metadata."""
    original_meta = file_meta.get("original_meta") if isinstance(file_meta.get("original_meta"), dict) else {}
    candidates = (
        file_meta.get("declared_file_size_bytes"),
        original_meta.get("declared_file_size_bytes"),
        parse_display_file_size_bytes(file_meta.get("name")),
        parse_display_file_size_bytes(file_meta.get("subject")),
        parse_display_file_size_bytes(original_meta.get("attachment_name")),
        parse_display_file_size_bytes(original_meta.get("name")),
    )
    return max((int(size) for size in candidates if size), default=0)


def _large_file_threshold_bytes() -> int:
    threshold_mb = max(1.0, min(_env_float("DOWNLOAD_LARGE_FILE_THRESHOLD_MB", 20.0), 1024.0))
    return int(threshold_mb * 1024 * 1024)


def _download_stream_stall_timeout_sec() -> float:
    """Return the no-progress timeout, distinct from the item total timeout."""
    return max(5.0, min(_env_float("DOWNLOAD_STREAM_STALL_TIMEOUT_SEC", 30.0), 180.0))


class _DownloadStreamFailure(Exception):
    def __init__(self, cause: BaseException, *, bytes_written: int, last_progress_at: float) -> None:
        super().__init__(str(cause or ""))
        self.cause = cause
        self.bytes_written = max(0, int(bytes_written or 0))
        self.last_progress_at = float(last_progress_at or time.monotonic())


def _should_defer_response_to_large_lane(
    file_meta: Dict[str, Any],
    content_length: int,
    *,
    worker_lane: str,
    large_queue_available: bool,
) -> bool:
    return bool(
        worker_lane == "normal"
        and large_queue_available
        and not file_meta.get("_large_lane_requeued")
        and int(content_length or 0) >= _large_file_threshold_bytes()
    )

def _download_http_request_timeout(file_meta: Dict[str, Any], base_timeout_sec: float) -> aiohttp.ClientTimeout:
    """Use a no-progress read timeout while the outer item deadline caps total time."""
    base_timeout = max(1.0, min(float(base_timeout_sec), 300.0))
    return aiohttp.ClientTimeout(
        total=None,
        connect=base_timeout,
        sock_connect=base_timeout,
        sock_read=_download_stream_stall_timeout_sec(),
    )


def _download_item_hard_timeout_sec(file_meta: Dict[str, Any]) -> float:
    """Bound the entire download item, including domain-lock waits and fallbacks."""
    base_timeout = max(30.0, min(_env_float("DOWNLOAD_ITEM_HARD_TIMEOUT_SEC", 90.0), 900.0))
    declared_size = _declared_file_size_bytes(file_meta)
    if declared_size < _large_file_threshold_bytes():
        return base_timeout
    return max(
        base_timeout,
        min(_env_float("DOWNLOAD_ITEM_LARGE_HARD_TIMEOUT_SEC", 300.0), 1800.0),
    )

async def _stream_http_response_to_file(
    response: Any,
    filepath: str,
    *,
    chunk_size: Optional[int] = None,
    sniff_bytes: int = 2048,
) -> tuple[int, bytes]:
    total = 0
    head = bytearray()
    stream_chunk_size = chunk_size or _download_http_stream_chunk_size()

    last_progress_at = time.monotonic()
    try:
        with open(filepath, "wb") as fh:
            async for chunk in response.content.iter_chunked(stream_chunk_size):
                if not chunk:
                    continue
                if len(head) < sniff_bytes:
                    remain = sniff_bytes - len(head)
                    head.extend(chunk[:remain])
                fh.write(chunk)
                total += len(chunk)
                last_progress_at = time.monotonic()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise _DownloadStreamFailure(
            exc,
            bytes_written=total,
            last_progress_at=last_progress_at,
        ) from exc

    return total, bytes(head)


def _playwright_expect_timeout_ms() -> int:
    """Common timeout for expect_download and direct file URL goto. Defaults to 90s."""
    try:
        v = int(os.getenv("DOWNLOAD_PLAYWRIGHT_EXPECT_TIMEOUT_MS", "90000") or "90000")
    except Exception:
        v = 90000
    return max(3000, min(int(v), 300_000))


def _playwright_expect_timeout_click_ms() -> int:
    """Timeout for link-click download wait. Falls back to expect timeout if unset."""
    raw = (os.getenv("DOWNLOAD_PLAYWRIGHT_EXPECT_TIMEOUT_CLICK_MS") or "").strip()
    if raw:
        try:
            v = int(raw)
            return max(3000, min(int(v), 300_000))
        except Exception:
            pass
    return _playwright_expect_timeout_ms()


def _portal_direct_fail_fast_enabled() -> bool:
    return str(os.getenv("DOWNLOAD_PORTAL_DIRECT_FAIL_FAST", "1")).strip().lower() in ("1", "true", "yes", "on")


def _portal_direct_expect_timeout_ms() -> int:
    try:
        v = int(os.getenv("DOWNLOAD_PORTAL_DIRECT_EXPECT_TIMEOUT_MS", "30000") or "30000")
    except Exception:
        v = 30000
    return max(3000, min(int(v), 120000))


def _portal_direct_request_timeout_ms() -> int:
    try:
        v = int(os.getenv("DOWNLOAD_PORTAL_DIRECT_REQUEST_TIMEOUT_MS", "90000") or "90000")
    except Exception:
        v = 90000
    return max(5000, min(int(v), 300000))


def _source_extract_request_timeout_ms() -> int:
    try:
        v = int(os.getenv("DOWNLOAD_PLAYWRIGHT_SOURCE_EXTRACT_TIMEOUT_MS", "90000") or "90000")
    except Exception:
        v = 90000
    return max(5000, min(int(v), 300000))


def _portal_direct_http_timeout_sec() -> float:
    try:
        v = float(os.getenv("DOWNLOAD_PORTAL_DIRECT_HTTP_TIMEOUT_SEC", "30") or "30")
    except Exception:
        v = 30.0
    return max(5.0, min(float(v), 60.0))


def _download_not_found_cache_ttl_sec() -> float:
    try:
        v = float(os.getenv("DOWNLOAD_NOT_FOUND_CACHE_TTL_SEC", "600") or "600")
    except Exception:
        v = 600.0
    return max(30.0, min(v, 3600.0))


def _is_not_found_download_error(exc: BaseException) -> bool:
    try:
        msg = str(exc or "").strip().lower()
    except Exception:
        msg = ""
    if not msg:
        return False
    return (
        "non-200: 404" in msg
        or "status=404" in msg
        or "404 not found" in msg
        or "찾을 수 없음" in msg
    )


def _is_timeout_download_error(exc: BaseException) -> bool:
    current: Optional[BaseException] = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TimeoutError):
            return True
        try:
            msg = str(current or "").strip().lower()
        except Exception:
            msg = ""
        if any(token in msg for token in (
            "timeout",
            "timed out",
            "download timeout",
            "waiting for event",
        )):
            return True
        current = current.__cause__ or current.__context__
    return False


def _download_transport_failure_reason(
    exc: BaseException,
    *,
    phase: str,
    bytes_written: int = 0,
) -> str:
    """Classify a transport failure without hiding the phase that failed."""
    current: Optional[BaseException] = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (ConnectionResetError, aiohttp.ClientPayloadError)):
            return "connection_reset"
        current = current.__cause__ or current.__context__

    if _is_timeout_download_error(exc):
        if phase == "http_response_headers_wait":
            return "header_timeout"
        if phase == "http_body_stream":
            return "stream_stall_timeout" if int(bytes_written or 0) > 0 else "body_timeout"
        return "connect_timeout"

    try:
        if isinstance(exc, aiohttp.ClientConnectorError):
            return "connect_failed"
    except Exception:
        pass
    return "transport_exception"


def _should_skip_portal_direct_download_event_wait(url: str) -> bool:
    return _is_portal_direct_file_url(url) and _portal_direct_fail_fast_enabled()


def _is_viewer_convert_url(u: str) -> bool:
    try:
        lu = (u or "").lower()
    except Exception:
        return False
    if "niied.go.kr" in lu and "/convert/convert.jsp" in lu and "file_id=" in lu:
        return True
    # LLSollu ezweb is a translated HTML page, never a downloadable attachment.
    return "webtrans.llsollu.com" in lu and ("/ezweb" in lu or "/ezweb/translate" in lu)


def _is_portal_direct_file_url(u: str) -> bool:
    """Public fileDown.do-style URLs are usually faster through context.request than expect_download/goto."""
    try:
        lu = (u or "").lower()
    except Exception:
        return False
    return (
        "filedown.do" in lu
        or "/file/direct/download.do" in lu
        or "filedownaction" in lu
        or "atchfileid=" in lu
        or "atch_file_id=" in lu
        or "filesn=" in lu
    )


def _is_component_direct_download_url(u: str) -> bool:
    """Return whether the URL is a direct component-file attachment endpoint."""
    try:
        path = (urlparse(str(u or "")).path or "").lower()
    except Exception:
        return False
    return path.endswith("/component/file/nd_filedownload.do")

def _is_server_side_direct_download_url(u: str) -> bool:
    """Return whether an opaque server-side route directly streams an attachment."""
    try:
        path = (urlparse(str(u or "")).path or "").lower()
    except Exception:
        return False
    return any(marker in path for marker in ("/afile/fileopen/", "/afile/filedownload/"))
_STATIC_DIRECT_DOCUMENT_EXTENSIONS = frozenset(str(ext or "").lower() for ext in DOC_EXTENSIONS)


def _is_static_direct_document_url(u: str) -> bool:
    """Return whether an HTTP URL path directly names a supported document file."""
    try:
        parsed = urlparse(str(u or ""))
        if parsed.scheme.lower() not in {"http", "https"}:
            return False
        path = unquote(parsed.path or "").rstrip("/").lower()
    except Exception:
        return False
    return any(path.endswith(ext) for ext in _STATIC_DIRECT_DOCUMENT_EXTENSIONS)

def _is_suwon_culture_direct_download_url(u: str) -> bool:
    """Suwon culture files accept a direct request with the detail-page referer."""
    try:
        parsed = urlparse(str(u or ""))
        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        return False
    return host in {"suwon.go.kr", "www.suwon.go.kr"} and path == "/component/file/nd_culturefiledownload.do"


def _is_suwon_component_direct_download_url(u: str) -> bool:
    """Suwon component downloads are direct files but frequently need a longer HTTP retry budget."""
    try:
        parsed = urlparse(str(u or ""))
        host = (parsed.hostname or "").lower()
        path = (parsed.path or "").lower()
    except Exception:
        return False
    return host in {"suwon.go.kr", "www.suwon.go.kr"} and path == "/component/file/nd_filedownload.do"


def _repair_suwon_component_file_download_url(u: str) -> tuple[str, bool]:
    """Repair known extraction typos in Suwon component-file download paths."""

    raw = str(u or "").strip()
    if not raw:
        return raw, False
    try:
        parsed = urlparse(raw)
        host = (parsed.hostname or "").lower()
        if host not in {"suwon.go.kr", "www.suwon.go.kr"}:
            return raw, False
        path = parsed.path or ""
        repaired_path = re.sub(r"(?i)/component/fe(?=/|$)", "/component/file", path)
        repaired_path = re.sub(
            r"(?i)/component/file/ndiledownload\.do$",
            "/component/file/ND_fileDownload.do",
            repaired_path,
        )
        if repaired_path == path:
            return raw, False
        return parsed._replace(path=repaired_path).geturl(), True
    except Exception:
        return raw, False



def _is_eminwon_filedown_url(u: str) -> bool:
    """Seoul e-minwon file download JSP; request GET is often enough after source-page cookies exist."""
    try:
        lu = (u or "").lower()
    except Exception:
        return False
    if "eminwon.seoul.kr" not in lu:
        return False
    return "filedownnew.jsp" in lu or "/emwp/jsp/" in lu


def _is_identity_kisa_url(u: str) -> bool:
    try:
        return "identity.kisa.or.kr" in str(u or "").lower()
    except Exception:
        return False


# Shared user-agent for Playwright new_context and context.request headers
_PLAYWRIGHT_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

_DOWNLOAD_DOMAIN_SEMAPHORES: Dict[Tuple[int, str, str, int], asyncio.Semaphore] = {}


def _host_env_key(host: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", str(host or "").strip().upper()).strip("_")


def _download_transport_slow_log_sec() -> float:
    try:
        value = float(os.getenv("DOWNLOAD_TRANSPORT_SLOW_LOG_SEC", "3") or "3")
    except Exception:
        value = 3.0
    return max(0.5, min(value, 60.0))


def _download_domain_default_concurrency() -> int:
    try:
        value = int(getattr(settings, "DOWNLOAD_WORKERS", 4) or 4)
    except Exception:
        value = 4
    return max(1, min(value, 8))


def _download_domain_concurrency_limit(host: str, strict_hosts: set[str], strict_domain_limit: int) -> int:
    host = str(host or "").strip().lower()
    if not host:
        return _download_domain_default_concurrency()
    if any(host == h or host.endswith("." + h) for h in strict_hosts):
        return max(1, min(int(strict_domain_limit or 1), 4))
    host_key = _host_env_key(host)
    for env_key in (f"DOWNLOAD_DOMAIN_MAX_CONCURRENT_{host_key}", f"FILE_CRAWL_DOMAIN_MAX_CONCURRENT_{host_key}"):
        raw = str(os.getenv(env_key, "") or "").strip()
        if not raw:
            continue
        try:
            return max(1, min(int(raw), 8))
        except Exception:
            continue
    return _download_domain_default_concurrency()


def _get_download_domain_semaphore(
    host: str,
    limit: int,
    *,
    job_id: str = "",
) -> asyncio.Semaphore:
    loop_id = id(asyncio.get_running_loop())
    scope = str(job_id or "__global__").strip() or "__global__"
    key = (loop_id, scope, str(host or "").strip().lower(), int(limit or 1))
    sem = _DOWNLOAD_DOMAIN_SEMAPHORES.get(key)
    if sem is None:
        sem = asyncio.Semaphore(max(1, int(limit or 1)))
        _DOWNLOAD_DOMAIN_SEMAPHORES[key] = sem
    return sem

def _eminwon_pre_goto_delay_sec() -> float:
    """Random delay before visiting e-minwon detail page to avoid timing-pattern failures."""
    raw = (os.getenv("DOWNLOAD_EMINWON_PRE_GOTO_DELAY_SEC") or "1.2,2.8").strip()
    try:
        if "," in raw:
            a, b = raw.split(",", 1)
            lo = float(a.strip())
            hi = float(b.strip())
        else:
            lo = float(raw)
            hi = lo
    except Exception:
        lo, hi = 1.2, 2.8
    lo = max(0.0, min(lo, 30.0))
    hi = max(lo, min(hi, 30.0))
    return random.uniform(lo, hi)


async def _extract_doc_created_at_async(filepath: str) -> Optional[str]:
    if not DOCUMENT_META_ENABLED:
        return None
    try:
        from utils.document_meta_date import extract_document_created_at
    except Exception:
        return None
    try:
        loop = asyncio.get_running_loop()
    except Exception:
        return None
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, extract_document_created_at, filepath),
            timeout=DOCUMENT_META_TIMEOUT_SEC,
        )
    except Exception:
        return None

def _short(s: object, n: int = 180) -> str:
    try:
        text = str(s)
    except Exception:
        return ""
    return text if len(text) <= n else (text[:n] + "...")


def _html_preview_for_log(text: object, n: int = 200) -> str:
    try:
        compact = re.sub(r"\s+", " ", str(text or "")).strip()
    except Exception:
        compact = ""
    return _short(compact, n)


def _file_pipeline_bottleneck_enabled() -> bool:
    return str(os.getenv("FILE_PIPELINE_BOTTLENECK_LOG", "0")).strip().lower() in ("1", "true", "yes", "on")


def _resolve_download_url(raw_url: Any, source_page: str = "") -> str:
    url = str(raw_url or "").strip()
    if not url:
        return ""

    def _finalize(candidate: str) -> str:
        normalized = _normalize_attachment_download_url(candidate)
        return _restore_site_specific_download_url(normalized, source_page)

    try:
        resolved = extract_download_url_from_js(url, source_page or None) or ""
        if not resolved:
            resolved = resolve_anseong_yhlib_download_url(url, source_page or None) or ""
    except Exception:
        resolved = ""
    if resolved and not resolved.lower().startswith("javascript:"):
        return _finalize(resolved.strip())
    if url.lower().startswith("javascript:"):
        match = re.search(r"['\"](https?://[^'\"]+)['\"]", url, re.IGNORECASE)
        if match:
            return _finalize(match.group(1).strip())
    return _finalize(url)


def _is_valid_http_url(raw_url: Any) -> bool:
    text = str(raw_url or "").strip()
    if not text:
        return False
    try:
        parsed = urlparse(text)
    except Exception:
        return False
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)


def _query_param_value(raw_url: Any, key_name: str) -> str:
    try:
        parsed = urlparse(str(raw_url or "").strip())
    except Exception:
        return ""
    target = str(key_name or "").strip().lower()
    for key, value in parse_qsl(parsed.query or "", keep_blank_values=False):
        if str(key or "").strip().lower() == target and str(value or "").strip():
            return str(value or "").strip()
    return ""


def _resolve_source_page_from_file_meta(file_meta: Optional[Dict[str, Any]]) -> str:
    if not isinstance(file_meta, dict):
        return ""
    original_meta = file_meta.get("original_meta") if isinstance(file_meta.get("original_meta"), dict) else {}
    inner_meta = original_meta.get("original_meta") if isinstance(original_meta.get("original_meta"), dict) else {}
    candidates = (
        file_meta.get("source_page"),
        file_meta.get("post_url"),
        file_meta.get("source_url"),
        original_meta.get("source_page"),
        original_meta.get("post_url"),
        original_meta.get("source_url"),
        original_meta.get("board_url"),
        inner_meta.get("source_page"),
        inner_meta.get("post_url"),
        inner_meta.get("source_url"),
        inner_meta.get("board_url"),
    )
    for value in candidates:
        text = str(value or "").strip()
        if text and text.lower().startswith(("http://", "https://")):
            return text
    return ""


def _menu_no_from_file_context(file_meta: Optional[Dict[str, Any]], source_page: str = "") -> str:
    if isinstance(file_meta, dict):
        for key in ("menuNo", "menu_no", "menu_no_value"):
            value = str(file_meta.get(key) or "").strip()
            if value:
                return value
        original_meta = file_meta.get("original_meta") if isinstance(file_meta.get("original_meta"), dict) else {}
        for key in ("menuNo", "menu_no", "menu_no_value"):
            value = str(original_meta.get(key) or "").strip()
            if value:
                return value
        for url_key in ("source_page", "post_url", "source_url", "board_url"):
            value = _query_param_value(file_meta.get(url_key), "menuNo")
            if value:
                return value
            value = _query_param_value(original_meta.get(url_key), "menuNo")
            if value:
                return value
    return _query_param_value(source_page, "menuNo")


def _repair_portal_file_down_url_context(url: str, source_page: str, file_meta: Optional[Dict[str, Any]]) -> tuple[str, bool]:
    raw = str(url or "").strip()
    if not raw:
        return "", False
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw, False
    path_lower = (parsed.path or "").lower()
    if path_lower != "/portal/cmmn/file/filedown.do":
        return raw, False

    pairs = parse_qsl(parsed.query or "", keep_blank_values=True)
    lower_params = {str(k or "").strip().lower(): str(v or "").strip() for k, v in pairs}
    if lower_params.get("menuno"):
        return raw, False
    if not (lower_params.get("atchfileid") and lower_params.get("filesn")):
        return raw, False

    menu_no = _menu_no_from_file_context(file_meta, source_page)
    cleaned_pairs = [(k, v) for k, v in pairs if str(k or "").strip().lower() != "menuno"]
    if not menu_no:
        rebuilt = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(cleaned_pairs, doseq=True), ""))
        return rebuilt, True

    rebuilt_pairs = [("menuNo", menu_no)] + cleaned_pairs
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urlencode(rebuilt_pairs, doseq=True), "")), False


def _cookie_domains_for_download(file_meta: Optional[Dict[str, Any]], url: str, source_page: str) -> list[str]:
    raw_candidates = [
        url,
        source_page,
        (file_meta or {}).get("access_url") if isinstance(file_meta, dict) else None,
        (file_meta or {}).get("server_domain") if isinstance(file_meta, dict) else None,
        (file_meta or {}).get("domain") if isinstance(file_meta, dict) else None,
    ]
    domains: list[str] = []
    seen: set[str] = set()
    for raw in raw_candidates:
        text = str(raw or "").strip()
        if not text:
            continue
        if not text.startswith(("http://", "https://")):
            text = "https://" + text.lstrip("/")
        try:
            host = (urlparse(text).hostname or "").strip().lower()
        except Exception:
            host = ""
        if not host or host in seen:
            continue
        seen.add(host)
        domains.append(host)
    return domains


def _request_cookies_from_file_meta(file_meta: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not isinstance(file_meta, dict):
        return {}
    candidates = [
        file_meta.get("request_cookies"),
        file_meta.get("cookies"),
        file_meta.get("_request_cookies"),
    ]
    original_meta = file_meta.get("original_meta")
    if isinstance(original_meta, dict):
        candidates.extend(
            [
                original_meta.get("request_cookies"),
                original_meta.get("cookies"),
                original_meta.get("_request_cookies"),
            ]
        )
    out: Dict[str, str] = {}
    for raw in candidates:
        if not isinstance(raw, dict):
            continue
        for key, value in raw.items():
            name = str(key or "").strip()
            val = str(value or "").strip()
            if name and val:
                out[name] = val
    return out


def _cookie_header_from_file_meta(file_meta: Optional[Dict[str, Any]]) -> str:
    cookies = _request_cookies_from_file_meta(file_meta)
    if not cookies:
        return ""
    return "; ".join(f"{name}={value}" for name, value in cookies.items())


def _merge_request_cookies_into_file_meta(file_meta: Optional[Dict[str, Any]], cookies: Dict[str, str]) -> None:
    if not isinstance(file_meta, dict) or not cookies:
        return
    existing = _request_cookies_from_file_meta(file_meta)
    existing.update({str(k): str(v) for k, v in cookies.items() if str(k or "").strip() and str(v or "").strip()})
    if existing:
        file_meta["_request_cookies"] = existing


def _cookie_header_from_aiohttp_session(session: aiohttp.ClientSession, *urls: str) -> str:
    pairs: Dict[str, str] = {}
    try:
        from yarl import URL  # type: ignore
    except Exception:
        URL = None  # type: ignore
    for raw in urls:
        target = str(raw or "").strip()
        if not target or not target.startswith(("http://", "https://")):
            continue
        try:
            cookie_map = session.cookie_jar.filter_cookies(URL(target) if URL else target)
        except Exception:
            continue
        try:
            for name, morsel in cookie_map.items():
                value = getattr(morsel, "value", None)
                if value is None:
                    value = str(morsel)
                if str(name or "").strip() and str(value or "").strip():
                    pairs[str(name)] = str(value)
        except Exception:
            continue
    return "; ".join(f"{name}={value}" for name, value in pairs.items())


def _cookies_from_header(cookie_header: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in str(cookie_header or "").split(";"):
        if "=" not in part:
            continue
        name, value = part.split("=", 1)
        name = name.strip()
        value = value.strip()
        if name and value:
            out[name] = value
    return out


def _manual_download_cookies(url: str = "", source_page: str = "") -> Dict[str, str]:
    raw = (os.getenv("DOWNLOAD_COOKIE_HEADER") or "").strip()
    if not raw:
        return {}
    domains_raw = (os.getenv("DOWNLOAD_COOKIE_DOMAINS") or "").strip()
    if domains_raw:
        domains = {d.strip().lower() for d in domains_raw.split(",") if d.strip()}
        hosts = set()
        for candidate in (url, source_page):
            try:
                host = (urlparse(str(candidate or "")).hostname or "").lower()
            except Exception:
                host = ""
            if host:
                hosts.add(host)
        if domains and not any(host == domain or host.endswith("." + domain) for host in hosts for domain in domains):
            return {}
    return _cookies_from_header(raw)


def _merge_cookie_headers(*headers: str) -> str:
    merged: Dict[str, str] = {}
    for header in headers:
        merged.update(_cookies_from_header(header))
    return "; ".join(f"{name}={value}" for name, value in merged.items())


async def _inject_playwright_context_cookies(
    context: Any,
    file_meta: Optional[Dict[str, Any]],
    *,
    url: str,
    source_page: str,
    worker_id: Optional[int] = None,
) -> int:
    cookies = _request_cookies_from_file_meta(file_meta)
    cookies.update(_manual_download_cookies(url, source_page))
    if not cookies:
        return 0
    domains = _cookie_domains_for_download(file_meta, url, source_page)
    if not domains:
        return 0
    payload = []
    for domain in domains:
        for name, value in cookies.items():
            payload.append(
                {
                    "name": name,
                    "value": value,
                    "domain": domain,
                    "path": "/",
                    "httpOnly": False,
                    "secure": True,
                    "sameSite": "Lax",
                }
            )
    if not payload:
        return 0
    add_cookies = getattr(context, "add_cookies", None)
    if not callable(add_cookies):
        return 0
    await add_cookies(payload)
    try:
        logger.info(
            "[Download][Worker %s] Playwright cookies injected | domains=%s count=%s",
            worker_id,
            domains,
            len(payload),
        )
    except Exception:
        pass
    return len(payload)


def _strip_javascript_prefix(raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text.lower().startswith("javascript:"):
        return text
    return text[len("javascript:"):].strip()


def _extract_javascript_handler_name(raw_url: Any) -> str:
    js_code = _strip_javascript_prefix(raw_url)
    if not js_code:
        return ""
    normalized = re.sub(r"^\s*return\b", "", js_code, flags=re.IGNORECASE).strip()
    normalized = re.sub(r"^\s*(?:void\s*\(\s*0\s*\)\s*;?\s*)+", "", normalized, flags=re.IGNORECASE)
    match = re.search(
        r"(?:^|[;\s])(?:window\.|self\.|top\.|parent\.)*([A-Za-z_$][\w$]*)\s*\(",
        normalized,
    )
    return (match.group(1) or "").strip() if match else ""



async def _evaluate_javascript_url(page: Any, raw_url: Any) -> bool:
    js_code = _strip_javascript_prefix(raw_url)
    if not js_code:
        return False

    handler_name = _extract_javascript_handler_name(raw_url)
    try:
        return bool(
            await page.evaluate(
                """({ code, handlerName }) => {
                    const scope = typeof window !== "undefined" ? window : globalThis;
                    if (handlerName) {
                        const fn =
                            (scope && typeof scope[handlerName] === "function" && scope[handlerName]) ||
                            (typeof globalThis !== "undefined" &&
                                typeof globalThis[handlerName] === "function" &&
                                globalThis[handlerName]) ||
                            null;
                        if (typeof fn !== "function") {
                            return false;
                        }
                    }
                    try {
                        scope.eval(code);
                        return true;
                    } catch (err) {
                        if (err && (err.name === "ReferenceError" || /is not defined/i.test(String(err)))) {
                            return false;
                        }
                        throw err;
                    }
                }""",
                {"code": js_code, "handlerName": handler_name or None},
            )
        )
    except Exception:
        raise


def _choose_playwright_navigation_target(url: str, source_page: str = "") -> Tuple[str, bool]:
    """
    Decide whether Playwright should navigate first or execute javascript directly.

    If a javascript download handler came from a detail page, we must load the
    detail page first and look for a matching clickable element in that DOM.
    """
    uu = str(url or "").strip()
    sp = str(source_page or "").strip()
    if uu.lower().startswith("javascript:"):
        if sp and not sp.lower().startswith("javascript:"):
            return sp, False
        return "", True

    if sp and uu:
        sp0 = sp.split("#", 1)[0].rstrip()
        uu0 = uu.split("#", 1)[0].rstrip()
        return (sp0 if sp0 != uu0 else uu0), False

    return (uu or sp).strip(), False


def _can_direct_navigate_download_url(url: str) -> bool:
    return not str(url or "").strip().lower().startswith("javascript:")


def _normalize_attachment_download_url(url: str) -> str:
    """
    Rewrite preview-style attachment URLs to the direct FileDown endpoint when
    atchFileId/fileSn are already present.
    """
    raw = str(url or "").strip()
    if not raw:
        return ""
    raw = re.sub(r";jsessionid=[^/?#]+", "", raw, flags=re.IGNORECASE)
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw

    path_lower = (parsed.path or "").lower()
    host_lower = (parsed.netloc or "").lower()
    should_rewrite = (
        "converttohtml.do" in path_lower
    )
    if not should_rewrite:
        return raw

    params = {
        (key or "").strip().lower(): (value or "").strip()
        for key, value in parse_qsl(parsed.query or "", keep_blank_values=False)
        if (key or "").strip() and (value or "").strip()
    }
    atch_file_id = params.get("atchfileid")
    file_sn = params.get("filesn")
    if not atch_file_id or not file_sn:
        return raw

    return urlunparse(
        (
            parsed.scheme or "https",
            parsed.netloc,
            "/cmm/fms/FileDown.do",
            "",
            urlencode([("atchFileId", atch_file_id), ("fileSn", file_sn)], doseq=True),
            "",
        )
    )


def _restore_site_specific_download_url(url: str, source_page: str = "") -> str:
    """
    Some portal boards expose a site-scoped fileDown endpoint whose parameters
    are not accepted by the global /cmm/fms/FileDown.do endpoint. Recover the
    original portal endpoint for already-normalized stale queue items.
    """
    raw = str(url or "").strip()
    src = str(source_page or "").strip()
    if not raw or not src:
        return raw
    try:
        parsed = urlparse(raw)
        src_parsed = urlparse(src)
    except Exception:
        return raw

    host = (parsed.netloc or "").lower()
    src_host = (src_parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    src_path = (src_parsed.path or "").lower()
    if not (
        host.endswith("gwangjin.go.kr")
        and src_host.endswith("gwangjin.go.kr")
        and path == "/cmm/fms/filedown.do"
        and src_path.startswith("/portal/")
    ):
        return raw

    params = {
        (key or "").strip().lower(): (value or "").strip()
        for key, value in parse_qsl(parsed.query or "", keep_blank_values=False)
        if (key or "").strip() and (value or "").strip()
    }
    source_params = {
        (key or "").strip().lower(): (value or "").strip()
        for key, value in parse_qsl(src_parsed.query or "", keep_blank_values=False)
        if (key or "").strip() and (value or "").strip()
    }
    atch_file_id = params.get("atchfileid")
    file_sn = params.get("filesn")
    if not atch_file_id or not file_sn:
        return raw

    query_pairs = []
    menu_no = source_params.get("menuno")
    if menu_no:
        query_pairs.append(("menuNo", menu_no))
    query_pairs.extend(
        [
            ("atchFileId", atch_file_id),
            ("fileSn", file_sn),
        ]
    )
    restored = urlunparse(
        (
            parsed.scheme or src_parsed.scheme or "https",
            parsed.netloc or src_parsed.netloc,
            "/portal/cmmn/file/fileDown.do",
            "",
            urlencode(query_pairs, doseq=True),
            "",
        )
    )
    if restored != raw:
        logger.info(
            "[Download] restored site-specific fileDown url | source=%s from=%s to=%s",
            _short(src, 180),
            _short(raw, 180),
            _short(restored, 180),
        )
    return restored


def _attachment_identity_tokens(value: Any, base_url: str = "") -> set[str]:
    raw = str(value or "").strip()
    if not raw:
        return set()

    out: set[str] = set()

    def _add_token(token: Any) -> None:
        text = str(token or "").strip().lower()
        if len(text) >= 6:
            out.add(text)

    try:
        for token in extract_attachment_key_candidates(raw):
            _add_token(token)
    except Exception:
        pass
    try:
        for token in extract_anseong_attachment_key_candidates(raw, base_url):
            _add_token(token)
    except Exception:
        pass

    try:
        resolved = extract_download_url_from_js(raw, base_url or None) or ""
    except Exception:
        resolved = ""
    if resolved and resolved != raw:
        try:
            for token in extract_attachment_key_candidates(resolved):
                _add_token(token)
        except Exception:
            pass

    raw_lower = raw.lower()
    if raw_lower.startswith("javascript:") or "preview(" in raw_lower or "filedown" in raw_lower:
        for match in re.finditer(r"['\"]([^'\"]{6,})['\"]", raw):
            candidate = (match.group(1) or "").strip()
            candidate_lower = candidate.lower()
            if (
                len(candidate_lower) >= 16
                and not any(ch in candidate_lower for ch in ("/", "\\", "?", "&", "="))
            ):
                _add_token(candidate_lower)

    return out


def _build_request_headers(
    base_headers: Dict[str, str],
    *,
    source_page: str = "",
    referer_override: str = "",
    include_origin: bool = True,
) -> Dict[str, str]:
    req_headers = dict(base_headers or {})
    referer_src = str(referer_override or "").strip() or str(source_page or "").strip()
    if referer_src:
        req_headers["Referer"] = referer_src
        if include_origin:
            try:
                p = urlparse(referer_src)
                if p.scheme and p.netloc:
                    req_headers["Origin"] = f"{p.scheme}://{p.netloc}"
            except Exception:
                pass
        else:
            req_headers.pop("Origin", None)
    return req_headers


def _request_header_variants(
    base_headers: Dict[str, str],
    *,
    source_page: str = "",
    referer_override: str = "",
    prefer_no_origin_first: bool = False,
) -> list[Dict[str, str]]:
    variants: list[Dict[str, str]] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for include_origin in (
        (False, True) if prefer_no_origin_first else (True, False)
    ):
        headers = _build_request_headers(
            base_headers,
            source_page=source_page,
            referer_override=referer_override,
            include_origin=include_origin,
        )
        key = tuple(sorted((str(k), str(v)) for k, v in headers.items()))
        if key in seen:
            continue
        seen.add(key)
        variants.append(headers)
    return variants


def _extract_html_followup_request(html_text: str, base_url: str) -> Optional[Dict[str, Any]]:
    text = str(html_text or "")
    if not text.strip():
        return None

    def _absolute(candidate: str) -> str:
        cand = unescape(str(candidate or "").strip())
        if not cand:
            return ""
        try:
            return urljoin(base_url, cand)
        except Exception:
            return cand

    meta_match = re.search(
        r'<meta[^>]+http-equiv=["\']?refresh["\']?[^>]+content=["\'][^"\']*url=([^"\'>]+)',
        text,
        re.IGNORECASE,
    )
    if meta_match:
        target = _absolute(meta_match.group(1))
        if target:
            return {"method": "GET", "url": target}

    js_match = re.search(
        r"""(?:window\.)?location(?:\.href|\.replace)?\s*(?:=\s*|\(\s*)['"]([^'"]+)['"]""",
        text,
        re.IGNORECASE,
    )
    if js_match:
        target = _absolute(js_match.group(1))
        if target:
            return {"method": "GET", "url": target}

    open_match = re.search(r"""window\.open\s*\(\s*['"]([^'"]+)['"]""", text, re.IGNORECASE)
    if open_match:
        target = _absolute(open_match.group(1))
        if target:
            return {"method": "GET", "url": target}

    form_match = re.search(r"<form\b([^>]*)>", text, re.IGNORECASE | re.DOTALL)
    if form_match:
        form_attrs = form_match.group(1) or ""
        action_match = re.search(r"""action=["']([^"']+)["']""", form_attrs, re.IGNORECASE)
        method_match = re.search(r"""method=["']([^"']+)["']""", form_attrs, re.IGNORECASE)
        action = _absolute(action_match.group(1) if action_match else "")
        method = (method_match.group(1) if method_match else "GET").strip().upper()
        inputs = re.findall(
            r"<input[^>]*name=[\"']([^\"']+)[\"'][^>]*value=[\"']([^\"']*)[\"'][^>]*>",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        data = {
            unescape(str(name or "").strip()): unescape(str(value or "").strip())
            for name, value in inputs
            if str(name or "").strip()
        }
        if action:
            return {"method": method or "GET", "url": action, "data": data}

    return None


def _looks_like_download_url(candidate: Any) -> bool:
    text = str(candidate or "").strip()
    if not text or text.startswith("#"):
        return False
    lowered = text.lower()
    if lowered.startswith(("mailto:", "tel:", "sms:")):
        return False
    if re.search(r"\.(?:hwp|hwpx|pdf|docx?|xlsx?|pptx?|zip|txt|csv)(?:[?#;&]|$)", lowered):
        return True
    download_hints = (
        "filedown",
        "filedownload",
        "download",
        "downfile",
        "down_item",
        "attach",
        "atch",
        "/cmm/fms/",
        "/cmmn/file/",
        "/file/",
    )
    param_hints = (
        "atchfileid=",
        "filesn=",
        "file_sn=",
        "fileid=",
        "file_id=",
        "file_path=",
        "sys_file_nm=",
        "stre_file_nm=",
        "orignl_file_nm=",
        "file_name=",
        "filename=",
    )
    return any(h in lowered for h in download_hints) and (
        "?" in lowered or any(p in lowered for p in param_hints)
    ) or any(p in lowered for p in param_hints)


def _extract_html_download_url(html_text: str, base_url: str, target_url: str = "") -> str:
    """Find a likely direct attachment URL from fallback HTML."""
    text = str(html_text or "")
    if not text.strip():
        return ""

    def _absolute(candidate: Any) -> str:
        cand = unescape(str(candidate or "").strip())
        if not cand:
            return ""
        try:
            return _normalize_attachment_download_url(urljoin(base_url, cand))
        except Exception:
            return _normalize_attachment_download_url(cand)

    raw_candidates: list[str] = []
    # Common attributes from anchors, buttons, forms, and data-* helpers.
    for attr in ("href", "action", "src", "data-url", "data-href", "data-download-url"):
        for match in re.finditer(
            rf"""{attr}\s*=\s*["']([^"']+)["']""",
            text,
            flags=re.IGNORECASE,
        ):
            raw_candidates.append(match.group(1))

    # JavaScript snippets: location.href='...', window.open('...'), fn('/fileDown.do?...')
    for pattern in (
        r"""(?:window\.)?location(?:\.href|\.replace)?\s*(?:=\s*|\(\s*)['"]([^'"]+)['"]""",
        r"""window\.open\s*\(\s*['"]([^'"]+)['"]""",
        r"""['"]([^'"]*(?:filedown|filedownload|download|downfile|atchfileid|filesn|sys_file_nm|file_path)[^'"]*)['"]""",
    ):
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            raw_candidates.append(match.group(1))

    candidates: list[str] = []
    seen: set[str] = set()
    for raw in raw_candidates:
        abs_url = _absolute(raw)
        if not abs_url or abs_url in seen:
            continue
        if not _looks_like_download_url(abs_url):
            continue
        seen.add(abs_url)
        candidates.append(abs_url)

    if not candidates:
        return ""

    target_tokens = _attachment_identity_tokens(target_url, base_url)
    if target_tokens:
        for candidate in candidates:
            candidate_tokens = _attachment_identity_tokens(candidate, base_url)
            if target_tokens & candidate_tokens:
                return candidate

    return candidates[0]


async def _prime_source_page_http_session(
    session: aiohttp.ClientSession,
    *,
    source_page: str,
    headers: Dict[str, str],
    worker_id: int,
    primed_source_pages: set[str],
) -> None:
    source_page = str(source_page or "").strip()
    if not source_page or source_page in primed_source_pages:
        return
    try:
        timeout_sec = float(os.getenv("DOWNLOAD_HTTP_SOURCE_PREWARM_TIMEOUT_SEC", "12") or "12")
    except Exception:
        timeout_sec = 12.0
    timeout_sec = max(3.0, min(timeout_sec, 30.0))
    try:
        async with session.get(
            source_page,
            timeout=timeout_sec,
            allow_redirects=True,
            headers=headers,
        ) as resp:
            await resp.read()
        primed_source_pages.add(source_page)
        logger.info(
            "[Download][Worker %s] source_page primed | source=%s",
            worker_id,
            _short(source_page, 200),
        )
    except Exception as exc:
        logger.debug(
            "[Download][Worker %s] source_page prewarm miss (continue) | source=%s err=%s",
            worker_id,
            _short(source_page, 200),
            _short(exc, 200),
        )


def _looks_like_access_denied_html(text: str) -> bool:
    low = str(text or "").lower()
    markers = (
        "login",
        "permission",
        "access denied",
        "unauthorized",
        "forbidden",
        "invalid access",
        "session",
        "로그인",
        "권한",
        "잘못된 접근",
        "잘못된 요청",
        "접근권한",
        "인증",
        "세션",
        "login",
        "permission",
        "access denied",
        "unauthorized",
        "forbidden",
        "invalid access",
    )
    return any(str(marker).lower() in low for marker in markers)


def _progress_queue_file_saved_payload(
    file_meta: Optional[Dict[str, Any]],
    file_info: Dict[str, Any],
    *,
    event_type: str = "file_saved",
) -> Dict[str, Any]:
    """
    MultiplexProgressQueue routes by top-level job_id.
    Keep job_id both at the payload level and inside file_info so file_saved
    events are not dropped before save/study processing.
    """
    jid: Optional[str] = None
    if isinstance(file_meta, dict):
        raw = file_meta.get("job_id")
        if raw is not None and str(raw).strip():
            jid = str(raw).strip()
    if not jid and isinstance(file_info, dict):
        raw2 = file_info.get("job_id")
        if raw2 is not None and str(raw2).strip():
            jid = str(raw2).strip()
    if jid and isinstance(file_info, dict) and not (str(file_info.get("job_id") or "").strip()):
        file_info = dict(file_info)
        file_info["job_id"] = jid
    payload: Dict[str, Any] = {
        "type": event_type,
        "job_id": jid,
        "file_info": file_info,
    }
    if not jid:
        logger.warning(
            "[Bottleneck][download_progress] file_saved without top-level job_id "
            "(GlobalPool drops unless ContextVar set) | url=%s path=%s",
            _short((file_info or {}).get("url"), 200),
            _short((file_info or {}).get("file_path") or (file_info or {}).get("local_path"), 160),
        )
    if _file_pipeline_bottleneck_enabled():
        defer = bool(
            isinstance(file_meta, dict) and file_meta.get("defer_save_batch_until_learn_list")
        )
        logger.info(
            "[Bottleneck][download_progress] enqueue file_saved | job_id=%s defer_save_batch=%s url=%s",
            jid,
            defer,
            _short((file_info or {}).get("url"), 180),
        )
    return payload




def _defer_file_local_postprocess(file_meta: Optional[Dict[str, Any]]) -> bool:
    """Move local-only post-download work out of file download workers."""
    if not isinstance(file_meta, dict) or not file_meta.get("defer_save_batch_until_learn_list"):
        return False
    return _env_bool("FILE_CRAWL_DEFER_LOCAL_POSTPROCESS", "1")


def _progress_queue_websync_failed_payload(
    file_meta: Optional[Dict[str, Any]],
    *,
    url: str,
    filepath: str,
    worker_id: int,
) -> Dict[str, Any]:
    source_page = ""
    name = ""
    public_url = ""
    public_status = ""
    detail_parts: list[str] = []
    if isinstance(file_meta, dict):
        source_page = _resolve_source_page_from_file_meta(file_meta)
        name = str(file_meta.get("name") or file_meta.get("subject") or "").strip()
        public_url = str(file_meta.get("_websync_public_url") or "").strip()
        public_status = str(file_meta.get("_websync_public_status") or "").strip()
        public_error = str(file_meta.get("_websync_public_error") or "").strip()
        if public_status:
            detail_parts.append(f"public_status={public_status}")
        if public_error:
            detail_parts.append(f"public_error={public_error}")
        if public_url:
            detail_parts.append(f"public_url={public_url}")
    detail_parts.append(f"local_path={filepath}")
    return {
        "type": "download_skipped",
        "job_id": str((file_meta or {}).get("job_id") or "").strip() or None,
        "url": url,
        "reason": "websync_failed",
        "detail": " | ".join(detail_parts),
        "source_page": source_page,
        "source_url": source_page,
        "file_url": url,
        "name": name,
        "filename": os.path.basename(filepath or "") or name,
        "local_path": filepath,
        "public_url": public_url,
        "public_status": public_status,
        "worker_id": worker_id,
    }
def _write_bytes(filepath: str, data: bytes) -> None:
    """Synchronous file write helper used through asyncio.to_thread."""
    # Ensure parent directory exists
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    with open(filepath, "wb") as fh:
        fh.write(data)


def _download_temp_path(filepath: str) -> str:
    return f"{filepath}.part-{uuid4().hex}"


def _download_temp_ready_timeout_sec() -> float:
    try:
        value = float(os.getenv("DOWNLOAD_TEMP_READY_TIMEOUT_SEC", "60") or "60")
    except Exception:
        value = 60.0
    return max(10.0, min(value, 300.0))


def _download_final_ready_timeout_sec() -> float:
    try:
        value = float(os.getenv("DOWNLOAD_FINAL_READY_TIMEOUT_SEC", "60") or "60")
    except Exception:
        value = 60.0
    return max(10.0, min(value, 300.0))


def _strip_partial_download_suffix(filename: str) -> str:
    text = str(filename or "").strip()
    if not text:
        return text
    low = text.lower()
    for marker in (".crdownload", ".download"):
        idx = low.find(marker)
        if idx > 0:
            return text[:idx]
    part_idx = low.find(".part")
    if part_idx > 0:
        # Treat .part as an incomplete download marker, not as a real extension.
        return text[:part_idx]
    return text


def _remove_file_quietly(filepath: str) -> None:
    try:
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
    except Exception:
        pass


async def _cleanup_active_download_temp_file(file_meta: Any, *, reason: str, worker_id: Any) -> None:
    """Remove only the tracked per-attempt .part file after a terminal failure."""
    if not isinstance(file_meta, dict):
        return
    filepath = str(file_meta.pop("_active_download_temp_filepath", "") or "").strip()
    if not filepath:
        return
    existed = False
    try:
        existed = await asyncio.to_thread(os.path.exists, filepath)
        if existed:
            await asyncio.to_thread(_remove_file_quietly, filepath)
        logger.info(
            "[DownloadTrace][temp_cleanup] job_id=%s worker=%s reason=%s existed=%s path=%s",
            file_meta.get("job_id"), worker_id, reason, existed, _short(filepath, 260),
        )
    except Exception as exc:
        logger.warning(
            "[DownloadTrace][temp_cleanup_failed] job_id=%s worker=%s reason=%s path=%s err=%s",
            file_meta.get("job_id"), worker_id, reason, _short(filepath, 260), exc,
        )


def _replace_file(src: str, dst: str) -> None:
    dirpath = os.path.dirname(dst)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    os.replace(src, dst)


def _expected_content_length(headers: Any) -> Optional[int]:
    try:
        encoding = str(headers.get("content-encoding") or "").strip()
    except Exception:
        encoding = ""
    if encoding:
        return None
    try:
        raw = str(headers.get("content-length") or "").strip()
        if not raw:
            return None
        value = int(raw)
    except Exception:
        return None
    return value if value > 0 else None


def _looks_like_html_payload(head: bytes) -> bool:
    prefix = bytes(head or b"")[:2048].lstrip().lower()
    return (
        prefix.startswith(b"<!doctype html")
        or prefix.startswith(b"<html")
        or prefix.startswith(b"<script")
        or b"<html" in prefix[:512]
    )


def _read_file_head(filepath: str, limit: int = 2048) -> bytes:
    try:
        with open(filepath, "rb") as fh:
            return fh.read(limit)
    except Exception:
        return b""


_HWP_OLE_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


def _looks_like_hwp_payload(head: bytes) -> bool:
    return bytes(head or b"")[:16].startswith(_HWP_OLE_MAGIC)


def _looks_like_zip_payload(head: bytes) -> bool:
    return bytes(head or b"")[:4].startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"))


_NON_DOCUMENT_ROUTE_EXTENSIONS = {
    ".do",
    ".jsp",
    ".php",
    ".asp",
    ".aspx",
    ".action",
    ".html",
    ".htm",
}


def _raw_download_file_extension(*, filename: str = "", filepath: str = "", url: str = "") -> str:
    for value in (filename, filepath, urlparse(url or "").path):
        ext = os.path.splitext(str(value or "").split("?", 1)[0])[1].lower()
        if ext:
            return ext
    return ""


def _download_file_extension(*, filename: str = "", filepath: str = "", url: str = "") -> str:
    ext = _raw_download_file_extension(filename=filename, filepath=filepath, url=url)
    if ext in _NON_DOCUMENT_ROUTE_EXTENSIONS:
        return ""
    return ext


def _is_weak_download_filename(filename: Any) -> bool:
    text = str(filename or "").strip()
    if not text or text.lower() in {"unknown", "download", "downloaded_file", "file"}:
        return True
    base = os.path.basename(text)
    stem, ext = os.path.splitext(base)
    ext = (ext or "").lower()
    if ext in _NON_DOCUMENT_ROUTE_EXTENSIONS:
        return True
    if re.fullmatch(r"[0-9a-f]{24,}", stem.lower()):
        return True
    if re.fullmatch(r"file_[0-9a-f]{8,}", stem.lower()):
        return True
    if re.fullmatch(r"(?:pdf|hwp|hwpx|xls|xlsx|ppt|pptx|doc|docx|csv|zip)\s*(?:download|file)", stem.lower()):
        return True
    if stem.lower() in {"download", "file", "filedownload", "downloadfile"}:
        return True
    collapsed = re.sub(r"[\s._\-()\[\]]+", "", stem)
    return not collapsed


def _is_opaque_download_route_filename(filename: Any, url: str) -> bool:
    """True when a response filename is merely the final opaque route token."""
    try:
        parsed = urlparse(str(url or ""))
        route_token = os.path.basename(unquote(parsed.path or "")).strip()
        candidate = os.path.splitext(os.path.basename(str(filename or "").strip()))[0]
        if not route_token or not candidate or candidate.lower() != route_token.lower():
            return False
        # A real filename normally has an extension in the URL or carries
        # readable separators. Compact alphanumeric route tokens do not.
        return bool(
            "." not in route_token
            and re.fullmatch(r"[A-Za-z0-9]{4,64}", route_token)
        )
    except Exception:
        return False


def _normalize_download_filename_candidate(value: Any) -> str:
    text = str(value or "").strip().strip("\"'")
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    if not _download_file_extension(filename=text, filepath="", url=""):
        m = re.search(
            r"(?i)^(.*?)(?:[_\-\s]+)(pdf|hwp|hwpx|xls|xlsx|ppt|pptx|doc|docx|csv|zip|rar|7z)$",
            text,
        )
        if m:
            stem = (m.group(1) or "").strip(" ._-")
            ext = (m.group(2) or "").lower()
            if stem:
                text = f"{stem}.{ext}"
    return text


def _decode_filename_candidate(value: Any) -> str:
    text = str(value or "").strip().strip("\"'")
    if not text:
        return ""
    try:
        text = unquote(text)
    except Exception:
        pass
    for encoding in ("utf-8", "cp949", "euc-kr"):
        try:
            return _normalize_download_filename_candidate(text.encode("latin-1").decode(encoding))
        except Exception:
            continue
    return _normalize_download_filename_candidate(text)


def _download_filename_stem_key(value: Any) -> str:
    text = _decode_filename_candidate(value)
    if not text:
        return ""
    base = os.path.basename(text)
    stem, _ = os.path.splitext(base)
    collapsed = re.sub(r"[\W_]+", "", stem, flags=re.UNICODE).lower()
    return collapsed or stem.strip().lower()


def _prefer_richer_download_filename(candidate: str, ordered: list[str]) -> str:
    if not candidate or _download_file_extension(filename=candidate, filepath="", url=""):
        return candidate
    stem_key = _download_filename_stem_key(candidate)
    if not stem_key:
        return candidate
    for other in ordered:
        if not other or other == candidate:
            continue
        if _is_weak_download_filename(other):
            continue
        if not _download_file_extension(filename=other, filepath="", url=""):
            continue
        if _download_filename_stem_key(other) == stem_key:
            return other
    return candidate


def _filename_candidates_from_url(url: str) -> list[str]:
    out: list[str] = []
    try:
        parsed = urlparse(str(url or ""))
        params = dict(parse_qsl(parsed.query or "", keep_blank_values=False))
    except Exception:
        parsed = None
        params = {}
    for key in (
        "user_file_nm",
        "userFileNm",
        "file_nm",
        "fileNm",
        "filename",
        "fileName",
        "org_file_nm",
        "orignlFileNm",
        "sys_file_nm",
        "sysFileNm",
    ):
        value = params.get(key)
        if value:
            out.append(_decode_filename_candidate(value))
    if parsed is not None:
        try:
            base = os.path.basename(unquote(parsed.path or ""))
        except Exception:
            base = ""
        if base and "." in base:
            out.append(_decode_filename_candidate(base))
    return [candidate for candidate in out if candidate]


def _filename_candidates_from_meta(file_meta: Optional[Dict[str, Any]]) -> list[str]:
    if not isinstance(file_meta, dict):
        return []
    out: list[str] = []
    # Page/API attachment metadata is the source-of-truth. Keep it ahead of
    # generic display labels and response headers for opaque download routes.
    keys = (
        "attachment_name",
        "original_name",
        "original_filename",
        "org_file_nm",
        "user_file_nm",
        "name",
        "subject",
        "filename",
        "file_name",
        "display_name",
        "title",
        "sys_file_nm",
    )
    for key in keys:
        value = file_meta.get(key)
        if value:
            out.append(_decode_filename_candidate(value))
    original_meta = file_meta.get("original_meta")
    if isinstance(original_meta, dict):
        out.extend(_filename_candidates_from_meta(original_meta))
    return [candidate for candidate in out if candidate]


def _best_download_filename(
    *candidates: Any,
    url: str = "",
    file_meta: Optional[Dict[str, Any]] = None,
    default: str = "unknown",
    prefer_meta: bool = False,
) -> str:
    ordered: list[str] = []
    decoded_candidates: list[str] = []
    for candidate in candidates:
        decoded = _decode_filename_candidate(candidate)
        if decoded:
            decoded_candidates.append(decoded)
    meta_candidates = _filename_candidates_from_meta(file_meta)
    url_candidates = _filename_candidates_from_url(url)

    if not prefer_meta and meta_candidates:
        prefer_meta = any(
            _is_opaque_download_route_filename(candidate, url)
            for candidate in decoded_candidates
        ) or _is_server_side_direct_download_url(url)

    if prefer_meta:
        ordered.extend(meta_candidates)
        ordered.extend(decoded_candidates)
    else:
        ordered.extend(decoded_candidates)
        ordered.extend(meta_candidates)
    ordered.extend(url_candidates)

    weak_fallback = ""
    for candidate in ordered:
        if not candidate:
            continue
        if _is_weak_download_filename(candidate):
            if not weak_fallback:
                weak_fallback = candidate
            continue
        selected = _prefer_richer_download_filename(candidate, ordered)
        _filename_debug_log(
            "best_download_filename_selected",
            selected=selected,
            original_candidate=candidate,
            prefer_meta=prefer_meta,
            input_candidates=decoded_candidates,
            meta_candidates=meta_candidates,
            url_candidates=url_candidates,
            weak_fallback=weak_fallback,
            url=url,
        )
        return selected
    result = weak_fallback or default
    _filename_debug_log(
        "best_download_filename_fallback",
        selected=result,
        prefer_meta=prefer_meta,
        input_candidates=decoded_candidates,
        meta_candidates=meta_candidates,
        url_candidates=url_candidates,
        weak_fallback=weak_fallback,
        default=default,
        url=url,
    )
    return result


def _extension_from_content_type(content_type: str) -> str:
    ct = str(content_type or "").lower()
    if "pdf" in ct:
        return ".pdf"
    if "hwpx" in ct:
        return ".hwpx"
    if "haansofthwp" in ct or "x-hwp" in ct or "hwp" in ct:
        return ".hwp"
    if "wordprocessingml" in ct:
        return ".docx"
    if "spreadsheetml" in ct:
        return ".xlsx"
    if "presentationml" in ct:
        return ".pptx"
    if "msword" in ct:
        return ".doc"
    if "powerpoint" in ct or "presentation" in ct:
        return ".ppt"
    if "excel" in ct or "spreadsheet" in ct:
        return ".xls"
    return ""


def _extension_from_zip_file(filepath: str) -> str:
    try:
        with zipfile.ZipFile(filepath, "r") as zf:
            names = [str(name or "").replace("\\", "/") for name in zf.namelist()]
            mt = ""
            try:
                mt = zf.read("mimetype").decode("utf-8", errors="ignore").lower()
            except Exception:
                mt = ""
    except Exception:
        return ""
    if any(name.startswith("Contents/") or name.endswith("/content.hpf") for name in names) or "hwp" in mt:
        return ".hwpx"
    if any(name.startswith("word/") for name in names):
        return ".docx"
    if any(name.startswith("xl/") for name in names):
        return ".xlsx"
    if any(name.startswith("ppt/") for name in names):
        return ".pptx"
    return ""


def _infer_download_extension(
    *,
    filepath: str = "",
    head: Optional[bytes] = None,
    content_type: str = "",
    url: str = "",
    filename: str = "",
) -> str:
    ext = _download_file_extension(filename=filename, filepath="", url=url)
    if ext:
        return ext
    ext = _extension_from_content_type(content_type)
    if ext:
        return ext
    sniff = bytes(head or b"")[:16]
    if sniff.startswith(b"%PDF"):
        return ".pdf"
    if _looks_like_hwp_payload(sniff):
        return ".hwp"
    if _looks_like_zip_payload(sniff) and filepath:
        ext = _extension_from_zip_file(filepath)
        if ext:
            return ext
    return ""


def _ensure_download_filename_extension(
    filename: str,
    *,
    filepath: str = "",
    head: Optional[bytes] = None,
    content_type: str = "",
    url: str = "",
) -> str:
    base = str(filename or "").strip() or "downloaded_file"
    raw_ext = _raw_download_file_extension(filename=base, filepath="", url="")
    sniff = head if head is not None else (_read_file_head(filepath) if filepath else b"")
    if raw_ext == ".hwpx" and _looks_like_hwp_payload(sniff):
        stem = os.path.splitext(base)[0].rstrip(". ").strip() or "downloaded_file"
        return f"{stem}.hwp"
    if raw_ext == ".hwp" and _looks_like_zip_payload(sniff) and filepath:
        detected_ext = _extension_from_zip_file(filepath)
        if detected_ext == ".hwpx":
            stem = os.path.splitext(base)[0].rstrip(". ").strip() or "downloaded_file"
            return f"{stem}.hwpx"
    if raw_ext and raw_ext not in _NON_DOCUMENT_ROUTE_EXTENSIONS:
        return base
    ext = _infer_download_extension(
        filepath=filepath,
        head=sniff,
        content_type=content_type,
        url=url,
        filename=base,
    )
    if not ext:
        return base
    if raw_ext and raw_ext in _NON_DOCUMENT_ROUTE_EXTENSIONS:
        stem = os.path.splitext(base)[0].rstrip(". ").strip() or "downloaded_file"
        return f"{stem}{ext}"
    return f"{base.rstrip('.')}{ext}"


def _validate_downloaded_file(
    filepath: str,
    *,
    filename: str = "",
    url: str = "",
    content_type: str = "",
    expected_size: Optional[int] = None,
    actual_size: Optional[int] = None,
    head: Optional[bytes] = None,
) -> None:
    if is_partial_download_path(filename):
        raise RuntimeError(f"download filename still contains partial marker: {filename}")
    size = actual_size
    if size is None:
        size = os.path.getsize(filepath)
    if size <= 0:
        raise ValueError("downloaded file is empty")
    if expected_size is not None and int(expected_size) > 0 and int(size) != int(expected_size):
        raise IOError(f"downloaded file size mismatch: expected={expected_size} actual={size}")

    sniff = head if head is not None else _read_file_head(filepath)
    if "text/html" in str(content_type or "").lower() or _looks_like_html_payload(sniff or b""):
        raise RuntimeError("downloaded payload is HTML, not a document")

    ext = _download_file_extension(filename=filename, filepath=filepath, url=url)
    if ext == ".hwpx":
        if not zipfile.is_zipfile(filepath):
            if _looks_like_hwp_payload(sniff or b""):
                raise zipfile.BadZipFile(
                    f"downloaded payload is HWP/OLE but filename is HWPX: {filepath}"
                )
            raise zipfile.BadZipFile(f"downloaded HWPX is not a valid ZIP container: {filepath}")
        try:
            with zipfile.ZipFile(filepath, "r") as zf:
                names = zf.namelist()
        except zipfile.BadZipFile:
            raise
        except Exception as exc:
            raise zipfile.BadZipFile(f"downloaded HWPX cannot be inspected: {filepath}") from exc
        if not names:
            raise zipfile.BadZipFile(f"downloaded HWPX ZIP is empty: {filepath}")
        normalized_names = [str(name or "").replace("\\", "/") for name in names]
        if not any(
            name.startswith("Contents/")
            or name == "mimetype"
            or name.endswith("/content.hpf")
            for name in normalized_names
        ):
            raise zipfile.BadZipFile(f"downloaded ZIP does not look like HWPX: {filepath}")


async def _sync_after_download_if_needed(file_meta: Dict, filepath: str) -> bool:
    # ?낅젰 寃?? 硫뷀?/寃쎈줈 ?먮뒗 sync ?뚮옒洹멸? ?놁쑝硫??숈옉?섏? ?딆쓬
    if not file_meta or not filepath:
        return True
    if not bool(file_meta.get("sync_after_download")):
        # ?숆린???뚮옒洹멸? 紐낆떆?섏? ?딆? 寃쎌슦 ?숆린??嫄대꼫?
        logger.debug("[Download][WebSync] skip sync (flag false) | file=%s", _short(filepath, 200))
        return True


    try:
        # ?묒냽 base(URL) 寃곗젙: ?곗꽑 access_url, ?놁쑝硫?server_domain/doman ?ъ슜
        access_url = None
        try:
            access_url = file_meta.get("access_url") or file_meta.get("server_domain") or file_meta.get("domain")
        except Exception:
            access_url = None
        db_name = resolve_db_name(file_meta)
        access_base = normalize_access_url(access_url, db_name)

        # chat_bot_id媛 ?놁쑝硫??숆린??遺덇?
        chat_bot_id = file_meta.get("chat_bot_id")
        if not chat_bot_id:
            logger.error(
                "[Download][WebSync] chat_bot_id missing; cannot expose uploaded file | url=%s path=%s",
                _short(file_meta.get("url"), 200),
                _short(filepath, 200),
            )
            return False

        # ?몃옒?? ?숆린???쒖옉 ?쒓컖 諛?吏꾩엯濡쒓렇
        start_t = time.monotonic()
        logger.debug(
            "[Download][WebSync] start | url=%s path=%s access_base=%s",
            _short(file_meta.get("url"), 200),
            _short(filepath, 200),
            _short(access_base, 200),
        )
        logger.debug("[Download][WebSync] entry debug | chat_bot_id_tail=%s job_id=%s",
                     str(chat_bot_id).split("-")[-1] if chat_bot_id else None,
                     file_meta.get("job_id"))

        # ?ㅼ젣 ?숆린???몄텧 (rsync / local copy / sftp ???대? 泥섎━)
        ok = await sync_file_to_webserver(
            local_file_path=filepath,
            access_base_url=access_base,
            chat_bot_id=chat_bot_id,
            db_name=db_name,
        )

        # ?몃옒?? ?꾨즺 ?쒓컙 諛?寃곌낵 濡쒓퉭
        dur_ms = int((time.monotonic() - start_t) * 1000)
        logger.debug(
            "[Download][WebSync] done | ok=%s url=%s file=%s dur_ms=%d",
            bool(ok),
            _short(file_meta.get("url"), 200),
            os.path.basename(filepath),
            dur_ms,
        )
        logger.debug("[Download][WebSync] done debug | ok=%s dur_ms=%d file_sig=%s",
                     bool(ok), dur_ms, getattr(file_meta.get("original_meta", {}), "attachment_name", None))
        if not ok:
            logger.error(
                "[Download][WebSync] sync returned false; block file_saved | url=%s path=%s access_base=%s db=%s chat_bot_id=%s",
                _short(file_meta.get("url"), 200),
                _short(filepath, 240),
                _short(access_base, 200),
                db_name,
                chat_bot_id,
            )
            return False
        if _env_bool("FILE_WEB_SYNC_VERIFY_HEAD", "1"):
            expected_url = ""
            expected_physical_path = ""
            expected_physical_exists = None
            try:
                storage_domain = get_storage_domain_for_db_name(db_name)
                expected_url = get_file_upload_content_url(
                    access_base,
                    storage_domain,
                    chat_bot_id,
                    os.path.basename(filepath),
                )
                tail = str(chat_bot_id or "").strip().split("-")[-1][-12:] or "unknown"
                expected_physical_path = os.path.join(
                    get_fileupload_root(),
                    storage_domain,
                    tail,
                    os.path.basename(filepath),
                )
                try:
                    expected_physical_exists = os.path.exists(expected_physical_path)
                except Exception:
                    expected_physical_exists = None
            except Exception as url_exc:
                logger.error(
                    "[Download][WebSync] public URL build failed; block file_saved | url=%s path=%s err=%s",
                    _short(file_meta.get("url"), 200),
                    _short(filepath, 240),
                    _short(url_exc, 200),
                )
                return False
            timeout = aiohttp.ClientTimeout(total=_env_float("FILE_WEB_SYNC_VERIFY_HEAD_TIMEOUT", 5.0))
            try:
                verify_attempts = int(_env_float("FILE_WEB_SYNC_VERIFY_HEAD_ATTEMPTS", 3.0))
            except Exception:
                verify_attempts = 3
            verify_attempts = max(1, min(int(verify_attempts or 1), 10))
            try:
                verify_delay_sec = _env_float("FILE_WEB_SYNC_VERIFY_HEAD_RETRY_DELAY_SEC", 1.0)
            except Exception:
                verify_delay_sec = 1.0
            verify_delay_sec = max(0.0, min(float(verify_delay_sec or 0.0), 30.0))
            last_status = None
            last_head_exc = None
            for verify_attempt in range(1, verify_attempts + 1):
                try:
                    async with aiohttp.ClientSession(timeout=timeout) as verify_session:
                        async with verify_session.head(expected_url, allow_redirects=True) as resp:
                            last_status = resp.status
                            if resp.status < 400:
                                last_head_exc = None
                                break
                except Exception as head_exc:
                    last_head_exc = head_exc
                if verify_attempt < verify_attempts and verify_delay_sec > 0:
                    await asyncio.sleep(verify_delay_sec)
            if last_head_exc is not None:
                file_meta["_websync_public_url"] = expected_url
                file_meta["_websync_public_error"] = repr(last_head_exc)
                if expected_physical_exists:
                    logger.warning(
                        "[Download][WebSync] public URL HEAD unconfirmed after sync; allow file_saved | attempts=%s public_url=%s expected_path=%s source_url=%s post_url=%s name=%s path=%s error_type=%s error=%r",
                        verify_attempts,
                        _short(expected_url, 260),
                        _short(expected_physical_path, 260),
                        _short(file_meta.get("url"), 200),
                        _short(_resolve_source_page_from_file_meta(file_meta), 220),
                        _short(file_meta.get("name") or file_meta.get("subject"), 160),
                        _short(filepath, 240),
                        type(last_head_exc).__name__,
                        last_head_exc,
                    )
                    return True
                logger.error(
                    "[Download][WebSync] public URL HEAD exception; block file_saved | attempts=%s public_url=%s expected_path=%s expected_exists=%s source_url=%s post_url=%s name=%s path=%s error_type=%s error=%r",
                    verify_attempts,
                    _short(expected_url, 260),
                    _short(expected_physical_path, 260),
                    expected_physical_exists,
                    _short(file_meta.get("url"), 200),
                    _short(_resolve_source_page_from_file_meta(file_meta), 220),
                    _short(file_meta.get("name") or file_meta.get("subject"), 160),
                    _short(filepath, 240),
                    type(last_head_exc).__name__,
                    last_head_exc,
                )
                return False
            if last_status is not None and int(last_status) >= 400:
                file_meta["_websync_public_url"] = expected_url
                file_meta["_websync_public_status"] = int(last_status)
                logger.error(
                    "[Download][WebSync] public URL HEAD failed; block file_saved | status=%s attempts=%s public_url=%s expected_path=%s expected_exists=%s source_url=%s post_url=%s name=%s path=%s",
                    last_status,
                    verify_attempts,
                    _short(expected_url, 260),
                    _short(expected_physical_path, 260),
                    expected_physical_exists,
                    _short(file_meta.get("url"), 200),
                    _short(_resolve_source_page_from_file_meta(file_meta), 220),
                    _short(file_meta.get("name") or file_meta.get("subject"), 160),
                    _short(filepath, 240),
                )
                return False
        return True
    except Exception as exc:
        # ?덉쇅 諛쒖깮 ??寃쎄퀬? ?붾쾭洹??뺣낫 ?④?
        logger.error(
            "[Download][WebSync] failed | url=%s path=%s err=%s",
            _short(file_meta.get("url"), 200),
            _short(filepath, 200),
            _short(exc, 200),
        )
        logger.debug("[Download][WebSync] exception detail", exc_info=True)
        return False

async def _get_download_dir(file_meta: Dict, default_download_dir: str, download_path_cache: Dict) -> str:
    """
    ?ㅼ슫濡쒕뱶 ?붾젆?좊━ 寃쎈줈瑜?怨꾩궛?섎뒗 ?ы띁 ?⑥닔
    ?댁젣 ?꾨찓?몃퀎 ?섏쐞 ?대뜑 ?앹꽦??吏?먰빀?덈떎.
    """
    try:
        chat_bot_id = file_meta.get('chat_bot_id')
        url = file_meta.get('url', '')
        db_name = resolve_db_name(file_meta)

        # ???붿껌?ы빆: ?????寃쎈줈??"?묒냽url/chat/uploaded_files/{uuid_tail12}" 湲곗?
        # - ?묒냽url? ?꾨줎?멸? ?꾨떖??access_url???곗꽑 ?ъ슜
        access_url = file_meta.get("access_url")
        access_base = normalize_access_url(access_url, db_name)
        # (fallback/濡쒓렇?? host??湲곗〈 server_domain???좎?
        domain = get_storage_domain_for_db_name(db_name) if db_name else (file_meta.get('server_domain') or file_meta.get('domain'))
        # ?꾨찓?몄씠 ?놁쑝硫?URL?먯꽌 異붿텧
        if (not domain or domain == 'unknown') and url:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.split(':')[0]
            if DOWNLOAD_PATH_DEBUG:
                logger.info(
                    "[DOWNLOAD][PathDebug] domain missing -> derived from url | url=%s derived_domain=%s",
                    _short(url, 200),
                    domain,
                )

        # chat_bot_id媛 ?놁쑝硫?DB(chatbot_setup)?먯꽌 理쒖떊 媛믪쓣 議고쉶?섏뿬 蹂닿컯
        if not chat_bot_id and db_name:
            try:
                from db.mariadb_save_update import get_latest_chat_bot_id_from_chatbot_setup
                chat_bot_id = await get_latest_chat_bot_id_from_chatbot_setup(str(db_name))
                if chat_bot_id:
                    file_meta["chat_bot_id"] = chat_bot_id
                if DOWNLOAD_PATH_DEBUG:
                    logger.info(
                        "[DOWNLOAD][PathDebug] chat_bot_id烏ε츑 via chatbot_setup | db=%s chat_bot_id_tail=%s",
                        db_name,
                        (str(chat_bot_id).split("-")[-1] if chat_bot_id else None),
                    )
            except Exception:
                pass

        if chat_bot_id:
            # 罹먯떆 ?ㅼ뿉 ?꾨찓???ы븿
            cache_key = f"{chat_bot_id}_{access_base}"
            if cache_key in download_path_cache:
                if DOWNLOAD_PATH_DEBUG:
                    logger.info(
                        "[DOWNLOAD][PathDebug] cache hit | db=%s server_domain=%s domain=%s chat_bot_id_tail=%s dir=%s",
                        db_name,
                        file_meta.get("server_domain"),
                        domain,
                        str(chat_bot_id).split("-")[-1] if chat_bot_id else None,
                        download_path_cache[cache_key],
                    )
                return download_path_cache[cache_key]
            
            # backend.shared.config??以묒븰 吏묒쨷??寃쎈줈 ?앹꽦 ?⑥닔 ?ъ슜
            # ?깅룞援ъ껌(db sungdong): ?묒냽 URL??sd.go.kr?댁뼱??濡쒖뺄 ?대뜑??get_storage_domain_for_db_name怨??숈씪?섍쾶 留욎땄
            download_dir = get_uploaded_files_local_dir(
                access_base_url=access_base,
                chat_bot_id=chat_bot_id,
                storage_domain=get_storage_domain_for_db_name(db_name) if db_name == "sungdong" else None,
            )
            
            # ?붾젆?좊━ ?앹꽦 (?꾩닔)
            os.makedirs(download_dir, exist_ok=True)
            download_path_cache[cache_key] = download_dir
            if DOWNLOAD_PATH_DEBUG:
                logger.info(
                    "[DOWNLOAD][PathDebug] cache miss -> computed dir | db=%s server_domain=%s domain=%s chat_bot_id_tail=%s dir=%s",
                    db_name,
                    file_meta.get("server_domain"),
                    domain,
                    str(chat_bot_id).split("-")[-1] if chat_bot_id else None,
                    download_dir,
                )
            return download_dir


           
        else:
            logger.debug(f"[DOWNLOAD] chat_bot_id missing, using default path: {default_download_dir}")
            if DOWNLOAD_PATH_DEBUG:
                logger.info(
                    "[DOWNLOAD][PathDebug] chat_bot_id missing -> fallback default dir | db=%s server_domain=%s domain=%s default_dir=%s",
                    db_name,
                    file_meta.get("server_domain"),
                    domain,
                    default_download_dir,
                )
            return default_download_dir
    except Exception as e:
        logger.warning(f"[DOWNLOAD] dynamic path creation failed, using default path: {e}", exc_info=True)
        if DOWNLOAD_PATH_DEBUG:
            logger.info(
                "[DOWNLOAD][PathDebug] exception -> fallback default dir | default_dir=%s err=%s",
                default_download_dir,
                _short(e, 240),
            )
        return default_download_dir

async def _download_with_playwright(browser, file_meta: Dict, download_dir: str, default_download_dir: str, browser_relauncher: Optional[Callable[[], Awaitable[Browser]]] = None, worker_id: Optional[int] = None):    
    """
    Download a file through Playwright fallback.
    - Uses page.expect_download() with click.
    - Falls back to page.goto() / link click style downloads.
    - Recovers from browser disconnects when a relauncher is available.
    """
    wtag = f"[Worker {worker_id}] " if worker_id is not None else ""
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
    from urllib.parse import urlparse, urljoin
    
    raw_url = str(file_meta.get("_raw_url") or file_meta.get('url') or "").strip()
    source_page = file_meta.get('source_page') or file_meta.get('url') or ""
    url = _resolve_download_url(file_meta.get('url'), source_page)
    if _is_viewer_convert_url(url):
        file_meta["_download_empty_reason"] = "viewer_convert_url"
        logger.debug(
            "[Download] %s[Playwright] skip viewer/convert URL before download wait | url=%s",
            wtag,
            _short(url, 220),
        )
        return None
    is_portal_direct = _is_portal_direct_file_url(url)
    suggested_name = file_meta.get('name', 'unknown')
    # source_page媛 ?놁쑝硫?URL???泥닿컪?쇰줈 ?ъ슜?섏뿬 Referer/濡쒓렇 ?깆뿉 ?쒖슜
    source_page = source_page or url

    context = None
    page = None
    target_tokens = _attachment_identity_tokens(url, source_page)
    target_tokens.update(_attachment_identity_tokens(raw_url, source_page))

    def _is_target_closed_error(exc: BaseException) -> bool:
        try:
            error_msg = str(exc).lower()
        except Exception:
            return False
        return (
            "target closed" in error_msg
            or "browser has been closed" in error_msg
            or "target page, context or browser has been closed" in error_msg
        )

    async def _relaunch_browser_for_target_closed(reason: str) -> Browser:
        nonlocal browser
        if not browser_relauncher:
            raise RuntimeError(f"browser relauncher is not available: {reason}")
        try:
            await asyncio.sleep(0.2)
        except Exception:
            pass
        browser = await browser_relauncher()
        logger.info(
            "[DOWNLOAD] [Playwright] browser relaunched | reason=%s worker=%s url=%s",
            reason,
            worker_id,
            _short(url, 220),
        )
        return browser

    async def _create_browser_context() -> Any:
        nonlocal browser, context
        for attempt in range(1, 3):
            try:
                if browser is None:
                    if browser_relauncher:
                        await _relaunch_browser_for_target_closed("context_create_browser_missing")
                    else:
                        raise RuntimeError("browser is missing and relauncher is not available")
                elif not browser.is_connected():
                    await _relaunch_browser_for_target_closed("context_create_browser_disconnected")

                context = await browser.new_context(
                    accept_downloads=True,
                    ignore_https_errors=True,
                    user_agent=_PLAYWRIGHT_DEFAULT_UA,
                    locale="ko-KR",
                )
                await configure_context_for_crawl(context, url or source_page or "")
                await _inject_playwright_context_cookies(
                    context,
                    file_meta,
                    url=url,
                    source_page=source_page,
                    worker_id=worker_id,
                )
                logger.debug("[DOWNLOAD] [Playwright] browser context created")
                return context
            except Exception as ctx_err:
                if attempt < 2 and _is_target_closed_error(ctx_err) and browser_relauncher:
                    logger.warning(
                        "[DOWNLOAD] [Playwright] target closed while creating context; relaunching browser | attempt=%s worker=%s url=%s",
                        attempt,
                        worker_id,
                        _short(url, 220),
                    )
                    try:
                        if context:
                            await context.close()
                    except Exception:
                        pass
                    context = None
                    await _relaunch_browser_for_target_closed("context_create_target_closed")
                    continue
                raise
        raise RuntimeError(f"Playwright context create failed: {url}")

    async def _create_page_with_recovery() -> Any:
        nonlocal context, page
        for attempt in range(1, 3):
            try:
                if context is None:
                    await _create_browser_context()
                page = await context.new_page()
                await apply_stealth_if_needed(page, url or source_page or "")
                logger.debug("[DOWNLOAD] [Playwright] page created")
                return page
            except PlaywrightError as page_err:
                if attempt < 2 and _is_target_closed_error(page_err) and browser_relauncher:
                    logger.warning(
                        "[DOWNLOAD] [Playwright] browser closed while creating page; recreating context | attempt=%s worker=%s url=%s",
                        attempt,
                        worker_id,
                        _short(url, 220),
                    )
                    try:
                        if page:
                            await page.close()
                    except Exception:
                        pass
                    page = None
                    try:
                        if context:
                            await context.close()
                    except Exception:
                        pass
                    context = None
                    await _relaunch_browser_for_target_closed("page_create_target_closed")
                    await _create_browser_context()
                    continue
                raise

    def _filename_from_content_disposition(content_disposition: str) -> Optional[str]:
        if not content_disposition:
            return None
        from urllib.parse import unquote

        # 1. Prefer RFC 5987 filename* value
        match = re.search(r'filename\*=UTF-8\'\'(.+)', content_disposition, re.IGNORECASE)
        if match:
            return unquote(match.group(1))

        # 2. Extract ordinary filename= value
        # Handles both quoted and unquoted filename values.
        match = re.search(r'filename=(?:["\']?([^"\'\n;]+)["\']?|([^"\';\n]+))', content_disposition, re.IGNORECASE)
        if not match:
            return None

        # Pull filename from the quoted or unquoted capture group.
        raw_filename = (match.group(1) or match.group(2)).strip()

        try:
            # Decode URL-encoded filenames such as %EB...
            if '%' in raw_filename:
                raw_filename = unquote(raw_filename)

            # Reinterpret latin-1 bytes using likely Korean encodings.
            # Try UTF-8 first, then CP949 for Korean public sites.
            binary_data = raw_filename.encode('latin-1')
            for encoding in ['utf-8', 'cp949']:
                try:
                    return binary_data.decode(encoding)
                except (UnicodeDecodeError, UnicodeEncodeError):
                    continue
            
            return raw_filename  # Return original if decoding attempts fail.
        except Exception:
            return raw_filename
            
    _context_request_source_primed = False

    async def _download_via_context_request() -> Optional[Dict]:
        nonlocal _context_request_source_primed
        if not context:
            raise RuntimeError("Playwright context is not available for request fallback")
        # ?대??먯꽌 `req_url = ??留?媛깆떊?쒕떎. `url = resolved_from_html`泥섎읆 ?대쫫???곕㈃
        # ???⑥닔媛 濡쒖뺄 `url`濡?媛꾩＜?섏뼱 HEAD/GET ?꾩뿉 UnboundLocalError媛 ?쒕떎.
        req_url = url
        if not _is_valid_http_url(req_url):
            raise RuntimeError(f"Playwright request fallback invalid URL: {req_url!r}")
        if _is_identity_kisa_url(req_url) or _is_identity_kisa_url(source_page):
            raise RuntimeError("identity_kisa_requires_browser_click: context.request fallback disabled")
        try:
            req_timeout_ms = int(os.getenv("DOWNLOAD_PLAYWRIGHT_REQUEST_TIMEOUT_MS", "90000") or "90000")
        except Exception:
            req_timeout_ms = 90000
        req_timeout_ms = max(5000, min(int(req_timeout_ms), 300000))
        if is_portal_direct and _portal_direct_fail_fast_enabled():
            req_timeout_ms = min(req_timeout_ms, _portal_direct_request_timeout_ms())
        try:
            large_bytes = int(os.getenv("DOWNLOAD_PLAYWRIGHT_LARGE_FILE_BYTES", str(50 * 1024 * 1024)) or str(50 * 1024 * 1024))
        except Exception:
            large_bytes = 50 * 1024 * 1024
        try:
            large_timeout_ms = int(os.getenv("DOWNLOAD_PLAYWRIGHT_LARGE_FILE_TIMEOUT_MS", "180000") or "180000")
        except Exception:
            large_timeout_ms = 180000
        large_timeout_ms = max(req_timeout_ms, min(int(large_timeout_ms), 600000))

        req_headers: Dict[str, str] = {
            "User-Agent": _PLAYWRIGHT_DEFAULT_UA,
            "Accept": "*/*",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        cookie_header = _cookie_header_from_file_meta(file_meta)
        manual_cookie_header = "; ".join(
            f"{name}={value}" for name, value in _manual_download_cookies(req_url, source_page).items()
        )
        merged_cookie_header = _merge_cookie_headers(cookie_header, manual_cookie_header)
        if merged_cookie_header:
            req_headers["Cookie"] = merged_cookie_header
        request_header_variants = _request_header_variants(
            req_headers,
            source_page=source_page,
            referer_override=(os.getenv("DOWNLOAD_PLAYWRIGHT_REFERER_OVERRIDE") or "").strip(),
            prefer_no_origin_first=_is_portal_direct_file_url(req_url),
        )

        async def _context_request(
            method: str,
            target_url: str,
            *,
            timeout_ms: int,
            data: Optional[Dict[str, Any]] = None,
        ):
            if not _is_valid_http_url(target_url):
                raise RuntimeError(f"Playwright context.request invalid URL skipped: {target_url!r}")
            last_exc: Optional[Exception] = None
            for headers_variant in request_header_variants:
                try:
                    if method == "HEAD":
                        return await context.request.head(target_url, headers=headers_variant, timeout=timeout_ms)
                    if method == "POST":
                        return await context.request.post(
                            target_url,
                            headers=headers_variant,
                            timeout=timeout_ms,
                            form=data or {},
                        )
                    return await context.request.get(target_url, headers=headers_variant, timeout=timeout_ms)
                except Exception as exc:
                    last_exc = exc if isinstance(exc, Exception) else RuntimeError(str(exc))
            if last_exc is not None:
                raise last_exc
            raise RuntimeError(f"Playwright context.request {method} failed without exception")

        # [?섏젙] Playwright 愿???먮윭 泥섎━瑜??꾪빐 ?꾪룷???뺤씤 (?곷떒???놁쓣 寃쎌슦 ?鍮?
        from playwright.async_api import Error as PlaywrightError

        if not _context_request_source_primed and (source_page or "").strip():
            try:
                prime_timeout_ms = min(15000, max(5000, int(req_timeout_ms / 4)))
                prime_resp = await _context_request("GET", source_page, timeout_ms=prime_timeout_ms)
                try:
                    await prime_resp.body()
                except Exception:
                    pass
                _context_request_source_primed = True
            except Exception as prime_exc:
                logger.debug(
                    "[Download] %s[Playwright] source_page request prime miss (continue) | source=%s err=%s",
                    wtag,
                    _short(source_page, 180),
                    _short(prime_exc, 180),
                )

        # ??⑸웾 ?뚯씪? timeout???됰꼮??以??(HEAD濡?content-length ?뺤씤)
        try:
            # [異붽?] ?ㅽ뻾 ??而⑦뀓?ㅽ듃 ?좏슚??泥댄겕
            if not context or not hasattr(context, "request"):
                logger.warning(f"[Download] {wtag} 而⑦뀓?ㅽ듃媛 ?ロ? HEAD ?붿껌??嫄대꼫?곷땲??")
                raise RuntimeError("Context closed")

            head_timeout_ms = min(10000, max(5000, int(req_timeout_ms / 6)))
            head_resp = await _context_request("HEAD", req_url, timeout_ms=head_timeout_ms)
            if head_resp and head_resp.ok:
                try:
                    clen = int(head_resp.headers.get("content-length") or "0")
                except Exception:
                    clen = 0
                if clen >= large_bytes:
                    req_timeout_ms = large_timeout_ms
        except Exception:
            pass

        # logger.info(
        #     "[Download] %s[Playwright] request fallback start | url=%s timeout_ms=%s",
        #     wtag, req_url, req_timeout_ms,
        # )

        # [?섏젙] 蹂??붿껌 ??TargetClosedError ?덉쇅 泥섎━ 異붽?
        try:
            # ?ъ감 而⑦뀓?ㅽ듃 ?좏슚???뺤씤
            if not context or not hasattr(context, "request"):
                raise RuntimeError(f"Playwright context request target closed: {req_url}")

            response = await _context_request("GET", req_url, timeout_ms=req_timeout_ms)
        except PlaywrightError as e:
            if "Target page, context or browser has been closed" in str(e):
                logger.warning(f"[Download] {wtag} browser context closed (TargetClosedError) | url={req_url}")
                raise RuntimeError(f"Playwright context request target closed: {req_url}") from e
            raise # ?ㅻⅨ Playwright ?먮윭???щ컻??
        if response.status != 200:
            if response.status in {401, 403}:
                raise RuntimeError(f"Playwright request fallback access denied non-200: {response.status}")
            raise RuntimeError(f"Playwright request fallback non-200: {response.status}")

        content_type = (response.headers.get("content-type") or "").lower()
        cd = response.headers.get("content-disposition", "")
        
        # ?ㅽ듃由쇱쑝濡?諛붾줈 ?ㅼ슫濡쒕뱶?섎뒗 寃쎌슦 content_type??'file'濡??ㅼ젙
        # - Content-Disposition??attachment媛 ?덇굅??
        # - application/octet-stream ?먮뒗 binary/octet-stream??寃쎌슦
        is_stream_download = False
        if cd:
            cd_lower = cd.lower()
            if 'attachment' in cd_lower or 'filename' in cd_lower:
                is_stream_download = True
        if not is_stream_download:
            if 'application/octet-stream' in content_type or 'binary/octet-stream' in content_type:
                is_stream_download = True
        
        if is_stream_download:
            content_type = 'file'
        
        if DOWNLOAD_DOC_ONLY and (
            content_type.startswith("image/")
            or content_type.startswith("video/")
            or content_type.startswith("audio/")
        ):
            logger.info(
                "[Download] %s[Playwright] Skipped (non-doc mime) | url=%s ct=%s",
                wtag, req_url, content_type,
            )
            return None

        body = await response.body()
        if not body:
            raise RuntimeError("Playwright request fallback returned empty body")

        head = body[:2048].lstrip().lower()
        if "text/html" in content_type or head.startswith(b"<!doctype html") or b"<html" in head:
            text = body.decode("utf-8", errors="ignore")
            real_url = _extract_html_download_url(text, req_url, target_url=req_url)

            if real_url:
                logger.info("[Download] %s HTML fallback found file link; retrying | url=%s", wtag, real_url)
                # 1以?二쇱꽍: 異붿텧???ㅼ젣 URL濡??ㅼ떆 GET ?붿껌??蹂대궡 ?뚯씪 ?곗씠?곕? 媛?몄샂
                if not _is_valid_http_url(real_url):
                    raise RuntimeError(f"Playwright request fallback found invalid HTML file URL: {real_url!r}")
                response = await _context_request("GET", real_url, timeout_ms=req_timeout_ms)
                body = await response.body()
                content_type = response.headers.get("content-type", "")
                req_url = real_url
            else:
                resolved_from_html = ""
                if resolved_from_html and resolved_from_html != req_url and _is_valid_http_url(resolved_from_html):
                    response = await _context_request("GET", resolved_from_html, timeout_ms=req_timeout_ms)
                    body = await response.body()
                    content_type = response.headers.get("content-type", "")
                    req_url = resolved_from_html
                else:
                    followup = _extract_html_followup_request(text, req_url)
                    if followup and str(followup.get("url") or "").strip():
                        followup_method = str(followup.get("method") or "GET").strip().upper()
                        followup_url = str(followup.get("url") or "").strip()
                        followup_data = followup.get("data") if isinstance(followup.get("data"), dict) else None
                        if not _is_valid_http_url(followup_url):
                            raise RuntimeError(f"Playwright request fallback found invalid follow-up URL: {followup_url!r}")
                        response = await _context_request(
                            followup_method,
                            followup_url,
                            timeout_ms=req_timeout_ms,
                            data=followup_data,
                        )
                        body = await response.body()
                        content_type = response.headers.get("content-type", "")
                        req_url = followup_url
                    else:
                        source_real_url = ""
                        if (source_page or "").strip() and str(source_page).strip() != req_url:
                            try:
                                source_resp = await _context_request(
                                    "GET",
                                    str(source_page).strip(),
                                    timeout_ms=min(req_timeout_ms, _source_extract_request_timeout_ms()),
                                )
                                source_body = await source_resp.body()
                                source_text = source_body.decode("utf-8", errors="ignore")
                                source_real_url = _extract_html_download_url(
                                    source_text,
                                    str(source_page).strip(),
                                    target_url=req_url,
                                )
                            except Exception as source_extract_exc:
                                logger.debug(
                                    "[Download] %s source_page HTML download-url extraction failed | source=%s err=%s",
                                    wtag,
                                    _short(source_page, 220),
                                    _short(source_extract_exc, 180),
                                )
                        if source_real_url and _is_valid_http_url(source_real_url):
                            logger.info(
                                "[Download] %s source_page HTML fallback found file link; retrying | url=%s",
                                wtag,
                                _short(source_real_url, 260),
                            )
                            response = await _context_request("GET", source_real_url, timeout_ms=req_timeout_ms)
                            body = await response.body()
                            content_type = response.headers.get("content-type", "")
                            req_url = source_real_url
                        else:
                            # 1以?二쇱꽍: HTML??諛섑솚?섏뿀?쇰굹 ?대??먯꽌 ?뚯씪 留곹겕瑜?李얠? 紐삵븳 寃쎌슦 理쒖쥌 ?ㅽ뙣 泥섎━??
                            logger.warning(
                                "[Download] %s HTML response fallback ended (no file link found) | url=%s content_type=%s",
                                wtag,
                                _short(req_url, 220),
                                _short(content_type or "", 120),
                            )
                            if DOWNLOAD_HTML_FALLBACK_BODY_LOG:
                                logger.debug(
                                    "[Download] %s HTML fallback body preview | url=%s preview=%s",
                                    wtag,
                                    _short(req_url, 220),
                                    _html_preview_for_log(text, 240),
                                )
                            raise RuntimeError("Playwright request fallback returned HTML content (No link found)")

        cd = response.headers.get("content-disposition", "")
        final_head = (body or b"")[:2048].lstrip().lower()
        final_content_type = (content_type or "").lower()
        if "text/html" in final_content_type or final_head.startswith(b"<!doctype html") or b"<html" in final_head:
            raise RuntimeError(f"downloaded payload is HTML, not a document: {req_url}")
        final_filename = _filename_from_content_disposition(cd)

        if not final_filename or final_filename == "unknown":
            from uuid import uuid4
            ext = ".bin"
            if ".pdf" in req_url.lower():
                ext = ".pdf"
            elif ".hwp" in req_url.lower():
                ext = ".hwp"
            final_filename = f"file_{uuid4().hex[:8]}{ext}"
        final_filename = _best_download_filename(
            final_filename,
            suggested_name,
            url=req_url,
            file_meta=file_meta,
            default=final_filename,
        )
        _trace_filename_resolution(file_meta, worker_id=worker_id, url=req_url, stage="playwright_request", response_filename=_filename_from_content_disposition(cd) or suggested_name, selected_filename=final_filename)
        _filename_debug_log(
            "playwright_response_filename",
            suggested_name=suggested_name,
            content_disposition_filename=_filename_from_content_disposition(cd),
            final_filename=final_filename,
            url=req_url,
            file_meta_name=(file_meta or {}).get("name") if isinstance(file_meta, dict) else None,
            attachment_name=((file_meta or {}).get("original_meta", {}) or {}).get("attachment_name") if isinstance(file_meta, dict) else None,
        )
        final_filename = _strip_partial_download_suffix(final_filename)

        # PHP ?듭씪: ?붿뒪?ъ뿉??md5(subject+time+uniqid).ext, DB subject?먮뒗 ?먮낯紐?
        original_subject = sanitize_filename(final_filename) or final_filename
        if is_blocked_non_document(final_filename, ""):
            logger.info(
                "[Download] %s[Playwright] Skipped (non-doc) | url=%s filename=%s",
                wtag, req_url, original_subject,
            )
            return None
        storage_filename = make_safe_storage_filename(final_filename)

        final_download_dir = download_dir or default_download_dir
        filepath = os.path.join(final_download_dir, storage_filename)
        expected_size = _expected_content_length(response.headers)
        tmp_filepath = _download_temp_path(filepath)
        try:
            # 1以?二쇱꽍: ?뚯씪???ㅼ젣濡?議댁옱?섎뒗吏 ??踰????뺤씤?섏뿬 ??젣 ??諛쒖깮?섎뒗 寃쎈줈 ?먮윭瑜?諛⑹???
            if os.path.exists(filepath):
                pass
            await asyncio.to_thread(_write_bytes, tmp_filepath, body)
            file_size = await wait_for_file_ready(
                tmp_filepath,
                timeout_sec=_download_temp_ready_timeout_sec(),
                allow_partial_name=True,
                check_partial_siblings=False,
            )
            final_filename = _ensure_download_filename_extension(
                final_filename,
                filepath=tmp_filepath,
                head=body[:2048],
                content_type=content_type,
                url=req_url,
            )
            final_filename = _strip_partial_download_suffix(final_filename)
            original_subject = sanitize_filename(final_filename) or final_filename
            storage_filename = make_safe_storage_filename(final_filename)
            filepath = os.path.join(final_download_dir, storage_filename)
            await asyncio.to_thread(
                _validate_downloaded_file,
                tmp_filepath,
                filename=original_subject,
                url=req_url,
                content_type=content_type,
                expected_size=expected_size,
                actual_size=file_size,
                head=body[:2048],
            )
            await asyncio.to_thread(_replace_file, tmp_filepath, filepath)
            # The unique temporary file was validated before the atomic replace.
            # A stale sibling from another attempt must not block this finalized file.
            file_size = await wait_for_file_ready(
                filepath,
                timeout_sec=_download_final_ready_timeout_sec(),
                stable_checks=3,
                check_partial_siblings=False,
            )
        except Exception as exc:
            await asyncio.to_thread(_remove_file_quietly, tmp_filepath)
            logger.warning(
                "[DOWNLOAD][Error] failed to write/validate file | url=%s filepath=%s err=%s",
                _short(req_url, 200),
                _short(filepath, 400),
                _short(exc, 300),
            )
            raise

        if file_size == 0:
            raise ValueError("downloaded file size is 0 bytes")

        # Debug log after file write completes and existence is confirmed
        try:
            exists = await asyncio.to_thread(os.path.exists, filepath)
        except Exception:
            exists = False
        logger.info(
            "[DOWNLOAD] file written | url=%s download_dir=%s filename=%s filepath=%s size=%s exists=%s",
            _short(req_url, 200),
            _short(final_download_dir, 200),
            storage_filename,
            _short(filepath, 400),
            int(file_size) if file_size is not None else None,
            bool(exists),
        )
        return {
            "file_path": filepath,
            "local_path": filepath,
            "url": req_url,
            "name": original_subject,
            "subject": original_subject,
            "display_name": original_subject,
            "attachment_name": original_subject,
            "storage_filename": storage_filename,
            "size": file_size,
            "content_type": content_type,
            "cate1": file_meta.get("cate1"),
            "cate2": file_meta.get("cate2"),
            "original_meta": file_meta,
            **_learn_list_ids_from_file_meta(file_meta),
            **_defer_save_batch_flag(file_meta),
        }

    async def _safe_query_selector(selector: str):
        """Safely query an element and suppress expected browser/page-closed errors."""
        try:
            if not page or page.is_closed():
                return None
            return await page.query_selector(selector)
        except PlaywrightError as e:
            error_msg = str(e).lower()

            # Return None when the page/browser is already closed.
            if "target closed" in error_msg or "browser has been closed" in error_msg:
                logger.warning(f"[DOWNLOAD] [Playwright] browser/page closed while querying selector (ignored): {e}")
                return None

            # Retry once when navigation destroyed the execution context.
            if "execution context was destroyed" in error_msg or "navigation" in error_msg:
                logger.warning(f"[DOWNLOAD] [Playwright] execution context destroyed/navigation detected; retrying selector | selector={selector}")
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=5000)
                    return await page.query_selector(selector)
                except PlaywrightError as retry_err:
                    retry_msg = str(retry_err).lower()
                    # If the page closes during retry, return None.
                    if "target closed" in retry_msg or "browser has been closed" in retry_msg:
                        logger.warning("[DOWNLOAD] [Playwright] page closed during selector retry (ignored)")
                        return None
                    logger.warning("[DOWNLOAD] [Playwright] selector retry failed (ignored): %s", retry_err)
                    return None
                except Exception:
                    return None

            # Propagate unexpected Playwright errors.
            raise

    def _attachment_link_matches(href: Optional[str], onclick: Optional[str] = None) -> bool:
        href_text = str(href or "").strip()
        onclick_text = str(onclick or "").strip()
        abs_href = ""
        if href_text:
            try:
                abs_href = urljoin(source_page or url, href_text)
            except Exception:
                abs_href = href_text

        normalized_abs = _normalize_attachment_download_url(abs_href) if abs_href else ""
        normalized_raw = _normalize_attachment_download_url(raw_url) if raw_url else ""
        if normalized_abs and normalized_abs == url:
            return True
        if normalized_abs and normalized_raw and normalized_abs == normalized_raw:
            return True
        if abs_href and raw_url and abs_href == raw_url:
            return True

        candidate_tokens = set()
        candidate_tokens.update(_attachment_identity_tokens(abs_href, source_page))
        candidate_tokens.update(_attachment_identity_tokens(href_text, source_page))
        candidate_tokens.update(_attachment_identity_tokens(onclick_text, source_page))
        return bool(target_tokens and candidate_tokens and target_tokens.intersection(candidate_tokens))

    try:
        # 釉뚮씪?곗? ?곌껐 ?곹깭 ?뺤씤
        if browser and not browser.is_connected():
            logger.warning(f"[DOWNLOAD] [Playwright] browser is disconnected; trying relaunch...")
            if browser_relauncher:
                try:
                    browser = await browser_relauncher()
                    logger.info(f"[DOWNLOAD] [Playwright] browser relaunch succeeded")
                except Exception as relaunch_err:
                    logger.error(f"[DOWNLOAD] [Playwright] browser relaunch failed: {relaunch_err}")
                    raise
            else:
                raise RuntimeError("browser is disconnected and relauncher is not available")
        
        # 釉뚮씪?곗? 而⑦뀓?ㅽ듃 ?앹꽦 (?ㅼ슫濡쒕뱶 寃쎈줈 ?ㅼ젙)
        context = await _create_browser_context()

        # fileDown.do + ?곸꽭 URL???덉쑝硫? 留곹겕 ?먯깋/expect_download(湲곕낯 ?섏떗~120珥? ?꾩뿉
        # ?숈씪 而⑦뀓?ㅽ듃??request濡?諛붿씠?덈━瑜?諛쏅뒗 寃쎈줈瑜?癒쇱? ?쒕룄?쒕떎.
        if _is_portal_direct_file_url(url) and (source_page or "").strip() and not _is_identity_kisa_url(url):
            try:
                early_info = await _download_via_context_request()
                if early_info:
                    try:
                        await context.close()
                    except Exception:
                        pass
                    return early_info
            except Exception as early_exc:
                if _is_target_closed_error(early_exc):
                    raise
                logger.info(
                    "[Download] %s[Playwright] early context.request miss (continue page flow) | url=%s err=%s",
                    wtag,
                    _short(url, 180),
                    _short(early_exc, 160),
                )
        
        page = await _create_page_with_recovery()
        
        # source_page濡??대룞
        # - ?쇰? ?ъ씠?몃뒗 由ъ냼???ㅽ겕由쏀듃 濡쒕뵫???먮젮 domcontentloaded媛 ?ㅻ옒 嫄몃┫ ???덈떎.
        # - ??寃쎌슦?먮룄 ?섏씠吏??"遺遺?濡쒕뱶"???곹깭?????덉쑝誘濡? goto timeout? 移섎챸?쇰줈 蹂댁? ?딄퀬
        #   留곹겕 ?먯깋/?ㅼ슫濡쒕뱶瑜?怨꾩냽 ?쒕룄?쒕떎.
        # - ERR_ABORTED / frame detached???ㅼ슫濡쒕뱶 ?몃━嫄?由щ떎?대젆???꾨젅??援먯껜濡??뷀엳 諛쒖깮?섎?濡?移섎챸?쇰줈 蹂댁? ?딅뒗??
        _flow_debug_print(f"[Download] {wtag}[Playwright] goto source page: {source_page}")
        if _is_eminwon_filedown_url(url):
            try:
                await asyncio.sleep(_eminwon_pre_goto_delay_sec())
            except Exception:
                pass
        try:
            goto_timeout_ms = int(os.getenv("DOWNLOAD_PLAYWRIGHT_SOURCE_GOTO_TIMEOUT_MS", "60000") or "60000")
        except Exception:
            goto_timeout_ms = 60000
        goto_timeout_ms = max(5000, min(int(goto_timeout_ms), 120000))

        async def _goto_source_page_once(wait_until: str) -> bool:
            try:
                nav_target, evaluate_js_directly = _choose_playwright_navigation_target(url, source_page)
                js_code = _strip_javascript_prefix(url)
                if nav_target:
                    await page.goto(nav_target, wait_until="commit", timeout=goto_timeout_ms, referer=source_page or None)
                    if wait_until != "commit":
                        try:
                            await page.wait_for_load_state(wait_until, timeout=min(goto_timeout_ms, 15000))
                        except PlaywrightTimeoutError:
                            logger.debug(
                                "[DOWNLOAD] [Playwright] source_page load_state timeout (continue) | wait_until=%s source_page=%s",
                                wait_until,
                                nav_target,
                            )
                elif evaluate_js_directly and js_code:
                    if not await _evaluate_javascript_url(page, url):
                        logger.info(
                            "[DOWNLOAD] [Playwright] javascript handler missing or direct evaluate failed; skip direct evaluate | url=%s source_page=%s",
                            _short(url, 220),
                            _short(source_page, 220),
                        )
                        return False
                else:
                    raise RuntimeError(f"Playwright could not determine a navigation target for url: {url}")
                return True
            except PlaywrightError as goto_err:
                msg = str(goto_err).lower()
                # 1以?二쇱꽍: ?ㅼ슫濡쒕뱶媛 ?쒖옉?섎㈃??諛쒖깮?섎뒗 以묐떒 ?먮윭???뺤긽?곸씤 ?먮쫫?쇰줈 媛꾩＜?섏뿬 ?덉쇅泥섎━??
                if "download is starting" in msg or "net::err_aborted" in msg:
                    logger.debug(f"[DOWNLOAD] [Playwright] download trigger succeeded (expected interruption): {url}")
                elif "err_connection_reset" in msg or "connection reset" in msg:
                    # ?쇰? 怨듦났留?WAF ?섍꼍?먯꽌 媛꾪뿉?곸쑝濡?reset 諛쒖깮: 吏㏐쾶 ?湲????곸쐞 ?ъ떆??猷⑦봽濡??섍릿??
                    try:
                        backoff = float(os.getenv("DOWNLOAD_EMINWON_CONNRESET_BACKOFF_SEC", "2.5") or "2.5")
                    except Exception:
                        backoff = 2.5
                    backoff = max(0.2, min(backoff, 30.0))
                    logger.warning(
                        "[DOWNLOAD] [Playwright] source_page connection reset (will retry) | source_page=%s backoff=%.1fs err=%s",
                        source_page,
                        backoff,
                        _short(goto_err, 220),
                    )
                    try:
                        await asyncio.sleep(backoff)
                    except Exception:
                        pass
                    return False
                else:
                    raise
                try:
                    _flow_debug_print(f" download goto timeout (continue) | source_page={source_page}")
                except Exception:
                    pass
                return False
            except PlaywrightError as goto_err:
                error_msg = str(goto_err).lower()
                # Browser/Target closed => ?ъ떆?꾪빐???섎? ?놁쑝誘濡??곸쐞?먯꽌 泥섎━
                if "target closed" in error_msg or "browser has been closed" in error_msg:
                    logger.warning("[DOWNLOAD] [Playwright] browser closed during page goto | source_page=%s err=%s", source_page, goto_err)
                    raise
                # ?뷀븳 鍮꾩튂紐??ㅻ퉬寃뚯씠??以묐떒 耳?댁뒪: 怨꾩냽 吏꾪뻾(吏곸젒 ?ㅼ슫濡쒕뱶 goto/expect_download濡??고쉶 媛??
                if (
                    "net::err_aborted" in error_msg
                    or "err_aborted" in error_msg
                    or "frame was detached" in error_msg
                    or "detached" in error_msg
                    or "navigation" in error_msg and "interrupted" in error_msg
                ):
                    logger.warning("[DOWNLOAD] [Playwright] source_page goto aborted/detached (continue) | source_page=%s err=%s", source_page, goto_err)
                    try:
                        _flow_debug_print(f" download goto aborted/detached (continue) | source_page={source_page} err={goto_err}")
                    except Exception:
                        pass
                    return False
                # 洹??몃뒗 湲곗〈?濡?移섎챸 泥섎━(?먯씤 ?뚯븙 ?꾩슂)
                raise

        # 2?뚭퉴吏 ?쒕룄:
        # - 1李? domcontentloaded
        # - ?ㅽ뙣 ??2李? commit (??媛踰쇱슫 wait_until, ?쇰? ?ъ씠?몄뿉?????곗쭚)
        ok = False
        try:
            ok = await _goto_source_page_once("domcontentloaded")
        except Exception:
            raise
        if not ok:
            # frame detached ?댄썑 page媛 遺덉븞?뺥븷 ???덉쑝?????섏씠吏濡?援먯껜 ????踰????쒕룄
            try:
                try:
                    await page.close()
                except Exception:
                    pass
                page = None
                page = await _create_page_with_recovery()
            except Exception:
                # ?섏씠吏 援먯껜 ?ㅽ뙣?대룄 ?꾨옒 吏곸젒 ?ㅼ슫濡쒕뱶 fallback???숈옉?????덉쓬(?섏씠吏媛 None? ?꾨떂)
                pass
            try:
                await _goto_source_page_once("commit")
            except Exception:
                raise

        # eminwon FileDownNew ?? ?곸꽭(source) 濡쒕뱶濡??몄뀡쨌荑좏궎 ?뺣낫 吏곹썑, ?먮┛ expect_download+goto(?뚯씪 URL) ?꾩뿉
        # ?숈씪 而⑦뀓?ㅽ듃 request濡?諛붿씠?덈━ ?섏떊??癒쇱? ?쒕룄?쒕떎.
        if _is_eminwon_filedown_url(url) and (source_page or "").strip():
            try:
                eminwon_info = await _download_via_context_request()
                if eminwon_info:
                    try:
                        await context.close()
                    except Exception:
                        pass
                    return eminwon_info
            except Exception as em_exc:
                if _is_target_closed_error(em_exc):
                    raise
                logger.info(
                    "[Download] %s[Playwright] eminwon post-source request miss (continue link/goto) | url=%s err=%s",
                    wtag,
                    _short(url, 180),
                    _short(em_exc, 200),
                )

        # ?뚯씪 URL??媛由ы궎??留곹겕 李얘린
        # ?щ윭 ?좏깮???쒕룄: href ?띿꽦??file_url???ы븿??留곹겕
        selector_paths: list[str] = []
        for candidate_url in (url, raw_url):
            try:
                path = (urlparse(candidate_url).path or "").strip()
            except Exception:
                path = ""
            if path and path not in selector_paths:
                selector_paths.append(path)
        link = None
        for path in selector_paths:
            link = await _safe_query_selector(f'a[href*="{path}"]')
            if link:
                break
        
        if not link:
            # ???볦? 踰붿쐞濡?寃?? href???뚯씪紐낆씠???ㅼ슫濡쒕뱶 愿???ㅼ썙???ы븿
            try:
                all_links = await page.query_selector_all("a")
                for candidate_link in all_links:
                    try:
                        href = await candidate_link.get_attribute('href')
                        onclick = await candidate_link.get_attribute('onclick')
                        if _attachment_link_matches(href, onclick):
                            link = candidate_link
                            break
                    except (PlaywrightError, RuntimeError, AttributeError, ValueError, OSError) as attr_err:
                        error_msg = str(attr_err).lower()
                        if "target closed" in error_msg or "browser has been closed" in error_msg:
                            logger.warning(f"[DOWNLOAD] [Playwright] target closed during get_attribute; skipping")
                            continue
                        logger.debug(f"[DOWNLOAD] [Playwright] get_attribute ?ㅻ쪟 (臾댁떆, 猷⑦봽 ?좎?): {attr_err}")
                        continue
                    except Exception as attr_err:
                        logger.debug(f"[DOWNLOAD] [Playwright] get_attribute ?덉쇅 ?ш큵 (臾댁떆, 猷⑦봽 ?좎?): {type(attr_err).__name__} {attr_err}")
                        continue
            except PlaywrightError as query_err:
                error_msg = str(query_err).lower()
                if "target closed" in error_msg or "browser has been closed" in error_msg:
                    logger.warning(f"[DOWNLOAD] [Playwright] page closed while searching links")
                    raise
                if "execution context was destroyed" in error_msg or "navigation" in error_msg:
                    logger.warning(
                        "[DOWNLOAD] [Playwright] 留곹겕 紐⑸줉 寃??以??ㅻ퉬寃뚯씠??媛먯? (臾댁떆)",
                    )
                    all_links = []
                    # fall through
                else:
                    raise

        if not link and str(url or "").strip().lower().startswith("javascript:"):
            try:
                clickable_candidates = await page.query_selector_all("[onclick]")
                for candidate_link in clickable_candidates:
                    try:
                        href = await candidate_link.get_attribute("href")
                        onclick = await candidate_link.get_attribute("onclick")
                        if _attachment_link_matches(href, onclick):
                            link = candidate_link
                            break
                    except (PlaywrightError, RuntimeError, AttributeError, ValueError, OSError) as attr_err:
                        logger.debug(
                            "[DOWNLOAD] [Playwright] onclick candidate inspect failed (ignored): %s",
                            attr_err,
                        )
                        continue
            except PlaywrightError as query_err:
                logger.debug(
                    "[DOWNLOAD] [Playwright] onclick candidate search failed (ignored): %s",
                    query_err,
                )

        if not link and str(url or "").strip().lower().startswith("javascript:"):
            js_code = _strip_javascript_prefix(url)
            if js_code:
                try:
                    expect_ms = _playwright_expect_timeout_ms()
                    if _is_identity_kisa_url(url) or _is_identity_kisa_url(source_page):
                        raise RuntimeError("identity_kisa_click_timeout: direct fallback disabled")
                    async with page.expect_download(timeout=expect_ms) as download_info:
                        triggered = await _evaluate_javascript_url(page, url)
                        if not triggered:
                            raise RuntimeError("javascript handler missing on current page")
                    download = await download_info.value
                except Exception:
                    logger.debug(
                        "[DOWNLOAD] [Playwright] guarded javascript evaluate download failed; continue fallback | url=%s",
                        url,
                        exc_info=True,
                    )

        if not link and "download" not in locals():
            # 2李?fallback:
            # - source_page?먯꽌 留곹겕 ?먯깋???ㅽ뙣?섍굅?? source_page 濡쒕뵫??遺덉셿?꾪븳 寃쎌슦媛 ?덈떎.
            # - fileDown.do 媛숈? direct download handler??URL濡?吏곸젒 ?대룞?대룄 ?ㅼ슫濡쒕뱶媛 ?몃━嫄곕맆 ???덉쑝誘濡?
            #   expect_download + goto(url)濡??ъ떆?꾪븳??
            if _should_skip_portal_direct_download_event_wait(url) and not _is_identity_kisa_url(url):
                logger.info(
                    "[Download] %s[Playwright] skip direct download event wait for portal direct url; rely on warmed request fallback only | url=%s",
                    wtag,
                    url,
                )
                try:
                    fallback_info = await _download_via_context_request()
                    if fallback_info:
                        return fallback_info
                except Exception as fallback_exc:
                    if _is_target_closed_error(fallback_exc):
                        raise
                    logger.info(
                        "[Download] %s[Playwright] portal direct request fallback miss; continue direct navigation path | url=%s err=%s",
                        wtag,
                        url,
                        _short(fallback_exc, 240),
                    )
            if _is_identity_kisa_url(url) or _is_identity_kisa_url(source_page):
                raise RuntimeError("identity_kisa_link_not_found: direct fallback disabled")
            try:
                expect_ms = _playwright_expect_timeout_ms()
                if is_portal_direct and _portal_direct_fail_fast_enabled():
                    expect_ms = min(expect_ms, _portal_direct_expect_timeout_ms())
                logger.info(
                    "[Download] %s[Playwright] link not found; trying direct download navigation | url=%s expect_timeout_ms=%s",
                    wtag, url, expect_ms,
                )
                async with page.expect_download(timeout=expect_ms) as download_info:
                    if not _can_direct_navigate_download_url(url):
                        raise RuntimeError(f"Playwright direct navigation is unavailable for javascript url: {url}")
                    # direct download URL濡?goto ??"Download is starting" ?덉쇅 ?먮뒗 Timeout??諛쒖깮?????덉쓬.
                    try:
                        await page.goto(url, wait_until="commit", timeout=expect_ms, referer=source_page or None)
                    except PlaywrightTimeoutError as te:
                        # ??꾩븘?껋? ?ㅽ듃?뚰겕/?쒕쾭 吏???먮뒗 釉뚮씪?곗? ?대깽??誘몃컻???볦씪 ???덉쑝誘濡?
                        # 利됱떆 而⑦뀓?ㅽ듃 request ?대갚???쒕룄?대낯??
                        logger.debug(
                            "[Download] %s[Playwright] direct goto timeout -> attempting context.request fallback | url=%s timeout_ms=%s err=%s",
                            wtag, url, expect_ms, _short(te, 200),
                        )
                        # close the expect_download context by cancelling it (context manager will handle)
                        raise te
                    except PlaywrightError as goto_err:
                        msg = str(goto_err).lower()
                        if "download is starting" in str(goto_err).lower():
                            logger.debug(
                                "[DOWNLOAD] [Playwright] direct goto raised 'Download is starting' (treated as success) | url=%s",
                                url,
                            )
                        else:
                            raise

                download = await download_info.value
                # below: reuse existing save logic
            except PlaywrightTimeoutError as direct_timeout:
                logger.warning(
                    "[DOWNLOAD] [Playwright] direct goto timed out; attempting context.request fallback | url=%s err=%s",
                    url,
                    _short(direct_timeout, 240),
                )
                try:
                    fallback_info = await _download_via_context_request()
                    if fallback_info:
                        return fallback_info
                except Exception as fallback_exc:
                    if _is_target_closed_error(fallback_exc):
                        raise
                    if _is_not_found_download_error(fallback_exc):
                        raise FileNotFoundError(
                            f"Playwright request fallback returned 404 for url: {url}"
                        ) from fallback_exc
                    logger.warning(
                        "[DOWNLOAD] [Playwright] context.request fallback failed after timeout | url=%s err=%s",
                        url,
                        _short(fallback_exc, 240),
                        exc_info=True,
                    )
                # if fallback failed, raise a TimeoutError to allow upstream retry logic to handle it
                raise TimeoutError(f"Playwright direct goto timed out and request fallback failed: {url}") from direct_timeout
            except Exception as direct_exc:
                logger.warning(
                    "[DOWNLOAD] [Playwright] direct goto failed; attempting context.request fallback | url=%s err=%s",
                    url,
                    _short(direct_exc, 240),
                )
                # try Playwright context request fallback (keeps cookies/headers)
                try:
                    fallback_info = await _download_via_context_request()
                    if fallback_info:
                        return fallback_info
                except Exception as fallback_exc:
                    if _is_target_closed_error(fallback_exc):
                        raise
                    if _is_not_found_download_error(fallback_exc):
                        raise FileNotFoundError(
                            f"Playwright request fallback returned 404 for url: {url}"
                        ) from fallback_exc
                    logger.warning(
                        "[DOWNLOAD] [Playwright] context.request fallback failed | url=%s err=%s",
                        url,
                        _short(fallback_exc, 240),
                        exc_info=True,
                    )
                # All fallbacks failed; propagate original error
                raise
        
        # ?ㅼ슫濡쒕뱶 ?湲?諛?留곹겕 ?대┃
        _flow_debug_print(f"[Download] {wtag}[Playwright] click download link: {url}")
        if "download" not in locals():
            try:
                expect_ms = _playwright_expect_timeout_click_ms()
                if is_portal_direct and _portal_direct_fail_fast_enabled():
                    expect_ms = min(expect_ms, _portal_direct_expect_timeout_ms())
                if _should_skip_portal_direct_download_event_wait(url) and not _is_identity_kisa_url(url):
                    try:
                        fallback_info = await _download_via_context_request()
                        if fallback_info:
                            return fallback_info
                    except Exception as fallback_exc:
                        if _is_target_closed_error(fallback_exc):
                            raise
                        logger.info(
                            "[Download] %s[Playwright] portal direct request fallback miss before click; continue short click wait | url=%s err=%s",
                            wtag,
                            url,
                            _short(fallback_exc, 240),
                        )
                try:
                    async with page.expect_download(timeout=expect_ms) as download_info:
                        try:
                            await link.click()
                        except PlaywrightError as click_err:
                            error_msg = str(click_err).lower()
                            if "target closed" in error_msg or "browser has been closed" in error_msg:
                                logger.warning(f"[DOWNLOAD] [Playwright] target closed while clicking link")
                                raise
                            else:
                                raise
                    download = await download_info.value
                except PlaywrightTimeoutError:
                    logger.warning(
                        "[Download] %s[Playwright] link click did not emit download before timeout; retrying direct goto | url=%s",
                        wtag, url,
                    )
                    if _should_skip_portal_direct_download_event_wait(url) and not _is_identity_kisa_url(url):
                        try:
                            fallback_info = await _download_via_context_request()
                            if fallback_info:
                                return fallback_info
                        except Exception as fallback_exc:
                            if _is_target_closed_error(fallback_exc):
                                raise
                            logger.info(
                                "[Download] %s[Playwright] portal direct request fallback miss after click timeout; continue direct goto | url=%s err=%s",
                                wtag,
                                url,
                                _short(fallback_exc, 240),
                            )
                    # 留곹겕 ?대┃?쇰줈 ?ㅼ슫濡쒕뱶 ?대깽?멸? 諛쒖깮?섏? ?딅뒗 ?ъ씠?몃? ?꾪븳 fallback
                    try:
                        async with page.expect_download(timeout=expect_ms) as download_info:
                            if not _can_direct_navigate_download_url(url):
                                raise RuntimeError(f"Playwright direct navigation is unavailable for javascript url: {url}")
                            try:
                                await page.goto(url, wait_until="commit", timeout=expect_ms, referer=source_page or None)
                            except PlaywrightError as goto_err:
                                msg = str(goto_err).lower()
                                if "download is starting" in msg:
                                    logger.debug(
                                        "[DOWNLOAD] [Playwright] direct goto raised 'Download is starting' (treated as success) | url=%s",
                                        url,
                                    )
                                else:
                                    raise
                        download = await download_info.value
                    except PlaywrightTimeoutError as goto_timeout:
                        logger.info(
                            "[Download] %s[Playwright] direct goto did not emit download before timeout; trying context.request fallback | url=%s timeout_ms=%s",
                            wtag,
                            url,
                            expect_ms,
                        )
                        try:
                            fallback_info = await _download_via_context_request()
                            if fallback_info:
                                return fallback_info
                        except Exception as fallback_exc:
                            if _is_target_closed_error(fallback_exc):
                                raise
                            if _is_not_found_download_error(fallback_exc):
                                raise FileNotFoundError(
                                    f"Playwright request fallback returned 404 for url: {url}"
                                ) from fallback_exc
                            logger.info(
                                "[Download] %s[Playwright] context.request fallback miss after direct goto timeout | url=%s err=%s",
                                wtag,
                                url,
                                _short(fallback_exc, 240),
                            )
                        raise TimeoutError(
                            f"Playwright download event timeout after click and direct goto: {url}"
                        ) from goto_timeout
                except PlaywrightError as download_err:
                    error_msg = str(download_err).lower()
                    if "target closed" in error_msg or "browser has been closed" in error_msg:
                        logger.warning(f"[DOWNLOAD] [Playwright] target closed while waiting for download")
                        raise
                    else:
                        raise
            except PlaywrightError as download_err:
                error_msg = str(download_err).lower()
                if "target closed" in error_msg or "browser has been closed" in error_msg:
                    logger.warning(f"[DOWNLOAD] [Playwright] target closed while waiting for download")
                    raise
                else:
                    raise
        
        # ?ㅼ슫濡쒕뱶 寃쎈줈 ?ㅼ젙 (湲곗〈 濡쒖쭅 ?ъ궗??
        # ... (湲곗〈 download_dir ?ㅼ젙 濡쒖쭅怨??숈씪)
        final_download_dir = download_dir or default_download_dir
        
        # ?뚯씪紐?寃곗젙
        try:
            suggested_path = download.suggested_filename
        except AttributeError:
            suggested_path = suggested_name
        
        if not suggested_path or suggested_path == 'unknown':
            from uuid import uuid4
            ext = '.bin'
            if '.pdf' in url.lower(): ext = '.pdf'
            elif '.hwp' in url.lower(): ext = '.hwp'
            suggested_path = f"file_{uuid4().hex[:8]}{ext}"
        response_suggested_path = suggested_path
        suggested_path = _best_download_filename(
            suggested_path,
            url=url,
            file_meta=file_meta,
            default=suggested_path,
        )
        _trace_filename_resolution(file_meta, worker_id=worker_id, url=url, stage="playwright_download", response_filename=response_suggested_path, selected_filename=suggested_path)
        suggested_path = _strip_partial_download_suffix(suggested_path)
        
        # PHP ?듭씪: ?붿뒪?ъ뿉??md5(subject+time+uniqid).ext, DB subject?먮뒗 ?먮낯紐?
        # Keep the page-extracted attachment title for DB/learning identity;
        # the Playwright suggested filename remains the physical storage name.
        original_subject = sanitize_filename(suggested_path) or suggested_path
        if is_blocked_non_document(original_subject, ""):
            logger.info("[Download] %s[Playwright] Skipped (non-doc) | url=%s filename=%s", wtag, url, original_subject)
            return None
        storage_filename = make_safe_storage_filename(suggested_path)
        filepath = os.path.join(final_download_dir, storage_filename)
        tmp_filepath = _download_temp_path(filepath)
        
        # 湲곗〈 ?뚯씪 ??젣
        if await asyncio.to_thread(os.path.exists, filepath):
            pass
        
        # ?ㅼ슫濡쒕뱶 ?뚯씪 ???
        # - Download.save_as: canceled ??(?섏씠吏 ?대룞/?ロ옒/?ㅽ듃?뚰겕 ???쇰줈 artifact媛 痍⑥냼????諛쒖깮?????덈떎.
        # - 媛?ν븯硫?download.path()濡?"?꾨즺 ?湲? ??save_as瑜??몄텧??痍⑥냼 ?뺣쪧????텣??
        try:
            try:
                _path_wait = float(os.getenv("DOWNLOAD_PLAYWRIGHT_PATH_WAIT_SEC", "15") or "15")
            except Exception:
                _path_wait = 15.0
            _path_wait = max(5.0, min(_path_wait, 120.0))
            try:
                # path()???ㅼ슫濡쒕뱶 ?꾨즺源뚯? 湲곕떎由곕떎. (?쇰? 釉뚮씪?곗?/?섍꼍?먯꽌 None?????덉뼱 ?덉쇅??臾댁떆)
                await asyncio.wait_for(download.path(), timeout=_path_wait)
            except Exception:
                pass
            await download.save_as(tmp_filepath)
        except PlaywrightError as save_err:
            await asyncio.to_thread(_remove_file_quietly, tmp_filepath)
            msg = str(save_err).lower()
            if "canceled" in msg or "cancelled" in msg:
                # ?곸쐞?먯꽌 ?ъ떆??濡쒓퉭 ?뺤콉???곸슜?????덈룄濡?紐낆떆?곸씤 ?먮윭濡??섑븨
                raise RuntimeError(f"Playwright download canceled: {url}") from save_err
            raise
        
        # ?뚯씪 ?ш린 ?뺤씤
        # - save_as ?꾩뿉???뚯씪????쾶 ?앹꽦?섍굅??寃쎈줈媛 ?꾨씫?????덉뼱 議댁옱 ?щ?瑜??ы솗?명븳??
        try:
            file_size = await wait_for_file_ready(
                tmp_filepath,
                timeout_sec=_download_temp_ready_timeout_sec(),
                allow_partial_name=True,
                check_partial_siblings=False,
            )
            suggested_path = _ensure_download_filename_extension(
                suggested_path,
                filepath=tmp_filepath,
                content_type="file",
                url=url,
            )
            suggested_path = _strip_partial_download_suffix(suggested_path)
            original_subject = sanitize_filename(suggested_path) or suggested_path
            storage_filename = make_safe_storage_filename(suggested_path)
            filepath = os.path.join(final_download_dir, storage_filename)
        except FileNotFoundError as size_err:
            raise RuntimeError(f"Playwright download file missing at getsize: {filepath}") from size_err
        try:
            await asyncio.to_thread(
                _validate_downloaded_file,
                tmp_filepath,
                filename=original_subject,
                url=url,
                content_type="file",
                actual_size=file_size,
            )
            await asyncio.to_thread(_replace_file, tmp_filepath, filepath)
            # The unique temporary file was validated before the atomic replace.
            # A stale sibling from another attempt must not block this finalized file.
            file_size = await wait_for_file_ready(
                filepath,
                timeout_sec=_download_final_ready_timeout_sec(),
                stable_checks=3,
                check_partial_siblings=False,
            )
        except Exception:
            await asyncio.to_thread(_remove_file_quietly, tmp_filepath)
            raise
        
        if file_size == 0:
            raise ValueError("downloaded file size is 0 bytes")
        
        source_page = file_meta.get('source_page', 'N/A')
        _flow_debug_print(f"[Download] {wtag}[Playwright] Saved: {storage_filename}")
        _flow_debug_print(f"[Download] {wtag}[Playwright] Full path: {filepath}")
        _flow_debug_print(f"[Download] {wtag}[Playwright] File size: {file_size} bytes")
        _flow_debug_print(f"[Download] {wtag}[Playwright] post URL: {source_page}")
        _flow_debug_print(f"[Download] {wtag}[Playwright] file URL: {url}")
        # Optional flow trace for HWP/HWPX downloads and study-worker routing.
        try:
            skip_study = bool(file_meta.get('skip_study_worker'))
            if (storage_filename or "").lower().endswith('.hwp') or (original_subject or "").lower().endswith('.hwp'):
                _flow_debug_print(f"[Download][Flow] hwp_saved_playwright file={storage_filename} path={filepath} size={file_size} skip_study={skip_study}")
            _flow_debug_print(f"[Download][Flow] study_flag url={url} skip_study_worker={skip_study}")
        except Exception:
            pass
        logger.debug("[Download] %s[Playwright] File saved successfully: %s (size: %s bytes) | post_url=%s | file_url=%s", wtag, filepath, file_size, source_page, url)
        
        # Treat Playwright direct download as an equivalent streamed download.
        return {
            'file_path': filepath,
            'local_path': filepath,
            'url': url,
            'name': original_subject,
            'subject': original_subject,
            'display_name': original_subject,
            'attachment_name': original_subject,
            'storage_filename': storage_filename,
            'size': file_size,
            'content_type': 'file',
            'original_meta': file_meta,
            **_learn_list_ids_from_file_meta(file_meta),
            **_defer_save_batch_flag(file_meta),
        }
        
    except PlaywrightTimeoutError as timeout_exc:
        logger.warning(
            "[DOWNLOAD] [Playwright] 다운로드 타임아웃, request fallback 시도 | url=%s err=%s",
            url,
            timeout_exc,
        )
        try:
            fallback_info = await _download_via_context_request()
            return fallback_info
        except Exception as fallback_exc:
            if _is_target_closed_error(fallback_exc):
                raise
            logger.warning(
                "[DOWNLOAD] [Playwright] request fallback 실패 | url=%s err=%s",
                url,
                fallback_exc,
                exc_info=True,
            )
            raise TimeoutError(f"Playwright 다운로드 타임아웃: {url}") from timeout_exc
    except PlaywrightError as pw_err:
        error_msg = str(pw_err).lower() # ?먮윭 硫붿떆吏瑜??뚮Ц?먮줈 蹂?섑븯??遺꾩꽍
        # 釉뚮씪?곗?媛 媛뺤젣濡??ロ삍?붿? ?뺤씤?섎뒗 議곌굔臾?
        if "target closed" in error_msg or "browser has been closed" in error_msg:
            logger.warning(f"[DOWNLOAD] [Playwright] TargetClosedError 諛쒖깮: {pw_err}")
            
            # [?섏젙] (target closed) ?쇰뒗 ?곷Ц ?ㅼ썙?쒕? 硫붿떆吏??諛섎뱶???ы븿?쒖폒???⑸땲??
            # 洹몃옒???곸쐞 download_item??is_closed 濡쒖쭅???대? ?≪븘??釉뚮씪?곗?瑜??ъ떆?묓빀?덈떎.
            raise RuntimeError(f"browser/page closed (target closed): {url}") from pw_err
        else:
            # ?ㅻⅨ 醫낅쪟??Playwright ?먮윭??湲곗〈?濡??ㅼ떆 ?섏쭚
            raise
    finally:
        # ?섏씠吏? 而⑦뀓?ㅽ듃瑜?媛곴컖 ?낅┰?곸쑝濡??レ븘 醫鍮??꾨줈?몄뒪瑜?諛⑹??⑸땲??
        if page:
            try:
                await page.close() # ?꾩옱 ?대┛ ?섏씠吏瑜?媛뺤젣濡??レ뒿?덈떎.
            except Exception: 
                pass # ?リ린 ?ㅽ뙣 ??濡쒓렇瑜??④린吏 ?딄퀬 ?ㅼ쓬 ?④퀎濡??섏뼱媛묐땲??
        
        if context:
            try:
                await context.close() # 釉뚮씪?곗? ?몄뀡(而⑦뀓?ㅽ듃)??醫낅즺?⑸땲??
            except Exception: 
                pass # 醫낅즺 ?ㅽ뙣 ?쒖뿉???꾨줈?몄뒪 諛⑹튂瑜?留됯린 ?꾪빐 臾댁떆?⑸땲??
        
        try:
            if context:
                await context.close()
        except Exception as ctx_close_err:
            logger.debug(f"[DOWNLOAD] [Playwright] 而⑦뀓?ㅽ듃 ?リ린 以??ㅻ쪟 (臾댁떆): {ctx_close_err}")

async def download_worker(
    in_queue: BatchQueue, 
    out_queue: BatchQueue, 
    progress_queue: asyncio.Queue,
    max_concurrent: int = 30,
    browser=None,  # Playwright Browser ?몄뒪?댁뒪 (fallback??
    browser_relauncher: Optional[Callable[[], Awaitable[Browser]]] = None,
    browser_getter: Optional[Callable[[], Optional[Browser]]] = None,
    browser_releaser: Optional[Callable[[Optional[Browser]], None]] = None,
    worker_id: int = 0,
    worker_lane: str = 'normal',
    large_download_queue: Optional[BatchQueue] = None,
    shared_download_semaphore: Optional[asyncio.Semaphore] = None,
):
    """
    Download Worker (real-time):
    - Consumes batches from in_queue (CollectionBatchQueue).
    - Uses a semaphore for parallel downloads.
    - Emits successful file paths to out_queue (SaveBatchQueue).
    - Reports progress through progress_queue.
    """
    # 湲곕낯 ?ㅼ슫濡쒕뱶 ?붾젆?좊━ (fallback??
    default_download_dir = str(settings.DOWNLOAD_PATH)
    
    # ?ㅼ슫濡쒕뱶 寃쎈줈 罹먯떆 (理쒖큹 1?뚮쭔 ?앹꽦)
    download_path_cache = {}  # key: (db_name, chat_bot_id, domain) -> download_dir
    
    # User-Agent ?ㅻ뜑 異붽?
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    sem = shared_download_semaphore or asyncio.Semaphore(max_concurrent)
    # ?꾨찓?몃퀎 ?숈떆???쒖뼱(怨듦났留?WAF 誘쇨컧 ?꾨찓??蹂댄샇)
    strict_hosts_raw = (os.getenv("DOWNLOAD_STRICT_DOMAIN_HOSTS") or "gwangjin.go.kr,gwangjin.eminwon.seoul.kr").strip()
    strict_hosts = {h.strip().lower() for h in strict_hosts_raw.split(",") if h.strip()}
    try:
        strict_domain_limit = int(os.getenv("DOWNLOAD_STRICT_DOMAIN_CONCURRENCY", "1") or "1")
    except Exception:
        strict_domain_limit = 1
    strict_domain_limit = max(1, min(strict_domain_limit, 4))
    try:
        cooldown_min_sec = float(os.getenv("DOWNLOAD_STRICT_DOMAIN_COOLDOWN_MIN_SEC", "0") or "0")
    except Exception:
        cooldown_min_sec = 0.0
    try:
        cooldown_max_sec = float(os.getenv("DOWNLOAD_STRICT_DOMAIN_COOLDOWN_MAX_SEC", "0") or "0")
    except Exception:
        cooldown_max_sec = 0.0
    cooldown_min_sec = max(0.0, min(cooldown_min_sec, 3600.0))
    cooldown_max_sec = max(cooldown_min_sec, min(cooldown_max_sec, 3600.0))
    try:
        http_timeout = float(os.getenv("DOWNLOAD_HTTP_TIMEOUT_SEC", "30") or "30")
    except Exception:
        http_timeout = 30.0
    http_timeout = max(5.0, min(http_timeout, 60.0))
    try:
        http_retries = int(os.getenv("DOWNLOAD_HTTP_RETRIES", "2") or "2")
    except Exception:
        http_retries = 2
    http_retries = max(1, min(http_retries, 3))
    try:
        pw_attempts = int(os.getenv("DOWNLOAD_PLAYWRIGHT_MAX_ATTEMPTS_PER_URL", "2") or "2")
    except Exception:
        pw_attempts = 2
    pw_attempts = max(1, min(pw_attempts, 4))
    fail_fast_hosts_raw = (os.getenv("DOWNLOAD_FAIL_FAST_DOMAINS") or "").strip()
    fail_fast_hosts = {h.strip().lower() for h in fail_fast_hosts_raw.split(",") if h.strip()}
    try:
        fail_fast_http_timeout = float(os.getenv("DOWNLOAD_FAIL_FAST_HTTP_TIMEOUT_SEC", "5") or "5")
    except Exception:
        fail_fast_http_timeout = 5.0
    fail_fast_http_timeout = max(1.0, min(fail_fast_http_timeout, 30.0))
    try:
        fail_fast_pw_attempts = int(os.getenv("DOWNLOAD_FAIL_FAST_PLAYWRIGHT_ATTEMPTS", "1") or "1")
    except Exception:
        fail_fast_pw_attempts = 1
    fail_fast_pw_attempts = max(1, min(fail_fast_pw_attempts, 2))
    not_found_cache_ttl_sec = _download_not_found_cache_ttl_sec()
    recent_not_found_urls: Dict[str, float] = {}
    try:
        access_denied_cache_ttl_sec = float(
            os.getenv("DOWNLOAD_ACCESS_DENIED_CACHE_TTL_SEC", str(not_found_cache_ttl_sec))
            or str(not_found_cache_ttl_sec)
        )
    except Exception:
        access_denied_cache_ttl_sec = not_found_cache_ttl_sec
    access_denied_cache_ttl_sec = max(30.0, min(access_denied_cache_ttl_sec, 3600.0))
    recent_access_denied_urls: Dict[str, float] = {}
    try:
        pw_fallback_limit = int(os.getenv("DOWNLOAD_PLAYWRIGHT_FALLBACK_CONCURRENCY", "1") or "1")
    except Exception:
        pw_fallback_limit = 1
    pw_fallback_limit = max(1, min(pw_fallback_limit, 8))
    playwright_fallback_sem = asyncio.Semaphore(pw_fallback_limit)
    primed_source_pages: set[str] = set()
    retry_enabled = str(os.getenv("DOWNLOAD_FAILED_RETRY_RAM_QUEUE", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
    try:
        retry_maxsize = int(os.getenv("DOWNLOAD_FAILED_RETRY_MAXSIZE", "500") or "500")
    except Exception:
        retry_maxsize = 500
    retry_maxsize = max(10, min(retry_maxsize, 10000))
    try:
        retry_delay_sec = float(os.getenv("DOWNLOAD_FAILED_RETRY_DELAY_SEC", "30") or "30")
    except Exception:
        retry_delay_sec = 30.0
    retry_delay_sec = max(1.0, min(retry_delay_sec, 1800.0))
    try:
        retry_max_attempts = int(os.getenv("DOWNLOAD_FAILED_RETRY_MAX_ATTEMPTS", "1") or "1")
    except Exception:
        retry_max_attempts = 1
    retry_max_attempts = max(0, min(retry_max_attempts, 5))
    try:
        retry_workers = int(os.getenv("DOWNLOAD_FAILED_RETRY_WORKERS", "1") or "1")
    except Exception:
        retry_workers = 1
    retry_workers = max(1, min(retry_workers, 4))
    retry_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=retry_maxsize)
    retry_seen: set[str] = set()

    def _retry_key(item: Dict[str, Any]) -> str:
        return (
            canonicalize_url_for_dedup(str(item.get("url") or item.get("_raw_url") or ""))
            or str(item.get("url") or item.get("_raw_url") or "").strip()
        )

    def _retryable_download_failure(reason: str, detail: str) -> bool:
        text = f"{reason} {detail}".lower()
        if any(token in text for token in ("non_doc", "access", "unauthorized", "forbidden", "401", "403", "404")):
            return False
        return True

    def _schedule_failed_retry(file_meta: Dict[str, Any], *, reason: str, detail: str = "") -> None:
        if not retry_enabled or retry_max_attempts <= 0:
            return
        if not _retryable_download_failure(reason, detail):
            return
        attempt = int(file_meta.get("_ram_retry_attempt", 0) or 0)
        if attempt >= retry_max_attempts:
            return
        item = dict(file_meta)
        item["_ram_retry_attempt"] = attempt + 1
        item["_ram_retry_reason"] = reason
        item["_ram_retry_detail"] = str(detail or "")[:300]
        key = _retry_key(item)
        if not key or key in retry_seen:
            return
        try:
            retry_queue.put_nowait(item)
            retry_seen.add(key)
            logger.info(
                "[DownloadRetry] scheduled | attempt=%s/%s delay=%.1fs queue=%s url=%s reason=%s detail=%s",
                attempt + 1,
                retry_max_attempts,
                retry_delay_sec,
                retry_queue.qsize(),
                _short(key, 220),
                reason,
                _short(detail, 160),
            )
        except asyncio.QueueFull:
            logger.warning(
                "[DownloadRetry] queue full; drop retry | maxsize=%s url=%s reason=%s",
                retry_maxsize,
                _short(key, 220),
                reason,
            )

    async def _failed_retry_worker(retry_worker_id: int) -> None:
        while True:
            item = await retry_queue.get()
            key = _retry_key(item)
            try:
                await asyncio.sleep(retry_delay_sec)
                try:
                    if key:
                        retry_seen.discard(key)
                except Exception:
                    pass
                logger.info(
                    "[DownloadRetry] retry_start | retry_worker=%s attempt=%s url=%s reason=%s",
                    retry_worker_id,
                    item.get("_ram_retry_attempt"),
                    _short(key, 220),
                    item.get("_ram_retry_reason"),
                )
                result = await download_item(session_for_retry, item)  # type: ignore[name-defined]
                if result and out_queue:
                    if result.get("defer_save_batch_until_learn_list"):
                        logger.debug(
                            "[DownloadRetry] out_queue skip (defer_save_batch_until_learn_list) | url=%s",
                            _short(result.get("url"), 200),
                        )
                    else:
                        await out_queue.put(result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "[DownloadRetry] retry_worker_error | retry_worker=%s url=%s err=%s",
                    retry_worker_id,
                    _short(key, 220),
                    _short(exc, 240),
                )
            finally:
                retry_queue.task_done()

    def _acquire_playwright_browser() -> Optional[Browser]:
        nonlocal browser
        current_browser: Optional[Browser] = None
        if browser_getter:
            try:
                current_browser = browser_getter()
            except Exception as exc:
                logger.debug(
                    "[Download][Worker %s] browser_getter failed; falling back to cached browser | err=%s",
                    worker_id,
                    exc,
                )
        if current_browser is not None:
            browser = current_browser
            return current_browser
        return browser

    def _release_playwright_browser(current_browser: Optional[Browser]) -> None:
        if not browser_releaser or current_browser is None:
            return
        try:
            browser_releaser(current_browser)
        except Exception as exc:
            logger.debug(
                "[Download][Worker %s] browser_releaser failed (ignored) | err=%s",
                worker_id,
                exc,
            )

    async def download_item(session, file_meta: Dict):
        """
        Collection?먯꽌 寃利앸맂 ?뚯씪???ㅼ슫濡쒕뱶
        """
        nonlocal browser  # ?몃? ?ㅼ퐫?꾩쓽 browser 蹂???낅뜲?댄듃瑜??꾪빐 ?꾩슂
        download_dir = None
        domain_sem: Optional[asyncio.Semaphore] = None
        domain_lock_acquired = False
        host = ""
        last_err_msg = ""

        async with sem:
            _set_download_activity_phase(file_meta, "url_resolve")
            source_page = _resolve_source_page_from_file_meta(file_meta)
            if source_page:
                file_meta["source_page"] = source_page
            raw_url = str(file_meta.get("url") or "").strip()
            if raw_url and not file_meta.get("_raw_url"):
                file_meta["_raw_url"] = raw_url
            url = _resolve_download_url(file_meta.get('url'), source_page)
            url, missing_portal_menu_context = _repair_portal_file_down_url_context(url, source_page, file_meta)
            url, repaired_suwon_download_url = _repair_suwon_component_file_download_url(url)
            if repaired_suwon_download_url:
                logger.warning(
                    "[Download][Worker %s] repaired malformed Suwon attachment URL | raw_url=%s repaired_url=%s post_url=%s",
                    worker_id,
                    _short(raw_url, 220),
                    _short(url, 220),
                    _short(source_page, 220),
                )
            if missing_portal_menu_context:
                file_meta["_download_empty_reason"] = "missing_post_url_menuNo"
                logger.warning(
                    "[Download][Worker %s] skip portal fileDown with missing menuNo context | url=%s raw_url=%s post_url=%s name=%s job_id=%s",
                    worker_id,
                    _short(url, 220),
                    _short(raw_url, 220),
                    _short(source_page, 220),
                    file_meta.get("name"),
                    file_meta.get("job_id"),
                )
                try:
                    await progress_queue.put(
                        {
                            "type": "download_skipped",
                            "url": url or raw_url,
                            "reason": "missing_post_url_menuNo",
                            "detail": "portal fileDown URL has empty menuNo and no detail post_url context",
                            "source_page": source_page,
                            "name": file_meta.get("name"),
                            "worker_id": worker_id,
                        }
                    )
                except Exception:
                    pass
                return None
            if not url:
                file_meta["_download_empty_reason"] = "empty_url"
                logger.warning(
                    "[Download][Worker %s] skip empty download url | source=%s name=%s job_id=%s",
                    worker_id,
                    _short(source_page, 220),
                    file_meta.get("name"),
                    file_meta.get("job_id"),
                )
                try:
                    await progress_queue.put(
                        {
                            "type": "download_skipped",
                            "url": None,
                            "reason": "empty_url",
                            "source_page": source_page,
                            "name": file_meta.get("name"),
                            "worker_id": worker_id,
                        }
                    )
                except Exception:
                    pass
                return None
            file_meta["url"] = url
            logger.info(
                "[DownloadTrace][url_resolved] job_id=%s worker=%s url=%s post_url=%s",
                file_meta.get("job_id"),
                worker_id,
                _short(url, 220),
                _short(source_page, 220),
            )
            if _is_viewer_convert_url(url):
                file_meta["_download_empty_reason"] = "viewer_convert_url"
                logger.debug(
                    "[Download][Worker %s] Skip viewer/convert URL candidate | url=%s name=%s",
                    worker_id,
                    _short(url, 220),
                    file_meta.get("name"),
                )
                try:
                    await progress_queue.put(
                        {
                            "type": "download_skipped",
                            "url": url,
                            "reason": "viewer_convert_url",
                            "detail": "viewer/convert URL is not a direct attachment download",
                            "worker_id": worker_id,
                        }
                    )
                except Exception:
                    pass
                return None
            cached_until = recent_not_found_urls.get(url)
            now_mono = time.monotonic()
            if cached_until:
                if cached_until > now_mono:
                    file_meta["_download_empty_reason"] = "recent_404_cache"
                    logger.info(
                        "[Download][Worker %s] Skip recent 404-cached url | ttl_left=%.1fs url=%s",
                        worker_id,
                        max(0.0, cached_until - now_mono),
                        _short(url, 220),
                    )
                    return None
                recent_not_found_urls.pop(url, None)
            denied_until = recent_access_denied_urls.get(url)
            if denied_until:
                if denied_until > now_mono:
                    file_meta["_download_empty_reason"] = "recent_access_denied_cache"
                    logger.info(
                        "[Download][Worker %s] Skip recent access-denied url | ttl_left=%.1fs url=%s",
                        worker_id,
                        max(0.0, denied_until - now_mono),
                        _short(url, 220),
                    )
                    return None
                recent_access_denied_urls.pop(url, None)
            per_item_pw_attempts = pw_attempts
            is_portal_direct = _is_portal_direct_file_url(url)
            is_portal_direct_fast = is_portal_direct and _portal_direct_fail_fast_enabled()
            per_item_http_retries = http_retries
            per_item_http_timeout = http_timeout
            if is_portal_direct_fast:
                per_item_pw_attempts = min(per_item_pw_attempts, 1)
                per_item_http_retries = 1
                per_item_http_timeout = min(per_item_http_timeout, _portal_direct_http_timeout_sec())
            is_component_direct_fast = _is_component_direct_download_url(url)
            is_server_side_direct = _is_server_side_direct_download_url(url)
            is_static_direct_document = _is_static_direct_document_url(url)
            is_http_only_direct = (
                is_portal_direct
                or is_component_direct_fast
                or is_server_side_direct
                or is_static_direct_document
            )
            is_suwon_component_direct = _is_suwon_component_direct_download_url(url)
            if is_component_direct_fast:
                per_item_pw_attempts = 0
                per_item_http_retries = 1
                per_item_http_timeout = min(
                    per_item_http_timeout,
                    max(3.0, min(_env_float("DOWNLOAD_COMPONENT_DIRECT_HTTP_TIMEOUT_SEC", 30.0), 30.0)),
                )
                if is_suwon_component_direct:
                    # The Suwon endpoint is a direct binary route, but it intermittently stalls
                    # beyond the generic component fail-fast budget. Keep it HTTP-only and retry.
                    per_item_http_retries = max(per_item_http_retries, 2)
                    per_item_http_timeout = max(per_item_http_timeout, 30.0)
                    logger.info(
                        "[Download][Worker %s] Suwon direct attachment retry policy | http_timeout=%.1fs retries=%s url=%s",
                        worker_id,
                        per_item_http_timeout,
                        per_item_http_retries,
                        _short(url, 220),
                    )
                else:
                    logger.info(
                        "[Download][Worker %s] direct attachment fail-fast policy | http_timeout=%.1fs url=%s",
                        worker_id,
                        per_item_http_timeout,
                        _short(url, 220),
                    )
            elif is_portal_direct or is_server_side_direct or is_static_direct_document:
                per_item_pw_attempts = 0
                per_item_http_retries = max(
                    1,
                    min(int(_env_float("DOWNLOAD_STATIC_DIRECT_HTTP_RETRIES", 2.0)), 3),
                )
                per_item_http_timeout = max(
                    5.0,
                    min(_env_float("DOWNLOAD_STATIC_DIRECT_HTTP_TIMEOUT_SEC", 30.0), 30.0),
                )
                logger.info(
                    "[Download][Worker %s] direct attachment HTTP-only policy | http_timeout=%.1fs retries=%s url=%s",
                    worker_id,
                    per_item_http_timeout,
                    per_item_http_retries,
                    _short(url, 220),
                )
            if (
                file_meta.get("defer_save_batch_until_learn_list")
                and completed_url_cached(url, stage="save")
            ):
                file_meta["_download_empty_reason"] = "completed_cache"
                logger.debug(
                    "[Download][Worker %s] Skip completed attachment already queued | url=%s",
                    worker_id,
                    _short(url, 220),
                )
                await progress_queue.put(
                    {
                        "type": "download_skipped",
                        "url": url,
                        "reason": "completed_cache",
                        "worker_id": worker_id,
                        "job_id": file_meta.get("job_id"),
                    }
                )
                return None
            if should_skip_attachment_at_scan(url, (file_meta.get("name") or "").strip()):
                file_meta["_download_empty_reason"] = "non_doc_precheck"
                logger.info(
                    "[Download][Filtered] Skip non-doc extension (pre-check) | worker_id=%s url=%s",
                    worker_id,
                    _short(url, 220),
                )
                try:
                    await progress_queue.put(
                        {
                            "type": "download_skipped",
                            "url": url,
                            "reason": "non_doc_precheck",
                            "worker_id": worker_id,
                        }
                    )
                except Exception:
                    pass
                return None
            try:
                host = (urlparse(str(url)).hostname or "").lower()
            except Exception:
                host = ""
            is_fail_fast_host = bool(host and any(host == h or host.endswith("." + h) for h in fail_fast_hosts))
            if is_fail_fast_host:
                per_item_pw_attempts = min(per_item_pw_attempts, fail_fast_pw_attempts)
                per_item_http_retries = 1
                per_item_http_timeout = min(per_item_http_timeout, fail_fast_http_timeout)
                logger.info(
                    "[Download][Worker %s] fail-fast domain policy | host=%s http_timeout=%.1fs pw_attempts=%s url=%s",
                    worker_id,
                    host,
                    per_item_http_timeout,
                    per_item_pw_attempts,
                    _short(url, 180),
                )
            try:
                if host:
                    domain_limit = _download_domain_concurrency_limit(host, strict_hosts, strict_domain_limit)
                    domain_sem = _get_download_domain_semaphore(
                        host,
                        domain_limit,
                        job_id=str(file_meta.get("job_id") or ""),
                    )
                    domain_lock_wait_started = time.monotonic()
                    _set_download_activity_phase(file_meta, "domain_slot_wait")
                    logger.info(
                        "[DownloadTrace][domain_wait] job_id=%s worker=%s host=%s limit=%s url=%s post_url=%s",
                        file_meta.get("job_id"),
                        worker_id,
                        host,
                        domain_limit,
                        _short(url, 220),
                        _short(source_page, 220),
                    )
                    logger.info(
                        "[Download][Worker %s] domain lock waiting | host=%s limit=%s url=%s post_url=%s",
                        worker_id,
                        host,
                        domain_limit,
                        _short(url, 220),
                        _short(source_page, 220),
                    )
                    if is_http_only_direct:
                        direct_lock_timeout_sec = max(
                            1.0,
                            min(_env_float("DOWNLOAD_COMPONENT_DIRECT_DOMAIN_WAIT_SEC", 5.0), 30.0),
                        )
                        try:
                            await asyncio.wait_for(
                                domain_sem.acquire(),
                                timeout=direct_lock_timeout_sec,
                            )
                        except asyncio.TimeoutError:
                            reason = f"domain_slot_timeout:{direct_lock_timeout_sec:.0f}s"
                            file_meta["_download_empty_reason"] = reason
                            file_meta["_download_last_error"] = reason
                            logger.warning(
                                "[DownloadTrace][domain_lock_timeout] job_id=%s worker=%s host=%s limit=%s wait_sec=%.1f url=%s post_url=%s name=%s",
                                file_meta.get("job_id"),
                                worker_id,
                                host,
                                domain_limit,
                                direct_lock_timeout_sec,
                                _short(url, 220),
                                _short(source_page, 220),
                                _short(file_meta.get("name") or file_meta.get("subject"), 160),
                            )
                            try:
                                _schedule_failed_retry(
                                    file_meta,
                                    reason="domain_slot_timeout",
                                    detail=reason,
                                )
                            except Exception as retry_exc:
                                logger.exception(
                                    "[DownloadRetry] schedule_failed | job_id=%s worker=%s url=%s err=%s",
                                    file_meta.get("job_id"),
                                    worker_id,
                                    _short(url, 220),
                                    retry_exc,
                                )
                            await progress_queue.put(
                                {
                                    "type": "download_skipped",
                                    "url": url,
                                    "reason": "domain_slot_timeout",
                                    "detail": reason,
                                    "source_page": source_page,
                                    "name": file_meta.get("name") or file_meta.get("subject"),
                                    "worker_id": worker_id,
                                    "job_id": file_meta.get("job_id"),
                                }
                            )
                            return None
                    else:
                        await domain_sem.acquire()
                    domain_lock_acquired = True
                    _set_download_activity_phase(file_meta, "download_path_prepare")
                    logger.info(
                        "[DownloadTrace][domain_acquired] job_id=%s worker=%s host=%s limit=%s wait_sec=%.3f url=%s",
                        file_meta.get("job_id"),
                        worker_id,
                        host,
                        domain_limit,
                        max(0.0, time.monotonic() - domain_lock_wait_started),
                        _short(url, 220),
                    )
                    logger.info(
                        "[Download][Worker %s] domain lock acquired | host=%s limit=%s wait_sec=%.3f url=%s",
                        worker_id,
                        host,
                        domain_limit,
                        max(0.0, time.monotonic() - domain_lock_wait_started),
                        _short(url, 220),
                    )
            except Exception:
                domain_sem = None
                domain_lock_acquired = False
            try:
                logger.info(
                    "[Download][Worker %s] Start | url=%s name=%s source=%s",
                    worker_id,
                    url,
                    file_meta.get("name"),
                    file_meta.get("source_page"),
                )
                # ?ㅼ슫濡쒕뱶 寃쎈줈 誘몃━ 怨꾩궛 (HTTP 諛?Playwright 紐⑤몢 ?ъ슜)
                try:
                    download_dir = await _get_download_dir(file_meta, default_download_dir, download_path_cache)
                except Exception as e:
                    logger.debug(f"[Download] 寃쎈줈 怨꾩궛 ?ㅽ뙣 (湲곕낯 寃쎈줈 ?ъ슜): {e}")
                    download_dir = default_download_dir

                # 1. HTTP ?ㅼ슫濡쒕뱶 ?쒕룄
                can_try_direct_http = _is_valid_http_url(url)
                if not can_try_direct_http:
                    logger.info(
                        "[Download][Worker %s] Skip direct HTTP; Playwright required | url=%s source=%s",
                        worker_id,
                        _short(url, 220),
                        _short(source_page, 220),
                    )
                attempt = 0
                transient_payload_retry_extended = False
                while attempt < per_item_http_retries:
                    attempt += 1
                    if not can_try_direct_http:
                        break
                    try:
                        file_meta["_download_http_attempt"] = attempt
                        req_headers = _build_request_headers(headers, source_page=source_page)
                        cookie_header = _cookie_header_from_file_meta(file_meta)
                        manual_cookie_header = "; ".join(
                            f"{name}={value}" for name, value in _manual_download_cookies(url, source_page).items()
                        )
                        merged_cookie_header = _merge_cookie_headers(cookie_header, manual_cookie_header)
                        if merged_cookie_header:
                            req_headers["Cookie"] = merged_cookie_header
                        prewarm_enabled = str(os.getenv("DOWNLOAD_HTTP_PREWARM_SOURCE_PAGE", "1")).strip().lower() in ("1", "true", "yes", "on")
                        if is_portal_direct_fast:
                            prewarm_enabled = str(os.getenv("DOWNLOAD_PORTAL_DIRECT_HTTP_PREWARM_SOURCE_PAGE", "0")).strip().lower() in ("1", "true", "yes", "on")
                        if _is_suwon_culture_direct_download_url(url) or is_http_only_direct:
                            prewarm_enabled = False
                        if source_page and prewarm_enabled:
                            _set_download_activity_phase(file_meta, "source_page_prewarm")
                            await _prime_source_page_http_session(
                                session,
                                source_page=source_page,
                                headers=req_headers,
                                worker_id=worker_id,
                                primed_source_pages=primed_source_pages,
                            )
                            jar_cookie_header = _cookie_header_from_aiohttp_session(session, url, source_page)
                            if jar_cookie_header:
                                req_headers["Cookie"] = _merge_cookie_headers(req_headers.get("Cookie", ""), jar_cookie_header)
                                _merge_request_cookies_into_file_meta(file_meta, _cookies_from_header(jar_cookie_header))
                                logger.info(
                                    "[Download][Worker %s] source_page cookies attached | host=%s cookie_count=%s source=%s",
                                    worker_id,
                                    host or "-",
                                    len(_cookies_from_header(jar_cookie_header)),
                                    _short(source_page, 180),
                                )
                        logger.debug(
                            "[DownloadTrace][http_request] job_id=%s worker=%s attempt=%s/%s "
                            "url=%s post_url=%s source_prewarm=%s timeout_sec=%.1f",
                            file_meta.get("job_id"),
                            worker_id,
                            attempt,
                            per_item_http_retries,
                            _short(url, 220),
                            _short(source_page, 220),
                            bool(source_page and prewarm_enabled),
                            per_item_http_timeout,
                        )

                        if is_http_only_direct:
                            logger.info(
                                "[DownloadTrace][direct_http_start] job_id=%s worker=%s timeout_sec=%.1f url=%s post_url=%s",
                                file_meta.get("job_id"),
                                worker_id,
                                per_item_http_timeout,
                                _short(url, 220),
                                _short(source_page, 220),
                            )
                        safe_url = url
                        try:
                            safe_url = quote(url, safe=":/?=&")
                        except Exception:
                            pass
                        if not safe_url or not safe_url.strip():
                            logger.debug("[Download][Worker %s] empty safe_url skip | url=%s", worker_id, url)
                            break

                        request_timeout = _download_http_request_timeout(
                            file_meta,
                            per_item_http_timeout,
                        )
                        timeout_total = getattr(request_timeout, "total", request_timeout)
                        timeout_connect = getattr(request_timeout, "connect", None)
                        timeout_read = getattr(request_timeout, "sock_read", None)
                        await _append_download_url_trace(
                            {
                                "event": "request_started",
                                "job_id": file_meta.get("job_id"),
                                "worker_id": worker_id,
                                "url": url,
                                "post_url": source_page,
                                "request_timeout_sec": timeout_total,
                                "connect_timeout_sec": timeout_connect,
                                "read_timeout_sec": timeout_read,
                                "http_attempt": attempt,
                                "direct_attachment": is_http_only_direct,
                            }
                        )
                        _set_download_activity_phase(file_meta, "http_response_headers_wait")
                        file_meta["_download_transport_phase"] = "http_response_headers_wait"
                        request_started_at = time.monotonic()
                        async with session.get(
                            safe_url,
                            timeout=request_timeout,
                            allow_redirects=True,
                            headers=req_headers,
                        ) as response:
                            _set_download_activity_phase(file_meta, "http_response_headers_received")
                            await _append_download_url_trace(
                                {
                                    "event": "target_response",
                                    "job_id": file_meta.get("job_id"),
                                    "worker_id": worker_id,
                                    "url": url,
                                    "request_url": safe_url,
                                    "post_url": source_page,
                                    "http_status": response.status,
                                    "response_url": str(response.url),
                                    "content_type": response.headers.get("content-type", ""),
                                    "content_length": response.headers.get("content-length", ""),
                                    "http_attempt": attempt,
                                }
                            )
                            _set_download_activity_phase(file_meta, "http_response_metadata")
                            response_header_elapsed_sec = max(0.0, time.monotonic() - request_started_at)
                            if response_header_elapsed_sec >= _download_transport_slow_log_sec():
                                logger.info(
                                    "[DownloadTrace][transport_headers_slow] job_id=%s worker=%s host=%s elapsed_sec=%.3f "
                                    "status=%s content_length=%s content_type=%s url=%s post_url=%s",
                                    file_meta.get("job_id"),
                                    worker_id,
                                    host or "-",
                                    response_header_elapsed_sec,
                                    response.status,
                                    response.headers.get("content-length", ""),
                                    response.headers.get("content-type", ""),
                                    _short(url, 220),
                                    _short(source_page, 220),
                                )
                            if response.status != 200:
                                file_meta["_download_empty_reason"] = f"http_status_{response.status}"
                                logger.debug(
                                    "[Download][Worker %s] HTTP non-200 | attempt=%s status=%s url=%s encoded=%s",
                                    worker_id,
                                    attempt,
                                    response.status,
                                    url,
                                    safe_url,
                                )
                                if attempt < per_item_http_retries:
                                    continue
                                break
                            
                            # ?뚯씪紐?諛?寃쎈줈 泥섎━
                            content_type = response.headers.get('content-type', '').lower()
                            cd = response.headers.get('content-disposition', '')

                            match = re.search(r'filename=(?:["\']?([^"\'\n;]+)["\']?|([^"\';\n]+))', cd, re.IGNORECASE)
                            
                            # 湲곕뒫蹂?1以?二쇱꽍?ㅻ챸: ?몄뀡 ?곌껐???좎??섎뒗 ?숈븞 蹂몃Ц???쎈룄濡?議곌굔臾?釉붾줉 ?꾩껜瑜??덉쑝濡??ㅼ뿬?곌린?⑸땲??
                            if match:
                                # 洹몃９ 1(?곗샂???덉쓬) ?먮뒗 洹몃９ 2(?곗샂???놁쓬)?먯꽌 媛?媛?몄삤湲?
                                raw_filename = match.group(1) or match.group(2)
                                raw_filename = raw_filename.strip()

                                # %-?몄퐫??URL ?몄퐫?? ?좎쿂由?(local import to avoid top-level dependency)
                                if '%' in raw_filename:
                                    try:
                                        from urllib.parse import unquote
                                        raw_filename = unquote(raw_filename)
                                    except Exception:
                                        pass

                                # 湲곕낯媛믪? ?먮낯. ?댄썑 Latin-1濡??섎せ ?댁꽍??諛붿씠?몃? ?ы빐???쒕룄
                                final_filename = raw_filename
                                try:
                                    b = raw_filename.encode('latin-1')
                                    try:
                                        final_filename = b.decode('utf-8')
                                    except UnicodeDecodeError:
                                        try:
                                            final_filename = b.decode('cp949')
                                        except UnicodeDecodeError:
                                            final_filename = raw_filename
                                except Exception:
                                    # ?대뼡 ?덉쇅媛 ????먮낯?쇰줈 ?대갚
                                    final_filename = raw_filename
                                
                                # 0) 臾몄꽌瑜섎쭔 ?덉슜 (?ㅻ뜑 湲곕컲 1李?李⑤떒)
                                # - content-disposition/?뚯씪紐낆쑝濡??뺤옣?먮? ?산린 ?꾩뿉, MIME??紐낇솗??硫?곕??붿뼱硫?利됱떆 李⑤떒
                                if DOWNLOAD_DOC_ONLY and (content_type.startswith("image/") or content_type.startswith("video/") or content_type.startswith("audio/")):
                                    file_meta["_download_empty_reason"] = f"non_doc_mime:{content_type}"
                                    logger.info(
                                        "[Download][Worker %s] Skipped (non-doc mime) | url=%s ct=%s",
                                        worker_id,
                                        url,
                                        content_type,
                                    )
                                    await progress_queue.put(
                                        {
                                            "type": "download_skipped",
                                            "url": url,
                                            "reason": "non_doc_mime",
                                            "content_type": content_type,
                                            "job_id": file_meta.get("job_id"),
                                            "source_page": file_meta.get("source_page"),
                                            "source_url": file_meta.get("source_url"),
                                            "worker_id": worker_id,
                                        }
                                    )
                                    if FLOW_DEBUG:
                                        logger.info(
                                            "[Flow] download_skipped | url=%s reason=non_doc_mime content_type=%s",
                                            _short(url, 220),
                                            content_type,
                                        )
                                    return None

                                head = b""
                                if "text/html" in content_type:
                                    file_meta["_download_empty_reason"] = f"html_content_type:{content_type}"
                                    logger.info(
                                        "[Download][Worker %s] HTTP returned HTML content-type; will fallback | attempt=%s url=%s ct=%s",
                                        worker_id,
                                        attempt,
                                        url,
                                        content_type,
                                    )
                                    if attempt < per_item_http_retries:
                                        await asyncio.sleep(0.5)
                                        continue
                                    break
                                
                                # HTML ?묐떟? 蹂댄넻 李⑤떒/濡쒓렇???섏씠吏?대?濡??뚯씪濡???ν븯吏 留먭퀬 ?ㅽ뙣 泥섎━
                                final_filename = None
                                if cd:
                                    from urllib.parse import unquote
                                    
                                    # 1. RFC 5987 (filename*) 泥섎━
                                    match = re.search(r'filename\*=UTF-8\'\'(.+)', cd, re.IGNORECASE)
                                    if match:
                                        final_filename = unquote(match.group(1))
                                    else:
                                        # 2. ?쇰컲 filename="..." 泥섎━
                                        match = re.search(r'filename="?([^";]+)"?', cd, re.IGNORECASE)
                                        if match:
                                            raw_filename = match.group(1)
                                            # Mojibake 蹂듦뎄 ?쒕룄 (Latin-1 -> UTF-8/CP949)
                                            try:
                                                # %-encoding???섏뼱 ?덈떎硫?unquote 癒쇱?
                                                if '%' in raw_filename:
                                                    raw_filename = unquote(raw_filename)
                                                
                                                # 1. UTF-8 蹂듦뎄 ?쒕룄
                                                try:
                                                    final_filename = raw_filename.encode('latin-1').decode('utf-8')
                                                except (UnicodeEncodeError, UnicodeDecodeError):
                                                    # 2. CP949 蹂듦뎄 ?쒕룄 (?쒓뎅???덇굅???쒕쾭 ???
                                                    try:
                                                        final_filename = raw_filename.encode('latin-1').decode('cp949')
                                                    except (UnicodeEncodeError, UnicodeDecodeError):
                                                        final_filename = raw_filename
                                            except Exception:
                                                final_filename = raw_filename
                                
                                if final_filename:
                                    logger.debug(f"[Download] filename extracted from headers: {final_filename}")
                                else:
                                    # 3. URL?먯꽌 異붿텧
                                    url_path = url.split('?')[0]
                                    url_filename = url_path.split('/')[-1]
                                    if url_filename and '.' in url_filename: final_filename = url_filename
                                
                                response_filename = final_filename
                                response_filename_is_opaque = _is_opaque_download_route_filename(
                                    response_filename,
                                    url,
                                )
                                server_side_direct = _is_server_side_direct_download_url(url)
                                response_filename_prefers_attachment = (
                                    response_filename_is_opaque or server_side_direct
                                )
                                if response_filename_is_opaque:
                                    logger.info(
                                        "[DownloadTrace][filename_opaque_route_token] job_id=%s worker=%s response_filename=%s attachment_filename=%s url=%s",
                                        file_meta.get("job_id"),
                                        worker_id,
                                        _short(response_filename, 160),
                                        _short(file_meta.get("attachment_name") or file_meta.get("original_name") or file_meta.get("name"), 160),
                                        _short(url, 220),
                                    )
                                attachment_filename = str(
                                    file_meta.get("attachment_name")
                                    or file_meta.get("name")
                                    or file_meta.get("subject")
                                    or ""
                                ).strip()
                                meta_filename_candidates = _filename_candidates_from_meta(file_meta)[:5]
                                url_filename_candidates = _filename_candidates_from_url(url)[:5]
                                final_filename = _best_download_filename(
                                    response_filename,
                                    url=url,
                                    file_meta=file_meta,
                                    default=file_meta.get("name", "unknown"),
                                    prefer_meta=response_filename_prefers_attachment,
                                )
                                _trace_filename_resolution(file_meta, worker_id=worker_id, url=url, stage="http_response", response_filename=response_filename, selected_filename=final_filename)
                                if response_filename_is_opaque:
                                    filename_selection_reason = "opaque_response_route_token_prefer_attachment"
                                elif server_side_direct:
                                    filename_selection_reason = "server_side_direct_prefer_attachment"
                                elif response_filename:
                                    filename_selection_reason = "response_filename_preferred"
                                else:
                                    filename_selection_reason = "attachment_or_url_fallback"
                                logger.info(
                                    "[DownloadTrace][filename_compare] job_id=%s worker=%s url=%s "
                                    "header_present=%s response_filename=%s attachment_filename=%s "
                                    "meta_candidates=%s url_candidates=%s opaque_route_token=%s server_side_direct=%s "
                                    "selected_filename=%s reason=%s",
                                    file_meta.get("job_id"),
                                    worker_id,
                                    _short(url, 220),
                                    bool(cd),
                                    _short(response_filename, 160),
                                    _short(attachment_filename, 160),
                                    meta_filename_candidates,
                                    url_filename_candidates,
                                    response_filename_is_opaque,
                                    server_side_direct,
                                    _short(final_filename, 160),
                                    filename_selection_reason,
                                )
                                _filename_debug_log(
                                    "http_response_filename",
                                    content_disposition=cd,
                                    selected_filename=final_filename,
                                    file_meta_name=(file_meta or {}).get("name") if isinstance(file_meta, dict) else None,
                                    attachment_name=((file_meta or {}).get("original_meta", {}) or {}).get("attachment_name") if isinstance(file_meta, dict) else None,
                                    url=url,
                                )
                                final_filename = _strip_partial_download_suffix(final_filename)
                                
                                # PHP ?듭씪: ?먮낯紐?subject) + ?붿뒪????λ챸(md5+ext)
                                original_subject = sanitize_filename(final_filename) or final_filename
                                if '.' not in original_subject:
                                    if 'pdf' in content_type: original_subject += '.pdf'
                                    elif 'hwp' in content_type: original_subject += '.hwp'
                                subject_with_ext = final_filename if (final_filename and '.' in final_filename) else (sanitize_filename(original_subject) or 'file.bin')
                                storage_filename = make_safe_storage_filename(subject_with_ext)
                                expected_size = _expected_content_length(response.headers)
                                if _should_defer_response_to_large_lane(
                                    file_meta,
                                    int(expected_size or 0),
                                    worker_lane=worker_lane,
                                    large_queue_available=large_download_queue is not None,
                                ):
                                    file_meta["declared_file_size_bytes"] = int(expected_size or 0)
                                    file_meta["download_lane"] = "large"
                                    file_meta["_large_lane_requeued"] = True
                                    try:
                                        _set_download_activity_phase(file_meta, "large_lane_enqueue_wait")
                                        await large_download_queue.put(file_meta)
                                        await large_download_queue.flush()
                                    except Exception as defer_exc:
                                        file_meta.pop("_large_lane_requeued", None)
                                        file_meta["download_lane"] = "normal"
                                        logger.warning(
                                            "[DownloadTrace][large_lane_defer_failed] job_id=%s worker=%s size=%s url=%s err=%s",
                                            file_meta.get("job_id"),
                                            worker_id,
                                            expected_size,
                                            _short(url, 220),
                                            defer_exc,
                                        )
                                    else:
                                        logger.info(
                                            "[DownloadTrace][large_lane_deferred] job_id=%s worker=%s size=%s url=%s post_url=%s",
                                            file_meta.get("job_id"),
                                            worker_id,
                                            expected_size,
                                            _short(url, 220),
                                            _short(source_page, 220),
                                        )
                                        _set_download_activity_phase(file_meta, "progress_queue_enqueue_wait")
                                        await progress_queue.put(
                                            {
                                                "type": "download_deferred",
                                                "url": url,
                                                "reason": "response_content_length_large",
                                                "declared_file_size_bytes": int(expected_size or 0),
                                                "source_page": source_page,
                                                "name": file_meta.get("name") or file_meta.get("subject"),
                                                "worker_id": worker_id,
                                                "job_id": file_meta.get("job_id"),
                                            }
                                        )
                                        return {
                                            "deferred_to_large_lane": True,
                                            "url": url,
                                            "declared_file_size_bytes": int(expected_size or 0),
                                        }

                                # 1) 臾몄꽌瑜섎쭔 ?덉슜 (?뚯씪紐??뺤옣??湲곕컲 2李?李⑤떒)
                                if is_blocked_non_document(storage_filename, content_type):
                                    file_meta["_download_empty_reason"] = f"non_doc_file:{storage_filename}:{content_type}"
                                    logger.debug(
                                        "[Download][Worker %s] Skipped (non-doc file) | url=%s filename=%s ct=%s",
                                        worker_id,
                                        url,
                                        storage_filename,
                                        content_type,
                                    )
                                    await progress_queue.put({'type': 'download_skipped', 'url': url, 'reason': 'non_doc_file', 'filename': storage_filename, 'content_type': content_type})
                                    if FLOW_DEBUG:
                                        logger.info(
                                            "[Flow] download_skipped | url=%s reason=non_doc_file name=%s content_type=%s",
                                            _short(url, 220),
                                            storage_filename,
                                            content_type,
                                        )
                                    return None
                                
                                filepath = os.path.join(download_dir, storage_filename) # ??λ맆 ?꾩껜 寃쎈줈 ?앹꽦
                                tmp_filepath = _download_temp_path(filepath)
                                file_meta["_active_download_temp_filepath"] = tmp_filepath
                                # 1以?二쇱꽍: HTTP ?ъ떆??猷⑦봽 ?댁뿉??湲곗〈???섎せ ?앹꽦???뚯씪???덉쓣 寃쎌슦留???젣??
                                if os.path.exists(filepath):
                                    pass

                                _set_download_activity_phase(file_meta, "http_body_stream")
                                file_meta["_download_transport_phase"] = "http_body_stream"
                                body_stream_started_at = time.monotonic()
                                try:
                                    file_size, head = await _stream_http_response_to_file(
                                        response,
                                        tmp_filepath,
                                    )
                                except Exception as stream_exc:
                                    bytes_written = int(getattr(stream_exc, "bytes_written", 0) or 0)
                                    last_progress_at = float(
                                        getattr(stream_exc, "last_progress_at", body_stream_started_at) or body_stream_started_at
                                    )
                                    no_progress_sec = max(0.0, time.monotonic() - last_progress_at)
                                    failure_reason = _download_transport_failure_reason(
                                        stream_exc,
                                        phase="http_body_stream",
                                        bytes_written=bytes_written,
                                    )
                                    await _cleanup_active_download_temp_file(
                                        file_meta,
                                        reason="http_stream_failed",
                                        worker_id=worker_id,
                                    )
                                    file_meta["_download_empty_reason"] = (
                                        f"{failure_reason}:bytes={bytes_written}:idle={no_progress_sec:.1f}s"
                                    )
                                    logger.warning(
                                        "[DownloadTrace][transport_body_failed] job_id=%s worker=%s host=%s header_elapsed_sec=%.3f "
                                        "body_elapsed_sec=%.3f failure_reason=%s bytes_written=%s no_progress_sec=%.3f "
                                        "stall_timeout_sec=%.1f url=%s post_url=%s err_type=%s err=%s err_repr=%r",
                                        file_meta.get("job_id"),
                                        worker_id,
                                        host or "-",
                                        response_header_elapsed_sec,
                                        max(0.0, time.monotonic() - body_stream_started_at),
                                        failure_reason,
                                        bytes_written,
                                        no_progress_sec,
                                        _download_stream_stall_timeout_sec(),
                                        _short(url, 220),
                                        _short(source_page, 220),
                                        type(stream_exc).__name__,
                                        stream_exc,
                                        stream_exc,
                                    )
                                    await _append_download_url_trace(
                                        {
                                            "event": "transport_failed",
                                            "job_id": file_meta.get("job_id"),
                                            "worker_id": worker_id,
                                            "worker_lane": worker_lane,
                                            "url": url,
                                            "post_url": source_page,
                                            "phase": "http_body_stream",
                                            "failure_reason": failure_reason,
                                            "bytes_written": bytes_written,
                                            "no_progress_sec": round(no_progress_sec, 3),
                                            "stream_stall_timeout_sec": _download_stream_stall_timeout_sec(),
                                            "error": _format_exception_for_log(stream_exc),
                                        }
                                    )
                                    raise
                                body_stream_elapsed_sec = max(0.0, time.monotonic() - body_stream_started_at)
                                if body_stream_elapsed_sec >= _download_transport_slow_log_sec():
                                    logger.info(
                                        "[DownloadTrace][transport_body_slow] job_id=%s worker=%s host=%s header_elapsed_sec=%.3f "
                                        "body_elapsed_sec=%.3f bytes=%s expected_bytes=%s url=%s post_url=%s",
                                        file_meta.get("job_id"),
                                        worker_id,
                                        host or "-",
                                        response_header_elapsed_sec,
                                        body_stream_elapsed_sec,
                                        file_size,
                                        response.headers.get("content-length", ""),
                                        _short(url, 220),
                                        _short(source_page, 220),
                                    )
                                file_size = await wait_for_file_ready(
                                    tmp_filepath,
                                    timeout_sec=_download_temp_ready_timeout_sec(),
                                    allow_partial_name=True,
                                    check_partial_siblings=False,
                                )
                                final_filename = _ensure_download_filename_extension(
                                    final_filename,
                                    filepath=tmp_filepath,
                                    head=head,
                                    content_type=content_type,
                                    url=url,
                                )
                                final_filename = _strip_partial_download_suffix(final_filename)
                                original_subject = sanitize_filename(final_filename) or final_filename
                                subject_with_ext = final_filename if (final_filename and '.' in final_filename) else (sanitize_filename(original_subject) or 'file.bin')
                                storage_filename = make_safe_storage_filename(subject_with_ext)
                                filepath = os.path.join(download_dir, storage_filename)
                                if not file_size:
                                    file_meta["_download_empty_reason"] = "zero_size_after_stream"
                                    if os.path.exists(tmp_filepath):
                                        os.remove(tmp_filepath)
                                    if attempt < http_retries:
                                        continue
                                    break
                                head = head.lstrip().lower() if isinstance(head, (bytes, bytearray)) else b""
                                if head.startswith(b"<!doctype html") or b"<html" in head:
                                    body_preview = ""
                                    try:
                                        body_preview = _read_file_head(tmp_filepath, 4096).decode("utf-8", errors="ignore")[:500]
                                    except Exception:
                                        body_preview = ""
                                    access_denied = _looks_like_access_denied_html(body_preview)
                                    if os.path.exists(tmp_filepath):
                                        os.remove(tmp_filepath)
                                    if access_denied:
                                        recent_access_denied_urls[url] = time.monotonic() + access_denied_cache_ttl_sec
                                    file_meta["_download_empty_reason"] = (
                                        "html_payload_access_denied" if access_denied else "html_payload"
                                    )
                                    logger.warning(
                                        "[Download][Worker %s] HTTP streamed HTML; will fallback | attempt=%s url=%s ct=%s access_denied=%s preview=%s",
                                        worker_id,
                                        attempt,
                                        url,
                                        content_type,
                                        access_denied,
                                        _short(body_preview, 180),
                                    )
                                    if attempt < http_retries:
                                        await asyncio.sleep(0.5)
                                        continue
                                    break

                                try:
                                    await asyncio.to_thread(
                                        _validate_downloaded_file,
                                        tmp_filepath,
                                        filename=original_subject,
                                        url=url,
                                        content_type=content_type,
                                        expected_size=expected_size,
                                        actual_size=file_size,
                                        head=head,
                                    )
                                    await asyncio.to_thread(_replace_file, tmp_filepath, filepath)
                                    file_meta.pop("_active_download_temp_filepath", None)
                                    # The unique temporary file was validated before the atomic replace.
                                    # A stale sibling from another attempt must not block this finalized file.
                                    file_size = await wait_for_file_ready(
                                        filepath,
                                        timeout_sec=_download_final_ready_timeout_sec(),
                                        stable_checks=3,
                                        check_partial_siblings=False,
                                    )
                                except Exception as validation_exc:
                                    await asyncio.to_thread(_remove_file_quietly, tmp_filepath)
                                    file_meta["_download_empty_reason"] = f"validation_failed:{_short(validation_exc, 160)}"
                                    logger.warning(
                                        "[Download][Worker %s] HTTP validation failed; will fallback | attempt=%s url=%s path=%s err=%s",
                                        worker_id,
                                        attempt,
                                        _short(url, 200),
                                        _short(filepath, 240),
                                        _short(validation_exc, 240),
                                    )
                                    if attempt < http_retries:
                                        await asyncio.sleep(0.5)
                                        continue
                                    break

                                if DOWNLOAD_PATH_DEBUG:
                                    logger.info(
                                        "[DOWNLOAD][PathDebug] about to write (http) | worker=%s db=%s server_domain=%s domain=%s chat_bot_id_tail=%s dir=%s filename=%s filepath=%s bytes=%s ct=%s",
                                        worker_id,
                                        file_meta.get("db_name"),
                                        file_meta.get("server_domain"),
                                        file_meta.get("domain"),
                                        (str(file_meta.get("chat_bot_id") or "").split("-")[-1] if file_meta.get("chat_bot_id") else None),
                                        download_dir,
                                        storage_filename,
                                        filepath,
                                        file_size,
                                        content_type,
                                    )
                                
                                logger.debug(
                                    "[Download][SaveAttempt][Worker %s] url=%s filename=%s path=%s bytes=%s",
                                    worker_id,
                                    url,
                                    storage_filename,
                                    filepath,
                                    file_size,
                                )
                                if DOWNLOAD_PATH_DEBUG:
                                    try:
                                        size_on_disk = os.path.getsize(filepath)
                                    except Exception:
                                        size_on_disk = None
                                    logger.debug(
                                        "[DOWNLOAD][PathDebug] wrote (http) | worker=%s filepath=%s size_on_disk=%s",
                                        worker_id,
                                        filepath,
                                        size_on_disk,
                                    )
                                
                                logger.debug(
                                    "[Download][Worker %s] HTTP saved | url=%s path=%s size=%s content_type=%s",
                                    worker_id,
                                    url,
                                    filepath,
                                    file_size,
                                    content_type,
                                )

                                # 臾몄꽌 ?대? 硫뷀??곗씠??湲곕컲 ?묒꽦??異붿텧 (DB content_created_at濡??꾨떖)
                                defer_local_postprocess = _defer_file_local_postprocess(file_meta)
                                doc_created_at = None
                                if not defer_local_postprocess:
                                    doc_created_at = await _extract_doc_created_at_async(filepath)
                                    websync_ok = await _sync_after_download_if_needed(file_meta, filepath)
                                    if not websync_ok:
                                        file_meta["_download_empty_reason"] = "websync_failed"
                                        await progress_queue.put(
                                            _progress_queue_websync_failed_payload(
                                                file_meta,
                                                url=url,
                                                filepath=filepath,
                                                worker_id=worker_id,
                                            )
                                        )
                                        return None
                                # 寃곌낵 蹂닿퀬
                                if FLOW_DEBUG:
                                    logger.info(
                                        "[Flow] saved_local | url=%s path=%s size=%s",
                                        _short(url, 220),
                                        _short(filepath, 220),
                                        file_size,
                                    )
                                await progress_queue.put(
                                    _progress_queue_file_saved_payload(
                                        file_meta,
                                        {
                                            "file_path": filepath,
                                            "local_path": filepath,
                                            "url": url,
                                            "name": original_subject,
                                            "subject": original_subject,
                                            "display_name": original_subject,
                                            "attachment_name": original_subject,
                                            "memo": file_meta.get("memo"),
                                            "storage_filename": storage_filename,
                                            "size": file_size,
                                            "job_id": file_meta.get("job_id"),
                                            "cate1": file_meta.get("cate1"),
                                            "cate2": file_meta.get("cate2"),
                                            "file_created_at": doc_created_at,
                                            "author": file_meta.get("author"),
                                            "content_author": file_meta.get("content_author") or file_meta.get("author") or file_meta.get("department"),
                                            "department": file_meta.get("department"),
                                            "author_kind": file_meta.get("author_kind"),
                                            "author_raw": file_meta.get("author_raw"),
                                            "department_raw": file_meta.get("department_raw"),
                                            "source_page": file_meta.get("source_page"),
                                            "reg_date": file_meta.get("reg_date"),
                                            "original_meta": file_meta,
                                            "skip_study_worker": bool(
                                                file_meta.get("skip_study_worker")
                                            ),
                                            **_learn_list_ids_from_file_meta(file_meta),
                                        },
                                        event_type=("download_local_saved" if defer_local_postprocess else "file_saved"),
                                    )
                                )
                                # Debug prints for HWP download and study flag
                                try:
                                    skip_study = bool(file_meta.get('skip_study_worker'))
                                    # Optional HWP/HWPX flow trace.
                                    _low = (storage_filename or "").lower()
                                    _osub = (original_subject or "").lower()
                                    if _low.endswith(".hwp") or _low.endswith(".hwpx") or _osub.endswith(".hwp") or _osub.endswith(".hwpx"):
                                        _flow_debug_print(f"[Download][Flow] hwp_downloaded_http file={storage_filename} path={filepath} size={file_size} skip_study={skip_study}")
                                    _flow_debug_print(f"[Download][Flow] study_flag url={url} skip_study_worker={skip_study}")
                                except Exception:
                                    pass
                                
                                logger.debug(
                                    "[Download][SaveDone] file_saved event emitted after download complete (save_count increments after DB save) | worker_id=%s url=%s path=%s",
                                    worker_id, _short(url, 200), _short(filepath, 200),
                                )
                                logger.debug(
                                    "[Download][SaveDone][PathDebug] url=%s local_path=%s exists=%s size=%s",
                                    url,
                                    filepath,
                                    os.path.exists(filepath),
                                    file_size,
                                )
                                logger.debug(
                                    "[Download][SaveDone][Worker %s] url=%s filename=%s path=%s size=%s",
                                    worker_id,
                                    url,
                                    storage_filename,
                                    filepath,
                                    file_size,
                                )
                                
                                return {
                                    'file_path': filepath, 
                                    'url': url,
                                    # 硫뷀??곗씠??蹂댁〈(?먯깋 ?④퀎?먯꽌 異붿텧??author/reg_date/source_page ??
                                    'author': file_meta.get('author'),
                                    'content_author': file_meta.get('content_author') or file_meta.get('author') or file_meta.get('department'),
                                    'department': file_meta.get('department'),
                                    'author_kind': file_meta.get('author_kind'),
                                    'author_raw': file_meta.get('author_raw'),
                                    'department_raw': file_meta.get('department_raw'),
                                    'reg_date': file_meta.get('reg_date'),
                                    'source_page': file_meta.get('source_page'),
                                    'original_meta': file_meta,
                                    'job_id': file_meta.get('job_id'),
                                    'chat_bot_id': file_meta.get('chat_bot_id'),
                                    'db_name': file_meta.get('db_name'),
                                    'name': original_subject,
                                    'subject': original_subject,
                                    'display_name': original_subject,
                                    'attachment_name': original_subject,
                                    # ?꾨줎?몄뿉???꾨떖??memo ?꾨떖
                                    'memo': file_meta.get('memo'),
                                    'storage_filename': storage_filename,
                                    'size': file_size,
                                    'content_type': content_type,
                                    'skip_study_worker': bool(file_meta.get('skip_study_worker')),
                                    **_learn_list_ids_from_file_meta(file_meta),
                                    **_defer_save_batch_flag(file_meta),
                                }
                    except Exception as http_exc:
                        last_err_msg = _format_exception_for_log(http_exc)
                        file_meta["_download_last_error"] = last_err_msg
                        transport_phase = str(
                            file_meta.get("_download_transport_phase") or "http_request"
                        )
                        failure_reason = _download_transport_failure_reason(
                            http_exc,
                            phase=transport_phase,
                            bytes_written=int(getattr(http_exc, "bytes_written", 0) or 0),
                        )
                        logger.warning(
                            "[DownloadTrace][transport_request_failed] job_id=%s worker=%s lane=%s phase=%s "
                            "failure_reason=%s attempt=%s/%s url=%s post_url=%s err=%s",
                            file_meta.get("job_id"),
                            worker_id,
                            worker_lane,
                            transport_phase,
                            failure_reason,
                            attempt,
                            per_item_http_retries,
                            _short(url, 220),
                            _short(source_page, 220),
                            _short(last_err_msg, 300),
                        )
                        await _append_download_url_trace(
                            {
                                "event": "transport_failed",
                                "job_id": file_meta.get("job_id"),
                                "worker_id": worker_id,
                                "worker_lane": worker_lane,
                                "url": url,
                                "post_url": source_page,
                                "phase": transport_phase,
                                "failure_reason": failure_reason,
                                "http_attempt": attempt,
                                "error": last_err_msg,
                            }
                        )
                        if (
                            attempt >= per_item_http_retries
                            and not transient_payload_retry_extended
                            and _is_retryable_incomplete_payload_error(http_exc)
                        ):
                            per_item_http_retries += 1
                            transient_payload_retry_extended = True
                            logger.warning(
                                "[DownloadTrace][transient_payload_retry] job_id=%s worker=%s "
                                "attempt=%s next_attempt=%s url=%s post_url=%s error=%s",
                                file_meta.get("job_id"),
                                worker_id,
                                attempt,
                                per_item_http_retries,
                                _short(url, 220),
                                _short(source_page, 220),
                                _short(last_err_msg, 240),
                            )
                        if attempt >= per_item_http_retries and not file_meta.get("_download_empty_reason"):
                            file_meta["_download_empty_reason"] = f"http_exception:{_short(last_err_msg, 160)}"
                        log_http_exception = logger.warning
                        if browser or browser_relauncher:
                            log_http_exception = logger.debug
                        log_http_exception(
                            "[Download][Worker %s] HTTP attempt exception | attempt=%s/%s url=%s err=%s fallback_available=%s",
                            worker_id,
                            attempt,
                            per_item_http_retries,
                            _short(url, 220),
                            _short(last_err_msg, 240),
                            bool(browser or browser_relauncher),
                        )
                        try:
                            await asyncio.to_thread(_remove_file_quietly, locals().get("tmp_filepath", ""))
                        except Exception:
                            pass
                        if attempt < per_item_http_retries: 
                            await asyncio.sleep(1)
                        continue

                # 2. Playwright Fallback
                if (browser or browser_relauncher) and per_item_pw_attempts > 0:
                    logger.debug(
                        "[Download][Worker %s] HTTP direct attempt did not produce a file; trying Playwright fallback | reason=%s last_error=%s url=%s",
                        worker_id,
                        _short(file_meta.get("_download_empty_reason") or "unknown", 180),
                        _short(file_meta.get("_download_last_error") or "-", 180),
                        url,
                    )
                    await playwright_fallback_sem.acquire()
                    logger.debug(
                        "[Download][Worker %s] Playwright fallback slot acquired | limit=%s url=%s",
                        worker_id,
                        pw_fallback_limit,
                        _short(url, 180),
                    )
                    # 釉뚮씪?곗?/?섏씠吏 ?ロ옒쨌痍⑥냼 ?쒖뿉留??쒗븳 ?잛닔 ???ъ떆??(臾댄븳 ?湲?諛⑹?)
                    for p_attempt in range(1, per_item_pw_attempts + 1):
                        current_browser: Optional[Browser] = None
                        try:
                            current_browser = _acquire_playwright_browser()
                            # 釉뚮씪?곗? ?곌껐 ?곹깭 ?뺤씤 諛??꾩슂 ???ъ떎??
                            if (current_browser is None) or (not current_browser.is_connected()):
                                logger.warning(f"[Download][Worker {worker_id}] Browser missing or disconnected, relaunching...")
                                _release_playwright_browser(current_browser)
                                current_browser = None
                                if browser_relauncher:
                                    # 釉뚮씪?곗? 醫낅즺 吏곹썑 吏㏐쾶 ?⑥쓣 怨좊Ⅸ ???ъ떎??
                                    await asyncio.sleep(0.5)
                                    browser = await browser_relauncher()
                                    current_browser = _acquire_playwright_browser()
                                    logger.debug(f"[Download][Worker {worker_id}] Browser relaunched successfully")
                                else:
                                    logger.error(f"[Download][Worker {worker_id}] No relauncher; stopping Playwright fallback")
                                    break
                            elif not current_browser and browser_relauncher:
                                logger.warning(f"[Download][Worker {worker_id}] Browser not available, launching via relauncher...")
                                browser = await browser_relauncher()
                                current_browser = _acquire_playwright_browser()
                            elif not current_browser and not browser_relauncher:
                                # 諛⑹뼱: ??議곌굔臾몄쑝濡??ㅼ뼱?ㅺ릿 ?대졄吏留? ?덉쟾?섍쾶 泥섎━
                                logger.debug(
                                    "[Download][Worker %s] No browser and no relauncher; skipping Playwright fallback | url=%s",
                                    worker_id,
                                    url,
                                )
                                break

                            file_info = await _download_with_playwright(
                                current_browser,
                                file_meta,
                                download_dir,
                                default_download_dir,
                                browser_relauncher=browser_relauncher,
                                worker_id=worker_id,
                            )
                            if file_info:
                                # 臾몄꽌 ?대? 硫뷀??곗씠??湲곕컲 ?묒꽦??異붿텧 (DB content_created_at濡??꾨떖)
                                try:
                                    fp = file_info.get("file_path") or file_info.get("local_path")
                                except Exception:
                                    fp = None
                                defer_local_postprocess = _defer_file_local_postprocess(file_meta)
                                doc_created_at = None
                                if fp and not defer_local_postprocess:
                                    doc_created_at = await _extract_doc_created_at_async(fp)
                                    websync_ok = await _sync_after_download_if_needed(file_meta, fp)
                                    if not websync_ok:
                                        file_meta["_download_empty_reason"] = "websync_failed"
                                        await progress_queue.put(
                                            _progress_queue_websync_failed_payload(
                                                file_meta,
                                                url=url,
                                                filepath=fp,
                                                worker_id=worker_id,
                                            )
                                        )
                                        return None
                                if FLOW_DEBUG:
                                    logger.info(
                                        "[Download][Worker %s] [Flow] saved_local | url=%s path=%s size=%s",
                                        worker_id,
                                        _short(url, 220),
                                        _short(file_info.get("file_path"), 220),
                                        file_info.get("size"),
                                    )
                                await progress_queue.put(
                                    _progress_queue_file_saved_payload(
                                        file_meta,
                                        {
                                            **file_info,
                                            "job_id": file_meta.get("job_id"),
                                            "skip_study_worker": bool(
                                                file_meta.get("skip_study_worker")
                                            ),
                                            "cate1": file_meta.get("cate1"),
                                            "cate2": file_meta.get("cate2"),
                                            "file_created_at": doc_created_at,
                                            "author": file_meta.get("author"),
                                            "content_author": file_meta.get("content_author") or file_meta.get("author") or file_meta.get("department"),
                                            "department": file_meta.get("department"),
                                            "author_kind": file_meta.get("author_kind"),
                                            "author_raw": file_meta.get("author_raw"),
                                            "department_raw": file_meta.get("department_raw"),
                                            "source_page": file_meta.get("source_page"),
                                            "reg_date": file_meta.get("reg_date"),
                                            "original_meta": file_meta,
                                            **_learn_list_ids_from_file_meta(file_meta),
                                        },
                                        event_type=("download_local_saved" if defer_local_postprocess else "file_saved"),
                                    )
                                )
                                # Debug prints for HWP and study flag (Playwright)
                                try:
                                    skip_study = bool(file_meta.get('skip_study_worker'))
                                    fname = os.path.basename(file_info.get('file_path') or file_info.get('local_path') or file_info.get('storage_filename') or '')
                                    _fl = fname.lower()
                                    _nm = (file_info.get("name") or "").lower()
                                    if _fl.endswith(".hwp") or _fl.endswith(".hwpx") or _nm.endswith(".hwp") or _nm.endswith(".hwpx"):
                                        _flow_debug_print(f"[Download][Flow] hwp_downloaded_playwright file={fname} path={file_info.get('file_path')} size={file_info.get('size')} skip_study={skip_study}")
                                    _flow_debug_print(f"[Download][Flow] study_flag url={url} skip_study_worker={skip_study}")
                                except Exception:
                                    pass
                                
                                logger.debug(
                                    "[Download][SaveDone] file_saved event emitted after download complete (save_count increments after DB save) | worker_id=%s url=%s path=%s",
                                    worker_id, _short(url, 200), _short(file_info.get("file_path"), 200),
                                )
                                logger.debug(
                                    "[Download][Worker %s] Playwright saved | url=%s path=%s size=%s",
                                    worker_id,
                                    url,
                                    file_info.get("file_path"),
                                    file_info.get("size"),
                                )
                                try:
                                    playwright_fallback_sem.release()
                                except Exception:
                                    pass
                                return {
                                    'file_path': file_info['file_path'], 
                                    'url': url,
                                    # 硫뷀??곗씠??蹂댁〈(?먯깋 ?④퀎?먯꽌 異붿텧??author/reg_date/source_page ??
                                    'author': file_meta.get('author'),
                                    'content_author': file_meta.get('content_author') or file_meta.get('author') or file_meta.get('department'),
                                    'department': file_meta.get('department'),
                                    'author_kind': file_meta.get('author_kind'),
                                    'author_raw': file_meta.get('author_raw'),
                                    'department_raw': file_meta.get('department_raw'),
                                    'reg_date': file_meta.get('reg_date'),
                                    'source_page': file_meta.get('source_page'),
                                    'original_meta': file_meta,
                                    'job_id': file_meta.get('job_id'),
                                    'chat_bot_id': file_meta.get('chat_bot_id'),
                                    'db_name': file_meta.get('db_name'),
                                    'name': file_info.get('name'),
                                    'content_type': file_info.get('content_type', 'file'),
                                    'skip_study_worker': bool(file_meta.get('skip_study_worker')),
                                    **_learn_list_ids_from_file_meta(file_meta),
                                    **_defer_save_batch_flag(file_meta),
                                }
                            break # ?깃났/寃곌낵?놁쓬 ??猷⑦봽 ?덉텧
                        except Exception as e:
                            last_err_msg = str(e or "")
                            from playwright.async_api import Error as PlaywrightError
                            error_msg = str(e).lower()
                            is_closed = any(p in error_msg for p in ["target closed", "browser has been closed", "connection closed"])
                            is_canceled = ("canceled" in error_msg) or ("cancelled" in error_msg)
                            is_timeout = _is_timeout_download_error(e)
                            is_not_found = isinstance(e, FileNotFoundError) or _is_not_found_download_error(e)
                            is_access_denied = (
                                "access_denied" in error_msg
                                or "access denied" in error_msg
                                or "unauthorized" in error_msg
                                or "forbidden" in error_msg
                                or "invalid access" in error_msg
                                or "downloaded payload is html" in error_msg
                                or "returned html content" in error_msg
                            )
                            if is_not_found:
                                recent_not_found_urls[url] = time.monotonic() + not_found_cache_ttl_sec
                            if is_access_denied:
                                recent_access_denied_urls[url] = time.monotonic() + access_denied_cache_ttl_sec
                             
                            if (is_closed or is_canceled or is_timeout) and p_attempt < per_item_pw_attempts:
                                if is_canceled:
                                    logger.warning(
                                        "[Download][Worker %s] Download canceled during Playwright fallback; retrying (attempt %s) | url=%s",
                                        worker_id,
                                        p_attempt,
                                        url,
                                    )
                                    await asyncio.sleep(0.5)
                                    continue
                                if is_timeout:
                                    logger.warning(
                                        "[PlaywrightDiag][fallback_timeout_reuse_browser] worker=%s attempt=%s/%s browser_connected=%s url=%s",
                                        worker_id,
                                        p_attempt,
                                        per_item_pw_attempts,
                                        bool(current_browser and current_browser.is_connected()),
                                        url,
                                    )
                                    # A navigation/download timeout commonly means the target
                                    # server is slow. It is not evidence that Chromium failed;
                                    # the fallback helper closes its page/context in finally and
                                    # the next attempt reuses the shared browser.
                                    await asyncio.sleep(0.5)
                                    continue
                                logger.warning(
                                    "[PlaywrightDiag][fallback_target_closed] job_id=%s worker=%s attempt=%s/%s "
                                    "browser_connected=%s url=%s post_url=%s error=%s",
                                    file_meta.get("job_id"),
                                    worker_id,
                                    p_attempt,
                                    per_item_pw_attempts,
                                    bool(current_browser and current_browser.is_connected()),
                                    _short(url, 220),
                                    _short(source_page, 220),
                                    _short(last_err_msg, 360),
                                )
                                if browser_relauncher: # 釉뚮씪?곗? ?ъ떎???쒕룄
                                    try: browser = await browser_relauncher()
                                    except: pass
                                continue
                            
                            # 理쒖쥌 ?ㅽ뙣 ??濡쒓퉭
                            if is_closed:
                                logger.error(f"[Download][Worker {worker_id}] Playwright fallback failed (target closed): {e}")
                            elif is_canceled:
                                logger.warning(
                                    "[Download][Worker %s] Playwright fallback failed (download canceled) | url=%s err=%s",
                                    worker_id,
                                    url,
                                    e,
                                )
                            elif is_timeout:
                                logger.warning(
                                    "[Download][Worker %s] Playwright fallback timed out after retries; skip url=%s err=%s",
                                    worker_id,
                                    _short(url, 220),
                                    _short(e, 240),
                                )
                            elif is_not_found:
                                logger.warning(
                                    "[Download][Worker %s] Playwright fallback got 404; mark url as non-retryable for a while | url=%s err=%s",
                                    worker_id,
                                    _short(url, 220),
                                    _short(e, 240),
                                )
                            elif is_access_denied:
                                logger.warning(
                                    "[Download][Worker %s] Playwright fallback got access-denied/HTML; cache and skip | ttl=%.1fs url=%s err=%s",
                                    worker_id,
                                    access_denied_cache_ttl_sec,
                                    _short(url, 220),
                                    _short(e, 240),
                                )
                            elif "websync_failed" in error_msg:
                                logger.warning(
                                    "[Download][Worker %s] websync failed after local download; skip file_saved | url=%s err=%s",
                                    worker_id,
                                    _short(url, 220),
                                    _short(e, 240),
                                )
                            else:
                                logger.error(f"[Download][Worker {worker_id}] Playwright fallback failed: {e}", exc_info=True)
                            break
                        finally:
                            _release_playwright_browser(current_browser)
                    try:
                        playwright_fallback_sem.release()
                    except Exception:
                        pass
                logger.warning(
                    "[DownloadTrace][file_saved_not_emitted] job_id=%s worker=%s url=%s "
                    "post_url=%s name=%s reason=%s last_error=%s http_attempts=%s playwright_attempts=%s",
                    file_meta.get("job_id"),
                    worker_id,
                    _short(url, 220),
                    _short(source_page, 220),
                    _short(file_meta.get("name") or file_meta.get("subject"), 160),
                    _short(
                        file_meta.get("_download_empty_reason")
                        or ("http_no_file" if per_item_pw_attempts <= 0 else "http_and_playwright_no_file"),
                        180,
                    ),
                    _short(last_err_msg or file_meta.get("_download_last_error") or "-", 240),
                    per_item_http_retries,
                    per_item_pw_attempts,
                )
                # 엄격 도메인 공통 실패 시 즉시 재시도 루프를 막기 위한 쿨다운.
                if host and any(host == h or host.endswith("." + h) for h in strict_hosts) and cooldown_max_sec > 0:
                    do_cooldown = True
                    if last_err_msg:
                        low = last_err_msg.lower()
                        # 타임아웃/연결중단/리셋 계열에만 쿨다운 적용.
                        do_cooldown = any(
                            k in low
                            for k in (
                                "timeout",
                                "timed out",
                                "connection reset",
                                "err_connection_reset",
                                "hang",
                                "target closed",
                                "connection refused",
                            )
                        )
                    if do_cooldown:
                        cd = random.uniform(cooldown_min_sec, cooldown_max_sec)
                        logger.warning(
                            "[Download][Worker %s] strict domain cooldown | host=%s sleep_sec=%.1f url=%s",
                            worker_id,
                            host,
                            cd,
                            _short(url, 200),
                        )
                        try:
                            await asyncio.sleep(cd)
                        except Exception:
                            pass
                try:
                    empty_reason = (
                        file_meta.get("_download_empty_reason")
                        or (f"last_error:{_short(last_err_msg, 160)}" if last_err_msg else "")
                        or ("http_no_file" if per_item_pw_attempts <= 0 else "http_and_playwright_no_file")
                    )
                    file_meta["_download_empty_reason"] = empty_reason
                    skip_reason = "websync_failed" if str(empty_reason or "").startswith("websync_failed") else "download_failed"
                    skip_payload = {
                        "type": "download_skipped",
                        "url": url,
                        "reason": skip_reason,
                        "detail": empty_reason,
                        "source_page": source_page,
                        "source_url": source_page,
                        "name": file_meta.get("name") or file_meta.get("subject"),
                        "filename": file_meta.get("storage_filename") or file_meta.get("name") or file_meta.get("subject"),
                        "worker_id": worker_id,
                        "job_id": file_meta.get("job_id"),
                    }
                    if skip_reason == "websync_failed":
                        skip_payload.update(
                            {
                                "local_path": str(empty_reason or "").split("websync_failed:", 1)[-1] if "websync_failed:" in str(empty_reason or "") else "",
                                "public_url": file_meta.get("_websync_public_url") or "",
                                "public_status": file_meta.get("_websync_public_status") or "",
                            }
                        )
                    _schedule_failed_retry(file_meta, reason=skip_reason, detail=empty_reason)
                    await progress_queue.put(skip_payload)
                    if FLOW_DEBUG:
                        logger.info(
                            "[Flow] download_skipped | url=%s reason=download_failed",
                            _short(url, 220),
                        )
                except Exception as pq_err:
                    logger.error(
                        "[Download][Worker %s] progress_queue download_skipped 실패 — 로컬만 기록 | url=%s err=%s",
                        worker_id,
                        _short(url, 200),
                        pq_err,
                    )

                return None
            finally:
                if domain_lock_acquired and domain_sem is not None:
                    try:
                        domain_sem.release()
                    except Exception:
                        pass
                progress_queue.put_nowait({'type': 'in_flight', 'stage': 'download', 'delta': -1})

    cancelled = False
    retry_tasks: list[asyncio.Task] = []
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            session_for_retry = session
            if retry_enabled and retry_max_attempts > 0:
                retry_tasks = [
                    asyncio.create_task(_failed_retry_worker(i + 1), name=f"download-failed-retry-{worker_id}-{i + 1}")
                    for i in range(retry_workers)
                ]
            while True:
                try:
                    batch_items = await in_queue.get()
                    
                    if not batch_items:
                        in_queue.task_done()
                        continue

                    raw_queue = getattr(in_queue, "queue", None)
                    try:
                        queued_after_take = int(raw_queue.qsize()) if raw_queue is not None else -1
                    except Exception:
                        queued_after_take = -1
                    try:
                        unfinished_after_take = int(getattr(raw_queue, "_unfinished_tasks", -1) or 0)
                    except Exception:
                        unfinished_after_take = -1
                    logger.info(
                        "[FileCrawlTrace][queue_batch_taken] worker=%s lane=%s job_ids=%s batch_size=%s "
                        "queued_after_take=%s unfinished_after_take=%s",
                        worker_id,
                        worker_lane,
                        sorted({str((item or {}).get("job_id") or "") for item in batch_items if isinstance(item, dict)}),
                        len(batch_items or []),
                        queued_after_take,
                        unfinished_after_take,
                    )
                    try:
                        _batch_len = len(batch_items or [])
                        _batch_mode = "multi" if _batch_len >= 2 else "single"
                        for _item_index, _item in enumerate(batch_items or [], 1):
                            _item_data = _item if isinstance(_item, dict) else {}
                            logger.info(
                                "[파일크롤링추적][다운로드시작] 파일URL=%s\n작업자=%s 묶음수=%s 방식=%s 순번=%s 파일명=%s 게시물URL=%s 작업ID=%s",
                                _short(_item_data.get("url") or _item, 220),
                                worker_id,
                                _batch_len,
                                _batch_mode,
                                _item_index,
                                _item_data.get("name") or "",
                                _short(_item_data.get("source_page") or "", 220),
                                _item_data.get("job_id") or "",
                            )
                    except Exception:
                        pass

                    async def _download_item_with_activity(item: Any) -> Any:
                        item_meta = item if isinstance(item, dict) else {}
                        activity_token = _register_download_activity(worker_id, item, worker_lane)
                        if isinstance(item_meta, dict):
                            item_meta["_download_activity_token"] = activity_token
                            _set_download_activity_phase(item_meta, "item_enter")
                        started_at = time.monotonic()
                        hard_timeout_sec = _download_item_hard_timeout_sec(item_meta)
                        logger.info(
                            "[DownloadTrace][item_enter] job_id=%s worker=%s lane=%s timeout_sec=%.1f url=%s post_url=%s name=%s",
                            item_meta.get("job_id"),
                            worker_id,
                            worker_lane,
                            hard_timeout_sec,
                            _short(item_meta.get("url") or item_meta.get("_raw_url"), 220),
                            _short(item_meta.get("source_page") or item_meta.get("source_url"), 220),
                            _short(item_meta.get("name") or item_meta.get("subject"), 160),
                        )
                        download_task: Optional[asyncio.Task] = None
                        try:
                            download_task = asyncio.create_task(
                                download_item(session, item),
                                name=f"download-item-{worker_id}",
                            )
                            result = await asyncio.wait_for(
                                asyncio.shield(download_task),
                                timeout=hard_timeout_sec,
                            )
                            if result and result.get("deferred_to_large_lane"):
                                item_meta["_url_trace_outcome"] = "deferred"
                            elif result:
                                item_meta["_url_trace_outcome"] = "saved"
                                item_meta["_url_trace_path"] = result.get("file_path") or result.get("local_path")
                            else:
                                item_meta["_url_trace_outcome"] = "skipped"
                            return result
                        except asyncio.TimeoutError:
                            if download_task is not None and not download_task.done():
                                stack = [
                                    f"{frame.f_code.co_filename}:{frame.f_lineno}:{frame.f_code.co_name}"
                                    for frame in download_task.get_stack(limit=12)
                                ]
                                logger.warning(
                                    "[DownloadTrace][item_hard_timeout_stack] job_id=%s worker=%s stack=%s",
                                    item_meta.get("job_id"),
                                    worker_id,
                                    stack or ["<no-python-frame>"],
                                )
                                download_task.cancel()
                                await asyncio.gather(download_task, return_exceptions=True)
                            reason = f"item_hard_timeout:{hard_timeout_sec:.0f}s"
                            if isinstance(item, dict):
                                item["_download_empty_reason"] = reason
                                item["_download_last_error"] = reason
                            await _cleanup_active_download_temp_file(
                                item_meta,
                                reason="item_hard_timeout",
                                worker_id=worker_id,
                            )
                            timeout_phase = str(item_meta.get("_download_trace_phase") or "unknown")
                            timeout_phase_started_at = float(item_meta.get("_download_trace_phase_started_at") or started_at)
                            logger.warning(
                                "[DownloadTrace][item_hard_timeout] job_id=%s worker=%s timeout_sec=%.1f "
                                "elapsed_sec=%.3f phase=%s phase_elapsed_sec=%.3f url=%s post_url=%s name=%s",
                                item_meta.get("job_id"),
                                worker_id,
                                hard_timeout_sec,
                                max(0.0, time.monotonic() - started_at),
                                timeout_phase,
                                max(0.0, time.monotonic() - timeout_phase_started_at),
                                _short(item_meta.get("url") or item_meta.get("_raw_url"), 220),
                                _short(item_meta.get("source_page") or item_meta.get("source_url"), 220),
                                _short(item_meta.get("name") or item_meta.get("subject"), 160),
                            )
                            try:
                                _schedule_failed_retry(item_meta, reason="download_timeout", detail=reason)
                            except Exception as retry_exc:
                                logger.exception(
                                    "[DownloadRetry] schedule_failed | job_id=%s worker=%s url=%s err=%s",
                                    item_meta.get("job_id"),
                                    worker_id,
                                    _short(item_meta.get("url") or item_meta.get("_raw_url"), 220),
                                    retry_exc,
                                )
                            await progress_queue.put(
                                {
                                    "type": "download_skipped",
                                    "url": item_meta.get("url") or item_meta.get("_raw_url"),
                                    "reason": "download_timeout",
                                    "detail": reason,
                                    "source_page": item_meta.get("source_page") or item_meta.get("source_url"),
                                    "name": item_meta.get("name") or item_meta.get("subject"),
                                    "worker_id": worker_id,
                                    "job_id": item_meta.get("job_id"),
                                }
                            )
                            return None
                        except Exception as exc:
                            if isinstance(item, dict):
                                item["_url_trace_outcome"] = "exception"
                                item["_url_trace_error"] = f"{type(exc).__name__}: {exc}"
                            raise
                        finally:
                            if download_task is not None and not download_task.done():
                                download_task.cancel()
                                await asyncio.gather(download_task, return_exceptions=True)
                            if isinstance(item, dict):
                                item["_download_elapsed_sec"] = max(0.0, time.monotonic() - started_at)
                            _clear_download_activity(activity_token)
                            if isinstance(item_meta, dict):
                                item_meta.pop("_download_activity_token", None)
                            try:
                                await _append_download_url_trace(
                                    {
                                        "event": "terminal",
                                        "job_id": item_meta.get("job_id"),
                                        "worker_id": worker_id,
                                        "url": item_meta.get("url") or item_meta.get("_raw_url"),
                                        "post_url": item_meta.get("source_page") or item_meta.get("source_url"),
                                        "outcome": item_meta.get("_url_trace_outcome")
                                        or ("skipped" if item_meta.get("_download_empty_reason") else "unknown"),
                                        "reason": item_meta.get("_download_empty_reason") or "",
                                        "error": item_meta.get("_url_trace_error")
                                        or item_meta.get("_download_last_error")
                                        or "",
                                        "elapsed_sec": item_meta.get("_download_elapsed_sec"),
                                        "saved_path": item_meta.get("_url_trace_path") or "",
                                    }
                                )
                            except Exception:
                                pass
                    task_to_index = {
                        asyncio.create_task(_download_item_with_activity(item)): index
                        for index, item in enumerate(batch_items)
                    }
                    pending_tasks = set(task_to_index)
                    raw_results = [None] * len(batch_items)
                    results = []
                    failed_count = 0
                    try:
                        while pending_tasks:
                            completed, pending_tasks = await asyncio.wait(
                                pending_tasks,
                                timeout=15.0,
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if not completed:
                                pending_urls = [
                                    _short(
                                        ((batch_items[task_to_index[task]] or {}).get("url")
                                        if isinstance(batch_items[task_to_index[task]], dict)
                                        else batch_items[task_to_index[task]]),
                                        180,
                                    )
                                    for task in pending_tasks
                                ][:5]
                                pending_details = []
                                now_mono = time.monotonic()
                                for task in pending_tasks:
                                    pending_item = batch_items[task_to_index[task]]
                                    pending_meta = pending_item if isinstance(pending_item, dict) else {}
                                    phase_started_at = float(
                                        pending_meta.get("_download_trace_phase_started_at") or now_mono
                                    )
                                    pending_details.append(
                                        {
                                            "url": _short(pending_meta.get("url") or pending_item, 180),
                                            "phase": pending_meta.get("_download_trace_phase") or "unknown",
                                            "phase_elapsed_sec": round(max(0.0, now_mono - phase_started_at), 3),
                                            "http_attempt": pending_meta.get("_download_http_attempt") or 0,
                                        }
                                    )
                                first_pending = batch_items[task_to_index[next(iter(pending_tasks))]]
                                first_pending_meta = first_pending if isinstance(first_pending, dict) else {}
                                logger.info(
                                    "[DownloadTrace][batch_waiting] job_id=%s worker=%s pending=%s batch_size=%s urls=%s details=%s",
                                    first_pending_meta.get("job_id") or "",
                                    worker_id,
                                    len(pending_tasks),
                                    len(batch_items),
                                    pending_urls,
                                    pending_details[:5],
                                )
                                continue
                            for task in completed:
                                item_index = task_to_index[task]
                                item = batch_items[item_index]
                                try:
                                    result = task.result()
                                except asyncio.CancelledError:
                                    result = None
                                    if isinstance(item, dict):
                                        item["_download_empty_reason"] = "stop_requested"
                                except Exception as exc:
                                    result = exc
                                raw_results[item_index] = result
                                item_meta = item if isinstance(item, dict) else {}
                                terminal_elapsed = float(item_meta.get("_download_elapsed_sec") or 0.0)
                                terminal_url = _short(item_meta.get("url") or item, 220)
                                terminal_post_url = _short(item_meta.get("source_page") or item_meta.get("source_url") or "", 220)
                                terminal_name = _short(item_meta.get("name") or item_meta.get("subject") or "", 160)
                                if isinstance(result, Exception):
                                    terminal_outcome = "exception"
                                    terminal_reason = _short(result, 240)
                                    terminal_level = logging.ERROR
                                elif result and result.get("deferred_to_large_lane"):
                                    terminal_outcome = "deferred"
                                    terminal_reason = "response_content_length_large"
                                    terminal_level = logging.INFO
                                elif result:
                                    terminal_outcome = "downloaded"
                                    terminal_reason = _short(result.get("file_path") or result.get("local_path") or "", 260)
                                    terminal_level = logging.INFO
                                else:
                                    terminal_outcome = "skipped"
                                    terminal_reason = _short(item_meta.get("_download_empty_reason") or "unknown", 180)
                                    terminal_level = logging.INFO
                                logger.log(
                                    terminal_level,
                                    "[FileCrawlTrace][download_terminal] worker=%s job_id=%s outcome=%s elapsed_sec=%.2f "
                                    "reason_or_path=%s url=%s post_url=%s name=%s",
                                    worker_id,
                                    item_meta.get("job_id"),
                                    terminal_outcome,
                                    terminal_elapsed,
                                    terminal_reason,
                                    terminal_url,
                                    terminal_post_url,
                                    terminal_name,
                                )
                                if isinstance(result, Exception):
                                    failed_count += 1
                                    logger.error(
                                        "[Download][Worker %s] item failed inside batch; continuing siblings | url=%s err=%s",
                                        worker_id,
                                        _short((item or {}).get("url") if isinstance(item, dict) else item, 220),
                                        result,
                                    )
                                    continue
                                results.append(result)
                                if result and out_queue and not result.get("defer_save_batch_until_learn_list") and not result.get("deferred_to_large_lane"):
                                    await out_queue.put(result)
                    except BaseException:
                        for task in pending_tasks:
                            task.cancel()
                        if pending_tasks:
                            await asyncio.gather(*pending_tasks, return_exceptions=True)
                        raise
                    try:
                        deferred_count = sum(
                            1
                            for result in results
                            if isinstance(result, dict) and result.get("deferred_to_large_lane")
                        )
                        ok_count = sum(
                            1
                            for result in results
                            if result and not (isinstance(result, dict) and result.get("deferred_to_large_lane"))
                        )
                        empty_count = max(0, len(batch_items or []) - failed_count - ok_count - deferred_count)
                        _batch_len = len(batch_items or [])
                        _batch_mode = "multi" if _batch_len >= 2 else "single"
                        empty_sample = [
                            {
                                "name": ((item or {}).get("name") if isinstance(item, dict) else "") or "",
                                "url": _short((item or {}).get("url") if isinstance(item, dict) else item, 220),
                                "reason": ((item or {}).get("_download_empty_reason") if isinstance(item, dict) else "") or "",
                                "last_error": _short(((item or {}).get("_download_last_error") if isinstance(item, dict) else "") or "", 180),
                            }
                            for item, result in zip(batch_items or [], raw_results or [])
                            if not isinstance(result, Exception) and not result
                        ][:10]
                        empty_reasons = [str(sample.get("reason") or "") for sample in empty_sample]
                        reason_counts: Dict[str, int] = {}
                        for reason in empty_reasons:
                            key = reason or "unknown"
                            reason_counts[key] = int(reason_counts.get(key, 0) or 0) + 1
                        benign_empty = bool(empty_reasons) and all(
                            reason == "viewer_convert_url"
                            or reason == "websync_failed"
                            or reason.startswith(("non_doc_file:", "non_doc_mime:", "non_doc_precheck"))
                            for reason in empty_reasons
                        )
                        batch_log_level = logging.DEBUG
                        if failed_count:
                            batch_log_level = logging.WARNING
                        elif empty_count and not benign_empty:
                            batch_log_level = logging.WARNING
                        logger.log(
                            batch_log_level,
                            "[FileMultiAttachDebug][download.batch_result] worker=%s batch_size=%s mode=%s ok=%s empty=%s failed=%s reasons=%s sample=%s",
                            worker_id,
                            _batch_len,
                            _batch_mode,
                            ok_count,
                            empty_count,
                            failed_count,
                            reason_counts,
                            empty_sample[:3],
                        )
                    except Exception:
                        pass

                    for res in results:
                        if res and out_queue and res.get("defer_save_batch_until_learn_list") and FLOW_DEBUG:
                            logger.info(
                                "[Flow] out_queue skip (defer_save_batch_until_learn_list) | path=%s url=%s",
                                _short(res.get("file_path") or res.get("local_path"), 260),
                                _short(res.get("url"), 200),
                            )
                    try:
                        ok_count = sum(1 for r in results if r)
                    except Exception:
                        pass
                    in_queue.task_done()
                    try:
                        queued_after_done = int(raw_queue.qsize()) if raw_queue is not None else -1
                    except Exception:
                        queued_after_done = -1
                    try:
                        unfinished_after_done = int(getattr(raw_queue, "_unfinished_tasks", -1) or 0)
                    except Exception:
                        unfinished_after_done = -1
                    logger.info(
                        "[FileCrawlTrace][queue_batch_done] worker=%s lane=%s job_ids=%s batch_size=%s downloaded=%s skipped=%s exceptions=%s "
                        "queued_after_done=%s unfinished_after_done=%s",
                        worker_id,
                        worker_lane,
                        sorted({str((item or {}).get("job_id") or "") for item in batch_items if isinstance(item, dict)}),
                        len(batch_items or []),
                        sum(1 for result in raw_results if result and not isinstance(result, Exception)),
                        sum(1 for result in raw_results if result is None),
                        sum(1 for result in raw_results if isinstance(result, Exception)),
                        queued_after_done,
                        unfinished_after_done,
                    )
                except asyncio.CancelledError:
                    cancelled = True
                    break
                except Exception as e:
                    logger.error(f"[Download] Error: {e}")
                    in_queue.task_done()
    finally:
        for task in retry_tasks:
            try:
                task.cancel()
            except Exception:
                pass
        if retry_tasks:
            try:
                await asyncio.gather(*retry_tasks, return_exceptions=True)
            except Exception:
                pass
        logger.info("[Download][Worker %s] 작업중지 (cancelled=%s)", worker_id, cancelled)









