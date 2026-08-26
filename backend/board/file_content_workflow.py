"""File crawl pipeline helpers built on the legacy board workflow surface.

This module owns attachment enqueueing, save progress, file learning handoff,
and file-crawl-specific progress reconciliation.
"""

from __future__ import annotations

import re
import asyncio
import logging
import os
import time
import unicodedata
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from config.settings import settings
from core.crawler.file_download_topology import file_crawl_download_topology
from utils.attachment_url_normalize import (
    canonicalize_attachment_url_for_learn_list,
    extract_attachment_key_candidates,
)
from utils.download_doc_filter import should_skip_attachment_at_scan
from utils.download_integrity import wait_for_file_ready
from utils.file import (
    parse_display_file_size_bytes,
    preserve_file_learning_subject,
    strip_fallback_download_label,
    strip_file_type_display_prefix,
    strip_trailing_file_size,
)
from utils.rrn_pattern_guard import learning_blocked_by_rrn_pattern, mask_rrn_like_patterns
from utils.url import canonicalize_url_for_dedup, extract_download_url_from_js
from core.crawler.dedup import get_cross_job_claim_info, try_acquire_cross_job_claim
from backend.board.anseong_file import (
    extract_anseong_attachment_key_candidates,
    resolve_anseong_yhlib_download_url,
)
from backend.shared.completed_url_ttl_cache import completed_url_cached, remember_completed_url
from backend.shared.file_name_debug import emit_file_name_debug
from backend.file.file_pg_duplicate import build_file_pg_duplicate_fingerprint
from backend.file.file_simhash_duplicate import (
    build_file_simhash_claim_key,
    build_file_simhash_duplicate_key,
)
from backend.file.file_learn_list_persistence import run_learn_list_ensure

from urllib.parse import unquote, urlencode, urlparse


try:
    from backend.shared.stage_url_report import append_stage_urls  # type: ignore
except Exception:

    def append_stage_urls(
        *,
        stage,
        urls,
        job_id=None,
        db_name=None,
        output_dir=None,
        extra_meta=None,
        entry_extra=None,
    ):
        try:
            import sys as _sys
            import os as _os

            project_root = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", ".."))
            if project_root not in _sys.path:
                _sys.path.insert(0, project_root)
            from backend.shared.stage_url_report import append_stage_urls as _impl  # type: ignore

            return _impl(
                stage=stage,
                urls=urls,
                job_id=job_id,
                db_name=db_name,
                output_dir=output_dir,
                extra_meta=extra_meta,
                entry_extra=entry_extra,
            )
        except Exception:
            return None


logger = logging.getLogger("backend.board.file_content_workflow")
FILE_DASHBOARD_DOWNLOAD_DEBUG_PREFIX = "[FILE_DASHBOARD_DOWNLOAD_DEBUG]"
_FILE_PDF_LARGE_FILE_BYTES = 50 * 1024 * 1024
_FILE_PDF_TEXT_EXTRACT_TIMEOUT_SEC = 180.0
_FILE_PDF_LARGE_TEXT_EXTRACT_TIMEOUT_SEC = 600.0


def _file_pdf_text_extract_timeout_sec(path: str) -> float:
    """Use a longer bounded budget only for genuinely large PDF files."""
    try:
        size = os.path.getsize(path) if path and os.path.isfile(path) else 0
    except OSError:
        size = 0
    if size >= _FILE_PDF_LARGE_FILE_BYTES:
        return _FILE_PDF_LARGE_TEXT_EXTRACT_TIMEOUT_SEC
    return _FILE_PDF_TEXT_EXTRACT_TIMEOUT_SEC


def _clip_log_value(value: Any, limit: int = 240) -> str:
    try:
        text = str(value or "").replace("\n", "\\n").replace("\r", "\\r").strip()
    except Exception:
        text = ""
    if len(text) > limit:
        return text[: max(0, limit - 3)] + "..."
    return text


def _file_pg_duplicate_fingerprint(file_name: Any, file_size: Any) -> Optional[Tuple[str, int]]:
    """Compatibility wrapper for the file duplicate identity helper."""
    return build_file_pg_duplicate_fingerprint(file_name, file_size)


def _attachment_exact_file_size_bytes(attachment: Any) -> int:
    """Read only an explicit, exact attachment byte count for pre-download skips."""
    if not isinstance(attachment, dict):
        return 0
    for key in ("exact_file_size_bytes", "file_size_bytes", "byte_size"):
        try:
            value = str(attachment.get(key) or "").strip()
        except Exception:
            value = ""
        if re.fullmatch(r"[1-9]\d{0,15}", value):
            try:
                return int(value)
            except ValueError:
                return 0
    return 0


def _response_content_length_bytes(headers: Any) -> int:
    """Accept an HTTP file byte length only when it is safe to compare exactly."""
    try:
        if str(headers.get("content-encoding") or "").strip():
            return 0
        value = str(headers.get("content-length") or "").strip()
    except Exception:
        return 0
    if not re.fullmatch(r"[1-9]\d{0,15}", value):
        return 0
    try:
        return int(value)
    except ValueError:
        return 0


def _response_attachment_filename(headers: Any) -> str:
    """Extract the server filename from response headers for pre-download identity."""
    try:
        content_disposition = str(
            headers.get("content-disposition") or headers.get("Content-Disposition") or ""
        )
    except Exception:
        return ""
    if not content_disposition:
        return ""
    match = re.search(r"filename\*\s*=\s*(?:UTF-8'')?\"?([^;\"]+)", content_disposition, re.IGNORECASE)
    if not match:
        match = re.search(r"filename\s*=\s*\"?([^;\"]+)", content_disposition, re.IGNORECASE)
    if not match:
        return ""
    name = unquote(match.group(1).strip().strip("'\\\""))
    return strip_file_type_display_prefix(strip_trailing_file_size(name)) or name


def _log_file_url_status(
    *,
    stage: str,
    status: str,
    process_url: str = "",
    post_url: str = "",
    file_url: str = "",
    selected: str = "",
    saved: str = "",
    learn: str = "",
    reason: str = "",
    error: Any = "",
    name: str = "",
    count: Any = "",
    learn_list_id: Any = "",
    job_id: Any = "",
    db_name: Any = "",
) -> None:
    try:
        status_text = str(status or "").strip().lower()
        reason_text = str(reason or "").strip().lower()
        error_text = str(error or "").strip()
        error_lower = error_text.lower()
        normal_skip_reasons = {
            "non_doc_file",
            "non_doc_precheck",
            "non_doc_mime",
            "viewer_convert_url",
            "scan_filter_non_doc",
            "completed_cache",
            "db_duplicate",
            "duplicate_existing",
            "duplicate_reuse_learned",
            "file_pipeline_skip_learning",
            "list_page",
            "menu_shell",
            "list_page_no_attachment_extract",
            "no_attachments",
            "attachment_empty",
        }
        failure_reasons = {
            "exception",
            "learn_list_no_row",
            "file_text_extract_empty",
            "learning_pipeline_failed",
            "upload_copy_failed",
            "download_failed",
            "download_timeout",
            "download timeout",
            "timeout",
            "connectiontimeouterror",
            "connection_timeout",
            "connection timeout",
            "ocr_status_429",
            "ocr_api_failed",
        }
        failure_tokens = (
            "download_failed",
            "download timeout",
            "download_timeout",
            "connectiontimeouterror",
            "connection timeout",
            "timeout",
            "timed out",
            "ocr_status_429",
            "statuscode 429",
            "status code 429",
            'response 429',
            "http 429",
            "429",
            "failed",
            "error",
            "exception",
        )
        is_normal_skip = reason_text in normal_skip_reasons
        is_error = (
            status_text in {"error", "failed"}
            or reason_text in failure_reasons
            or any(token in error_lower for token in failure_tokens)
        )
        if not is_error or is_normal_skip:
            return
        logger.error(
            "file crawl url error | stage=%s status=%s job_id=%s db=%s process_url=%s post_url=%s file_url=%s selected=%s saved=%s learn=%s count=%s learn_list_id=%s name=%s reason=%s error=%s",
            _clip_log_value(stage, 80),
            _clip_log_value(status, 80),
            _clip_log_value(job_id, 80),
            _clip_log_value(db_name, 80),
            _clip_log_value(process_url, 500),
            _clip_log_value(post_url, 500),
            _clip_log_value(file_url, 500),
            _clip_log_value(selected, 40),
            _clip_log_value(saved, 40),
            _clip_log_value(learn, 40),
            _clip_log_value(count, 40),
            _clip_log_value(learn_list_id, 80),
            _clip_log_value(name, 260),
            _clip_log_value(reason, 300),
            _clip_log_value(error, 800),
        )
    except Exception:
        pass


def _file_study_debug_enabled() -> bool:
    return str(os.getenv("FILE_STUDY_DEBUG", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return default


def _file_study_stats_snapshot(workflow: Any) -> Dict[str, int]:
    try:
        stats = getattr(workflow, "stats", {}) or {}
    except Exception:
        stats = {}
    save = _safe_int(stats.get("file_save_count", stats.get("save_count", 0)))
    study_done = _safe_int(stats.get("file_study_done_count", stats.get("study_done_count", 0)))
    study_success = _safe_int(stats.get("file_study_success_count", stats.get("study_success_count", 0)))
    study_failed = _safe_int(stats.get("file_study_failed_count", stats.get("study_failed_count", 0)))
    study_skipped = _safe_int(stats.get("file_study_skipped_count", stats.get("study_skipped_count", 0)))
    study_accounted = min(save, max(0, study_success + study_failed))
    return {
        "save": save,
        "study_done": study_done,
        "study_success": study_success,
        "study_failed": study_failed,
        "study_skipped": study_skipped,
        "study_accounted": study_accounted,
        "save_study_gap": max(0, save - study_accounted),
    }


def _log_file_study_debug(workflow: Any, event: str, **fields: Any) -> None:
    if not _file_study_debug_enabled():
        return
    post_url = str(fields.pop("post_url", "") or "").strip()
    merged: Dict[str, Any] = _file_study_stats_snapshot(workflow)
    merged.update(fields)
    parts: List[str] = []
    for key, value in merged.items():
        if value is None:
            continue
        text = str(value).replace("\n", "\\n")
        if len(text) > 240:
            text = text[:237] + "..."
        parts.append(f"{key}={text}")
    if post_url:
        logger.info(
            "[FileStudyDebug][%s] post_url=%s\n%s",
            event,
            post_url,
            " ".join(parts) if parts else "-",
        )
    else:
        suffix = " " + " ".join(parts) if parts else ""
        logger.info("[FileStudyDebug][%s]%s", event, suffix)


def _log_file_processing_trace(
    *,
    stage: str,
    post_url: Any,
    file_url: Any = "",
    file_name: Any = "",
    selected: str = "예",
    saved: str = "대기",
    learn: str = "대기",
    learn_list_id: Any = None,
    reason: Any = "",
) -> None:
    """파일별 선별 이후 저장·학습 흐름을 게시물 URL 기준으로 남긴다."""
    logger.info(
        "[파일처리추적] 게시물=%s\n단계=%s 파일=%s 파일URL=%s 선별=%s 저장=%s 학습=%s learn_list_id=%s 사유=%s",
        str(post_url or ""),
        str(stage or ""),
        str(file_name or ""),
        str(file_url or ""),
        str(selected or "-"),
        str(saved or "-"),
        str(learn or "-"),
        str(learn_list_id or "-"),
        str(reason or "-"),
    )


def _file_debug_log_level() -> int:
    raw = str(os.getenv("FILE_DASHBOARD_DOWNLOAD_DEBUG_LEVEL", "DEBUG") or "DEBUG").strip().upper()
    if raw in {"WARNING", "WARN"}:
        return logging.WARNING
    if raw == "INFO":
        return logging.INFO
    return logging.DEBUG


def _file_dashboard_download_debug(message: str, *args: Any) -> None:
    try:
        logger.log(_file_debug_log_level(), "%s " + message, FILE_DASHBOARD_DOWNLOAD_DEBUG_PREFIX, *args)
    except Exception:
        pass


def _file_multi_attach_debug(message: str, *args: Any) -> None:
    try:
        logger.log(_file_debug_log_level(), message, *args)
    except Exception:
        pass


def _file_candidate_response_validation_enabled() -> bool:
    return str(os.getenv("FILE_CRAWL_CANDIDATE_RESPONSE_VALIDATION", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _file_candidate_response_validation_threshold() -> float:
    try:
        value = float(os.getenv("FILE_CRAWL_CANDIDATE_RESPONSE_VALIDATION_SCORE_MAX", "0.74") or "0.74")
    except Exception:
        value = 0.74
    return max(0.0, min(value, 1.0))


def _file_candidate_response_validation_timeout_sec() -> float:
    try:
        value = float(os.getenv("FILE_CRAWL_CANDIDATE_RESPONSE_VALIDATION_TIMEOUT_SEC", "4") or "4")
    except Exception:
        value = 4.0
    return max(1.0, min(value, 15.0))


def _file_candidate_response_validation_max_per_detail() -> int:
    try:
        value = int(os.getenv("FILE_CRAWL_CANDIDATE_RESPONSE_VALIDATION_MAX_PER_DETAIL", "8") or "8")
    except Exception:
        value = 8
    return max(0, min(value, 64))


def _file_candidate_response_validation_budget_sec() -> float:
    try:
        value = float(os.getenv("FILE_CRAWL_CANDIDATE_RESPONSE_VALIDATION_BUDGET_SEC", "10") or "10")
    except Exception:
        value = 10.0
    return max(1.0, min(value, 60.0))


def _file_candidate_response_validation_concurrency() -> int:
    try:
        value = int(os.getenv("FILE_CRAWL_CANDIDATE_RESPONSE_VALIDATION_CONCURRENCY", "2") or "2")
    except Exception:
        value = 2
    return max(1, min(value, 8))


def _is_generic_attachment_display_name(value: Any) -> bool:
    text = re.sub(r"\s+", "", str(value or "")).strip().lower()
    if not text:
        return True
    if "." in text:
        stem = text.rsplit(".", 1)[0]
    else:
        stem = text
    return stem in {
        "새창열림",
        "새창열기",
        "새창",
        "다운로드",
        "파일다운로드",
        "첨부파일",
        "첨부",
        "download",
        "file",
        "attachment",
        "view",
        "open",
    }


def _file_name_from_download_url(file_url: str) -> str:
    try:
        path = urlparse(str(file_url or "")).path
    except Exception:
        path = ""
    name = unquote(os.path.basename(path or "")).strip()
    if not name or "." not in name:
        return ""
    if _is_generic_attachment_display_name(name):
        return ""
    return strip_file_type_display_prefix(strip_trailing_file_size(name)) or name

def _looks_like_file_response(headers: Any, file_url: str = "", file_name: str = "") -> bool:
    try:
        content_disposition = str(headers.get("Content-Disposition") or headers.get("content-disposition") or "").lower()
        content_type = str(headers.get("Content-Type") or headers.get("content-type") or "").lower()
    except Exception:
        content_disposition = ""
        content_type = ""
    if "attachment" in content_disposition or "filename" in content_disposition:
        return True
    if any(token in content_type for token in (
        "application/pdf",
        "application/octet-stream",
        "application/haansofthwp",
        "application/vnd",
        "application/zip",
        "application/x-zip-compressed",
        "image/",
        "text/csv",
        "text/plain",
    )):
        return True
    return False


def _looks_like_html_response_body(chunk: bytes) -> bool:
    head = (chunk or b"").lstrip().lower()
    return head.startswith((b"<!doctype html", b"<html", b"<head", b"<body", b"<script"))


def _file_candidate_scan_filter_trusted(attach: Dict[str, Any]) -> bool:
    kind = str((attach or {}).get("kind") or (attach or {}).get("source") or "").strip().lower()
    if kind in {"egov_pair", "nd_file_pair", "ne_direct_file", "gm"}:
        return True
    try:
        score = float((attach or {}).get("candidate_score") or 0.0)
    except Exception:
        score = 0.0
    reason = str((attach or {}).get("candidate_reason") or "").strip().lower()
    return score >= 0.88 and any(token in reason for token in ("download", "file", "egov", "nd_file"))


def _file_candidate_is_clear_document(file_url: str, file_name: str = "", attach: Optional[Dict[str, Any]] = None) -> bool:
    blob = " ".join(str(v or "") for v in (file_url, file_name)).lower()
    if re.search(r"\.(?:hwp|hwpx|pdf|doc|docx|xls|xlsx|ppt|pptx|csv|txt)(?:$|[?#;&\s])", blob):
        return True
    url_l = str(file_url or "").lower()
    handler_tokens = (
        "file/download",
        "filedown",
        "filedownload",
        "downloadbbsfile",
        "atchfileid",
        "atch_file",
        "nd_filedownload",
    )
    if any(token in url_l for token in handler_tokens):
        return True
    if _file_candidate_scan_filter_trusted(attach or {}):
        return True
    return False


def _content_author_debug_enabled() -> bool:
    return str(
        os.getenv("FILE_CONTENT_AUTHOR_DEBUG", os.getenv("CONTENT_AUTHOR_DEBUG", "0"))
        or "0"
    ).strip().lower() in {"1", "true", "yes", "on"}


def _content_author_debug_value(value: Any, limit: int = 180) -> str:
    try:
        text = str(value or "").strip()
    except Exception:
        text = ""
    return text[:limit]


def _filename_debug_log(location: str, **data: Any) -> None:
    emit_file_name_debug(component="file_content", location=location, data=data, logger=logger)


def _append_file_extension_if_missing(title: str, *sources: Any) -> str:
    text = str(title or "").strip()
    if not text or os.path.splitext(text)[1]:
        return text
    for source in sources:
        raw = str(source or "").strip()
        if not raw:
            continue
        try:
            path = urlparse(raw).path if "://" in raw else raw
        except Exception:
            path = raw
        ext = os.path.splitext(path)[1].strip()
        if ext and len(ext) <= 10 and re.match(r"^\.[A-Za-z0-9]+$", ext):
            return f"{text}{ext}"
    return text


def _resolve_learning_title(info: Optional[Dict[str, Any]], file_path: str = "") -> str:
    if not isinstance(info, dict):
        return os.path.basename(file_path or "")
    original_meta = info.get("original_meta")
    inner_meta = original_meta.get("original_meta") if isinstance(original_meta, dict) else None
    extension_sources = [
        file_path,
        info.get("file_path"),
        info.get("local_path"),
        info.get("url"),
        info.get("content"),
        info.get("source_url"),
        info.get("href"),
        original_meta.get("url") if isinstance(original_meta, dict) else None,
        original_meta.get("content") if isinstance(original_meta, dict) else None,
        original_meta.get("source_url") if isinstance(original_meta, dict) else None,
        original_meta.get("href") if isinstance(original_meta, dict) else None,
        inner_meta.get("url") if isinstance(inner_meta, dict) else None,
        inner_meta.get("content") if isinstance(inner_meta, dict) else None,
        inner_meta.get("source_url") if isinstance(inner_meta, dict) else None,
        inner_meta.get("href") if isinstance(inner_meta, dict) else None,
    ]
    candidates = [
        info.get("display_name"),
        info.get("attachment_name"),
        original_meta.get("attachment_name") if isinstance(original_meta, dict) else None,
        inner_meta.get("attachment_name") if isinstance(inner_meta, dict) else None,
        info.get("title"),
        original_meta.get("title") if isinstance(original_meta, dict) else None,
        inner_meta.get("title") if isinstance(inner_meta, dict) else None,
        info.get("subject"),
        info.get("name"),
        original_meta.get("name") if isinstance(original_meta, dict) else None,
        inner_meta.get("name") if isinstance(inner_meta, dict) else None,
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            cleaned_text = strip_fallback_download_label(text) or text
            return _append_file_extension_if_missing(cleaned_text, *extension_sources)
    return os.path.basename(file_path or "")


def _resolve_downloaded_file_subject(info: Optional[Dict[str, Any]], file_path: str = "") -> str:
    """Resolve the final downloaded filename for DB/PG identity after file_saved."""
    candidates: List[Any] = []
    if isinstance(info, dict):
        original_meta = info.get("original_meta") if isinstance(info.get("original_meta"), dict) else {}
        candidates.extend(
            [
                info.get("attachment_name"),
                info.get("display_name"),
                original_meta.get("attachment_name"),
                original_meta.get("display_name"),
                info.get("original_name"),
                original_meta.get("original_name"),
                info.get("subject"),
                info.get("name"),
                info.get("saved_filename"),
                info.get("storage_filename"),
                os.path.basename(str(info.get("file_path") or "")),
                os.path.basename(str(info.get("local_path") or "")),
            ]
        )
    if file_path:
        candidates.append(os.path.basename(file_path))
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text:
            continue
        cleaned = strip_fallback_download_label(text) or text
        return preserve_file_learning_subject(cleaned)
    return preserve_file_learning_subject(_resolve_learning_title(info, file_path))


def _resolve_persisted_file_learning_subject(
    persisted_subject: str,
    info: Optional[Dict[str, Any]],
    file_path: str = "",
) -> str:
    """Keep PostgreSQL chunk subjects identical to the MariaDB file subject."""
    subject = str(persisted_subject or "").strip() or _resolve_learning_title(info, file_path)
    return preserve_file_learning_subject(subject)


def _resolve_file_detail_source_url(info: Optional[Dict[str, Any]]) -> str:
    if not isinstance(info, dict):
        return ""
    original_meta = info.get("original_meta") if isinstance(info.get("original_meta"), dict) else {}
    inner_meta = original_meta.get("original_meta") if isinstance(original_meta.get("original_meta"), dict) else {}
    candidates = (
        info.get("source_page"),
        info.get("post_url"),
        info.get("board_url"),
        info.get("detail_url"),
        original_meta.get("source_page"),
        original_meta.get("post_url"),
        original_meta.get("board_url"),
        original_meta.get("detail_url"),
        inner_meta.get("source_page"),
        inner_meta.get("post_url"),
        inner_meta.get("board_url"),
        inner_meta.get("detail_url"),
        original_meta.get("source_url"),
        info.get("source_url"),
    )
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _file_info_with_persisted_identity(
    info: Optional[Dict[str, Any]],
    *,
    subject: str,
    storage_filename: str,
    file_size: Any = None,
) -> Dict[str, Any]:
    """Carry one logical name and one physical name through PG metadata unchanged."""
    result = dict(info or {})
    original_meta = result.get("original_meta")
    metadata = dict(original_meta) if isinstance(original_meta, dict) else {}
    result.update(
        {
            "name": subject,
            "subject": subject,
            "source_filename": subject,
            "display_name": subject,
            "attachment_name": subject,
            "storage_filename": storage_filename,
        }
    )
    try:
        size_value = int(file_size or result.get("file_size") or result.get("size") or 0)
    except Exception:
        size_value = 0
    detail_source_url = _resolve_file_detail_source_url(result)
    metadata_update = {
        "attachment_name": subject,
        "display_name": subject,
        "source_filename": subject,
        "storage_filename": storage_filename,
    }
    if detail_source_url:
        result["source_url"] = detail_source_url
        result["source_page"] = result.get("source_page") or detail_source_url
        metadata_update["source_url"] = detail_source_url
        metadata_update["source_page"] = detail_source_url
    if size_value > 0:
        metadata_update["file_size"] = size_value
        metadata_update["content_size_bytes"] = size_value
    metadata.update(metadata_update)
    result["original_meta"] = metadata
    return result


def _file_pipeline_call_log_enabled() -> bool:
    return str(os.getenv("FILE_PIPELINE_CALL_LOG", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _file_pipeline_bottleneck_log_enabled() -> bool:
    return str(os.getenv("FILE_PIPELINE_BOTTLENECK_LOG", "0")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _file_crawl_save_throttle_seconds() -> float:
    try:
        ms = float(os.getenv("FILE_CRAWL_SAVE_THROTTLE_MS", "200") or "200")
    except Exception:
        ms = 200.0
    ms = max(0.0, min(ms, 10_000.0))
    return ms / 1000.0


def _file_crawl_learn_backpressure_enabled() -> bool:
    return str(os.getenv("FILE_CRAWL_LEARN_BACKPRESSURE", "1")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _file_crawl_learn_backpressure_max_gap() -> int:
    default_gap = _file_crawl_learn_completion_window_default()
    try:
        value = int(
            os.getenv("FILE_CRAWL_LEARN_BACKPRESSURE_MAX_GAP", str(default_gap))
            or str(default_gap)
        )
    except Exception:
        value = default_gap
    return max(1, min(value, 256))


def _file_crawl_learn_backpressure_check_seconds() -> float:
    try:
        value = float(os.getenv("FILE_CRAWL_LEARN_BACKPRESSURE_CHECK_SEC", "0.5") or "0.5")
    except Exception:
        value = 0.5
    return max(0.1, min(value, 10.0))


def _file_crawl_pipeline_concurrency_default() -> int:
    try:
        value = int(getattr(settings, "FILE_CRAWL_PIPELINE_CONCURRENCY", 2) or 2)
    except Exception:
        value = 2
    return max(1, min(value, 64))


def _file_crawl_learn_concurrency_default() -> int:
    try:
        value = int(getattr(settings, "FILE_CRAWL_LEARN_CONCURRENCY", 3) or 3)
    except Exception:
        value = 3
    return max(1, min(value, 32))
def _file_pipeline_worker_config() -> Dict[str, int]:
    try:
        collection_workers = int(os.getenv("FILE_CRAWL_COLLECTION_WORKERS", "1") or "1")
    except Exception:
        collection_workers = 1
    collection_workers = max(1, min(collection_workers, 1))
    try:
        study_workers = int(os.getenv("FILE_CRAWL_STUDY_WORKERS", str(_file_crawl_learn_concurrency_default())) or str(_file_crawl_learn_concurrency_default()))
    except Exception:
        study_workers = 1
    study_workers = max(1, min(study_workers, 4))
    download_topology = file_crawl_download_topology()
    return {
        "scan_workers": 1,
        "collection_workers": collection_workers,
        "download_workers": download_topology["total_workers"],
        "http_download_workers": download_topology["normal_workers"],
        "playwright_download_workers": download_topology["playwright_workers"],
        "download_max_concurrent": download_topology["max_concurrent"],
        "study_workers": study_workers,
    }


_file_learn_list_insert_semaphore: Optional[asyncio.Semaphore] = None
_file_learn_list_insert_semaphore_size: Optional[int] = None


def _file_learn_list_insert_concurrency_default() -> int:
    """Keep LEARN_LIST insert concurrency aligned with the four save workers."""
    try:
        value = int(os.getenv("FILE_LEARN_LIST_INSERT_CONCURRENCY", "4") or "4")
    except Exception:
        value = 4
    return max(1, min(value, 4))


def _get_file_learn_list_insert_semaphore() -> asyncio.Semaphore:
    global _file_learn_list_insert_semaphore
    global _file_learn_list_insert_semaphore_size

    size = _file_learn_list_insert_concurrency_default()
    if _file_learn_list_insert_semaphore is None or _file_learn_list_insert_semaphore_size != size:
        _file_learn_list_insert_semaphore = asyncio.Semaphore(size)
        _file_learn_list_insert_semaphore_size = size
    return _file_learn_list_insert_semaphore


def _file_learn_list_ensure_timeout_sec() -> float:
    try:
        value = float(os.getenv("FILE_LEARN_LIST_ENSURE_TIMEOUT_SEC", "300") or "300")
    except Exception:
        value = 300.0
    return max(30.0, min(value, 900.0))


def _file_learn_list_db_operation_timeout_sec() -> float:
    try:
        value = float(os.getenv("FILE_LEARN_LIST_DB_OPERATION_TIMEOUT_SEC", "45") or "45")
    except Exception:
        value = 45.0
    return max(5.0, min(value, 120.0))

def _file_crawl_learn_completion_window_default() -> int:
    default_window = max(
        _file_crawl_pipeline_concurrency_default(),
        _file_crawl_learn_concurrency_default() * 2,
    )
    try:
        value = int(
            os.getenv(
                "FILE_CRAWL_LEARN_COMPLETION_WINDOW",
                str(default_window),
            )
            or str(default_window)
        )
    except Exception:
        value = default_window
    return max(1, min(value, 128))


def _file_pipeline_collection_queue_maxsize() -> int:
    try:
        value = int(os.getenv("FILE_PIPELINE_COLLECTION_QUEUE_MAXSIZE", "500") or "500")
    except Exception:
        value = 500
    return max(30, min(value, 5000))


def _file_pipeline_collection_batch_size_default() -> int:
    try:
        value = int(getattr(settings, "FILE_PIPELINE_COLLECTION_BATCH_SIZE", 1) or 1)
    except Exception:
        value = 1
    return max(1, min(value, 256))


def _file_crawl_learn_backpressure_max_delay_seconds() -> float:
    try:
        value = float(
            os.getenv("FILE_CRAWL_LEARN_BACKPRESSURE_MAX_DELAY_SEC", "2.0") or "2.0"
        )
    except Exception:
        value = 2.0
    return max(0.1, min(value, 30.0))


def _delete_downloaded_source_after_learn_enabled() -> bool:
    return str(os.getenv("FILE_CRAWL_DELETE_DOWNLOADED_SOURCE_AFTER_LEARN", "1")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _excel_sheets_dict_to_plain_text(sheets: Any) -> str:
    """Convert Excel sheet dictionaries returned by xls_edu helpers to plain text."""
    if not isinstance(sheets, dict) or not sheets:
        return ""
    parts: List[str] = []
    for sheet_name, sheet_rows in sheets.items():
        parts.append(f"## {sheet_name}")
        if not isinstance(sheet_rows, list):
            continue
        for row in sheet_rows:
            if isinstance(row, list):
                parts.append(" | ".join(str(c).strip() for c in row))
            else:
                parts.append(str(row).strip())
        parts.append("")
    return "\n".join(parts).strip()


def _extract_excel_plain_text_sync(file_path: str) -> str:
    """Extract plain text from an Excel file using the available xls_edu helper."""
    try:
        from edu.xls_edu import extract_excel_to_plain_text as _fn

        return _fn(file_path)
    except ImportError:
        pass

    _load = None
    try:
        from edu.xls_edu import load_excel_sheets_dict as _load
    except ImportError:
        pass
    if _load is not None:
        try:
            return _excel_sheets_dict_to_plain_text(_load(file_path))
        except Exception:
            pass

    try:
        from edu.xls_edu import extract_excel_data as _async_ext
    except ImportError:
        return ""

    async def _run() -> Any:
        return await _async_ext(file_path)

    try:
        sheets = asyncio.run(_run())
    except RuntimeError:
        return ""
    except Exception:
        return ""
    return _excel_sheets_dict_to_plain_text(sheets)


def log_calls(func):
    """Wrap a function and emit file pipeline call logs when enabled."""
    import functools
    import inspect

    if inspect.iscoroutinefunction(func):
        @functools.wraps(func)
        async def _async(*args, **kwargs):
            if _file_pipeline_call_log_enabled():
                try:
                    self = args[0] if args else None
                    jid = getattr(self, "job_id", None) if self else None
                    logger.debug("[file_crawl][board][file] call %s | job_id=%s", func.__name__, jid)
                except Exception:
                    pass
            return await func(*args, **kwargs)

        return _async

    @functools.wraps(func)
    def _sync(*args, **kwargs):
        if _file_pipeline_call_log_enabled():
            try:
                self = args[0] if args else None
                jid = getattr(self, "job_id", None) if self else None
                logger.debug("[file_crawl][board][file] call %s | job_id=%s", func.__name__, jid)
            except Exception:
                pass
        return func(*args, **kwargs)

    return _sync


@log_calls
def _board_project_root() -> str:
    """Return the project root for this backend module."""
    return str(Path(__file__).resolve().parent.parent.parent)


class BoardContentFilePipelineMixin:
    """File crawling pipeline mixin for BoardContentWorkflow."""

    def _record_file_simhash_gate_result(
        self,
        *,
        decision: str,
        reason: str,
        simhash: str = "",
        file_name: str = "",
        file_url: str = "",
        source_url: str = "",
        matched_learn_list_id: Any = None,
        matched_status: Any = None,
    ) -> None:
        """Keep a bounded, runtime-only trace for the file SimHash gate."""
        try:
            records = getattr(self, "_file_simhash_gate_results", None)
            if not isinstance(records, dict):
                records = {}
                self._file_simhash_gate_results = records
            file_key = self._build_stats_counter_key(url=file_url) or str(file_url or "").strip()
            if not file_key:
                return
            records.pop(file_key, None)
            records[file_key] = {
                "decision": str(decision or "").strip(),
                "reason": str(reason or "").strip(),
                "simhash": str(simhash or "").strip(),
                "file_name": str(file_name or "").strip(),
                "file_url": str(file_url or "").strip(),
                "source_url": str(source_url or "").strip(),
                "matched_learn_list_id": matched_learn_list_id,
                "matched_status": str(matched_status or "").strip(),
                "recorded_at": datetime.now().isoformat(timespec="seconds"),
            }
            while len(records) > 100:
                records.pop(next(iter(records)), None)
        except Exception:
            logger.debug("[FileSimHash][gate_result_record_failed]", exc_info=True)

    job_id: str
    chat_bot_id: str
    db_name: str
    start_date: Any
    end_date: Any
    use_global_pool: bool
    stop_event: asyncio.Event
    stats: Dict[str, Any]
    progress_callback: Any
    sync_after_download: bool
    access_url: Any
    server_domain: Any
    _file_job_queues: Any
    _file_worker_manager: Any
    _file_worker_manager_cleanup_pending: Any
    _file_worker_task: Optional[asyncio.Task]
    _file_progress_task: Optional[asyncio.Task]
    _file_progress_tasks: Set[asyncio.Task]
    _file_queue_watchdog_task: Optional[asyncio.Task]
    _file_pipeline_lock: asyncio.Lock
    _stats_lock: asyncio.Lock

    def _get_file_pipeline_learn_semaphore(self) -> asyncio.Semaphore:
        sem = getattr(self, "_file_pipeline_learn_semaphore", None)
        if sem is None:
            try:
                _plc = int(
                    os.getenv(
                        "FILE_CRAWL_LEARN_CONCURRENCY",
                        str(_file_crawl_learn_concurrency_default()),
                    )
                    or str(_file_crawl_learn_concurrency_default())
                )
            except Exception:
                _plc = _file_crawl_learn_concurrency_default()
            _plc = max(1, min(_plc, 32))
            sem = asyncio.Semaphore(_plc)
            self._file_pipeline_learn_semaphore = sem
        return sem

    def _file_progress_worker_count(self) -> int:
        try:
            workers = int(os.getenv("FILE_CRAWL_SAVE_WORKERS", "3") or "3")
        except Exception:
            workers = 2
        return max(1, min(workers, 4))

    def _file_progress_dispatch_worker_count(self) -> int:
        try:
            workers = int(os.getenv("FILE_CRAWL_PROGRESS_DISPATCH_WORKERS", "2") or "2")
        except Exception:
            workers = 2
        return max(1, min(workers, 4))

    def _file_local_finalize_worker_count(self) -> int:
        try:
            workers = int(os.getenv("FILE_CRAWL_LOCAL_FINALIZE_WORKERS", "2") or "2")
        except Exception:
            workers = 2
        return max(1, min(workers, 8))

    def _trace_file_pipeline_transition(
        self,
        *,
        stage: str,
        url: str = "",
        post_url: str = "",
        file_name: str = "",
        detail: str = "",
    ) -> None:
        """Keep a small per-job stage ledger for selected-but-unsaved diagnosis."""
        key = str(url or "").strip() or f"unknown:{time.monotonic_ns()}"
        now = time.monotonic()
        ledger = getattr(self, "_file_pipeline_trace_ledger", None)
        if not isinstance(ledger, dict):
            ledger = {}
            self._file_pipeline_trace_ledger = ledger
        previous = ledger.get(key) if isinstance(ledger.get(key), dict) else {}
        ledger[key] = {
            "stage": stage,
            "updated_at": now,
            "first_seen_at": float(previous.get("first_seen_at") or now),
            "url": str(url or previous.get("url") or "")[:220],
            "post_url": str(post_url or previous.get("post_url") or "")[:220],
            "file": str(file_name or previous.get("file") or "")[:160],
            "detail": str(detail or "")[:180],
        }
        while len(ledger) > 80:
            oldest_key = next(iter(ledger), None)
            if oldest_key is None:
                break
            ledger.pop(oldest_key, None)
        logger.info(
            "[FilePipelineTrace][전환]\n"
            "작업ID=%s\n"
            "이전단계=%s\n"
            "현재단계=%s\n"
            "파일URL=%s\n"
            "게시물URL=%s\n"
            "파일명=%s\n"
            "상세=%s",
            getattr(self, "job_id", None),
            previous.get("stage") or "-",
            stage,
            str(url or "")[:220],
            str(post_url or "")[:220],
            str(file_name or "")[:160],
            str(detail or "-")[:180],
        )

    def _file_pipeline_trace_summary(self) -> Dict[str, Any]:
        ledger = getattr(self, "_file_pipeline_trace_ledger", None)
        if not isinstance(ledger, dict):
            return {"stages": {}, "samples": []}
        now = time.monotonic()
        stages: Dict[str, int] = {}
        samples: List[Dict[str, Any]] = []
        for value in list(ledger.values()):
            if not isinstance(value, dict):
                continue
            stage = str(value.get("stage") or "unknown")
            stages[stage] = stages.get(stage, 0) + 1
            if len(samples) < 8:
                samples.append({
                    "stage": stage,
                    "elapsed_sec": round(max(0.0, now - float(value.get("updated_at") or now)), 1),
                    "url": value.get("url") or "",
                    "file": value.get("file") or "",
                    "detail": value.get("detail") or "",
                })
        return {"stages": stages, "samples": samples}

    def _log_file_pipeline_worker_done(self, task: asyncio.Task, stage: str) -> None:
        """Keep worker exits visible so the watchdog is not the first clue."""
        try:
            task_name = task.get_name()
            if task.cancelled():
                logger.info(
                    "[FileCrawlTrace][worker_stopped] job_id=%s stage=%s worker=%s reason=cancelled",
                    getattr(self, "job_id", None),
                    stage,
                    task_name,
                )
                return

            error = task.exception()
            if error is None:
                stopped_by_request = bool(getattr(self, "stop_event", None) and self.stop_event.is_set())
                log = logger.info if stopped_by_request else logger.warning
                log(
                    "[FileCrawlTrace][worker_stopped] job_id=%s stage=%s worker=%s reason=%s",
                    getattr(self, "job_id", None),
                    stage,
                    task_name,
                    "stop_requested" if stopped_by_request else "returned",
                )
                return

            logger.error(
                "[FileCrawlTrace][worker_stopped] job_id=%s stage=%s worker=%s reason=exception err_type=%s err=%s",
                getattr(self, "job_id", None),
                stage,
                task_name,
                type(error).__name__,
                error,
                exc_info=(type(error), error, error.__traceback__),
            )
        except Exception:
            logger.exception(
                "[FileCrawlTrace][worker_exit_log_failed] job_id=%s stage=%s",
                getattr(self, "job_id", None),
                stage,
            )

    async def _ensure_file_progress_workers(self) -> None:
        save_queue = getattr(self, "_file_save_queue", None)
        if save_queue is None:
            try:
                maxsize = int(os.getenv("FILE_CRAWL_SAVE_QUEUE_MAXSIZE", "500") or "500")
            except Exception:
                maxsize = 500
            save_queue = asyncio.Queue(maxsize=max(1, min(maxsize, 5000)))
            self._file_save_queue = save_queue

        async def _ensure_tasks(attribute: str, desired: int, factory: Any, label: str) -> Set[asyncio.Task]:
            existing = getattr(self, attribute, None)
            is_initial_start = not isinstance(existing, set)
            if not isinstance(existing, set):
                existing = set()
            alive = {task for task in existing if isinstance(task, asyncio.Task) and not task.done()}
            missing = max(0, desired - len(alive))
            if missing:
                start_index = len(alive)
                new_tasks = {
                    asyncio.create_task(
                        factory(),
                        name=f"{label}-{getattr(self, 'job_id', 'unknown')}-{start_index + index + 1}",
                    )
                    for index in range(missing)
                }
                for task in new_tasks:
                    task.add_done_callback(
                        lambda completed, worker_stage=label: self._log_file_pipeline_worker_done(completed, worker_stage)
                    )
                alive.update(new_tasks)
                log = logger.info if is_initial_start else logger.warning
                event = "workers_started" if is_initial_start else "workers_replenished"
                log(
                    "[FileCrawlTrace][%s] job_id=%s stage=%s desired=%s alive_before=%s started=%s",
                    event,
                    getattr(self, "job_id", None),
                    label,
                    desired,
                    desired - missing,
                    missing,
                )
            setattr(self, attribute, alive)
            return alive

        dispatch_tasks = await _ensure_tasks(
            "_file_progress_dispatch_tasks",
            self._file_progress_dispatch_worker_count(),
            self._run_file_progress_loop,
            "file-progress-dispatch",
        )
        save_tasks = await _ensure_tasks(
            "_file_progress_tasks",
            self._file_progress_worker_count(),
            self._run_file_save_loop,
            "file-progress-save",
        )
        self._file_progress_task = next(iter(save_tasks), None)
        logger.debug(
            "[FileCrawlTrace][progress_pipeline_ready] job_id=%s dispatch_workers=%s save_workers=%s save_queue_max=%s",
            getattr(self, "job_id", None),
            len(dispatch_tasks),
            len(save_tasks),
            save_queue.maxsize,
        )

    async def _wait_for_file_save_drain(self) -> None:
        queue = getattr(self, "_file_save_queue", None)
        if queue is not None:
            await queue.join()

    def _file_learning_request_worker_count(self) -> int:
        return _file_crawl_learn_concurrency_default()

    def _file_learning_request_lane(self, item: Dict[str, Any]) -> str:
        candidate = str(
            item.get("file_path")
            or item.get("local_path")
            or item.get("file_name")
            or ""
        )
        return "pdf" if os.path.splitext(candidate)[1].lower() == ".pdf" else "normal"

    async def _ensure_file_learning_request_workers(self) -> None:
        """Run file parsing and external batch submission outside save workers."""
        normal_queue = getattr(self, "_file_learning_request_queue", None)
        if normal_queue is None:
            # Saving is the durable crawl boundary. Do not let slow parsing or an
            # external embedding submission block the save worker at queue.put().
            # The workflow finalizer still drains this queue before it reports
            # completion, but it does not wait for the external callback.
            normal_queue = asyncio.Queue()
            self._file_learning_request_queue = normal_queue
        pdf_queue = getattr(self, "_file_learning_pdf_request_queue", None)
        if pdf_queue is None:
            pdf_queue = asyncio.Queue()
            self._file_learning_pdf_request_queue = pdf_queue

        tasks = getattr(self, "_file_learning_request_tasks", None)
        if not isinstance(tasks, set):
            tasks = set()
        alive = {task for task in tasks if isinstance(task, asyncio.Task) and not task.done()}
        workers = self._file_learning_request_worker_count()
        pdf_workers = 1 if workers > 1 else 0
        normal_workers = max(1, workers - pdf_workers)
        alive_by_lane = {
            "normal": {
                task for task in alive
                if str(task.get_name()).startswith("file-learning-request-")
            },
            "pdf": {
                task for task in alive
                if str(task.get_name()).startswith("file-learning-pdf-request-")
            },
        }
        started_workers = False
        for lane, queue, desired in (
            ("normal", normal_queue, normal_workers),
            ("pdf", pdf_queue, pdf_workers),
        ):
            missing = max(0, desired - len(alive_by_lane[lane]))
            for index in range(missing):
                worker_id = len(alive_by_lane[lane]) + index + 1
                prefix = "file-learning-pdf-request" if lane == "pdf" else "file-learning-request"
                task = asyncio.create_task(
                    self._run_file_learning_request_worker(worker_id, queue=queue, lane=lane),
                    name=f"{prefix}-{getattr(self, 'job_id', 'unknown')}-{worker_id}",
                )
                alive.add(task)
                alive_by_lane[lane].add(task)
                started_workers = True
        if started_workers:
            logger.info(
                "[FileLearnRequest][workers_ready] job_id=%s normal_workers=%s pdf_workers=%s normal_queue_max=%s pdf_queue_max=%s",
                getattr(self, "job_id", None),
                len(alive_by_lane["normal"]),
                len(alive_by_lane["pdf"]),
                normal_queue.maxsize,
                pdf_queue.maxsize,
            )
        self._file_learning_request_tasks = alive

    async def _enqueue_file_learning_request(self, item: Dict[str, Any]) -> None:
        await self._ensure_file_learning_request_workers()
        lane = self._file_learning_request_lane(item)
        queue_name = "_file_learning_pdf_request_queue" if lane == "pdf" else "_file_learning_request_queue"
        queue = getattr(self, queue_name, None)
        if queue is None:
            raise RuntimeError("file_learning_request_queue_unavailable")
        queue.put_nowait(item)
        logger.info(
            "[FileLearnRequest][queued] job_id=%s lane=%s learn_list_id=%s file_url=%s pending=%s queue_max=unbounded",
            getattr(self, "job_id", None),
            lane,
            item.get("pre_learn_list_id"),
            str(item.get("url") or "")[:220],
            int(getattr(queue, "_unfinished_tasks", 0) or 0),
        )

    async def _run_file_learning_request_worker(
        self,
        worker_id: int,
        *,
        queue: asyncio.Queue,
        lane: str,
    ) -> None:
        while True:
            item = await queue.get()
            try:
                if not isinstance(item, dict) or self.stop_event.is_set():
                    continue
                started = time.perf_counter()
                sem = self._get_file_pipeline_learn_semaphore()
                logger.info(
                    "[FileLearnTrace][item_taken] job_id=%s worker=%s lane=%s learn_list_id=%s queue_after_take=%s file_url=%s",
                    getattr(self, "job_id", None),
                    worker_id,
                    lane,
                    item.get("pre_learn_list_id"),
                    queue.qsize(),
                    str(item.get("url") or "")[:220],
                )
                slot_wait_started = time.perf_counter()
                async with sem:
                    logger.info(
                        "[FileLearnTrace][slot_acquired] job_id=%s worker=%s lane=%s learn_list_id=%s wait_ms=%s file_url=%s",
                        getattr(self, "job_id", None),
                        worker_id,
                        lane,
                        item.get("pre_learn_list_id"),
                        int((time.perf_counter() - slot_wait_started) * 1000),
                        str(item.get("url") or "")[:220],
                    )
                    await self._file_run_saved_file_learn_after_save(**item)
                save_key = str(item.get("save_key") or "").strip()
                outcome = self._file_study_outcome_for_key(save_key) if save_key else ""
                if save_key and outcome == "failed":
                    retry_scheduled = self._schedule_file_learning_retry(dict(item), attempt=0)
                    if not retry_scheduled:
                        await self._discard_file_crawl_local_download(
                            str(item.get("file_path") or item.get("local_path") or ""),
                            reason="learning_retry_exhausted",
                        )
                logger.info(
                    "[FileLearnRequest][submitted] job_id=%s worker=%s lane=%s learn_list_id=%s file_url=%s elapsed_ms=%s outcome=%s",
                    getattr(self, "job_id", None),
                    worker_id,
                    lane,
                    item.get("pre_learn_list_id"),
                    str(item.get("url") or "")[:220],
                    int((time.perf_counter() - started) * 1000),
                    outcome or "pending_callback",
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(
                    "[FileLearnRequest][worker_failed] job_id=%s worker=%s lane=%s learn_list_id=%s file_url=%s err=%s",
                    getattr(self, "job_id", None),
                    worker_id,
                    lane,
                    item.get("pre_learn_list_id") if isinstance(item, dict) else None,
                    str(item.get("url") or "")[:220] if isinstance(item, dict) else "",
                    exc,
                )
            finally:
                queue.task_done()

    def file_learning_request_dispatch_complete(self) -> bool:
        for queue_name in ("_file_learning_request_queue", "_file_learning_pdf_request_queue"):
            queue = getattr(self, queue_name, None)
            if queue is not None and int(getattr(queue, "_unfinished_tasks", 0) or 0) > 0:
                return False
        retries = getattr(self, "_file_parallel_learn_tasks", set()) or set()
        return not any(isinstance(task, asyncio.Task) and not task.done() for task in retries)

    def file_learning_callbacks_detached(self) -> bool:
        """A file crawl completes after external learning requests are accepted."""
        return True

    async def _wait_for_file_learning_request_drain(self) -> None:
        for queue_name in ("_file_learning_request_queue", "_file_learning_pdf_request_queue"):
            queue = getattr(self, queue_name, None)
            if queue is not None:
                await queue.join()
        retries = [
            task
            for task in (getattr(self, "_file_parallel_learn_tasks", set()) or set())
            if isinstance(task, asyncio.Task) and not task.done()
        ]
        if retries:
            await asyncio.gather(*retries, return_exceptions=True)

    async def _stop_file_learning_request_workers(self, *, graceful: bool) -> None:
        if graceful:
            await self._wait_for_file_learning_request_drain()
        tasks = list(getattr(self, "_file_learning_request_tasks", set()) or set())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._file_learning_request_tasks = set()
        self._file_learning_request_queue = None
        self._file_learning_pdf_request_queue = None

    async def _stop_file_progress_workers(self) -> None:
        tasks = set(getattr(self, "_file_progress_dispatch_tasks", set()) or set())
        tasks.update(set(getattr(self, "_file_progress_tasks", set()) or set()))
        fallback = getattr(self, "_file_progress_task", None)
        if isinstance(fallback, asyncio.Task):
            tasks.add(fallback)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._file_progress_dispatch_tasks = set()
        self._file_progress_tasks = set()
        self._file_progress_task = None
        self._file_save_queue = None
    def _file_parallel_learn_enabled(self) -> bool:
        if not getattr(self, "is_attachment_file_crawl_workflow", False):
            return False
        try:
            return str(os.getenv("FILE_CRAWL_PARALLEL_LEARN", "1")).strip().lower() in (
                "1",
                "true",
                "yes",
                "on",
            )
        except Exception:
            return True

    def _file_learn_retry_enabled(self) -> bool:
        return str(os.getenv("FILE_LEARN_FAILED_RETRY_RAM_QUEUE", "1") or "1").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )

    def _file_learn_retry_delay_sec(self) -> float:
        try:
            value = float(os.getenv("FILE_LEARN_FAILED_RETRY_DELAY_SEC", "30") or "30")
        except Exception:
            value = 30.0
        return max(1.0, min(value, 1800.0))

    def _file_learn_retry_max_attempts(self) -> int:
        try:
            value = int(os.getenv("FILE_LEARN_FAILED_RETRY_MAX_ATTEMPTS", "1") or "1")
        except Exception:
            value = 1
        return max(0, min(value, 5))

    def _file_learn_retry_inflight(self) -> set[str]:
        inflight = getattr(self, "_file_learn_retry_inflight_keys", None)
        if not isinstance(inflight, set):
            inflight = set()
            self._file_learn_retry_inflight_keys = inflight
        return inflight

    def _file_study_outcome_for_key(self, save_key: str) -> str:
        outcomes = getattr(self, "_counted_study_outcomes", None)
        if not isinstance(outcomes, dict):
            return ""
        return str(outcomes.get(save_key) or "").strip().lower()

    def _file_learn_retryable(self) -> bool:
        reason = str((self.stats or {}).get("file_study_fail_reason") or "").strip().lower()
        if not reason:
            return True
        return not any(token in reason for token in ("rrn", "personal", "file_text_extract_empty", "duplicate", "skipped"))

    def _schedule_file_learning_retry(self, kwargs: Dict[str, Any], *, attempt: int) -> bool:
        if not self._file_learn_retry_enabled():
            return False
        max_attempts = self._file_learn_retry_max_attempts()
        if attempt >= max_attempts or not self._file_learn_retryable():
            return False
        save_key = str(kwargs.get("save_key") or "").strip()
        if not save_key:
            return False
        inflight = self._file_learn_retry_inflight()
        if save_key in inflight:
            return False
        inflight.add(save_key)

        async def _retry_runner() -> None:
            try:
                delay = self._file_learn_retry_delay_sec()
                logger.debug(
                    "[LearnRetry] scheduled | job_id=%s attempt=%s/%s delay=%.1fs url=%s file=%s",
                    getattr(self, "job_id", None),
                    attempt + 1,
                    max_attempts,
                    delay,
                    str(kwargs.get("url") or "")[:180],
                    os.path.basename(str(kwargs.get("file_path") or "")),
                )
                await asyncio.sleep(delay)
                retry_kwargs = dict(kwargs)
                retry_kwargs["pre_extracted_text"] = None
                await self._file_run_saved_file_learn_after_save(**retry_kwargs)
                outcome = self._file_study_outcome_for_key(save_key)
                if outcome == "failed":
                    retry_scheduled = self._schedule_file_learning_retry(
                        retry_kwargs,
                        attempt=attempt + 1,
                    )
                    if not retry_scheduled:
                        await self._discard_file_crawl_local_download(
                            str(retry_kwargs.get("file_path") or retry_kwargs.get("local_path") or ""),
                            reason="learning_retry_exhausted",
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug(
                    "[LearnRetry] retry failed | job_id=%s save_key=%s err=%s",
                    getattr(self, "job_id", None),
                    save_key[:180],
                    exc,
                )
            finally:
                inflight.discard(save_key)

        task = asyncio.create_task(_retry_runner())
        self._register_file_parallel_learn_task(task)
        return True

    async def _reconcile_file_study_counts_from_learn_list(self) -> None:
        """Keep file study counters tied to actual pipeline outcomes.

        ``LEARN_LIST.status = 'Y'`` is set when an embedding batch is accepted,
        before its callback confirms completion. It must not be used to upgrade a
        failed or pending file to a successful learning result.
        """
        logger.debug(
            "[board][file] learn count reconcile skipped | job_id=%s reason=status_y_is_submit_state",
            getattr(self, "job_id", None),
        )
    async def _wait_for_file_learn_backpressure(self, *, file_url: str = "") -> None:
        if not getattr(self, "is_attachment_file_crawl_workflow", False):
            return
        if bool(getattr(self, "file_pipeline_skip_learning", False)):
            return
        if not _file_crawl_learn_backpressure_enabled():
            return

        allowed_gap = _file_crawl_learn_backpressure_max_gap()
        base_delay = _file_crawl_learn_backpressure_check_seconds()
        max_delay = _file_crawl_learn_backpressure_max_delay_seconds()

        if self.stop_event.is_set():
            return

        async with self._stats_lock:
            save_count = int((self.stats or {}).get("save_count", 0) or 0)
            study_done_count = int(
                (self.stats or {}).get(
                    "file_study_done_count",
                    (self.stats or {}).get("study_done_count", 0),
                )
                or 0
            )
            study_success_count = int(
                (self.stats or {}).get(
                    "file_study_success_count",
                    (self.stats or {}).get("study_success_count", 0),
                )
                or 0
            )

        gap = max(0, save_count - study_done_count)
        if gap < allowed_gap:
            return

        pause_logged = False
        while not self.stop_event.is_set():
            async with self._stats_lock:
                save_count = int((self.stats or {}).get("save_count", 0) or 0)
                study_done_count = int(
                    (self.stats or {}).get(
                        "file_study_done_count",
                        (self.stats or {}).get("study_done_count", 0),
                    )
                    or 0
                )
                study_success_count = int(
                    (self.stats or {}).get(
                        "file_study_success_count",
                        (self.stats or {}).get("study_success_count", 0),
                    )
                    or 0
                )

            gap = max(0, save_count - study_done_count)
            if gap < allowed_gap:
                if pause_logged:
                    logger.info(
                        "[Phase][file][learn_backpressure.resume] job_id=%s save=%s study_done=%s "
                        "study_success=%s gap=%s allowed=%s url=%s",
                        getattr(self, "job_id", None),
                        save_count,
                        study_done_count,
                        study_success_count,
                        gap,
                        allowed_gap,
                        (file_url or "")[:180],
                    )
                return

            delay_sec = min(max_delay, max(base_delay, base_delay * float((gap - allowed_gap) + 1)))
            if not pause_logged:
                logger.info(
                    "[Phase][file][learn_backpressure.pause] job_id=%s save=%s study_done=%s "
                    "study_success=%s gap=%s allowed=%s delay_sec=%.2f url=%s",
                    getattr(self, "job_id", None),
                    save_count,
                    study_done_count,
                    study_success_count,
                    gap,
                    allowed_gap,
                    delay_sec,
                    (file_url or "")[:180],
                )
                pause_logged = True
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay_sec)
            except asyncio.TimeoutError:
                continue

    @log_calls
    async def _ensure_file_pipeline(self) -> None:
        """Ensure the file pipeline worker manager or global worker pool is ready."""
        from core.crawler.queues import create_job_queues
        from core.crawler.manager import WorkerManager
        from core.crawler.global_pool import get_global_worker_pool

        try:
            use_global_pool = bool(getattr(self, "use_global_pool", False)) or str(
                os.getenv("GLOBAL_WORKER_POOL", "0")
            ).strip() == "1"
        except Exception:
            use_global_pool = bool(getattr(self, "use_global_pool", False))
        try:
            self.use_global_pool = bool(use_global_pool)
        except Exception:
            pass
        # Attachment file crawling uses an isolated WorkerManager per job.
        # This prevents another job from consuming this job's download workers.
        if getattr(self, "is_attachment_file_crawl_workflow", False):
            use_global_pool = False
            self.use_global_pool = False
        if self._file_job_queues:
            await self._ensure_file_local_finalize_workers()
        if self._file_job_queues and (self._file_worker_manager or use_global_pool):
            if not use_global_pool:
                return
            try:
                pool = get_global_worker_pool()
                await pool.register_job_and_ensure_started(
                    self.job_id or "unknown", self._file_job_queues
                )
                health = pool.worker_health_snapshot(job_id=str(getattr(self, "job_id", "") or ""))
                if int(health.get("download_alive", 0) or 0) < 1:
                    raise RuntimeError(f"global download worker unavailable: {health}")
                return
            except Exception as exc:
                logger.warning(
                    "[파일크롤링추적][큐소비복구] 글로벌 워커 재등록 실패, 로컬 워커로 전환 | 작업ID=%s 오류=%r",
                    self.job_id,
                    exc,
                )
                use_global_pool = False
                self.use_global_pool = False
        async with self._file_pipeline_lock:
            if self._file_job_queues:
                await self._ensure_file_local_finalize_workers()
            if self._file_job_queues and (self._file_worker_manager or use_global_pool):
                if not use_global_pool:
                    return
                try:
                    pool = get_global_worker_pool()
                    await pool.register_job_and_ensure_started(
                        self.job_id or "unknown", self._file_job_queues
                    )
                    health = pool.worker_health_snapshot(job_id=str(getattr(self, "job_id", "") or ""))
                    if int(health.get("download_alive", 0) or 0) < 1:
                        raise RuntimeError(f"global download worker unavailable: {health}")
                    return
                except Exception as exc:
                    logger.warning(
                        "[파일크롤링추적][큐소비복구] 글로벌 워커 재등록 실패, 로컬 워커로 전환 | 작업ID=%s 오류=%r",
                        self.job_id,
                        exc,
                    )
                    use_global_pool = False
                    self.use_global_pool = False
            if not self._file_job_queues:
                queue_key = self.job_id or f"file-{uuid.uuid4().hex}"
                coll_bs: Optional[int] = None
                if getattr(self, "is_attachment_file_crawl_workflow", False):
                    try:
                        coll_bs = int(
                            os.getenv(
                                "FILE_PIPELINE_COLLECTION_BATCH_SIZE",
                                str(_file_pipeline_collection_batch_size_default()),
                            )
                            or str(_file_pipeline_collection_batch_size_default())
                        )
                    except Exception:
                        coll_bs = _file_pipeline_collection_batch_size_default()
                    coll_bs = max(1, min(coll_bs, 256))
                self._file_job_queues = create_job_queues(
                    queue_key,
                    collection_batch_size=coll_bs,
                    collection_queue_maxsize=(
                        _file_pipeline_collection_queue_maxsize()
                        if getattr(self, "is_attachment_file_crawl_workflow", False)
                        else None
                    ),
                )
            try:
                _plc = int(
                    os.getenv(
                        "FILE_CRAWL_LEARN_CONCURRENCY",
                        str(_file_crawl_learn_concurrency_default()),
                    )
                    or str(_file_crawl_learn_concurrency_default())
                )
            except Exception:
                _plc = _file_crawl_learn_concurrency_default()
            _plc = max(1, min(_plc, 32))
            self._file_pipeline_learn_semaphore = asyncio.Semaphore(_plc)
            if use_global_pool:
                try:
                    pool = get_global_worker_pool()
                    await pool.register_job_and_ensure_started(
                        self.job_id or "unknown", self._file_job_queues
                    )
                    health = pool.worker_health_snapshot(job_id=str(getattr(self, "job_id", "") or ""))
                    if int(health.get("download_alive", 0) or 0) < 1:
                        raise RuntimeError(f"global download worker unavailable: {health}")
                    self._file_worker_manager = None
                    self._file_worker_task = None
                    logger.debug("[file_crawl][board][file] GlobalWorkerPool enabled | job_id=%s", self.job_id)
                except Exception as exc:
                    logger.debug(
                        "[file_crawl][board][file] GlobalWorkerPool enable failed; fallback to local WorkerManager | err=%s",
                        exc,
                    )
                    use_global_pool = False
                    self.use_global_pool = False
            if not use_global_pool:
                self._file_worker_manager_cleanup_pending = None
                worker_config = _file_pipeline_worker_config()
                logger.info(
                "[FileCrawlTrace][worker_config] job_id=%s download_workers=%s http_download_workers=%s playwright_download_workers=%s download_max_concurrent=%s save_workers=%s study_workers=%s",
                self.job_id,
                worker_config.get("download_workers"),
                worker_config.get("http_download_workers"),
                worker_config.get("playwright_download_workers"),
                worker_config.get("download_max_concurrent"),
                    self._file_progress_worker_count(),
                    worker_config.get("study_workers"),
                )
                self._file_worker_manager = WorkerManager(
                    on_collection_batch=None,
                    chat_bot_id=self.chat_bot_id,
                    db_name=self.db_name,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    max_depth=0,
                    job_queues=self._file_job_queues,
                    worker_config_override=worker_config,
                    defer_browser_launch=True,
                )
                self._file_worker_task = asyncio.create_task(self._file_worker_manager.start())

                def _log_file_worker_task_done(task: asyncio.Task) -> None:
                    try:
                        exc = task.exception()
                    except asyncio.CancelledError:
                        return
                    except Exception as callback_exc:
                        logger.error(
                            "[file_crawl][workers] worker task status check failed | job_id=%s err=%s",
                            self.job_id,
                            callback_exc,
                        )
                        return
                    if exc is not None:
                        logger.error(
                            "[file_crawl][workers] worker task stopped with error | job_id=%s err=%r",
                            self.job_id,
                            exc,
                            exc_info=(type(exc), exc, getattr(exc, "__traceback__", None)),
                        )

                self._file_worker_task.add_done_callback(_log_file_worker_task_done)
                try:
                    # WorkerManager.start() returns once all worker tasks are
                    # created.  Do not enqueue attachments before that point:
                    # otherwise a Playwright/startup failure presents as an
                    # unexplained per-job download_total=0 state.
                    await asyncio.wait_for(
                        asyncio.shield(self._file_worker_task),
                        timeout=45.0,
                    )
                except Exception as exc:
                    logger.error(
                        "[FileCrawlTrace][download_workers_start_failed] job_id=%s err_type=%s err=%s",
                        self.job_id,
                        type(exc).__name__,
                        exc,
                    )
                    raise
                worker_health = self._file_pipeline_worker_health_snapshot()
                if int(worker_health.get("download_alive", 0) or 0) < 1:
                    raise RuntimeError(f"file download workers unavailable after start: {worker_health}")
                logger.info(
                    "[FileCrawlTrace][download_workers_ready] job_id=%s workers=%s",
                    self.job_id,
                    worker_health,
                )
                logger.debug(
                    "[file_crawl][workers] local pipeline worker redistribution | job_id=%s workers=%s",
                    self.job_id,
                    worker_config,
                )
                logger.debug("[file_crawl][board][file] file pipeline started | job_id=%s", self.job_id)
            await self._ensure_file_local_finalize_workers()
            await self._ensure_file_progress_workers()
            await self._ensure_file_learning_request_workers()
            if not getattr(self, "_file_queue_watchdog_task", None) or self._file_queue_watchdog_task.done():
                self._file_queue_watchdog_task = asyncio.create_task(
                    self._run_file_queue_watchdog(),
                    name=f"file-queue-watchdog-{self.job_id or 'unknown'}",
                )

            if not getattr(self, "_file_pipeline_review_logged", False):
                try:
                    domain_concurrency = int(
                        getattr(settings, "DOWNLOAD_WORKERS", 4) or 4
                    )
                except Exception:
                    domain_concurrency = 4
                domain_concurrency = max(1, min(domain_concurrency, 8))
                try:
                    retry_enabled = str(
                        os.getenv("DOWNLOAD_FAILED_RETRY_RAM_QUEUE", "1") or "1"
                    ).strip().lower() in {"1", "true", "yes", "on"}
                    retry_workers = int(os.getenv("DOWNLOAD_FAILED_RETRY_WORKERS", "1") or "1")
                except Exception:
                    retry_enabled = True
                    retry_workers = 1
                retry_workers = max(1, min(retry_workers, 4))
                finalize_tasks = getattr(self, "_file_local_finalize_tasks", set()) or set()
                finalize_workers = sum(
                    1 for task in finalize_tasks if isinstance(task, asyncio.Task) and not task.done()
                )
                logger.info(
                    "[FileCrawlReview][startup]\n"
                    "job_id=%s\n"
                    "db=%s\n"
                    "pool=job_specific\n"
                    "domain_semaphore=job_id scoped, limit=%s, release=download_item finally, direct_wait_timeout=%ss\n"
                    "retry_queue=enabled:%s workers:%s, source_queue_task_done=finally, "
                    "review=retry path bypasses active tracking and item hard timeout\n"
                    "worker_blocking=HTTP async, document metadata executor, web sync async subprocess, "
                    "download slot may wait for non-deferred postprocess\n"
                    "local_postprocess=deferred:%s finalize_workers:%s, "
                    "review=finalize queue must be drained before terminal completion\n"
                    "completion_counter=file_saved event -> progress loop -> LEARN_LIST/save stats; "
                    "missing terminal counter can be a real finalize-drain race, not only a log omission",
                    getattr(self, "job_id", None),
                    getattr(self, "db_name", None),
                    domain_concurrency,
                    os.getenv("DOWNLOAD_COMPONENT_DIRECT_DOMAIN_WAIT_SEC", "5"),
                    retry_enabled,
                    retry_workers,
                    str(os.getenv("FILE_CRAWL_DEFER_LOCAL_POSTPROCESS", "1") or "1").strip(),
                    finalize_workers,
                )
                self._file_pipeline_review_logged = True
    def _file_candidate_validation_semaphore(self) -> asyncio.Semaphore:
        sem = getattr(self, "_file_candidate_response_validation_semaphore", None)
        if sem is None:
            sem = asyncio.Semaphore(_file_candidate_response_validation_concurrency())
            setattr(self, "_file_candidate_response_validation_semaphore", sem)
        return sem

    @log_calls
    async def _validate_weak_file_candidate_response(
        self,
        *,
        file_url: str,
        file_name: str,
        attach: Dict[str, Any],
        post_url: str,
    ) -> Tuple[bool, str]:
        timeout_sec = _file_candidate_response_validation_timeout_sec()
        headers = {
            "Accept": "*/*",
            "Referer": post_url or file_url,
            "User-Agent": os.getenv("FILE_CRAWL_USER_AGENT", "Mozilla/5.0"),
        }
        async with self._file_candidate_validation_semaphore():
            try:
                import aiohttp

                session = None
                if hasattr(self, "_get_http_session"):
                    session = await self._get_http_session(timeout_sec=timeout_sec)
                close_session = False
                if session is None:
                    timeout = aiohttp.ClientTimeout(total=timeout_sec)
                    session = aiohttp.ClientSession(timeout=timeout, headers=headers)
                    close_session = True
                try:
                    try:
                        async with session.request(
                            "HEAD",
                            file_url,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=timeout_sec),
                            allow_redirects=True,
                        ) as resp:
                            status = int(resp.status or 0)
                            if 200 <= status < 400 and _looks_like_file_response(resp.headers, file_url, file_name):
                                return True, "head_file_response"
                            if status not in {401, 403, 405, 500, 501} and status >= 400:
                                return False, f"head_status_{status}"
                    except Exception:
                        pass

                    get_headers = dict(headers)
                    get_headers["Range"] = "bytes=0-2047"
                    async with session.get(
                        file_url,
                        headers=get_headers,
                        timeout=aiohttp.ClientTimeout(total=timeout_sec),
                        allow_redirects=True,
                    ) as resp:
                        status = int(resp.status or 0)
                        if not (200 <= status < 400):
                            return False, f"get_status_{status}"
                        try:
                            chunk = await resp.content.read(512)
                        except Exception:
                            chunk = b""
                        if _looks_like_html_response_body(chunk):
                            return False, "get_html_response"
                        if _looks_like_file_response(resp.headers, file_url, file_name):
                            return True, "get_file_response"
                        if chunk:
                            return True, "get_non_html_response"
                        return False, "get_empty_response"
                finally:
                    if close_session:
                        try:
                            await session.close()
                        except Exception:
                            pass
            except Exception as exc:
                return False, f"validation_exception:{type(exc).__name__}"

    async def _enqueue_file_downloads(
        self,
        *,
        post_url: str,
        board_url: str,
        reg_date: Optional[str],
        attachments: List[Dict[str, Any]],
        author: Optional[str],
        department: Optional[str],
        author_kind: Optional[str],
        author_raw: Optional[str],
        department_raw: Optional[str],
        contact_phone: Optional[str],
        view_count: Optional[int],
        sync_after_download: Optional[bool] = None,
        detail_cates: Optional[Tuple[Optional[str], Optional[str]]] = None,
        post_title: Optional[str] = None,
    ) -> None:
        # File crawl owns attachment enqueueing and preserves board-derived metadata.
        if not getattr(self, "is_attachment_file_crawl_workflow", False):
            return

        if not attachments:
            _log_file_url_status(
                stage="candidate_select",
                status="skipped",
                process_url=post_url,
                post_url=post_url,
                selected="no",
                saved="no",
                learn="not_started",
                reason="no_attachments",
                count=0,
                job_id=getattr(self, "job_id", ""),
                db_name=getattr(self, "db_name", ""),
            )
            logger.debug(
                "[FileProbeDebug][enqueue.skip_no_attachments] job_id=%s post=%s",
                getattr(self, "job_id", None),
                (post_url or "")[:220],
            )
            return


        await self._ensure_file_pipeline()
        if not self._file_job_queues:
            return

        effective_job_id = str(self.job_id or "").strip() or "unknown"
        if _content_author_debug_enabled():
            logger.debug(
                "[ContentAuthorDebug][enqueue.input] job_id=%s post=%s attachment_count=%s author=%r content_author=%r department=%r kind=%r raw=%r",
                effective_job_id,
                (post_url or "")[:220],
                len(attachments or []),
                _content_author_debug_value(author),
                _content_author_debug_value(author),
                _content_author_debug_value(department),
                _content_author_debug_value(author_kind),
                _content_author_debug_value(author_raw),
            )
        collection_batch_queue = self._file_job_queues.collection_batch_queue
        large_collection_batch_queue = getattr(
            self._file_job_queues,
            "large_collection_batch_queue",
            collection_batch_queue,
        )

        def _collection_queue_stored_count(queue=None) -> int:
            """Return the number of file items currently stored in the batch queue."""
            queue = queue or collection_batch_queue
            buffered = len(getattr(queue, "buffer", []) or [])
            raw_queue = getattr(queue, "queue", None)
            try:
                queued = sum(
                    len(batch) if isinstance(batch, (list, tuple)) else 1
                    for batch in list(getattr(raw_queue, "_queue", []) or [])
                )
            except Exception:
                try:
                    queued = int(raw_queue.qsize()) if raw_queue is not None else 0
                except Exception:
                    queued = 0
            return queued + buffered

        def _collection_queue_trace_snapshot(queue=None) -> Dict[str, Any]:
            queue = queue or collection_batch_queue
            raw_queue = getattr(queue, "queue", None)
            try:
                queue_size = int(raw_queue.qsize()) if raw_queue is not None else 0
            except Exception:
                queue_size = 0
            try:
                queue_maxsize = int(getattr(raw_queue, "maxsize", 0) or 0)
            except Exception:
                queue_maxsize = 0
            try:
                unfinished = int(getattr(raw_queue, "_unfinished_tasks", 0) or 0)
            except Exception:
                unfinished = 0
            return {
                "items": _collection_queue_stored_count(queue),
                "batches": queue_size,
                "max_batches": queue_maxsize,
                "buffer": len(getattr(queue, "buffer", []) or []),
                "unfinished": unfinished,
                "full": bool(queue_maxsize and queue_size >= queue_maxsize),
            }

        def _collection_queue_worker_health() -> Dict[str, Any]:
            return self._file_pipeline_worker_health_snapshot()

        async def _put_collection_queue_with_trace(
            item: Dict[str, Any],
            *,
            file_url: str,
            file_name: str,
        ) -> None:
            lane = "normal"
            target_queue = collection_batch_queue
            item["download_lane"] = lane
            try:
                declared_size_bytes = max(0, int(item.get("declared_file_size_bytes") or 0))
            except (TypeError, ValueError):
                declared_size_bytes = 0
            metadata_size_detail = (
                f"메타데이터 파일용량={declared_size_bytes} bytes"
                if declared_size_bytes > 0
                else "메타데이터 파일용량 미확인"
            )
            before = _collection_queue_trace_snapshot(target_queue)
            started_at = time.monotonic()
            if before["full"]:
                logger.warning(
                    "[파일크롤링추적][큐적재대기시작] 작업ID=%s 게시물URL=%s 파일URL=%s 파일명=%s "
                    "사유=queue_full 큐=%s 워커=%s",
                    effective_job_id,
                    (post_url or "")[:220],
                    (file_url or "")[:220],
                    (file_name or "")[:160],
                    before,
                    _collection_queue_worker_health(),
                )
            await target_queue.put(item)
            self._trace_file_pipeline_transition(
                stage="download_queue_enqueued",
                url=file_url,
                post_url=post_url,
                file_name=file_name,
                detail=f"lane={lane} {metadata_size_detail}",
            )
            wait_sec = time.monotonic() - started_at
            if before["full"] or wait_sec >= 3.0:
                after = _collection_queue_trace_snapshot(target_queue)
                reason = "queue_full" if before["full"] else "slow_queue_put"
                logger.warning(
                    "[파일크롤링추적][큐적재대기완료] 작업ID=%s 게시물URL=%s 파일URL=%s 파일명=%s "
                    "대기초=%.2f 사유=%s 이전큐=%s 이후큐=%s 워커=%s",
                    effective_job_id,
                    (post_url or "")[:220],
                    (file_url or "")[:220],
                    (file_name or "")[:160],
                    wait_sec,
                    reason,
                    before,
                    after,
                    _collection_queue_worker_health(),
                )
        def _current_saved_count() -> int:
            """Return the current job-local LEARN_LIST save success count."""
            try:
                return max(0, int(self.stats.get("save_count", 0) or 0))
            except Exception:
                return 0

        enqueued = 0
        raw_attachment_count = len(attachments or [])
        enqueue_missing_reason_counts: Dict[str, int] = {}
        enqueue_missing_samples: List[Dict[str, str]] = []

        def _track_enqueue_missing(
            reason: str,
            *,
            count: int = 1,
            file_url: str = "",
            file_name: str = "",
        ) -> None:
            normalized_reason = str(reason or "unknown")
            normalized_count = max(0, int(count or 0))
            if normalized_count <= 0:
                return
            enqueue_missing_reason_counts[normalized_reason] = int(
                enqueue_missing_reason_counts.get(normalized_reason, 0) or 0
            ) + normalized_count
            if len(enqueue_missing_samples) < 10:
                enqueue_missing_samples.append(
                    {
                        "reason": normalized_reason,
                        "url": str(file_url or "")[:300],
                        "name": str(file_name or "")[:160],
                    }
                )
            logger.warning(
                "[FileCrawlTrace][enqueue_missing] job_id=%s post_url=%s file_url=%s reason=%s count=%s file=%s",
                effective_job_id,
                (post_url or "")[:300],
                (file_url or "")[:300],
                normalized_reason,
                normalized_count,
                (file_name or "")[:160],
            )
            logger.warning(
                "[파일크롤링추적][큐누락] 파일URL=%s\n작업ID=%s 사유=%s 건수=%s 파일명=%s",
                (file_url or post_url)[:300],
                effective_job_id,
                normalized_reason,
                normalized_count,
                (file_name or "")[:160],
            )

        async def _record_enqueue_totals(
            *,
            raw_count: int,
            candidate_count: int = 0,
            enqueued_count: int = 0,
            duplicate_count: int = 0,
            reason: str = "",
        ) -> None:
            try:
                dropped_count = max(0, int(raw_count or 0) - int(enqueued_count or 0))
                async with self._stats_lock:
                    self.stats["file_attachment_enqueue_input_total"] = int(
                        self.stats.get("file_attachment_enqueue_input_total", 0) or 0
                    ) + max(0, int(raw_count or 0))
                    self.stats["file_attachment_enqueue_candidate_total"] = int(
                        self.stats.get("file_attachment_enqueue_candidate_total", 0) or 0
                    ) + max(0, int(candidate_count or 0))
                    self.stats["file_attachment_enqueue_enqueued_total"] = int(
                        self.stats.get("file_attachment_enqueue_enqueued_total", 0) or 0
                    ) + max(0, int(enqueued_count or 0))
                    self.stats["file_attachment_enqueued_count"] = int(
                        self.stats.get("file_attachment_enqueued_count", 0) or 0
                    ) + max(0, int(enqueued_count or 0))
                    self.stats["file_attachment_enqueue_duplicate_drop_total"] = int(
                        self.stats.get("file_attachment_enqueue_duplicate_drop_total", 0) or 0
                    ) + max(0, int(duplicate_count or 0))
                    self.stats["file_attachment_enqueue_dropped_total"] = int(
                        self.stats.get("file_attachment_enqueue_dropped_total", 0) or 0
                    ) + dropped_count
                    if reason and dropped_count > 0:
                        key = f"file_attachment_enqueue_drop_{reason}_total"
                        self.stats[key] = int(self.stats.get(key, 0) or 0) + dropped_count
                    self._bump_stats_revision_locked()
            except Exception:
                pass

        try:
            async with self._stats_lock:
                self.stats["file_attachment_raw_count"] = max(
                    int(self.stats.get("file_attachment_raw_count", 0) or 0),
                    len(attachments or []),
                )
        except Exception:
            pass
        try:
            _file_multi_attach_debug(
                "[FileMultiAttachDebug][enqueue.start] job_id=%s post=%s raw=%s mode=%s sample=%s",
                effective_job_id,
                (post_url or "")[:220],
                len(attachments or []),
                "multi" if len(attachments or []) >= 2 else "single",
                [
                    {
                        "name": (a.get("name") or a.get("title") or a.get("text") or "")[:120],
                        "href": (a.get("href") or "")[:220],
                    }
                    for a in (attachments or [])[:5]
                    if isinstance(a, dict)
                ],
            )
        except Exception:
            pass
        _file_dashboard_download_debug(
            "enqueue.start job_id=%s post=%s raw=%s sample=%s",
            effective_job_id,
            (post_url or "")[:220],
            len(attachments or []),
            [
                {
                    "name": (a.get("name") or a.get("title") or a.get("text") or "")[:120],
                    "href": (a.get("href") or "")[:220],
                }
                for a in (attachments or [])[:10]
                if isinstance(a, dict)
            ],
        )

        candidates: List[Tuple[Any, str, str, str, List[str]]] = []
        skip_reason_counts: Dict[str, int] = {}
        validation_started_at = time.monotonic()
        validation_count = 0
        validation_max_count = _file_candidate_response_validation_max_per_detail()
        validation_budget_sec = _file_candidate_response_validation_budget_sec()

        def _count_candidate_skip(reason: str) -> None:
            key = str(reason or "unknown")
            skip_reason_counts[key] = int(skip_reason_counts.get(key, 0) or 0) + 1

        for attach in attachments:
            file_url, file_name, file_url_key = self._normalize_file_url(attach, post_url)
            dedup_keys = self._file_url_dedup_keys(file_url, file_url_key)
            try:
                self._record_job_result_stage(
                    url=file_url_key or file_url,
                    stage="file_attachment",
                    status="discovered",
                    source_url=post_url,
                    file_url=file_url,
                    file_name=file_name,
                )
            except Exception:
                pass
            if not file_url or any(k in self._seen_file_urls for k in dedup_keys):
                try:
                    self._record_job_result_stage(
                        url=file_url_key or file_url,
                        stage="file_attachment",
                        status="skipped",
                        reason="empty_url" if not file_url else "seen_url",
                        source_url=post_url,
                        file_url=file_url,
                        file_name=file_name,
                    )
                except Exception:
                    pass
                _log_file_url_status(
                    stage="candidate_select",
                    status="skipped",
                    process_url=file_url or post_url,
                    post_url=post_url,
                    file_url=file_url,
                    selected="no",
                    saved="no",
                    learn="not_started",
                    reason="empty_url" if not file_url else "seen_url",
                    name=file_name,
                    job_id=effective_job_id,
                    db_name=getattr(self, "db_name", ""),
                )
                _file_dashboard_download_debug(
                    "candidate.skip reason=%s job_id=%s post=%s name=%s url=%s key=%s",
                    "empty_url" if not file_url else "seen_url",
                    effective_job_id,
                    (post_url or "")[:160],
                    (file_name or "")[:120],
                    (file_url or "")[:220],
                    (file_url_key or "")[:220],
                )
                logger.debug(
                    "[FileProbeDebug][enqueue.candidate_skip] reason=%s job_id=%s post=%s name=%s url=%s key=%s",
                    "empty_url" if not file_url else "seen_url",
                    effective_job_id,
                    (post_url or "")[:160],
                    (file_name or "")[:120],
                    (file_url or "")[:220],
                    (file_url_key or "")[:220],
                )
                _count_candidate_skip("empty_url" if not file_url else "seen_url")
                continue
            # A successful LEARN_LIST save makes the attachment reusable for this
            # process-wide TTL window, including attachment-file crawl jobs.
            # File crawls intentionally refresh persisted learning data on each
            # run.  Per-job URL de-duplication above remains the guard; the
            # process-wide completed cache must not suppress a later refresh.
            skip_by_completed_cache = (
                not bool(getattr(self, "is_attachment_file_crawl_workflow", False))
                and completed_url_cached(file_url_key or file_url, stage="save")
            )
            if skip_by_completed_cache:
                try:
                    self._record_job_result_stage(
                        url=file_url_key or file_url,
                        stage="file_attachment",
                        status="skipped",
                        reason="completed_cache",
                        source_url=post_url,
                        file_url=file_url,
                        file_name=file_name,
                    )
                except Exception:
                    pass
                _log_file_url_status(
                    stage="candidate_select",
                    status="skipped",
                    process_url=file_url or post_url,
                    post_url=post_url,
                    file_url=file_url,
                    selected="no",
                    saved="no",
                    learn="skipped",
                    reason="completed_cache",
                    name=file_name,
                    job_id=effective_job_id,
                    db_name=getattr(self, "db_name", ""),
                )
                _file_dashboard_download_debug(
                    "candidate.skip reason=completed_cache job_id=%s post=%s name=%s url=%s key=%s ttl_sec=%s",
                    effective_job_id,
                    (post_url or "")[:160],
                    (file_name or "")[:120],
                    (file_url or "")[:220],
                    (file_url_key or "")[:220],
                    os.getenv("FILE_CRAWL_COMPLETED_URL_CACHE_TTL_SEC", "300"),
                )
                logger.debug(
                    "[FileProbeDebug][enqueue.candidate_skip] reason=completed_cache job_id=%s post=%s name=%s url=%s key=%s ttl_sec=%s",
                    effective_job_id,
                    (post_url or "")[:160],
                    (file_name or "")[:120],
                    (file_url or "")[:220],
                    (file_url_key or "")[:220],
                    os.getenv("FILE_CRAWL_COMPLETED_URL_CACHE_TTL_SEC", "300"),
                )
                _count_candidate_skip("completed_cache")
                continue
            candidate_score = 1.0
            try:
                candidate_score = float(attach.get("candidate_score") or 1.0)
            except Exception:
                candidate_score = 1.0
            clear_document_candidate = _file_candidate_is_clear_document(file_url, file_name, attach)
            should_validate_response = (
                _file_candidate_response_validation_enabled()
                and bool(attach.get("needs_response_validation"))
                and candidate_score <= _file_candidate_response_validation_threshold()
                and not clear_document_candidate
            )
            if should_validate_response:
                validation_elapsed = time.monotonic() - validation_started_at
                if validation_max_count <= 0 or validation_count >= validation_max_count or validation_elapsed >= validation_budget_sec:
                    attach["response_validation_reason"] = "validation_budget_exceeded"
                else:
                    validation_count += 1
                    validation_ok, validation_reason = await self._validate_weak_file_candidate_response(
                        file_url=file_url,
                        file_name=file_name,
                        attach=attach,
                        post_url=post_url,
                    )
                    attach["response_validation_reason"] = validation_reason
                    if not validation_ok:
                        clear_document = clear_document_candidate
                        if clear_document:
                            attach["response_validation_reason"] = f"validation_failed_but_clear_document:{validation_reason}"
                            attach["response_validation_passed_open"] = True
                            logger.debug(
                                "[FileProbeDebug][enqueue.candidate_validation_pass] reason=clear_document_validation_failed job_id=%s post=%s name=%s url=%s key=%s candidate_score=%s validation_reason=%s",
                                effective_job_id,
                                (post_url or "")[:160],
                                (file_name or "")[:120],
                                (file_url or "")[:220],
                                (file_url_key or "")[:220],
                                candidate_score,
                                validation_reason,
                            )
                        else:
                            try:
                                self._record_job_result_stage(
                                    url=file_url_key or file_url,
                                    stage="file_attachment",
                                    status="skipped",
                                    reason="response_validation_failed",
                                    source_url=post_url,
                                    file_url=file_url,
                                    file_name=file_name,
                                    detail=validation_reason,
                                )
                            except Exception:
                                pass
                            _log_file_url_status(
                                stage="candidate_select",
                                status="skipped",
                                process_url=file_url or post_url,
                                post_url=post_url,
                                file_url=file_url,
                                selected="no",
                                saved="no",
                                learn="not_started",
                                reason="response_validation_failed",
                                error=validation_reason,
                                name=file_name,
                                job_id=effective_job_id,
                                db_name=getattr(self, "db_name", ""),
                            )
                            logger.debug(
                                "[FileProbeDebug][enqueue.candidate_skip] reason=response_validation_failed job_id=%s post=%s name=%s url=%s key=%s candidate_score=%s validation_reason=%s",
                                effective_job_id,
                                (post_url or "")[:160],
                                (file_name or "")[:120],
                                (file_url or "")[:220],
                                (file_url_key or "")[:220],
                                candidate_score,
                                validation_reason,
                            )
                            _count_candidate_skip("response_validation_failed")
                            continue
                    else:
                        attach["response_validated"] = True

            scan_filter_trusted = _file_candidate_scan_filter_trusted(attach)
            if should_skip_attachment_at_scan(file_url, file_name or "") and not bool(
                attach.get("response_validated") or attach.get("response_validation_passed_open") or scan_filter_trusted
            ):
                try:
                    self._record_job_result_stage(
                        url=file_url_key or file_url,
                        stage="file_attachment",
                        status="skipped",
                        reason="scan_filter_non_doc",
                        source_url=post_url,
                        file_url=file_url,
                        file_name=file_name,
                    )
                except Exception:
                    pass
                _log_file_url_status(
                    stage="candidate_select",
                    status="skipped",
                    process_url=file_url or post_url,
                    post_url=post_url,
                    file_url=file_url,
                    selected="no",
                    saved="no",
                    learn="not_started",
                    reason="scan_filter_non_doc",
                    name=file_name,
                    job_id=effective_job_id,
                    db_name=getattr(self, "db_name", ""),
                )
                _file_dashboard_download_debug(
                    "candidate.skip reason=scan_filter_non_doc job_id=%s post=%s name=%s url=%s key=%s download_doc_only=%s",
                    effective_job_id,
                    (post_url or "")[:160],
                    (file_name or "")[:120],
                    (file_url or "")[:220],
                    (file_url_key or "")[:220],
                    os.getenv("DOWNLOAD_DOC_ONLY", "1"),
                )
                logger.debug(
                    "[FileProbeDebug][enqueue.candidate_skip] reason=scan_filter_non_doc job_id=%s post=%s name=%s url=%s key=%s download_doc_only=%s",
                    effective_job_id,
                    (post_url or "")[:160],
                    (file_name or "")[:120],
                    (file_url or "")[:220],
                    (file_url_key or "")[:220],
                    os.getenv("DOWNLOAD_DOC_ONLY", "1"),
                )
                _count_candidate_skip("scan_filter_non_doc")
                continue
            candidates.append((attach, file_url, file_name, file_url_key, dedup_keys))

        if skip_reason_counts:
            try:
                async with self._stats_lock:
                    reason_counts = self.stats.setdefault(
                        "file_attachment_enqueue_candidate_skip_reason_counts", {}
                    )
                    if not isinstance(reason_counts, dict):
                        reason_counts = {}
                        self.stats["file_attachment_enqueue_candidate_skip_reason_counts"] = reason_counts
                    for reason, count in skip_reason_counts.items():
                        reason_counts[reason] = int(reason_counts.get(reason, 0) or 0) + int(count or 0)
            except Exception:
                logger.debug("[파일크롤링추적][후보제외] 사유 통계 기록 실패", exc_info=True)

        if not candidates:
            raw_count = len(attachments or [])
            normal_skip_only = raw_count > 0 and sum(skip_reason_counts.values()) >= raw_count
            skip_summary = ",".join(
                f"{reason}:{count}" for reason, count in sorted(skip_reason_counts.items())
            ) or "none"
            _file_dashboard_download_debug(
                "enqueue.no_candidates job_id=%s post=%s raw_count=%s seen_total=%s skip_reasons=%s normal_skip_only=%s",
                effective_job_id,
                (post_url or "")[:220],
                raw_count,
                len(getattr(self, "_seen_file_urls", set()) or []),
                skip_summary,
                normal_skip_only,
            )
            log_no_candidates = logger.debug if normal_skip_only else logger.debug
            _log_file_url_status(
                stage="candidate_select",
                status="skipped",
                process_url=post_url,
                post_url=post_url,
                selected="no",
                saved="no",
                learn="not_started",
                reason="no_candidates:" + skip_summary,
                count=raw_count,
                job_id=effective_job_id,
                db_name=getattr(self, "db_name", ""),
            )
            log_no_candidates(
                "[FileProbeDebug][enqueue.no_candidates] job_id=%s post=%s raw_count=%s seen_total=%s skip_reasons=%s normal_skip_only=%s",
                effective_job_id,
                (post_url or "")[:220],
                raw_count,
                len(getattr(self, "_seen_file_urls", set()) or []),
                skip_summary,
                normal_skip_only,
            )
            skip_reason_labels = {
                "empty_url": "파일 URL 없음",
                "seen_url": "동일 파일 URL이 이미 큐 등록됨",
                "completed_cache": "최근 저장 완료 캐시",
                "response_validation_failed": "파일 응답 검증 실패",
                "scan_filter_non_doc": "비문서 확장자 필터",
            }
            skip_reason_text = ", ".join(
                f"{skip_reason_labels.get(reason, reason)}({count})"
                for reason, count in sorted(skip_reason_counts.items())
                if int(count or 0) > 0
            ) or "원인 미확인"
            logger.info(
                "[파일크롤링추적][후보제외] 게시물URL=%s\n작업ID=%s 후보입력=%s 후보통과=0 사유=%s",
                (post_url or "")[:300],
                effective_job_id,
                raw_count,
                skip_reason_text,
            )
            await _record_enqueue_totals(
                raw_count=raw_attachment_count,
                candidate_count=0,
                enqueued_count=0,
                reason="no_candidates",
            )
            return enqueued
        try:
            async with self._stats_lock:
                self.stats["file_attachment_pre_duplicate_candidate_count"] = max(
                    int(self.stats.get("file_attachment_pre_duplicate_candidate_count", 0) or 0),
                    len(candidates or []),
                )
        except Exception:
            pass
        try:
            _file_multi_attach_debug(
                "[FileMultiAttachDebug][candidate.summary] job_id=%s post=%s raw=%s candidates=%s skipped=%s mode=%s candidate_sample=%s",
                effective_job_id,
                (post_url or "")[:220],
                len(attachments or []),
                len(candidates or []),
                max(0, len(attachments or []) - len(candidates or [])),
                "multi" if len(attachments or []) >= 2 else "single",
                [
                    {
                        "name": (name or "")[:120],
                        "url": (url or "")[:220],
                        "key": (key or "")[:220],
                    }
                    for _, url, name, key, _ in (candidates or [])[:10]
                ],
            )
        except Exception:
            pass
        _file_dashboard_download_debug(
            "candidate.summary job_id=%s post=%s raw=%s candidates=%s skipped=%s sample=%s",
            effective_job_id,
            (post_url or "")[:220],
            len(attachments or []),
            len(candidates or []),
            max(0, len(attachments or []) - len(candidates or [])),
            [
                {
                    "name": (name or "")[:120],
                    "url": (url or "")[:220],
                    "key": (key or "")[:220],
                }
                for _, url, name, key, _ in (candidates or [])[:10]
            ],
        )


        try:
            async with self._stats_lock:
                self.stats["file_attachment_candidate_count"] = max(
                    int(self.stats.get("file_attachment_candidate_count", 0) or 0),
                    len(candidates or []),
                )
        except Exception:
            pass

        await self._record_scan_entries([c[0] for c in candidates], post_url, effective_job_id)

        # Load learned file identities once per job.  Candidates with an
        # explicit byte size can be excluded before they occupy a download
        # worker; unknown-size candidates are checked again after download.
        await self._load_file_pg_duplicate_fingerprints()

        dup_bundle = None
        conc = 0

        nc1, nc2 = (None, None)
        if detail_cates is not None:
            nc1, nc2 = detail_cates[0], detail_cates[1]
        if not (str(nc1 or "").strip() or str(nc2 or "").strip()):
            logger.debug(
                "[Cate][file] enqueue category left empty; url_pattern fallback disabled | job_id=%s post=%s",
                effective_job_id,
                (post_url or "")[:220],
            )

        async def _check_candidate_duplicate(candidate: Tuple[Dict[str, Any], str, str, str, List[str]]) -> bool:
            # Existing LEARN_LIST/PG rows are refreshed by the learning UPSERT.
            # Do not spend one MariaDB round-trip per attachment merely to
            # decide whether an otherwise valid file should be skipped.
            return False

        dup_results = await asyncio.gather(
            *(_check_candidate_duplicate(candidate) for candidate in candidates),
            return_exceptions=True,
        )
        dup_flags = []
        for result in dup_results:
            if isinstance(result, Exception):
                logger.warning(
                    "[Duplicate][file] pre-queue exact URL check failed open | job_id=%s post=%s err=%s",
                    effective_job_id,
                    (post_url or "")[:220],
                    result,
                )
                dup_flags.append(False)
            else:
                dup_flags.append(bool(result))
        # Keep every post-candidate duplicate path in this summary count so a
        # deliberately skipped item cannot later be reported as queue missing.
        duplicate_count = sum(1 for flag in dup_flags if flag)
        runtime_duplicate_reason_counts: Dict[str, int] = {}

        def _record_runtime_duplicate_skip(reason: str, file_url: str, file_name: str) -> None:
            nonlocal duplicate_count
            normalized_reason = str(reason or "runtime_duplicate")
            duplicate_count += 1
            runtime_duplicate_reason_counts[normalized_reason] = int(
                runtime_duplicate_reason_counts.get(normalized_reason, 0) or 0
            ) + 1
            logger.info(
                "[FileCrawlTrace][enqueue_duplicate_skip] job_id=%s post_url=%s file_url=%s reason=%s file=%s",
                effective_job_id,
                (post_url or "")[:300],
                (file_url or "")[:300],
                normalized_reason,
                (file_name or "")[:160],
            )

        try:
            selected_after_db_duplicate = sum(1 for flag in dup_flags if not flag)
            async with self._stats_lock:
                self.stats["file_attachment_candidate_count"] = max(
                    int(self.stats.get("file_attachment_candidate_count", 0) or 0),
                    selected_after_db_duplicate,
                )
        except Exception:
            pass
        try:
            _file_multi_attach_debug(
                "[FileMultiAttachDebug][dup.summary] job_id=%s post=%s candidates=%s duplicate=%s remaining=%s dup_concurrency=%s mode=%s duplicate_urls=%s",
                effective_job_id,
                (post_url or "")[:220],
                len(candidates or []),
                duplicate_count,
                max(0, len(candidates or []) - duplicate_count),
                conc,
                "multi" if len(candidates or []) >= 2 else "single",
                [
                    {
                        "name": (name or "")[:120],
                        "url": (url or "")[:220],
                    }
                    for (_, url, name, _, _), flag in zip(candidates or [], dup_flags or [])
                    if flag
                ][:10],
            )
        except Exception:
            pass
        _file_dashboard_download_debug(
            "dup.summary job_id=%s post=%s candidates=%s duplicate=%s remaining=%s",
            effective_job_id,
            (post_url or "")[:220],
            len(candidates or []),
            sum(1 for flag in dup_flags if flag),
            max(0, len(candidates or []) - sum(1 for flag in dup_flags if flag)),
        )

        _tn = "-"
        try:
            _ct0 = asyncio.current_task()
            if _ct0 is not None:
                _tn = getattr(_ct0, "get_name", lambda: "-")() or "-"
        except Exception:
            pass
        try:
            from db.mariadb_save_update import learn_list_file_dup_debug_log as _dup_log
        except Exception:
            _dup_log = None  # type: ignore[misc,assignment]

        enqueue_lock = getattr(self, "_file_enqueue_lock", None)

        for candidate_index, ((attach, file_url, file_name, file_url_key, dedup_keys), is_duplicated) in enumerate(zip(candidates, dup_flags)):
            if is_duplicated:
                try:
                    self._record_job_result_stage(
                        url=file_url_key or file_url,
                        stage="file_attachment",
                        status="skipped",
                        reason="db_duplicate",
                        source_url=post_url,
                        file_url=file_url,
                        file_name=file_name,
                    )
                except Exception:
                    pass
                _log_file_url_status(
                    stage="candidate_select",
                    status="skipped",
                    process_url=file_url or post_url,
                    post_url=post_url,
                    file_url=file_url,
                    selected="no",
                    saved="no",
                    learn="skipped",
                    reason="db_duplicate",
                    name=file_name,
                    job_id=effective_job_id,
                    db_name=getattr(self, "db_name", ""),
                )
                _file_dashboard_download_debug(
                    "enqueue.skip_duplicate job_id=%s post=%s name=%s url=%s key=%s",
                    effective_job_id,
                    (post_url or "")[:160],
                    (file_name or "")[:120],
                    (file_url or "")[:220],
                    (file_url_key or "")[:220],
                )
                if _dup_log:
                    _dup_log(
                        'duplicate skip (matched in DB) | job_id=%s task=%s file_url_key=%s post=%s',
                        effective_job_id,
                        _tn,
                        (file_url_key or "")[:220],
                        (post_url or "")[:120],
                    )
                continue

            try:
                exact_file_size = int((attach or {}).get("_exact_file_size_bytes") or 0)
            except (TypeError, ValueError):
                exact_file_size = 0
            if exact_file_size > 0 and self._is_file_pg_duplicate(file_name, exact_file_size):
                await self._backfill_file_pg_duplicate_source_url_if_missing(
                    file_name=file_name,
                    file_size=exact_file_size,
                    source_url=post_url,
                )
                _record_runtime_duplicate_skip(
                    "pg_learned_match_pre_download",
                    file_url,
                    file_name,
                )
                _count_candidate_skip("pg_learned_match_pre_download")
                self._record_job_result_stage(
                    url=file_url_key or file_url,
                    stage="file_attachment",
                    status="skipped",
                    reason="pg_learned_match_pre_download",
                    source_url=post_url,
                    file_url=file_url,
                    file_name=file_name,
                )
                _log_file_url_status(
                    stage="candidate_select",
                    status="skipped",
                    process_url=file_url or post_url,
                    post_url=post_url,
                    file_url=file_url,
                    selected="no",
                    saved="no",
                    learn="skipped",
                    reason="pg_learned_match_pre_download",
                    name=file_name,
                    job_id=effective_job_id,
                    db_name=getattr(self, "db_name", ""),
                )
                continue

            # Most detail pages expose a display filename but not an exact byte
            # size.  Probe headers only when PG already has the same filename,
            # then keep a learned file out of the download queue entirely.
            if exact_file_size <= 0 and self._file_pg_duplicate_name_exists(file_name):
                response_file_name, response_file_size = await self._probe_attachment_response_metadata_before_download(
                    file_url=file_url,
                    post_url=post_url,
                    file_name=file_name,
                )
                duplicate_name = response_file_name or file_name
                if response_file_size > 0 and self._is_file_pg_duplicate(duplicate_name, response_file_size):
                    await self._backfill_file_pg_duplicate_source_url_if_missing(
                        file_name=duplicate_name,
                        file_size=response_file_size,
                        source_url=post_url,
                    )
                    _record_runtime_duplicate_skip(
                        "pg_learned_match_pre_download_probe",
                        file_url,
                        duplicate_name,
                    )
                    _count_candidate_skip("pg_learned_match_pre_download_probe")
                    self._record_job_result_stage(
                        url=file_url_key or file_url,
                        stage="file_attachment",
                        status="skipped",
                        reason="pg_learned_match_pre_download_probe",
                        source_url=post_url,
                        file_url=file_url,
                        file_name=duplicate_name,
                    )
                    _log_file_url_status(
                        stage="candidate_select",
                        status="skipped",
                        process_url=file_url or post_url,
                        post_url=post_url,
                        file_url=file_url,
                        selected="no",
                        saved="no",
                        learn="skipped",
                        reason="pg_learned_match_pre_download_probe",
                        name=duplicate_name,
                        job_id=effective_job_id,
                        db_name=getattr(self, "db_name", ""),
                    )
                    logger.info(
                        "[FilePgDuplicate][skip] job_id=%s phase=pre_download_probe file=%s size=%s post_url=%s file_url=%s",
                        effective_job_id,
                        (duplicate_name or "")[:160],
                        response_file_size,
                        (post_url or "")[:220],
                        (file_url or "")[:220],
                    )
                    continue

            claim_url = file_url_key or file_url
            claim_acquired = await try_acquire_cross_job_claim(
                "file_download",
                str(getattr(self, "db_name", "") or ""),
                claim_url,
                effective_job_id,
            )
            if not claim_acquired:
                claim_owner, claim_ttl_sec = await get_cross_job_claim_info(
                    "file_download",
                    str(getattr(self, "db_name", "") or ""),
                    claim_url,
                )
                claim_reason = (
                    "duplicate_same_job_claim"
                    if claim_owner and claim_owner == str(effective_job_id or "")
                    else "duplicate_other_job_claim"
                )
                _record_runtime_duplicate_skip(
                    claim_reason,
                    file_url,
                    file_name,
                )
                _count_candidate_skip(claim_reason)
                logger.info(
                    "[FileCrawlTrace][enqueue_claim_conflict] job_id=%s claim_owner=%s claim_ttl_sec=%s post_url=%s file_url=%s file=%s",
                    effective_job_id,
                    claim_owner or "unknown",
                    claim_ttl_sec,
                    (post_url or "")[:300],
                    (file_url or "")[:300],
                    (file_name or "")[:160],
                )
                self._record_job_result_stage(
                    url=claim_url,
                    stage="file_attachment",
                    status="skipped",
                    reason=claim_reason,
                    source_url=post_url,
                    file_url=file_url,
                    file_name=file_name,
                )
                _log_file_url_status(
                    stage="candidate_select",
                    status="skipped",
                    process_url=file_url or post_url,
                    post_url=post_url,
                    file_url=file_url,
                    selected="no",
                    saved="no",
                    learn="skipped",
                    reason=claim_reason,
                    name=file_name,
                    job_id=effective_job_id,
                    db_name=getattr(self, "db_name", ""),
                )
                continue

            if _dup_log:
                _dup_log(
                    'candidate selected | job_id=%s task=%s file_url=%s post=%s',
                    effective_job_id,
                    _tn,
                    (file_url or "")[:220],
                    (post_url or "")[:120],
                )

            _log_file_url_status(
                stage="candidate_select",
                status="selected",
                process_url=file_url or post_url,
                post_url=post_url,
                file_url=file_url,
                selected="yes",
                saved="pending",
                learn="skipped" if bool(getattr(self, "file_pipeline_skip_learning", False)) else "pending",
                name=file_name,
                job_id=effective_job_id,
                db_name=getattr(self, "db_name", ""),
            )
            file_meta = self._build_file_meta(
                file_url, file_name, post_url, board_url, reg_date, author,
                department, author_kind, author_raw, department_raw,
                contact_phone, view_count, sync_after_download, effective_job_id,
                detail_cates=detail_cates,
                post_title=post_title,
                source_download_name=attach.get("download_name"),
            )
            if file_meta:
                file_meta["declared_file_size_bytes"] = int(attach.get("_declared_file_size_bytes") or 0)
                file_meta["download_lane"] = "normal"
                # A valid document is selected when its download work enters
                # the queue. Storage and learning have separate counters.
                file_meta["_file_selection_counted"] = False
                file_meta["_file_selection_pending_reason"] = "awaiting_download_queue"
            if not file_meta:
                _track_enqueue_missing(
                    "file_meta_empty",
                    file_url=file_url,
                    file_name=file_name,
                )
                _log_file_url_status(
                    stage="download_enqueue",
                    status="skipped",
                    process_url=file_url or post_url,
                    post_url=post_url,
                    file_url=file_url,
                    selected="yes",
                    saved="no",
                    learn="not_started",
                    reason="file_meta_empty",
                    name=file_name,
                    job_id=effective_job_id,
                    db_name=getattr(self, "db_name", ""),
                )
                continue
            try:
                request_method = str(attach.get("method") or "GET").strip().upper()
                request_params = attach.get("params") or {}
                request_raw = attach.get("raw") or ""
                needs_response_validation = bool(attach.get("needs_response_validation"))
                response_validated = bool(attach.get("response_validated"))
                response_validation_reason = str(attach.get("response_validation_reason") or "")
                file_meta["request_method"] = request_method
                file_meta["request_params"] = request_params
                file_meta["needs_response_validation"] = needs_response_validation
                file_meta["response_validated"] = response_validated
                file_meta["response_validation_reason"] = response_validation_reason
                original_meta = file_meta.get("original_meta")
                if isinstance(original_meta, dict):
                    original_meta["request_method"] = request_method
                    original_meta["request_params"] = request_params
                    original_meta["attachment_action_raw"] = request_raw
                    original_meta["needs_response_validation"] = needs_response_validation
                    original_meta["response_validated"] = response_validated
                    original_meta["response_validation_reason"] = response_validation_reason
                    original_meta["candidate_score"] = attach.get("candidate_score")
                    original_meta["candidate_reason"] = attach.get("candidate_reason")
            except Exception:
                pass

            if enqueue_lock is not None:
                enqueue_lock_wait_started = time.monotonic()
                async with enqueue_lock:
                    enqueue_lock_wait_sec = time.monotonic() - enqueue_lock_wait_started
                    if enqueue_lock_wait_sec >= 3.0:
                        logger.warning(
                            "[파일크롤링추적][큐적재락대기완료] 작업ID=%s 게시물URL=%s 파일URL=%s 파일명=%s "
                            "대기초=%.2f 큐=%s",
                            effective_job_id,
                            (post_url or "")[:220],
                            (file_url or "")[:220],
                            (file_name or "")[:160],
                            enqueue_lock_wait_sec,
                            _collection_queue_trace_snapshot(),
                        )
                    await self._wait_for_file_learn_backpressure(file_url=file_url)
                    if self.stop_event.is_set():
                        _track_enqueue_missing(
                            "stop_requested",
                            count=len(candidates) - candidate_index,
                            file_url=file_url,
                            file_name=file_name,
                        )
                        break
                    if any(k in self._seen_file_urls for k in dedup_keys):
                        _record_runtime_duplicate_skip(
                            "seen_before_enqueue",
                            file_url,
                            file_name,
                        )
                        _count_candidate_skip("seen_before_enqueue")
                        _file_dashboard_download_debug(
                            "enqueue.skip reason=seen_before_enqueue job_id=%s post=%s name=%s url=%s keys=%s",
                            effective_job_id, (post_url or "")[:160], (file_name or "")[:120],
                            (file_url or "")[:220], dedup_keys[:5],
                        )
                        continue
                    self._seen_file_urls.update(dedup_keys)
                    _file_dashboard_download_debug(
                        "queue.put job_id=%s post=%s name=%s url=%s key=%s queue_size_before=%s",
                        effective_job_id, (post_url or "")[:160], (file_name or "")[:120],
                        (file_url or "")[:220], (file_url_key or "")[:220],
                        _collection_queue_stored_count(),
                    )
                    await _put_collection_queue_with_trace(file_meta, file_url=file_url, file_name=file_name)
                    _log_file_url_status(
                        stage="download_enqueue",
                        status="queued",
                        process_url=file_url or post_url,
                        post_url=post_url,
                        file_url=file_url,
                        selected="pending",
                        saved="pending",
                        learn="skipped" if bool(getattr(self, "file_pipeline_skip_learning", False)) else "pending",
                        name=file_name,
                        job_id=effective_job_id,
                        db_name=getattr(self, "db_name", ""),
                    )
                    enqueued += 1
            else:
                await self._wait_for_file_learn_backpressure(file_url=file_url)
                if self.stop_event.is_set():
                    _track_enqueue_missing(
                        "stop_requested",
                        count=len(candidates) - candidate_index,
                        file_url=file_url,
                        file_name=file_name,
                    )
                    break
                if any(k in self._seen_file_urls for k in dedup_keys):
                    _record_runtime_duplicate_skip(
                        "seen_before_enqueue",
                        file_url,
                        file_name,
                    )
                    _count_candidate_skip("seen_before_enqueue")
                    _file_dashboard_download_debug(
                        "enqueue.skip reason=seen_before_enqueue job_id=%s post=%s name=%s url=%s keys=%s",
                        effective_job_id, (post_url or "")[:160], (file_name or "")[:120],
                        (file_url or "")[:220], dedup_keys[:5],
                    )
                    continue
                self._seen_file_urls.update(dedup_keys)
                _file_dashboard_download_debug(
                    "queue.put job_id=%s post=%s name=%s url=%s key=%s queue_size_before=%s",
                    effective_job_id, (post_url or "")[:160], (file_name or "")[:120],
                    (file_url or "")[:220], (file_url_key or "")[:220],
                    _collection_queue_stored_count(),
                )
                await _put_collection_queue_with_trace(file_meta, file_url=file_url, file_name=file_name)
                _log_file_url_status(
                    stage="download_enqueue",
                    status="queued",
                    process_url=file_url or post_url,
                    post_url=post_url,
                    file_url=file_url,
                    selected="pending",
                    saved="pending",
                    learn="skipped" if bool(getattr(self, "file_pipeline_skip_learning", False)) else "pending",
                    name=file_name,
                    job_id=effective_job_id,
                    db_name=getattr(self, "db_name", ""),
                )
                enqueued += 1

        known_missing_count = sum(enqueue_missing_reason_counts.values())
        unclassified_missing_count = max(
            0,
            len(candidates or []) - enqueued - duplicate_count - known_missing_count,
        )
        if unclassified_missing_count:
            _track_enqueue_missing("unknown", count=unclassified_missing_count)
        if enqueue_missing_reason_counts:
            try:
                async with self._stats_lock:
                    reason_counts = self.stats.setdefault("file_attachment_enqueue_missing_reason_counts", {})
                    if not isinstance(reason_counts, dict):
                        reason_counts = {}
                        self.stats["file_attachment_enqueue_missing_reason_counts"] = reason_counts
                    for reason, count in enqueue_missing_reason_counts.items():
                        reason_counts[reason] = int(reason_counts.get(reason, 0) or 0) + int(count or 0)
                    samples = self.stats.setdefault("file_attachment_enqueue_missing_samples", [])
                    if isinstance(samples, list) and len(samples) < 10:
                        samples.extend(enqueue_missing_samples[: max(0, 10 - len(samples))])
            except Exception:
                logger.debug("[파일크롤링추적][큐누락] 사유 통계 기록 실패", exc_info=True)

        if runtime_duplicate_reason_counts:
            try:
                async with self._stats_lock:
                    reason_counts = self.stats.setdefault(
                        "file_attachment_enqueue_runtime_duplicate_reason_counts", {}
                    )
                    if not isinstance(reason_counts, dict):
                        reason_counts = {}
                        self.stats["file_attachment_enqueue_runtime_duplicate_reason_counts"] = reason_counts
                    for reason, count in runtime_duplicate_reason_counts.items():
                        reason_counts[reason] = int(reason_counts.get(reason, 0) or 0) + int(count or 0)
            except Exception:
                logger.debug("[FileCrawlTrace][enqueue_duplicate_skip] reason stats update failed", exc_info=True)

        if enqueued:
            try:
                await collection_batch_queue.flush()
                await large_collection_batch_queue.flush()
            except Exception:
                pass
        if enqueued:
            logger.info(
                "[파일크롤링추적][큐전달완료] 게시물URL=%s\n작업ID=%s 큐등록=%s \033[1;33m큐대기\033[0m=%s 현재저장=%s 버퍼잔량=%s 다음단계=다운로드시작",
                (post_url or "")[:220],
                effective_job_id,
                enqueued,
                _collection_queue_stored_count(),
                _current_saved_count(),
                len(getattr(collection_batch_queue, "buffer", []) or []),
            )
        try:
            _file_multi_attach_debug(
                "[FileMultiAttachDebug][enqueue.summary] job_id=%s post=%s raw=%s candidates=%s enqueued=%s mode=%s stop_event=%s queue_size=%s buffer_size=%s seen_total=%s",
                effective_job_id,
                (post_url or "")[:220],
                len(attachments or []),
                len(candidates or []),
                enqueued,
                "multi" if len(attachments or []) >= 2 else "single",
                bool(self.stop_event.is_set()),
                _collection_queue_stored_count(),
                len(getattr(collection_batch_queue, "buffer", []) or []),
                len(getattr(self, "_seen_file_urls", set()) or []),
            )
        except Exception:
            pass
        _file_dashboard_download_debug(
            "enqueue.summary job_id=%s post=%s raw=%s candidates=%s enqueued=%s stop_event=%s queue_size=%s buffer_size=%s seen_total=%s",
            effective_job_id,
            (post_url or "")[:220],
            len(attachments or []),
            len(candidates or []),
            enqueued,
            bool(self.stop_event.is_set()),
            _collection_queue_stored_count(),
            len(getattr(collection_batch_queue, "buffer", []) or []),
            len(getattr(self, "_seen_file_urls", set()) or []),
        )
        await _record_enqueue_totals(
            raw_count=raw_attachment_count,
            candidate_count=len(candidates or []),
            enqueued_count=enqueued,
            duplicate_count=duplicate_count,
            reason="post_filter",
        )
        if enqueued:
            logger.info(
                "[파일큐등록] 작업ID=%s\n게시물=%s\n후보=%s 큐등록=%s \033[1;33m큐대기\033[0m=%s 다음단계=다운로드",
                effective_job_id,
                post_url,
                len(candidates or []),
                enqueued,
                _collection_queue_stored_count(),
            )
        try:
            logger.debug(
                "[FileProbeDebug][enqueue.done] job_id=%s post=%s candidates=%s enqueued=%s queue_size=%s",
                effective_job_id,
                (post_url or "")[:220],
                len(candidates or []),
                enqueued,
                _collection_queue_stored_count(),
            )
        except Exception:
            pass

        if enqueued:
            async with self._stats_lock:
                self._sync_file_mode_scan_count()
            if self.progress_callback:
                self.progress_callback(self.get_stats())
            
        return enqueued

    # ---------------------------------------------------------
    # ---------------------------------------------------------

    @log_calls
    async def _record_scan_entries(self, attachments, post_url, effective_job_id):
        try:
            scan_entries = []
            for a in attachments or []:
                u = (a.get("href") or "").strip()
                if u:
                    scan_entries.append({
                        "url": u,
                        "source_page": post_url,
                        "name": (a.get("name") or a.get("title") or a.get("text") or "").strip(),
                    })
            if scan_entries:
                append_stage_urls(stage="scan", urls=scan_entries, job_id=effective_job_id, db_name=self.db_name)
        except Exception as e:
            # ignore scan entry recording errors
            logger.debug("[_record_scan_entries] scan record failed: %s", e)

    async def _load_file_pg_duplicate_fingerprints(self) -> None:
        """Load completed PG file identities once for this crawl job."""
        if getattr(self, "_file_pg_duplicate_fingerprints_loaded", False):
            return
        self._file_pg_duplicate_fingerprints_loaded = True
        self._file_pg_duplicate_fingerprints = set()
        self._file_pg_duplicate_names = set()

        job_id = str(getattr(self, "job_id", "") or "")
        db_name = str(getattr(self, "db_name", "") or "")
        logger.info("[FilePgDuplicate][load_begin] job_id=%s db=%s", job_id, db_name)
        try:
            table_name = await self._resolve_file_pg_training_table_for_source_check()
            if not table_name or not re.fullmatch(r"td_[a-z0-9_]+_training_data", table_name):
                logger.info(
                    "[FilePgDuplicate][load_done] job_id=%s db=%s table=%s rows=0 fingerprints=0 reason=training_table_unavailable",
                    job_id,
                    db_name,
                    table_name or "-",
                )
                return

            from db.db_operations import execute_query as pg_execute_query

            rows = await pg_execute_query(
                f"""
                SELECT DISTINCT
                    COALESCE(NULLIF(content_metadata ->> 'attachment_name', ''), NULLIF(subject, '')) AS file_name,
                    COALESCE(
                        NULLIF(content_metadata ->> 'file_size', ''),
                        NULLIF(content_metadata ->> 'content_size_bytes', '')
                    ) AS file_size
                FROM public.{table_name}
                WHERE COALESCE(content_metadata ->> 'source_category', 'file') = 'file'
                  AND COALESCE(
                        NULLIF(content_metadata ->> 'file_size', ''),
                        NULLIF(content_metadata ->> 'content_size_bytes', '')
                  ) ~ '^[0-9]+$'
                """,
                fetch=True,
                dbname=db_name,
            )
            fingerprints: Set[Tuple[str, int]] = set()
            for row in rows or []:
                data = dict(row or {})
                fingerprint = _file_pg_duplicate_fingerprint(
                    data.get("file_name"), data.get("file_size")
                )
                if fingerprint:
                    fingerprints.add(fingerprint)
            self._file_pg_duplicate_fingerprints = fingerprints
            self._file_pg_duplicate_names = {name for name, _size in fingerprints}
            logger.info(
                "[FilePgDuplicate][load_done] job_id=%s db=%s table=%s rows=%s fingerprints=%s",
                job_id,
                db_name,
                table_name,
                len(rows or []),
                len(fingerprints),
            )
        except Exception as exc:
            logger.warning(
                "[FilePgDuplicate][load_failed] job_id=%s db=%s err=%s",
                job_id,
                db_name,
                exc,
            )

    def _is_file_pg_duplicate(self, file_name: Any, file_size: Any) -> bool:
        fingerprint = _file_pg_duplicate_fingerprint(file_name, file_size)
        if not fingerprint:
            return False
        return fingerprint in (getattr(self, "_file_pg_duplicate_fingerprints", set()) or set())

    async def _find_file_maria_simhash_duplicate(
        self,
        *,
        simhash_decimal: str,
        normalized_length: int,
        file_size: int,
    ) -> Optional[Dict[str, Any]]:
        """Find a file LEARN_LIST row with the same decimal SimHash."""
        identity = build_file_simhash_duplicate_key(
            simhash_decimal=simhash_decimal,
            normalized_length=normalized_length,
            file_size=file_size,
        )
        if not identity:
            return None
        try:
            from db.mysql_db_config import mysql_execute_query

            lookup_bundle = getattr(self, "_file_simhash_maria_lookup_bundle", None)
            if not isinstance(lookup_bundle, dict):
                from db.mariadb_save_update import (
                    ensure_learn_list_standard_columns,
                    get_account_identifier_from_chatbot_setup,
                    get_learn_list_table_name,
                )

                account_identifier = await get_account_identifier_from_chatbot_setup(
                    str(getattr(self, "chat_bot_id", "") or ""),
                    str(getattr(self, "db_name", "") or ""),
                )
                learn_table = get_learn_list_table_name(account_identifier)
                columns = await ensure_learn_list_standard_columns(
                    str(getattr(self, "db_name", "") or ""),
                    learn_table,
                )
                lookup_bundle = {
                    "learn_table": learn_table,
                    "has_hash": "hash" in columns,
                }
                self._file_simhash_maria_lookup_bundle = lookup_bundle

            learn_table = str(lookup_bundle.get("learn_table") or "")
            if not learn_table or not bool(lookup_bundle.get("has_hash")):
                if not bool(lookup_bundle.get("missing_hash_logged")):
                    logger.warning(
                        "[FileSimHash][maria_lookup_skipped] job_id=%s db=%s table=%s reason=hash_column_missing",
                        getattr(self, "job_id", None),
                        getattr(self, "db_name", None),
                        learn_table,
                    )
                    lookup_bundle["missing_hash_logged"] = True
                return None

            rows = await mysql_execute_query(
                f"""
                SELECT `id`, `status`
                FROM `{learn_table}`
                WHERE `content_type` = 'file'
                  AND `hash` = %s
                LIMIT 1
                """,
                (identity[0],),
                fetch=True,
                dbname=str(getattr(self, "db_name", "") or ""),
                op_name=f"file_simhash_duplicate_lookup:job={getattr(self, 'job_id', None) or '-'}",
            )
            if rows:
                matched_row = dict(rows[0] or {})
                logger.info(
                    "[FileSimHash][maria_lookup_result] job_id=%s db=%s table=%s result=matched simhash=%s matched_learn_list_id=%s matched_status=%s",
                    getattr(self, "job_id", None),
                    getattr(self, "db_name", None),
                    learn_table,
                    identity[0],
                    matched_row.get("id"),
                    matched_row.get("status"),
                )
                return matched_row
            logger.info(
                "[FileSimHash][maria_lookup_result] job_id=%s db=%s table=%s result=not_matched simhash=%s",
                getattr(self, "job_id", None),
                getattr(self, "db_name", None),
                learn_table,
                identity[0],
            )
        except Exception as exc:
            logger.warning(
                "[FileSimHash][maria_lookup_failed] job_id=%s db=%s hash_length=%s normalized_length=%s file_size=%s err_type=%s",
                getattr(self, "job_id", None),
                getattr(self, "db_name", None),
                len(identity[0]),
                identity[1],
                identity[2],
                type(exc).__name__,
            )
        return None

    async def _prepare_file_simhash_before_persist(
        self,
        *,
        info: Dict[str, Any],
        file_name: str,
        file_size: int,
        file_url: str,
        extracted_text: str,
    ) -> Tuple[str, Optional[Tuple[str, int, int]]]:
        """Mask a parsed file and wait for the hash used by the PG duplicate gate."""
        text = str(extracted_text or "").strip()
        if not text:
            return text, None
        if learning_blocked_by_rrn_pattern(text):
            if str(os.getenv("FILE_CRAWL_RRN_MASK_INSTEAD_OF_BLOCK", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}:
                text = mask_rrn_like_patterns(text)
            else:
                logger.info(
                    "[FileSimHash][pre_persist_skipped] job_id=%s file_url=%s reason=rrn_blocked",
                    getattr(self, "job_id", None),
                    (file_url or "")[:220],
                )
                return text, None
        from backend.file.file_learning_text_mask import mask_file_learning_text
        from backend.shared.file_simhash_generation import generate_file_simhash_result

        text, fixed_mask_count = mask_file_learning_text(text)
        if fixed_mask_count:
            logger.info(
                "[FileSimHash][pre_persist_masked] job_id=%s file_url=%s replacements=%s",
                getattr(self, "job_id", None),
                (file_url or "")[:220],
                fixed_mask_count,
            )
        generated = await generate_file_simhash_result(
            job_id=str(getattr(self, "job_id", "") or ""),
            learn_list_row_id=None,
            db_name=str(getattr(self, "db_name", "") or ""),
            chat_bot_id=str(getattr(self, "chat_bot_id", "") or ""),
            file_url=file_url,
            source_url=_resolve_file_detail_source_url(info),
            title=str(file_name or "").strip(),
            content=text,
        )
        if generated is None:
            return text, None
        identity = build_file_simhash_duplicate_key(
            simhash_decimal=generated.value,
            normalized_length=generated.normalized_length,
            file_size=file_size,
        )
        if not identity:
            return text, None
        info["simhash_decimal"] = identity[0]
        info["simhash_normalized_length"] = identity[1]
        original_meta = dict(info.get("original_meta") or {})
        original_meta["simhash_decimal"] = identity[0]
        original_meta["simhash_normalized_length"] = identity[1]
        info["original_meta"] = original_meta
        logger.info(
            "[FileSimHash][pre_persist_completed] job_id=%s file_url=%s hash_length=%s normalized_length=%s file_size=%s",
            getattr(self, "job_id", None),
            (file_url or "")[:220],
            len(identity[0]),
            identity[1],
            identity[2],
        )
        return text, identity

    async def _backfill_file_pg_duplicate_source_url_if_missing(
        self,
        *,
        file_name: Any,
        file_size: Any,
        source_url: Any,
    ) -> int:
        """Fill only missing PG source_url values for an existing file identity.

        A file can be learned before its board detail URL is retained in PG.
        Keep the duplicate as a no-progress skip, but repair that missing
        metadata from the detail page currently being crawled.
        """
        fingerprint = _file_pg_duplicate_fingerprint(file_name, file_size)
        normalized_source_url = str(source_url or "").strip()
        if not fingerprint or not normalized_source_url:
            return 0

        checked = getattr(self, "_file_pg_source_url_backfill_checked", None)
        if not isinstance(checked, set):
            checked = set()
            self._file_pg_source_url_backfill_checked = checked
        if fingerprint in checked:
            return 0

        table_name = await self._resolve_file_pg_training_table_for_source_check()
        if not table_name or not re.fullmatch(r"td_[a-z0-9_]+_training_data", table_name):
            return 0

        try:
            from db.db_operations import execute_query as pg_execute_query

            size_value = str(fingerprint[1])
            candidates = await pg_execute_query(
                f"""
                SELECT
                    id::text AS id,
                    COALESCE(NULLIF(content_metadata ->> 'attachment_name', ''), NULLIF(subject, '')) AS file_name,
                    COALESCE(
                        NULLIF(content_metadata ->> 'file_size', ''),
                        NULLIF(content_metadata ->> 'content_size_bytes', '')
                    ) AS file_size
                FROM public.{table_name}
                WHERE COALESCE(content_metadata ->> 'source_category', 'file') = 'file'
                  AND COALESCE(
                        NULLIF(content_metadata ->> 'file_size', ''),
                        NULLIF(content_metadata ->> 'content_size_bytes', '')
                  ) = $1
                  AND COALESCE(NULLIF(BTRIM(content_metadata ->> 'source_url'), ''), '') = ''
                """,
                (size_value,),
                fetch=True,
                dbname=str(getattr(self, "db_name", "") or ""),
            )
            matched_ids = [
                str(dict(row or {}).get("id") or "").strip()
                for row in candidates or []
                if _file_pg_duplicate_fingerprint(
                    dict(row or {}).get("file_name"),
                    dict(row or {}).get("file_size"),
                ) == fingerprint
                and str(dict(row or {}).get("id") or "").strip()
            ]
            if not matched_ids:
                checked.add(fingerprint)
                logger.info(
                    "[FilePgDuplicate][source_url_backfill] job_id=%s table=%s file=%s size=%s result=no_missing_source_url",
                    getattr(self, "job_id", None),
                    table_name,
                    str(file_name or "")[:160],
                    fingerprint[1],
                )
                return 0

            updated_rows = await pg_execute_query(
                f"""
                UPDATE public.{table_name}
                SET content_metadata = jsonb_set(
                    COALESCE(content_metadata::jsonb, '{{}}'::jsonb),
                    '{{source_url}}',
                    to_jsonb($1::text),
                    true
                )
                WHERE id::text = ANY($2::text[])
                  AND COALESCE(NULLIF(BTRIM(content_metadata ->> 'source_url'), ''), '') = ''
                RETURNING id
                """,
                (normalized_source_url, matched_ids),
                fetch=True,
                dbname=str(getattr(self, "db_name", "") or ""),
            )
            updated_count = len(updated_rows or [])
            checked.add(fingerprint)
            logger.info(
                "[FilePgDuplicate][source_url_backfill] job_id=%s table=%s file=%s size=%s updated_rows=%s source_url=%s",
                getattr(self, "job_id", None),
                table_name,
                str(file_name or "")[:160],
                fingerprint[1],
                updated_count,
                normalized_source_url[:220],
            )
            return updated_count
        except Exception as exc:
            logger.warning(
                "[FilePgDuplicate][source_url_backfill_failed] job_id=%s table=%s file=%s size=%s source_url=%s err_type=%s err=%s",
                getattr(self, "job_id", None),
                table_name,
                str(file_name or "")[:160],
                fingerprint[1],
                normalized_source_url[:220],
                type(exc).__name__,
                exc,
            )
            return 0

    def _file_pg_duplicate_name_exists(self, file_name: Any) -> bool:
        """Avoid a preflight request unless PG has at least one same-name file."""
        fingerprint = _file_pg_duplicate_fingerprint(file_name, 1)
        if not fingerprint:
            return False
        return fingerprint[0] in (getattr(self, "_file_pg_duplicate_names", set()) or set())

    async def _probe_attachment_response_metadata_before_download(
        self,
        *,
        file_url: str,
        post_url: str,
        file_name: str,
    ) -> Tuple[str, int]:
        """Read final response filename and exact bytes without downloading its body."""
        if not file_url:
            return "", 0
        try:
            import aiohttp
            from utils.http_client import get_aiohttp_session

            timeout = aiohttp.ClientTimeout(total=5.0, connect=3.0, sock_read=5.0)
            headers = {
                "Accept": "*/*",
                "Referer": post_url or file_url,
                "User-Agent": "Mozilla/5.0",
            }
            async with get_aiohttp_session(headers=headers, timeout=timeout) as session:
                async with session.head(file_url, allow_redirects=True) as response:
                    size = _response_content_length_bytes(response.headers)
                    response_name = _response_attachment_filename(response.headers)
                    logger.info(
                        "[FilePgDuplicate][pre_download_probe] job_id=%s method=head status=%s size=%s attachment_file=%s response_file=%s url=%s",
                        getattr(self, "job_id", None),
                        response.status,
                        size or "-",
                        (file_name or "")[:180],
                        (response_name or "-")[:180],
                        (file_url or "")[:220],
                    )
                    if 200 <= response.status < 400 and size:
                        return response_name, size
                # Some municipal endpoints reject HEAD but accept GET. Ask only
                # for byte zero to obtain headers for the pre-download comparison.
                async with session.get(
                    file_url,
                    allow_redirects=True,
                    headers={"Range": "bytes=0-0"},
                ) as response:
                    size = _response_content_length_bytes(response.headers)
                    response_name = _response_attachment_filename(response.headers)
                    if not size:
                        try:
                            content_range = str(response.headers.get("content-range") or "")
                            size = int(content_range.rsplit("/", 1)[-1]) if "/" in content_range else 0
                        except (TypeError, ValueError):
                            size = 0
                    logger.info(
                        "[FilePgDuplicate][pre_download_probe] job_id=%s method=range_get status=%s size=%s attachment_file=%s response_file=%s url=%s",
                        getattr(self, "job_id", None),
                        response.status,
                        size or "-",
                        (file_name or "")[:180],
                        (response_name or "-")[:180],
                        (file_url or "")[:220],
                    )
                    return (response_name, size) if 200 <= response.status < 400 else ("", 0)
        except Exception as exc:
            logger.info(
                "[FilePgDuplicate][pre_download_probe_failed] job_id=%s file=%s url=%s err_type=%s",
                getattr(self, "job_id", None),
                (file_name or "")[:180],
                (file_url or "")[:220],
                type(exc).__name__,
            )
            return "", 0

    async def _confirm_file_selection_for_persist(
        self,
        *,
        info: Dict[str, Any],
        url: str,
        file_name: str,
        file_size: int,
        row_id: Any,
    ) -> bool:
        """Count a document only after a new LEARN_LIST row was created."""
        original_meta = info.get("original_meta") if isinstance(info.get("original_meta"), dict) else {}
        if info.get("_file_selection_counted") or original_meta.get("_file_selection_counted"):
            return False
        info["_file_selection_counted"] = True
        original_meta["_file_selection_counted"] = True
        original_meta.pop("_file_selection_pending_reason", None)
        async with self._stats_lock:
            self.stats["file_attachment_selection_confirmed_total"] = int(
                self.stats.get("file_attachment_selection_confirmed_total", 0) or 0
            ) + 1
            self._bump_stats_revision_locked()
        try:
            self._record_job_result_stage(
                url=url,
                stage="selection",
                status="success",
                source_url=info.get("source_page") or info.get("source_url"),
                file_url=url,
                file_name=file_name,
                db_id=row_id,
            )
        except Exception:
            pass
        logger.info(
            "[FileCounterTrace][selection_persisted] job_id=%s row_id=%s file=%s size=%s file_url=%s",
            getattr(self, "job_id", None),
            row_id,
            (file_name or "")[:180],
            file_size,
            (url or "")[:220],
        )
        return True

    async def _record_file_persist_outcome(
        self,
        *,
        outcome: str,
        reason: str,
        file_url: str,
        file_name: str,
        row_id: Any = None,
        persistence_action: str = "",
    ) -> None:
        """Record the MariaDB outcome separately from physical file storage."""
        outcome_key = re.sub(r"[^a-z0-9_]+", "_", str(outcome or "unknown").lower()).strip("_") or "unknown"
        async with self._stats_lock:
            key = f"file_persist_{outcome_key}_count"
            self.stats[key] = int(self.stats.get(key, 0) or 0) + 1
            self._bump_stats_revision_locked()
        logger.info(
            "[FilePersist][db_outcome] job_id=%s db=%s outcome=%s reason=%s persistence_action=%s row_id=%s file_url=%s file=%s",
            getattr(self, "job_id", None),
            getattr(self, "db_name", None),
            outcome_key,
            reason or "-",
            persistence_action or "-",
            row_id if row_id is not None else "-",
            (file_url or "")[:220],
            (file_name or "")[:180],
        )

    def _remember_file_pg_duplicate_fingerprint(self, file_name: Any, file_size: Any) -> None:
        fingerprint = _file_pg_duplicate_fingerprint(file_name, file_size)
        if fingerprint:
            fingerprints = getattr(self, "_file_pg_duplicate_fingerprints", None)
            if not isinstance(fingerprints, set):
                fingerprints = set()
                self._file_pg_duplicate_fingerprints = fingerprints
            fingerprints.add(fingerprint)
            names = getattr(self, "_file_pg_duplicate_names", None)
            if not isinstance(names, set):
                names = set()
                self._file_pg_duplicate_names = names
            names.add(fingerprint[0])

    async def _resolve_file_pg_training_table_for_source_check(self) -> str:
        if getattr(self, "_file_pg_source_check_table_resolved", False):
            return str(getattr(self, "_file_pg_source_check_table", "") or "")
        try:
            self._file_pg_source_check_table_resolved = True
        except Exception:
            pass

        table_name = ""
        try:
            if not (self.chat_bot_id and self.db_name):
                return ""
            from db.db_operations import execute_query as pg_execute_query

            chat_ids: List[str] = []
            try:
                setup_rows = await pg_execute_query(
                    "SELECT chat_id FROM chatbot_setup WHERE chat_bot_id = $1 LIMIT 1",
                    (str(self.chat_bot_id),),
                    fetch=True,
                    dbname=str(self.db_name),
                )
                for row in setup_rows or []:
                    value = dict(row).get("chat_id") if row else None
                    text = str(value or "").strip()
                    if text and text not in chat_ids:
                        chat_ids.append(text)
            except Exception as exc:
                logger.debug(
                    "[FileSourceDup] PG chatbot_setup lookup skipped | job_id=%s db=%s err=%s",
                    getattr(self, "job_id", None),
                    getattr(self, "db_name", None),
                    exc,
                )

            raw_chat_bot_id = str(self.chat_bot_id or "").strip()
            compact_chat_bot_id = re.sub(r"[^A-Za-z0-9_]", "", raw_chat_bot_id)
            for value in (raw_chat_bot_id, compact_chat_bot_id):
                if value and value not in chat_ids:
                    chat_ids.append(value)

            candidates: List[str] = []
            for chat_id in chat_ids:
                safe = re.sub(r"[^A-Za-z0-9_]", "", str(chat_id or ""))
                if not safe:
                    continue
                candidate = f"td_{safe}_training_data".lower()
                if candidate not in candidates:
                    candidates.append(candidate)
            if not candidates:
                return ""

            placeholders = ", ".join(f"${idx}" for idx in range(1, len(candidates) + 1))
            rows = await pg_execute_query(
                f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ({placeholders})
                """,
                tuple(candidates),
                fetch=True,
                dbname=str(self.db_name),
            )
            existing = {
                str(dict(row).get("table_name") or "").strip().lower()
                for row in rows or []
                if row and dict(row).get("table_name")
            }
            table_name = next((candidate for candidate in candidates if candidate in existing), "")
        except Exception as exc:
            logger.debug(
                "[FileSourceDup] PG training table resolve failed | job_id=%s db=%s err=%s",
                getattr(self, "job_id", None),
                getattr(self, "db_name", None),
                exc,
            )
            table_name = ""
        try:
            self._file_pg_source_check_table = table_name
        except Exception:
            pass
        return table_name

    async def _find_file_pg_source_url_processed_row(
        self,
        *,
        post_url: str,
        effective_job_id: str,
    ) -> Optional[Dict[str, Any]]:
        # Policy 1B: file crawling must not skip a whole detail page by source_url.
        # Duplicate checks run at file level after attachment discovery/download.
        return None

    async def _record_file_source_url_duplicate_skip(
        self,
        *,
        post_url: str,
        effective_job_id: str,
        row: Dict[str, Any],
        attachment_count: int,
    ) -> None:
        try:
            async with self._stats_lock:
                self.stats["event"] = "file_source_url_duplicate_reuse"
                self.stats["message"] = "source_url already processed"
                self.stats["file_source_url_duplicate_skip_count"] = int(
                    self.stats.get("file_source_url_duplicate_skip_count", 0) or 0
                ) + 1
                self.stats["file_download_skipped_count"] = int(
                    self.stats.get("file_download_skipped_count", 0) or 0
                ) + max(1, int(attachment_count or 0))
                samples = list(self.stats.get("file_download_skipped_samples") or [])
                samples.append(
                    {
                        "reason": "source_url_duplicate_reuse",
                        "url": str(post_url or "")[:220],
                        "pg_row_id": row.get("id") if isinstance(row, dict) else None,
                        "attachment_count": int(attachment_count or 0),
                    }
                )
                self.stats["file_download_skipped_samples"] = samples[-20:]
        except Exception:
            pass
        try:
            append_stage_urls(
                stage="skip",
                urls=[
                    {
                        "url": post_url,
                        "reason": "source_url_duplicate_reuse",
                        "pg_row_id": row.get("id") if isinstance(row, dict) else None,
                        "attachment_count": int(attachment_count or 0),
                    }
                ],
                job_id=effective_job_id,
                db_name=self.db_name,
            )
        except Exception:
            pass
        try:
            logger.debug(
                "[FileSourceDup] duplicate reuse done | job_id=%s post=%s attachment_count=%s pg_row_id=%s",
                effective_job_id,
                (post_url or "")[:220],
                attachment_count,
                row.get("id") if isinstance(row, dict) else None,
            )
            if self.progress_callback:
                self.progress_callback(self.get_stats())
        except Exception:
            pass

    @log_calls
    def _normalize_file_url(self, attach, post_url):
        file_url = (attach.get("href") or "").strip()
        file_name = (
            attach.get("attachment_name")
            or attach.get("display_name")
            or attach.get("name")
            or attach.get("title")
            or attach.get("text")
            or attach.get("alt")
            or attach.get("img_alt")
            or ""
        ).strip()
        try:
            attach["_exact_file_size_bytes"] = _attachment_exact_file_size_bytes(attach)
            attach["_declared_file_size_bytes"] = int(attach.get("declared_file_size_bytes") or parse_display_file_size_bytes(file_name) or 0)
        except Exception:
            attach["_exact_file_size_bytes"] = 0
            attach["_declared_file_size_bytes"] = 0
        if file_name:
            cleaned_name = strip_file_type_display_prefix(strip_trailing_file_size(file_name))
            cleaned_name = strip_fallback_download_label(cleaned_name) or cleaned_name
            file_name = cleaned_name or file_name
        
        if file_url.lower().startswith("javascript:"):
            
            actual_url = extract_download_url_from_js(file_url, post_url)
            if not actual_url:
                actual_url = resolve_anseong_yhlib_download_url(file_url, post_url)
            
            if not actual_url or actual_url.startswith("javascript:"):
                match = re.search(r"['\"](https?://[^'\"]+)['\"]", file_url)
                if match:
                    actual_url = match.group(1)

            if actual_url and not actual_url.startswith("javascript:"):
                file_url = actual_url
            else:
                # no usable URL extracted from JS, fall through
                pass
        url_file_name = _file_name_from_download_url(file_url)
        if url_file_name and _is_generic_attachment_display_name(file_name):
            file_name = url_file_name
            try:
                for key in ("attachment_name", "display_name", "name", "original_name"):
                    if _is_generic_attachment_display_name(attach.get(key)):
                        attach[key] = url_file_name
            except Exception:
                pass
        file_url_key = canonicalize_attachment_url_for_learn_list(file_url) or canonicalize_url_for_dedup(file_url) or file_url
        method = str(attach.get("method") or "GET").strip().upper()
        params = attach.get("params") or {}
        if method != "GET" or params:
            try:
                params_key = urlencode(sorted((str(k), str(v)) for k, v in dict(params).items()))
            except Exception:
                params_key = str(params or "")
            file_url_key = f"{file_url_key}|method={method}|params={params_key}"
        
        return file_url, file_name, file_url_key

    def _file_url_dedup_keys(self, file_url: str, file_url_key: str) -> List[str]:
        keys: List[str] = []

        primary = str(file_url_key or "").strip()
        if primary:
            keys.append(primary)

        try:
            for raw_key in extract_attachment_key_candidates(file_url):
                key = str(raw_key or "").strip()
                if not key:
                    continue
                tagged = f"attach_key:{key.lower()}"
                if tagged not in keys:
                    keys.append(tagged)
            for raw_key in extract_anseong_attachment_key_candidates(file_url):
                key = str(raw_key or "").strip()
                if not key:
                    continue
                tagged = f"attach_key:{key.lower()}"
                if tagged not in keys:
                    keys.append(tagged)
        except Exception:
            pass

        return keys

    async def _notify_file_dup_scan(
        self, post_url: str, effective_job_id: str, reason: str = "duplicated in db"
    ) -> None:
        try:
            if self._file_job_queues:
                await self._file_job_queues.progress_queue.put(
                    {
                        "type": "scan",
                        "count": 1,
                        "items": [post_url],
                        "job_id": effective_job_id,
                    }
                )
            append_stage_urls(
                stage="scan",
                urls=[{"url": post_url, "reason": reason}],
                job_id=effective_job_id,
                db_name=self.db_name,
            )
        except Exception:
            pass

    async def _backfill_duplicate_file_summary_if_needed(
        self,
        *,
        learn_list_id: Any,
        reason: str = "duplicate_file_reuse",
    ) -> None:
        from db.mariadb_save_update import (
            get_account_identifier_from_chatbot_setup,
            get_learn_list_table_name,
        )
        from db.mysql_db_config import mysql_execute_query
        from utils.learn_list_keyword import process_single_item_keywords

        try:
            learn_id = int(learn_list_id or 0)
        except Exception:
            learn_id = 0
        if learn_id <= 0 or not (self.chat_bot_id and self.db_name):
            logger.debug(
                "[DuplicateSummaryDebug] file skipped | job_id=%s learn_list_id=%r reason=%s detail=invalid_context",
                getattr(self, "job_id", None),
                learn_list_id,
                reason,
            )
            return

        async def _resolve_rows(fetch_result: Any, *, phase: str) -> Any:
            resolved = fetch_result
            unwrap_count = 0
            while unwrap_count < 3 and (asyncio.isfuture(resolved) or hasattr(resolved, "__await__")):
                resolved = await resolved
                unwrap_count += 1
            if unwrap_count:
                logger.debug(
                    "[DuplicateSummaryDebug] file awaitable rows unwrapped | job_id=%s learn_list_id=%s reason=%s phase=%s unwrap_count=%s",
                    getattr(self, "job_id", None),
                    learn_id,
                    reason,
                    phase,
                    unwrap_count,
                )
            return resolved

        try:
            account_identifier = await get_account_identifier_from_chatbot_setup(
                str(self.chat_bot_id),
                str(self.db_name),
            )
            learn_table = get_learn_list_table_name(account_identifier)
            rows = await mysql_execute_query(
                (
                    f"SELECT `id`, `subject`, `content`, `content_type`, `memo1` "
                    f"FROM `{learn_table}` WHERE `id` = %s LIMIT 1"
                ),
                (learn_id,),
                fetch=True,
                dbname=self.db_name,
            )
            rows = await _resolve_rows(rows, phase="precheck")
        except Exception as exc:
            logger.debug(
                "[DuplicateSummaryDebug] file precheck failed | job_id=%s learn_list_id=%s reason=%s err=%s",
                getattr(self, "job_id", None),
                learn_id,
                reason,
                exc,
            )
            return

        row = (rows or [{}])[0] if rows else {}
        current_memo = str((row or {}).get("memo1") or "").strip()
        if current_memo:
            logger.debug(
                "[DuplicateSummaryDebug] file skipped | job_id=%s learn_list_id=%s reason=%s detail=already_filled memo1_len=%s",
                getattr(self, "job_id", None),
                learn_id,
                reason,
                len(current_memo),
            )
            return

        subject = str((row or {}).get("subject") or "").strip()
        content = str((row or {}).get("content") or "").strip()
        content_type = str((row or {}).get("content_type") or "file").strip().lower() or "file"
        logger.debug(
            "[DuplicateSummaryDebug] file start | job_id=%s learn_list_id=%s reason=%s content_type=%s subject_len=%s content_len=%s",
            getattr(self, "job_id", None),
            learn_id,
            reason,
            content_type,
            len(subject),
            len(content),
        )

        try:
            result = await process_single_item_keywords(
                chat_bot_id=str(self.chat_bot_id),
                maria_db_name=str(self.db_name),
                item_id=learn_id,
                subject=subject,
                content=content,
                content_type=content_type,
                pg_db_name=str(self.db_name),
            )
            logger.debug(
                "[DuplicateSummaryDebug] file backfill attempted | job_id=%s learn_list_id=%s reason=%s status=%s result_reason=%s subject=%s",
                getattr(self, "job_id", None),
                learn_id,
                reason,
                (result or {}).get("status"),
                (result or {}).get("reason"),
                subject[:120],
            )
        except Exception as exc:
            logger.debug(
                "[DuplicateSummaryDebug] file backfill failed | job_id=%s learn_list_id=%s reason=%s err=%s",
                getattr(self, "job_id", None),
                learn_id,
                reason,
                exc,
            )

    async def _file_crawl_post_summarize_keywords(
        self,
        *,
        file_url: str,
        learn_list_id: Optional[int] = None,
        subject: str = "",
        normalized_text: str = "",
    ) -> None:
        """
        Dispatch the summary API after a file crawl item has finished learning.
        """
        enabled = str(os.getenv("FILE_CRAWL_POST_SUMMARIZE_KEYWORDS", "1") or "1").strip().lower()
        if enabled not in {"1", "true", "yes", "y", "on"}:
            logger.debug(
                "[FileSummaryAPI][CallDebug] skip disabled | job_id=%s learn_list_id=%s env=%s file_url=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                enabled,
                (file_url or "")[:180],
            )
            return
        if not (self.chat_bot_id and self.db_name):
            logger.debug(
                "[FileSummaryAPI][CallDebug] skip missing context | job_id=%s file_url=%s chat_bot_id=%s db=%s",
                getattr(self, "job_id", None),
                (file_url or "")[:180],
                bool(self.chat_bot_id),
                self.db_name,
            )
            return

        target_url = str(file_url or "").strip()
        if not target_url.startswith(("http://", "https://")):
            logger.debug(
                "[FileSummaryAPI][CallDebug] skip non-http file_url | job_id=%s learn_list_id=%s file_url=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                target_url[:180],
            )
            return

        dispatch_key = f"{int(learn_list_id or 0)}::{canonicalize_attachment_url_for_learn_list(target_url) or target_url}"
        try:
            dispatched = getattr(self, "_file_summarize_dispatched_keys", None)
            if not isinstance(dispatched, set):
                dispatched = set()
                self._file_summarize_dispatched_keys = dispatched
            if dispatch_key in dispatched:
                logger.debug(
                    "[FileSummaryAPI][CallDebug] skip duplicate dispatch | job_id=%s learn_list_id=%s key=%s url=%s",
                    getattr(self, "job_id", None),
                    learn_list_id,
                    dispatch_key[:220],
                    target_url[:180],
                )
                return
            dispatched.add(dispatch_key)
        except Exception:
            pass

        from backend.shared.summarize_keywords_client import (
            enqueue_summarize_keywords,
            post_summarize_keywords,
            summarize_keywords_endpoint,
            summarize_keywords_payload_concurrency,
            summarize_keywords_timeout_sec,
            summarize_keywords_use_queue,
        )
        from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot

        payload: Dict[str, Any] = {
            "chat_bot_id": str(self.chat_bot_id),
            "db_name": str(self.db_name),
            "target_db": str(self.db_name),
            "target": "learn_list",
            "contents": [target_url],
            "content_type": "file",
            "concurrency": summarize_keywords_payload_concurrency(),
            "source": "file_crawl",
            "file_crawl": True,
            "source_url": target_url,
        }
        try:
            learn_table_for_summary = await resolve_learn_list_table_name_for_chatbot(
                self.chat_bot_id,
                self.db_name,
            )
        except Exception as table_exc:
            learn_table_for_summary = ""
            logger.debug(
                "[FileSummaryAPI][CallDebug] learn table resolve failed | job_id=%s learn_list_id=%s db=%s err=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                self.db_name,
                table_exc,
            )
        if learn_table_for_summary:
            payload["learn_table"] = str(learn_table_for_summary)
            payload["target_table"] = str(learn_table_for_summary)
        if learn_list_id:
            payload["learn_list_id"] = int(learn_list_id)
        if subject:
            payload["subject"] = str(subject).strip()
        include_text = str(os.getenv("FILE_CRAWL_SUMMARY_INCLUDE_TEXT", "1") or "1").strip().lower()
        if normalized_text and include_text in {"1", "true", "yes", "y", "on"}:
            payload["normalized_text"] = normalized_text
            payload["normalized_contents"] = [normalized_text]

        endpoint = summarize_keywords_endpoint()
        try:
            row_debug: Dict[str, Any] = {}
            original_target_url = target_url
            if learn_table_for_summary and learn_list_id:
                from db.mysql_db_config import mysql_execute_query

                rows = await mysql_execute_query(
                    (
                        f"SELECT `id`, `status`, `content_type`, "
                        f"LEFT(`content`, 2048) AS `content`, "
                        f"CHAR_LENGTH(`subject`) AS `subject_len`, "
                        f"CHAR_LENGTH(`memo1`) AS `memo1_len` "
                        f"FROM `{learn_table_for_summary}` WHERE `id` = %s LIMIT 1"
                    ),
                    (int(learn_list_id),),
                    fetch=True,
                    dbname=self.db_name,
                )
                if rows:
                    row = rows[0] or {}
                    stored_content = str(row.get("content") or "").strip()
                    if stored_content.startswith(("http://", "https://")) and stored_content != target_url:
                        target_url = stored_content
                        payload["contents"] = [target_url]
                        payload["source_url"] = target_url
                    row_debug = {
                        "found": True,
                        "id": row.get("id"),
                        "status": row.get("status"),
                        "content_type": row.get("content_type"),
                        "content_match": stored_content == target_url,
                        "original_url_match": stored_content == original_target_url,
                        "content": stored_content[:180],
                        "subject_len": int(row.get("subject_len") or 0),
                        "memo1_len": int(row.get("memo1_len") or 0),
                    }
                else:
                    row_debug = {"found": False}
            logger.debug(
                "[FileSummaryAPI][Debug] target_ready | job_id=%s db=%s table=%s learn_list_id=%s payload_keys=%s content_type=%s original_url=%s dispatch_url=%s normalized_text_len=%s row=%s",
                getattr(self, "job_id", None),
                self.db_name,
                learn_table_for_summary or "-",
                learn_list_id,
                sorted(payload.keys()),
                payload.get("content_type"),
                original_target_url[:180],
                target_url[:180],
                len(str(payload.get("normalized_text") or "")),
                row_debug,
            )
        except Exception as debug_exc:
            logger.debug(
                "[FileSummaryAPI][Debug] target_ready_failed | job_id=%s learn_list_id=%s url=%s err=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                target_url[:180],
                debug_exc,
            )
        logger.debug(
            "[FileSummaryAPI][CallDebug] dispatch_start | job_id=%s learn_list_id=%s url=%s queue=%s endpoint=%s",
            getattr(self, "job_id", None),
            learn_list_id,
            target_url[:180],
            summarize_keywords_use_queue(),
            endpoint,
        )
        if summarize_keywords_use_queue():
            await enqueue_summarize_keywords(
                endpoint,
                payload,
                timeout_sec=summarize_keywords_timeout_sec(),
            )
            logger.debug(
                "[FileSummaryAPI] dispatch_queued | job_id=%s learn_list_id=%s url=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                target_url[:180],
            )
            return

        status, body = await post_summarize_keywords(
            endpoint,
            payload,
            timeout_sec=summarize_keywords_timeout_sec(),
        )
        if status != 200:
            logger.debug(
                "[FileSummaryAPI] dispatch_failed | job_id=%s learn_list_id=%s status=%s detail=%s url=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                status,
                " ".join(str(body or "").split())[:240],
                target_url[:180],
            )
            return
        try:
            import json as _json

            parsed = _json.loads(body or "{}")
            logger.debug(
                "[FileSummaryAPI][Debug] response_ok | job_id=%s learn_list_id=%s total=%s updated=%s skipped=%s errors=%s url=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                parsed.get("total") if isinstance(parsed, dict) else None,
                parsed.get("updated") if isinstance(parsed, dict) else None,
                parsed.get("skipped") if isinstance(parsed, dict) else None,
                parsed.get("errors") if isinstance(parsed, dict) else None,
                target_url[:180],
            )
        except Exception as parse_exc:
            logger.debug(
                "[FileSummaryAPI][Debug] response_parse_failed | job_id=%s learn_list_id=%s url=%s err=%s body=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                target_url[:180],
                parse_exc,
                " ".join(str(body or "").split())[:240],
            )
        logger.debug(
            "[FileSummaryAPI] dispatch_done | job_id=%s learn_list_id=%s url=%s",
            getattr(self, "job_id", None),
            learn_list_id,
            target_url[:180],
        )

    async def _resolve_file_learn_list_duplicate_row(
        self,
        *,
        row: Dict[str, Any],
        learn_table: str,
        post_url: str,
        effective_job_id: str,
        new_cate1: Optional[str],
        new_cate2: Optional[str],
        author_meta: Optional[Dict[str, Any]] = None,
        emit_progress: bool = True,
        notify_scan: bool = True,
    ) -> bool:
        """Resolve an existing LEARN_LIST duplicate row for file crawling.

        When content/file enqueue finds a duplicate, this updates missing category
        or author metadata on the existing row when the configured policy allows it.
        Returns True when the duplicate should be treated as already handled.
        """
        from backend.file.file_category_apply import map_board_cate2_to_file_learning
        from db.mariadb_save_update import (
            learn_list_row_cates_both_empty,
            learn_list_merge_cate_on_duplicate_row,
        )

        if not row or row.get("id") is None:
            return False

        allow_existing_update = str(
            os.getenv("FILE_CRAWL_ALLOW_EXISTING_DUPLICATE_ROW_UPDATE", "0") or "0"
        ).strip().lower() in ("1", "true", "yes", "on")
        if not allow_existing_update:
            logger.debug(
                "[Duplicate][file] existing LEARN_LIST row matched; content update disabled, category handled by sub_cate | job_id=%s row_id=%s post=%s",
                effective_job_id,
                row.get("id"),
                (post_url or "")[:180],
            )

        parsed_cols = {
            key
            for key in (
                "content_author",
                "content_author_kind",
                "content_author_raw",
                "content_department",
                "content_department_raw",
            )
            if key in row
        }

        if "cate1" not in row and not parsed_cols:
            await self._notify_file_dup_scan(post_url, effective_job_id)
            return True

        has_c2_col = "cate2" in row
        db_c1 = str(row.get("cate1") or "").strip()
        db_c2 = str(row.get("cate2") or "").strip() if has_c2_col else ""

        cate_missing = "cate1" in row and learn_list_row_cates_both_empty(db_c1, db_c2, has_c2_col)
        author_missing = False
        if parsed_cols:
            try:
                from db.mariadb_save_update import _learn_list_author_missing  # type: ignore

                author_missing = "content_author" in parsed_cols and _learn_list_author_missing(
                    row.get("content_author")
                )
            except Exception:
                author_missing = "content_author" in parsed_cols and not str(
                    row.get("content_author") or ""
                ).strip()
        if not cate_missing and not author_missing:
            logger.debug(
                "[Duplicate][file] existing row has category/author; merge will defer to sub_cate | job_id=%s row_id=%s post=%s",
                effective_job_id,
                row.get("id"),
                (post_url or "")[:180],
            )

        cols_set: Set[str] = set()
        if "cate1" in row:
            cols_set.add("cate1")
        if has_c2_col:
            cols_set.add("cate2")
        cols_set.update(parsed_cols)
        resolved_cate1 = str(new_cate1 or "").strip()
        resolved_cate2 = str(new_cate2 or "").strip()
        if not (resolved_cate1 or resolved_cate2):
            w1, w2 = getattr(self, "cate1", None), getattr(self, "cate2", None)
            if str(w1 or "").strip() or str(w2 or "").strip():
                resolved_cate1 = str(w1 or "").strip()
                resolved_cate2 = str(w2 or "").strip()
        ref_cate1, ref_cate2 = resolved_cate1, resolved_cate2
        try:
            if self.chat_bot_id and self.db_name and (resolved_cate1 or resolved_cate2):
                resolved_cate1, resolved_cate2 = await map_board_cate2_to_file_learning(
                    chat_bot_id=str(self.chat_bot_id),
                    db_name=str(self.db_name),
                    board_cate1=resolved_cate1,
                    board_cate2=resolved_cate2,
                    access_url=getattr(self, "access_url", None),
                    request_cookies=getattr(self, "_category_sync_request_cookies", None),
                )
        except Exception as exc:
            logger.debug(
                '[Duplicate][file] failed to infer category from reference post | post=%s ref=(%s,%s) err=%s',
                (post_url or "")[:120],
                ref_cate1,
                ref_cate2,
                exc,
            )
        meta: Dict[str, Any] = dict(author_meta or {})
        try:
            from backend.shared.sub_cate_mode import get_sub_cate_mode_from_config

            meta["_sub_cate_mode"] = await get_sub_cate_mode_from_config(
                str(self.chat_bot_id or ""),
                dbname=str(self.db_name or ""),
            )
        except Exception:
            meta["_sub_cate_mode"] = "emp"
        meta["cate1"] = resolved_cate1
        meta["cate2"] = resolved_cate2
        if ref_cate1 or ref_cate2:
            original_meta = dict(meta.get("original_meta") or {})
            original_meta.update({"ref_cate1": ref_cate1, "ref_cate2": ref_cate2})
            meta["original_meta"] = original_meta
        updated = await learn_list_merge_cate_on_duplicate_row(
            self.db_name, learn_table, cols_set, row, meta
        )
        if updated:
            db_id = row.get("id")
            logger.debug(
                '[Duplicate][file] merged category metadata into existing row (ID: %s) | post=%s',
                db_id,
                (post_url or "")[:120],
            )
            if emit_progress:
                async with self._stats_lock:
                    self.stats["save_update_count"] = (
                        int(self.stats.get("save_update_count", 0) or 0) + 1
                    )
                if self.progress_callback:
                    self.progress_callback(self.get_stats())
            logger.info(
                "[FileCateBackfill][duplicate_row_updated] job_id=%s row_id=%s post_url=%s emit_progress=%s",
                effective_job_id,
                db_id,
                (post_url or "")[:220],
                emit_progress,
            )
            if notify_scan:
                await self._notify_file_dup_scan(
                    post_url,
                    effective_job_id,
                    reason="duplicated_in_db_cate_updated",
                )
        elif notify_scan:
            await self._notify_file_dup_scan(post_url, effective_job_id)
        return True

    async def _log_file_learn_list_row_debug(
        self,
        *,
        stage: str,
        learn_list_id: Any,
        url: str = "",
        file_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        if str(os.getenv("FILE_LEARN_LIST_ROW_DEBUG", "0") or "0").strip().lower() not in {"1", "true", "yes", "on"}:
            return
        try:
            row_id = int(learn_list_id or 0)
        except Exception:
            row_id = 0
        if row_id <= 0 or not self.chat_bot_id or not self.db_name:
            logger.debug(
                "[LearningTrace][board_file.row_debug] stage=%s job_id=%s db=%s url=%s learn_list_id=%s skipped=missing_context",
                stage,
                getattr(self, "job_id", None),
                getattr(self, "db_name", None),
                (url or "")[:180],
                learn_list_id,
            )
            return
        try:
            from db.mariadb_save_update import (
                coalesce_learn_list_cates,
                get_account_identifier_from_chatbot_setup,
                get_learn_list_table_name,
                ensure_learn_list_standard_columns,
            )
            from db.mysql_db_config import mysql_execute_query

            account_identifier = await get_account_identifier_from_chatbot_setup(
                str(self.chat_bot_id),
                str(self.db_name),
            )
            learn_table = get_learn_list_table_name(account_identifier)
            cols = await ensure_learn_list_standard_columns(str(self.db_name), learn_table)
            select_cols = ["`id`"]
            for col in ("status", "content_type", "cate1", "cate2", "subject", "content", "source_page"):
                if col in cols:
                    select_cols.append(f"`{col}`")
            rows = await mysql_execute_query(
                f"SELECT {', '.join(select_cols)} FROM `{learn_table}` WHERE id = %s LIMIT 1",
                (row_id,),
                fetch=True,
                dbname=str(self.db_name),
                op_name="file_learn_list_row_debug",
            )
            row = rows[0] if rows else {}
            expected_cate1, expected_cate2 = coalesce_learn_list_cates(file_info or {})
            logger.debug(
                "[LearningTrace][board_file.row_debug] stage=%s job_id=%s db=%s table=%s url=%s "
                "learn_list_id=%s found=%s status=%s content_type=%s row_cate=(%r,%r) expected_cate=(%r,%r) "
                "subject=%s content=%s source_page=%s",
                stage,
                getattr(self, "job_id", None),
                getattr(self, "db_name", None),
                learn_table,
                (url or "")[:180],
                row_id,
                bool(rows),
                (row or {}).get("status"),
                (row or {}).get("content_type"),
                (row or {}).get("cate1"),
                (row or {}).get("cate2"),
                expected_cate1,
                expected_cate2,
                str((row or {}).get("subject") or "")[:120],
                str((row or {}).get("content") or "")[:180],
                str((row or {}).get("source_page") or "")[:180],
            )
        except Exception as exc:
            logger.error(
                "[LearningError][board_file.row_debug] stage=%s job_id=%s db=%s url=%s learn_list_id=%s failed=%s",
                stage,
                getattr(self, "job_id", None),
                getattr(self, "db_name", None),
                (url or "")[:180],
                learn_list_id,
                exc,
            )

    @log_calls
    async def _is_file_duplicated_in_db(
        self,
        file_url,
        post_url,
        effective_job_id,
        *,
        new_cate1: Optional[str] = None,
        new_cate2: Optional[str] = None,
        author_meta: Optional[Dict[str, Any]] = None,
    ):
        try:
            # Resolve the LEARN_LIST row for a file URL in the current chatbot DB.
            if not (self.chat_bot_id and self.db_name and post_url):
                return False

            from db.mariadb_save_update import (
                get_account_identifier_from_chatbot_setup,
                get_learn_list_table_name,
                ensure_learn_list_standard_columns,
            )
            from db.mysql_db_config import mysql_execute_query

            account_identifier = await get_account_identifier_from_chatbot_setup(
                self.chat_bot_id, self.db_name
            )
            learn_table = get_learn_list_table_name(account_identifier)
            cols = await ensure_learn_list_standard_columns(self.db_name, learn_table)

            if not cols or "content" not in cols:
                return False

            target_col = "content"
            lookup_key = file_url

            dedup_type = (
                "file" if getattr(self, "is_attachment_file_crawl_workflow", False) else "board"
            )

            sel_parts = ["`id`"]
            if "cate1" in cols and "cate2" in cols:
                sel_parts.extend(["`cate1`", "`cate2`"])
            for parsed_col in (
                "content_author",
                "content_author_kind",
                "content_author_raw",
                "content_department",
                "content_department_raw",
            ):
                if parsed_col in cols:
                    sel_parts.append(f"`{parsed_col}`")
            sel_sql = ", ".join(sel_parts)

            type_filter_sql = ""
            params_suffix: tuple[Any, ...] = tuple()
            if dedup_type == "file" and "content_type" in cols:
                type_filter_sql = " AND LOWER(COALESCE(`content_type`, '')) = %s"
                params_suffix = ("file",)
            elif "type" in cols:
                if dedup_type == "file":
                    type_filter_sql = " AND (`type` = %s OR `type` IS NULL OR `type` = '')"
                else:
                    type_filter_sql = " AND `type` = %s"
                params_suffix = (dedup_type,)
            sql = f"SELECT {sel_sql} FROM `{learn_table}` WHERE `{target_col}` = %s{type_filter_sql} LIMIT 1"
            params = (lookup_key, *params_suffix)

            rows = await mysql_execute_query(
                sql,
                params,
                fetch=True,
                dbname=self.db_name,
                op_name=f"file_prequeue_url_duplicate_lookup:job={effective_job_id or '-'}",
            )

            if rows and isinstance(rows[0], dict):
                return await self._resolve_file_learn_list_duplicate_row(
                    row=rows[0],
                    learn_table=learn_table,
                    post_url=post_url,
                    effective_job_id=effective_job_id,
                    new_cate1=new_cate1,
                    new_cate2=new_cate2,
                    author_meta=author_meta,
                )

            if target_col == "content":
                sys_dup = await self._try_duplicate_by_attachment_key_like(
                    learn_table=learn_table,
                    file_url=file_url,
                    post_url=post_url,
                    effective_job_id=effective_job_id,
                    has_type=("type" in cols),
                    dedup_type=dedup_type,
                    new_cate1=new_cate1,
                    new_cate2=new_cate2,
                    author_meta=author_meta,
                )
                if sys_dup:
                    return True

        except Exception as e:
            # DB check failed; log and allow processing to continue
            logger.debug("[_is_file_duplicated_in_db] DB check error: %s", e)
        return False

    async def _ensure_file_dup_sql_bundle(self) -> Optional[Dict[str, Any]]:
        """Resolve and cache the SQL/table metadata needed for LEARN_LIST duplicate checks."""
        if getattr(self, "_file_dup_sql_bundle_resolved", False):
            return getattr(self, "_file_dup_sql_bundle_v", None)
        try:
            self._file_dup_sql_bundle_resolved = True
        except Exception:
            pass
        bundle: Optional[Dict[str, Any]] = None
        try:
            if not (self.chat_bot_id and self.db_name):
                self._file_dup_sql_bundle_v = None
                return None
            from db.mariadb_save_update import (
                get_account_identifier_from_chatbot_setup,
                get_learn_list_table_name,
                ensure_learn_list_standard_columns,
            )

            account_identifier = await get_account_identifier_from_chatbot_setup(
                self.chat_bot_id, self.db_name
            )
            learn_table = get_learn_list_table_name(account_identifier)
            cols = await ensure_learn_list_standard_columns(self.db_name, learn_table)
            if not cols or "content" not in cols:
                self._file_dup_sql_bundle_v = None
                return None

            target_col = "content"
            key_mode = "raw"

            dedup_type = "file" if getattr(self, "is_attachment_file_crawl_workflow", False) else "board"

            sel_parts = ["`id`"]
            if "cate1" in cols and "cate2" in cols:
                sel_parts.extend(["`cate1`", "`cate2`"])
            for parsed_col in (
                "content_author",
                "content_author_kind",
                "content_author_raw",
                "content_department",
                "content_department_raw",
            ):
                if parsed_col in cols:
                    sel_parts.append(f"`{parsed_col}`")
            sel_sql = ", ".join(sel_parts)

            type_filter_sql = ""
            has_type = False
            uses_content_type = False
            if dedup_type == "file" and "content_type" in cols:
                type_filter_sql = " AND LOWER(COALESCE(`content_type`, '')) = %s"
                uses_content_type = True
            elif "type" in cols:
                if dedup_type == "file":
                    type_filter_sql = " AND (`type` = %s OR `type` IS NULL OR `type` = '')"
                else:
                    type_filter_sql = " AND `type` = %s"
                has_type = True
            sql = f"SELECT {sel_sql} FROM `{learn_table}` WHERE `{target_col}` = %s{type_filter_sql} LIMIT 1"

            bundle = {
                "sql": sql,
                "has_type": has_type,
                "uses_content_type": uses_content_type,
                "dedup_type": dedup_type,
                "key_mode": key_mode,
                "target_col": target_col,
                "learn_table": learn_table,
            }
        except Exception as exc:
            logger.debug("[_ensure_file_dup_sql_bundle] %s", exc)
            bundle = None
        try:
            self._file_dup_sql_bundle_v = bundle
        except Exception:
            pass
        return bundle

    async def _try_duplicate_by_sys_file_nm_like(
        self,
        *,
        learn_table: str,
        file_url: str,
        post_url: str,
        effective_job_id: str,
        has_type: bool,
        dedup_type: str,
        new_cate1: Optional[str] = None,
        new_cate2: Optional[str] = None,
        author_meta: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Disabled: speed-first file crawl does not run attachment-key LIKE duplicate checks."""
        return False
    async def _try_duplicate_by_attachment_key_like(
        self,
        *,
        learn_table: str,
        file_url: str,
        post_url: str,
        effective_job_id: str,
        has_type: bool,
        dedup_type: str,
        new_cate1: Optional[str] = None,
        new_cate2: Optional[str] = None,
        author_meta: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Disabled: speed-first file crawl does not run attachment-key LIKE duplicate checks."""
        return False
    async def _apply_file_dup_query(
        self,
        file_url: str,
        post_url: str,
        effective_job_id: str,
        dup_bundle: Optional[Dict[str, Any]],
        *,
        new_cate1: Optional[str] = None,
        new_cate2: Optional[str] = None,
        author_meta: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Check an existing LEARN_LIST row before scheduling a file download."""
        if not dup_bundle:
            if (
                getattr(self, "is_attachment_file_crawl_workflow", False)
                and str(os.getenv("FILE_CRAWL_DUP_CHECK_FAIL_OPEN_ON_BUNDLE_MISS", "0") or "1").strip().lower()
                in ("1", "true", "yes", "on")
            ):
                logger.debug(
                    '[Duplicate][file] duplicate SQL bundle unavailable; continuing fail-open | job_id=%s url=%s post=%s',
                    effective_job_id,
                    (file_url or "")[:180],
                    (post_url or "")[:180],
                )
                return False
            return await self._is_file_duplicated_in_db(
                file_url,
                post_url,
                effective_job_id,
                new_cate1=new_cate1,
                new_cate2=new_cate2,
                author_meta=author_meta,
            )
        try:
            from db.mysql_db_config import mysql_execute_query

            if dup_bundle.get("key_mode") == "canon":
                key = (
                    canonicalize_attachment_url_for_learn_list(file_url)
                    or canonicalize_url_for_dedup(file_url)
                    or file_url
                )
            else:
                key = file_url
            if dup_bundle.get("uses_content_type"):
                params = (key, "file")
            elif dup_bundle.get("has_type"):
                params = (key, dup_bundle.get("dedup_type") or "file")
            else:
                params = (key,)
            duplicate_started = time.perf_counter()
            rows = await mysql_execute_query(
                dup_bundle["sql"],
                params,
                fetch=True,
                dbname=self.db_name,
                op_name=f"file_prequeue_url_duplicate_lookup:job={effective_job_id or '-'}",
            )
            self._record_file_db_operation(
                operation="file_prequeue_url_duplicate_lookup",
                elapsed_sec=time.perf_counter() - duplicate_started,
            )
            if rows and isinstance(rows[0], dict):
                learn_table = dup_bundle.get("learn_table") or ""
                try:
                    from db.mariadb_save_update import learn_list_file_dup_debug_log

                    _tn = "-"
                    try:
                        _ct = asyncio.current_task()
                        if _ct is not None:
                            _tn = getattr(_ct, "get_name", lambda: "-")() or "-"
                    except Exception:
                        pass
                    learn_list_file_dup_debug_log(
                        "dup_query HIT | job_id=%s task=%s table=%s row_id=%s key=%s post=%s",
                        effective_job_id,
                        _tn,
                        learn_table,
                        (rows[0] or {}).get("id"),
                        (key[:220] if isinstance(key, str) else str(key))[:220],
                        (post_url or "")[:120],
                    )
                except Exception:
                    pass
                if not learn_table:
                    await self._notify_file_dup_scan(post_url, effective_job_id)
                    return True
                return await self._resolve_file_learn_list_duplicate_row(
                    row=rows[0],
                    learn_table=str(learn_table),
                    post_url=post_url,
                    effective_job_id=effective_job_id,
                    new_cate1=new_cate1,
                    new_cate2=new_cate2,
                    author_meta=author_meta,
                )
            elif dup_bundle.get("learn_table"):
                sys_dup = await self._try_duplicate_by_attachment_key_like(
                    learn_table=str(dup_bundle.get("learn_table") or ""),
                    file_url=file_url,
                    post_url=post_url,
                    effective_job_id=effective_job_id,
                    has_type=bool(dup_bundle.get("has_type")),
                    dedup_type=str(dup_bundle.get("dedup_type") or "file"),
                    new_cate1=new_cate1,
                    new_cate2=new_cate2,
                    author_meta=author_meta,
                )
                if sys_dup:
                    return True
            try:
                from db.mariadb_save_update import learn_list_file_dup_debug_log

                _tn2 = "-"
                try:
                    _ct2 = asyncio.current_task()
                    if _ct2 is not None:
                        _tn2 = getattr(_ct2, "get_name", lambda: "-")() or "-"
                except Exception:
                    pass
                learn_list_file_dup_debug_log(
                    "dup_query MISS | job_id=%s task=%s table=%s key=%s post=%s",
                    effective_job_id,
                    _tn2,
                    dup_bundle.get("learn_table") or "",
                    (key[:220] if isinstance(key, str) else str(key))[:220],
                    (post_url or "")[:120],
                )
            except Exception:
                pass
        except Exception as exc:
            logger.debug("[_apply_file_dup_query] %s", exc)
        return False

    @log_calls
    def _build_file_meta(
        self,
        file_url,
        file_name,
        post_url,
        board_url,
        reg_date,
        author,
        department,
        author_kind,
        author_raw,
        department_raw,
        contact_phone,
        view_count,
        sync_after_download,
        effective_job_id,
        detail_cates: Optional[Tuple[Optional[str], Optional[str]]] = None,
        post_title: Optional[str] = None,
        source_download_name: Optional[str] = None,
    ):
        default_sync_after_download = bool(getattr(self, "sync_after_download", False))
        if getattr(self, "is_attachment_file_crawl_workflow", False):
            default_sync_after_download = True
        sync_val = sync_after_download if sync_after_download is not None else default_sync_after_download
        workflow_cate1 = getattr(self, "cate1", None)
        workflow_cate2 = getattr(self, "cate2", None)
        if detail_cates is not None:
            meta_cate1 = detail_cates[0] or workflow_cate1
            meta_cate2 = detail_cates[1] or workflow_cate2
        else:
            meta_cate1 = workflow_cate1
            meta_cate2 = workflow_cate2
        content_author_value = author or department

        payload = {
            "url": file_url,
            "name": file_name or "attachment",
            "source_page": post_url,
            "source_url": post_url,
            "post_title": post_title or "",
            "attachment_name": file_name or "attachment",
            "download_name": source_download_name or "",
            "request_cookies": getattr(self, "_category_sync_request_cookies", None) or {},
            "reg_date": reg_date or "",
            "author": author,
            "content_author": content_author_value,
            "department": department,
            "author_kind": author_kind,
            "author_raw": author_raw,
            "department_raw": department_raw,
            "contact_phone": contact_phone,
            "view_count": view_count,
            "original_meta": {
                "post_url": post_url,
                "board_url": board_url,
                "source_page": post_url,
                "source_url": post_url,
                "post_title": post_title or "",
                "reg_date": reg_date,
                "author": author,
                "content_author": content_author_value,
                "department": department,
                "author_kind": author_kind,
                "author_raw": author_raw,
                "department_raw": department_raw,
                "contact_phone": contact_phone,
                "view_count": view_count,
                "attachment_name": file_name,
                "download_name": source_download_name or "",
                "job_id": effective_job_id,
                "request_cookies": getattr(self, "_category_sync_request_cookies", None) or {},
                "board_cate1_comment": meta_cate1,
                "board_cate2": meta_cate2,
                "cate1": meta_cate1,
                "cate2": meta_cate2,
                "store_cate1": meta_cate1,
                "store_cate2": meta_cate2,
            },
            "job_id": effective_job_id,
            "chat_bot_id": getattr(self, "chat_bot_id", None),
            "db_name": getattr(self, "db_name", None),
            "domain": urlparse(post_url).netloc if post_url else None,
            "server_domain": getattr(self, "server_domain", None),
            "access_url": getattr(self, "access_url", None),
            "unique_id": getattr(self, "unique_id", None),
            "cate1": meta_cate1 or "",
            "cate2": meta_cate2 or "",
            "memo": getattr(self, "memo", None),
            "skip_study_worker": bool(getattr(self, "file_pipeline_skip_learning", False)),
            "sync_after_download": sync_val,
            # True: disk save and LEARN_LIST save completed; downstream save/study can proceed.
            "defer_save_batch_until_learn_list": True,
        }
        if not str(file_url or "").strip():
            logger.debug(
                "[FileCrawl][enqueue] empty file_url while building meta; skip candidate | post=%s name=%s job_id=%s",
                (post_url or "")[:220],
                file_name,
                effective_job_id,
            )
            return None
        _filename_debug_log(
            "build_file_meta",
            file_url=file_url,
            file_name=file_name,
            payload_name=payload.get("name"),
            source_page=post_url,
            attachment_name=((payload.get("original_meta") or {}).get("attachment_name")),
            reg_date=reg_date,
        )
        if _content_author_debug_enabled():
            logger.debug(
                "[ContentAuthorDebug][build_file_meta] job_id=%s file_url=%s post=%s file_name=%r author=%r content_author=%r department=%r kind=%r raw=%r original_author=%r",
                effective_job_id,
                (file_url or "")[:220],
                (post_url or "")[:220],
                _content_author_debug_value(file_name),
                _content_author_debug_value(payload.get("author")),
                _content_author_debug_value(payload.get("content_author")),
                _content_author_debug_value(payload.get("department")),
                _content_author_debug_value(payload.get("author_kind")),
                _content_author_debug_value(payload.get("author_raw")),
                _content_author_debug_value((payload.get("original_meta") or {}).get("content_author")),
            )
        return payload

    @log_calls
    def _resolve_path_for_learning_file(self, path: str) -> Optional[str]:
        """Resolve a saved file path before learning.

        Downloaded files may be referenced by absolute paths, paths relative to the
        project root, or fileupload storage paths. This helper tries those locations
        before reporting an empty extract.
        """
        if not path or not str(path).strip():
            return None
        raw = str(path).strip()
        candidates: List[str] = []
        if os.path.isabs(raw):
            candidates.append(os.path.normpath(raw))
        else:
            candidates.append(os.path.normpath(os.path.abspath(raw)))
            candidates.append(os.path.normpath(os.path.join(_board_project_root(), raw.replace("/", os.sep))))
            try:
                from config.settings import get_fileupload_root

                uuid_part = str(getattr(self, "chat_bot_id", "") or "").split("-")[-1]
                base = os.path.normpath(get_fileupload_root())
                try:
                    from config.settings import get_storage_domain_for_db_name

                    storage_domain = get_storage_domain_for_db_name(getattr(self, "db_name", None))
                    candidates.append(os.path.join(base, storage_domain, uuid_part, os.path.basename(raw)))
                except Exception:
                    pass
                candidates.append(os.path.join(base, uuid_part, os.path.basename(raw)))
                if "fileupload" in raw.replace("\\", "/").lower():
                    tail = raw.replace("\\", "/").split("fileupload", 1)[-1].strip("/")
                    if tail:
                        candidates.append(os.path.normpath(os.path.join(base, tail)))
            except Exception:
                pass
        seen: set[str] = set()
        for c in candidates:
            if not c or c in seen:
                continue
            seen.add(c)
            if os.path.isfile(c):
                return c
        logger.debug(
            "[file_crawl][board][file] learning file not found | tried=%s original=%r",
            candidates[:6],
            raw[:200],
        )
        return None

    @log_calls
    async def _extract_text_from_saved_file_for_learning(self, path: str) -> str:
        """Resolve the saved file path and extract text for learning."""
        resolved = self._resolve_path_for_learning_file(path)
        if not resolved:
            return ""
        path = resolved
        try:
            failures = getattr(self, "_file_text_extract_failure_reasons", None)
            if isinstance(failures, dict):
                failures.pop(path, None)
        except Exception:
            pass
        try:
            await wait_for_file_ready(path, timeout_sec=30.0, check_partial_siblings=False)
        except Exception as exc:
            logger.debug(
                "[board][file] learning file not ready | path=%s err=%s",
                path,
                exc,
            )
            return ""
        ext = os.path.splitext(path)[1].lower()
        try:
            from edu.learn_file_plain_text import (
                LEARN_PLAIN_TEXT_EXTS,
                extract_plain_text_like_learn_modules,
            )

            if ext and ext not in LEARN_PLAIN_TEXT_EXTS:
                logger.debug(
                    "[file_crawl][board][file] no extractor for extension | ext=%s path=%s",
                    ext,
                    path,
                )
                return ""
            extract_timeout_sec: float | None = None
            if ext == ".pdf":
                guarded_timeout_sec = _file_pdf_text_extract_timeout_sec(path)
            else:
                try:
                    guarded_timeout_sec = float(
                        os.getenv("FILE_TEXT_EXTRACT_TIMEOUT_SEC", "1800") or "1800"
                    )
                except Exception:
                    guarded_timeout_sec = 1800.0
            guarded_timeout_sec = max(0.0, min(guarded_timeout_sec, 24 * 3600.0))
            if guarded_timeout_sec > 0:
                buffer_sec = min(60.0, max(5.0, guarded_timeout_sec * 0.1))
                extract_timeout_sec = max(5.0, guarded_timeout_sec - buffer_sec)
            extracted_text = await extract_plain_text_like_learn_modules(
                path,
                personal_info_filter="N",
                timeout_sec=extract_timeout_sec,
            )
            try:
                from edu.document_markdown_fallback import (
                    extract_structured_markdown,
                    should_use_structured_markdown_fallback,
                )

                if should_use_structured_markdown_fallback(path, extracted_text):
                    markdown = await extract_structured_markdown(path)
                    if len((markdown or "").strip()) > len((extracted_text or "").strip()):
                        logger.debug(
                            "[DocFallback] structured markdown selected | path=%s ext=%s plain_chars=%s markdown_chars=%s",
                            path,
                            ext,
                            len((extracted_text or "").strip()),
                            len((markdown or "").strip()),
                        )
                        return markdown
                    if markdown:
                        logger.debug(
                            "[DocFallback] structured markdown ignored (not richer) | path=%s ext=%s plain_chars=%s markdown_chars=%s",
                            path,
                            ext,
                            len((extracted_text or "").strip()),
                            len((markdown or "").strip()),
                        )
            except Exception as fallback_exc:
                logger.debug(
                    "[DocFallback] structured markdown fallback unavailable | path=%s ext=%s err=%s",
                    path,
                    ext,
                    fallback_exc,
                )
            return extracted_text
        except TimeoutError as exc:
            try:
                failures = getattr(self, "_file_text_extract_failure_reasons", None)
                if not isinstance(failures, dict):
                    failures = {}
                    self._file_text_extract_failure_reasons = failures
                failures[path] = "file_text_extract_timeout"
            except Exception:
                pass
            logger.error("[BoardFile] text extract timed out | path=%s ext=%s err=%s", path, ext, exc)
            return ""
        except Exception as e:
            logger.error(
                "[BoardFile] text extract failed | path=%s ext=%s err=%s",
                path,
                ext,
                e,
            )
            return ""

    async def _extract_text_from_saved_file_for_learning_guarded(
        self,
        path: str,
        *,
        url: str = "",
        file_name: str = "",
    ) -> str:
        """Run file text extraction with heartbeat logs and a bounded timeout."""
        ext = os.path.splitext(path or "")[1].lower()
        timeout_env = "FILE_PDF_TEXT_EXTRACT_TIMEOUT_SEC" if ext == ".pdf" else "FILE_TEXT_EXTRACT_TIMEOUT_SEC"
        timeout_default = "180" if ext == ".pdf" else "1800"
        if ext == ".pdf":
            try:
                size = os.path.getsize(path) if path and os.path.isfile(path) else 0
            except Exception:
                size = 0
            logger.debug(
                "[PDFDebug][guarded_extract.start] job_id=%s url=%s file=%s path=%s size=%s timeout_env=%s heartbeat_env=%s",
                getattr(self, "job_id", None),
                (url or "")[:180],
                (file_name or os.path.basename(path or ""))[:160],
                (path or "")[:260],
                size,
                os.getenv(timeout_env, timeout_default),
                os.getenv("FILE_TEXT_EXTRACT_HEARTBEAT_SEC"),
            )
        if ext == ".pdf":
            timeout_sec = _file_pdf_text_extract_timeout_sec(path)
        else:
            try:
                timeout_sec = float(os.getenv(timeout_env, timeout_default) or timeout_default)
            except Exception:
                timeout_sec = 1800.0
        try:
            heartbeat_sec = float(os.getenv("FILE_TEXT_EXTRACT_HEARTBEAT_SEC", "60") or "60")
        except Exception:
            heartbeat_sec = 60.0
        timeout_sec = max(0.0, min(timeout_sec, 24 * 3600.0))
        heartbeat_sec = max(5.0, min(heartbeat_sec, 3600.0))

        started = time.perf_counter()
        last_heartbeat = started
        task = asyncio.create_task(self._extract_text_from_saved_file_for_learning(path))
        while True:
            elapsed = time.perf_counter() - started
            if timeout_sec > 0 and elapsed >= timeout_sec:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                try:
                    failures = getattr(self, "_file_text_extract_failure_reasons", None)
                    if not isinstance(failures, dict):
                        failures = {}
                        self._file_text_extract_failure_reasons = failures
                    failures[self._resolve_path_for_learning_file(path) or path] = "file_text_extract_timeout"
                except Exception:
                    pass
                logger.error(
                    "[FileTextExtractTimeout] timeout=%ss elapsed=%sms job_id=%s url=%s file=%s path=%s",
                    int(timeout_sec),
                    int(elapsed * 1000),
                    getattr(self, "job_id", None),
                    (url or "")[:180],
                    (file_name or os.path.basename(path or ""))[:160],
                    (path or "")[:260],
                )
                return ""

            wait_slice = heartbeat_sec
            if timeout_sec > 0:
                wait_slice = min(wait_slice, max(0.1, timeout_sec - elapsed))
            try:
                result = await asyncio.wait_for(asyncio.shield(task), timeout=wait_slice)
                if ext == ".pdf":
                    logger.debug(
                        "[PDFDebug][guarded_extract.done] job_id=%s url=%s file=%s chars=%s preview=%s",
                        getattr(self, "job_id", None),
                        (url or "")[:180],
                        (file_name or os.path.basename(path or ""))[:160],
                        len((result or "").strip()),
                        " ".join(str(result or "").split())[:160],
                    )
                return result
            except asyncio.TimeoutError:
                now = time.perf_counter()
                if now - last_heartbeat >= heartbeat_sec:
                    last_heartbeat = now
                    logger.debug(
                        "[FileTextExtractHeartbeat] elapsed=%sms timeout=%ss job_id=%s url=%s file=%s path=%s",
                        int((now - started) * 1000),
                        int(timeout_sec) if timeout_sec > 0 else 0,
                        getattr(self, "job_id", None),
                        (url or "")[:180],
                        (file_name or os.path.basename(path or ""))[:160],
                        (path or "")[:260],
                    )
                    if ext == ".pdf":
                        logger.debug(
                            "[PDFDebug][guarded_extract.heartbeat] elapsed_ms=%s timeout=%ss job_id=%s file=%s",
                            int((now - started) * 1000),
                            int(timeout_sec) if timeout_sec > 0 else 0,
                            getattr(self, "job_id", None),
                            (file_name or os.path.basename(path or ""))[:160],
                        )

    @log_calls
    async def _skip_file_selection_for_rrn_pattern(
        self,
        *,
        extracted_text: str,
        url: str,
        file_path: str,
        save_key: str,
    ) -> bool:
        if not learning_blocked_by_rrn_pattern(extracted_text):
            return False
        if str(os.getenv("FILE_CRAWL_RRN_MASK_INSTEAD_OF_BLOCK", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}:
            logger.debug(
                "[file_crawl][board][file] rrn pattern detected before selection; continue with masking | job_id=%s url=%s path=%s",
                getattr(self, "job_id", None),
                (url or "")[:200],
                (file_path or "")[:260],
            )
            return False
        logger.debug(
            "[file_crawl][board][file] rrn pattern detected, exclude before selection | job_id=%s url=%s path=%s",
            getattr(self, "job_id", None),
            (url or "")[:200],
            (file_path or "")[:260],
        )
        await self._mark_save_skipped(url=save_key)
        try:
            await self._record_study_skip(
                reason="learning_blocked_by_rrn_pattern",
                url=url,
                path=file_path,
                detail="file text contains resident-registration-number-like pattern",
            )
        except Exception:
            pass
        await self._mark_study_done(url=save_key, outcome="skipped")
        if self.progress_callback:
            try:
                self.progress_callback(self.get_stats())
            except Exception:
                pass
        return True

    @log_calls
    async def _rollback_saved_selection_for_rrn_pattern(self, *, save_key: str) -> None:
        # A later learning-policy rejection does not undo a file that was
        # already verified in storage. Keep the physical-storage counter
        # immutable; only the study result becomes skipped.
        if save_key:
            logger.info(
                "[FileCounterTrace][storage_preserved_after_learning_skip] job_id=%s key=%s",
                getattr(self, "job_id", None),
                (save_key or "")[:220],
            )

    async def await_background_completion(self) -> None:
        """Wait until every saved file has completed its external learning request."""
        await self._wait_for_file_local_finalize_drain()
        await self._wait_for_file_save_drain()
        await self._wait_for_file_learning_request_drain()
        if self.file_learning_request_dispatch_complete():
            return
        try:
            stats = self.get_stats() if hasattr(self, "get_stats") else dict(getattr(self, "stats", {}) or {})
        except Exception:
            stats = dict(getattr(self, "stats", {}) or {})
        try:
            save_count = int(stats.get("save_count", 0) or 0)
            study_success = int(stats.get("file_study_success_count", stats.get("study_success_count", 0)) or 0)
            study_failed = int(stats.get("file_study_failed_count", stats.get("study_failed_count", 0)) or 0)
        except Exception:
            save_count = 0
            study_success = 0
            study_failed = 0
        if save_count > 0 and (study_success + study_failed) >= save_count:
            await self._reconcile_file_study_counts_from_learn_list()
            return

        pending: List[asyncio.Task] = [
            task
            for task in (
                set(getattr(self, "_file_progress_dispatch_tasks", set()) or set())
                | set(getattr(self, "_file_progress_tasks", set()) or set())
            )
            if isinstance(task, asyncio.Task) and not task.done()
        ]
        worker_task = getattr(self, "_file_worker_task", None)
        if isinstance(worker_task, asyncio.Task) and not worker_task.done():
            pending.append(worker_task)
        if not pending:
            await self._reconcile_file_study_counts_from_learn_list()
            return
        results = await asyncio.gather(*pending, return_exceptions=True)
        for r in results:
            if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                try:
                    logger.debug(
                        "[file_crawl][board][file] await_background_completion task result | job_id=%s err=%s",
                        getattr(self, "job_id", None),
                        r,
                    )
                except Exception:
                    pass
        await self._reconcile_file_study_counts_from_learn_list()

    @log_calls
    async def _flush_and_drain_file_queues_best_effort(self) -> None:
        """Discard queued and buffered file work without blocking during hard shutdown."""
        qs = getattr(self, "_file_job_queues", None)
        if not qs:
            return
        jid = str(self.job_id or "unknown").strip() or "unknown"
        try:
            if bool(getattr(self, "use_global_pool", False)):
                from core.crawler.global_pool import get_global_worker_pool

                get_global_worker_pool().disable_scan(jid)
        except Exception:
            pass
        try:
            counts = await qs.drain()
            logger.info(
                "[FileCrawlTrace][stop_queue_drained] job_id=%s counts=%s",
                self.job_id,
                counts,
            )
        except Exception as exc:
            logger.debug(
                "[file_crawl][board][file] queue drain skipped | job_id=%s err=%s",
                self.job_id,
                exc,
            )

    @log_calls
    async def _shutdown_file_pipeline(self, *, graceful: bool = False) -> None:
        """Shutdown file pipeline workers and release related resources."""
        from core.crawler.global_pool import get_global_worker_pool

        # Graceful finalize already waits for queue joins; do not discard late arrivals.
        if not graceful:
            await self._flush_and_drain_file_queues_best_effort()
        await self._cancel_file_parallel_learn_tasks()

        watchdog_task = getattr(self, "_file_queue_watchdog_task", None)
        if watchdog_task:
            try:
                watchdog_task.cancel()
                await watchdog_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._file_queue_watchdog_task = None
        try:
            await self._stop_file_local_finalize_workers(graceful=graceful)
        except Exception:
            logger.exception(
                "[FileCrawlTrace][local_finalize_shutdown_failed] job_id=%s graceful=%s",
                getattr(self, "job_id", None),
                graceful,
            )
        try:
            await self._stop_file_progress_workers()
        except Exception:
            logger.exception(
                "[FileCrawlTrace][save_workers_shutdown_failed] job_id=%s graceful=%s",
                getattr(self, "job_id", None),
                graceful,
            )
        try:
            await self._stop_file_learning_request_workers(graceful=graceful)
        except Exception:
            logger.exception(
                "[FileLearnRequest][workers_shutdown_failed] job_id=%s graceful=%s",
                getattr(self, "job_id", None),
                graceful,
            )
        if self._file_worker_manager:
            manager = self._file_worker_manager
            try:
                await manager.stop(
                    graceful=graceful,
                    stop_scan=True,
                    stop_collection=True,
                    stop_download=True,
                    stop_study=True,
                    stop_flush_task=True,
                    close_browser=not graceful,
                    stop_playwright=not graceful,
                    reset_deduplicator=True,
                )
            except Exception:
                pass
            if graceful:
                self._file_worker_manager_cleanup_pending = manager
            else:
                self._file_worker_manager_cleanup_pending = None
            self._file_worker_manager = None
            self._file_worker_task = None
        if bool(getattr(self, "use_global_pool", False)) and self._file_job_queues:
            try:
                pool = get_global_worker_pool()
                jid = self.job_id or "unknown"
                await pool.unregister_job(jid)
                await pool.close_resources_if_no_jobs()
            except Exception:
                pass
        self._file_job_queues = None

    async def _cancel_file_parallel_learn_tasks(self) -> None:
        tasks = getattr(self, "_file_parallel_learn_tasks", None)
        if not isinstance(tasks, set) or not tasks:
            return
        to_cancel = [t for t in list(tasks) if isinstance(t, asyncio.Task) and not t.done()]
        for task in to_cancel:
            try:
                task.cancel()
            except Exception:
                pass
        if to_cancel:
            try:
                await asyncio.gather(*to_cancel, return_exceptions=True)
            except Exception:
                pass
        try:
            tasks.clear()
        except Exception:
            pass

    @log_calls
    async def _wait_for_save_done(self, from_count: int, needed: int, *, timeout_sec: float = 60.0) -> None:
        """Wait until save_done reaches the expected count before continuing, best-effort."""
        if not needed or needed <= 0:
            return
        try:
            t0 = time.monotonic()
        except Exception:
            t0 = 0.0
        target = int(from_count or 0) + int(needed or 0)
        while True:
            raw_final = str(getattr(self, "final_status", "") or "").strip().lower()
            if raw_final in {"error", "failed", "fail", "exception"}:
                logger.debug(
                    "[file_crawl][board][file] wait_for_save_done interrupted by error status | job_id=%s from=%s needed=%s",
                    self.job_id,
                    from_count,
                    needed,
                )
                return
            if getattr(self, "_hard_stop", False) or self.stop_event.is_set():
                logger.debug(
                    "[file_crawl][board][file] wait_for_save_done interrupted by stop | job_id=%s from=%s needed=%s",
                    self.job_id,
                    from_count,
                    needed,
                )
                return
            try:
                async with self._stats_lock:
                    cur = int(self.stats.get("save_done_count", 0) or 0)
            except Exception:
                cur = 0
            if cur >= target:
                return
            try:
                if time.monotonic() - t0 >= float(timeout_sec or 60.0):
                    logger.debug(
                        "[file_crawl][board][file] wait_for_save_done timeout | job_id=%s from=%s needed=%s cur=%s",
                        self.job_id,
                        from_count,
                        needed,
                        cur,
                    )
                    return
            except Exception:
                pass
            await asyncio.sleep(0.5)

    @log_calls
    async def _wait_for_study_done(self, from_count: int, needed: int, *, timeout_sec: float = 300.0) -> None:
        """Wait until file study tasks finish before marking the workflow done."""
        if not needed or needed <= 0:
            return
        try:
            t0 = time.monotonic()
        except Exception:
            t0 = 0.0
        target = int(from_count or 0) + int(needed or 0)
        while True:
            raw_final = str(getattr(self, "final_status", "") or "").strip().lower()
            if raw_final in {"error", "failed", "fail", "exception"}:
                logger.debug(
                    "[file_crawl][board][file] wait_for_study_done interrupted by error status | job_id=%s from=%s needed=%s",
                    self.job_id,
                    from_count,
                    needed,
                )
                return
            if getattr(self, "_hard_stop", False) or self.stop_event.is_set():
                logger.debug(
                    "[file_crawl][board][file] wait_for_study_done interrupted by stop | job_id=%s from=%s needed=%s",
                    self.job_id,
                    from_count,
                    needed,
                )
                return
            try:
                async with self._stats_lock:
                    cur_done = int(
                        (self.stats or {}).get(
                            "file_study_done_count",
                            (self.stats or {}).get("study_done_count", 0),
                        )
                        or 0
                    )
                    cur_success = int(
                        (self.stats or {}).get(
                            "file_study_success_count",
                            (self.stats or {}).get("study_success_count", 0),
                        )
                        or 0
                    )
            except Exception:
                cur_done = 0
                cur_success = 0
            if cur_done >= target:
                return
            try:
                if time.monotonic() - t0 >= float(timeout_sec or 300.0):
                    logger.debug(
                        "[file_crawl][board][file] wait_for_study_done timeout | job_id=%s from=%s needed=%s cur_done=%s cur_success=%s",
                        self.job_id,
                        from_count,
                        needed,
                        cur_done,
                        cur_success,
                    )
                    return
            except Exception:
                pass
            await asyncio.sleep(0.5)

    async def _ensure_learn_list_row_for_file_save(
        self,
        *,
        info: Dict[str, Any],
        file_path: str,
        file_name: str,
        file_size: int,
    ) -> Optional[int]:
        """Ensure a LEARN_LIST row exists for a saved file.

        File crawling first stores one LEARN_LIST row through insert_into_learn_list
        with status=N. When DB saving is disabled, return -1 and keep counts local.
        Return None when the DB row cannot be created.
        """
        if not getattr(self, "enable_db_save", True):
            logger.debug(
                "[FileProbeDebug][save.skip_db_disabled] job_id=%s url=%s file=%s",
                getattr(self, "job_id", None),
                (info.get("url") or "")[:220],
                file_name,
            )
            return -1
        for _k in ("db_id", "learn_list_id"):
            _raw = info.get(_k)
            if _raw is not None and str(_raw).strip():
                try:
                    _vid = int(_raw)
                    if _vid > 0:
                        return _vid
                except Exception:
                    pass
        cid = getattr(self, "chat_bot_id", None)
        dbn = getattr(self, "db_name", None)
        url = (info.get("url") or "").strip()
        if not (cid and dbn and url):
            logger.debug(
                "[file_crawl][board][file] LEARN_LIST lookup skipped: missing chat_bot_id/db_name/url | job_id=%s",
                getattr(self, "job_id", None),
            )
            return None
        from db.mariadb_save_update import (
            coalesce_learn_list_cates,
            get_account_identifier_from_chatbot_setup,
            get_learn_list_table_name,
            ensure_learn_list_standard_columns,
            insert_into_learn_list,
            learn_list_file_dup_debug_log,
            learn_list_merge_cate_on_duplicate_row,
        )
        from db.mysql_db_config import mysql_execute_query

        original_meta = info.get("original_meta") if isinstance(info.get("original_meta"), dict) else {}
        info_author = (
            info.get("author")
            or info.get("content_author")
            or original_meta.get("author")
            or original_meta.get("content_author")
        )
        info_department = info.get("department") or original_meta.get("department")
        info_content_author = info.get("content_author") or info_author or info_department
        info_author_kind = info.get("author_kind") or original_meta.get("author_kind")
        info_author_raw = info.get("author_raw") or original_meta.get("author_raw")
        info_department_raw = info.get("department_raw") or original_meta.get("department_raw")
        info_created_at = (
            info.get("file_created_at")
            or info.get("content_created_at")
            or info.get("created_at")
            or original_meta.get("file_created_at")
            or original_meta.get("content_created_at")
            or original_meta.get("created_at")
            or info.get("reg_date")
            or original_meta.get("reg_date")
        )

        file_info: Dict[str, Any] = {
            "url": url,
            "name": file_name,
            "source_filename": file_name,
            # This exact value is also passed to PG learning.  Make it the
            # highest-priority Maria subject candidate so both stores share
            # one file identity even when nested metadata was normalized.
            "display_name": file_name,
            "attachment_name": file_name,
            "saved_filename": info.get("saved_filename") or os.path.basename(file_path),
            "storage_filename": info.get("storage_filename") or os.path.basename(file_path),
            "size": file_size if file_size else (info.get("size") or 0),
            "file_path": file_path,
            "local_path": info.get("local_path") or file_path,
            "author": info_author,
            "content_author": info_content_author,
            "department": info_department,
            "author_kind": info_author_kind,
            "author_raw": info_author_raw,
            "department_raw": info_department_raw,
            "source_url": _resolve_file_detail_source_url(info),
            "reg_date": info.get("reg_date") or original_meta.get("reg_date"),
            "file_created_at": info_created_at,
            "content_created_at": info_created_at,
            "created_at": info_created_at,
            "original_meta": original_meta,
            "job_id": getattr(self, "job_id", None),
            "_category_sync_request_cookies": getattr(self, "_category_sync_request_cookies", None),
        }
        simhash_decimal = str(info.get("simhash_decimal") or "").strip()
        if simhash_decimal:
            # SimHash is generated after text extraction and stored on the
            # progress item.  The MariaDB helper receives file_info, so carry
            # the value across this payload boundary explicitly.
            file_info["simhash_decimal"] = simhash_decimal
        if _content_author_debug_enabled():
            logger.debug(
                "[ContentAuthorDebug][ensure.payload] job_id=%s url=%s file=%r info_author=%r info_content_author=%r file_author=%r file_content_author=%r department=%r kind=%r raw=%r original_author=%r source_url=%s",
                getattr(self, "job_id", None),
                (url or "")[:220],
                _content_author_debug_value(file_name),
                _content_author_debug_value(info.get("author")),
                _content_author_debug_value(info.get("content_author")),
                _content_author_debug_value(file_info.get("author")),
                _content_author_debug_value(file_info.get("content_author")),
                _content_author_debug_value(file_info.get("department")),
                _content_author_debug_value(file_info.get("author_kind")),
                _content_author_debug_value(file_info.get("author_raw")),
                _content_author_debug_value(original_meta.get("content_author") or original_meta.get("author")),
                (str(file_info.get("source_url") or "")[:220]),
            )
        try:
            c1, c2 = coalesce_learn_list_cates(info)
            if not c1:
                _w = getattr(self, "cate1", None)
                if _w is not None and str(_w).strip():
                    c1 = str(_w).strip()
            if not c2:
                _w = getattr(self, "cate2", None)
                if _w is not None and str(_w).strip():
                    c2 = str(_w).strip()
            file_info["cate1"] = c1
            file_info["cate2"] = c2
        except Exception:
            pass

        async def _mark_duplicate(row_id: Any, status: str) -> int:
            normalized_status = str(status or "").strip().upper()
            file_info["learn_list_duplicate"] = True
            file_info["learn_list_existing_status"] = normalized_status or None
            try:
                info["learn_list_duplicate"] = True
                info["learn_list_existing_status"] = normalized_status or None
                info["learn_list_reused_learned"] = normalized_status == "Y"
            except Exception:
                pass
            try:
                return int(row_id or -1)
            except Exception:
                return -1

        async def _insert_file_row(*, existing_row_id: Optional[int] = None) -> Any:
            sem = _get_file_learn_list_insert_semaphore()
            gate_started = time.perf_counter()
            async with sem:
                gate_wait_ms = int((time.perf_counter() - gate_started) * 1000)
                operation_prefix = (
                    "file_learn_list_update"
                    if existing_row_id is not None
                    else "file_learn_list_insert"
                )
                self._record_file_db_operation(
                    operation=f"{operation_prefix}_queue_wait",
                    elapsed_sec=gate_wait_ms / 1000.0,
                )
                if gate_wait_ms >= 1000:
                    logger.debug(
                        "[Bottleneck][learn_list_insert_gate] waited=%sms concurrency=%s job_id=%s url=%s",
                        gate_wait_ms,
                        _file_learn_list_insert_semaphore_size,
                        getattr(self, "job_id", None),
                        (url or "")[:160],
                    )
                operation_timeout_sec = _file_learn_list_db_operation_timeout_sec()
                insert_started = time.perf_counter()

                def _record_insert_stage(stage: str, elapsed_sec: float) -> None:
                    self._record_file_db_operation(
                        operation=f"{operation_prefix}_{stage}",
                        elapsed_sec=elapsed_sec,
                    )

                try:
                    row_id = await asyncio.wait_for(
                        insert_into_learn_list(
                            chat_bot_id=str(cid),
                            db_name=str(dbn),
                            file_info=file_info,
                            existing_row_id=existing_row_id,
                            diagnostic_callback=_record_insert_stage,
                        ),
                        timeout=operation_timeout_sec,
                    )
                    content_url = str(file_info.get("_learn_list_content_url") or "").strip()
                    if content_url:
                        info["_learn_list_content_url"] = content_url
                    self._record_file_db_operation(
                        operation=(
                            "file_learn_list_update"
                            if existing_row_id is not None
                            else "file_learn_list_insert"
                        ),
                        elapsed_sec=time.perf_counter() - insert_started,
                    )
                except asyncio.TimeoutError:
                    info["_learn_list_ensure_failure_reason"] = "insert_timeout"
                    logger.error(
                        "[FilePersist][insert_timeout] job_id=%s db=%s timeout_sec=%.1f post_url=%s file_url=%s file=%s",
                        getattr(self, "job_id", None), str(dbn), operation_timeout_sec,
                        str(file_info.get("source_url") or "")[:220], str(url or "")[:220], str(file_name or "")[:160],
                    )
                    return None
                elapsed_sec = time.perf_counter() - insert_started
                if elapsed_sec >= 1.0:
                    logger.warning(
                        "[FilePersist][insert_slow] job_id=%s db=%s elapsed_sec=%.3f file=%s",
                        getattr(self, "job_id", None), str(dbn), elapsed_sec, str(file_name or "")[:160],
                    )
                return row_id
        row_id = await _insert_file_row()
        if _content_author_debug_enabled():
            logger.debug(
                "[ContentAuthorDebug][ensure.insert_result] job_id=%s row_id=%s url=%s file_author=%r file_content_author=%r duplicate=%s existing_status=%r",
                getattr(self, "job_id", None),
                row_id,
                (url or "")[:220],
                _content_author_debug_value(file_info.get("author")),
                _content_author_debug_value(file_info.get("content_author")),
                bool(file_info.get("learn_list_duplicate")),
                _content_author_debug_value(file_info.get("learn_list_existing_status")),
            )
        try:
            info["learn_list_duplicate"] = bool(file_info.get("learn_list_duplicate"))
            existing_status = str(file_info.get("learn_list_existing_status") or "").strip().upper()
            info["learn_list_existing_status"] = existing_status or None
            info["learn_list_reused_learned"] = bool(
                info.get("learn_list_duplicate")
            ) and existing_status == "Y"
        except Exception:
            pass
        try:
            learn_list_file_dup_debug_log(
                'file insert_into_learn_list result | job_id=%s row_id=%s learn_list_duplicate=%s canon_origin=%s raw_url=%s',
                file_info.get("job_id"),
                row_id,
                bool(file_info.get("learn_list_duplicate")),
                (canonicalize_attachment_url_for_learn_list(url) or canonicalize_url_for_dedup(url) or "")[:220],
                (url or "")[:220],
            )
        except Exception:
            pass
        try:
            parsed_row_id = int(row_id or 0)
        except Exception:
            parsed_row_id = 0
        if parsed_row_id > 0:
            await self._log_file_learn_list_row_debug(
                stage="after_insert_ensure",
                learn_list_id=parsed_row_id,
                url=url,
                file_info=file_info,
            )
            try:
                info_cate1_for_repair = str(file_info.get("cate1") or "").strip()
                info_cate2_for_repair = str(file_info.get("cate2") or "").strip()
                if info_cate1_for_repair or info_cate2_for_repair:
                    account_identifier_for_repair = await get_account_identifier_from_chatbot_setup(str(cid), str(dbn))
                    learn_table_for_repair = get_learn_list_table_name(account_identifier_for_repair)
                    repair_rows = await mysql_execute_query(
                        f"SELECT id, cate1, cate2 FROM `{learn_table_for_repair}` WHERE id = %s LIMIT 1",
                        (parsed_row_id,),
                        fetch=True,
                        dbname=str(dbn),
                    )
                    if repair_rows:
                        repaired = await learn_list_merge_cate_on_duplicate_row(
                            str(dbn),
                            learn_table_for_repair,
                            {"cate1", "cate2"},
                            repair_rows[0],
                            file_info,
                        )
                        logger.debug(
                            "[LearningTrace][board_file.cate_repair_after_insert] job_id=%s db=%s table=%s url=%s learn_list_id=%s input_cate=(%r,%r) repaired=%s",
                            getattr(self, "job_id", None),
                            dbn,
                            learn_table_for_repair,
                            (url or "")[:180],
                            parsed_row_id,
                            info_cate1_for_repair,
                            info_cate2_for_repair,
                            repaired,
                        )
                        if repaired:
                            await self._log_file_learn_list_row_debug(
                                stage="after_insert_cate_repair",
                                learn_list_id=parsed_row_id,
                                url=url,
                                file_info=file_info,
                            )
            except Exception as repair_exc:
                logger.error(
                    "[LearningError][board_file.cate_repair_after_insert] failed | job_id=%s db=%s url=%s learn_list_id=%s err=%s",
                    getattr(self, "job_id", None),
                    dbn,
                    (url or "")[:180],
                    parsed_row_id,
                    repair_exc,
                )
        if not row_id:
            return None
        try:
            return int(row_id)
        except Exception:
            return None

    def _register_file_parallel_learn_task(self, t: asyncio.Task) -> None:
        st = getattr(self, "_file_parallel_learn_tasks", None)
        if not isinstance(st, set):
            return
        st.add(t)

        def _rm(fut: asyncio.Task) -> None:
            try:
                st.discard(fut)
            except Exception:
                pass

        t.add_done_callback(_rm)

    async def _reset_duplicate_file_learning_for_relearn(
        self,
        *,
        learn_table: str,
        duplicate_ids: Set[int],
        current_id: Optional[int],
        file_name: str,
        file_size: int,
        info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Reset previously learned duplicate file rows and force relearn."""
        from db.mysql_db_config import mysql_execute_query

        normalized_ids: List[int] = []
        for raw_id in duplicate_ids or set():
            try:
                parsed_id = int(raw_id)
            except Exception:
                continue
            if parsed_id > 0 and parsed_id not in normalized_ids:
                normalized_ids.append(parsed_id)

        if not normalized_ids:
            return

        logger.debug(
            "[Duplicate][file] learned duplicate detected; reset old learning and relearn | job_id=%s current_id=%s duplicate_ids=%s subject=%s size=%s",
            getattr(self, "job_id", None),
            current_id,
            normalized_ids,
            (file_name or "")[:100],
            file_size,
        )

        try:
            placeholders = ", ".join(["%s"] * len(normalized_ids))
            reset_sql = f"UPDATE `{learn_table}` SET status = 'N' WHERE id IN ({placeholders})"
            await mysql_execute_query(
                reset_sql,
                tuple(normalized_ids),
                dbname=self.db_name,
            )
            if isinstance(info, dict):
                info["learn_list_existing_status"] = "N"
        except Exception as ex:
            logger.debug(
                "[Duplicate][file] failed to reset LEARN_LIST rows before relearn | job_id=%s ids=%s err=%s",
                getattr(self, "job_id", None),
                normalized_ids,
                ex,
            )

        try:
            pg_table = await self._ensure_pg_table_name()
            if pg_table and self.db_name:
                from db.db_operations import delete_data

                ok = await delete_data(
                    pg_table,
                    {"subject": str(file_name), "content_type": "file"},
                    dbname=self.db_name,
                )
                logger.debug(
                    "[Duplicate][file] deleted previous PG rows before relearn | job_id=%s subject=%s size=%s pg_ok=%s",
                    getattr(self, "job_id", None),
                    (file_name or "")[:100],
                    file_size,
                    ok,
                )
            else:
                logger.debug(
                    "[Duplicate][file] PG table unavailable; continuing relearn after LEARN_LIST reset | job_id=%s subject=%s",
                    getattr(self, "job_id", None),
                    (file_name or "")[:100],
                )
        except Exception as ex:
            logger.debug(
                "[Duplicate][file] failed to delete previous PG rows; continuing relearn | job_id=%s subject=%s err=%s",
                getattr(self, "job_id", None),
                (file_name or "")[:80],
                ex,
            )

    async def _file_run_saved_file_learn_after_save(
        self,
        *,
        info: Dict[str, Any],
        url: str,
        url_key: str,
        file_path: str,
        save_key: str,
        file_name: str,
        file_size: int,
        pre_learn_list_id: Optional[int],
        pre_extracted_text: Optional[str] = None,
    ) -> None:
        """Run learning for a saved file after save_count has been recorded.

        If the same subject/category already has a learned row, reuse it when the
        duplicate policy allows it. Otherwise extract text and run the learning
        pipeline for the pending LEARN_LIST row.
        """
        try:
            valid_learn_list_id = int(pre_learn_list_id or 0)
        except Exception:
            valid_learn_list_id = 0
        if valid_learn_list_id <= 0:
            logger.error(
                "[FilePersist][learning_blocked_missing_row] job_id=%s db=%s row_id=%s post_url=%s file_url=%s file=%s",
                getattr(self, "job_id", None),
                getattr(self, "db_name", None),
                pre_learn_list_id,
                (info.get("source_page") or info.get("source_url") or "")[:220],
                (url or "")[:220],
                file_name,
            )
            try:
                self._record_job_result_stage(
                    url=save_key,
                    stage="study",
                    status="skipped",
                    reason="missing_learn_list_row",
                    source_url=info.get("source_page") or info.get("source_url"),
                    file_url=url,
                    file_name=file_name,
                    file_path=file_path,
                )
            except Exception:
                pass
            await self._mark_study_done(url=save_key, outcome="skipped")
            return

        from db.mysql_db_config import mysql_execute_query
        from db.mariadb_save_update import get_account_identifier_from_chatbot_setup, get_learn_list_table_name

        upload_path: Optional[str] = None
        learning_completed_ok = False

        def _record_file_study(status: str, reason: str = "", *, path: str = "", db_id: Any = None) -> None:
            try:
                self._record_job_result_stage(
                    url=save_key,
                    stage="study",
                    status=status,
                    reason=reason,
                    source_url=info.get("source_page") or info.get("source_url"),
                    file_url=url,
                    file_name=file_name,
                    file_path=path or upload_path or file_path,
                    db_id=db_id if db_id is not None else pre_learn_list_id,
                )
            except Exception:
                pass
            original_meta = info.get("original_meta") if isinstance(info, dict) else {}
            if not isinstance(original_meta, dict):
                original_meta = {}
            _log_file_study_debug(
                self,
                "record_file_study",
                status=status,
                reason=reason or "-",
                learn_list_id=db_id if db_id is not None else pre_learn_list_id,
                post_url=info.get("source_page") or info.get("source_url") or "",
                file_name=file_name,
                file_size=file_size,
                cate1=info.get("cate1") if isinstance(info, dict) else "",
                cate2=info.get("cate2") if isinstance(info, dict) else "",
                ref_cate1=original_meta.get("ref_cate1") or original_meta.get("store_cate1") or "",
                ref_cate2=original_meta.get("ref_cate2") or original_meta.get("store_cate2") or "",
            )
            _log_file_processing_trace(
                stage="학습결과",
                post_url=info.get("source_page") or info.get("source_url") or "",
                file_url=url,
                file_name=file_name,
                selected="예",
                saved="완료",
                learn=status,
                learn_list_id=db_id if db_id is not None else pre_learn_list_id,
                reason=reason or "-",
            )

        try:
            _log_file_study_debug(
                self,
                "after_save_enter",
                url=(url or "")[:180],
                source_page=(str(info.get("source_page") or info.get("source_url") or "")[:180]),
                save_key=save_key,
                url_key=url_key,
                learn_list_id=pre_learn_list_id,
                file_name=file_name,
                file_size=file_size,
                has_pre_extracted=bool((pre_extracted_text or "").strip()),
            )
            logger.info(
                "[파일크롤링추적][학습시작] 파일URL=%s\n작업ID=%s DB=%s 학습목록ID=%s 파일=%s",
                (url or "")[:180],
                getattr(self, "job_id", None),
                getattr(self, "db_name", None),
                pre_learn_list_id,
                os.path.basename(file_path or ""),
            )
            _log_file_url_status(
                stage="learning",
                status="start",
                process_url=url,
                post_url=info.get("source_page") or info.get("source_url") or "",
                file_url=url,
                selected="yes",
                saved="yes",
                learn="running",
                name=file_name,
                learn_list_id=pre_learn_list_id,
                job_id=getattr(self, "job_id", ""),
                db_name=getattr(self, "db_name", ""),
            )
            if self.stop_event.is_set():
                _log_file_url_status(
                    stage="learning",
                    status="skipped",
                    process_url=url,
                    post_url=info.get("source_page") or info.get("source_url") or "",
                    file_url=url,
                    selected="yes",
                    saved="yes",
                    learn="skipped",
                    reason="stop_requested",
                    name=file_name,
                    learn_list_id=pre_learn_list_id,
                    job_id=getattr(self, "job_id", ""),
                    db_name=getattr(self, "db_name", ""),
                )
                _record_file_study("skipped", "stop_requested")
                await self._mark_study_done(url=save_key, outcome="skipped")
                return
            t_cp0 = time.perf_counter()
            logger.debug(
                "[LearningTrace][board_file.before_copy] job_id=%s db=%s url=%s src=%s",
                getattr(self, "job_id", None),
                getattr(self, "db_name", None),
                (url or "")[:180],
                (file_path or "")[:220],
            )
            upload_path = await self._copy_file_to_upload_path(file_path)
            copy_elapsed_sec = time.perf_counter() - t_cp0
            if copy_elapsed_sec >= 3.0:
                logger.warning(
                    "[FileLearnTrace][upload_copy_slow] job_id=%s elapsed_sec=%.3f learn_list_id=%s src=%s dst=%s",
                    getattr(self, "job_id", None),
                    copy_elapsed_sec,
                    pre_learn_list_id,
                    (file_path or "")[:220],
                    (upload_path or "")[:220],
                )
            if _file_pipeline_bottleneck_log_enabled():
                logger.debug(
                    "[Bottleneck][after_save] copy_to_upload %sms ok=%s",
                    int((time.perf_counter() - t_cp0) * 1000),
                    bool(upload_path),
                )

            if upload_path:
                file_subject = _resolve_persisted_file_learning_subject(
                    file_name,
                    info,
                    file_path,
                )
                storage_filename = str(
                    info.get("storage_filename")
                    or os.path.basename(upload_path or file_path or "")
                ).strip()
                ext_hint = os.path.splitext(upload_path or file_path or "")[1].lower()
                extracted_text = (pre_extracted_text or "").strip()
                if extracted_text:
                    logger.debug(
                        "[LearningTrace][board_file.reuse_extract] job_id=%s db=%s url=%s chars=%s ext=%s",
                        getattr(self, "job_id", None),
                        getattr(self, "db_name", None),
                        (url or "")[:180],
                        len(extracted_text),
                        ext_hint,
                    )
                    if _file_pipeline_bottleneck_log_enabled():
                        logger.debug(
                            "[Bottleneck][after_save] text_extract reused chars=%s ext=%s",
                            len(extracted_text),
                            ext_hint,
                        )
                    if ext_hint == ".pdf":
                        logger.debug(
                            "[PDFDebug][board_file.reuse_extract] job_id=%s url=%s chars=%s preview=%s",
                            getattr(self, "job_id", None),
                            (url or "")[:180],
                            len(extracted_text),
                            " ".join(str(extracted_text).split())[:160],
                        )
                else:
                    t_ex0 = time.perf_counter()
                    logger.info(
                        "[FileLearnTrace][text_extract_begin] job_id=%s learn_list_id=%s ext=%s file_url=%s file=%s",
                        getattr(self, "job_id", None),
                        pre_learn_list_id,
                        ext_hint,
                        (url or "")[:220],
                        file_subject[:160],
                    )
                    logger.debug(
                        "[LearningTrace][board_file.before_extract] job_id=%s db=%s url=%s upload_path=%s",
                        getattr(self, "job_id", None),
                        getattr(self, "db_name", None),
                        (url or "")[:180],
                        (upload_path or "")[:220],
                    )
                    extracted_text = await self._extract_text_from_saved_file_for_learning_guarded(
                        upload_path,
                        url=url,
                        file_name=file_subject,
                    )
                    extract_elapsed_sec = time.perf_counter() - t_ex0
                    logger.info(
                        "[FileLearnTrace][text_extract_done] job_id=%s learn_list_id=%s ext=%s elapsed_ms=%s chars=%s file_url=%s file=%s",
                        getattr(self, "job_id", None),
                        pre_learn_list_id,
                        ext_hint,
                        int(extract_elapsed_sec * 1000),
                        len((extracted_text or "").strip()),
                        (url or "")[:220],
                        file_subject[:160],
                    )
                    if extract_elapsed_sec >= 3.0:
                        logger.warning(
                            "[FileLearnTrace][text_extract_slow] job_id=%s elapsed_sec=%.3f learn_list_id=%s ext=%s chars=%s file_url=%s file=%s",
                            getattr(self, "job_id", None),
                            extract_elapsed_sec,
                            pre_learn_list_id,
                            ext_hint,
                            len((extracted_text or "").strip()),
                            (url or "")[:220],
                            file_subject[:160],
                        )
                    logger.debug(
                        "[LearningTrace][board_file.after_extract] job_id=%s db=%s url=%s elapsed_ms=%s chars=%s ext=%s",
                        getattr(self, "job_id", None),
                        getattr(self, "db_name", None),
                        (url or "")[:180],
                        int((time.perf_counter() - t_ex0) * 1000),
                        len((extracted_text or "").strip()),
                        ext_hint,
                    )
                    if _file_pipeline_bottleneck_log_enabled():
                        logger.debug(
                            "[Bottleneck][after_save] text_extract %sms chars=%s ext=%s",
                            int((time.perf_counter() - t_ex0) * 1000),
                            len((extracted_text or "").strip()),
                            ext_hint,
                        )
                    if ext_hint == ".pdf":
                        logger.debug(
                            "[PDFDebug][board_file.after_extract] job_id=%s url=%s chars=%s preview=%s",
                            getattr(self, "job_id", None),
                            (url or "")[:180],
                            len((extracted_text or "").strip()),
                            " ".join(str(extracted_text or "").split())[:160],
                        )
                if not (extracted_text or "").strip():
                    try:
                        failures = getattr(self, "_file_text_extract_failure_reasons", None)
                        extract_failure_reason = failures.pop(upload_path, "") if isinstance(failures, dict) else ""
                    except Exception:
                        extract_failure_reason = ""
                    empty_reason = extract_failure_reason or "file_text_extract_empty"
                    if ext_hint == ".hwpx":
                        try:
                            from edu.hwp_edu import is_encrypted_hwpx

                            if is_encrypted_hwpx(upload_path):
                                empty_reason = "encrypted_hwpx"
                                empty_detail = (
                                    "HWPX file is encrypted according to META-INF/manifest.xml, so text extraction and learning are blocked."
                                )
                            else:
                                empty_detail = (
                                    'HWP/HWPX text extraction returned empty content. The file may be image-based, encrypted, or unsupported by the current extractor. Enable OCR or check the source document.'
                                )
                        except Exception:
                            empty_detail = (
                                'HWP/HWPX text extraction returned empty content. The file may be image-based, encrypted, or unsupported by the current extractor. Enable OCR or check the source document.'
                            )
                    elif ext_hint in (".hwp", ".hwpx"):
                        empty_detail = (
                            'HWP/HWPX text extraction returned empty content. The file may be image-based, encrypted, or unsupported by the current extractor. Enable OCR or check the source document.'
                        )
                    elif ext_hint == ".pdf":
                        if empty_reason == "file_text_extract_timeout":
                            empty_detail = (
                                'PDF text extraction timed out before completion. Increase FILE_PDF_TEXT_EXTRACT_TIMEOUT_SEC only for documents that need it, or enable OCR fallback for scanned PDFs.'
                            )
                        else:
                            empty_detail = (
                                'PDF text extraction returned empty content. If this is a scanned PDF, set FILE_CRAWL_PDF_OCR_FALLBACK=1 and configure UPSTAGE_API_KEY to enable OCR fallback.'
                            )
                    elif ext_hint in (".xls", ".xlsx", ".csv"):
                        empty_detail = (
                            'Spreadsheet text extraction returned empty content. The file may be empty, protected, or unsupported.'
                        )
                    else:
                        empty_detail = 'File text extraction returned empty content, so learning was skipped.'
                    logger.debug(
                        "[file_crawl][board][file] empty extract, skip learn | ext=%s url=%s path=%s | %s",
                        ext_hint,
                        url,
                        upload_path,
                        empty_detail,
                    )
                    if ext_hint == ".pdf":
                        logger.debug(
                            "[PDFDebug][board_file.empty_extract] job_id=%s url=%s learn_list_id=%s path=%s detail=%s",
                            getattr(self, "job_id", None),
                            (url or "")[:180],
                            pre_learn_list_id,
                            (upload_path or "")[:260],
                            empty_detail,
                        )
                        logger.debug(
                            "[PDFDebug][board_file.empty_extract.env] job_id=%s ocr_env=%s has_upstage_key=%s",
                            getattr(self, "job_id", None),
                            os.getenv("FILE_CRAWL_PDF_OCR_FALLBACK"),
                            bool(str(os.getenv("UPSTAGE_API_KEY", "") or "").strip()),
                        )
                    try:
                        await self._record_study_fail(
                            reason=empty_reason,
                            emit_fail_detail_log=False,
                            url=url,
                            path=upload_path,
                            file_ext=ext_hint,
                            detail=empty_detail,
                        )
                    except Exception:
                        pass
                    await self._mark_study_done(url=save_key, outcome="failed")
                    _log_file_url_status(
                        stage="learning",
                        status="failed",
                        process_url=url,
                        post_url=info.get("source_page") or info.get("source_url") or "",
                        file_url=url,
                        selected="yes",
                        saved="yes",
                        learn="failed",
                        reason=empty_reason,
                        name=file_name,
                        learn_list_id=pre_learn_list_id,
                        job_id=getattr(self, "job_id", ""),
                        db_name=getattr(self, "db_name", ""),
                    )
                    _record_file_study("failed", empty_reason, path=upload_path)
                    return
                if learning_blocked_by_rrn_pattern(extracted_text):
                    if str(os.getenv("FILE_CRAWL_RRN_MASK_INSTEAD_OF_BLOCK", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}:
                        before_len = len(extracted_text or "")
                        extracted_text = mask_rrn_like_patterns(extracted_text)
                        logger.debug(
                            "[file_crawl][board][file] rrn pattern masked before learn | ext=%s url=%s path=%s chars=%s",
                            ext_hint,
                            url,
                            upload_path,
                            before_len,
                        )
                    else:
                        blocked_detail = (
                            'Learning was blocked because the extracted text appears to contain a resident registration number pattern or other sensitive personal information.'
                        )
                        logger.debug(
                            "[file_crawl][board][file] rrn pattern detected, block learn | ext=%s url=%s path=%s",
                            ext_hint,
                            url,
                            upload_path,
                        )
                        try:
                            await self._record_study_fail(
                                reason="learning_blocked_by_rrn_pattern",
                                emit_fail_detail_log=False,
                                url=url,
                                path=upload_path,
                                file_ext=ext_hint,
                                detail=blocked_detail,
                            )
                        except Exception:
                            pass
                        await self._rollback_saved_selection_for_rrn_pattern(save_key=save_key)
                        try:
                            await self._record_study_skip(
                                reason="learning_blocked_by_rrn_pattern",
                                url=url,
                                path=upload_path,
                                learn_list_id=pre_learn_list_id,
                                detail=blocked_detail,
                            )
                        except Exception:
                            pass
                        await self._mark_study_done(url=save_key, outcome="skipped")
                        _log_file_url_status(
                            stage="learning",
                            status="skipped",
                            process_url=url,
                            post_url=info.get("source_page") or info.get("source_url") or "",
                            file_url=url,
                            selected="yes",
                            saved="yes",
                            learn="skipped",
                            reason="learning_blocked_by_rrn_pattern",
                            name=file_name,
                            learn_list_id=pre_learn_list_id,
                            job_id=getattr(self, "job_id", ""),
                            db_name=getattr(self, "db_name", ""),
                        )
                        _record_file_study("skipped", "learning_blocked_by_rrn_pattern", path=upload_path)
                        return
                from backend.file.file_learning_text_mask import mask_file_learning_text

                extracted_text, fixed_mask_count = mask_file_learning_text(extracted_text)
                if fixed_mask_count:
                    logger.info(
                        "[file_crawl][board][file] fixed keyword masked before learn | job_id=%s url=%s count=%s",
                        getattr(self, "job_id", None),
                        (url or "")[:220],
                        fixed_mask_count,
                    )
                file_result = {
                    "source_url": url,
                    "source": url,
                    "source_page": info.get("source_page"),
                    "subject": file_subject,
                    "title": file_subject,
                    "content": extracted_text,
                    "local_path": upload_path,
                    "content_type": "file",
                    "reg_date": info.get("reg_date"),
                    "file_size": file_size,
                    "cate1": info.get("cate1"),
                    "cate2": info.get("cate2"),
                    "file_info": _file_info_with_persisted_identity(
                        info,
                        subject=file_subject,
                        storage_filename=storage_filename,
                        file_size=file_size,
                    ),
                }
                if _file_pipeline_bottleneck_log_enabled():
                    logger.debug(
                        "[Bottleneck][after_save] pre _run_learning_pipeline cumulative %sms",
                        int((time.perf_counter() - t_as0) * 1000),
                    )
                learn_pipeline_started = time.perf_counter()
                logger.info(
                    "[FileLearnTrace][embedding_request_begin] job_id=%s learn_list_id=%s chars=%s file_url=%s file=%s",
                    getattr(self, "job_id", None),
                    pre_learn_list_id,
                    len((extracted_text or "").strip()),
                    (url or "")[:220],
                    file_subject[:160],
                )
                logger.debug(
                    "[LearningTrace][board_file.before_pipeline] job_id=%s db=%s url=%s learn_list_id=%s file=%s",
                    getattr(self, "job_id", None),
                    getattr(self, "db_name", None),
                    (url or "")[:180],
                    pre_learn_list_id,
                    os.path.basename(upload_path or file_path or ""),
                )
                if ext_hint == ".pdf":
                    logger.debug(
                        "[PDFDebug][board_file.before_pipeline] job_id=%s url=%s learn_list_id=%s chars=%s",
                        getattr(self, "job_id", None),
                        (url or "")[:180],
                        pre_learn_list_id,
                        len((extracted_text or "").strip()),
                    )
                _log_file_study_debug(
                    self,
                    "before_learning_pipeline",
                    url=(url or "")[:180],
                    learn_list_id=pre_learn_list_id,
                    subject=(file_result.get("subject") or "")[:160],
                    text_chars=len((extracted_text or "").strip()),
                    file_size=file_size,
                    cate1=info.get("cate1") if isinstance(info, dict) else "",
                    cate2=info.get("cate2") if isinstance(info, dict) else "",
                )
                learning_completed_ok = await self._run_learning_pipeline(
                    url=url,
                    url_key=url_key,
                    subject_value=file_result["subject"],
                    result=file_result,
                    memo_for_learning=getattr(self, "memo", ""),
                    learn_list_id=pre_learn_list_id,
                    start_time=time.monotonic(),
                )
                logger.info(
                    "[FileLearnTrace][embedding_request_done] job_id=%s learn_list_id=%s elapsed_ms=%s accepted=%s pending_callback=%s file_url=%s",
                    getattr(self, "job_id", None),
                    pre_learn_list_id,
                    int((time.perf_counter() - learn_pipeline_started) * 1000),
                    learning_completed_ok,
                    bool(file_result.get("embedding_batch_background_queued")),
                    (url or "")[:220],
                )
                _log_file_study_debug(
                    self,
                    "after_learning_pipeline",
                    url=(url or "")[:180],
                    learn_list_id=pre_learn_list_id,
                    learning_ok=learning_completed_ok,
                    background_queued=bool(file_result.get("embedding_batch_background_queued")),
                    elapsed_ms=int((time.perf_counter() - learn_pipeline_started) * 1000),
                )
                logger.debug(
                    "[FileSummaryAPI][CallDebug] after_learning | job_id=%s learn_list_id=%s learning_ok=%s summary_enabled=%s url=%s",
                    getattr(self, "job_id", None),
                    pre_learn_list_id,
                    learning_completed_ok,
                    str(os.getenv("FILE_CRAWL_POST_SUMMARIZE_KEYWORDS", "1") or "1").strip(),
                    (url or "")[:180],
                )
                batch_pending = bool(file_result.get("embedding_batch_background_queued"))
                if learning_completed_ok:
                    _log_file_url_status(
                        stage="learning",
                        status="success",
                        process_url=url,
                        post_url=info.get("source_page") or info.get("source_url") or "",
                        file_url=url,
                        selected="yes",
                        saved="yes",
                        learn="success",
                        name=file_name,
                        learn_list_id=pre_learn_list_id,
                        job_id=getattr(self, "job_id", ""),
                        db_name=getattr(self, "db_name", ""),
                    )
                    _record_file_study("success", path=upload_path)
                    remember_completed_url(url_key or save_key or url, stage="study")
                    if bool(file_result.get("embedding_batch_background_queued")):
                        logger.debug(
                            "[FileSummaryAPI][CallDebug] skip immediate summary: embedding batch queued in background | job_id=%s learn_list_id=%s url=%s",
                            getattr(self, "job_id", None),
                            pre_learn_list_id,
                            (url or "")[:180],
                        )
                    else:
                        try:
                            await self._file_crawl_post_summarize_keywords(
                                file_url=url,
                                learn_list_id=pre_learn_list_id,
                                subject=file_result["subject"],
                                normalized_text=str(extracted_text or ""),
                            )
                        except Exception as sum_exc:
                            logger.debug(
                                "[FileSummaryAPI] dispatch_error | job_id=%s learn_list_id=%s url=%s err=%s",
                                getattr(self, "job_id", None),
                                pre_learn_list_id,
                                (url or "")[:180],
                                sum_exc,
                            )
                elif batch_pending:
                    _log_file_url_status(
                        stage="learning",
                        status="queued",
                        process_url=url,
                        post_url=info.get("source_page") or info.get("source_url") or "",
                        file_url=url,
                        selected="yes",
                        saved="yes",
                        learn="queued",
                        reason="embedding_batch_pending_callback",
                        name=file_name,
                        learn_list_id=pre_learn_list_id,
                        job_id=getattr(self, "job_id", ""),
                        db_name=getattr(self, "db_name", ""),
                    )
                else:
                    _log_file_url_status(
                        stage="learning",
                        status="failed",
                        process_url=url,
                        post_url=info.get("source_page") or info.get("source_url") or "",
                        file_url=url,
                        selected="yes",
                        saved="yes",
                        learn="failed",
                        reason="learning_pipeline_failed",
                        name=file_name,
                        learn_list_id=pre_learn_list_id,
                        job_id=getattr(self, "job_id", ""),
                        db_name=getattr(self, "db_name", ""),
                    )
                    _record_file_study("failed", "learning_pipeline_failed", path=upload_path)
                    logger.debug(
                        "[FileSummaryAPI][CallDebug] skip after learning failed | job_id=%s learn_list_id=%s url=%s",
                        getattr(self, "job_id", None),
                        pre_learn_list_id,
                        (url or "")[:180],
                    )
                logger.debug(
                    "[LearningTrace][board_file.after_pipeline] job_id=%s db=%s url=%s learn_list_id=%s elapsed_ms=%s",
                    getattr(self, "job_id", None),
                    getattr(self, "db_name", None),
                    (url or "")[:180],
                    pre_learn_list_id,
                    int((time.perf_counter() - learn_pipeline_started) * 1000),
                )
                await self._log_file_learn_list_row_debug(
                    stage="after_pipeline",
                    learn_list_id=pre_learn_list_id,
                    url=url,
                    file_info=info if isinstance(info, dict) else None,
                )
                if ext_hint == ".pdf":
                    logger.debug(
                        "[PDFDebug][board_file.after_pipeline] job_id=%s url=%s learn_list_id=%s ok=%s",
                        getattr(self, "job_id", None),
                        (url or "")[:180],
                        pre_learn_list_id,
                        learning_completed_ok,
                    )
                try:
                    file_result["content"] = ""
                except Exception:
                    pass
                extracted_text = ""
                if _file_pipeline_bottleneck_log_enabled():
                    logger.debug(
                        "[Bottleneck][after_save] post _run_learning_pipeline cumulative %sms url=%s",
                        int((time.perf_counter() - t_as0) * 1000),
                        (url or "")[:160],
                    )
            else:
                _log_file_url_status(
                    stage="learning",
                    status="failed",
                    process_url=url,
                    post_url=info.get("source_page") or info.get("source_url") or "",
                    file_url=url,
                    selected="yes",
                    saved="yes",
                    learn="failed",
                    reason="upload_copy_failed",
                    name=file_name,
                    learn_list_id=pre_learn_list_id,
                    job_id=getattr(self, "job_id", ""),
                    db_name=getattr(self, "db_name", ""),
                )
                _record_file_study("failed", "upload_copy_failed", path=file_path)
                await self._mark_study_done(url=save_key, outcome="failed")
        except Exception as e:
            _log_file_url_status(
                stage="learning",
                status="error",
                process_url=url,
                post_url=info.get("source_page") or info.get("source_url") or "",
                file_url=url,
                selected="yes",
                saved="yes",
                learn="failed",
                reason="exception",
                error=repr(e),
                name=file_name,
                learn_list_id=pre_learn_list_id,
                job_id=getattr(self, "job_id", ""),
                db_name=getattr(self, "db_name", ""),
            )
            logger.error(f"[FileLearningError] {url} | {e}")
            _record_file_study("failed", str(e), path=upload_path or file_path)
            await self._mark_study_done(url=save_key, outcome="failed")
        if self.progress_callback:
            self.progress_callback(self.get_stats())
        logger.debug(
            "[file_crawl][board][file] save_done | job_id=%s url=%s path=%s",
            self.job_id,
            url,
            file_path,
        )
        try:
            learning_handoff_complete = bool(learning_completed_ok) or bool(
                isinstance(locals().get("file_result"), dict)
                and locals().get("file_result", {}).get("embedding_batch_background_queued")
            )
            await self._cleanup_downloaded_source_file_after_learn(
                source_path=file_path,
                upload_path=upload_path,
                url=url,
                learn_completed=learning_handoff_complete,
            )
        except Exception:
            pass

    async def _cleanup_downloaded_source_file_after_learn(
        self,
        *,
        source_path: str,
        upload_path: Optional[str],
        url: str,
        learn_completed: bool,
    ) -> None:
        if not _delete_downloaded_source_after_learn_enabled():
            return
        if not bool(learn_completed):
            logger.debug(
                "[FileCleanup] keep downloaded source because learning not completed | job_id=%s url=%s src=%s",
                getattr(self, "job_id", None),
                (url or "")[:200],
                source_path,
            )
            return
        src = str(source_path or "").strip()
        dst = str(upload_path or "").strip()
        if not src or not dst:
            return
        try:
            src_norm = os.path.normcase(os.path.abspath(src))
            dst_norm = os.path.normcase(os.path.abspath(dst))
        except Exception:
            src_norm = src
            dst_norm = dst
        if not src_norm or src_norm == dst_norm:
            return
        try:
            if not os.path.isfile(src):
                return
        except Exception:
            return
        try:
            await asyncio.to_thread(os.remove, src)
            logger.debug(
                "[FileCleanup] removed downloaded local source after storage handoff | job_id=%s url=%s src=%s kept=%s",
                getattr(self, "job_id", None),
                (url or "")[:200],
                src,
                dst,
            )
        except Exception as exc:
            logger.debug(
                "[FileCleanup] cleanup downloaded source failed | job_id=%s url=%s src=%s err=%s",
                getattr(self, "job_id", None),
                (url or "")[:200],
                src,
                exc,
            )

    def _file_pipeline_worker_health_snapshot(self) -> Dict[str, Any]:
        """Report actual per-job worker tasks instead of the completed startup task."""
        if bool(getattr(self, "use_global_pool", False)):
            try:
                from core.crawler.global_pool import get_global_worker_pool

                return get_global_worker_pool().worker_health_snapshot(
                    job_id=str(getattr(self, "job_id", "") or "")
                )
            except Exception as exc:
                return {"health_error": str(exc)}

        manager = getattr(self, "_file_worker_manager", None)
        result: Dict[str, Any] = {"mode": "local", "manager_present": bool(manager)}
        stages = []
        if manager is not None:
            stages.extend(
                (("scan", getattr(manager, "scan_tasks", [])),
                 ("collection", getattr(manager, "collection_tasks", [])),
                 ("download", getattr(manager, "download_tasks", [])),
                 ("study", getattr(manager, "study_tasks", [])))
            )
        stages.extend(
            (("progress_dispatch", getattr(self, "_file_progress_dispatch_tasks", set())),
             ("save", getattr(self, "_file_progress_tasks", set())),
             ("local_finalize", getattr(self, "_file_local_finalize_tasks", set())),
             ("learn", getattr(self, "_file_parallel_learn_tasks", set())))
        )
        for stage, stage_tasks in stages:
            tasks = list(stage_tasks or [])
            failures: List[str] = []
            active: List[str] = []
            for task in tasks:
                if isinstance(task, asyncio.Task) and not task.done():
                    stack = task.get_stack(limit=1)
                    if stack:
                        frame = stack[-1]
                        active.append(f"{task.get_name()}:{frame.f_code.co_name}:{frame.f_lineno}")
                    else:
                        active.append(task.get_name())
                if not isinstance(task, asyncio.Task) or not task.done() or task.cancelled():
                    continue
                try:
                    exc = task.exception()
                except Exception as exc:
                    failures.append(f"exception_read_failed:{type(exc).__name__}:{exc}")
                else:
                    if exc is not None:
                        failures.append(f"{type(exc).__name__}:{exc}")
            result[f"{stage}_total"] = len(tasks)
            result[f"{stage}_alive"] = sum(1 for task in tasks if isinstance(task, asyncio.Task) and not task.done())
            result[f"{stage}_done"] = sum(1 for task in tasks if isinstance(task, asyncio.Task) and task.done())
            result[f"{stage}_cancelled"] = sum(1 for task in tasks if isinstance(task, asyncio.Task) and task.cancelled())
            if active:
                result[f"{stage}_active"] = active[:8]
            if failures:
                result[f"{stage}_failures"] = failures[:5]
        try:
            from core.crawler.workers.download import get_download_worker_activity_snapshot

            result["download_inflight"] = get_download_worker_activity_snapshot(
                job_id=str(getattr(self, "job_id", "") or "")
            )[:8]
        except Exception as exc:
            result["download_inflight_error"] = f"{type(exc).__name__}:{exc}"
        active_progress = getattr(self, "_file_progress_active", None)
        if isinstance(active_progress, dict):
            now = time.monotonic()
            result["save_inflight"] = [
                {**dict(value), "elapsed_sec": round(max(0.0, now - float(value.get("started_at", now))), 1)}
                for value in list(active_progress.values())[:8]
                if isinstance(value, dict)
            ]
        for name, queue in (
            ("file_save", getattr(self, "_file_save_queue", None)),
            ("local_finalize", getattr(self, "_file_local_finalize_queue", None)),
            ("learning_request", getattr(self, "_file_learning_request_queue", None)),
            ("learning_pdf_request", getattr(self, "_file_learning_pdf_request_queue", None)),
        ):
            if queue is None:
                continue
            try:
                result[f"{name}_queue"] = queue.qsize()
                result[f"{name}_queue_unfinished"] = int(getattr(queue, "_unfinished_tasks", 0) or 0)
            except Exception as exc:
                result[f"{name}_queue_error"] = f"{type(exc).__name__}:{exc}"
        return result

    def _record_file_db_operation(self, *, operation: str, elapsed_sec: float) -> None:
        """Keep bounded per-job DB timing metrics without per-item success logs."""
        op = str(operation or "unknown")
        elapsed_ms = max(0.0, float(elapsed_sec or 0.0) * 1000.0)
        try:
            from backend.shared.file_crawl_db_diagnostics import record_file_crawl_db_operation

            record_file_crawl_db_operation(
                job_id=getattr(self, "job_id", ""),
                db_name=getattr(self, "db_name", ""),
                operation=op,
                elapsed_ms=elapsed_ms,
            )
        except Exception:
            pass
        metrics = getattr(self, "_file_db_operation_metrics", None)
        if not isinstance(metrics, dict):
            metrics = {}
            self._file_db_operation_metrics = metrics
        bucket = metrics.setdefault(op, {"count": 0, "total_ms": 0.0, "max_ms": 0.0, "samples_ms": []})
        bucket["count"] = int(bucket.get("count", 0) or 0) + 1
        bucket["total_ms"] = float(bucket.get("total_ms", 0.0) or 0.0) + elapsed_ms
        bucket["max_ms"] = max(float(bucket.get("max_ms", 0.0) or 0.0), elapsed_ms)
        samples = bucket.setdefault("samples_ms", [])
        if len(samples) < 512:
            samples.append(elapsed_ms)

    def _file_db_operation_metrics_snapshot(self) -> Dict[str, Dict[str, float]]:
        metrics = getattr(self, "_file_db_operation_metrics", None)
        if not isinstance(metrics, dict):
            return {}
        summary: Dict[str, Dict[str, float]] = {}
        for op, bucket in metrics.items():
            count = int(bucket.get("count", 0) or 0)
            if count <= 0:
                continue
            samples = sorted(float(value) for value in (bucket.get("samples_ms") or []))
            p95 = samples[min(len(samples) - 1, max(0, int((len(samples) - 1) * 0.95)))] if samples else 0.0
            total_ms = float(bucket.get("total_ms", 0.0) or 0.0)
            summary[str(op)] = {
                "count": count,
                "avg_ms": round(total_ms / count, 1),
                "p95_ms": round(p95, 1),
                "max_ms": round(float(bucket.get("max_ms", 0.0) or 0.0), 1),
                "total_ms": round(total_ms, 1),
            }
        return summary

    async def _run_file_queue_watchdog(self) -> None:
        """Emit a rate-limited queue heartbeat and warn when its state stops changing."""
        try:
            interval_sec = float(os.getenv("FILE_CRAWL_QUEUE_STALL_LOG_SEC", "10") or "10")
        except Exception:
            interval_sec = 10.0
        interval_sec = max(3.0, min(interval_sec, 300.0))
        last_activity = None
        last_change_at = time.monotonic()

        while True:
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=interval_sec)
                return
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise

            queues = getattr(self, "_file_job_queues", None)
            if queues is None:
                return
            await self._ensure_file_progress_workers()
            await self._ensure_file_local_finalize_workers()
            try:
                snapshot = queues.debug_snapshot() if hasattr(queues, "debug_snapshot") else queues.snapshot()
            except Exception:
                snapshot = {}
            save_queue = getattr(self, "_file_save_queue", None)
            if save_queue is not None:
                try:
                    snapshot["file_save_queue"] = save_queue.qsize()
                    snapshot["file_save_queue_unfinished"] = int(getattr(save_queue, "_unfinished_tasks", 0) or 0)
                except Exception:
                    snapshot["file_save_queue"] = -1
            try:
                async with self._stats_lock:
                    stats = dict(self.stats or {})
            except Exception:
                stats = {}

            normal_pending = max(
                int(snapshot.get("collection_batch_queue_unfinished", 0) or 0),
                int(snapshot.get("collection_batch_queue", 0) or 0)
                    + int(snapshot.get("collection_batch_queue_buffer", 0) or 0),
            )
            large_pending = max(
                int(snapshot.get("large_collection_batch_queue_unfinished", 0) or 0),
                int(snapshot.get("large_collection_batch_queue", 0) or 0)
                    + int(snapshot.get("large_collection_batch_queue_buffer", 0) or 0),
            )
            pending = max(0, normal_pending + large_pending)
            selected_count = int(stats.get("file_attachment_selection_confirmed_total", 0) or 0)
            saved_count = int(stats.get("save_count", 0) or 0)
            if selected_count > saved_count:
                trace_summary = self._file_pipeline_trace_summary()
                logger.info(
                    "[FilePipelineTrace][selection_save_gap]\n"
                    "job_id=%s\n"
                    "selected=%s\n"
                    "saved=%s\n"
                    "gap=%s\n"
                    "download_queue_pending=%s\n"
                    "progress_queue=%s\n"
                    "save_queue=%s\n"
                    "local_finalize_queue=%s\n"
                    "stages=%s\n"
                    "samples=%s",
                    getattr(self, "job_id", None),
                    selected_count,
                    saved_count,
                    selected_count - saved_count,
                    pending,
                    snapshot.get("progress_queue_unfinished", 0),
                    snapshot.get("file_save_queue_unfinished", 0),
                    snapshot.get("local_finalize_queue_unfinished", 0),
                    trace_summary.get("stages"),
                    trace_summary.get("samples"),
                )
            if pending <= 0:
                last_activity = None
                last_change_at = time.monotonic()
                continue

            try:
                raw_queue = getattr(getattr(queues, "collection_batch_queue", None), "queue", None)
                queue_maxsize = int(getattr(raw_queue, "maxsize", 0) or 0)
            except Exception:
                queue_maxsize = 0
            worker_health: Dict[str, Any] = {}
            registered = None
            if bool(getattr(self, "use_global_pool", False)):
                try:
                    from core.crawler.global_pool import get_global_worker_pool

                    pool = get_global_worker_pool()
                    worker_health = pool.worker_health_snapshot(job_id=str(getattr(self, "job_id", "") or ""))
                    registered = str(self.job_id or "unknown") in pool.registered_jobs
                except Exception as exc:
                    worker_health = {"health_error": str(exc)}
            else:
                worker_health = self._file_pipeline_worker_health_snapshot()
            progress_trace: Dict[str, Any] = {}
            try:
                progress_raw = list(getattr(queues.progress_queue, "_queue", []) or [])
                event_counts: Dict[str, int] = {}
                event_samples: List[Dict[str, str]] = []
                for event in progress_raw:
                    event_type = str(event.get("type") if isinstance(event, dict) else type(event).__name__)
                    event_counts[event_type] = event_counts.get(event_type, 0) + 1
                    if len(event_samples) < 5 and isinstance(event, dict):
                        info = event.get("file_info") if isinstance(event.get("file_info"), dict) else {}
                        event_samples.append({
                            "type": event_type,
                            "url": str(info.get("url") or event.get("url") or "")[:220],
                            "post_url": str(info.get("source_page") or event.get("source_page") or event.get("source_url") or "")[:220],
                        })
                progress_trace = {"types": event_counts, "samples": event_samples}
            except Exception as trace_exc:
                progress_trace = {"trace_error": f"{type(trace_exc).__name__}:{trace_exc}"}
            logger.debug(
                "[FileCrawlTrace][progress_queue] job_id=%s queued=%s unfinished=%s trace=%s",
                getattr(self, "job_id", None),
                snapshot.get("progress_queue", 0),
                snapshot.get("progress_queue_unfinished", 0),
                progress_trace,
            )
            logger.debug(
                "[FileCrawlTrace][queue_status] job_id=%s pending=%s queued=%s max_batches=%s "
                "buffer=%s unfinished=%s saved=%s learned=%s global_registered=%s workers=%s",
                getattr(self, "job_id", None),
                pending,
                snapshot.get("collection_batch_queue", 0),
                queue_maxsize,
                snapshot.get("collection_batch_queue_buffer", 0),
                snapshot.get("collection_batch_queue_unfinished", 0),
                stats.get("save_count", 0),
                stats.get("file_study_done_count", stats.get("study_done_count", 0)),
                registered,
                worker_health,
            )
            logger.info(
                "[FileCrawlTrace][download_lanes] job_id=%s http_queued=%s http_unfinished=%s playwright_queued=%s playwright_unfinished=%s",
                getattr(self, "job_id", None),
                snapshot.get("collection_batch_queue", 0),
                snapshot.get("collection_batch_queue_unfinished", 0),
                snapshot.get("large_collection_batch_queue", 0),
                snapshot.get("large_collection_batch_queue_unfinished", 0),
            )
            db_metrics = self._file_db_operation_metrics_snapshot()
            if db_metrics:
                logger.info(
                    "[FileDbTrace][summary] job_id=%s operations=%s",
                    getattr(self, "job_id", None),
                    db_metrics,
                )
            activity = (
                int(stats.get("save_count", 0) or 0),
                int(stats.get("file_study_done_count", stats.get("study_done_count", 0)) or 0),
                int(snapshot.get("collection_batch_queue", 0) or 0),
                int(snapshot.get("collection_batch_queue_buffer", 0) or 0),
                int(snapshot.get("collection_batch_queue_unfinished", 0) or 0),
                int(snapshot.get("large_collection_batch_queue", 0) or 0),
                int(snapshot.get("large_collection_batch_queue_unfinished", 0) or 0),
                int(snapshot.get("progress_queue_unfinished", 0) or 0),
            )
            now = time.monotonic()
            if activity != last_activity:
                last_activity = activity
                last_change_at = now
                continue

            stagnant_sec = now - last_change_at
            if stagnant_sec < interval_sec:
                continue

            worker_health: Dict[str, Any] = {}
            registered = None
            if bool(getattr(self, "use_global_pool", False)):
                try:
                    from core.crawler.global_pool import get_global_worker_pool

                    pool = get_global_worker_pool()
                    worker_health = pool.worker_health_snapshot(job_id=str(getattr(self, "job_id", "") or ""))
                    registered = str(self.job_id or "unknown") in pool.registered_jobs
                except Exception as exc:
                    worker_health = {"health_error": str(exc)}
            else:
                worker_health = self._file_pipeline_worker_health_snapshot()

            # A batch remains unfinished while a worker is actively downloading it.
            # The ordinary 10-second watchdog interval is too short to call that a stall.
            download_inflight = []
            if isinstance(worker_health, dict):
                download_inflight = (
                    worker_health.get("download_inflight")
                    or worker_health.get("download_active")
                    or []
                )
            if not isinstance(download_inflight, list):
                download_inflight = []
            active_download_elapsed = max(
                (float(item.get("elapsed_sec") or 0.0) for item in download_inflight if isinstance(item, dict)),
                default=0.0,
            )
            download_stall_warn_sec = max(45.0, interval_sec * 3.0)
            if active_download_elapsed > 0.0 and active_download_elapsed < download_stall_warn_sec:
                busy_lines = [
                    "worker={worker} lane={lane} elapsed_sec={elapsed} url={url} post_url={post_url} name={name}".format(
                        worker=item.get("worker"), lane=item.get("lane") or "unknown",
                        elapsed=item.get("elapsed_sec") or 0, url=item.get("url") or "-",
                        post_url=item.get("post_url") or "-", name=item.get("name") or "-",
                    )
                    for item in download_inflight if isinstance(item, dict)
                ]
                logger.info(
                    "[FileCrawlTrace][download_workers_busy]\njob_id=%s\nactive_count=%s\n%s",
                    getattr(self, "job_id", None), len(busy_lines),
                    "\n".join(busy_lines) or "active_detail=unavailable",
                )
                logger.debug(
                    "[FileCrawlTrace][download_waiting] job_id=%s stagnant_sec=%.1f download_elapsed_sec=%.1f threshold_sec=%.1f",
                    getattr(self, "job_id", None),
                    stagnant_sec,
                    active_download_elapsed,
                    download_stall_warn_sec,
                )
                last_change_at = now
                continue

            if download_inflight:
                # Large workers may be occupied for minutes while normal items
                # are still waiting. Add at most one short-lived normal worker
                # per watchdog pass; WorkerManager caps the elastic pool and
                # force-expires every elastic task after ten minutes.
                normal_waiting = max(
                    0,
                    int(snapshot.get("collection_batch_queue", 0) or 0)
                    + int(snapshot.get("collection_batch_queue_buffer", 0) or 0),
                )
                large_active = sum(
                    1
                    for item in download_inflight
                    if isinstance(item, dict) and str(item.get("lane") or "").lower() == "large"
                )
                normal_active = sum(
                    1
                    for item in download_inflight
                    if isinstance(item, dict) and str(item.get("lane") or "").lower() == "normal"
                )
                manager = getattr(self, "_file_worker_manager", None)
                if manager is None and bool(getattr(self, "use_global_pool", False)):
                    try:
                        from core.crawler.global_pool import get_global_worker_pool

                        manager = get_global_worker_pool()
                    except Exception:
                        manager = None
                runtime = dict(getattr(manager, "_download_runtime_config", {}) or {}) if manager else {}
                base_large = int(runtime.get("large_workers") or 0)
                base_normal = int(runtime.get("normal_workers") or 0)
                if (
                    normal_waiting > 0
                    and base_large > 0
                    and large_active >= base_large
                    and normal_active >= base_normal
                    and manager is not None
                    and hasattr(manager, "ensure_elastic_normal_download_worker")
                ):
                    try:
                        await manager.ensure_elastic_normal_download_worker(
                            reason=(
                                f"job={getattr(self, 'job_id', None)} "
                                f"normal_waiting={normal_waiting} "
                                f"large_active={large_active}/{base_large} "
                                f"normal_active={normal_active}/{base_normal}"
                            ),
                            max_elastic_workers=2,
                            lifetime_sec=600.0,
                        )
                    except Exception as scale_exc:
                        logger.warning(
                            "[FileCrawlTrace][elastic_download_scale_failed] job_id=%s err_type=%s err=%s",
                            getattr(self, "job_id", None),
                            type(scale_exc).__name__,
                            scale_exc,
                        )
                active_lines = []
                for item in download_inflight:
                    if not isinstance(item, dict):
                        continue
                    active_lines.append(
                        "worker={worker} lane={lane} elapsed_sec={elapsed} url={url} post_url={post_url} name={name}".format(
                            worker=item.get("worker"),
                            lane=item.get("lane") or "unknown",
                            elapsed=item.get("elapsed_sec") or 0,
                            url=item.get("url") or "-",
                            post_url=item.get("post_url") or "-",
                            name=item.get("name") or "-",
                        )
                    )
                logger.warning(
                    "[FileCrawlTrace][stall_active_downloads]\njob_id=%s\nactive_count=%s\n%s",
                    getattr(self, "job_id", None),
                    len(active_lines),
                    "\n".join(active_lines) or "active_detail=unavailable",
                )
            else:
                logger.warning(
                    "[FileCrawlTrace][stall_active_downloads]\njob_id=%s\nactive_count=0\nreason=no_active_download_worker",
                    getattr(self, "job_id", None),
                )
            logger.warning(
                "[FileCrawlTrace][progress_queue_stall] job_id=%s stalled_sec=%.1f progress=%s workers=%s",
                getattr(self, "job_id", None),
                stagnant_sec,
                progress_trace,
                worker_health,
            )
            last_change_at = now

    async def _ensure_file_local_finalize_workers(self) -> None:
        """Start bounded crawler-local workers for post-download file work."""
        queue = getattr(self, "_file_local_finalize_queue", None)
        if queue is None:
            try:
                maxsize = int(os.getenv("FILE_CRAWL_LOCAL_FINALIZE_QUEUE_MAXSIZE", "100") or "100")
            except Exception:
                maxsize = 100
            queue = asyncio.Queue(maxsize=max(1, min(maxsize, 1000)))
            self._file_local_finalize_queue = queue
        tasks = getattr(self, "_file_local_finalize_tasks", None)
        if not isinstance(tasks, set):
            tasks = set()
        alive = {task for task in tasks if isinstance(task, asyncio.Task) and not task.done()}
        desired = self._file_local_finalize_worker_count()
        missing = max(0, desired - len(alive))
        if not missing:
            self._file_local_finalize_tasks = alive
            return
        is_initial_start = not tasks
        start_index = len(alive)
        new_tasks = {
            asyncio.create_task(
                self._run_file_local_finalize_worker(start_index + index + 1),
                name=f"file-local-finalize-{getattr(self, 'job_id', 'unknown')}-{start_index + index + 1}",
            )
            for index in range(missing)
        }
        for task in new_tasks:
            task.add_done_callback(
                lambda completed: self._log_file_pipeline_worker_done(completed, "local_finalize")
            )
        alive.update(new_tasks)
        self._file_local_finalize_tasks = alive
        log = logger.info if is_initial_start else logger.warning
        event = "local_finalize_workers_started" if is_initial_start else "local_finalize_workers_replenished"
        log(
            "[FileCrawlTrace][%s] job_id=%s desired=%s alive_before=%s started=%s queue_max=%s",
            event,
            getattr(self, "job_id", None),
            desired,
            desired - missing,
            missing,
            queue.maxsize,
        )

    async def _enqueue_file_local_finalize(self, item: Dict[str, Any]) -> None:
        await self._ensure_file_local_finalize_workers()
        queue = getattr(self, "_file_local_finalize_queue", None)
        if queue is None:
            raise RuntimeError("local finalize queue unavailable")
        await queue.put(item)
        info = item.get("file_info") if isinstance(item, dict) and isinstance(item.get("file_info"), dict) else {}
        self._trace_file_pipeline_transition(
            stage="local_finalize_queue_enqueued",
            url=str(info.get("url") or item.get("url") or ""),
            post_url=str(info.get("source_page") or item.get("source_page") or ""),
            file_name=str(info.get("name") or info.get("subject") or item.get("name") or ""),
        )

    async def _sync_file_after_learn_list_persist(
        self,
        *,
        info: Dict[str, Any],
        file_path: str,
        file_url: str,
        post_url: str,
        learn_list_id: Any,
    ) -> bool:
        """Expose a downloaded file only after its LEARN_LIST row is ready."""
        original_meta = info.get("original_meta") if isinstance(info.get("original_meta"), dict) else {}
        sync_meta = dict(original_meta or info)
        sync_meta.setdefault("job_id", getattr(self, "job_id", None))
        sync_meta.setdefault("db_name", getattr(self, "db_name", None))
        sync_meta.setdefault("chat_bot_id", getattr(self, "chat_bot_id", None))
        sync_meta.setdefault("url", file_url)
        sync_meta.setdefault("source_page", post_url)
        sync_meta["sync_after_download"] = bool(
            info.get("sync_after_download")
            or sync_meta.get("sync_after_download")
        )
        try:
            from core.crawler.workers.download import _sync_after_download_if_needed

            logger.info(
                "[FileStorageSync][begin] job_id=%s learn_list_id=%s file_url=%s path=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                (file_url or "")[:220],
                (file_path or "")[:260],
            )
            ok = await _sync_after_download_if_needed(sync_meta, file_path)
        except Exception as exc:
            logger.exception(
                "[FileStorageSync][failed] job_id=%s learn_list_id=%s file_url=%s error=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                (file_url or "")[:220],
                exc,
            )
            return False
        if not ok:
            logger.error(
                "[FileStorageSync][failed] job_id=%s learn_list_id=%s file_url=%s",
                getattr(self, "job_id", None),
                learn_list_id,
                (file_url or "")[:220],
            )
            return False
        self._trace_file_pipeline_transition(
            stage="storage_sync_completed",
            url=file_url,
            post_url=post_url,
            file_name=str(info.get("name") or info.get("subject") or ""),
            detail=f"learn_list_id={learn_list_id}",
        )
        return True

    async def _discard_file_crawl_local_download(self, file_path: str, *, reason: str) -> None:
        """Remove only job download staging files that will not be persisted."""
        if not file_path:
            return
        try:
            staging_root = os.path.normcase(os.path.abspath(os.path.join(_board_project_root(), "downloads")))
            candidate = os.path.normcase(os.path.abspath(file_path))
            if os.path.commonpath([staging_root, candidate]) != staging_root:
                logger.warning(
                    "[FileStorageCleanup][skipped] job_id=%s reason=outside_download_staging path=%s",
                    getattr(self, "job_id", None),
                    (file_path or "")[:260],
                )
                return
            if os.path.isfile(candidate):
                await asyncio.to_thread(os.remove, candidate)
                logger.debug(
                    "[FileStorageCleanup][removed] job_id=%s reason=%s path=%s",
                    getattr(self, "job_id", None),
                    reason,
                    candidate[:260],
                )
        except Exception as exc:
            logger.warning(
                "[FileStorageCleanup][failed] job_id=%s reason=%s path=%s error=%s",
                getattr(self, "job_id", None),
                reason,
                (file_path or "")[:260],
                exc,
            )

    async def _run_file_local_finalize_worker(self, worker_id: int) -> None:
        queue = getattr(self, "_file_local_finalize_queue", None)
        if queue is None:
            return
        while True:
            item = await queue.get()
            try:
                if not isinstance(item, dict) or self.stop_event.is_set():
                    continue
                info = dict(item.get("file_info") or {})
                file_path = str(info.get("file_path") or info.get("local_path") or "").strip()
                file_url = str(info.get("url") or "").strip()
                post_url = str(info.get("source_page") or info.get("source_url") or "").strip()
                if not file_path:
                    raise RuntimeError("local_file_path_missing")
                started = time.perf_counter()
                await wait_for_file_ready(file_path, timeout_sec=30.0, check_partial_siblings=False)
                from core.crawler.workers.download import _extract_doc_created_at_async

                info["file_created_at"] = await _extract_doc_created_at_async(file_path)
                event = dict(item)
                event["type"] = "file_saved"
                event["file_info"] = info
                await self._file_job_queues.progress_queue.put(event)
                self._trace_file_pipeline_transition(
                    stage="local_finalize_emitted",
                    url=file_url,
                    post_url=post_url,
                    file_name=str(info.get("name") or info.get("subject") or ""),
                    detail=str(event.get("type") or ""),
                )
                logger.info(
                    "[FileCrawlTrace][local_finalize_done] job_id=%s worker=%s elapsed_ms=%s url=%s post_url=%s",
                    item.get("job_id"),
                    worker_id,
                    int((time.perf_counter() - started) * 1000),
                    file_url[:220],
                    post_url[:220],
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                try:
                    info = item.get("file_info") if isinstance(item, dict) else {}
                    await self._file_job_queues.progress_queue.put(
                        {
                            "type": "download_skipped",
                            "job_id": item.get("job_id") if isinstance(item, dict) else getattr(self, "job_id", None),
                            "url": (info or {}).get("url"),
                            "source_page": (info or {}).get("source_page") or (info or {}).get("source_url"),
                            "source_url": (info or {}).get("source_page") or (info or {}).get("source_url"),
                            "name": (info or {}).get("name") or (info or {}).get("subject"),
                            "reason": "local_finalize_failed",
                            "detail": f"{type(exc).__name__}: {exc}",
                            "worker_id": worker_id,
                        }
                    )
                except Exception:
                    logger.exception(
                        "[FileCrawlTrace][local_finalize_emit_failed] job_id=%s worker=%s",
                        getattr(self, "job_id", None),
                        worker_id,
                    )
            finally:
                queue.task_done()

    async def _wait_for_file_local_finalize_drain(self) -> None:
        queue = getattr(self, "_file_local_finalize_queue", None)
        if queue is not None:
            await queue.join()

    async def _stop_file_local_finalize_workers(self, *, graceful: bool) -> None:
        queue = getattr(self, "_file_local_finalize_queue", None)
        tasks = list(getattr(self, "_file_local_finalize_tasks", set()) or set())
        if graceful and queue is not None:
            await queue.join()
        if not graceful and queue is not None:
            while True:
                try:
                    queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
        for task in tasks:
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._file_local_finalize_tasks = set()
        self._file_local_finalize_queue = None

    @log_calls
    async def _run_file_progress_loop(self) -> None:
        """Drain download events quickly, then hand them to bounded save workers."""
        if not self._file_job_queues:
            return
        progress_queue = self._file_job_queues.progress_queue
        save_queue = getattr(self, "_file_save_queue", None)
        if save_queue is None:
            return
        while True:
            try:
                item = await asyncio.wait_for(progress_queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                if self.stop_event.is_set() and progress_queue.empty():
                    return
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[FileCrawlTrace][progress_dispatch_get_failed] job_id=%s", getattr(self, "job_id", None))
                return
            try:
                await save_queue.put(item)
                info = item.get("file_info") if isinstance(item, dict) and isinstance(item.get("file_info"), dict) else {}
                self._trace_file_pipeline_transition(
                    stage="save_queue_enqueued",
                    url=str(info.get("url") or item.get("url") or ""),
                    post_url=str(info.get("source_page") or item.get("source_page") or ""),
                    file_name=str(info.get("name") or info.get("subject") or item.get("name") or ""),
                    detail=str(item.get("type") or ""),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[FileCrawlTrace][progress_dispatch_enqueue_failed] job_id=%s", getattr(self, "job_id", None))
            finally:
                progress_queue.task_done()
    @log_calls
    async def _run_file_save_loop(self) -> None:
        """save_queue에서 file_saved / download_skipped 이벤트를 받아 파일 저장 통계를 갱신한다."""
        save_queue = getattr(self, "_file_save_queue", None)
        if save_queue is None:
            return
        active_progress = getattr(self, "_file_progress_active", None)
        if not isinstance(active_progress, dict):
            active_progress = {}
            self._file_progress_active = active_progress

        current_task = asyncio.current_task()
        save_worker_name = current_task.get_name() if current_task is not None else "file-progress-save"
        logger.info(
            "[FileSaveTrace][worker_started] job_id=%s worker=%s queue_max=%s",
            getattr(self, "job_id", None),
            save_worker_name,
            getattr(save_queue, "maxsize", None),
        )

        while True:
            trace_key = ""
            trace_started_at = 0.0
            try:
                item = await asyncio.wait_for(save_queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                try:
                    if self.stop_event.is_set() and save_queue.empty():
                        break
                except Exception:
                    if self.stop_event.is_set():
                        break
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(
                    "[FileCrawlTrace][save_worker_get_failed] job_id=%s worker=%s err_type=%s err=%s",
                    getattr(self, "job_id", None),
                    save_worker_name,
                    type(exc).__name__,
                    exc,
                )
                return

            current_task = asyncio.current_task()
            trace_key = current_task.get_name() if current_task is not None else f"progress-{id(item)}"
            trace_info = item.get("file_info") if isinstance(item, dict) and isinstance(item.get("file_info"), dict) else {}
            trace_started_at = time.monotonic()
            active_progress[trace_key] = {
                "started_at": trace_started_at,
                "stage": "save_item_taken",
                "type": str(item.get("type") if isinstance(item, dict) else type(item).__name__),
                "url": str(trace_info.get("url") or (item.get("url") if isinstance(item, dict) else "") or "")[:220],
                "post_url": str(trace_info.get("source_page") or (item.get("source_page") if isinstance(item, dict) else "") or "")[:220],
            }
            logger.info(
                "[FileSaveTrace][item_taken] job_id=%s worker=%s event=%s url=%s queue_after_take=%s unfinished_after_take=%s",
                getattr(self, "job_id", None), trace_key, active_progress[trace_key]["type"], active_progress[trace_key]["url"],
                save_queue.qsize(), int(getattr(save_queue, "_unfinished_tasks", 0) or 0),
            )
            logger.info(
                "[FileSaveTrace][item_payload] job_id=%s worker=%s event=%s post_url=%s file_url=%s file=%s path=%s size=%s reason=%s",
                getattr(self, "job_id", None),
                trace_key,
                active_progress[trace_key]["type"],
                (trace_info.get("source_page") or trace_info.get("source_url") or (item.get("source_page") if isinstance(item, dict) else "") or "")[:220],
                active_progress[trace_key]["url"],
                _content_author_debug_value(trace_info.get("name") or trace_info.get("subject") or (item.get("name") if isinstance(item, dict) else "")),
                (str(trace_info.get("file_path") or trace_info.get("local_path") or (item.get("file_path") if isinstance(item, dict) else "") or "")[:260]),
                trace_info.get("size") if trace_info else (item.get("size") if isinstance(item, dict) else None),
                (trace_info.get("reason") or (item.get("reason") if isinstance(item, dict) else "") or "")[:160],
            )
            self._trace_file_pipeline_transition(
                stage="save_worker_taken",
                url=active_progress[trace_key]["url"],
                post_url=active_progress[trace_key]["post_url"],
                file_name=str(trace_info.get("name") or trace_info.get("subject") or ""),
                detail=active_progress[trace_key]["type"],
            )
            skip_pq_task_done = False
            evt_type = None
            try:
                if not isinstance(item, dict):
                    continue
                evt_type = item.get("type")

                if evt_type == "download_local_saved":
                    await self._enqueue_file_local_finalize(item)
                    logger.info(
                        "[FileCrawlTrace][local_finalize_enqueued] job_id=%s url=%s post_url=%s",
                        item.get("job_id"),
                        ((item.get("file_info") or {}).get("url") or "")[:220],
                        ((item.get("file_info") or {}).get("source_page") or "")[:220],
                    )
                    continue

                if evt_type == "file_saved":
                    t_fs0 = time.perf_counter()
                    info = item.get("file_info") or {}
                    url = (info.get("url") or "").strip()
                    logger.info(
                        "[파일크롤링추적][다운로드저장완료] 파일URL=%s\n작업ID=%s DB=%s 파일=%s 저장경로=%s 용량=%s 게시물URL=%s",
                        (url or "")[:220],
                        getattr(self, "job_id", None),
                        getattr(self, "db_name", None),
                        _content_author_debug_value(info.get("name") or info.get("subject")),
                        (str(info.get("file_path") or info.get("local_path") or "")[:260]),
                        info.get("size"),
                        (str(info.get("source_page") or info.get("source_url") or "")[:220]),
                    )
                    _file_multi_attach_debug(
                        "[FileMultiAttachDebug][progress.file_saved] job_id=%s item_job=%s url=%s name=%r path=%s size=%s",
                        getattr(self, "job_id", None),
                        item.get("job_id"),
                        (url or "")[:220],
                        _content_author_debug_value(info.get("name") or info.get("subject")),
                        (str(info.get("file_path") or info.get("local_path") or "")[:260]),
                        info.get("size"),
                    )
                    _file_dashboard_download_debug(
                        "progress.file_saved job_id=%s item_job=%s url=%s name=%s path=%s size=%s source_page=%s",
                        getattr(self, "job_id", None),
                        item.get("job_id"),
                        (url or "")[:220],
                        _content_author_debug_value(info.get("name") or info.get("subject")),
                        (str(info.get("file_path") or info.get("local_path") or "")[:260]),
                        info.get("size"),
                        (str(info.get("source_page") or "")[:220]),
                    )
                    _log_file_url_status(
                        stage="download_save",
                        status="file_saved_event",
                        process_url=url,
                        post_url=info.get("source_page") or info.get("source_url") or "",
                        file_url=url,
                        selected="yes",
                        saved="pending",
                        learn="skipped" if bool(getattr(self, "file_pipeline_skip_learning", False)) else "pending",
                        name=info.get("name") or info.get("subject") or "",
                        job_id=getattr(self, "job_id", ""),
                        db_name=getattr(self, "db_name", ""),
                    )
                    if _content_author_debug_enabled():
                        original_meta = info.get("original_meta") if isinstance(info.get("original_meta"), dict) else {}
                        logger.debug(
                            "[ContentAuthorDebug][progress.file_saved] job_id=%s item_job=%s url=%s name=%r author=%r content_author=%r department=%r kind=%r raw=%r original_author=%r source_page=%s",
                            getattr(self, "job_id", None),
                            item.get("job_id"),
                            (url or "")[:220],
                            _content_author_debug_value(info.get("name") or info.get("subject")),
                            _content_author_debug_value(info.get("author")),
                            _content_author_debug_value(info.get("content_author")),
                            _content_author_debug_value(info.get("department")),
                            _content_author_debug_value(info.get("author_kind")),
                            _content_author_debug_value(info.get("author_raw")),
                            _content_author_debug_value(original_meta.get("content_author") or original_meta.get("author")),
                            (str(info.get("source_page") or "")[:220]),
                        )
                    url_key = (
                        canonicalize_attachment_url_for_learn_list(url)
                        or canonicalize_url_for_dedup(url)
                        or url
                    )
                    file_path = info.get("file_path") or info.get("local_path")
                    save_key = url_key or (str(file_path).strip() if file_path else "")
                    
                    file_name = _resolve_downloaded_file_subject(info, file_path)
                    file_size = 0
                    path_ok = bool(file_path and os.path.isfile(file_path))
                    logger.info(
                        "[FilePersist][progress_received] job_id=%s db=%s post_url=%s file_url=%s file=%s path=%s path_exists=%s",
                        getattr(self, "job_id", None),
                        getattr(self, "db_name", None),
                        (info.get("source_page") or info.get("source_url") or "")[:220],
                        (url or "")[:220],
                        file_name,
                        (file_path or "")[:260],
                        path_ok,
                    )
                    if file_path and not path_ok:
                        for _retry in range(5):
                            await asyncio.sleep(0.05)
                            if os.path.isfile(file_path):
                                path_ok = True
                                break
                    if path_ok:
                        try:
                            file_size = os.path.getsize(file_path)
                        except OSError:
                            file_size = 0
                    if file_path and path_ok:
                        file_ready_started = time.perf_counter()
                        logger.info(
                            "[FilePersist][file_ready_check_begin] job_id=%s file_url=%s path=%s",
                            getattr(self, "job_id", None),
                            (url or "")[:220],
                            (file_path or "")[:260],
                        )
                        try:
                            file_size = await wait_for_file_ready(file_path, timeout_sec=30.0, check_partial_siblings=False)
                            logger.info(
                                "[FilePersist][file_ready_check_done] job_id=%s file_url=%s elapsed_ms=%s size=%s",
                                getattr(self, "job_id", None),
                                (url or "")[:220],
                                int((time.perf_counter() - file_ready_started) * 1000),
                                file_size,
                            )
                            self._trace_file_pipeline_transition(
                                stage="file_ready",
                                url=url,
                                post_url=str(info.get("source_page") or info.get("source_url") or ""),
                                file_name=file_name,
                                detail=f"size={file_size}",
                            )
                            logger.info(
                                "[FilePersist][storage_ready_for_persist] job_id=%s file_url=%s size=%s",
                                getattr(self, "job_id", None),
                                (url or "")[:220],
                                file_size,
                            )
                        except Exception as exc:
                            path_ok = False
                            file_size = 0
                            logger.warning(
                                "[FilePersist][file_ready_check_failed] job_id=%s file_url=%s path=%s elapsed_ms=%s err=%s",
                                getattr(self, "job_id", None),
                                (url or "")[:200],
                                (file_path or "")[:300],
                                int((time.perf_counter() - file_ready_started) * 1000),
                                exc,
                            )
                    if file_path and not path_ok:
                        logger.warning(
                            "[FilePersist][file_path_unavailable] job_id=%s file_url=%s path=%s",
                            getattr(self, "job_id", None),
                            (url or "")[:200],
                            (file_path or "")[:300],
                        )
                    else:
                        logger.debug(
                            "[file_crawl][board][file] save_stage_verify | job_id=%s path_ok=%s size=%s path=%s",
                            getattr(self, "job_id", None),
                            path_ok,
                            file_size,
                            (file_path or "")[:260],
                        )
                    if _file_pipeline_bottleneck_log_enabled():
                        logger.debug(
                            "[Bottleneck][progress_loop] file_saved path/verify %sms | item_job=%s wf_job=%s url=%s path_ok=%s",
                            int((time.perf_counter() - t_fs0) * 1000),
                            item.get("job_id"),
                            getattr(self, "job_id", None),
                            (url or "")[:180],
                            path_ok,
                        )

                    logger.info(
                        "[FilePersist][persist_begin] job_id=%s worker=%s db=%s post_url=%s file_url=%s file=%s size=%s path_ok=%s path=%s",
                        getattr(self, "job_id", None),
                        trace_key,
                        getattr(self, "db_name", None),
                        (info.get("source_page") or info.get("source_url") or "")[:220],
                        (url or "")[:220],
                        file_name,
                        file_size,
                        path_ok,
                        (file_path or "")[:260],
                    )

                    if save_key and file_path and path_ok:
                        if self._is_file_pg_duplicate(file_name, file_size):
                            # Metadata-only repair must never occupy every save
                            # worker.  PG can be unavailable even though the
                            # duplicate decision was already made in memory.
                            active_progress[trace_key]["stage"] = "pg_duplicate_source_url_backfill"
                            self._trace_file_pipeline_transition(
                                stage="pg_duplicate_source_url_backfill_begin",
                                url=url,
                                post_url=str(info.get("source_page") or info.get("source_url") or ""),
                                file_name=file_name,
                                detail=f"size={file_size}",
                            )
                            try:
                                backfill_count = await asyncio.wait_for(
                                    self._backfill_file_pg_duplicate_source_url_if_missing(
                                        file_name=file_name,
                                        file_size=file_size,
                                        source_url=info.get("source_page") or info.get("source_url"),
                                    ),
                                    timeout=10.0,
                                )
                            except asyncio.TimeoutError:
                                logger.warning(
                                    "[FilePgDuplicate][source_url_backfill_timeout] job_id=%s timeout_sec=10.0 file=%s size=%s post_url=%s file_url=%s",
                                    getattr(self, "job_id", None),
                                    file_name,
                                    file_size,
                                    (info.get("source_page") or info.get("source_url") or "")[:220],
                                    (url or "")[:220],
                                )
                                self._trace_file_pipeline_transition(
                                    stage="pg_duplicate_source_url_backfill_timeout",
                                    url=url,
                                    post_url=str(info.get("source_page") or info.get("source_url") or ""),
                                    file_name=file_name,
                                    detail="timeout_sec=10.0",
                                )
                            else:
                                self._trace_file_pipeline_transition(
                                    stage="pg_duplicate_source_url_backfill_done",
                                    url=url,
                                    post_url=str(info.get("source_page") or info.get("source_url") or ""),
                                    file_name=file_name,
                                    detail=f"updated_rows={backfill_count}",
                                )
                            logger.info(
                                "[FilePgDuplicate][skip] job_id=%s phase=post_download file=%s size=%s post_url=%s file_url=%s reason=pg_learned_match",
                                getattr(self, "job_id", None),
                                file_name,
                                file_size,
                                (info.get("source_page") or info.get("source_url") or "")[:220],
                                (url or "")[:220],
                            )
                            self._record_job_result_stage(
                                url=save_key,
                                stage="persist",
                                status="skipped",
                                reason="pg_learned_match_post_download",
                                source_url=info.get("source_page") or info.get("source_url"),
                                file_url=url,
                                file_name=file_name,
                                file_path=file_path,
                            )
                            _log_file_url_status(
                                stage="learn_list_persist",
                                status="skipped",
                                process_url=url,
                                post_url=info.get("source_page") or info.get("source_url") or "",
                                file_url=url,
                                selected="yes",
                                saved="yes",
                                learn="skipped",
                                reason="pg_learned_match_post_download",
                                name=file_name,
                                job_id=getattr(self, "job_id", ""),
                                db_name=getattr(self, "db_name", ""),
                            )
                            await self._record_file_persist_outcome(
                                outcome="skipped",
                                reason="pg_learned_match_post_download",
                                file_url=url,
                                file_name=file_name,
                            )
                            await self._discard_file_crawl_local_download(
                                file_path,
                                reason="pg_learned_match_post_download",
                            )
                            await self._mark_save_skipped(url=save_key)
                            continue
                        throttle_sec = _file_crawl_save_throttle_seconds()
                        if throttle_sec > 0:
                            await asyncio.sleep(throttle_sec)
                        pre_extracted_text = None
                        if not bool(getattr(self, "file_pipeline_skip_learning", False)):
                            pre_extracted_text = await self._extract_text_from_saved_file_for_learning_guarded(
                                file_path,
                                url=url,
                                file_name=file_name,
                            )
                        if pre_extracted_text:
                            active_progress[trace_key]["stage"] = "simhash_pre_persist"
                            pre_extracted_text, simhash_identity = await self._prepare_file_simhash_before_persist(
                                info=info,
                                file_name=file_name,
                                file_size=file_size,
                                file_url=url,
                                extracted_text=pre_extracted_text,
                            )
                            if simhash_identity:
                                # MariaDB duplicate matching is hash-only.  Keep the
                                # cross-job claim on that same identity so concurrent
                                # files with differing size metadata cannot both insert.
                                hash_key = build_file_simhash_claim_key(simhash_identity[0]) or simhash_identity[0]
                                hash_claim_acquired = await try_acquire_cross_job_claim(
                                    "file_simhash",
                                    str(getattr(self, "db_name", "") or ""),
                                    hash_key,
                                    str(getattr(self, "job_id", "") or ""),
                                )
                                matched_maria_row = await self._find_file_maria_simhash_duplicate(
                                    simhash_decimal=simhash_identity[0],
                                    normalized_length=simhash_identity[1],
                                    file_size=simhash_identity[2],
                                )
                                if matched_maria_row or not hash_claim_acquired:
                                    duplicate_reason = (
                                        "maria_simhash_match_pre_persist"
                                        if matched_maria_row
                                        else "simhash_claim_conflict_pre_persist"
                                    )
                                    async with self._stats_lock:
                                        self.stats["file_simhash_duplicate_skip_count"] = int(
                                            self.stats.get("file_simhash_duplicate_skip_count", 0) or 0
                                        ) + 1
                                        self._bump_stats_revision_locked()
                                    self._record_file_simhash_gate_result(
                                        decision="duplicate_skip",
                                        reason=duplicate_reason,
                                        simhash=simhash_identity[0],
                                        file_name=file_name,
                                        file_url=url,
                                        source_url=str(info.get("source_page") or info.get("source_url") or ""),
                                        matched_learn_list_id=(matched_maria_row or {}).get("id"),
                                        matched_status=(matched_maria_row or {}).get("status"),
                                    )
                                    logger.info(
                                        "[FileSimHash][pre_persist_duplicate_skip] job_id=%s reason=%s simhash=%s matched_learn_list_id=%s matched_status=%s file=%s file_url=%s",
                                        getattr(self, "job_id", None),
                                        duplicate_reason,
                                        simhash_identity[0],
                                        (matched_maria_row or {}).get("id") if matched_maria_row else "-",
                                        (matched_maria_row or {}).get("status") if matched_maria_row else "-",
                                        (file_name or "")[:160],
                                        (url or "")[:220],
                                    )
                                    self._record_job_result_stage(
                                        url=save_key,
                                        stage="persist",
                                        status="skipped",
                                        reason=duplicate_reason,
                                        source_url=info.get("source_page") or info.get("source_url"),
                                        file_url=url,
                                        file_name=file_name,
                                        file_path=file_path,
                                    )
                                    await self._record_file_persist_outcome(
                                        outcome="skipped",
                                        reason=duplicate_reason,
                                        file_url=url,
                                        file_name=file_name,
                                    )
                                    await self._discard_file_crawl_local_download(
                                        file_path,
                                        reason=duplicate_reason,
                                    )
                                    await self._mark_save_skipped(url=save_key)
                                    continue
                                logger.info(
                                    "[FileSimHash][pre_persist_gate] job_id=%s decision=allow_insert simhash=%s hash_claim_acquired=%s file=%s file_url=%s",
                                    getattr(self, "job_id", None),
                                    simhash_identity[0],
                                    hash_claim_acquired,
                                    (file_name or "")[:160],
                                    (url or "")[:220],
                                )
                                async with self._stats_lock:
                                    self.stats["file_simhash_gate_pass_count"] = int(
                                        self.stats.get("file_simhash_gate_pass_count", 0) or 0
                                    ) + 1
                                    self._bump_stats_revision_locked()
                                self._record_file_simhash_gate_result(
                                    decision="allow_insert",
                                    reason="maria_not_matched",
                                    simhash=simhash_identity[0],
                                    file_name=file_name,
                                    file_url=url,
                                    source_url=str(info.get("source_page") or info.get("source_url") or ""),
                                )
                            else:
                                logger.info(
                                    "[FileSimHash][pre_persist_gate] job_id=%s decision=allow_insert_without_hash reason=simhash_unavailable file=%s file_url=%s",
                                    getattr(self, "job_id", None),
                                    (file_name or "")[:160],
                                    (url or "")[:220],
                                )
                                async with self._stats_lock:
                                    self.stats["file_simhash_gate_unavailable_allow_count"] = int(
                                        self.stats.get("file_simhash_gate_unavailable_allow_count", 0) or 0
                                    ) + 1
                                    self._bump_stats_revision_locked()
                                self._record_file_simhash_gate_result(
                                    decision="allow_insert_without_hash",
                                    reason="simhash_unavailable",
                                    file_name=file_name,
                                    file_url=url,
                                    source_url=str(info.get("source_page") or info.get("source_url") or ""),
                                )
                        else:
                            logger.info(
                                "[FileSimHash][pre_persist_gate] job_id=%s decision=allow_insert_without_hash reason=empty_extracted_text file=%s file_url=%s",
                                getattr(self, "job_id", None),
                                (file_name or "")[:160],
                                (url or "")[:220],
                            )
                            async with self._stats_lock:
                                self.stats["file_simhash_gate_unavailable_allow_count"] = int(
                                    self.stats.get("file_simhash_gate_unavailable_allow_count", 0) or 0
                                ) + 1
                                self._bump_stats_revision_locked()
                            self._record_file_simhash_gate_result(
                                decision="allow_insert_without_hash",
                                reason="empty_extracted_text",
                                file_name=file_name,
                                file_url=url,
                                source_url=str(info.get("source_page") or info.get("source_url") or ""),
                            )
                        active_progress[trace_key]["stage"] = "learn_list_ensure"
                        self._trace_file_pipeline_transition(
                            stage="learn_list_ensure_begin",
                            url=url,
                            post_url=str(info.get("source_page") or info.get("source_url") or ""),
                            file_name=file_name,
                            detail=f"size={file_size}",
                        )
                        logger.info(
                            "[FilePersist][learn_list_ensure_begin] job_id=%s db=%s enable_db_save=%s post_url=%s file_url=%s file=%s size=%s",
                            getattr(self, "job_id", None),
                            getattr(self, "db_name", None),
                            bool(getattr(self, "enable_db_save", True)),
                            (info.get("source_page") or info.get("source_url") or "")[:220],
                            (url or "")[:220],
                            file_name,
                            file_size,
                        )
                        t_ll0 = time.perf_counter()
                        ensure_timeout_sec = _file_learn_list_ensure_timeout_sec()
                        try:
                            row_out = await run_learn_list_ensure(
                                lambda: self._ensure_learn_list_row_for_file_save(
                                    info=info,
                                    file_path=file_path,
                                    file_name=file_name,
                                    file_size=file_size,
                                ),
                                ensure_timeout_sec,
                            )
                        except asyncio.TimeoutError:
                            logger.error(
                                "[FileSaveTrace][learn_list_ensure_timeout] job_id=%s db=%s timeout_sec=%.1f post_url=%s file_url=%s file=%s",
                                getattr(self, "job_id", None),
                                getattr(self, "db_name", None),
                                ensure_timeout_sec,
                                (info.get("source_page") or info.get("source_url") or "")[:220],
                                (url or "")[:220],
                                file_name,
                            )
                            try:
                                self._record_job_result_stage(
                                    url=save_key,
                                    stage="persist",
                                    status="failed",
                                    reason="learn_list_ensure_timeout",
                                    source_url=info.get("source_page") or info.get("source_url"),
                                    file_url=url,
                                    file_name=file_name,
                                    file_path=file_path,
                                )
                            except Exception:
                                pass
                            await self._record_file_persist_outcome(
                                outcome="failed",
                                reason="learn_list_ensure_timeout",
                                file_url=url,
                                file_name=file_name,
                            )
                            await self._discard_file_crawl_local_download(
                                file_path,
                                reason="learn_list_ensure_timeout",
                            )
                            await self._mark_save_done(url=save_key, ok=False)
                            await self._mark_study_done(url=save_key, outcome="skipped")
                            logger.info(
                                "[FilePersist][persist_result] job_id=%s worker=%s result=failed reason=learn_list_ensure_timeout post_url=%s file_url=%s file=%s",
                                getattr(self, "job_id", None), trace_key,
                                (info.get("source_page") or info.get("source_url") or "")[:220],
                                (url or "")[:220], file_name,
                            )
                            if self.progress_callback:
                                self.progress_callback(self.get_stats())
                            continue
                        active_progress[trace_key]["stage"] = "learn_list_ensure_done"
                        logger.info(
                            "[FilePersist][learn_list_ensure_done] job_id=%s worker=%s elapsed_ms=%s row_id=%s post_url=%s file_url=%s file=%s",
                            getattr(self, "job_id", None),
                            trace_key,
                            int((time.perf_counter() - t_ll0) * 1000),
                            row_out,
                            (info.get("source_page") or info.get("source_url") or "")[:220],
                            (url or "")[:220],
                            file_name,
                        )
                        if _file_pipeline_bottleneck_log_enabled():
                            logger.debug(
                                "[Bottleneck][progress_loop] insert_into_learn_list(ensure) %sms row_out=%s url=%s",
                                int((time.perf_counter() - t_ll0) * 1000),
                                row_out,
                                (url or "")[:160],
                            )
                        if row_out is None:
                            logger.error(
                                "[FilePersist][learn_list_row_missing] job_id=%s db=%s table=%s enable_db_save=%s post_url=%s file_url=%s file=%s",
                                getattr(self, "job_id", None),
                                getattr(self, "db_name", None),
                                info.get("_learn_list_table_name") or "unresolved",
                                bool(getattr(self, "enable_db_save", True)),
                                (info.get("source_page") or info.get("source_url") or "")[:220],
                                (url or "")[:220],
                                file_name,
                            )
                            _log_file_url_status(
                                stage="learn_list_persist",
                                status="error",
                                process_url=url,
                                post_url=info.get("source_page") or info.get("source_url") or "",
                                file_url=url,
                                selected="yes",
                                saved="yes",
                                learn="not_started",
                                reason="learn_list_no_row",
                                name=file_name,
                                job_id=getattr(self, "job_id", ""),
                                db_name=getattr(self, "db_name", ""),
                            )
                            logger.debug(
                                "[file_crawl][board][file] save_count skipped: LEARN_LIST save returned no row id | job_id=%s url=%s",
                                getattr(self, "job_id", None),
                                (url or "")[:200],
                            )
                            try:
                                self._record_job_result_stage(
                                    url=save_key,
                                    stage="persist",
                                    status="failed",
                                    reason="learn_list_no_row",
                                    source_url=info.get("source_page") or info.get("source_url"),
                                    file_url=url,
                                    file_name=file_name,
                                    file_path=file_path,
                                )
                            except Exception:
                                pass
                            await self._record_file_persist_outcome(
                                outcome="failed",
                                reason="learn_list_no_row",
                                file_url=url,
                                file_name=file_name,
                            )
                            await self._discard_file_crawl_local_download(
                                file_path,
                                reason="learn_list_no_row",
                            )
                            await self._mark_save_done(url=save_key, ok=False)
                            await self._mark_study_done(url=save_key, outcome="skipped")
                            logger.info(
                                "[FilePersist][persist_result] job_id=%s worker=%s result=failed reason=learn_list_no_row post_url=%s file_url=%s file=%s",
                                getattr(self, "job_id", None), trace_key,
                                (info.get("source_page") or info.get("source_url") or "")[:220],
                                (url or "")[:220], file_name,
                            )
                            if self.progress_callback:
                                self.progress_callback(self.get_stats())
                            continue

                        try:
                            row_id_int = int(row_out)
                        except Exception:
                            row_id_int = 0
                        if row_id_int <= 0:
                            logger.error(
                                "[FilePersist][learn_list_invalid_row_id] job_id=%s db=%s table=%s row_id=%s enable_db_save=%s post_url=%s file_url=%s file=%s",
                                getattr(self, "job_id", None),
                                getattr(self, "db_name", None),
                                info.get("_learn_list_table_name") or "unresolved",
                                row_out,
                                bool(getattr(self, "enable_db_save", True)),
                                (info.get("source_page") or info.get("source_url") or "")[:220],
                                (url or "")[:220],
                                file_name,
                            )
                            try:
                                self._record_job_result_stage(
                                    url=save_key,
                                    stage="persist",
                                    status="failed",
                                    reason="learn_list_invalid_row_id",
                                    source_url=info.get("source_page") or info.get("source_url"),
                                    file_url=url,
                                    file_name=file_name,
                                    file_path=file_path,
                                )
                            except Exception:
                                pass
                            await self._record_file_persist_outcome(
                                outcome="failed",
                                reason="learn_list_invalid_row_id",
                                file_url=url,
                                file_name=file_name,
                                row_id=row_out,
                            )
                            await self._discard_file_crawl_local_download(
                                file_path,
                                reason="learn_list_invalid_row_id",
                            )
                            await self._mark_save_done(url=save_key, ok=False)
                            await self._mark_study_done(url=save_key, outcome="skipped")
                            logger.info(
                                "[FilePersist][persist_result] job_id=%s worker=%s result=failed reason=learn_list_invalid_row_id row_id=%s post_url=%s file_url=%s file=%s",
                                getattr(self, "job_id", None), trace_key, row_out,
                                (info.get("source_page") or info.get("source_url") or "")[:220],
                                (url or "")[:220], file_name,
                            )
                            if self.progress_callback:
                                self.progress_callback(self.get_stats())
                            continue
                        logger.info(
                            "[FilePersist][learn_list_row_ready] job_id=%s db=%s table=%s row_id=%s post_url=%s file_url=%s file=%s",
                            getattr(self, "job_id", None),
                            getattr(self, "db_name", None),
                            info.get("_learn_list_table_name") or "unresolved",
                            row_id_int,
                            (info.get("source_page") or info.get("source_url") or "")[:220],
                            (url or "")[:220],
                            file_name,
                        )
                        if row_id_int > 0:
                            saved_ids = getattr(self, "_file_saved_learn_list_ids", None)
                            if not isinstance(saved_ids, set):
                                saved_ids = set()
                                self._file_saved_learn_list_ids = saved_ids
                            saved_ids.add(row_id_int)

                        duplicate_existing = bool(info.get("learn_list_duplicate"))
                        persistence_action = "update" if duplicate_existing else "insert"
                        duplicate_learned = bool(info.get("learn_list_reused_learned")) or (
                            duplicate_existing
                            and str(info.get("learn_list_existing_status") or "").strip().upper() == "Y"
                        )
                        if duplicate_existing and not duplicate_learned:
                            # A same-name/same-size LEARN_LIST row is an
                            # identity match, not a terminal skip.  Continue
                            # through the learning request so PG's
                            # content+chunk_num UPSERT overwrites stale chunks.
                            logger.info(
                                "[FilePersist][duplicate_reprocess] job_id=%s worker=%s row_id=%s prior_status=%s file=%s size=%s file_url=%s",
                                getattr(self, "job_id", None),
                                trace_key,
                                row_out,
                                info.get("learn_list_existing_status") or "-",
                                file_name,
                                file_size,
                                (url or "")[:220],
                            )
                            info["learn_list_duplicate"] = False
                            info["learn_list_reused_learned"] = False
                            duplicate_existing = False
                            duplicate_learned = False
                        if duplicate_existing and duplicate_learned:
                            # Category-only backfill of a learned duplicate is
                            # not a new save or learning result.  Do not touch
                            # counters, stats revision, or progress_callback;
                            # otherwise Redis would report work that was only
                            # metadata repair on an existing row.
                            logger.info(
                                "[FileCateBackfill][learned_duplicate_no_progress] job_id=%s url=%s learn_list_id=%s status=%s",
                                getattr(self, "job_id", None),
                                (url or "")[:180],
                                row_out,
                                info.get("learn_list_existing_status"),
                            )
                            await self._record_file_persist_outcome(
                                outcome="skipped",
                                reason="duplicate_reuse_learned",
                                file_url=url,
                                file_name=file_name,
                                row_id=row_out,
                                persistence_action="existing_row",
                            )
                            await self._discard_file_crawl_local_download(
                                file_path,
                                reason="duplicate_reuse_learned",
                            )
                            await self._mark_save_skipped(url=save_key)
                            continue
                        if persistence_action == "insert":
                            # insert_into_learn_list creates files with status=N.
                            # Keep this earlier DB-row boundary distinct from
                            # storage completion and learning callback success.
                            async with self._stats_lock:
                                self.stats["file_learn_list_status_n_insert_count"] = (
                                    int(self.stats.get("file_learn_list_status_n_insert_count", 0) or 0) + 1
                                )
                                self._bump_stats_revision_locked()
                        if bool(getattr(self, "enable_db_save", True)) and row_id_int > 0:
                            active_progress[trace_key]["stage"] = "storage_sync_after_persist"
                            storage_synced = await self._sync_file_after_learn_list_persist(
                                info=info,
                                file_path=file_path,
                                file_url=url,
                                post_url=str(info.get("source_page") or info.get("source_url") or ""),
                                learn_list_id=row_out,
                            )
                            if not storage_synced:
                                try:
                                    self._record_job_result_stage(
                                        url=save_key,
                                        stage="storage",
                                        status="failed",
                                        reason="storage_sync_failed",
                                        source_url=info.get("source_page") or info.get("source_url"),
                                        file_url=url,
                                        file_name=file_name,
                                        file_path=file_path,
                                        db_id=row_out,
                                    )
                                except Exception:
                                    pass
                                await self._record_file_persist_outcome(
                                    outcome="failed",
                                    reason="storage_sync_failed",
                                    file_url=url,
                                    file_name=file_name,
                                    row_id=row_out,
                                    persistence_action=persistence_action,
                                )
                                await self._discard_file_crawl_local_download(
                                    file_path,
                                    reason="storage_sync_failed",
                                )
                                await self._mark_save_done(url=save_key, ok=False)
                                await self._mark_study_done(url=save_key, outcome="skipped")
                                _log_file_url_status(
                                    stage="storage_sync",
                                    status="failed",
                                    process_url=url,
                                    post_url=info.get("source_page") or info.get("source_url") or "",
                                    file_url=url,
                                    selected="yes",
                                    saved="no",
                                    learn="skipped",
                                    reason="storage_sync_failed",
                                    name=file_name,
                                    learn_list_id=row_out,
                                    job_id=getattr(self, "job_id", ""),
                                    db_name=getattr(self, "db_name", ""),
                                )
                                if self.progress_callback:
                                    self.progress_callback(self.get_stats())
                                continue
                        try:
                            self._record_job_result_stage(
                                url=save_key,
                                stage="save" if persistence_action == "insert" else "persist",
                                status="success",
                                source_url=info.get("source_page") or info.get("source_url"),
                                file_url=url,
                                file_name=file_name,
                                file_path=file_path,
                                db_id=row_out,
                            )
                        except Exception:
                            pass
                        async with self._stats_lock:
                            counter_key = (
                                "file_save_update_count"
                                if persistence_action == "update"
                                else "file_save_insert_count"
                            )
                            self.stats[counter_key] = int(self.stats.get(counter_key, 0) or 0) + 1
                            self._bump_stats_revision_locked()
                        await self._record_file_persist_outcome(
                            outcome=persistence_action,
                            reason="learn_list_row_ready",
                            file_url=url,
                            file_name=file_name,
                            row_id=row_out,
                            persistence_action=persistence_action,
                        )
                        if persistence_action == "insert":
                            await self._confirm_file_selection_for_persist(
                                info=info,
                                url=save_key,
                                file_name=file_name,
                                file_size=file_size,
                                row_id=row_out,
                            )
                            await self._mark_save_done(url=save_key, ok=True)
                            try:
                                append_stage_urls(
                                    stage="save",
                                    urls=[{"url": save_key, "file_path": file_path}],
                                    job_id=getattr(self, "job_id", None),
                                    db_name=getattr(self, "db_name", None),
                                )
                            except Exception as exc:
                                logger.warning(
                                    "[FilePersist][insert_stage_append_failed] job_id=%s file_url=%s err=%s",
                                    getattr(self, "job_id", None),
                                    (url or "")[:220],
                                    exc,
                                )
                            if self.progress_callback:
                                self.progress_callback(self.get_stats())
                        else:
                            await self._mark_save_skipped(url=save_key)
                        logger.info(
                            "[FilePersist][persist_result] job_id=%s worker=%s result=saved persistence=%s db=%s table=%s row_id=%s post_url=%s file_url=%s file=%s size=%s",
                            getattr(self, "job_id", None), trace_key, persistence_action,
                            getattr(self, "db_name", None), info.get("_learn_list_table_name") or "unresolved", row_out,
                            (info.get("source_page") or info.get("source_url") or "")[:220],
                            (url or "")[:220], file_name, file_size,
                        )
                        self._trace_file_pipeline_transition(
                            stage="save_completed",
                            url=url,
                            post_url=str(info.get("source_page") or info.get("source_url") or ""),
                            file_name=file_name,
                            detail=f"learn_list_id={row_out}",
                        )
                        # Prevent the same attachment from being learned twice within this job
                        # while its external learning callback is still pending.
                        self._remember_file_pg_duplicate_fingerprint(file_name, file_size)
                        _log_file_processing_trace(
                            stage="저장완료",
                            post_url=info.get("source_page") or info.get("source_url") or "",
                            file_url=url,
                            file_name=file_name,
                            selected="예",
                            saved="완료",
                            learn="스킵" if bool(getattr(self, "file_pipeline_skip_learning", False)) else "대기",
                            learn_list_id=row_out,
                        )
                        _log_file_url_status(
                            stage="learn_list_persist",
                            status="saved",
                            process_url=url,
                            post_url=info.get("source_page") or info.get("source_url") or "",
                            file_url=url,
                            selected="yes",
                            saved="yes",
                            learn="skipped" if bool(getattr(self, "file_pipeline_skip_learning", False)) else "pending",
                            name=file_name,
                            learn_list_id=row_out,
                            job_id=getattr(self, "job_id", ""),
                            db_name=getattr(self, "db_name", ""),
                        )
                        remember_completed_url(save_key, stage="save")
                        try:
                            append_stage_urls(
                                stage="collection",
                                urls=[
                                    {
                                        "url": url_key,
                                        "source_page": info.get("source_page") or url,
                                        "name": file_name,
                                    }
                                ],
                                job_id=getattr(self, "job_id", None),
                                db_name=getattr(self, "db_name", None),
                            )
                        except Exception:
                            pass

                        if self.progress_callback:
                            self.progress_callback(self.get_stats())
                        pre_learn_list_id = row_out if row_out > 0 else None
                        _log_file_url_status(
                            stage="learning",
                            status="dispatch",
                            process_url=url,
                            post_url=info.get("source_page") or info.get("source_url") or "",
                            file_url=url,
                            selected="yes",
                            saved="yes",
                            learn="skipped" if bool(getattr(self, "file_pipeline_skip_learning", False)) else "pending",
                            name=file_name,
                            learn_list_id=pre_learn_list_id,
                            job_id=getattr(self, "job_id", ""),
                            db_name=getattr(self, "db_name", ""),
                        )
                        logger.info(
                            "[파일크롤링추적][학습전달] 파일URL=%s\n작업ID=%s DB=%s 학습목록ID=%s 파일=%s 병렬=%s 학습생략=%s",
                            (url or "")[:180],
                            getattr(self, "job_id", None),
                            getattr(self, "db_name", None),
                            pre_learn_list_id,
                            os.path.basename(file_path or ""),
                            self._file_parallel_learn_enabled(),
                            bool(getattr(self, "file_pipeline_skip_learning", False)),
                        )

                        if bool(getattr(self, "file_pipeline_skip_learning", False)):
                            try:
                                await self._record_study_skip(
                                    reason="file_pipeline_skip_learning",
                                    url=url,
                                    learn_list_id=pre_learn_list_id,
                                    detail="file_pipeline_skip_learning is enabled",
                                )
                            except Exception:
                                pass
                            try:
                                self._record_job_result_stage(
                                    url=save_key,
                                    stage="study",
                                    status="skipped",
                                    reason="file_pipeline_skip_learning",
                                    source_url=info.get("source_page") or info.get("source_url"),
                                    file_url=url,
                                    file_name=file_name,
                                    file_path=file_path,
                                    db_id=pre_learn_list_id,
                                )
                            except Exception:
                                pass
                            await self._mark_study_done(url=save_key, outcome="skipped")
                            _log_file_url_status(
                                stage="learning",
                                status="skipped",
                                process_url=url,
                                post_url=info.get("source_page") or info.get("source_url") or "",
                                file_url=url,
                                selected="yes",
                                saved="yes",
                                learn="skipped",
                                reason="file_pipeline_skip_learning",
                                name=file_name,
                                learn_list_id=pre_learn_list_id,
                                job_id=getattr(self, "job_id", ""),
                                db_name=getattr(self, "db_name", ""),
                            )
                            if self.progress_callback:
                                self.progress_callback(self.get_stats())
                            logger.debug(
                                "[file_crawl][board][file] save_done (no learn) | job_id=%s url=%s path=%s learn_list_id=%s",
                                self.job_id,
                                url,
                                file_path,
                                pre_learn_list_id,
                            )
                            continue

                        await self._enqueue_file_learning_request(
                            {
                                "info": info,
                                "url": url,
                                "url_key": url_key,
                                "file_path": file_path,
                                "save_key": save_key,
                                "file_name": file_name,
                                "file_size": file_size,
                                "pre_learn_list_id": pre_learn_list_id,
                                "pre_extracted_text": pre_extracted_text,
                            }
                        )
                    else:
                        persist_reason = "file_path_unavailable" if not path_ok else "save_input_missing"
                        terminal_save_key = save_key or url_key or url
                        logger.error(
                            "[FilePersist][persist_result] job_id=%s worker=%s result=failed reason=%s post_url=%s file_url=%s file=%s path=%s path_ok=%s",
                            getattr(self, "job_id", None),
                            trace_key,
                            persist_reason,
                            (info.get("source_page") or info.get("source_url") or "")[:220],
                            (url or "")[:220],
                            file_name,
                            (file_path or "")[:260],
                            path_ok,
                        )
                        if terminal_save_key:
                            try:
                                self._record_job_result_stage(
                                    url=terminal_save_key,
                                    stage="save",
                                    status="failed",
                                    reason=persist_reason,
                                    source_url=info.get("source_page") or info.get("source_url"),
                                    file_url=url,
                                    file_name=file_name,
                                    file_path=file_path,
                                )
                            except Exception:
                                pass
                            await self._mark_save_done(url=terminal_save_key, ok=False)
                            await self._mark_study_done(url=terminal_save_key, outcome="skipped")
                        if self.progress_callback:
                            self.progress_callback(self.get_stats())
                elif evt_type == "download_deferred":
                    logger.info(
                        "[FileCrawlTrace][download_deferred_to_large_lane] job_id=%s item_job=%s url=%s post_url=%s size=%s worker=%s",
                        getattr(self, "job_id", None),
                        item.get("job_id"),
                        (item.get("url") or "")[:220],
                        (item.get("source_page") or item.get("source_url") or "")[:220],
                        item.get("declared_file_size_bytes"),
                        item.get("worker_id"),
                    )
                elif evt_type == "download_skipped":
                    url = (item.get("url") or "").strip()
                    reason = str(item.get("reason") or "").strip() or "unknown"
                    detail = str(item.get("detail") or item.get("content_type") or item.get("filename") or "").strip()
                    log_download_skipped = logger.debug if reason in {"non_doc_file", "non_doc_precheck", "non_doc_mime", "viewer_convert_url"} else logger.warning
                    log_download_skipped(
                        "[FileMultiAttachDebug][progress.download_skipped] job_id=%s item_job=%s url=%s post_url=%s reason=%s detail=%s worker=%s",
                        getattr(self, "job_id", None),
                        item.get("job_id"),
                        (url or "")[:220],
                        (item.get("source_page") or item.get("source_url") or "")[:220],
                        reason,
                        detail[:300],
                        item.get("worker_id"),
                    )
                    _file_dashboard_download_debug(
                        "progress.download_skipped job_id=%s item_job=%s url=%s reason=%s detail=%s worker=%s",
                        getattr(self, "job_id", None),
                        item.get("job_id"),
                        (url or "")[:220],
                        reason,
                        detail[:300],
                        item.get("worker_id"),
                    )
                    _log_file_url_status(
                        stage="download_save",
                        status="skipped",
                        process_url=url,
                        post_url=item.get("source_page") or item.get("source_url") or "",
                        file_url=url,
                        selected="yes",
                        saved="no",
                        learn="skipped",
                        reason=reason,
                        error=detail,
                        name=item.get("filename") or item.get("file_name") or "",
                        job_id=getattr(self, "job_id", ""),
                        db_name=getattr(self, "db_name", ""),
                    )
                    url_key = (
                        canonicalize_attachment_url_for_learn_list(url)
                        or canonicalize_url_for_dedup(url)
                        or url
                    )
                    if url_key:
                        try:
                            self._record_job_result_stage(
                                url=url_key,
                                stage="save",
                                status="failed",
                                reason=reason,
                                source_url=item.get("source_page") or item.get("source_url"),
                                file_url=url,
                                file_name=item.get("filename") or item.get("file_name") or "",
                                detail=detail,
                            )
                        except Exception:
                            pass
                        await self._mark_save_done(url=url_key, ok=False)
                        await self._mark_study_done(url=url_key, outcome="skipped")
                        try:
                            async with self._stats_lock:
                                self.stats["file_download_skipped_count"] = int(
                                    self.stats.get("file_download_skipped_count", 0) or 0
                                ) + 1
                                reason_key = re.sub(r"[^A-Za-z0-9_]+", "_", reason).strip("_").lower()[:80] or "unknown"
                                self.stats[f"file_download_skipped_{reason_key}_count"] = int(
                                    self.stats.get(f"file_download_skipped_{reason_key}_count", 0) or 0
                                ) + 1
                                samples = self.stats.get("file_download_skipped_samples")
                                if not isinstance(samples, list):
                                    samples = []
                                samples.append({"url": url[:500], "reason": reason, "detail": detail[:300]})
                                self.stats["file_download_skipped_samples"] = samples[-20:]
                        except Exception:
                            pass
                        if self.progress_callback:
                            self.progress_callback(self.get_stats())
                        log_save_skipped = logger.debug if reason in {"non_doc_file", "non_doc_precheck", "viewer_convert_url"} else logger.debug
                        log_save_skipped(
                            "[file_crawl][board][file] save_skipped | job_id=%s url=%s reason=%s",
                            self.job_id,
                            url,
                            reason,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as item_exc:
                logger.exception(
                    "[file_crawl][board][file] progress loop item error | job_id=%s event=%s err=%s",
                    getattr(self, "job_id", None),
                    evt_type,
                    item_exc,
                )
            finally:
                trace_state = dict(active_progress.get(trace_key) or {}) if trace_key else {}
                elapsed_sec = max(0.0, time.monotonic() - trace_started_at) if trace_started_at else 0.0
                if trace_key:
                    active_progress.pop(trace_key, None)
                if not skip_pq_task_done:
                    try:
                        save_queue.task_done()
                    except Exception:
                        pass
                if trace_key:
                    logger.info(
                        "[FileSaveTrace][item_done] job_id=%s worker=%s event=%s stage=%s elapsed_sec=%.2f queue_after_done=%s unfinished_after_done=%s url=%s",
                        getattr(self, "job_id", None), trace_key, evt_type, trace_state.get("stage", "unknown"), elapsed_sec,
                        save_queue.qsize(), int(getattr(save_queue, "_unfinished_tasks", 0) or 0), trace_state.get("url", ""),
                    )

    @log_calls
    async def _copy_file_to_upload_path(self, source_path: str) -> Optional[str]:
        target_path = ""
        try:
            import shutil
            from config.settings import get_fileupload_root

            mode = str(os.getenv("FILE_CRAWL_UPLOAD_COPY_MODE", "auto") or "auto").strip().lower()
            if mode in {"source", "direct", "none"}:
                return source_path

            uuid_part = str(self.chat_bot_id).split("-")[-1]
            try:
                from config.settings import get_storage_domain_for_db_name

                storage_domain = get_storage_domain_for_db_name(getattr(self, "db_name", None))
            except Exception:
                storage_domain = "unknown.han.kr"
            target_dir = os.path.normpath(os.path.join(get_fileupload_root(), storage_domain, uuid_part))
            os.makedirs(target_dir, exist_ok=True)

            file_name = os.path.basename(source_path)
            target_path = os.path.normpath(os.path.join(target_dir, file_name))

            source_exists = False
            target_exists = False
            try:
                source_exists = os.path.isfile(source_path)
            except Exception:
                source_exists = False
            try:
                target_exists = os.path.isfile(target_path)
            except Exception:
                target_exists = False
            if not source_exists:
                if target_exists:
                    logger.debug(
                        "[FileCopy] source missing; reuse existing upload file | source=%s target=%s",
                        source_path,
                        target_path,
                    )
                    return target_path
                logger.error(
                    "[FileCopyFailed] source missing before copy | source=%s target=%s",
                    source_path,
                    target_path,
                )
                return None

            try:
                if os.path.samefile(source_path, target_path):
                    return target_path
            except Exception:
                pass

            if mode in {"auto", "hardlink", "link", "hardlink_only"}:
                try:
                    if os.path.exists(target_path):
                        try:
                            os.remove(target_path)
                        except Exception:
                            if mode == "hardlink_only":
                                return None
                            raise
                    await asyncio.to_thread(os.link, source_path, target_path)
                    return target_path
                except Exception as link_exc:
                    if mode == "hardlink_only":
                        logger.debug(
                            "[FileCopyFailed] hardlink_only failed | src=%s dst=%s err=%s",
                            source_path,
                            target_path,
                            link_exc,
                        )
                        return None
                    logger.debug(
                        "[FileCopy] hardlink unavailable; fallback to copy2 | src=%s dst=%s err=%s",
                        source_path,
                        target_path,
                        link_exc,
                    )

            await asyncio.to_thread(shutil.copy2, source_path, target_path)
            return target_path
        except Exception as e:
            logger.error("[FileCopyFailed] copy failed | source=%s target=%s err=%s", source_path, target_path, e)
            return None





























