"""Shared Stage-3 ingress for attachment file crawling.

The fast attachment front ends at an explicit boundary: document candidates are
put into the production download queue here. Download, local finalization,
LEARN_LIST persistence and learning requests remain owned by the existing
pipeline workers.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Optional, Tuple

from utils.download_doc_filter import should_skip_attachment_at_scan
from utils.url import canonicalize_url_for_dedup, normalize_attachment_href

logger = logging.getLogger("backend.file.file_crawl_stage3")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _attachment_url(attachment: Dict[str, Any]) -> str:
    return _text(
        attachment.get("url")
        or attachment.get("file_url")
        or attachment.get("href")
        or attachment.get("download_url")
    )


def _attachment_name(attachment: Dict[str, Any], fallback: str) -> str:
    return _text(
        attachment.get("name")
        or attachment.get("attachment_name")
        or attachment.get("subject")
        or attachment.get("filename")
        or fallback
    )


def _canonical_key(url: str) -> str:
    raw = _text(url)
    if not raw:
        return ""
    try:
        return canonicalize_url_for_dedup(raw) or raw
    except Exception:
        return raw


def _size(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


async def _record_stage3_enqueue_progress(workflow: Any, queued_count: int) -> None:
    """Publish Stage-3 queue ingress without changing save/selection semantics."""
    if queued_count <= 0:
        return

    stats = getattr(workflow, "stats", None)
    if isinstance(stats, dict):
        stats_lock = getattr(workflow, "_stats_lock", None)

        def _update_stats() -> None:
            total = int(stats.get("file_fast_front_enqueued_count", 0) or 0) + queued_count
            stats["file_fast_front_enqueued_count"] = total
            stats["file_attachment_queue_enqueued_count"] = total
            bump_revision = getattr(workflow, "_bump_stats_revision_locked", None)
            if callable(bump_revision):
                bump_revision()

        if stats_lock is not None:
            async with stats_lock:
                _update_stats()
        else:
            _update_stats()

    progress_callback = getattr(workflow, "progress_callback", None)
    get_stats = getattr(workflow, "get_stats", None)
    if callable(progress_callback) and callable(get_stats):
        try:
            progress_callback(get_stats())
        except Exception:
            logger.debug(
                "[FileCrawlStage3][progress_publish_failed] job_id=%s",
                getattr(workflow, "job_id", ""),
                exc_info=True,
            )


async def enqueue_file_crawl_stage3_candidates(
    workflow: Any,
    *,
    post_url: str,
    board_url: str,
    attachments: Iterable[Dict[str, Any]],
    reg_date: Optional[str] = None,
    author: Optional[str] = None,
    department: Optional[str] = None,
    author_kind: Optional[str] = None,
    author_raw: Optional[str] = None,
    department_raw: Optional[str] = None,
    detail_cates: Optional[Tuple[Optional[str], Optional[str]]] = None,
    post_title: Optional[str] = None,
) -> Dict[str, int]:
    """Queue document candidates through the operational download pipeline.

    This intentionally does not call ``_enqueue_file_downloads``.  The old
    prequeue duplicate/claim/backpressure path is bypassed; Stage-3 owns only
    document filtering, and in-job URL dedupe.
    """
    if not bool(getattr(workflow, "is_attachment_file_crawl_workflow", False)):
        return {"queued": 0, "non_document": 0, "duplicate": 0, "invalid": 0}

    await workflow._ensure_file_pipeline()
    queues = getattr(workflow, "_file_job_queues", None)
    if queues is None:
        raise RuntimeError("file download queues are unavailable")

    seen_urls = getattr(workflow, "_file_stage3_seen_urls", None)
    if not isinstance(seen_urls, set):
        seen_urls = set()
        workflow._file_stage3_seen_urls = seen_urls

    cates = detail_cates or (None, None)
    cate1, cate2 = _text(cates[0]), _text(cates[1])
    counters = {"queued": 0, "non_document": 0, "duplicate": 0, "invalid": 0}
    normal_queue = queues.collection_batch_queue

    for raw_attachment in attachments or []:
        if not isinstance(raw_attachment, dict):
            counters["invalid"] += 1
            continue
        attachment = dict(raw_attachment)
        raw_url = _attachment_url(attachment)
        file_url = normalize_attachment_href(raw_url) if raw_url else ""
        file_name = _attachment_name(attachment, _text(post_title))
        key = _canonical_key(file_url)
        if not file_url or not key:
            counters["invalid"] += 1
            continue
        if key in seen_urls:
            counters["duplicate"] += 1
            continue
        if should_skip_attachment_at_scan(file_url, file_name):
            counters["non_document"] += 1
            continue

        seen_urls.add(key)
        declared_size = _size(
            attachment.get("declared_file_size_bytes")
            or attachment.get("_declared_file_size_bytes")
        )
        exact_size = _size(
            attachment.get("exact_file_size_bytes")
            or attachment.get("_exact_file_size_bytes")
        )
        item = {
            **attachment,
            "job_id": _text(getattr(workflow, "job_id", "")),
            "url": file_url,
            "name": file_name,
            "subject": file_name,
            "attachment_name": file_name,
            "source_page": _text(post_url),
            "source_url": _text(post_url),
            "board_url": _text(board_url) or _text(post_url),
            "db_name": _text(getattr(workflow, "db_name", "")),
            "chat_bot_id": _text(getattr(workflow, "chat_bot_id", "")),
            "reg_date": _text(reg_date) or None,
            "author": _text(author) or None,
            "department": _text(department) or None,
            "author_kind": _text(author_kind) or None,
            "author_raw": _text(author_raw) or None,
            "department_raw": _text(department_raw) or None,
            "cate1": cate1,
            "cate2": cate2,
            "declared_file_size_bytes": declared_size,
            "exact_file_size_bytes": exact_size,
            "defer_save_batch_until_learn_list": True,
            "skip_study_worker": False,
            "sync_after_download": True,
            "file_crawl_stage3": True,
        }
        target_queue = normal_queue
        lane = "normal"
        item["download_lane"] = lane
        await target_queue.put(item)
        counters["queued"] += 1
        trace = getattr(workflow, "_trace_file_pipeline_transition", None)
        if callable(trace):
            trace(
                stage="download_queue_enqueued",
                url=file_url,
                post_url=_text(post_url),
                file_name=file_name,
                detail=f"stage3_shared lane={lane} declared_size_bytes={declared_size}",
            )

    await normal_queue.flush()
    await _record_stage3_enqueue_progress(workflow, counters["queued"])
    logger.info(
        "[FileCrawlStage3][enqueue] job_id=%s db=%s post_url=%s queued=%s non_document=%s duplicate=%s invalid=%s",
        getattr(workflow, "job_id", ""),
        getattr(workflow, "db_name", ""),
        _text(post_url)[:220],
        counters["queued"],
        counters["non_document"],
        counters["duplicate"],
        counters["invalid"],
    )
    return counters
