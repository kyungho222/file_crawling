"""Stage-by-stage validation endpoints for the file crawl pipeline.

Stage 1 loads exploration rows, Stage 2 extracts attachment candidates, and
Stage 3 runs the production download workers through local-file verification.
The lab never connects the download output to MariaDB persistence or learning.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse

from config.settings import settings
from core.crawler.batch_queue import BatchQueue
from core.crawler.file_download_topology import file_crawl_download_topology
from core.crawler.queues import create_job_queues, dispose_job_queues
from core.crawler.workers.download import (
    download_worker,
    get_download_worker_activity_snapshot,
)
from backend.file.fast_attachment_producer import run_fast_file_attachment_front
from backend.file.file_crawl_attachment_snapshot import read_file_crawl_attachment_snapshot
from backend.file.file_download_workflow import (
    FileDownloadWorkflow,
    _file_crawl_detail_fetch_timeout_sec,
)
from backend.shared.batch_embedding_scheduler import get_pending_embedding_callback_count
from backend.shared.crawl_redis_keys import (
    crawl_state_key,
    crawl_state_scan_pattern,
    db_name_from_crawl_state_key,
)
from backend.shared.crawler_state import crawler_state
from backend.shared.file_crawl_post_urls import load_file_crawl_post_url_strings
from backend.shared.job_completion_summary import build_job_completion_summary
from backend.shared.progress_contract import is_file_mode_workflow
from db.crawl_db_manager import get_crawling_log_summary, resolve_crawling_log_id
from db.db_redis import get_redis
from utils.download_doc_filter import should_skip_attachment_at_scan
from utils.url import canonicalize_url_for_dedup

logger = logging.getLogger("backend.file.file_crawl_stage_lab")

router = APIRouter(prefix="/file-crawl-stage-lab", tags=["file-crawl-stage-lab"])

_ROOT = Path(__file__).resolve().parents[2]
_HTML_PATH = _ROOT / "dashboard" / "file_crawl_stage_lab.html"
_SIMHASH_HTML_PATH = _ROOT / "dashboard" / "file_crawl_simhash_payload.html"
_MAX_JOB_HISTORY = 100
_MAX_EVENT_HISTORY = 200
_DEFAULT_START_URL_LIMIT = 30_000
_RESOURCE_CLOSE_TIMEOUT_SEC = 10.0
_LAB_JOBS: Dict[str, Dict[str, Any]] = {}
_OPERATION_DB_COUNTS_CACHE: Dict[tuple[str, str], tuple[float, Dict[str, Any]]] = {}
_OPERATION_DB_COUNTS_CACHE_TTL_SEC = 5.0


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _as_text(value: Any) -> str:
    return str(value or "").strip()


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_value(item) for item in value]
    return str(value)


async def _operation_database_counts(
    *,
    job_id: str,
    db_name: str,
    craw_id: Any = None,
) -> Dict[str, Any]:
    """Return the persisted crawl-log counters without polling MariaDB every refresh."""
    jid = _as_text(job_id)
    dbn = _as_text(db_name)
    if not jid or not dbn:
        return {"available": False}

    cache_key = (dbn, jid)
    now = time.monotonic()
    cached = _OPERATION_DB_COUNTS_CACHE.get(cache_key)
    if cached and now - cached[0] < _OPERATION_DB_COUNTS_CACHE_TTL_SEC:
        return dict(cached[1])

    try:
        log_id = int(craw_id) if str(craw_id or "").strip().isdigit() else None
        if log_id is None:
            log_id = await resolve_crawling_log_id(jid, dbname=dbn)
        summary = await get_crawling_log_summary(jid, dbname=dbn, log_id=log_id)
        result = {
            "available": bool(summary),
            "log_id": int(summary.get("id", log_id or 0) or 0),
            "saved_count": int(summary.get("save", 0) or 0),
            "study_success_count": int(summary.get("study", 0) or 0),
            "status": _as_text(summary.get("status")),
        }
    except Exception as exc:
        logger.debug(
            "[FileCrawlStageLab][db_counts_unavailable] job_id=%s db=%s err=%s",
            jid,
            dbn,
            exc,
        )
        result = {"available": False}
    _OPERATION_DB_COUNTS_CACHE[cache_key] = (now, result)
    return dict(result)


def _decode_redis_hash(values: Dict[Any, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for raw_key, raw_value in (values or {}).items():
        key = raw_key.decode("utf-8", errors="replace") if isinstance(raw_key, bytes) else str(raw_key)
        value = raw_value.decode("utf-8", errors="replace") if isinstance(raw_value, bytes) else raw_value
        if isinstance(value, str) and value[:1] in {"{", "["}:
            try:
                result[key] = json.loads(value)
                continue
            except (TypeError, ValueError):
                pass
        result[key] = value
    return result


def _file_download_queue_waiting_view(workflow: Any) -> Dict[str, int]:
    """Return the current file-item count waiting in the download lanes."""
    job_queues = getattr(workflow, "_file_job_queues", None)

    def waiting_items(queue: Any) -> int:
        if queue is None:
            return 0
        raw_queue = getattr(queue, "queue", None)
        try:
            queued = sum(
                len(batch) if isinstance(batch, (list, tuple)) else 1
                for batch in list(getattr(raw_queue, "_queue", []) or [])
            )
        except Exception:
            queued = 0
        try:
            buffered = len(getattr(queue, "buffer", []) or [])
        except Exception:
            buffered = 0
        return max(0, queued + buffered)

    normal = waiting_items(getattr(job_queues, "collection_batch_queue", None))
    large = waiting_items(getattr(job_queues, "large_collection_batch_queue", None))
    return {"normal": normal, "large": large, "total": normal + large}


def _file_processing_view(
    stats: Any,
    attachment_found_count: int,
    *,
    queue_waiting: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    values = stats if isinstance(stats, dict) else {}

    def stat_count(*keys: str) -> int:
        for key in keys:
            try:
                value = int(values.get(key, 0) or 0)
            except (TypeError, ValueError):
                value = 0
            if value > 0:
                return value
        return 0

    queued_total_count = stat_count("file_fast_front_enqueued_count")
    queue_waiting = queue_waiting if isinstance(queue_waiting, dict) else {}
    queued_count = max(0, int(queue_waiting.get("total", 0) or 0))
    saved_count = stat_count("save_count", "save_success_count")
    study_success_count = stat_count(
        "file_study_success_count",
        "study_success_count",
        "study_count",
    )
    study_failed_count = stat_count("file_study_failed_count", "study_failed_count")
    study_done_count = max(
        stat_count("file_study_done_count", "study_done_count"),
        study_success_count + study_failed_count,
    )
    study_pending_count = max(0, saved_count - study_done_count)
    if attachment_found_count <= 0:
        processing_status = "첨부 없음"
    elif queued_total_count <= 0:
        processing_status = "첨부 추출 완료"
    elif queued_count > 0 or saved_count < queued_total_count:
        processing_status = "다운로드·저장 진행 중"
    elif study_pending_count > 0:
        processing_status = "학습 콜백 대기"
    elif study_failed_count > 0:
        processing_status = "학습 일부 실패"
    else:
        processing_status = "처리 완료"
    return {
        "status": processing_status,
        "queued_count": queued_count,
        "download_queue_current_count": queued_count,
        "queued_total_count": queued_total_count,
        "queue_waiting_normal_count": max(0, int(queue_waiting.get("normal", 0) or 0)),
        "queue_waiting_large_count": max(0, int(queue_waiting.get("large", 0) or 0)),
        "learn_list_inserted_count": stat_count("file_learn_list_status_n_insert_count"),
        "saved_count": saved_count,
        "study_pending_count": study_pending_count,
        "study_success_count": study_success_count,
        "study_failed_count": study_failed_count,
    }


def _file_simhash_gate_view(stats: Any, workflow: Any = None) -> Dict[str, Any]:
    """Expose existing SimHash gate counters without dashboard DB queries."""
    values = stats if isinstance(stats, dict) else {}

    def count(key: str) -> int:
        try:
            return max(0, int(values.get(key, 0) or 0))
        except (TypeError, ValueError):
            return 0

    results: List[Dict[str, Any]] = []
    raw_records = getattr(workflow, "_file_simhash_gate_results", {}) if workflow is not None else {}
    if isinstance(raw_records, dict):
        for item in list(raw_records.values())[-100:]:
            if isinstance(item, dict):
                results.append(_json_value(item))
    return {
        "duplicate_skip_count": count("file_simhash_duplicate_skip_count"),
        "pass_count": count("file_simhash_gate_pass_count"),
        "unavailable_allow_count": count("file_simhash_gate_unavailable_allow_count"),
        "results": results,
    }


def _file_worker_occupancy_view(workers: Any) -> List[Dict[str, Any]]:
    """Normalize existing worker health into a compact dashboard view."""
    values = workers if isinstance(workers, dict) else {}

    def count(key: str) -> int:
        try:
            return max(0, int(values.get(key, 0) or 0))
        except (TypeError, ValueError):
            return 0

    def entries(*keys: str) -> List[Dict[str, Any]]:
        for key in keys:
            raw = values.get(key)
            if not isinstance(raw, list):
                continue
            result: List[Dict[str, Any]] = []
            for item in raw[:8]:
                if isinstance(item, dict):
                    result.append(_json_value(item))
                else:
                    result.append({"summary": _as_text(item)})
            return result
        return []

    stages = (
        ("첨부 추출", "collection", ("collection_active",)),
        ("다운로드", "download", ("download_inflight", "download_active")),
        ("로컬 후처리", "local_finalize", ("local_finalize_active",)),
        ("저장", "save", ("save_inflight", "save_active")),
        ("학습", "study", ("study_active", "learn_active")),
    )
    result: List[Dict[str, Any]] = []
    for label, key, item_keys in stages:
        items = entries(*item_keys)
        item_busy = len(items) if key in {"download", "save"} else 0
        result.append({
            "stage": label,
            "total": count(f"{key}_total"),
            "alive": count(f"{key}_alive"),
            "busy": item_busy,
            "items": items,
        })
    return result


def _operation_workflow_snapshot(workflow: Any) -> Dict[str, Any]:
    try:
        stats = workflow.get_stats() if hasattr(workflow, "get_stats") else getattr(workflow, "stats", {})
    except Exception as exc:
        stats = {"snapshot_error": f"{type(exc).__name__}: {exc}"}
    queues: Dict[str, Any] = {}
    workers: Dict[str, Any] = {}
    try:
        job_queues = getattr(workflow, "_file_job_queues", None)
        if job_queues is not None:
            queues = _json_value(job_queues.debug_snapshot())
    except Exception as exc:
        queues = {"snapshot_error": f"{type(exc).__name__}: {exc}"}
    try:
        health = getattr(workflow, "_file_pipeline_worker_health_snapshot", None)
        if callable(health):
            workers = _json_value(health())
    except Exception as exc:
        workers = {"snapshot_error": f"{type(exc).__name__}: {exc}"}
    return {
        "stats": _json_value(dict(stats or {})),
        "queues": queues,
        "workers": workers,
        "workflow": {
            "db_name": _as_text(getattr(workflow, "db_name", "")),
            "chat_bot_id": _as_text(getattr(workflow, "chat_bot_id", "")),
            "final_status": _as_text(getattr(workflow, "final_status", "")),
            "stop_requested": bool(getattr(workflow, "_stop_requested", False)),
            "enable_db_save": getattr(workflow, "enable_db_save", None),
            "enable_learning": getattr(workflow, "enable_learning", None),
            "skip_learning": getattr(workflow, "file_pipeline_skip_learning", None),
        },
    }


def _operational_attachment_view(
    workflow: Any,
    snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return the durable snapshot, overlaid with currently running results."""
    try:
        stats = workflow.get_stats() if hasattr(workflow, "get_stats") else getattr(workflow, "stats", {})
    except Exception:
        stats = {}
    stats = stats if isinstance(stats, dict) else {}
    front = getattr(workflow, "fast_file_front_result", None)
    front = front if isinstance(front, dict) else {}
    raw_results = front.get("results") if isinstance(front.get("results"), list) else []
    results_by_url: Dict[str, Dict[str, Any]] = {}
    result_order: List[str] = []

    def add_result(item: Any) -> None:
        if not isinstance(item, dict):
            return
        url = _as_text(item.get("url"))
        if not url:
            return
        attachments: List[Dict[str, Any]] = []
        for attachment in item.get("attachments") or []:
            if not isinstance(attachment, dict):
                continue
            attachments.append({
                "name": _as_text(attachment.get("name")),
                "url": _as_text(attachment.get("url")),
            })
        if url not in results_by_url:
            result_order.append(url)
        results_by_url[url] = {
            "url": url,
            "attachment_count": int(item.get("attachment_count", len(attachments)) or 0),
            "attachments": attachments,
        }

    snapshot = snapshot if isinstance(snapshot, dict) else {}
    for item in snapshot.get("detail_results") or []:
        add_result(item)
    for item in raw_results:
        add_result(item)
    results = [results_by_url[url] for url in result_order if url in results_by_url]
    loaded_url_count = int(
        snapshot.get("loaded_url_count", 0)
        or getattr(workflow, "pre_explored_start_urls_count", 0)
        or stats.get("file_fast_front_post_count", 0)
        or stats.get("scan_count", 0)
        or len(raw_results)
        or 0
    )
    attachment_found_count = sum(
        int(item.get("attachment_count", 0) or 0) for item in results
    )
    worker_snapshot = _operation_workflow_snapshot(workflow)
    queue_waiting = _file_download_queue_waiting_view(workflow)
    worker_occupancy = _file_worker_occupancy_view(worker_snapshot.get("workers"))
    return {
        "loaded_url_count": loaded_url_count,
        "detail_results": results,
        "detail_visited_count": len(results),
        "attachment_found_count": attachment_found_count,
        "processing": _file_processing_view(
            stats,
            attachment_found_count,
            queue_waiting=queue_waiting,
        ),
        "simhash_gate": _file_simhash_gate_view(stats, workflow),
        "worker_occupancy": worker_occupancy,
        "workflow": {
            "db_name": _as_text(getattr(workflow, "db_name", "")) or _as_text(snapshot.get("db_name")),
            "chat_bot_id": _as_text(getattr(workflow, "chat_bot_id", "")) or _as_text(snapshot.get("chat_bot_id")),
            "final_status": _as_text(getattr(workflow, "final_status", "")),
        },
    }


async def _operation_redis_state(job_id: str, db_name: str = "") -> Dict[str, Any]:
    """Load the current crawler state without requiring a dashboard-owned job."""
    try:
        redis = await get_redis()
        meta = _decode_redis_hash(await redis.hgetall(f"job_meta:{job_id}"))
        resolved_db = _as_text(meta.get("dbname")) or _as_text(db_name)
        state_key = crawl_state_key(resolved_db, job_id) if resolved_db else ""
        state = _decode_redis_hash(await redis.hgetall(state_key)) if state_key else {}
        if not state:
            async for raw_key in redis.scan_iter(match=crawl_state_scan_pattern(job_id), count=5):
                key = raw_key.decode("utf-8", errors="replace") if isinstance(raw_key, bytes) else str(raw_key)
                state = _decode_redis_hash(await redis.hgetall(key))
                resolved_db = resolved_db or _as_text(db_name_from_crawl_state_key(key))
                state_key = key
                if state:
                    break
        return {
            "available": bool(state),
            "db_name": resolved_db,
            "state_key": state_key,
            "state": state,
        }
    except Exception as exc:
        return {"available": False, "db_name": _as_text(db_name), "state_key": "", "state": {}, "error": f"{type(exc).__name__}: {exc}"}


def _as_optional_positive_int(value: Any, *, field: str) -> Optional[int]:
    if value is None or _as_text(value) == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{field} must be a positive integer") from exc
    if parsed <= 0:
        raise HTTPException(status_code=400, detail=f"{field} must be a positive integer")
    return parsed


def _lab_event(job: Dict[str, Any], event: str, **detail: Any) -> None:
    events = job.setdefault("events", [])
    events.append({"event": event, "at": _now(), **detail})
    del events[:-_MAX_EVENT_HISTORY]


def _job_view(job: Dict[str, Any], *, include_results: bool = False) -> Dict[str, Any]:
    view = {
        key: value
        for key, value in job.items()
        if key not in {
            "task",
            "results",
            "events",
            "post_items",
            "workflow",
            "stop_event",
            "job_queues",
            "worker_tasks",
            "progress_task",
            "terminal_event",
            "download_candidates",
            "terminal_result_keys",
            "seen_candidate_urls",
            "candidate_by_key",
            "deferred_candidate_keys",
            "deferred_seen_candidate_keys",
            "queue_ready_event",
            "producer_done_event",
        }
    }
    view["event_count"] = len(job.get("events") or [])
    view["result_count"] = len(job.get("results") or [])
    if _as_text(job.get("status")) in {"completed", "failed", "stopped"}:
        stage = _as_text(job.get("stage"))
        workflow_name = {
            "start_url_load": "파일 시작 URL",
            "attachment_extract": "파일 첨부 추출",
            "download_validation": "파일 다운로드 검증",
        }.get(stage, "파일 단계 검증")
        completed_count = int(job.get("completed_count") or view["result_count"] or 0)
        view["completion_summary"] = build_job_completion_summary(
            {"collection_count": completed_count},
            job_id=_as_text(job.get("job_id")) or "-",
            workflow_name=workflow_name,
            status=_as_text(job.get("status")),
            processing_count=completed_count,
        )
    if include_results:
        view["results"] = list(job.get("results") or [])
    return view


async def _close_attachment_extraction_workflow_resources(workflow: Any) -> None:
    """Close the per-job HTTP and Playwright resources even while a lab task is cancelled."""
    cleanup_tasks: List[asyncio.Task] = []
    for cleanup_name in ("_close_http_session", "_close_playwright"):
        cleanup = getattr(workflow, cleanup_name, None)
        if not callable(cleanup):
            continue
        cleanup_tasks.append(
            asyncio.create_task(
                cleanup(),
                name=f"file-crawl-stage-lab:resource-close:{cleanup_name}",
            )
        )
    if not cleanup_tasks:
        return

    completion = asyncio.gather(*cleanup_tasks, return_exceptions=True)
    try:
        results = await asyncio.wait_for(
            asyncio.shield(completion),
            timeout=_RESOURCE_CLOSE_TIMEOUT_SEC,
        )
    except asyncio.CancelledError:
        # The workflow was stopped while cleanup was in progress. The cleanup
        # tasks continue independently; wait once more before propagating stop.
        try:
            await asyncio.wait_for(
                asyncio.shield(completion),
                timeout=_RESOURCE_CLOSE_TIMEOUT_SEC,
            )
        except BaseException:
            logger.warning("[FileCrawlStageLab][resource_close_interrupted]", exc_info=True)
            await _force_close_attachment_extraction_browser(workflow)
        raise
    except asyncio.TimeoutError:
        logger.warning(
            "[FileCrawlStageLab][resource_close_timeout] timeout_sec=%s tasks=%s",
            _RESOURCE_CLOSE_TIMEOUT_SEC,
            len(cleanup_tasks),
        )
        for task in cleanup_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        await _force_close_attachment_extraction_browser(workflow)
        return

    for cleanup_name, result in zip(("_close_http_session", "_close_playwright"), results):
        if isinstance(result, BaseException):
            logger.warning(
                "[FileCrawlStageLab][resource_close_failed] cleanup=%s err=%s",
                cleanup_name,
                result,
            )


async def _force_close_attachment_extraction_browser(workflow: Any) -> None:
    """Best-effort OS cleanup for a browser that did not close within the grace period."""
    browser = getattr(workflow, "_pw_browser", None)
    if browser is None:
        return
    try:
        from core.crawler.global_pool import _terminate_browser_process_os_best_effort

        await _terminate_browser_process_os_best_effort(browser)
        logger.warning(
            "[FileCrawlStageLab][resource_force_close] browser_id=%s",
            id(browser),
        )
    except Exception:
        logger.warning("[FileCrawlStageLab][resource_force_close_failed]", exc_info=True)


def _download_key(value: Any) -> str:
    raw = _as_text(value)
    if not raw:
        return ""
    try:
        return canonicalize_url_for_dedup(raw) or raw
    except Exception:
        return raw


def _stage_attachment_extract_concurrency() -> int:
    """Keep the diagnostic lab's detail-page traffic deliberately small."""
    return 2


def _stage_download_worker_config() -> Dict[str, int]:
    """Keep diagnostic downloads independent from, and smaller than, production workers."""
    return file_crawl_download_topology()


def _download_validation_candidates_for_result(
    source_job: Dict[str, Any],
    job_id: str,
    result: Dict[str, Any],
    seen_urls: set[str],
) -> tuple[List[Dict[str, Any]], int]:
    """Convert one stage-2 result into document-only production payloads."""
    candidates: List[Dict[str, Any]] = []
    non_document_filtered = 0
    post_url = _as_text(result.get("url"))
    breadcrumb = result.get("breadcrumb") if isinstance(result.get("breadcrumb"), dict) else {}
    for attachment in result.get("attachments") or []:
        if not isinstance(attachment, dict):
            continue
        file_url = _as_text(attachment.get("url"))
        file_name = _as_text(attachment.get("name"))
        key = _download_key(file_url)
        if not file_url or not key or key in seen_urls:
            continue
        # Stage 2 already removes extraction noise. Reuse the downloader's
        # document gate before enqueueing, so Stage 3 only owns documents.
        if should_skip_attachment_at_scan(file_url, file_name):
            non_document_filtered += 1
            continue
        seen_urls.add(key)
        try:
            declared_size = max(0, int(attachment.get("declared_file_size_bytes") or 0))
        except (TypeError, ValueError):
            declared_size = 0
        try:
            exact_size = max(0, int(attachment.get("exact_file_size_bytes") or 0))
        except (TypeError, ValueError):
            exact_size = 0
        candidates.append(
            {
                "job_id": job_id,
                "url": file_url,
                "name": file_name,
                "subject": file_name,
                "attachment_name": file_name,
                "source_page": post_url,
                "source_url": post_url,
                "board_url": post_url,
                "db_name": source_job.get("db_name"),
                "chat_bot_id": source_job.get("chat_bot_id"),
                "reg_date": _as_text(result.get("reg_date")),
                "author": _as_text(result.get("author")),
                "department": _as_text(result.get("department")),
                "cate1": _as_text(breadcrumb.get("cate1")),
                "cate2": _as_text(breadcrumb.get("cate2")),
                "declared_file_size_bytes": declared_size,
                "exact_file_size_bytes": exact_size,
                "_stage_lab_candidate_found_at": time.monotonic(),
                # The regular file pipeline sends this event to a local
                # finalize/save path. The lab consumes it as its terminal
                # verification event and deliberately has no save worker.
                "defer_save_batch_until_learn_list": True,
                "skip_study_worker": True,
                "sync_after_download": False,
                "file_crawl_stage_lab": True,
            }
        )
    return candidates, non_document_filtered


def _build_download_validation_candidates(source_job: Dict[str, Any], job_id: str) -> tuple[List[Dict[str, Any]], int]:
    """Compatibility helper for a completed stage-2 result set."""
    candidates: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    non_document_filtered = 0
    for result in source_job.get("results") or []:
        if not isinstance(result, dict):
            continue
        items, filtered = _download_validation_candidates_for_result(
            source_job,
            job_id,
            result,
            seen_urls,
        )
        candidates.extend(items)
        non_document_filtered += filtered
    return candidates, non_document_filtered


def _new_download_validation_job(
    *,
    source_job_id: str,
    db_name: str,
    chat_bot_id: str,
    learn_list_id: Any,
    initial_candidates: Optional[List[Dict[str, Any]]] = None,
    non_document_filtered_count: int = 0,
    producer_done: bool,
    job_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a stage-3 job whose producer can feed it while it is running."""
    runtime = _stage_download_worker_config()
    producer_done_event = asyncio.Event()
    if producer_done:
        producer_done_event.set()
    return {
        "job_id": job_id or str(uuid.uuid4()),
        "stage": "download_validation",
        "source_job_id": source_job_id,
        "db_name": db_name,
        "chat_bot_id": chat_bot_id,
        "learn_list_id": learn_list_id,
        # Counts grow only as stage 2 confirms a document candidate and
        # successfully hands it to the production download queue.
        "target_count": 0,
        "queued_count": 0,
        "completed_count": 0,
        "pending_count": 0,
        "downloaded_count": 0,
        "skipped_count": 0,
        # A local file exists when the common worker emits one of these
        # events. Keep receipt and candidate matching separate so the lab can
        # expose any contract mismatch instead of silently leaving it pending.
        "storage_event_received_count": 0,
        "storage_event_matched_count": 0,
        "storage_event_unmatched_count": 0,
        "deferred_count": 0,
        "deferred_unique_count": 0,
        "deferred_current_count": 0,
        "deferred_candidate_keys": set(),
        "deferred_seen_candidate_keys": set(),
        "download_inflight": [],
        "non_document_filtered_count": non_document_filtered_count,
        "status": "queued",
        "created_at": _now(),
        "started_at": "",
        "completed_at": "",
        "elapsed_ms": None,
        "error": "",
        "stop_requested": False,
        "results": [],
        "events": [],
        "download_candidates": list(initial_candidates or []),
        "terminal_result_keys": set(),
        "seen_candidate_urls": set(),
        "candidate_by_key": {},
        "queue_ready_event": asyncio.Event(),
        "producer_done_event": producer_done_event,
        "stop_event": asyncio.Event(),
        "options": {
            "source_job_id": source_job_id,
            "document_only": True,
            "noise_filtered_by_stage_2": True,
            "download": True,
            "local_storage": "downloads/",
            "db_save": False,
            "learning": False,
            "download_workers": runtime["total_workers"],
            "normal_download_workers": runtime["normal_workers"],
            "large_download_workers": runtime["large_workers"],
            "download_max_concurrent": runtime["max_concurrent"],
        },
    }


async def _enqueue_download_validation_candidates(
    job: Dict[str, Any],
    candidates: List[Dict[str, Any]],
) -> None:
    """Feed stage-2 document candidates directly into the running stage-3 lanes."""
    if not candidates:
        return
    queue_ready_event = job.get("queue_ready_event")
    if not isinstance(queue_ready_event, asyncio.Event):
        raise RuntimeError("download validation queue is unavailable")
    await queue_ready_event.wait()
    if job.get("stop_requested"):
        return
    queues = job.get("job_queues")
    if queues is None:
        raise RuntimeError("download validation queues were released")

    job["target_count"] = int(job.get("target_count", 0) or 0) + len(candidates)
    job["pending_count"] = int(job.get("pending_count", 0) or 0) + len(candidates)
    candidate_by_key = job.setdefault("candidate_by_key", {})
    for item in candidates:
        key = _download_key(item.get("url"))
        candidate_by_key[key] = item
        target_queue = queues.collection_batch_queue
        item["download_lane"] = "normal"
        enqueue_started = time.monotonic()
        await target_queue.put(item)
        item["_stage_lab_queue_enqueued_at"] = time.monotonic()
        item["_stage_lab_enqueue_wait_ms"] = round(
            (item["_stage_lab_queue_enqueued_at"] - enqueue_started) * 1000,
            1,
        )
        _lab_event(
            job,
            "download_candidate_enqueued",
            url=_as_text(item.get("url")),
            name=_as_text(item.get("name")),
            lane=item["download_lane"],
            enqueue_wait_ms=item["_stage_lab_enqueue_wait_ms"],
        )
    await queues.collection_batch_queue.flush()
    await queues.large_collection_batch_queue.flush()
    job["queued_count"] = int(job.get("queued_count", 0) or 0) + len(candidates)
    job["queue_snapshot"] = queues.debug_snapshot()
    _lab_event(
        job,
        "download_queue_enqueued",
        queued_count=job["queued_count"],
        added_count=len(candidates),
        pending_count=job["pending_count"],
    )


def _download_event_candidate_keys(event: Dict[str, Any]) -> List[str]:
    event = _download_progress_event_fields(event)
    keys: List[str] = []
    values = [event.get("url")]
    original_meta = event.get("original_meta")
    if isinstance(original_meta, dict):
        values.extend([original_meta.get("url"), original_meta.get("_raw_url")])
    for value in values:
        key = _download_key(value)
        if key and key not in keys:
            keys.append(key)
    return keys


def _download_progress_event_fields(event: Dict[str, Any]) -> Dict[str, Any]:
    """Expose download progress payload fields emitted beneath ``file_info``.

    The common download worker keeps its file-saved contract nested so the
    normal save pipeline can consume it.  The stage lab has no save worker and
    must use that same event as its terminal download result.
    """
    file_info = event.get("file_info")
    if not isinstance(file_info, dict):
        return event
    return {**file_info, **event}


async def _run_download_validation(job_id: str) -> None:
    job = _LAB_JOBS.get(job_id)
    if job is None:
        return

    started = time.perf_counter()
    workflow: Optional[FileDownloadWorkflow] = None
    worker_tasks: List[asyncio.Task] = []
    progress_task: Optional[asyncio.Task] = None
    queues = None
    terminal_event = asyncio.Event()
    job["status"] = "downloading"
    job["started_at"] = _now()
    _lab_event(job, "download_validation_started", target_count=job["target_count"])
    try:
        workflow = FileDownloadWorkflow()
        workflow.job_id = job_id
        workflow.db_name = job["db_name"]
        workflow.chat_bot_id = job["chat_bot_id"]
        workflow.file_pipeline_skip_learning = True
        workflow.enable_db_save = False
        workflow.enable_learning = False
        workflow.stop_event = job["stop_event"]
        job["workflow"] = workflow

        queues = create_job_queues(job_id, collection_batch_size=1)
        # download_worker does not put deferred local-save results onto this
        # queue. Keep the output immediate if a downloader returns another
        # result type, while never creating a DB persist consumer.
        queues.save_batch_queue = BatchQueue(batch_size=1)
        job["job_queues"] = queues
        job["terminal_event"] = terminal_event
        job.setdefault("candidate_by_key", {})

        def producer_finished_and_drained() -> bool:
            producer_done = job.get("producer_done_event")
            return bool(
                isinstance(producer_done, asyncio.Event)
                and producer_done.is_set()
                and int(job.get("completed_count", 0) or 0) >= int(job.get("target_count", 0) or 0)
            )

        async def record_download_worker_taken(event: Dict[str, Any]) -> None:
            event_keys = _download_event_candidate_keys(event)
            key = next(
                (value for value in event_keys if value in job["candidate_by_key"]),
                event_keys[0] if event_keys else "",
            )
            candidate = job["candidate_by_key"].get(key, {})
            if not candidate:
                return
            job["deferred_candidate_keys"].discard(key)
            job["deferred_current_count"] = len(job["deferred_candidate_keys"])
            taken_at = time.monotonic()
            candidate["_stage_lab_worker_taken_at"] = taken_at
            enqueued_at = float(candidate.get("_stage_lab_queue_enqueued_at") or taken_at)
            _lab_event(
                job,
                "download_worker_taken",
                url=_as_text(event.get("url")) or _as_text(candidate.get("url")),
                name=_as_text(event.get("name")) or _as_text(candidate.get("name")),
                worker_id=event.get("worker_id"),
                lane=_as_text(event.get("lane")) or _as_text(candidate.get("download_lane")),
                queue_wait_ms=round((taken_at - enqueued_at) * 1000, 1),
            )

        async def consume_progress() -> None:
            while True:
                event = await queues.progress_queue.get()
                try:
                    if not isinstance(event, dict):
                        continue
                    event_type = _as_text(event.get("type"))
                    if event_type == "download_deferred":
                        deferred_keys = _download_event_candidate_keys(event)
                        deferred_key = next(
                            (value for value in deferred_keys if value in job["candidate_by_key"]),
                            deferred_keys[0] if deferred_keys else "",
                        )
                        job["deferred_count"] = int(job.get("deferred_count", 0) or 0) + 1
                        if deferred_key:
                            job["deferred_candidate_keys"].add(deferred_key)
                            job["deferred_seen_candidate_keys"].add(deferred_key)
                        job["deferred_unique_count"] = len(job["deferred_seen_candidate_keys"])
                        job["deferred_current_count"] = len(job["deferred_candidate_keys"])
                        _lab_event(
                            job,
                            "download_deferred",
                            url=_as_text(event.get("url")),
                            name=_as_text(event.get("name")),
                            reason=_as_text(event.get("reason")),
                            worker_id=event.get("worker_id"),
                            lane=_as_text(event.get("lane")),
                            domain_host=_as_text(event.get("domain_host")),
                            domain_limit=event.get("domain_limit"),
                            defer_attempt=event.get("defer_attempt"),
                        )
                        continue
                    if event_type == "download_item_taken":
                        await record_download_worker_taken(event)
                        continue
                    if event_type not in {"download_local_saved", "file_saved", "download_skipped"}:
                        continue
                    result_event = _download_progress_event_fields(event)
                    downloaded = event_type in {"download_local_saved", "file_saved"}
                    if downloaded:
                        job["storage_event_received_count"] = int(
                            job.get("storage_event_received_count", 0) or 0
                        ) + 1
                    event_keys = _download_event_candidate_keys(result_event)
                    key = next(
                        (value for value in event_keys if value in job["candidate_by_key"]),
                        event_keys[0] if event_keys else "",
                    )
                    if not key or key in job["terminal_result_keys"]:
                        if downloaded:
                            job["storage_event_unmatched_count"] = int(
                                job.get("storage_event_unmatched_count", 0) or 0
                            ) + 1
                            logger.warning(
                                "[FileCrawlStageLab][storage_event_unmatched] job_id=%s url=%s path=%s keys=%s",
                                job_id,
                                result_event.get("url") or "-",
                                result_event.get("file_path") or result_event.get("local_path") or "-",
                                event_keys,
                            )
                        continue
                    candidate = job["candidate_by_key"].get(key, {})
                    job["terminal_result_keys"].add(key)
                    job["deferred_candidate_keys"].discard(key)
                    job["deferred_current_count"] = len(job["deferred_candidate_keys"])
                    if downloaded:
                        job["storage_event_matched_count"] = int(
                            job.get("storage_event_matched_count", 0) or 0
                        ) + 1
                    terminal_at = time.monotonic()
                    found_at = float(candidate.get("_stage_lab_candidate_found_at") or terminal_at)
                    enqueued_at = float(candidate.get("_stage_lab_queue_enqueued_at") or terminal_at)
                    taken_at = float(candidate.get("_stage_lab_worker_taken_at") or terminal_at)
                    diagnostics = result_event.get("lab_download_diagnostics")
                    if not isinstance(diagnostics, dict):
                        diagnostics = {}
                    result = {
                        "index": len(job["results"]) + 1,
                        "status": "downloaded" if downloaded else "skipped",
                        "name": _as_text(result_event.get("name")) or _as_text(candidate.get("name")),
                        "url": _as_text(result_event.get("url")) or _as_text(candidate.get("url")),
                        "post_url": _as_text(result_event.get("source_page")) or _as_text(candidate.get("source_page")),
                        "size": result_event.get("size") or 0,
                        "file_path": _as_text(result_event.get("file_path") or result_event.get("local_path")),
                        "reason": _as_text(result_event.get("reason")),
                        "detail": _as_text(result_event.get("detail") or result_event.get("content_type") or result_event.get("filename")),
                        "worker_id": result_event.get("worker_id"),
                        "lane": _as_text(result_event.get("lane")) or _as_text(candidate.get("download_lane")),
                        "candidate_to_queue_ms": round((enqueued_at - found_at) * 1000, 1),
                        "queue_wait_ms": round((taken_at - enqueued_at) * 1000, 1),
                        "download_elapsed_ms": round((terminal_at - taken_at) * 1000, 1),
                        "diagnostics": dict(diagnostics),
                    }
                    job["results"].append(result)
                    counter = "downloaded_count" if downloaded else "skipped_count"
                    job[counter] = int(job.get(counter, 0) or 0) + 1
                    job["completed_count"] = len(job["results"])
                    job["pending_count"] = max(0, int(job["target_count"]) - job["completed_count"])
                    _lab_event(job, "download_terminal", **result)
                    if not downloaded:
                        logger.info(
                            "[FileCrawlStageLab][download_failure_detail] job_id=%s worker=%s lane=%s "
                            "phase=%s status=%s content_type=%s content_length=%s bytes=%s "
                            "http_attempts=%s playwright_attempts=%s error=%s url=%s",
                            job_id,
                            result.get("worker_id"),
                            result.get("lane"),
                            diagnostics.get("phase") or "-",
                            diagnostics.get("http_status") if diagnostics.get("http_status") is not None else "-",
                            diagnostics.get("content_type") or "-",
                            diagnostics.get("content_length") if diagnostics.get("content_length") is not None else "-",
                            diagnostics.get("bytes_written") if diagnostics.get("bytes_written") is not None else "-",
                            diagnostics.get("http_attempts") if diagnostics.get("http_attempts") is not None else "-",
                            diagnostics.get("playwright_attempts") if diagnostics.get("playwright_attempts") is not None else "-",
                            diagnostics.get("last_error") or result.get("detail") or result.get("reason") or "-",
                            result.get("url"),
                        )
                    if producer_finished_and_drained():
                        terminal_event.set()
                finally:
                    queues.progress_queue.task_done()

        progress_task = asyncio.create_task(
            consume_progress(),
            name=f"file-crawl-stage-lab:download-progress:{job_id}",
        )
        job["progress_task"] = progress_task

        def get_browser() -> Any:
            return getattr(workflow, "_pw_browser", None)

        async def relaunch_browser() -> Any:
            await workflow._close_playwright()
            await workflow._ensure_playwright()
            return getattr(workflow, "_pw_browser", None)

        runtime = _stage_download_worker_config()
        normal_semaphore = asyncio.Semaphore(runtime["normal_workers"])
        for worker_id in range(1, runtime["normal_workers"] + 1):
            worker_tasks.append(
                asyncio.create_task(
                    download_worker(
                        queues.collection_batch_queue,
                        queues.save_batch_queue,
                        progress_queue=queues.progress_queue,
                        max_concurrent=runtime["max_concurrent"],
                        browser_getter=get_browser,
                        browser_releaser=lambda _browser: None,
                        browser_relauncher=relaunch_browser,
                        worker_id=worker_id,
                        worker_lane="normal",
                        large_download_queue=(
                            queues.large_collection_batch_queue if runtime["large_workers"] else None
                        ),
                        shared_download_semaphore=normal_semaphore,
                        item_taken_callback=record_download_worker_taken,
                    ),
                    name=f"file-crawl-stage-lab:download-normal:{job_id}:{worker_id}",
                )
            )
        if runtime["large_workers"]:
            large_semaphore = asyncio.Semaphore(runtime["large_workers"])
            for index in range(runtime["large_workers"]):
                worker_id = runtime["normal_workers"] + index + 1
                worker_tasks.append(
                    asyncio.create_task(
                        download_worker(
                            queues.large_collection_batch_queue,
                            queues.save_batch_queue,
                            progress_queue=queues.progress_queue,
                            max_concurrent=runtime["max_concurrent"],
                            browser_getter=get_browser,
                            browser_releaser=lambda _browser: None,
                            browser_relauncher=relaunch_browser,
                            worker_id=worker_id,
                            worker_lane="large",
                            fallback_in_queue=queues.collection_batch_queue,
                            shared_download_semaphore=large_semaphore,
                            item_taken_callback=record_download_worker_taken,
                        ),
                        name=f"file-crawl-stage-lab:download-large:{job_id}:{worker_id}",
                    )
                )
        job["worker_tasks"] = worker_tasks
        queue_ready_event = job.get("queue_ready_event")
        if isinstance(queue_ready_event, asyncio.Event):
            queue_ready_event.set()
        initial_candidates = list(job.get("download_candidates") or [])
        if initial_candidates:
            await _enqueue_download_validation_candidates(job, initial_candidates)
            job["download_candidates"] = []
        job["queue_snapshot"] = queues.debug_snapshot()
        logger.info(
            "[FileCrawlStageLab][download_queue_ready] job_id=%s db=%s normal_workers=%s large_workers=%s",
            job_id,
            job["db_name"],
            runtime["normal_workers"],
            runtime["large_workers"],
        )

        last_wait_event = time.monotonic()
        while not terminal_event.is_set():
            if job.get("stop_requested"):
                raise asyncio.CancelledError
            job["download_inflight"] = get_download_worker_activity_snapshot(job_id=job_id)
            if producer_finished_and_drained():
                terminal_event.set()
                continue
            try:
                await asyncio.wait_for(terminal_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                if time.monotonic() - last_wait_event >= 30.0:
                    job["queue_snapshot"] = queues.debug_snapshot()
                    _lab_event(
                        job,
                        "download_waiting",
                        completed_count=job.get("completed_count", 0),
                        pending_count=job.get("pending_count", 0),
                    )
                    last_wait_event = time.monotonic()

        job["status"] = "completed"
        job["completed_at"] = _now()
        job["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)
        job["queue_snapshot"] = queues.debug_snapshot()
        job["download_inflight"] = []
        _lab_event(
            job,
            "download_validation_completed",
            downloaded_count=job.get("downloaded_count", 0),
            skipped_count=job.get("skipped_count", 0),
            elapsed_ms=job["elapsed_ms"],
        )
    except asyncio.CancelledError:
        job["status"] = "stopped"
        job["completed_at"] = _now()
        _lab_event(job, "download_validation_stopped", completed_count=job.get("completed_count", 0))
        raise
    except Exception as exc:
        job["status"] = "failed"
        job["completed_at"] = _now()
        job["error"] = f"{type(exc).__name__}: {exc}"
        _lab_event(job, "download_validation_failed", error=job["error"])
        logger.exception(
            "[FileCrawlStageLab][download_validation_failed] job_id=%s db=%s",
            job_id,
            job.get("db_name"),
        )
    finally:
        job["download_inflight"] = []
        for task in worker_tasks:
            if not task.done():
                task.cancel()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        if progress_task is not None and not progress_task.done():
            progress_task.cancel()
        if progress_task is not None:
            await asyncio.gather(progress_task, return_exceptions=True)
        if workflow is not None:
            await _close_attachment_extraction_workflow_resources(workflow)
        if queues is not None:
            await dispose_job_queues(job_id)
        job.pop("workflow", None)
        job.pop("job_queues", None)
        job.pop("worker_tasks", None)
        job.pop("progress_task", None)
        job.pop("terminal_event", None)


async def _run_start_url_load(job_id: str) -> None:
    job = _LAB_JOBS.get(job_id)
    if job is None:
        return
    started = time.perf_counter()
    job["status"] = "loading"
    job["started_at"] = _now()
    _lab_event(job, "start_url_load_started")
    try:
        options = dict(job["options"])
        rows = await load_file_crawl_post_url_strings(**options)
        if job.get("stop_requested"):
            job["status"] = "stopped"
            _lab_event(job, "start_url_load_stopped", loaded_count=len(rows or []))
            return
        post_items: List[Dict[str, Any]] = []
        results: List[Dict[str, Any]] = []
        for index, row in enumerate(rows or [], start=1):
            if not isinstance(row, dict):
                row = {"url": _as_text(row)}
            post_items.append(dict(row))
            results.append(
                {
                    "index": index,
                    "url": _as_text(row.get("url")),
                    "type": _as_text(row.get("type")),
                }
            )
        job["post_items"] = post_items
        job["results"] = results
        job["status"] = "completed"
        job["completed_at"] = _now()
        job["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)
        _lab_event(job, "start_url_load_completed", loaded_count=len(results), elapsed_ms=job["elapsed_ms"])
        logger.info(
            "[FileCrawlStageLab][start_urls_loaded] job_id=%s db=%s chat_bot_id=%s loaded=%s elapsed_ms=%s",
            job_id,
            job["db_name"],
            job["chat_bot_id"],
            len(results),
            job["elapsed_ms"],
        )
    except asyncio.CancelledError:
        job["status"] = "stopped"
        job["completed_at"] = _now()
        _lab_event(job, "start_url_load_stopped")
        raise
    except Exception as exc:
        job["status"] = "failed"
        job["completed_at"] = _now()
        job["error"] = f"{type(exc).__name__}: {exc}"
        _lab_event(job, "start_url_load_failed", error=job["error"])
        logger.exception(
            "[FileCrawlStageLab][start_urls_failed] job_id=%s db=%s chat_bot_id=%s",
            job_id,
            job["db_name"],
            job["chat_bot_id"],
        )


async def _run_attachment_extraction(job_id: str) -> None:
    job = _LAB_JOBS.get(job_id)
    if job is None:
        return
    started = time.perf_counter()
    workflow: Optional[FileDownloadWorkflow] = None
    job["status"] = "extracting"
    job["started_at"] = _now()
    _lab_event(job, "attachment_extract_started", source_job_id=job["source_job_id"])
    try:
        workflow = FileDownloadWorkflow()
        workflow.job_id = job_id
        workflow.db_name = job["db_name"]
        workflow.chat_bot_id = job["chat_bot_id"]
        workflow.file_pipeline_skip_learning = True
        workflow.enable_db_save = False
        workflow.enable_learning = False
        workflow.stop_event = job["stop_event"]
        job["workflow"] = workflow

        async def on_result(result: Dict[str, Any]) -> None:
            if job.get("stop_requested"):
                return
            indexed_result = {"index": len(job["results"]) + 1, **result}
            job["results"].append(indexed_result)
            downstream_job = _LAB_JOBS.get(_as_text(job.get("downstream_job_id")))
            if downstream_job is not None and not downstream_job.get("stop_requested"):
                candidates, non_document_filtered = _download_validation_candidates_for_result(
                    job,
                    downstream_job["job_id"],
                    indexed_result,
                    downstream_job["seen_candidate_urls"],
                )
                downstream_job["non_document_filtered_count"] = int(
                    downstream_job.get("non_document_filtered_count", 0) or 0
                ) + non_document_filtered
                await _enqueue_download_validation_candidates(downstream_job, candidates)
            _lab_event(
                job,
                "attachment_extract_result",
                result_count=len(job["results"]),
                attachment_count=int(result.get("attachment_count") or 0),
                post_url=_as_text(result.get("url")),
            )

        extraction_summary = await run_fast_file_attachment_front(
            workflow=workflow,
            post_items=list(job["post_items"]),
            concurrency=_stage_attachment_extract_concurrency(),
            timeout_sec=_file_crawl_detail_fetch_timeout_sec({}),
            enqueue=False,
            include_attachment_details=True,
            include_breadcrumb_metadata=True,
            playwright_fallback_on_fetch_failure=True,
            result_callback=on_result,
        )
        job["attachment_count"] = int(
            (extraction_summary or {}).get("attachment_count")
            or sum(int(item.get("attachment_count") or 0) for item in job["results"])
        )
        if job.get("stop_requested"):
            job["status"] = "stopped"
            _lab_event(job, "attachment_extract_stopped", result_count=len(job["results"]))
            return
        job["status"] = "completed"
        job["completed_at"] = _now()
        job["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 1)
        _lab_event(
            job,
            "attachment_extract_completed",
            result_count=len(job["results"]),
            attachment_count=job["attachment_count"],
            elapsed_ms=job["elapsed_ms"],
        )
        logger.info(
            "[FileCrawlStageLab][attachment_extract_completed] job_id=%s source_job_id=%s db=%s posts=%s",
            job_id,
            job["source_job_id"],
            job["db_name"],
            len(job["results"]),
        )
    except asyncio.CancelledError:
        job["status"] = "stopped"
        job["completed_at"] = _now()
        _lab_event(job, "attachment_extract_stopped", result_count=len(job["results"]))
        raise
    except Exception as exc:
        job["status"] = "failed"
        job["completed_at"] = _now()
        job["error"] = f"{type(exc).__name__}: {exc}"
        _lab_event(job, "attachment_extract_failed", error=job["error"])
        logger.exception(
            "[FileCrawlStageLab][attachment_extract_failed] job_id=%s source_job_id=%s db=%s",
            job_id,
            job["source_job_id"],
            job["db_name"],
        )
    finally:
        downstream_job = _LAB_JOBS.get(_as_text(job.get("downstream_job_id")))
        if downstream_job is not None:
            producer_done_event = downstream_job.get("producer_done_event")
            if isinstance(producer_done_event, asyncio.Event):
                producer_done_event.set()
            terminal_event = downstream_job.get("terminal_event")
            if (
                isinstance(terminal_event, asyncio.Event)
                and int(downstream_job.get("completed_count", 0) or 0)
                >= int(downstream_job.get("target_count", 0) or 0)
            ):
                terminal_event.set()
        if workflow is not None:
            await _close_attachment_extraction_workflow_resources(workflow)
        job.pop("workflow", None)


@router.get("", response_class=FileResponse)
@router.get("/", response_class=FileResponse)
async def file_crawl_stage_lab_page() -> FileResponse:
    if not _HTML_PATH.exists():
        raise HTTPException(status_code=404, detail="file crawl stage lab frontend missing")
    return FileResponse(
        _HTML_PATH,
        media_type="text/html; charset=utf-8",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "X-File-Crawl-Stage-Lab-Build": "no-idle-poll-v2",
        },
    )


@router.get("/simhash", response_class=FileResponse)
async def file_crawl_simhash_payload_page() -> FileResponse:
    if not _SIMHASH_HTML_PATH.exists():
        raise HTTPException(status_code=404, detail="file SimHash payload frontend missing")
    return FileResponse(
        _SIMHASH_HTML_PATH,
        media_type="text/html; charset=utf-8",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@router.get("/api/simhash/requests")
async def list_file_simhash_requests(
    after_sequence: int = 0,
    job_id: str = "",
) -> Dict[str, Any]:
    from backend.shared.file_simhash_request_trace import list_file_simhash_request_trace

    requests = list_file_simhash_request_trace(
        after_sequence=after_sequence,
        job_id=job_id,
    )
    return {"requests": requests, "count": len(requests)}


@router.get("/api/simhash/events")
async def stream_file_simhash_requests(
    after_sequence: int = 0,
    job_id: str = "",
) -> StreamingResponse:
    from backend.shared.file_simhash_request_trace import list_file_simhash_request_trace

    async def event_stream():
        sequence = max(0, int(after_sequence or 0))
        while True:
            entries = list_file_simhash_request_trace(
                after_sequence=sequence,
                job_id=job_id,
            )
            for entry in entries:
                sequence = max(sequence, int(entry.get("sequence") or 0))
                yield f"id: {sequence}\nevent: simhash_request\ndata: {json.dumps(entry, ensure_ascii=False)}\n\n"
            yield ": keepalive\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/operational-jobs/{job_id}")
async def get_operational_file_crawl_job(job_id: str) -> Dict[str, Any]:
    """Read an already running production file crawl by its real job ID."""
    jid = _as_text(job_id)
    if not jid:
        raise HTTPException(status_code=400, detail="job_id is required")

    workflow = crawler_state.workflows.get(jid)
    if workflow is not None and not is_file_mode_workflow(workflow):
        raise HTTPException(status_code=404, detail="job_id is not a file crawl workflow")

    history = dict(crawler_state.job_history.get(jid) or {})
    snapshot = await asyncio.to_thread(read_file_crawl_attachment_snapshot, jid)
    runtime = _operational_attachment_view(workflow, snapshot) if workflow is not None else {
        "loaded_url_count": int(snapshot.get("loaded_url_count", 0) or 0),
        "detail_results": list(snapshot.get("detail_results") or []),
        "detail_visited_count": int(snapshot.get("detail_visited_count", 0) or 0),
        "attachment_found_count": int(snapshot.get("attachment_found_count", 0) or 0),
        "processing": _file_processing_view(
            history.get("stats") if isinstance(history.get("stats"), dict) else history,
            int(snapshot.get("attachment_found_count", 0) or 0),
        ),
        "simhash_gate": _file_simhash_gate_view(
            history.get("stats") if isinstance(history.get("stats"), dict) else history,
        ),
        "worker_occupancy": [],
        "workflow": {
            "db_name": _as_text(history.get("db_name")) or _as_text(snapshot.get("db_name")),
            "chat_bot_id": _as_text(history.get("chat_bot_id")) or _as_text(snapshot.get("chat_bot_id")),
            "final_status": _as_text(history.get("status")),
            "stop_requested": False,
            "enable_db_save": None,
            "enable_learning": None,
            "skip_learning": None,
        },
    }
    redis_state = await _operation_redis_state(jid, runtime["workflow"].get("db_name") or "")
    if workflow is None and isinstance(redis_state.get("state"), dict):
        redis_stats = redis_state["state"].get("stats")
        runtime["processing"] = _file_processing_view(
            redis_stats if isinstance(redis_stats, dict) else redis_state["state"],
            int(runtime.get("attachment_found_count", 0) or 0),
        )
        runtime["simhash_gate"] = _file_simhash_gate_view(
            redis_stats if isinstance(redis_stats, dict) else redis_state["state"],
        )
    if not workflow and not history and not redis_state.get("available") and not snapshot.get("available"):
        raise HTTPException(status_code=404, detail="operational file crawl job was not found")
    if not runtime["workflow"].get("db_name") and redis_state.get("db_name"):
        runtime["workflow"]["db_name"] = redis_state["db_name"]
    craw_id = getattr(workflow, "craw_id", None) if workflow is not None else history.get("craw_id")
    database = await _operation_database_counts(
        job_id=jid,
        db_name=_as_text(runtime["workflow"].get("db_name")),
        craw_id=craw_id,
    )
    return {
        "job_id": jid,
        "source": "runtime_and_snapshot" if workflow is not None and snapshot.get("available") else (
            "runtime" if workflow is not None else (
                "attachment_snapshot" if snapshot.get("available") else "redis_or_history"
            )
        ),
        "active": workflow is not None,
        "history": _json_value(history),
        **runtime,
        "database": database,
        "redis_available": bool(redis_state.get("available")),
        "message": (
            "추출 결과를 기록하면서 실행 중입니다."
            if workflow is not None and snapshot.get("available")
            else (
                "기록된 상세페이지·첨부파일 목록입니다."
                if snapshot.get("available")
                else "이 Job은 첨부파일 추출 스냅샷이 생성되기 전 작업입니다."
            )
        ),
    }


@router.post("/api/jobs")
async def start_start_url_lab_job(request: Request) -> Dict[str, Any]:
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid JSON payload: {exc}") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON object payload required")

    db_name = _as_text(body.get("db_name"))
    chat_bot_id = _as_text(body.get("chat_bot_id"))
    learn_list_id = _as_optional_positive_int(body.get("learn_list_id"), field="learn_list_id")
    if not db_name or not chat_bot_id:
        raise HTTPException(status_code=400, detail="db_name and chat_bot_id are required")
    if learn_list_id is None:
        raise HTTPException(status_code=400, detail="learn_list_id is required")

    job_id = _as_text(body.get("job_id")) or str(uuid.uuid4())
    if job_id in _LAB_JOBS and _LAB_JOBS[job_id].get("status") in {"queued", "loading"}:
        raise HTTPException(status_code=409, detail="job_id is already active")

    options = {
        "db_name": db_name,
        "chat_bot_id": chat_bot_id,
        "contents_url": None,
        "target_domains": None,
        "method": _as_text(body.get("method")) or "period",
        "target_date": body.get("start_urls_target_date") or body.get("target_date"),
        "exploration_date_filter_enabled": bool(body.get("exploration_date_filter_enabled", False)),
        "scope_path_prefix": None,
        "start_urls_order": _as_text(body.get("start_urls_order")) or None,
        "use_category_rules": False,
        "dedupe_urls": True,
        "limit": _DEFAULT_START_URL_LIMIT,
        "learn_list_id_scope": learn_list_id,
        # Same scope behavior as the file branch of crawl_start._prepare_crawl.
        "scope_by_contents_learn_list_id": True,
    }
    job: Dict[str, Any] = {
        "job_id": job_id,
        "stage": "start_url_load",
        "db_name": db_name,
        "chat_bot_id": chat_bot_id,
        "learn_list_id": learn_list_id,
        "start_url_limit": _DEFAULT_START_URL_LIMIT,
        "status": "queued",
        "created_at": _now(),
        "started_at": "",
        "completed_at": "",
        "elapsed_ms": None,
        "error": "",
        "stop_requested": False,
        "options": options,
        "results": [],
        "post_items": [],
        "events": [],
    }
    _lab_event(job, "job_queued")
    task = asyncio.create_task(_run_start_url_load(job_id), name=f"file-crawl-stage-lab:{job_id}")
    job["task"] = task
    _LAB_JOBS[job_id] = job
    _trim_finished_jobs()
    return _job_view(job)


@router.post("/api/jobs/{job_id}/attachment-extractions")
async def start_attachment_extraction_lab_job(job_id: str) -> Dict[str, Any]:
    source_job = _LAB_JOBS.get(_as_text(job_id))
    if source_job is None:
        raise HTTPException(status_code=404, detail="source job not found")
    if source_job.get("stage") != "start_url_load":
        raise HTTPException(status_code=400, detail="source job must be a stage 1 start URL job")
    if source_job.get("status") != "completed":
        raise HTTPException(status_code=409, detail="stage 1 must be completed before stage 2 starts")
    post_items = list(source_job.get("post_items") or [])
    if not post_items:
        raise HTTPException(status_code=409, detail="stage 1 has no detail URLs to extract")

    extraction_job_id = str(uuid.uuid4())
    validation_job = _new_download_validation_job(
        source_job_id=extraction_job_id,
        db_name=source_job["db_name"],
        chat_bot_id=source_job["chat_bot_id"],
        learn_list_id=source_job["learn_list_id"],
        producer_done=False,
    )
    job: Dict[str, Any] = {
        "job_id": extraction_job_id,
        "stage": "attachment_extract",
        "source_job_id": source_job["job_id"],
        "db_name": source_job["db_name"],
        "chat_bot_id": source_job["chat_bot_id"],
        "learn_list_id": source_job["learn_list_id"],
        "target_count": len(post_items),
        "status": "queued",
        "created_at": _now(),
        "started_at": "",
        "completed_at": "",
        "elapsed_ms": None,
        "error": "",
        "stop_requested": False,
        "post_items": post_items,
        "results": [],
        "events": [],
        "downstream_job_id": validation_job["job_id"],
        "options": {
            "source_job_id": source_job["job_id"],
            "enqueue": False,
            "download": False,
            "save": False,
            "learning": False,
            "playwright_fallback_on_fetch_failure": True,
            "concurrency": _stage_attachment_extract_concurrency(),
            "timeout_sec": _file_crawl_detail_fetch_timeout_sec({}),
        },
        "stop_event": asyncio.Event(),
    }
    _lab_event(job, "attachment_extract_queued", source_job_id=source_job["job_id"], post_count=len(post_items))
    _LAB_JOBS[validation_job["job_id"]] = validation_job
    validation_task = asyncio.create_task(
        _run_download_validation(validation_job["job_id"]),
        name=f"file-crawl-stage-lab:download-validation:{validation_job['job_id']}",
    )
    validation_job["task"] = validation_task
    _lab_event(
        validation_job,
        "download_validation_queued",
        source_job_id=extraction_job_id,
        candidate_count=0,
        streaming_from_stage_2=True,
    )
    _LAB_JOBS[extraction_job_id] = job
    task = asyncio.create_task(
        _run_attachment_extraction(extraction_job_id),
        name=f"file-crawl-stage-lab:attachment-extract:{extraction_job_id}",
    )
    job["task"] = task
    _trim_finished_jobs()
    return _job_view(job)


@router.post("/api/jobs/{job_id}/download-validations")
async def start_download_validation_lab_job(job_id: str) -> Dict[str, Any]:
    source_job = _LAB_JOBS.get(_as_text(job_id))
    if source_job is None:
        raise HTTPException(status_code=404, detail="source job not found")
    if source_job.get("stage") != "attachment_extract":
        raise HTTPException(status_code=400, detail="source job must be a stage 2 attachment extraction job")
    downstream_job = _LAB_JOBS.get(_as_text(source_job.get("downstream_job_id")))
    if downstream_job is not None:
        return _job_view(downstream_job)
    if source_job.get("status") != "completed":
        raise HTTPException(status_code=409, detail="stage 2 must be completed before stage 3 starts")

    validation_job_id = str(uuid.uuid4())
    candidates, non_document_filtered_count = _build_download_validation_candidates(
        source_job,
        validation_job_id,
    )
    job = _new_download_validation_job(
        source_job_id=source_job["job_id"],
        db_name=source_job["db_name"],
        chat_bot_id=source_job["chat_bot_id"],
        learn_list_id=source_job["learn_list_id"],
        initial_candidates=candidates,
        non_document_filtered_count=non_document_filtered_count,
        producer_done=True,
        job_id=validation_job_id,
    )
    _lab_event(
        job,
        "download_validation_queued",
        source_job_id=source_job["job_id"],
        candidate_count=0,
        non_document_filtered_count=non_document_filtered_count,
    )
    _LAB_JOBS[validation_job_id] = job
    task = asyncio.create_task(
        _run_download_validation(validation_job_id),
        name=f"file-crawl-stage-lab:download-validation:{validation_job_id}",
    )
    job["task"] = task
    _trim_finished_jobs()
    return _job_view(job)


@router.get("/api/jobs")
async def list_start_url_lab_jobs() -> Dict[str, Any]:
    jobs = sorted(
        (_job_view(job) for job in _LAB_JOBS.values()),
        key=lambda item: str(item.get("created_at") or ""),
        reverse=True,
    )
    return {"jobs": jobs}


@router.get("/api/jobs/{job_id}")
async def get_start_url_lab_job(job_id: str) -> Dict[str, Any]:
    job = _LAB_JOBS.get(_as_text(job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return {**_job_view(job), "events": list(job.get("events") or [])}


@router.get("/api/jobs/{job_id}/results")
async def get_start_url_lab_results(job_id: str, offset: int = 0, limit: int = 100) -> Dict[str, Any]:
    job = _LAB_JOBS.get(_as_text(job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    start = max(0, int(offset))
    size = max(1, min(int(limit), _DEFAULT_START_URL_LIMIT))
    results = list(job.get("results") or [])
    return {
        "job_id": job_id,
        "status": job.get("status"),
        "total": len(results),
        "offset": start,
        "limit": size,
        "results": results[start : start + size],
    }


@router.post("/api/jobs/{job_id}/stop")
async def stop_start_url_lab_job(job_id: str) -> Dict[str, Any]:
    job = _LAB_JOBS.get(_as_text(job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    related_job_ids = [job["job_id"]]
    downstream_job_id = _as_text(job.get("downstream_job_id"))
    if downstream_job_id:
        related_job_ids.append(downstream_job_id)
    for related_job_id in related_job_ids:
        related_job = _LAB_JOBS.get(related_job_id)
        if related_job is None:
            continue
        related_job["stop_requested"] = True
        task = related_job.get("task")
        if isinstance(task, asyncio.Task) and not task.done():
            task.cancel()
        stop_event = related_job.get("stop_event")
        if isinstance(stop_event, asyncio.Event):
            stop_event.set()
        if related_job.get("status") == "queued":
            related_job["status"] = "stopped"
            related_job["completed_at"] = _now()
        _lab_event(related_job, "stop_requested", requested_from_job_id=job["job_id"])
    return _job_view(job)


@router.get("/api/jobs/{job_id}/events")
async def stream_start_url_lab_events(job_id: str) -> StreamingResponse:
    job = _LAB_JOBS.get(_as_text(job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")

    async def event_stream():
        sent = 0
        while True:
            current = _LAB_JOBS.get(job_id)
            if current is None:
                return
            events = list(current.get("events") or [])
            while sent < len(events):
                yield f"event: stage\ndata: {json.dumps(events[sent], ensure_ascii=False)}\n\n"
                sent += 1
            if current.get("status") in {"completed", "failed", "stopped"}:
                yield f"event: terminal\ndata: {json.dumps(_job_view(current), ensure_ascii=False)}\n\n"
                return
            yield ": keepalive\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _trim_finished_jobs() -> None:
    finished = [
        job
        for job in _LAB_JOBS.values()
        if job.get("status") in {"completed", "failed", "stopped"}
    ]
    finished.sort(key=lambda item: str(item.get("completed_at") or item.get("created_at") or ""))
    for job in finished[:-_MAX_JOB_HISTORY]:
        _LAB_JOBS.pop(str(job.get("job_id") or ""), None)
