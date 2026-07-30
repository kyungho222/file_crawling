import asyncio
import hashlib
import json
import logging
import os
import time
import random
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from types import SimpleNamespace
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import parse_qsl, urlparse
from uuid import uuid4

from fastapi import BackgroundTasks, Request, APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from backend.shared.pre_explored_url import (
    _load_category_url_pattern_object,
    count_exploration_post_urls,
    resolve_cate_for_detail_url,
    stream_asadal_urls_from_db,
)
from backend.shared.file_crawl_post_urls import (
    load_file_crawl_post_url_strings,
)
from backend.shared.crawl_dispatcher import dispatch_and_schedule_workflow
from backend.shared.crawl_monitor import monitor_auto_stop
from backend.shared.crawl_shared import (
    bool_from_payload,
    bootstrap_job_state,
    cache_job_metadata,
    detect_board_crawl,
    publish_client_redis_heartbeat,
    resolve_stream_matched_rules_only,
    swallow_task_exception,
)
from backend.shared.duplicate_category_only_mode import ignore_period_enabled
from backend.shared.summary_only_mode import (
    is_summary_only_request,
    normalize_duplicate_summary_request_mode,
    normalize_summary_request_mode,
)
from backend.shared.title_only_mode import (
    is_title_only_request,
    normalize_duplicate_title_request_mode,
)
from backend.shared.title_candidate_mode import (
    apply_title_candidate_preview,
    get_title_candidate_preview_status,
    queue_title_candidate_preview,
    request_title_candidate_preview_stop,
)
from backend.shared.sub_change_mode import (
    is_partial_title_change_request,
    is_partial_title_only_request,
    partial_title_change_enabled,
    partial_update_fields_without_title,
)
from backend.shared.type_postprocess import is_type_postprocess_request, run_type_postprocess
from backend.shared.partial_category_postprocess import (
    is_partial_category_postprocess_request,
    partial_category_debug_reason,
    run_partial_category_postprocess,
)
from backend.shared.file_category_mode import (
    file_category_mode,
    is_file_category_update_only_request,
    is_file_crawl_request,
)
from backend.shared.detail_page_utils import is_detail_page_url
from backend.shared.direct_detail_category import build_direct_detail_start_url_item
from backend.shared.learn_list_start_url_dedupe import (
    apply_learn_list_start_url_dedupe,
    filter_start_urls_against_loaded_learn_list_cache,
    load_learn_list_url_keys,
)
from backend.shared.learn_list_duplicate_groups import load_learn_list_url_duplicate_groups
from backend.shared.duplicate_learning_metadata_postprocess import (
    request_duplicate_learning_metadata_postprocess_stop,
    run_duplicate_learning_metadata_postprocess,
)
from backend.shared.redis_sse_service import update_state_only, get_redis, send_message_to_redis_sse
from backend.shared.stage_url_report import append_stage_urls
from backend.shared.url_scope import (
    extract_service_scope_path_prefix,
    extract_scope_host,
    normalize_scope_path_prefix,
    scope_path_prefix_enabled,
    url_matches_scope_identities,
)
from backend.shared.crawler_state import crawler_state
from utils.url import ensure_url_scheme
from utils.timezone_utils import get_local_now
from utils.db_name import resolve_db_name
from db.mariadb_save_update import (
    _ensure_file_learning_category_mapping,
    ensure_learn_list_standard_columns,
    get_account_identifier_from_chatbot_setup,
    get_category_table_name,
    get_learn_list_table_name,
    resolve_learn_list_table_name_for_chatbot,
)
from db.mysql_db_config import mysql_execute_query
from backend.file.file_category_apply import (
    preview_file_category_apply_plan,
    sync_existing_file_categories_from_homepage_learning,
)
from backend.file.file_category_update_only import run_file_category_update_only

logger = logging.getLogger("backend.shared.crawl_start")
CONCURRENT_CRAWL_LOG_PREFIX = "[ConcurrentCrawlStartDebug]"
SONGPA_TITLE_TRACE_PREFIX = "[SongpaTitleTrace]"


def _songpa_title_trace(stage: str, *, url: str = "", **fields: Any) -> None:
    if "songpa.go.kr" not in str(url or "").lower():
        return
    try:
        compact = {
            str(k): (str(v or "")[:240] if v is not None else "")
            for k, v in (fields or {}).items()
        }
        logger.warning("%s stage=%s url=%s fields=%s", SONGPA_TITLE_TRACE_PREFIX, stage, str(url or "")[:240], compact)
        try:
            os.makedirs("logs", exist_ok=True)
            with open(os.path.join("logs", "songpa_title_trace.log"), "a", encoding="utf-8") as fp:
                fp.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {SONGPA_TITLE_TRACE_PREFIX} module=crawl_start stage={stage} url={str(url or '')[:240]} fields={compact}\n")
        except Exception:
            pass
    except Exception:
        pass

router = APIRouter()
_ACCELERATED_PARSE_SAVE_LOGGER: Optional[logging.Logger] = None

_BURST_DEDUPE_JOB_KEY_PREFIX = "crawl_start:burst_dedupe_job:"
_START_URLS_DEFAULT_START_DATE_ISO = "2026-01-01"
_TODAY_DATE_ALIASES = frozenset({"today", "오늘", "금일", "now"})
_PARTIAL_DEBOUNCE_SECONDS = 0.5
_PARTIAL_DEBOUNCE_QUEUES: Dict[str, Dict[str, Any]] = {}
_PARTIAL_DEBOUNCE_LOCK = asyncio.Lock()


def _accelerated_parse_save_logger() -> logging.Logger:
    global _ACCELERATED_PARSE_SAVE_LOGGER
    if _ACCELERATED_PARSE_SAVE_LOGGER is not None:
        return _ACCELERATED_PARSE_SAVE_LOGGER

    log = logging.getLogger("backend.accelerated_parse_save")
    log.setLevel(logging.INFO)
    log.propagate = False
    if not log.handlers:
        default_log_file = os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, "downloads", "accelerated_parse_save.log")
        )
        log_file = os.path.abspath(os.getenv("ACCELERATED_PARSE_SAVE_LOG_FILE", default_log_file) or default_log_file)
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(handler)
    _ACCELERATED_PARSE_SAVE_LOGGER = log
    return log


def _log_accelerated_parse_save(event: str, **fields: Any) -> None:
    try:
        safe = {
            key: (value[:500] if isinstance(value, str) and len(value) > 500 else value)
            for key, value in (fields or {}).items()
        }
        _accelerated_parse_save_logger().info(
            json.dumps({"event": event, **safe}, ensure_ascii=False, default=str)
        )
    except Exception:
        pass


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return int(value)
    except Exception:
        return default


def _resolve_redis_exploration_count(data: Dict[str, Any], actual_count: int) -> int:
    actual = max(0, _safe_int(actual_count, 0))
    if not isinstance(data, dict):
        return actual
    display_max = max(
        0,
        _safe_int(
            data.get("exploration_post_total_count")
            or data.get("explorationPostTotalCount")
            or data.get("exploration_display_max_count")
            or data.get("explorationDisplayMaxCount")
            or data.get("redis_exploration_max_count")
            or data.get("redisExplorationMaxCount"),
            0,
        ),
    )
    if bool_from_payload(
        data.get("exploration_display_count_fixed")
        or data.get("explorationDisplayCountFixed")
        or data.get("redis_exploration_count_fixed")
        or data.get("redisExplorationCountFixed")
    ) and display_max > 0:
        return display_max
    if bool_from_payload(data.get("accelerated_crawl")) or str(data.get("crawl_mode") or "").strip().lower() == "accelerated":
        return actual
    return actual


def _accelerated_sse_publish_enabled(data: Dict[str, Any]) -> bool:
    for key in ("accelerated_sse_enabled", "acceleratedSseEnabled", "sse_enabled", "sseEnabled"):
        if key in data:
            return bool_from_payload(data.get(key))
    return str(os.getenv("ACCELERATED_SSE_PUBLISH_ENABLED", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _accelerated_redis_state_enabled(data: Dict[str, Any]) -> bool:
    for key in ("accelerated_redis_state_enabled", "acceleratedRedisStateEnabled", "redis_state_enabled", "redisStateEnabled"):
        if key in data:
            return bool_from_payload(data.get(key))
    return str(os.getenv("ACCELERATED_REDIS_STATE_ENABLED", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _set_accelerated_memory_state(job_id: str, payload: Dict[str, Any]) -> None:
    try:
        if not hasattr(crawler_state, "accelerated_job_stats"):
            crawler_state.accelerated_job_stats = {}
        crawler_state.accelerated_job_stats[str(job_id)] = dict(payload or {})
    except Exception:
        pass


def _normalize_file_crawl_route_hint(data: Dict[str, Any], *, stage: str) -> None:
    """Keep file-crawl requests on the file workflow even when older clients send mixed hints."""
    if not isinstance(data, dict):
        return
    try:
        content_type = str(data.get("content_type") or "").strip().lower()
    except Exception:
        content_type = ""
    try:
        colle = str(data.get("colle") or "").strip().lower()
    except Exception:
        colle = ""

    should_force_file = content_type in {"file", "attach", "attachment"} or colle == "file"
    if not should_force_file:
        return

    before_colle = data.get("colle")
    before_content_type = data.get("content_type")
    data["colle"] = "file"
    data["ui_colle"] = "file"
    data["colle_mode"] = "file"
    data["_file_crawl_mode"] = True
    if not content_type or content_type in {"file", "attach", "attachment"}:
        data["content_type"] = "file"
    try:
        logger.debug(
            "[FileRouteDebug][%s] force file route | job_id=%s before_colle=%s before_content_type=%s after_colle=%s after_content_type=%s",
            stage,
            data.get("job_id"),
            before_colle,
            before_content_type,
            data.get("colle"),
            data.get("content_type"),
        )
    except Exception:
        pass


def _partial_update_fields(data: Dict[str, Any]) -> set[str]:
    fields = (data or {}).get("partial_update_fields")
    if isinstance(fields, str):
        fields = [fields]
    if not isinstance(fields, list):
        return set()
    return {str(item or "").strip().lower() for item in fields if str(item or "").strip()}


def _burst_dedupe_enabled() -> bool:
    try:
        return str(os.getenv("CRAWL_START_BURST_DEDUPE", "1") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
    except Exception:
        return True


def _burst_dedupe_ttl_seconds() -> int:
    try:
        ttl = int(os.getenv("CRAWL_START_BURST_DEDUPE_TTL_SECONDS", "20") or "20")
    except Exception:
        ttl = 20
    return max(5, min(ttl, 120))


def _normalize_string_list(value: Any) -> List[str]:
    if isinstance(value, list):
        items = value
    elif value in (None, ""):
        items = []
    else:
        items = [value]
    out: List[str] = []
    for item in items:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _resolve_request_categories(data: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    cate1 = data.get("cate1")
    cate2 = data.get("cate2")
    if cate1 is None:
        cate1 = meta.get("cate1")
    if cate2 is None:
        cate2 = meta.get("cate2")
    try:
        cate1_text = str(cate1).strip() if cate1 is not None else None
    except Exception:
        cate1_text = None
    try:
        cate2_text = str(cate2).strip() if cate2 is not None else None
    except Exception:
        cate2_text = None
    return cate1_text or None, cate2_text or None


def _logical_crawl_request_fingerprint(data: Dict[str, Any], *, db_name: str) -> str:
    colle = str(data.get("colle") or "").strip().lower()
    contents = _normalize_string_list(data.get("contents"))
    contents_url = _resolve_primary_contents_url(data) or ""
    target_domains = sorted(set(_normalize_string_list(data.get("target_domains"))))
    target_date = _normalize_string_list(data.get("target_date"))
    scope_path_prefix = _resolve_requested_scope_path_prefix(data)
    method = str(data.get("method") or "period").strip().lower()
    crawl_mode = str(data.get("crawl_mode") or "").strip().lower()
    summary_mode = _resolve_summary_request_mode(data)
    duplicate_summary_mode = normalize_duplicate_summary_request_mode(
        data.get("duplicate_summary_mode")
        or data.get("duplicateSummaryMode")
        or data.get("board_duplicate_summary")
        or data.get("duplicate_summary")
    )
    duplicate_title_mode = normalize_duplicate_title_request_mode(
        data.get("duplicate_title_mode")
        or data.get("duplicateTitleMode")
        or data.get("board_duplicate_title")
        or data.get("duplicate_title")
        or data.get("title_mode")
        or data.get("titleMode")
    )
    start_urls_order = _resolve_start_urls_order(data)
    chat_bot_id = str(
        data.get("chat_bot_id") or (data.get("metadata") or {}).get("chat_bot_id") or ""
    ).strip()

    normalized_contents = []
    for item in contents[:5]:
        normalized_contents.append(ensure_url_scheme(item) if "://" in item or item.startswith("www.") else item)

    if contents_url:
        contents_url = ensure_url_scheme(contents_url)

    payload = {
        "colle": colle,
        "db_name": str(db_name or "").strip().lower(),
        "chat_bot_id": chat_bot_id,
        "contents": normalized_contents,
        "contents_url": contents_url,
        "target_domains": target_domains,
        "target_date": target_date,
        "scope_path_prefix": scope_path_prefix,
        "method": method,
        "crawl_mode": crawl_mode,
        "summary_mode": summary_mode,
        "duplicate_summary_mode": duplicate_summary_mode,
        "duplicate_title_mode": duplicate_title_mode,
        "start_urls_order": start_urls_order,
        "file_category_mode": file_category_mode(data),
        "file_category_update_only": is_file_category_update_only_request(data),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _resolve_summary_request_mode(data: Dict[str, Any]) -> str:
    duplicate_summary_mode = normalize_duplicate_summary_request_mode(
        data.get("duplicate_summary_mode")
        or data.get("duplicateSummaryMode")
        or data.get("board_duplicate_summary")
        or data.get("duplicate_summary")
    )
    if duplicate_summary_mode == "summary":
        return "on"
    raw = (
        data.get("summary_mode")
        or data.get("summaryMode")
        or data.get("board_summary")
        or data.get("summary_only")
        or ""
    )
    normalized = normalize_summary_request_mode(raw)
    if normalized == "only":
        return "only"
    return "on" if normalized == "on" else "off"


def _is_summary_only_request(data: Dict[str, Any]) -> bool:
    return is_summary_only_request(data or {})


def _is_partial_summary_request(data: Dict[str, Any]) -> bool:
    fields = _partial_update_fields(data)
    return (
        str(data.get("colle") or "").strip().lower() == "content"
        and bool(fields & {"symmary", "summary"})
        and data.get("partial_target_filter") is not None
    )


def _is_partial_content_relearn_request(data: Dict[str, Any]) -> bool:
    if str(os.getenv("PARTIAL_CONTENT_RELEARN_ENABLED", "0") or "0").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
        "y",
    }:
        return False
    fields = _partial_update_fields(data)
    return (
        str(data.get("colle") or "").strip().lower() == "content"
        and "content" in fields
        and not bool(fields & {"title", "cate", "symmary", "summary"})
        and data.get("partial_target_filter") is not None
    )


_PARTIAL_INTERNAL_ORDER = ("title", "symmary", "summary", "content", "type", "cate")


def _ordered_partial_internal_fields(data: Dict[str, Any]) -> List[str]:
    raw_fields = (data or {}).get("partial_update_fields")
    if isinstance(raw_fields, str):
        raw_items = [raw_fields]
    elif isinstance(raw_fields, list):
        raw_items = raw_fields
    else:
        raw_items = []
    out: List[str] = []
    for item in raw_items:
        field = str(item or "").strip().lower()
        if field == "summary":
            field = "symmary"
        if field in {"title", "symmary", "content", "type", "cate"} and field not in out:
            out.append(field)
    return out


def _empty_field_save_counts() -> Dict[str, int]:
    return {
        "title": 0,
        "content": 0,
        "cate": 0,
        "symmary": 0,
        "type": 0,
        "url": 0,
        "web_de": 0,
    }


def _merge_field_save_counts(results: List[Dict[str, Any]]) -> Dict[str, int]:
    merged = _empty_field_save_counts()
    for result in results:
        counts = result.get("field_save_counts") if isinstance(result, dict) else {}
        if not isinstance(counts, dict):
            continue
        for key in merged:
            try:
                merged[key] += int(counts.get(key, 0) or 0)
            except Exception:
                pass
    return merged


def _is_debounceable_partial_request(data: Dict[str, Any]) -> bool:
    fields = _ordered_partial_internal_fields(data)
    return (
        str((data or {}).get("colle") or "").strip().lower() == "content"
        and len(fields) >= 1
        and all(field in {"title", "symmary", "content", "type", "cate"} for field in fields)
        and (data or {}).get("partial_target_filter") is not None
    )


async def _enqueue_partial_debounce(
    data: Dict[str, Any],
    background_tasks: BackgroundTasks,
    *,
    db_name: str,
    job_id: str,
) -> bool:
    if not job_id or not _is_debounceable_partial_request(data):
        return False
    fields = _ordered_partial_internal_fields(data)
    async with _PARTIAL_DEBOUNCE_LOCK:
        queue = _PARTIAL_DEBOUNCE_QUEUES.get(job_id)
        if not queue:
            queue = {
                "data_by_field": {},
                "background_tasks": background_tasks,
                "db_name": db_name,
                "task": None,
            }
            _PARTIAL_DEBOUNCE_QUEUES[job_id] = queue
        for field in fields:
            queue["data_by_field"][field] = dict(data)
        if queue.get("task") is None or queue["task"].done():
            queue["task"] = asyncio.create_task(
                _flush_partial_debounce(job_id),
                name=f"partial_debounce:{job_id}",
            )
    logger.info("[PartialDebounce] queued | job_id=%s db=%s fields=%s", job_id, db_name, fields)
    queued_counts = _empty_field_save_counts()
    await update_state_only(
        job_id=job_id,
        account_name=db_name,
        payload={
            "status": "running",
            "event": "partial_sequence_waiting",
            "job_id": job_id,
            "account_name": db_name,
            "total_count": len(fields),
            "scan_count": len(fields),
            "collection_count": 0,
            "save_count": 0,
            "study_count": 0,
            "field_save_counts": queued_counts,
            "source": "partial_sequence",
            "partial_sequence_running": True,
            "message": "partial sequence queued",
        },
    )
    await send_message_to_redis_sse(
        job_id=job_id,
        dbname=db_name,
        message={
            "status": "running",
            "event": "partial_sequence_waiting",
            "job_id": job_id,
            "account_name": db_name,
            "total_count": len(fields),
            "scan_count": len(fields),
            "collection_count": 0,
            "save_count": 0,
            "study_count": 0,
            "field_save_counts": queued_counts,
            "source": "partial_sequence",
            "partial_sequence_running": True,
            "message": "partial sequence queued",
        },
    )
    return True


async def _flush_partial_debounce(job_id: str) -> None:
    await asyncio.sleep(_PARTIAL_DEBOUNCE_SECONDS)
    async with _PARTIAL_DEBOUNCE_LOCK:
        queue = _PARTIAL_DEBOUNCE_QUEUES.pop(job_id, None)
    if not queue:
        return
    data_by_field = dict(queue.get("data_by_field") or {})
    fields = [field for field in ("title", "symmary", "content", "type", "cate") if field in data_by_field]
    if not fields:
        return
    base_data = dict(data_by_field[fields[0]])
    base_data["partial_update_fields"] = fields
    try:
        await _run_partial_internal_sequence(
            base_data,
            queue.get("background_tasks"),
            db_name=str(queue.get("db_name") or resolve_db_name(base_data, default="dev_user") or "dev_user"),
            job_id=job_id,
            fields=fields,
            data_by_field=data_by_field,
        )
    except Exception as exc:
        logger.exception("[PartialDebounce] flush failed | job_id=%s fields=%s err=%s", job_id, fields, exc)


def _is_title_only_request(data: Dict[str, Any]) -> bool:
    return is_title_only_request(data or {})


def _resolve_start_urls_order(data: Dict[str, Any]) -> str:
    raw = str(
        data.get("start_urls_order")
        or data.get("crawl_direction")
        or data.get("start_url_direction")
        or ""
    ).strip().lower()
    if raw in {"reverse", "desc", "backward", "backwards", "from_back", "back"}:
        return "reverse"
    if raw in {"shuffle", "random", "rand", "randomize", "mixed"}:
        return "shuffle"
    if data.get("reverse_start_urls") is True:
        return "reverse"
    if data.get("shuffle_start_urls") is True:
        return "shuffle"
    return "forward"


async def _acquire_burst_dedupe_lock(
    *,
    redis,
    data: Dict[str, Any],
    db_name: str,
    job_id: str,
) -> tuple[bool, str, Optional[str], int]:
    fingerprint = _logical_crawl_request_fingerprint(data, db_name=db_name)
    key = f"crawl_start:burst_dedupe:{fingerprint}"
    ttl = _burst_dedupe_ttl_seconds()
    job_id = str(job_id or "").strip()
    acquired = False
    existing_job_id: Optional[str] = None
    try:
        acquired = bool(await redis.set(key, job_id, ex=ttl, nx=True))
    except Exception:
        return True, key, None, ttl

    if not acquired:
        try:
            raw_existing = await redis.get(key)
            if isinstance(raw_existing, (bytes, bytearray)):
                raw_existing = raw_existing.decode("utf-8", errors="replace")
            existing_job_id = str(raw_existing or "").strip() or None
        except Exception:
            existing_job_id = None
        if existing_job_id and existing_job_id != job_id:
            job_key = f"{key}:job:{job_id}"
            try:
                job_acquired = bool(await redis.set(job_key, job_id, ex=ttl, nx=True))
            except Exception:
                job_acquired = True
            if job_acquired:
                logger.info(
                    "[StartDedupe] same fingerprint but different job_id; allow concurrent start | "
                    "job_id=%s existing_job_id=%s ttl=%s",
                    job_id,
                    existing_job_id,
                    ttl,
                )
                return True, job_key, existing_job_id, ttl
    return acquired, key, existing_job_id, ttl


async def _remember_burst_dedupe_lock(*, redis, job_id: str, dedupe_key: str, ttl: int) -> None:
    if not job_id or not dedupe_key:
        return
    await redis.set(f"{_BURST_DEDUPE_JOB_KEY_PREFIX}{job_id}", dedupe_key, ex=max(5, int(ttl or 5)))


async def release_burst_dedupe_lock(job_id: Optional[str], *, redis=None) -> None:
    job_id = str(job_id or "").strip()
    if not job_id:
        return
    client = redis or await get_redis()
    mapping_key = f"{_BURST_DEDUPE_JOB_KEY_PREFIX}{job_id}"
    raw_dedupe_key = await client.get(mapping_key)
    dedupe_key = ""
    if isinstance(raw_dedupe_key, (bytes, bytearray)):
        dedupe_key = raw_dedupe_key.decode("utf-8", errors="replace").strip()
    elif raw_dedupe_key is not None:
        dedupe_key = str(raw_dedupe_key).strip()
    delete_keys = [mapping_key]
    if dedupe_key:
        delete_keys.append(dedupe_key)
    await client.delete(*delete_keys)


def _first_contents_url(contents: object) -> Optional[str]:
    try:
        if isinstance(contents, list) and contents:
            first = contents[0]
            if isinstance(first, dict):
                first = first.get("url") or first.get("content") or first.get("contents_url") or first.get("target_url")
            value = str(first or "").strip()
            return value or None
        if isinstance(contents, dict):
            value = str(contents.get("url") or contents.get("content") or contents.get("contents_url") or contents.get("target_url") or "").strip()
            return value or None
        if isinstance(contents, str):
            value = contents.strip()
            return value or None
    except Exception:
        return None
    return None


def _resolve_primary_contents_url(data: Dict[str, Any]) -> Optional[str]:
    try:
        for candidate in (
            _first_contents_url(data.get("contents")),
            str(data.get("contents_url") or "").strip(),
            str(data.get("target_url") or "").strip(),
        ):
            if candidate:
                return candidate
    except Exception:
        return None
    return None


def _resolve_learn_list_id_scope(data: Dict[str, Any]) -> Any:
    for key in ("learn_list_id", "learnListId", "learn_id", "learnId", "db_id", "dbId"):
        value = data.get(key)
        if value not in (None, ""):
            return value
    for source_key in ("contents", "contents_url", "target_url"):
        value = data.get(source_key)
        items = value if isinstance(value, list) else [value]
        for item in items:
            if not isinstance(item, dict):
                continue
            for key in ("learn_list_id", "learnListId", "learn_id", "learnId", "db_id", "dbId", "id"):
                item_value = item.get(key)
                if item_value not in (None, ""):
                    return item_value
    return None


def _resolve_requested_scope_path_prefix(data: Dict[str, Any]) -> str:
    if not scope_path_prefix_enabled(data):
        data["scope_path_prefix"] = ""
        return ""

    for key in ("scope_path_prefix", "path_prefix", "start_urls_path_prefix"):
        if key not in data:
            continue
        normalized = normalize_scope_path_prefix(data.get(key))
        data["scope_path_prefix"] = normalized
        return normalized
    normalized = normalize_scope_path_prefix(data.get("scope_path_prefix"))
    if not normalized:
        normalized = extract_service_scope_path_prefix(_resolve_primary_contents_url(data))
    if "scope_path_prefix" in data or normalized:
        data["scope_path_prefix"] = normalized
    return normalized


def _direct_url_matches_requested_scope(url: str, scope_path_prefix: str) -> bool:
    normalized_url = ensure_url_scheme(str(url or "").strip())
    if not normalized_url:
        return False
    normalized_prefix = normalize_scope_path_prefix(scope_path_prefix)
    if not normalized_prefix:
        return True
    scope_host = extract_scope_host(normalized_url)
    if not scope_host:
        return False
    return url_matches_scope_identities(
        normalized_url,
        [scope_host],
        path_prefix=normalized_prefix,
    )


def _coerce_stream_matched_rules_only(raw: Any) -> bool:
    """stream_matched_rules_only 값을 정규화한다. 기본값은 False(명시적으로 켠 경우에만 규칙에 매칭된 post start_urls만 사용)."""
    return resolve_stream_matched_rules_only({"stream_matched_rules_only": raw}, default=False)


def _resolve_start_urls_date_filter_enabled(data: Dict[str, Any]) -> bool:
    for key in (
        "start_urls_date_filter_enabled",
        "exploration_date_filter_enabled",
        "start_urls_date_filter",
    ):
        if key in data:
            return bool_from_payload(data.get(key))
    return False


def _normalize_start_urls_target_date(data: Dict[str, Any], *, enabled: bool) -> Optional[List[str]]:
    if not enabled:
        return None

    try:
        today_iso = get_local_now().date().isoformat()
    except Exception:
        today_iso = datetime.now().date().isoformat()

    raw = data.get("start_urls_target_date")
    if isinstance(raw, list):
        start_raw = str(raw[0] or "").strip() if len(raw) >= 1 else ""
        end_raw = str(raw[1] or "").strip() if len(raw) >= 2 else ""
    else:
        start_raw = ""
        end_raw = ""

    changed = False
    if not start_raw:
        start_raw = _START_URLS_DEFAULT_START_DATE_ISO
        changed = True
    if not end_raw or end_raw.lower() in _TODAY_DATE_ALIASES:
        end_raw = today_iso
        changed = True

    normalized = [start_raw, end_raw]
    if changed or raw != normalized:
        data["start_urls_target_date"] = normalized
    return normalized


def _safe_print(*args, **kwargs):
    try: print(*args, **kwargs, flush=True)
    except: pass

def _is_job_terminal(job_id: str) -> bool:
    """작업이 완료/중단/오류 등 terminal 상태인지 확인해 SSE/중복 실행 흐름에서 더 진행하지 않도록 한다."""
    try:
        hist = crawler_state.job_history.get(job_id) if job_id else None
        st = str((hist or {}).get("status") or "").strip().lower()
        if st in {"completed", "cancelled", "error", "stop", "stopped", "stop_requested"}:
            return True
    except Exception:
        pass
    try:
        wf = crawler_state.workflows.get(job_id) if job_id else None
        if wf is not None:
            raw_final = str(getattr(wf, "final_status", "") or "").strip().lower()
            if raw_final in {"completed", "complete", "finished", "cancelled", "canceled", "stopped", "stop", "error", "failed", "fail"}:
                return True
    except Exception:
        pass
    return False


def _accelerated_item_url(item: Any) -> str:
    if isinstance(item, dict):
        return str(item.get("url") or item.get("content") or item.get("href") or "").strip()
    return str(item or "").strip()


def _accelerated_item_category_hint(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    raw = str(item.get("cate_match") or "").strip()
    if raw.startswith("cate_match|"):
        return raw
    raw_type = str(item.get("type") or "").strip()
    if raw_type.startswith("cate_match|"):
        return raw_type
    cate1 = str(
        item.get("cate1")
        or item.get("category1")
        or item.get("category")
        or item.get("main_cate")
        or item.get("main_category")
        or ""
    ).strip()
    cate2 = str(
        item.get("cate2")
        or item.get("category2")
        or item.get("sub_cate")
        or item.get("sub_category")
        or item.get("board_name")
        or item.get("title_category")
        or ""
    ).strip()
    if cate1 or cate2:
        return f"cate_match|{cate1}|{cate2}"
    return ""


def _accelerated_parse_playwright_disabled() -> bool:
    return str(os.getenv("ACCELERATED_DISABLE_PLAYWRIGHT", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _accelerated_url_host(url: str) -> str:
    try:
        return (urlparse(str(url or "")).netloc or "").lower().removeprefix("www.")
    except Exception:
        return ""


def _accelerated_should_skip_fast_url(url: str, *, target_host: str = "") -> Optional[str]:
    raw = str(url or "").strip()
    low = raw.lower()
    if not raw:
        return "empty_url"
    if target_host:
        host = _accelerated_url_host(raw)
        if host and host != target_host and not host.endswith("." + target_host):
            return "out_of_target_host"
    if "ready.do" in low:
        return "ready_page"
    if "/main/" in low and "view.do" not in low and "nttid=" not in low:
        return "main_page"
    if low.endswith("/main.do") or "/main/main.do" in low:
        return "main_page"
    return None


def _accelerated_is_likely_detail_url(url: str) -> bool:
    low = str(url or "").strip().lower()
    if not low:
        return False
    if any(token in low for token in ("list.do", "selectbbslist.do", "/main/", "main.do", ".jsp")):
        return False
    detail_tokens = (
        "view.do",
        "detail.do",
        "contractview.do",
        "accountview.do",
        "bd_selectbbs.do",
        "bd_selectnftcbbsdetail.do",
        "bd_selectlobastcmbbsdetail.do",
        "selectbbsnttview.do",
        "nttid=",
        "nttno=",
        "ctrtacctbookmngno=",
    )
    return any(token in low for token in detail_tokens)


def _prefilter_accelerated_start_urls(
    start_urls: List[Any],
    *,
    contents_url: str,
) -> Tuple[List[Any], Dict[str, Any]]:
    target_host = _accelerated_url_host(contents_url)
    before = len(start_urls or [])
    apply_detail_filter = before >= int(os.getenv("ACCELERATED_PREFILTER_DETAIL_MIN_COUNT", "5000") or "5000")
    filtered: List[Any] = []
    skipped = 0
    reason_counts: Dict[str, int] = {}
    samples: List[Dict[str, str]] = []

    for item in start_urls or []:
        raw_url = _accelerated_item_url(item)
        reason = ""
        if target_host:
            host = _accelerated_url_host(raw_url)
            if host and host != target_host and not host.endswith("." + target_host):
                reason = "out_of_target_host"
        if not reason and apply_detail_filter and not _accelerated_is_likely_detail_url(raw_url):
            reason = "not_likely_detail"
        if reason:
            skipped += 1
            reason_counts[reason] = int(reason_counts.get(reason, 0) or 0) + 1
            if len(samples) < 10:
                samples.append({"url": raw_url[:240], "reason": reason})
            continue
        filtered.append(item)

    return filtered, {
        "enabled": True,
        "before": before,
        "after": len(filtered),
        "skipped": skipped,
        "target_host": target_host,
        "detail_filter_applied": apply_detail_filter,
        "reason_counts": reason_counts,
        "samples": samples,
    }


def _filter_accelerated_fast_urls(
    start_urls: List[Any],
    *,
    contents_url: str,
) -> Tuple[List[Any], Dict[str, Any]]:
    target_host = _accelerated_url_host(contents_url)
    filtered: List[Any] = []
    skipped = 0
    reason_counts: Dict[str, int] = {}
    samples: List[Dict[str, str]] = []
    for item in start_urls or []:
        raw_url = _accelerated_item_url(item)
        reason = _accelerated_should_skip_fast_url(raw_url, target_host=target_host)
        if reason:
            skipped += 1
            reason_counts[reason] = int(reason_counts.get(reason, 0) or 0) + 1
            if len(samples) < 10:
                samples.append({"url": raw_url[:240], "reason": reason})
            continue
        if isinstance(item, dict) and _accelerated_parse_playwright_disabled():
            item = dict(item)
            item["disable_playwright"] = True
        if isinstance(item, dict):
            cate_hint = _accelerated_item_category_hint(item)
            if cate_hint:
                item = dict(item)
                item["cate_match"] = cate_hint
        filtered.append(item)
    return filtered, {
        "enabled": True,
        "before": len(start_urls or []),
        "after": len(filtered),
        "skipped": skipped,
        "target_host": target_host,
        "reason_counts": reason_counts,
        "samples": samples,
        "disable_playwright": _accelerated_parse_playwright_disabled(),
    }


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    try:
        return str(value)
    except Exception:
        return repr(value)


def _accelerated_mariadb_payload_paths(job_id: str) -> Dict[str, str]:
    log_path = os.getenv(
        "ACCELERATED_PARSE_SAVE_LOG_FILE",
        os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                os.pardir,
                os.pardir,
                "downloads",
                "accelerated_parse_save.log",
            )
        ),
    )
    base_dir = os.path.dirname(os.path.abspath(log_path))
    safe_job_id = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(job_id or "unknown"))
    return {
        "jsonl": os.path.join(base_dir, f"accelerated_mariadb_payloads_{safe_job_id}.jsonl"),
        "json": os.path.join(base_dir, f"accelerated_mariadb_payloads_{safe_job_id}.json"),
    }


def _safe_int_or_none(value: Any) -> Optional[int]:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return int(text)
    except Exception:
        return None


def _accelerated_crawling_log_wait_timeout_seconds() -> float:
    try:
        value = float(os.getenv("CRAWLING_LOG_WAIT_TIMEOUT_SEC", "3") or "3")
    except Exception:
        value = 3.0
    return max(0.0, min(value, 30.0))


async def _wait_for_accelerated_crawling_log_row(job_id: str, db_name: str) -> bool:
    """Wait briefly for the externally-created ASADAL_CRAWLING_LOG row."""
    if not job_id or not db_name:
        return False
    timeout = _accelerated_crawling_log_wait_timeout_seconds()
    if timeout <= 0:
        return False
    deadline = time.monotonic() + timeout
    delay = 0.15
    while True:
        try:
            from db.crawl_db_manager import get_crawling_log_summary

            summary = await get_crawling_log_summary(job_id, dbname=db_name)
            if summary:
                _log_accelerated_parse_save(
                    "crawling_log_row_ready",
                    job_id=job_id,
                    db_name=db_name,
                    waited_ms=int(max(0.0, timeout - (deadline - time.monotonic())) * 1000),
                )
                return True
        except Exception as exc:
            logger.debug(
                "[AcceleratedCrawl] crawling_log row wait check failed | job_id=%s db=%s err=%s",
                job_id,
                db_name,
                exc,
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            _log_accelerated_parse_save(
                "crawling_log_row_wait_timeout",
                job_id=job_id,
                db_name=db_name,
                waited_ms=int(timeout * 1000),
            )
            return False
        await asyncio.sleep(min(delay, remaining))
        delay = min(delay * 1.5, 1.0)


async def _update_accelerated_crawling_log(
    *,
    job_id: str,
    db_name: str,
    data: Optional[Dict[str, Any]] = None,
    scan: Optional[int] = None,
    collection: Optional[int] = None,
    saved: Optional[int] = None,
    study: Optional[int] = None,
    status: Optional[str] = None,
    pages: Optional[int] = None,
    reason: str = "",
) -> bool:
    if not job_id or not db_name:
        return False
    payload = data or {}
    db_scan = scan
    db_status = status
    if str(status or "").strip().lower() == "running":
        db_status = "start"
    db_collection = collection
    if saved is not None:
        db_collection = saved
    try:
        original_scan = _safe_int_or_none(
            payload.get("crawling_log_scan_count")
            or payload.get("original_exploration_scan_count")
            or payload.get("exploration_original_count")
        )
        if original_scan is not None and original_scan > 0:
            db_scan = original_scan
    except Exception:
        db_scan = scan
    log_id = (
        _safe_int_or_none(payload.get("id"))
        or _safe_int_or_none(payload.get("craw_id"))
        or _safe_int_or_none(payload.get("crawling_log_id"))
        or _safe_int_or_none((payload.get("metadata") or {}).get("craw_id") if isinstance(payload.get("metadata"), dict) else None)
    )
    colle = str(payload.get("colle") or payload.get("crawl_type") or "").strip() or None
    try:
        from db.crawl_db_manager import update_crawling_log_counters

        if log_id is None:
            await _wait_for_accelerated_crawling_log_row(str(job_id), str(db_name))

        ok = await update_crawling_log_counters(
            job_id=str(job_id),
            scan=db_scan,
            collection=db_collection,
            saved=saved,
            study=study,
            dbname=str(db_name),
            status=db_status,
            log_id=log_id,
            pages=pages,
            colle=colle,
        )
        _log_accelerated_parse_save(
            "crawling_log_update_done",
            job_id=job_id,
            db_name=db_name,
            reason=reason,
            status=db_status,
            runtime_status=status,
            scan=db_scan,
            runtime_scan=scan,
            collection=db_collection,
            runtime_collection=collection,
            saved=saved,
            study=study,
            pages=pages,
            log_id=log_id,
            ok=bool(ok),
        )
        return bool(ok)
    except Exception as exc:
        _log_accelerated_parse_save(
            "crawling_log_update_error",
            job_id=job_id,
            db_name=db_name,
            reason=reason,
            status=db_status,
            runtime_status=status,
            scan=db_scan,
            runtime_scan=scan,
            collection=db_collection,
            runtime_collection=collection,
            saved=saved,
            study=study,
            pages=pages,
            log_id=log_id,
            error=str(exc),
        )
        logger.warning(
            "[AcceleratedCrawl] crawling_log update failed | job_id=%s db=%s status=%s reason=%s err=%s",
            job_id,
            db_name,
            status,
            reason,
            exc,
        )
        return False


def _build_accelerated_mariadb_record(
    *,
    job_id: str,
    db_name: str,
    chat_bot_id: str,
    index: int,
    total: int,
    url: str,
    post_info: Dict[str, Any],
    display: Dict[str, Any],
    learning_result: Dict[str, Any],
    elapsed_ms: int,
) -> Dict[str, Any]:
    info = dict(post_info or {})
    stored_url = str(info.get("post_url") or info.get("url") or url or "").strip()
    title = str(info.get("title") or info.get("subject") or stored_url or "").strip()
    web_title = str(info.get("web_title") or title or "").strip()
    try:
        size_val = int(info.get("size") or 0)
    except Exception:
        size_val = 0
    reg_date = str(info.get("reg_date") or info.get("content_created_at") or "").strip()
    updated_date = str(info.get("content_updated_at") or "").strip()
    author = str(info.get("author") or info.get("content_author") or "").strip()

    learn_list_row_preview: Dict[str, Any] = {
        "content": stored_url,
        "subject": title or stored_url,
        "content_type": str(info.get("content_type") or "url").strip() or "url",
        "status": "N",
        "size": size_val,
        "created_at": datetime.now().isoformat(),
        "type": str(info.get("type") or "post").strip() or "post",
        "cate1": info.get("cate1") or "",
        "cate2": info.get("cate2") or "",
        "web_title": web_title or title or "",
        "content_created_at": reg_date,
        "content_updated_at": updated_date,
        "memo1": "",
        "content_author": author,
    }
    return _json_safe_value(
        {
            "job_id": job_id,
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "index": index,
            "total": total,
            "url": stored_url,
            "elapsed_ms": elapsed_ms,
            "saved_at": datetime.now().isoformat(),
            "mariadb_target": "LEARN_LIST",
            "mariadb_operation": "insert_board_post_into_learn_list",
            "learn_list_row_preview": learn_list_row_preview,
            "post_info": info,
            "display": dict(display or {}),
            "learning_result": dict(learning_result or {}),
        }
    )


def _write_json_file(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_json_safe_value(payload), f, ensure_ascii=False, indent=2)


def _append_jsonl_file(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(_json_safe_value(payload), ensure_ascii=False))
        f.write("\n")


def _append_jsonl_batch_file(path: str, payloads: List[Any]) -> None:
    if not payloads:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for payload in payloads:
            f.write(json.dumps(_json_safe_value(payload), ensure_ascii=False))
            f.write("\n")


async def _run_accelerated_parse_selected(
    *,
    data: Dict[str, Any],
    db_name: str,
    job_id: str,
    chat_bot_id: str,
    selected_urls: List[Any],
    scan_count: int,
    dedupe_meta: Dict[str, Any],
    warm_meta: Dict[str, Any],
    redis_scan_count: Optional[int] = None,
) -> Dict[str, Any]:
    from backend.board.board_content_workflow import BoardContentWorkflow, _DetailItem
    from utils.url import canonicalize_url_for_dedup

    total_selected = len(selected_urls or [])
    redis_scan_count = _safe_int(redis_scan_count, _resolve_redis_exploration_count(data, scan_count))
    target_url = _resolve_primary_contents_url(data) or ""
    memo = (data.get("memo") or [""])[0] if isinstance(data.get("memo"), list) else data.get("memo")
    try:
        raw_concurrency = int(os.getenv("ACCELERATED_PARSE_CONCURRENCY", "5") or "5")
    except Exception:
        raw_concurrency = 10
    parse_concurrency = max(1, min(raw_concurrency, 50))
    try:
        raw_parse_timeout = float(
            data.get("accelerated_parse_timeout_sec")
            or data.get("acceleratedParseTimeoutSec")
            or os.getenv("ACCELERATED_PARSE_TIMEOUT_SEC", "15")
            or "15"
        )
    except Exception:
        raw_parse_timeout = 15.0
    parse_timeout_sec = max(5.0, min(raw_parse_timeout, 300.0))
    aggregate_lock = asyncio.Lock()
    mariadb_record_lock = asyncio.Lock()
    publish_lock = asyncio.Lock()
    mariadb_write_queue: asyncio.Queue = asyncio.Queue()
    postprocess_queue: asyncio.Queue = asyncio.Queue()
    mariadb_records: List[Dict[str, Any]] = []
    failed_records: List[Dict[str, Any]] = []
    mariadb_written_count = 0
    mariadb_write_errors = 0
    mariadb_insert_success_count = 0
    mariadb_insert_failed_count = 0
    embedding_batch_submitted_count = 0
    embedding_batch_failed_count = 0
    embedding_batch_skipped_count = 0
    summary_submit_count = 0
    summary_failed_count = 0
    accelerated_pg_table_name: Optional[str] = None
    accelerated_pg_table_checked = False
    accelerated_pg_table_lock = asyncio.Lock()
    mariadb_paths = _accelerated_mariadb_payload_paths(job_id)
    mariadb_insert_enabled = str(os.getenv("ACCELERATED_MARIADB_INSERT_ENABLED", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    try:
        mariadb_json_batch_size = max(
            1,
            min(
                int(
                    data.get("accelerated_mariadb_batch_size")
                    or data.get("accelerated_batch_size")
                    or os.getenv("ACCELERATED_MARIADB_JSON_BATCH_SIZE", "10")
                    or "10"
                ),
                500,
            ),
        )
    except Exception:
        mariadb_json_batch_size = 10
    try:
        mariadb_json_flush_interval_sec = max(
            0.5,
            min(float(os.getenv("ACCELERATED_MARIADB_JSON_FLUSH_INTERVAL_SEC", "2") or "2"), 30.0),
        )
    except Exception:
        mariadb_json_flush_interval_sec = 2.0
    embedding_batch_enabled = str(os.getenv("ACCELERATED_EMBEDDING_BATCH_ENABLED", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    sse_publish_enabled = _accelerated_sse_publish_enabled(data)
    redis_state_enabled = _accelerated_redis_state_enabled(data)
    summary_submit_enabled = str(os.getenv("ACCELERATED_SUMMARY_ENABLED", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    try:
        postprocess_concurrency = max(
            1,
            min(int(os.getenv("ACCELERATED_POSTPROCESS_CONCURRENCY", "2") or "2"), 10),
        )
    except Exception:
        postprocess_concurrency = 2
    try:
        crawling_log_update_interval_sec = max(
            5.0,
            min(float(os.getenv("ACCELERATED_CRAWLING_LOG_UPDATE_INTERVAL_SEC", "15") or "15"), 300.0),
        )
    except Exception:
        crawling_log_update_interval_sec = 15.0
    last_crawling_log_update_ts = 0.0
    last_crawling_log_counts: Tuple[int, int, int, int] = (-1, -1, -1, -1)
    last_progress_publish_ts = 0.0
    last_progress_publish_done = 0
    try:
        progress_publish_interval_sec = max(
            0.5,
            min(float(os.getenv("ACCELERATED_PROGRESS_PUBLISH_INTERVAL_SEC", "3") or "3"), 30.0),
        )
    except Exception:
        progress_publish_interval_sec = 3.0
    try:
        progress_publish_every = max(
            1,
            min(int(os.getenv("ACCELERATED_PROGRESS_PUBLISH_EVERY", "20") or "20"), 1000),
        )
    except Exception:
        progress_publish_every = 20
    aggregate_stats: Dict[str, Any] = {
        "save_count": 0,
        "save_done_count": 0,
        "save_failed_count": 0,
        "parsed_count": 0,
        "study_count": 0,
        "study_done_count": 0,
        "study_success_count": 0,
        "study_failed_count": 0,
        "last_parsed_url": None,
        "last_parsed_title": None,
        "last_parsed_content_size": None,
    }
    category_rule_obj_cache: Optional[Dict[str, Any]] = None
    category_rule_loaded = False
    category_rule_count = 0
    category_url_to_cate_map: Dict[str, str] = {}
    try:
        for _item in selected_urls or []:
            if not isinstance(_item, dict):
                continue
            _url = _accelerated_item_url(_item)
            _key = canonicalize_url_for_dedup(_url) or _url
            _hint = _accelerated_item_category_hint(_item)
            if _key and _hint:
                category_url_to_cate_map[_key] = _hint
        if chat_bot_id and db_name:
            import json as _json
            from backend.shared.pre_explored_url import get_category_url_pattern_raw

            raw_rules = await get_category_url_pattern_raw(
                str(chat_bot_id),
                str(data.get("method") or "period"),
                str(db_name),
                contents_url=target_url or None,
            )
            if raw_rules:
                category_rule_obj_cache = _json.loads(raw_rules) if isinstance(raw_rules, str) else raw_rules
                category_rule_loaded = True
                try:
                    category_rule_count = len((category_rule_obj_cache or {}).get("rules") or [])
                except Exception:
                    category_rule_count = 0
    except Exception as exc:
        category_rule_obj_cache = None
        category_rule_loaded = False
        _log_accelerated_parse_save(
            "category_rule_load_error",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            error=str(exc),
        )

    def _new_parse_workflow(scope_item: Any = None) -> BoardContentWorkflow:
        wf = BoardContentWorkflow()
        wf.job_id = job_id
        wf.db_name = db_name
        wf.chat_bot_id = chat_bot_id
        wf.target_url = target_url
        wf.contents_url = target_url
        wf.cate1 = data.get("cate1")
        wf.cate2 = data.get("cate2")
        wf.memo = memo
        wf.enable_db_save = False
        wf.enable_learning = False
        wf.accelerated_parse_only = True
        wf.accelerated_fast_parse_only = True
        wf.auto_category_enabled = True
        try:
            wf.accelerated_fetch_timeout_sec = max(
                1.0,
                min(
                    float(
                        data.get("accelerated_fetch_timeout_sec")
                        or data.get("acceleratedFetchTimeoutSec")
                        or os.getenv("FILE_CRAWL_ACCELERATED_FETCH_TIMEOUT_SEC")
                        or os.getenv("ACCELERATED_FETCH_TIMEOUT_SEC", "10")
                        or "10"
                    ),
                    30.0,
                ),
            )
        except Exception:
            wf.accelerated_fetch_timeout_sec = 10.0
        try:
            file_detail_concurrency_default = (
                os.getenv("BOARD_CONTENT_PIPELINE_DETAIL_CONCURRENCY")
                or os.getenv("BOARD_CONTENT_DETAIL_CONCURRENCY")
                or "1"
            )
            wf.board_dashboard_detail_concurrency = max(
                1,
                min(
                    int(
                        data.get("file_crawl_detail_concurrency")
                        or data.get("fileCrawlDetailConcurrency")
                        or os.getenv("FILE_CRAWL_DETAIL_CONCURRENCY")
                        or file_detail_concurrency_default
                    ),
                    1,
                ),
            )
        except Exception:
            wf.board_dashboard_detail_concurrency = 1
        try:
            wf._domain_fetch_max_concurrent = max(
                1,
                min(
                    int(
                        data.get("file_crawl_domain_max_concurrent")
                        or data.get("fileCrawlDomainMaxConcurrent")
                        or os.getenv("FILE_CRAWL_DOMAIN_MAX_CONCURRENT")
                        or os.getenv("BOARD_FETCH_DOMAIN_MAX_CONCURRENT")
                        or "1"
                    ),
                    1,
                ),
            )
        except Exception:
            wf._domain_fetch_max_concurrent = 1
        try:
            wf._domain_fetch_min_delay_sec = max(
                0.0,
                min(
                    float(
                        data.get("file_crawl_fetch_min_delay_sec")
                        or data.get("fileCrawlFetchMinDelaySec")
                        or os.getenv("FILE_CRAWL_FETCH_MIN_DELAY_SEC", "0.8")
                        or "0.8"
                    ),
                    10.0,
                ),
            )
        except Exception:
            wf._domain_fetch_min_delay_sec = 0.8
        try:
            wf._domain_fetch_max_delay_sec = max(
                float(getattr(wf, "_domain_fetch_min_delay_sec", 0.8) or 0.8),
                min(
                    float(
                        data.get("file_crawl_fetch_max_delay_sec")
                        or data.get("fileCrawlFetchMaxDelaySec")
                        or os.getenv("FILE_CRAWL_FETCH_MAX_DELAY_SEC", "1.8")
                        or "1.8"
                    ),
                    15.0,
                ),
            )
        except Exception:
            wf._domain_fetch_max_delay_sec = 1.8
        wf.start_urls_override_source = "accelerated_crawl"
        wf.pre_explored_start_urls_count = total_selected
        wf._url_to_cate_map = dict(category_url_to_cate_map)
        wf._category_rule_obj_cache = category_rule_obj_cache
        wf.stats["scan_count"] = int(scan_count or 0)
        wf.stats["total_count"] = int(scan_count or 0)
        wf.stats["collection_count"] = 0
        try:
            wf._configure_path_scope(
                start_urls=[scope_item] if scope_item is not None else [],
                target_domains=data.get("target_domains"),
                contents_url=target_url,
            )
        except Exception:
            pass
        return wf

    async def _publish(event: str, extra: Optional[Dict[str, Any]] = None) -> None:
        async with aggregate_lock:
            snapshot = dict(aggregate_stats)
        payload = {
            "status": "running",
            "event": event,
            "source": "accelerated_crawl",
            "scan_count": int(redis_scan_count or 0),
            "total_count": int(redis_scan_count or 0),
            "actual_scan_count": int(scan_count or 0),
            "collection_count": int(snapshot.get("parsed_count", 0) or 0),
            "save_count": int(snapshot.get("save_count", 0) or 0),
            "save_done_count": int(snapshot.get("save_done_count", 0) or 0),
            "save_failed_count": int(snapshot.get("save_failed_count", 0) or 0),
            "study_count": int(snapshot.get("study_count", 0) or 0),
            "study_done_count": int(snapshot.get("study_done_count", 0) or 0),
            "study_success_count": int(snapshot.get("study_success_count", 0) or 0),
            "study_failed_count": int(snapshot.get("study_failed_count", 0) or 0),
            "failed_record_count": int(snapshot.get("failed_record_count", 0) or 0),
            "failed_records_sample": list(snapshot.get("failed_records_sample") or []),
            "h3": "file crawl completed",
            "message": "file crawl completed",
        }
        payload.update(extra or {})
        payload["scan_count"] = int(redis_scan_count or 0)
        payload["total_count"] = int(redis_scan_count or 0)
        payload["actual_scan_count"] = int(scan_count or 0)
        payload["collection_count"] = int(snapshot.get("parsed_count", 0) or 0)
        # Counter fields must always reflect the aggregate DB-confirmed state.
        # Per-item progress payloads may be produced before the writer finishes a
        # MariaDB batch, so do not let stale extras overwrite these values.
        payload["save_count"] = int(snapshot.get("save_count", 0) or 0)
        payload["save_done_count"] = int(snapshot.get("save_done_count", 0) or 0)
        payload["save_failed_count"] = int(snapshot.get("save_failed_count", 0) or 0)
        payload["study_count"] = int(snapshot.get("study_count", 0) or 0)
        payload["study_done_count"] = int(snapshot.get("study_done_count", 0) or 0)
        payload["study_success_count"] = int(snapshot.get("study_success_count", 0) or 0)
        payload["study_failed_count"] = int(snapshot.get("study_failed_count", 0) or 0)
        payload["failed_record_count"] = int(snapshot.get("failed_record_count", 0) or 0)
        payload["failed_records_sample"] = list(snapshot.get("failed_records_sample") or [])
        async with publish_lock:
            _set_accelerated_memory_state(job_id, payload)
            if redis_state_enabled:
                await update_state_only(job_id=job_id, account_name=db_name, payload=payload)
            if sse_publish_enabled:
                await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=payload)

    async def _publish_progress_if_due(extra: Dict[str, Any]) -> None:
        nonlocal last_progress_publish_ts, last_progress_publish_done
        done_count = int((extra or {}).get("parsed") or 0)
        now = time.monotonic()
        if (
            done_count < total_selected
            and done_count - last_progress_publish_done < progress_publish_every
            and now - last_progress_publish_ts < progress_publish_interval_sec
        ):
            return
        last_progress_publish_ts = now
        last_progress_publish_done = done_count
        await _publish("accelerated_parse_progress", extra)
        await _update_crawling_log_if_due(reason="progress_publish")

    async def _update_crawling_log_if_due(*, reason: str, status: Optional[str] = None, force: bool = False) -> None:
        nonlocal last_crawling_log_update_ts, last_crawling_log_counts
        async with aggregate_lock:
            snapshot = dict(aggregate_stats)
        scan_val = int(scan_count or 0)
        collection_val = int(snapshot.get("parsed_count", 0) or 0)
        saved_val = int(snapshot.get("save_count", 0) or 0)
        study_val = int(snapshot.get("study_count", 0) or 0)
        counts = (scan_val, collection_val, saved_val, study_val)
        now = time.monotonic()
        if (
            not force
            and status is None
            and counts == last_crawling_log_counts
            and now - last_crawling_log_update_ts < crawling_log_update_interval_sec
        ):
            return
        if (
            not force
            and status is None
            and now - last_crawling_log_update_ts < crawling_log_update_interval_sec
        ):
            return
        last_crawling_log_update_ts = now
        last_crawling_log_counts = counts
        await _update_accelerated_crawling_log(
            job_id=job_id,
            db_name=db_name,
            data=data,
            scan=scan_val,
            collection=collection_val,
            saved=saved_val,
            study=study_val,
            status=status,
            reason=reason,
        )

    async def _mariadb_json_writer() -> None:
        nonlocal mariadb_written_count, mariadb_write_errors
        batch: List[Dict[str, Any]] = []

        async def _write_snapshot() -> None:
            async with mariadb_record_lock:
                snapshot_records = list(mariadb_records)
                snapshot_failed_records = list(failed_records)
            snapshot_payload = {
                "job_id": job_id,
                "db_name": db_name,
                "chat_bot_id": chat_bot_id,
                "status": "running",
                "mariadb_target": "LEARN_LIST",
                "mariadb_operation": "insert_board_post_into_learn_list",
                "record_count": len(snapshot_records),
                "failed_record_count": len(snapshot_failed_records),
                "jsonl_written_count": mariadb_written_count,
                "jsonl_write_errors": mariadb_write_errors,
                "mariadb_insert_enabled": mariadb_insert_enabled,
                "mariadb_insert_success_count": mariadb_insert_success_count,
                "mariadb_insert_failed_count": mariadb_insert_failed_count,
                "embedding_batch_enabled": embedding_batch_enabled,
                "embedding_batch_submitted_count": embedding_batch_submitted_count,
                "embedding_batch_failed_count": embedding_batch_failed_count,
                "embedding_batch_skipped_count": embedding_batch_skipped_count,
                "summary_submit_enabled": summary_submit_enabled,
                "summary_submit_count": summary_submit_count,
                "summary_failed_count": summary_failed_count,
                "category_rule_loaded": category_rule_loaded,
                "category_rule_count": category_rule_count,
                "category_url_hint_count": len(category_url_to_cate_map),
                "jsonl_path": mariadb_paths["jsonl"],
                "records": snapshot_records,
                "failed_records": snapshot_failed_records[-200:],
            }
            await asyncio.to_thread(_write_json_file, mariadb_paths["json"], snapshot_payload)

        async def _flush() -> None:
            nonlocal mariadb_written_count, mariadb_write_errors, mariadb_insert_success_count, mariadb_insert_failed_count, batch
            if not batch:
                return
            flush_items = batch
            batch = []
            started_at = time.perf_counter()
            try:
                await asyncio.to_thread(_append_jsonl_batch_file, mariadb_paths["jsonl"], flush_items)
                mariadb_written_count += len(flush_items)
                await _write_snapshot()
                _log_accelerated_parse_save(
                    "mariadb_json_batch_saved",
                    job_id=job_id,
                    db_name=db_name,
                    jsonl=mariadb_paths["jsonl"],
                    batch_count=len(flush_items),
                    written_count=mariadb_written_count,
                    elapsed_ms=int((time.perf_counter() - started_at) * 1000),
                )
            except Exception as exc:
                mariadb_write_errors += len(flush_items)
                _log_accelerated_parse_save(
                    "mariadb_json_batch_error",
                    job_id=job_id,
                    db_name=db_name,
                    jsonl=mariadb_paths["jsonl"],
                    batch_count=len(flush_items),
                    error=str(exc),
                    elapsed_ms=int((time.perf_counter() - started_at) * 1000),
                )
                return

            if not mariadb_insert_enabled:
                return
            insert_started_at = time.perf_counter()
            try:
                from db.mariadb_save_update import insert_board_posts_into_learn_list_batch

                insert_pairs = [
                    (item, item.get("post_info"))
                    for item in flush_items
                    if isinstance(item.get("post_info"), dict)
                ]
                post_infos = [post_info for _, post_info in insert_pairs]
                logger.info(
                    "[test002][AcceleratedMariadbDebug] flush_insert_prepare | job_id=%s db=%s flush_items=%s insert_pairs=%s post_infos=%s sample_urls=%s sample_titles=%s",
                    job_id,
                    db_name,
                    len(flush_items),
                    len(insert_pairs),
                    len(post_infos),
                    [
                        str(((post_info or {}).get("post_url") or (post_info or {}).get("url") or ""))[:180]
                        for post_info in post_infos[:5]
                    ],
                    [
                        str(((post_info or {}).get("title") or (post_info or {}).get("subject") or ""))[:120]
                        for post_info in post_infos[:5]
                    ],
                )
                _log_accelerated_parse_save(
                    "mariadb_insert_batch_begin",
                    job_id=job_id,
                    db_name=db_name,
                    save_count_source="mariadb_insert",
                    batch_count=len(post_infos),
                    jsonl_written_count=mariadb_written_count,
                )
                if not post_infos:
                    return
                results = await insert_board_posts_into_learn_list_batch(
                    chat_bot_id=str(chat_bot_id),
                    db_name=str(db_name),
                    post_infos=post_infos,
                )
                logger.info(
                    "[test002][AcceleratedMariadbDebug] batch_insert_result | job_id=%s db=%s post_infos=%s result_count=%s result_hit_count=%s none_count=%s sample_results=%s sample_duplicate_flags=%s",
                    job_id,
                    db_name,
                    len(post_infos),
                    len(results or []),
                    sum(1 for value in (results or []) if value),
                    sum(1 for value in (results or []) if not value),
                    list(results or [])[:10],
                    [bool((post_info or {}).get("learn_list_duplicate")) for post_info in post_infos[:10]],
                )
                inserted_count = 0
                duplicate_count = 0
                failed_count = 0
                for (record, _post_info), learn_list_id in zip(insert_pairs, results):
                    duplicate_row = bool((_post_info or {}).get("learn_list_duplicate"))
                    inserted_ok = bool(learn_list_id) and not duplicate_row
                    try:
                        record["learn_list_id"] = int(learn_list_id) if learn_list_id else None
                        record["mariadb_insert_ok"] = bool(learn_list_id)
                        record["mariadb_inserted"] = bool(inserted_ok)
                        record["mariadb_duplicate"] = bool(duplicate_row)
                    except Exception:
                        pass
                    if inserted_ok:
                        inserted_count += 1
                    elif learn_list_id and duplicate_row:
                        duplicate_count += 1
                    else:
                        failed_count += 1
                    if inserted_ok:
                        try:
                            postprocess_queue.put_nowait(record)
                        except Exception:
                            pass
                async with aggregate_lock:
                    mariadb_insert_success_count += inserted_count
                    mariadb_insert_failed_count += failed_count
                    aggregate_stats["save_count"] = int(aggregate_stats.get("save_count", 0) or 0) + inserted_count
                    aggregate_stats["save_done_count"] = int(aggregate_stats.get("save_done_count", 0) or 0) + len(post_infos)
                    aggregate_stats["save_failed_count"] = int(aggregate_stats.get("save_failed_count", 0) or 0) + failed_count
                    aggregate_save_count = int(aggregate_stats.get("save_count", 0) or 0)
                    aggregate_done_count = int(aggregate_stats.get("save_done_count", 0) or 0)
                    aggregate_failed_count = int(aggregate_stats.get("save_failed_count", 0) or 0)
                logger.info(
                    "[test002][AcceleratedMariadbDebug] batch_insert_classified | job_id=%s db=%s batch_count=%s inserted=%s duplicates=%s failed=%s aggregate_save=%s aggregate_done=%s aggregate_failed=%s postprocess_queued=%s",
                    job_id,
                    db_name,
                    len(post_infos),
                    inserted_count,
                    duplicate_count,
                    failed_count,
                    aggregate_save_count,
                    aggregate_done_count,
                    aggregate_failed_count,
                    inserted_count,
                )
                await _write_snapshot()
                _log_accelerated_parse_save(
                    "mariadb_insert_batch_done",
                    job_id=job_id,
                    db_name=db_name,
                    save_count_source="mariadb_insert",
                    batch_count=len(post_infos),
                    success_count=inserted_count,
                    inserted_count=inserted_count,
                    duplicate_count=duplicate_count,
                    failed_count=failed_count,
                    save_count=aggregate_save_count,
                    save_done_count=aggregate_done_count,
                    save_failed_count=aggregate_failed_count,
                    elapsed_ms=int((time.perf_counter() - insert_started_at) * 1000),
                )
                await _update_crawling_log_if_due(reason="mariadb_insert_batch_done")
                await _publish_progress_if_due(
                    {
                        "current_index": int(flush_items[-1].get("index") or 0) if flush_items else 0,
                        "parsed": int(aggregate_stats.get("parsed_count", 0) or 0),
                        "last_url": str(flush_items[-1].get("url") or "") if flush_items else "",
                        "last_parse_ok": True,
                        "concurrency": parse_concurrency,
                        "parse_timeout_sec": parse_timeout_sec,
                        "save_count": aggregate_save_count,
                        "save_done_count": aggregate_done_count,
                        "save_failed_count": aggregate_failed_count,
                        "mariadb_insert_success_count": mariadb_insert_success_count,
                        "mariadb_insert_failed_count": mariadb_insert_failed_count,
                    }
                )
            except Exception as exc:
                failed_count = len(flush_items)
                async with aggregate_lock:
                    mariadb_insert_failed_count += failed_count
                    aggregate_stats["save_done_count"] = int(aggregate_stats.get("save_done_count", 0) or 0) + failed_count
                    aggregate_stats["save_failed_count"] = int(aggregate_stats.get("save_failed_count", 0) or 0) + failed_count
                _log_accelerated_parse_save(
                    "mariadb_insert_batch_error",
                    job_id=job_id,
                    db_name=db_name,
                    save_count_source="mariadb_insert",
                    batch_count=len(flush_items),
                    error=str(exc),
                    elapsed_ms=int((time.perf_counter() - insert_started_at) * 1000),
                )

        while True:
            try:
                item = await asyncio.wait_for(
                    mariadb_write_queue.get(),
                    timeout=mariadb_json_flush_interval_sec,
                )
            except asyncio.TimeoutError:
                await _flush()
                continue
            try:
                if item is None:
                    await _flush()
                    return
                batch.append(item)
                if len(batch) >= mariadb_json_batch_size:
                    await _flush()
            finally:
                mariadb_write_queue.task_done()

    async def _ensure_accelerated_pg_table_name() -> Optional[str]:
        nonlocal accelerated_pg_table_name, accelerated_pg_table_checked
        if accelerated_pg_table_checked:
            return accelerated_pg_table_name
        async with accelerated_pg_table_lock:
            if accelerated_pg_table_checked:
                return accelerated_pg_table_name
            accelerated_pg_table_checked = True
            try:
                from utils.whoami import get_chat_id_from_db

                cid = await get_chat_id_from_db(db_name, chat_bot_id)
                if cid:
                    accelerated_pg_table_name = f"td_{cid}_training_data".lower()
                    _log_accelerated_parse_save(
                        "postprocess_pg_table_resolved",
                        job_id=job_id,
                        db_name=db_name,
                        chat_bot_id=chat_bot_id,
                        table_name=accelerated_pg_table_name,
                    )
            except Exception as exc:
                accelerated_pg_table_name = None
                _log_accelerated_parse_save(
                    "postprocess_pg_table_resolve_error",
                    job_id=job_id,
                    db_name=db_name,
                    chat_bot_id=chat_bot_id,
                    error=str(exc),
                )
            return accelerated_pg_table_name

    def _build_accelerated_learning_result(record: Dict[str, Any]) -> Dict[str, Any]:
        post_info = dict((record or {}).get("post_info") or {})
        learning_result = dict((record or {}).get("learning_result") or {})
        preview = dict((record or {}).get("learn_list_row_preview") or {})
        source_url = str(
            learning_result.get("source")
            or post_info.get("post_url")
            or post_info.get("url")
            or preview.get("content")
            or (record or {}).get("url")
            or ""
        ).strip()
        title = str(
            learning_result.get("title")
            or learning_result.get("subject")
            or post_info.get("title")
            or post_info.get("subject")
            or preview.get("subject")
            or source_url
        ).strip()
        content = str(learning_result.get("content") or post_info.get("content") or "").strip()
        return {
            **learning_result,
            "source": source_url,
            "title": title,
            "subject": title,
            "web_title": str(learning_result.get("web_title") or post_info.get("web_title") or title).strip(),
            "content": content,
            "type": str(learning_result.get("type") or post_info.get("type") or preview.get("type") or "post").strip() or "post",
            "content_type": str(
                learning_result.get("content_type")
                or post_info.get("content_type")
                or preview.get("content_type")
                or "url"
            ).strip()
            or "url",
            "size": learning_result.get("size") or post_info.get("size") or preview.get("size") or len(content),
            "cate1": learning_result.get("cate1") or post_info.get("cate1") or preview.get("cate1") or "",
            "cate2": learning_result.get("cate2") or post_info.get("cate2") or preview.get("cate2") or "",
            "reg_date": learning_result.get("reg_date") or post_info.get("reg_date") or preview.get("content_created_at"),
            "content_updated_at": (
                learning_result.get("content_updated_at")
                or post_info.get("content_updated_at")
                or preview.get("content_updated_at")
            ),
            "author": learning_result.get("author") or post_info.get("author") or preview.get("content_author") or "",
            "department": learning_result.get("department") or post_info.get("department") or "",
        }

    async def _submit_accelerated_summary(
        *,
        record: Dict[str, Any],
        result: Dict[str, Any],
        learn_list_id: int,
        normalized_text: str,
    ) -> None:
        nonlocal summary_submit_count, summary_failed_count
        if not summary_submit_enabled:
            return
        source_url = str(result.get("source") or (record or {}).get("url") or "").strip()
        if not (source_url and db_name and chat_bot_id and learn_list_id):
            return
        try:
            from backend.shared.summarize_keywords_client import (
                enqueue_summarize_keywords,
                post_summarize_keywords,
                summarize_keywords_endpoint,
                summarize_keywords_payload_concurrency,
                summarize_keywords_timeout_sec,
                summarize_keywords_use_queue,
            )

            payload: Dict[str, Any] = {
                "chat_bot_id": str(chat_bot_id),
                "db_name": str(db_name),
                "target_db": str(db_name),
                "target": "learn_list",
                "contents": [source_url],
                "content_type": "url",
                "concurrency": summarize_keywords_payload_concurrency(),
                "learn_list_id": int(learn_list_id),
            }
            if normalized_text:
                payload["normalized_text"] = normalized_text
                payload["normalized_contents"] = [normalized_text]
                payload["source_url"] = source_url
            table_name = f"ASADAL_{str(chat_bot_id).strip()[-12:]}_LEARN_LIST"
            payload["learn_table"] = table_name
            payload["target_table"] = table_name
            if summarize_keywords_use_queue():
                await enqueue_summarize_keywords(
                    summarize_keywords_endpoint(),
                    payload,
                    timeout_sec=summarize_keywords_timeout_sec(),
                )
                status = "queued"
            else:
                http_status, body = await post_summarize_keywords(
                    summarize_keywords_endpoint(),
                    payload,
                    timeout_sec=summarize_keywords_timeout_sec(),
                )
                if int(http_status or 0) != 200:
                    raise RuntimeError(f"http={http_status} body={str(body or '')[:240]}")
                status = "submitted"
            summary_submit_count += 1
            record["summary_submit_status"] = status
            _log_accelerated_parse_save(
                "summary_submit_done",
                job_id=job_id,
                db_name=db_name,
                learn_list_id=learn_list_id,
                status=status,
                normalized_text_len=len(normalized_text or ""),
                url=source_url,
            )
        except Exception as exc:
            summary_failed_count += 1
            record["summary_submit_status"] = "failed"
            record["summary_submit_error"] = str(exc)
            _log_accelerated_parse_save(
                "summary_submit_error",
                job_id=job_id,
                db_name=db_name,
                learn_list_id=learn_list_id,
                error=str(exc),
                url=str(result.get("source") or (record or {}).get("url") or "")[:240],
            )

    async def _mark_accelerated_study_status_y(
        *,
        record: Dict[str, Any],
        learn_list_id: int,
        source_url: str,
        batch_id: Optional[Any] = None,
        chunks: Optional[int] = None,
    ) -> None:
        if not isinstance(record, dict):
            return
        if bool(record.get("_accelerated_study_counted")):
            return
        record["_accelerated_study_counted"] = True
        try:
            append_stage_urls(
                stage="study",
                urls=[
                    {
                        "url": source_url,
                        "db_id": str(learn_list_id),
                    }
                ],
                job_id=job_id,
                db_name=db_name,
            )
        except Exception:
            pass
        async with aggregate_lock:
            aggregate_stats["study_count"] = int(aggregate_stats.get("study_count", 0) or 0) + 1
            aggregate_stats["study_done_count"] = int(aggregate_stats.get("study_done_count", 0) or 0) + 1
            aggregate_stats["study_success_count"] = int(aggregate_stats.get("study_success_count", 0) or 0) + 1
            study_count_now = int(aggregate_stats.get("study_count", 0) or 0)
            save_count_now = int(aggregate_stats.get("save_count", 0) or 0)
        record["study_counted_on_status_y"] = True
        _log_accelerated_parse_save(
            "study_count_incremented",
            job_id=job_id,
            db_name=db_name,
            learn_list_id=learn_list_id,
            batch_id=batch_id,
            chunks=chunks,
            study_count=study_count_now,
            save_count=save_count_now,
            reason="status_y_on_submit",
            url=source_url,
        )
        await _update_crawling_log_if_due(reason="status_y_on_submit", force=True)

    async def _run_accelerated_postprocess(record: Dict[str, Any], worker_id: int) -> None:
        nonlocal embedding_batch_submitted_count, embedding_batch_failed_count, embedding_batch_skipped_count
        learn_list_id_raw = (record or {}).get("learn_list_id")
        try:
            learn_list_id = int(learn_list_id_raw or 0)
        except Exception:
            learn_list_id = 0
        result = _build_accelerated_learning_result(record)
        source_url = str(result.get("source") or "").strip()
        normalized_text = str(result.get("content") or "").strip()
        if learn_list_id <= 0 or not source_url:
            embedding_batch_skipped_count += 1
            return

        batch_submitted = False
        summary_dispatched_by_embedding = False
        if embedding_batch_enabled:
            started_at = time.perf_counter()
            try:
                from backend.shared.batch_embedding_scheduler import (
                    is_batch_embedding_scheduler_enabled,
                    submit_crawled_url_embedding_batch,
                )

                if not is_batch_embedding_scheduler_enabled(result.get("content_type")):
                    embedding_batch_skipped_count += 1
                    record["embedding_batch_status"] = "disabled"
                    _log_accelerated_parse_save(
                        "embedding_batch_skipped",
                        job_id=job_id,
                        db_name=db_name,
                        learn_list_id=learn_list_id,
                        reason="scheduler_disabled",
                        url=source_url,
                    )
                else:
                    table_name = await _ensure_accelerated_pg_table_name()
                    if not table_name:
                        raise RuntimeError("pg_table_name_unresolved")
                    context_obj = SimpleNamespace(
                        table_name=table_name,
                        dbname=db_name,
                        job_id=job_id,
                        job_manager=None,
                        job_progress=None,
                        chat_bot_id=chat_bot_id,
                        crawl_mode=data.get("crawl_mode"),
                        url_filter=data.get("url_filter"),
                        content_type=result.get("content_type"),
                        contents=data.get("contents"),
                        subjects=data.get("subjects"),
                        memo=str((record.get("post_info") or {}).get("memo1") or memo or ""),
                        cate1=result.get("cate1") or "",
                        cate2=result.get("cate2") or "",
                        craw_id=data.get("id") or data.get("craw_id") or None,
                        unique_id=data.get("unique_id"),
                        server_domain=data.get("server_domain"),
                    )
                    submit_result = await submit_crawled_url_embedding_batch(
                        result=result,
                        subject=str(result.get("title") or source_url),
                        memo=str((record.get("post_info") or {}).get("memo1") or memo or ""),
                        context=context_obj,
                        learn_list_id=learn_list_id,
                        display_name=str(result.get("title") or source_url),
                        post_reg_date=result.get("reg_date"),
                        preserve_created_at=False,
                    )
                    batch_submitted = str((submit_result or {}).get("status") or "") == "submitted"
                    embedding_batch_submitted_count += 1
                    record["embedding_batch_status"] = submit_result.get("status")
                    record["embedding_batch_id"] = submit_result.get("batch_id")
                    record["embedding_chunks"] = int(submit_result.get("chunks") or 0)
                    record["embedding_status_y_on_submit"] = bool(submit_result.get("status_y_on_submit"))
                    summary_dispatched_by_embedding = bool(
                        submit_result.get("summary_dispatched_on_submit")
                    )
                    if bool(submit_result.get("status_y_on_submit")):
                        await _mark_accelerated_study_status_y(
                            record=record,
                            learn_list_id=learn_list_id,
                            source_url=source_url,
                            batch_id=submit_result.get("batch_id"),
                            chunks=int(submit_result.get("chunks") or 0),
                        )
                    _log_accelerated_parse_save(
                        "embedding_batch_submit_done",
                        job_id=job_id,
                        db_name=db_name,
                        learn_list_id=learn_list_id,
                        batch_id=submit_result.get("batch_id"),
                        chunks=int(submit_result.get("chunks") or 0),
                        status_y_on_submit=bool(submit_result.get("status_y_on_submit")),
                        elapsed_ms=int((time.perf_counter() - started_at) * 1000),
                        worker_id=worker_id,
                        url=source_url,
                    )
            except Exception as exc:
                embedding_batch_failed_count += 1
                record["embedding_batch_status"] = "failed"
                record["embedding_batch_error"] = str(exc)
                _log_accelerated_parse_save(
                    "embedding_batch_submit_error",
                    job_id=job_id,
                    db_name=db_name,
                    learn_list_id=learn_list_id,
                    error=str(exc),
                    elapsed_ms=int((time.perf_counter() - started_at) * 1000),
                    worker_id=worker_id,
                    url=source_url,
                )
        else:
            embedding_batch_skipped_count += 1

        if summary_submit_enabled and not summary_dispatched_by_embedding:
            await _submit_accelerated_summary(
                record=record,
                result=result,
                learn_list_id=learn_list_id,
                normalized_text=normalized_text,
            )

    async def _postprocess_worker(worker_id: int) -> None:
        while True:
            record = await postprocess_queue.get()
            try:
                if record is None:
                    return
                try:
                    await _run_accelerated_postprocess(record, worker_id)
                except Exception as exc:
                    _log_accelerated_parse_save(
                        "postprocess_worker_error",
                        job_id=job_id,
                        db_name=db_name,
                        chat_bot_id=chat_bot_id,
                        worker_id=worker_id,
                        error=str(exc),
                        url=str((record or {}).get("url") or "")[:240] if isinstance(record, dict) else "",
                    )
            finally:
                postprocess_queue.task_done()

    await _publish(
        "accelerated_parse_started",
        {
            "learn_list_duplicate_exclude_result": {**dedupe_meta, "warm_meta": warm_meta},
            "start_urls_sample": selected_urls[:10],
        },
    )
    _log_accelerated_parse_save(
        "started",
        job_id=job_id,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        scan_count=int(scan_count or 0),
        collection_count=0,
        duplicate_count=int((dedupe_meta or {}).get("duplicates") or 0),
        learn_list_loaded=int((warm_meta or {}).get("loaded") or 0),
        dedupe_lookup=(dedupe_meta or {}).get("lookup"),
        dedupe_table=(dedupe_meta or {}).get("table"),
        duplicate_samples=(dedupe_meta or {}).get("duplicate_samples") or [],
        concurrency=parse_concurrency,
        parse_timeout_sec=parse_timeout_sec,
        progress_publish_interval_sec=progress_publish_interval_sec,
        progress_publish_every=progress_publish_every,
        crawling_log_update_interval_sec=crawling_log_update_interval_sec,
        mariadb_json_batch_size=mariadb_json_batch_size,
        mariadb_json_flush_interval_sec=mariadb_json_flush_interval_sec,
        mariadb_insert_enabled=mariadb_insert_enabled,
        embedding_batch_enabled=embedding_batch_enabled,
        summary_submit_enabled=summary_submit_enabled,
        postprocess_concurrency=postprocess_concurrency,
        category_rule_loaded=category_rule_loaded,
        category_rule_count=category_rule_count,
        category_url_hint_count=len(category_url_to_cate_map),
        mariadb_jsonl=mariadb_paths.get("jsonl"),
        mariadb_json=mariadb_paths.get("json"),
    )
    await _update_crawling_log_if_due(reason="parse_started", status="running", force=True)

    queue: asyncio.Queue = asyncio.Queue()
    for idx, item in enumerate(selected_urls or [], start=1):
        queue.put_nowait((idx, item))

    mariadb_writer_task = asyncio.create_task(_mariadb_json_writer(), name=f"accelerated-json-writer:{job_id}")
    postprocess_tasks = [
        asyncio.create_task(_postprocess_worker(worker_id), name=f"accelerated-postprocess:{job_id}:{worker_id}")
        for worker_id in range(1, postprocess_concurrency + 1)
    ]

    async def _parse_worker(worker_id: int) -> None:
        workflow = _new_parse_workflow()
        try:
            while True:
                try:
                    idx, item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await _parse_one(workflow, worker_id, idx, item)
                finally:
                    queue.task_done()
        finally:
            try:
                cleanup = getattr(workflow, "_cleanup_stop_resources", None)
                if cleanup:
                    await cleanup()
            except Exception:
                pass

    async def _parse_one(workflow: BoardContentWorkflow, worker_id: int, idx: int, item: Any) -> None:
        if _is_job_terminal(job_id):
            _log_accelerated_parse_save(
                "parse_stopped_terminal",
                job_id=job_id,
                db_name=db_name,
                index=idx,
                total=total_selected,
                worker_id=worker_id,
            )
            return
        raw_url = _accelerated_item_url(item)
        if not raw_url:
            _log_accelerated_parse_save(
                "parse_skip_empty_url",
                job_id=job_id,
                db_name=db_name,
                index=idx,
                total=total_selected,
                worker_id=worker_id,
            )
            return
        item_type = str((item or {}).get("type") or "post") if isinstance(item, dict) else "post"
        item_cate_match = _accelerated_item_category_hint(item)
        item_title = str((item or {}).get("title") or (item or {}).get("subject") or "") if isinstance(item, dict) else ""
        disable_playwright = (
            bool((item or {}).get("disable_playwright")) if isinstance(item, dict) else False
        ) or _accelerated_parse_playwright_disabled()
        parse_started_at = time.perf_counter()
        _log_accelerated_parse_save(
            "parse_begin",
            job_id=job_id,
            db_name=db_name,
            index=idx,
            total=total_selected,
            url=raw_url,
            type=item_type,
            cate_match=item_cate_match,
            cate_applied=bool(item_cate_match),
            title_hint=item_title,
            disable_playwright=disable_playwright,
            worker_id=worker_id,
            timeout_sec=parse_timeout_sec,
        )
        before_done = int(workflow.stats.get("save_done_count", 0) or 0)
        before_failed = int(workflow.stats.get("save_failed_count", 0) or 0)
        try:
            workflow.stats.pop("last_parsed_url", None)
            workflow.stats.pop("last_parsed_title", None)
            workflow.stats.pop("last_parsed_content_size", None)
        except Exception:
            pass
        detail = _DetailItem(
            url=raw_url,
            board_url=str((item or {}).get("board_url") or "") if isinstance(item, dict) else "",
            type=item_type,
            cate_match=item_cate_match,
            reg_date_str=str((item or {}).get("reg_date") or "") if isinstance(item, dict) else "",
            subject=str((item or {}).get("subject") or "") if isinstance(item, dict) else "",
            title=str((item or {}).get("title") or "") if isinstance(item, dict) else "",
            disable_playwright=disable_playwright,
        )
        before_save = int(workflow.stats.get("save_count", 0) or 0)
        parse_exception = None
        try:
            await asyncio.wait_for(workflow._process_one_detail(detail), timeout=parse_timeout_sec)
        except asyncio.TimeoutError:
            parse_exception = f"parse_timeout_after_{parse_timeout_sec:g}s"
            key = canonicalize_url_for_dedup(raw_url) or raw_url
            await workflow._mark_save_done(url=key, ok=False)
            async with aggregate_lock:
                aggregate_save_count = int(aggregate_stats.get("save_count", 0) or 0)
                aggregate_done_count = int(aggregate_stats.get("save_done_count", 0) or 0)
                aggregate_failed_count = int(aggregate_stats.get("save_failed_count", 0) or 0)
            _log_accelerated_parse_save(
                "parse_timeout",
                job_id=job_id,
                db_name=db_name,
                index=idx,
                total=total_selected,
                url=raw_url,
                elapsed_ms=int((time.perf_counter() - parse_started_at) * 1000),
                timeout_sec=parse_timeout_sec,
                parse_local_save_count=int(workflow.stats.get("save_count", 0) or 0),
                parse_local_done_count=int(workflow.stats.get("save_done_count", 0) or 0),
                parse_local_failed_count=int(workflow.stats.get("save_failed_count", 0) or 0),
                save_count=aggregate_save_count,
                save_done_count=aggregate_done_count,
                save_failed_count=aggregate_failed_count,
                save_count_source="mariadb_insert",
                worker_id=worker_id,
            )
        except Exception as exc:
            parse_exception = str(exc)
            key = canonicalize_url_for_dedup(raw_url) or raw_url
            await workflow._mark_save_done(url=key, ok=False)
            async with aggregate_lock:
                aggregate_save_count = int(aggregate_stats.get("save_count", 0) or 0)
                aggregate_done_count = int(aggregate_stats.get("save_done_count", 0) or 0)
                aggregate_failed_count = int(aggregate_stats.get("save_failed_count", 0) or 0)
            _log_accelerated_parse_save(
                "parse_error",
                job_id=job_id,
                db_name=db_name,
                index=idx,
                total=total_selected,
                url=raw_url,
                error=str(exc),
                elapsed_ms=int((time.perf_counter() - parse_started_at) * 1000),
                parse_local_save_count=int(workflow.stats.get("save_count", 0) or 0),
                parse_local_done_count=int(workflow.stats.get("save_done_count", 0) or 0),
                parse_local_failed_count=int(workflow.stats.get("save_failed_count", 0) or 0),
                save_count=aggregate_save_count,
                save_done_count=aggregate_done_count,
                save_failed_count=aggregate_failed_count,
                save_count_source="mariadb_insert",
                worker_id=worker_id,
            )
            logger.warning(
                "[AcceleratedCrawl] parse failed | job_id=%s index=%s/%s url=%s err=%s",
                job_id,
                idx,
                total_selected,
                raw_url[:180],
                exc,
            )
        after_save = int(workflow.stats.get("save_count", 0) or 0)
        after_done = int(workflow.stats.get("save_done_count", 0) or 0)
        after_failed = int(workflow.stats.get("save_failed_count", 0) or 0)
        if after_done == before_done and after_save <= before_save:
            key = canonicalize_url_for_dedup(raw_url) or raw_url
            await workflow._mark_save_done(url=key, ok=False)
            after_done = int(workflow.stats.get("save_done_count", 0) or 0)
            after_failed = int(workflow.stats.get("save_failed_count", 0) or 0)
            if not parse_exception:
                parse_exception = "fast_static_fetch_failed_or_empty"
        ok = after_save > before_save
        counted_failed = after_failed > before_failed
        skipped = after_done == before_done
        elapsed_ms = int((time.perf_counter() - parse_started_at) * 1000)
        failed_record_count = 0
        if not ok:
            failed_record = _json_safe_value(
                {
                    "job_id": job_id,
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "index": idx,
                    "total": total_selected,
                    "url": raw_url,
                    "reason": parse_exception or "parse_failed_without_save",
                    "elapsed_ms": elapsed_ms,
                    "worker_id": worker_id,
                    "disable_playwright": bool(disable_playwright),
                    "fast_mode": True,
                    "retry_hint": "retry_with_normal_crawl_or_playwright",
                    "failed_at": datetime.now().isoformat(),
                }
            )
            async with mariadb_record_lock:
                failed_records.append(failed_record)
                failed_record_count = len(failed_records)
        async with aggregate_lock:
            aggregate_save_count = int(aggregate_stats.get("save_count", 0) or 0)
            aggregate_done_count = int(aggregate_stats.get("save_done_count", 0) or 0)
            aggregate_failed_count = int(aggregate_stats.get("save_failed_count", 0) or 0)
        _log_accelerated_parse_save(
            "parse_done",
            job_id=job_id,
            db_name=db_name,
            index=idx,
            total=total_selected,
            url=raw_url,
            ok=ok,
            skipped=skipped,
            result=(
                "parsed_and_queued_for_mariadb"
                if ok
                else "parse_failed"
                if counted_failed
                else "no_save_count_increment"
            ),
            error=parse_exception,
            elapsed_ms=elapsed_ms,
            parse_local_save_count_before=before_save,
            parse_local_save_count=after_save,
            parse_local_done_count=after_done,
            parse_local_failed_count=after_failed,
            save_count=aggregate_save_count,
            save_done_count=aggregate_done_count,
            save_failed_count=aggregate_failed_count,
            failed_record_count=failed_record_count,
            save_count_source="mariadb_insert",
            cate_match=item_cate_match,
            cate_applied=bool(item_cate_match),
            title=workflow.stats.get("last_parsed_title"),
            content_size=workflow.stats.get("last_parsed_content_size"),
            worker_id=worker_id,
        )
        if ok:
            post_info = dict(workflow.stats.get("last_post_info") or {})
            if post_info:
                mariadb_record = _build_accelerated_mariadb_record(
                    job_id=job_id,
                    db_name=db_name,
                    chat_bot_id=chat_bot_id,
                    index=idx,
                    total=total_selected,
                    url=raw_url,
                    post_info=post_info,
                    display=dict(workflow.stats.get("last_display") or {}),
                    learning_result=dict(workflow.stats.get("last_learning_result") or {}),
                    elapsed_ms=elapsed_ms,
                )
                async with mariadb_record_lock:
                    mariadb_records.append(mariadb_record)
                    mariadb_record_count = len(mariadb_records)
                mariadb_write_queue.put_nowait(mariadb_record)
                _log_accelerated_parse_save(
                    "mariadb_json_queued",
                    job_id=job_id,
                    db_name=db_name,
                    index=idx,
                    total=total_selected,
                    url=raw_url,
                    worker_id=worker_id,
                    jsonl=mariadb_paths["jsonl"],
                    records=mariadb_record_count,
                    queue_size=mariadb_write_queue.qsize(),
                    cate1=(mariadb_record.get("post_info") or {}).get("cate1"),
                    cate2=(mariadb_record.get("post_info") or {}).get("cate2"),
                    subject=(mariadb_record.get("learn_list_row_preview") or {}).get("subject"),
                    size=(mariadb_record.get("learn_list_row_preview") or {}).get("size"),
                )
        async with aggregate_lock:
            aggregate_stats["parsed_count"] = int(aggregate_stats.get("parsed_count", 0) or 0) + max(0, after_done - before_done)
            aggregate_stats["save_done_count"] = int(aggregate_stats.get("save_done_count", 0) or 0) + max(0, after_done - before_done)
            aggregate_stats["save_failed_count"] = int(aggregate_stats.get("save_failed_count", 0) or 0) + max(0, after_failed - before_failed)
            aggregate_stats["failed_record_count"] = len(failed_records)
            aggregate_stats["failed_records_sample"] = list(failed_records[-20:])
            aggregate_stats["last_parsed_url"] = raw_url
            aggregate_stats["last_parsed_title"] = workflow.stats.get("last_parsed_title")
            aggregate_stats["last_parsed_content_size"] = workflow.stats.get("last_parsed_content_size")
            aggregate_save_count = int(aggregate_stats.get("save_count", 0) or 0)
            aggregate_done_count = int(aggregate_stats.get("parsed_count", 0) or 0)
        await _publish_progress_if_due(
            {
                "current_index": idx,
                "parsed": aggregate_done_count,
                "last_url": raw_url,
                "last_parse_ok": after_save > before_save,
                "concurrency": parse_concurrency,
                "parse_timeout_sec": parse_timeout_sec,
                "worker_id": worker_id,
                "save_count": aggregate_save_count,
                "save_done_count": int(aggregate_stats.get("save_done_count", 0) or 0),
                "save_failed_count": int(aggregate_stats.get("save_failed_count", 0) or 0),
            }
        )

    workers = [asyncio.create_task(_parse_worker(worker_id)) for worker_id in range(1, parse_concurrency + 1)]
    try:
        await asyncio.gather(*workers)
    except Exception as exc:
        _log_accelerated_parse_save(
            "worker_pool_error",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            error=str(exc),
            concurrency=parse_concurrency,
            parse_timeout_sec=parse_timeout_sec,
        )
        raise

    await mariadb_write_queue.put(None)
    try:
        await mariadb_writer_task
    except Exception as exc:
        _log_accelerated_parse_save(
            "mariadb_json_writer_error",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            error=str(exc),
            queued_records=len(mariadb_records),
        )
        for _ in postprocess_tasks:
            postprocess_queue.put_nowait(None)
        await asyncio.gather(*postprocess_tasks, return_exceptions=True)
        raise
    await postprocess_queue.join()
    for _ in postprocess_tasks:
        postprocess_queue.put_nowait(None)
    await asyncio.gather(*postprocess_tasks, return_exceptions=True)

    async with aggregate_lock:
        final_stats = dict(aggregate_stats)
    async with mariadb_record_lock:
        final_mariadb_payload = {
            "job_id": job_id,
            "db_name": db_name,
            "chat_bot_id": chat_bot_id,
            "mariadb_target": "LEARN_LIST",
            "mariadb_operation": "insert_board_post_into_learn_list",
            "record_count": len(mariadb_records),
            "failed_record_count": len(failed_records),
            "jsonl_written_count": mariadb_written_count,
            "jsonl_write_errors": mariadb_write_errors,
            "mariadb_insert_enabled": mariadb_insert_enabled,
            "mariadb_insert_success_count": mariadb_insert_success_count,
            "mariadb_insert_failed_count": mariadb_insert_failed_count,
            "embedding_batch_enabled": embedding_batch_enabled,
            "embedding_batch_submitted_count": embedding_batch_submitted_count,
            "embedding_batch_failed_count": embedding_batch_failed_count,
            "embedding_batch_skipped_count": embedding_batch_skipped_count,
            "summary_submit_enabled": summary_submit_enabled,
            "summary_submit_count": summary_submit_count,
            "summary_failed_count": summary_failed_count,
            "jsonl_path": mariadb_paths["jsonl"],
            "records": list(mariadb_records),
            "failed_records": list(failed_records),
        }
        await asyncio.to_thread(_write_json_file, mariadb_paths["json"], final_mariadb_payload)
    final_payload = {
        "status": "completed",
        "event": "accelerated_parse_completed",
        "source": "accelerated_crawl",
        "scan_count": int(redis_scan_count or 0),
        "total_count": int(redis_scan_count or 0),
        "actual_scan_count": int(scan_count or 0),
        "collection_count": int(final_stats.get("parsed_count", 0) or 0),
        "save_count": int(final_stats.get("save_count", 0) or 0),
        "save_done_count": int(final_stats.get("save_done_count", 0) or 0),
        "save_failed_count": int(final_stats.get("save_failed_count", 0) or 0),
        "parsed_count": int(final_stats.get("parsed_count", 0) or 0),
        "study_count": int(final_stats.get("study_count", 0) or 0),
        "study_done_count": int(final_stats.get("study_done_count", 0) or 0),
        "study_success_count": int(final_stats.get("study_success_count", 0) or 0),
        "study_failed_count": int(final_stats.get("study_failed_count", 0) or 0),
        "learn_list_duplicate_exclude_result": {**dedupe_meta, "warm_meta": warm_meta},
        "concurrency": parse_concurrency,
        "parse_timeout_sec": parse_timeout_sec,
        "mariadb_json_path": mariadb_paths["json"],
        "mariadb_jsonl_path": mariadb_paths["jsonl"],
        "mariadb_json_record_count": len(mariadb_records),
        "failed_record_count": len(failed_records),
        "failed_records_sample": list(failed_records[-20:]),
        "mariadb_jsonl_written_count": mariadb_written_count,
        "mariadb_jsonl_write_errors": mariadb_write_errors,
        "mariadb_insert_enabled": mariadb_insert_enabled,
        "mariadb_insert_success_count": mariadb_insert_success_count,
        "mariadb_insert_failed_count": mariadb_insert_failed_count,
        "embedding_batch_enabled": embedding_batch_enabled,
        "embedding_batch_submitted_count": embedding_batch_submitted_count,
        "embedding_batch_failed_count": embedding_batch_failed_count,
        "embedding_batch_skipped_count": embedding_batch_skipped_count,
        "summary_submit_enabled": summary_submit_enabled,
        "summary_submit_count": summary_submit_count,
        "summary_failed_count": summary_failed_count,
        "category_rule_loaded": category_rule_loaded,
        "category_rule_count": category_rule_count,
        "category_url_hint_count": len(category_url_to_cate_map),
        "h3": "crawl status",
        "message": "crawl status",
    }
    _set_accelerated_memory_state(job_id, final_payload)
    if redis_state_enabled:
        await update_state_only(job_id=job_id, account_name=db_name, payload=final_payload)
    if sse_publish_enabled:
        await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=final_payload)
    await _update_crawling_log_if_due(reason="parse_completed", status="completed", force=True)
    _log_accelerated_parse_save(
        "completed",
        job_id=job_id,
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        scan_count=int(scan_count or 0),
        collection_count=int(final_stats.get("parsed_count", 0) or 0),
        save_count=int(final_stats.get("save_count", 0) or 0),
        save_done_count=int(final_stats.get("save_done_count", 0) or 0),
        save_failed_count=int(final_stats.get("save_failed_count", 0) or 0),
        study_count=int(final_stats.get("study_count", 0) or 0),
        study_done_count=int(final_stats.get("study_done_count", 0) or 0),
        study_success_count=int(final_stats.get("study_success_count", 0) or 0),
        study_failed_count=int(final_stats.get("study_failed_count", 0) or 0),
        concurrency=parse_concurrency,
        parse_timeout_sec=parse_timeout_sec,
        mariadb_json=mariadb_paths["json"],
        mariadb_jsonl=mariadb_paths["jsonl"],
        mariadb_json_record_count=len(mariadb_records),
        mariadb_jsonl_written_count=mariadb_written_count,
        mariadb_jsonl_write_errors=mariadb_write_errors,
        mariadb_insert_enabled=mariadb_insert_enabled,
        mariadb_insert_success_count=mariadb_insert_success_count,
        mariadb_insert_failed_count=mariadb_insert_failed_count,
        embedding_batch_enabled=embedding_batch_enabled,
        embedding_batch_submitted_count=embedding_batch_submitted_count,
        embedding_batch_failed_count=embedding_batch_failed_count,
        embedding_batch_skipped_count=embedding_batch_skipped_count,
        summary_submit_enabled=summary_submit_enabled,
        summary_submit_count=summary_submit_count,
        summary_failed_count=summary_failed_count,
        category_rule_loaded=category_rule_loaded,
        category_rule_count=category_rule_count,
        category_url_hint_count=len(category_url_to_cate_map),
    )
    crawler_state.record_history(job_id, "completed", "accelerated_parse_completed", db_name, chat_bot_id=chat_bot_id)
    return final_payload

# ==========================================
# 1. LEARN_LIST URL 중복 그룹 조회 API (중복 URL을 화면에서 확인/정리하기 위한 보조 엔드포인트)
# ==========================================
@router.post("/backend/learn-list/url-duplicates")
async def learn_list_url_duplicates(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        db_name = resolve_db_name(body, default="dev_user") or "dev_user"
        chat_bot_id = str(
            body.get("chat_bot_id")
            or (body.get("metadata") or {}).get("chat_bot_id")
            or ""
        ).strip()
        result = await load_learn_list_url_duplicate_groups(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            limit=body.get("limit"),
            created_at_start=body.get("created_at_start") or body.get("createdAtStart"),
            created_at_end=body.get("created_at_end") or body.get("createdAtEnd"),
        )
        return JSONResponse(jsonable_encoder(result), status_code=200)
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[LearnListDuplicateGroups] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/learn-list/metadata-postprocess")
async def learn_list_metadata_postprocess(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        result = await run_duplicate_learning_metadata_postprocess(body)
        return JSONResponse(jsonable_encoder(result), status_code=200)
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[LearnListMetadataPostprocess] endpoint failed: %s", exc)
        return JSONResponse(
            {"status": "error", "message": str(exc), "source": "duplicate_learning_metadata_postprocess"},
            status_code=500,
        )


@router.post("/backend/learn-list/metadata-postprocess/stop")
async def learn_list_metadata_postprocess_stop(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        job_id = str(body.get("job_id") or body.get("jobId") or "").strip()
        if not job_id:
            return JSONResponse({"status": "error", "message": "job_id is required"}, status_code=400)
        ok = request_duplicate_learning_metadata_postprocess_stop(job_id)
        return JSONResponse({"status": "stop", "job_id": job_id, "stop_requested": ok}, status_code=200)
    except Exception as exc:
        logger.exception("[LearnListMetadataPostprocessStop] endpoint failed: %s", exc)
        return JSONResponse(
            {"status": "error", "message": str(exc), "source": "duplicate_learning_metadata_postprocess_stop"},
            status_code=500,
        )


@router.post("/backend/learn-list/title-candidates")
async def learn_list_title_candidates(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        result = queue_title_candidate_preview(body)
        return JSONResponse(jsonable_encoder(result), status_code=200)
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[TitleCandidates] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/learn-list/title-candidates/status")
async def learn_list_title_candidates_status(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        result = get_title_candidate_preview_status(body)
        return JSONResponse(jsonable_encoder(result), status_code=200)
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[TitleCandidatesStatus] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/learn-list/title-candidates/stop")
async def learn_list_title_candidates_stop(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        result = await request_title_candidate_preview_stop(body)
        return JSONResponse(jsonable_encoder(result), status_code=200)
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[TitleCandidatesStop] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/learn-list/title-candidates/apply")
async def learn_list_title_candidates_apply(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        result = await apply_title_candidate_preview(body)
        return JSONResponse(jsonable_encoder(result), status_code=200)
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[TitleCandidatesApply] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/file-category/preview")
async def file_category_preview(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        db_name = resolve_db_name(body, default="dev_user") or "dev_user"
        chat_bot_id = str(
            body.get("chat_bot_id")
            or (body.get("metadata") or {}).get("chat_bot_id")
            or ""
        ).strip()
        if not chat_bot_id:
            return JSONResponse({"status": "error", "message": "chat_bot_id is required"}, status_code=400)
        plan = await preview_file_category_apply_plan(chat_bot_id=chat_bot_id, db_name=db_name)
        rows: List[Dict[str, Any]] = []
        for item in list(plan.get("plan_items") or []):
            if not isinstance(item, dict):
                continue
            source_cate1 = dict(item.get("source_cate1") or {})
            target_cate1 = dict(item.get("target_cate1") or {})
            for cate2_plan in list(item.get("cate2_plans") or []):
                if not isinstance(cate2_plan, dict):
                    continue
                source_cate2 = dict(cate2_plan.get("source_cate2") or {})
                target_cate2 = dict(cate2_plan.get("target_cate2") or {})
                rows.append(
                    {
                        "source_cate1_code": str(source_cate1.get("cate_code") or ""),
                        "source_cate1_name": str(source_cate1.get("cate_name") or ""),
                        "source_cate2_code": str(source_cate2.get("cate_code") or ""),
                        "source_cate2_name": str(source_cate2.get("cate_name") or ""),
                        "target_cate1_code": str(target_cate1.get("cate_code") or ""),
                        "target_cate1_name": str(target_cate1.get("cate_name") or ""),
                        "target_cate2_code": str(target_cate2.get("cate_code") or ""),
                        "target_cate2_name": str(target_cate2.get("cate_name") or ""),
                        "target_exists": bool(cate2_plan.get("target_cate2_exists")),
                        "matched_file_rows": int(cate2_plan.get("matched_file_rows") or 0),
                    }
                )
        plan["preview_rows"] = rows
        plan["preview_row_count"] = len(rows)
        return JSONResponse(jsonable_encoder({"status": "success", "preview": plan}), status_code=200)
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[FileCategoryPreview] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


def _safe_pg_training_table_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    import re

    if not re.fullmatch(r"td_[a-z0-9_]+_training_data", text):
        return ""
    return text


def _pg_training_table_candidates_from_ids(*values: Any) -> List[str]:
    candidates: List[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        for ident in (text, text.replace("-", "")):
            ident = ident.strip().lower()
            if not ident:
                continue
            table = _safe_pg_training_table_name(f"td_{ident}_training_data")
            if table and table not in candidates:
                candidates.append(table)
    return candidates


def _parse_pg_content_metadata(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        text = value.strip()
        for _ in range(2):
            try:
                parsed = json.loads(text)
            except Exception:
                return {}
            if isinstance(parsed, dict):
                return dict(parsed)
            if isinstance(parsed, str) and parsed.strip():
                text = parsed.strip()
                continue
            return {}
    return {}


def _source_url_from_pg_content_metadata(metadata: Dict[str, Any]) -> str:
    if not isinstance(metadata, dict):
        return ""
    for key in (
        "source_url",
        "sourceUrl",
        "source",
        "attachment_download_url",
        "attachmentDownloadUrl",
        "download_url",
        "downloadUrl",
        "file_url",
        "fileUrl",
        "url",
    ):
        value = metadata.get(key)
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _pg_metadata_like_pattern(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return "%" + text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"

def _crawl_start_aux_pg_timeout_sec() -> float:
    try:
        return max(1.0, min(float(os.getenv("CRAWL_START_AUX_PG_TIMEOUT_SEC", "6") or "6"), 30.0))
    except Exception:
        return 6.0


async def _optional_pg_execute_query(label: str, query: str, params=None, *, fetch: bool = True, dbname: Optional[str] = None):

    timeout_sec = _crawl_start_aux_pg_timeout_sec()
    started = time.perf_counter()
    safe_query = " ".join(str(query or "").split())[:220]
    try:
        logger.debug(
            "[BottleneckTrace][pg_aux_start] label=%s db=%s fetch=%s timeout_sec=%.1f query=%s",
            label,
            dbname,
            bool(fetch),
            timeout_sec,
            safe_query,
        )
        result = await asyncio.wait_for(
            pg_execute_query(query, params, fetch=fetch, dbname=dbname),
            timeout=timeout_sec,
        )
        logger.debug(
            "[BottleneckTrace][pg_aux_done] label=%s db=%s elapsed_ms=%s rows=%s",
            label,
            dbname,
            int((time.perf_counter() - started) * 1000),
            len(result or []) if isinstance(result, list) else "-",
        )
        return result
    except asyncio.TimeoutError:
        logger.debug(
            "[BottleneckTrace][pg_aux_timeout] label=%s db=%s timeout_sec=%.1f elapsed_ms=%s action=fail_open",
            label,
            dbname,
            timeout_sec,
            int((time.perf_counter() - started) * 1000),
        )
        return [] if fetch else None
    except Exception as exc:
        logger.debug(
            "[BottleneckTrace][pg_aux_failed] label=%s db=%s elapsed_ms=%s err=%s action=fail_open",
            label,
            dbname,
            int((time.perf_counter() - started) * 1000),
            exc,
            exc_info=True,
        )
        return [] if fetch else None



async def _resolve_pg_training_table_for_file_dashboard(
    *,
    db_name: str,
    chat_bot_id: str,
    account_identifier: Any,
) -> str:

    chat_id = ""
    try:
        setup_rows = await _optional_pg_execute_query(
            "file_dashboard_chatbot_setup",
            "SELECT chat_id FROM chatbot_setup WHERE chat_bot_id = $1 LIMIT 1",
            (chat_bot_id,),
            fetch=True,
            dbname=db_name,
        )
        if setup_rows:
            setup_row = dict(setup_rows[0])
            chat_id = str(setup_row.get("chat_id") or "").strip()
    except Exception as exc:
        logger.info(
            "[LearnListFileRows] PostgreSQL chatbot_setup lookup skipped | db=%s chat_bot_id=%s err=%s",
            db_name,
            chat_bot_id,
            exc,
        )

    candidates = _pg_training_table_candidates_from_ids(
        chat_id,
        account_identifier,
        chat_bot_id,
    )
    if not candidates:
        return ""

    placeholders = ", ".join(f"${idx}" for idx in range(1, len(candidates) + 1))
    rows = await _optional_pg_execute_query(
        "file_dashboard_training_table_exists",
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN ({placeholders})
        """,
        tuple(candidates),
        fetch=True,
        dbname=db_name,
    )
    existing = {
        str(dict(row).get("table_name") or "").strip().lower()
        for row in rows or []
        if row and dict(row).get("table_name")
    }
    return next((candidate for candidate in candidates if candidate in existing), "")


async def _load_pg_text_data_for_file_rows(
    *,
    db_name: str,
    chat_bot_id: str,
    account_identifier: Any,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not rows:
        return {"training_table": "", "matched_rows": 0, "reason": "no_rows"}


    training_table = await _resolve_pg_training_table_for_file_dashboard(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        account_identifier=account_identifier,
    )
    if not training_table:
        return {"training_table": "", "matched_rows": 0, "reason": "training_table_not_found"}

    column_rows = await _optional_pg_execute_query(
        "file_dashboard_training_columns",
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = $1
        """,
        (training_table,),
        fetch=True,
        dbname=db_name,
    )
    columns = {
        str(dict(row).get("column_name") or "").strip()
        for row in column_rows or []
        if row and dict(row).get("column_name")
    }
    has_text_data = "text_data" in columns
    has_content_metadata = "content_metadata" in columns
    if not has_text_data and not has_content_metadata:
        return {
            "training_table": training_table,
            "matched_rows": 0,
            "source_url_matched_rows": 0,
            "reason": "text_data_content_metadata_columns_not_found",
        }

    key_to_row_ids: Dict[Tuple[str, str], List[str]] = {}
    for row in rows:
        row_id = str(row.get("id") or "").strip()
        if not row_id:
            continue
        content_type = str(row.get("content_type") or row.get("type") or "").strip().lower()
        if content_type in {"image", "text"}:
            for candidate in [str(row.get("subject") or "").strip()]:
                if candidate and "subject" in columns:
                    key_to_row_ids.setdefault(("subject", candidate), []).append(row_id)
        else:
            for candidate in _learn_list_url_match_candidates(row.get("content")):
                if candidate and "content" in columns:
                    key_to_row_ids.setdefault(("content", candidate), []).append(row_id)
                if candidate and has_content_metadata:
                    key_to_row_ids.setdefault(("metadata_source_url", candidate), []).append(row_id)
            subject_candidate = str(row.get("subject") or "").strip()
            if subject_candidate and "subject" in columns:
                key_to_row_ids.setdefault(("subject", subject_candidate), []).append(row_id)

    content_values = sorted({value for (col, value) in key_to_row_ids if col == "content"})
    subject_values = sorted({value for (col, value) in key_to_row_ids if col == "subject"})
    metadata_source_values = sorted({value for (col, value) in key_to_row_ids if col == "metadata_source_url"})
    conditions: List[str] = []
    params: List[Any] = []
    if content_values and "content" in columns:
        params.append(content_values)
        conditions.append(f"content = ANY(${len(params)}::text[])")
    if subject_values and "subject" in columns:
        params.append(subject_values)
        conditions.append(f"subject = ANY(${len(params)}::text[])")
    if metadata_source_values and has_content_metadata:
        params.append(metadata_source_values)
        conditions.append(
            "("
            f"content_metadata::text LIKE ANY(${len(params)}::text[])"
            ")"
        )
        params[-1] = [_pg_metadata_like_pattern(value) for value in metadata_source_values if _pg_metadata_like_pattern(value)]
    if not conditions:
        return {"training_table": training_table, "matched_rows": 0, "reason": "lookup_column_not_found"}

    select_cols: List[str] = []
    if has_text_data:
        select_cols.append("text_data")
    if has_content_metadata:
        select_cols.append("content_metadata")
    if "content" in columns:
        select_cols.append("content")
    if "subject" in columns:
        select_cols.append("subject")
    if "id" in columns:
        select_cols.append("id")
    select_cols = list(dict.fromkeys(select_cols))
    text_where = "text_data IS NOT NULL" if has_text_data else "1=1"
    pg_rows = await _optional_pg_execute_query(
        "file_dashboard_training_text_lookup",
        f"""
        SELECT {", ".join(select_cols)}
        FROM public.{training_table}
        WHERE {text_where}
          AND ({" OR ".join(conditions)})
        ORDER BY id
        """,
        tuple(params),
        fetch=True,
        dbname=db_name,
    )
    text_by_row_id: Dict[str, List[str]] = {}
    source_url_by_row_id: Dict[str, str] = {}
    seen_by_row_id: Dict[str, set[str]] = {}
    for pg_row in pg_rows or []:
        if not pg_row:
            continue
        pg_row = dict(pg_row)
        text_data = str(pg_row.get("text_data") or "").strip()
        metadata = _parse_pg_content_metadata(pg_row.get("content_metadata"))
        pg_source_url = _source_url_from_pg_content_metadata(metadata)
        matched_ids: set[str] = set()
        for col in ("content", "subject"):
            value = str(pg_row.get(col) or "").strip()
            if not value:
                continue
            matched_ids.update(key_to_row_ids.get((col, value), []))
        for candidate in _learn_list_url_match_candidates(pg_source_url):
            matched_ids.update(key_to_row_ids.get(("metadata_source_url", candidate), []))
        for row_id in matched_ids:
            if text_data:
                seen = seen_by_row_id.setdefault(row_id, set())
                if text_data not in seen:
                    seen.add(text_data)
                    text_by_row_id.setdefault(row_id, []).append(text_data)
            if pg_source_url and not source_url_by_row_id.get(row_id):
                source_url_by_row_id[row_id] = pg_source_url

    matched_rows = 0
    source_url_matched_rows = 0
    for row in rows:
        row_id = str(row.get("id") or "").strip()
        texts = text_by_row_id.get(row_id) or []
        row["text_data"] = "\n".join(texts)
        if texts:
            matched_rows += 1
        pg_source_url = source_url_by_row_id.get(row_id, "")
        if pg_source_url:
            row["pg_source_url"] = pg_source_url
            row["source_url"] = pg_source_url
            source_url_matched_rows += 1

    return {
        "training_table": training_table,
        "matched_rows": matched_rows,
        "source_url_matched_rows": source_url_matched_rows,
        "reason": "ok",
    }


_RECONCILE_SQL_IDENT_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def _reconcile_safe_identifier(value: Any, *, label: str) -> str:
    text = str(value or "").strip()
    if not text or any(ch not in _RECONCILE_SQL_IDENT_CHARS for ch in text):
        raise ValueError(f"unsafe {label}: {text!r}")
    return text


def _reconcile_quote_maria_identifier(value: Any, *, label: str) -> str:
    return f"`{_reconcile_safe_identifier(value, label=label)}`"


def _reconcile_quote_pg_table(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("pg_table is required")
    parts = text.split(".") if "." in text else ["public", text]
    if len(parts) != 2:
        raise ValueError(f"unsafe pg_table: {text!r}")
    safe_parts = [_reconcile_safe_identifier(part, label="pg_table") for part in parts]
    return ".".join(f'"{part}"' for part in safe_parts)


def _reconcile_row_to_dict(row: Any) -> Dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def _reconcile_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


@router.post("/backend/learn-list/file-chunk-reconcile")
async def learn_list_file_chunk_reconcile(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        db_name = resolve_db_name(body, default="dev_user") or "dev_user"
        pg_db = str(body.get("pg_db") or db_name).strip() or db_name
        pg_table = str(body.get("pg_table") or "").strip()
        if not pg_table:
            return JSONResponse({"status": "error", "message": "pg_table is required"}, status_code=400)
        pg_table_sql = _reconcile_quote_pg_table(pg_table)

        chat_bot_id = str(
            body.get("chat_bot_id")
            or (body.get("metadata") or {}).get("chat_bot_id")
            or ""
        ).strip()
        learn_table = str(body.get("learn_table") or "ASADAL_CRAWLING_LEARN_LIST").strip()
        if chat_bot_id:
            resolved_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
            if resolved_table:
                learn_table = resolved_table
        learn_table = _reconcile_safe_identifier(learn_table, label="learn_table")
        learn_table_sql = _reconcile_quote_maria_identifier(learn_table, label="learn_table")

        try:
            batch_size = int(body.get("batch_size") or 100)
        except Exception:
            batch_size = 100
        batch_size = max(1, min(batch_size, 500))
        try:
            max_rows = int(body.get("max_rows") or 0)
        except Exception:
            max_rows = 0
        max_rows = max(0, max_rows)
        try:
            max_id = int(body.get("max_id") or 0)
        except Exception:
            max_id = 0
        max_id = max(0, max_id)
        try:
            after_id = int(body.get("after_id") or 0)
        except Exception:
            after_id = 0
        after_id = max(0, after_id)
        try:
            report_sample_limit = int(body.get("report_sample_limit") or 5000)
        except Exception:
            report_sample_limit = 5000
        report_sample_limit = max(1, min(report_sample_limit, 50000))
        include_existing_chunk = _reconcile_bool(body.get("include_existing_chunk"))
        apply_chunk_update = _reconcile_bool(body.get("apply_chunk_update"))

        cols = await ensure_learn_list_standard_columns(db_name, learn_table)
        if not cols:
            return JSONResponse(
                {"status": "error", "message": "learn_list table columns not found", "learn_table": learn_table},
                status_code=404,
            )
        required_cols = {"id", "content", "content_type", "chunk"}
        missing = sorted(required_cols - set(cols))
        if missing:
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list missing required columns",
                    "learn_table": learn_table,
                    "missing_columns": missing,
                },
                status_code=400,
            )

        select_candidates = [
            "id", "content", "subject", "content_type", "status", "size", "chunk",
            "cate1", "cate2", "memo1", "content_address", "created_at",
            "content_author", "content_updated_at", "content_created_at",
        ]
        select_cols = [col for col in select_candidates if col in cols]
        select_sql = ", ".join(f"`{col}`" for col in select_cols)

        from db.db_operations import execute_query as pg_execute_query
        from backend.shared.db_write_queue import run_db_write

        started = time.perf_counter()
        processed = 0
        batches = 0
        chunk_update_candidates: List[Dict[str, Any]] = []
        chunk_updated: List[Dict[str, Any]] = []
        relearn_candidates: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []
        chunk_update_candidate_count = 0
        chunk_updated_count = 0
        relearn_candidate_count = 0
        skipped_count = 0

        while True:
            remaining = batch_size
            if max_rows > 0:
                remaining = min(remaining, max(0, max_rows - processed))
                if remaining <= 0:
                    break
            conditions = [
                "`id` > %s",
                "LOWER(COALESCE(`content_type`, '')) = 'file'",
                "`content` IS NOT NULL",
                "TRIM(`content`) <> ''",
            ]
            params: List[Any] = [after_id]
            if max_id > 0:
                conditions.append("`id` <= %s")
                params.append(max_id)
            if not include_existing_chunk:
                conditions.append("(`chunk` IS NULL OR TRIM(CAST(`chunk` AS CHAR)) = '' OR CAST(`chunk` AS CHAR) = '0')")
            params.append(remaining)
            rows = await mysql_execute_query(
                f"""
                SELECT {select_sql}
                FROM {learn_table_sql}
                WHERE {' AND '.join(conditions)}
                ORDER BY `id` ASC
                LIMIT %s
                """,
                tuple(params),
                fetch=True,
                dbname=db_name,
            )
            rows_list = [_reconcile_row_to_dict(row) for row in (rows or [])]
            rows_list = [row for row in rows_list if row]
            if not rows_list:
                break

            batches += 1
            processed += len(rows_list)
            after_id = max(int(row.get("id") or after_id) for row in rows_list)
            urls = [str(row.get("content") or "").strip() for row in rows_list if str(row.get("content") or "").strip()]
            unique_urls = list(dict.fromkeys(urls))
            pg_counts: Dict[str, int] = {}
            if unique_urls:
                pg_rows = await pg_execute_query(
                    f"""
                    SELECT content, COUNT(*)::int AS chunk_count
                    FROM {pg_table_sql}
                    WHERE content = ANY($1::text[])
                    GROUP BY content
                    """,
                    (unique_urls,),
                    fetch=True,
                    dbname=pg_db,
                )
                for raw in pg_rows or []:
                    row = _reconcile_row_to_dict(raw)
                    content = str(row.get("content") or "").strip()
                    if not content:
                        continue
                    try:
                        count = int(row.get("chunk_count") or 0)
                    except Exception:
                        count = 0
                    pg_counts[content] = count

            for row in rows_list:
                try:
                    row_id = int(row.get("id") or 0)
                except Exception:
                    row_id = 0
                content = str(row.get("content") or "").strip()
                if not content:
                    skipped_count += 1
                    if len(skipped) < report_sample_limit:
                        skipped.append({"id": row_id, "reason": "blank_content"})
                    continue
                chunk_count = int(pg_counts.get(content) or 0)
                item = {
                    "id": row_id,
                    "content": content,
                    "subject": row.get("subject"),
                    "status": row.get("status"),
                    "maria_chunk": row.get("chunk"),
                    "pg_chunk_count": chunk_count,
                }
                if chunk_count > 0:
                    chunk_update_candidate_count += 1
                    if len(chunk_update_candidates) < report_sample_limit:
                        chunk_update_candidates.append(item)
                    if apply_chunk_update:
                        async def _update_chunk(row_id=row_id, chunk_count=chunk_count):
                            return await mysql_execute_query(
                                f"""
                                UPDATE {learn_table_sql}
                                SET `chunk` = %s
                                WHERE `id` = %s
                                  AND LOWER(COALESCE(`content_type`, '')) = 'file'
                                  AND (`chunk` IS NULL OR TRIM(CAST(`chunk` AS CHAR)) = '' OR CAST(`chunk` AS CHAR) = '0')
                                """,
                                (chunk_count, row_id),
                                fetch=False,
                                dbname=db_name,
                            )
                        await run_db_write("learn_list.file_chunk_reconcile", _update_chunk)
                        chunk_updated_count += 1
                        if len(chunk_updated) < report_sample_limit:
                            chunk_updated.append({**item, "updated": True})
                else:
                    relearn_candidate_count += 1
                    if len(relearn_candidates) < report_sample_limit:
                        relearn_candidates.append({**item, "reason": "pg_chunk_missing", "action": "skip_report"})

            if max_rows > 0 and processed >= max_rows:
                break

        elapsed = round(time.perf_counter() - started, 3)
        summary = {
            "chunk_update_candidates": chunk_update_candidate_count,
            "chunk_updated": chunk_updated_count,
            "relearn_candidates": relearn_candidate_count,
            "skipped": skipped_count,
        }
        return JSONResponse(
            jsonable_encoder(
                {
                    "status": "success",
                    "dry_run": not apply_chunk_update,
                    "db_name": db_name,
                    "pg_db": pg_db,
                    "pg_table": pg_table,
                    "learn_table": learn_table,
                    "processed": processed,
                    "batches": batches,
                    "last_id": after_id,
                    "elapsed_sec": elapsed,
                    "summary": summary,
                    "chunk_update_candidates": chunk_update_candidates,
                    "chunk_updated": chunk_updated,
                    "relearn_candidates": relearn_candidates,
                    "skipped": skipped,
                }
            ),
            status_code=200,
        )
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[LearnListFileChunkReconcile] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/learn-list/file-rows")
async def learn_list_file_rows(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        db_name = resolve_db_name(body, default="dev_user") or "dev_user"
        chat_bot_id = str(
            body.get("chat_bot_id")
            or (body.get("metadata") or {}).get("chat_bot_id")
            or ""
        ).strip()
        if not chat_bot_id:
            return JSONResponse({"status": "error", "message": "chat_bot_id is required"}, status_code=400)
        try:
            limit = int(body.get("limit") or 500)
        except Exception:
            limit = 500
        limit = max(1, min(limit, 5000))

        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        learn_list_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
        if not learn_list_table:
            learn_list_table = get_learn_list_table_name(account_identifier)
        cols = await ensure_learn_list_standard_columns(db_name, learn_list_table)
        if not cols:
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list table columns not found",
                    "table": learn_list_table,
                },
                status_code=404,
            )

        select_candidates = [
            "id",
            "subject",
            "content",
            "content_type",
            "type",
            "cate1",
            "cate2",
            "status",
            "size",
            "source_page",
            "created_at",
            "content_created_at",
        ]
        select_cols = [col for col in select_candidates if col in cols]
        if "id" not in select_cols:
            select_cols.insert(0, "id")
        if "content_type" in cols:
            where_sql = "LOWER(COALESCE(`content_type`, '')) = 'file'"
        elif "type" in cols:
            where_sql = "LOWER(COALESCE(`type`, '')) = 'file'"
        else:
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list has no content_type/type column",
                    "table": learn_list_table,
                    "columns": sorted(cols),
                },
                status_code=400,
            )
        order_sql = "`id` DESC" if "id" in cols else "1"
        rows = await mysql_execute_query(
            f"""
            SELECT {', '.join(f'`{col}`' for col in select_cols)}
            FROM `{learn_list_table}`
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT %s
            """,
            (limit,),
            fetch=True,
            dbname=db_name,
        )
        rows_list = list(rows or [])
        pg_text_data_meta = {}
        try:
            pg_text_data_meta = await _load_pg_text_data_for_file_rows(
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                account_identifier=account_identifier,
                rows=rows_list,
            )
        except Exception as exc:
            logger.exception("[LearnListFileRows] PostgreSQL text_data lookup failed: %s", exc)
            pg_text_data_meta = {"training_table": "", "matched_rows": 0, "reason": str(exc)}
            for row in rows_list:
                row["text_data"] = ""
        response_columns = list(select_cols)
        if "text_data" not in response_columns:
            response_columns.append("text_data")
        return JSONResponse(
            jsonable_encoder(
                {
                    "status": "success",
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "account_identifier": account_identifier,
                    "table": learn_list_table,
                    "columns": response_columns,
                    "pg_text_data": pg_text_data_meta,
                    "count": len(rows_list),
                    "limit": limit,
                    "rows": rows_list,
                }
            ),
            status_code=200,
        )
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[LearnListFileRows] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/learn-list/post-rows")
async def learn_list_post_rows(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        db_name = resolve_db_name(body, default="dev_user") or "dev_user"
        chat_bot_id = str(
            body.get("chat_bot_id")
            or (body.get("metadata") or {}).get("chat_bot_id")
            or ""
        ).strip()
        if not chat_bot_id:
            return JSONResponse({"status": "error", "message": "chat_bot_id is required"}, status_code=400)
        try:
            limit = int(body.get("limit") or 500)
        except Exception:
            limit = 500
        limit = max(1, min(limit, 5000))

        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        learn_list_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
        if not learn_list_table:
            learn_list_table = get_learn_list_table_name(account_identifier)
        cols = await ensure_learn_list_standard_columns(db_name, learn_list_table)
        if not cols:
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list table columns not found",
                    "table": learn_list_table,
                },
                status_code=404,
            )

        select_candidates = [
            "id",
            "subject",
            "content",
            "content_type",
            "type",
            "cate1",
            "cate2",
            "status",
            "source_page",
            "created_at",
            "content_created_at",
        ]
        select_cols = [col for col in select_candidates if col in cols]
        if "id" not in select_cols:
            select_cols.insert(0, "id")
        if "content" not in select_cols:
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list has no content column",
                    "table": learn_list_table,
                    "columns": sorted(cols),
                },
                status_code=400,
            )

        where_parts = ["`content` IS NOT NULL", "TRIM(CAST(`content` AS CHAR)) <> ''"]
        params: List[Any] = []
        if "type" in cols:
            where_parts.append("LOWER(COALESCE(`type`, '')) = 'post'")
        elif "content_type" in cols:
            where_parts.append("LOWER(COALESCE(`content_type`, '')) = 'url'")
        if body.get("status") not in (None, "") and "status" in cols:
            where_parts.append("UPPER(COALESCE(`status`, '')) = %s")
            params.append(str(body.get("status")).strip().upper())
        order_sql = "`id` DESC" if "id" in cols else "1"
        rows = await mysql_execute_query(
            f"""
            SELECT {', '.join(f'`{col}`' for col in select_cols)}
            FROM `{learn_list_table}`
            WHERE {' AND '.join(where_parts)}
            ORDER BY {order_sql}
            LIMIT %s
            """,
            tuple(params + [limit]),
            fetch=True,
            dbname=db_name,
        )
        rows_list = list(rows or [])
        return JSONResponse(
            jsonable_encoder(
                {
                    "status": "success",
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "account_identifier": account_identifier,
                    "table": learn_list_table,
                    "columns": select_cols,
                    "count": len(rows_list),
                    "limit": limit,
                    "rows": rows_list,
                    "urls": [str((row or {}).get("content") or "").strip() for row in rows_list if str((row or {}).get("content") or "").strip()],
                }
            ),
            status_code=200,
        )
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[LearnListPostRows] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


def _learn_list_url_match_candidates(*values: Any) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        candidates = [text]
        if text.startswith("https://"):
            candidates.append("http://" + text[len("https://") :])
        elif text.startswith("http://"):
            candidates.append("https://" + text[len("http://") :])
        for candidate in candidates:
            if candidate and candidate not in seen:
                seen.add(candidate)
                out.append(candidate)
    return out


async def _infer_file_source_category_from_learn_list(
    *,
    db_name: str,
    learn_list_table: str,
    cols: set[str],
    file_where: str,
    row: Dict[str, Any],
    category_rule_obj: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str, str]:
    source_values = _learn_list_url_match_candidates(
        row.get("pg_source_url"),
        row.get("source_url"),
    )
    if source_values and "content" in cols and "cate2" in cols:
        placeholders = ", ".join(["%s"] * len(source_values))
        content_type_prefix = ""
        type_fallback_suffix = ""
        query_params = tuple(source_values)
        if "content_type" in cols:
            content_type_prefix = "`content_type` = %s AND"
            query_params = ("url", *source_values)
        elif "type" in cols:
            type_fallback_suffix = "AND LOWER(COALESCE(`type`, '')) = 'url'"
        inferred_rows = await mysql_execute_query(
            f"""
            SELECT `cate1`, `cate2`, `id`, `subject`
            FROM `{learn_list_table}`
            WHERE {content_type_prefix} `content` IN ({placeholders})
              {type_fallback_suffix}
              AND COALESCE(NULLIF(`cate2`, ''), '') <> ''
            ORDER BY `id` DESC
            LIMIT 1
            """,
            query_params,
            fetch=True,
            dbname=db_name,
        )
        inferred = (inferred_rows or [None])[0]
        if isinstance(inferred, dict):
            return (
                str(inferred.get("cate1") or "").strip(),
                str(inferred.get("cate2") or "").strip(),
                "source_content_url_cate_matched",
            )
    if category_rule_obj and source_values:
        for candidate in source_values:
            try:
                resolved_pair = resolve_cate_for_detail_url(str(candidate or ""), category_rule_obj)
            except Exception:
                resolved_pair = None
            if resolved_pair:
                return (
                    str(resolved_pair[0] or "").strip(),
                    str(resolved_pair[1] or "").strip(),
                    "category_rule_cate_resolved",
                )
    source_cols = [col for col in ("source_page", "content") if col in cols]
    if not source_values or not source_cols or "cate2" not in cols:
        return ("", "", "source_cate_empty")
    placeholders = ", ".join(["%s"] * len(source_values))
    source_where = "(" + " OR ".join(f"`{col}` IN ({placeholders})" for col in source_cols) + ")"
    params: List[Any] = []
    for _col in source_cols:
        params.extend(source_values)
    inferred_rows = await mysql_execute_query(
        f"""
        SELECT `cate1`, `cate2`, `id`, `subject`
        FROM `{learn_list_table}`
        WHERE {source_where}
          AND NOT ({file_where})
          AND COALESCE(NULLIF(`cate2`, ''), '') <> ''
        ORDER BY `id` DESC
        LIMIT 1
        """,
        tuple(params),
        fetch=True,
        dbname=db_name,
    )
    inferred = (inferred_rows or [None])[0]
    if not isinstance(inferred, dict):
        return ("", "", "source_cate_empty")
    return (
        str(inferred.get("cate1") or "").strip(),
        str(inferred.get("cate2") or "").strip(),
        "source_page_cate_inferred",
    )


async def _load_category_names_by_code(
    *,
    db_name: str,
    chat_bot_id: str,
    codes: List[str],
) -> Dict[str, str]:
    unique_codes = sorted({str(code or "").strip() for code in codes if str(code or "").strip()})
    if not unique_codes:
        return {}
    table_name = get_category_table_name(chat_bot_id)
    placeholders = ", ".join(["%s"] * len(unique_codes))
    try:
        rows = await mysql_execute_query(
            f"""
            SELECT `cate_code`, `cate_name`
            FROM `{table_name}`
            WHERE `cate_code` IN ({placeholders})
            """,
            tuple(unique_codes),
            fetch=True,
            dbname=db_name,
        )
    except Exception as exc:
        logger.warning(
            "[LearnListFileCategoryDryRun] category name lookup failed | db=%s table=%s codes=%s err=%s",
            db_name,
            table_name,
            unique_codes[:20],
            exc,
        )
        return {}
    names: Dict[str, str] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        code = str(row.get("cate_code") or "").strip()
        name = str(row.get("cate_name") or "").strip()
        if code and name:
            names[code] = name
    return names


def _learn_list_file_category_debug_counts_enabled() -> bool:
    return str(os.getenv("LEARN_LIST_FILE_CATEGORY_DEBUG_COUNTS", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


async def _learn_list_file_category_debug_counts(
    *,
    db_name: str,
    learn_list_table: str,
    cols: set[str],
) -> Dict[str, Any]:
    debug: Dict[str, Any] = {
        "table": learn_list_table,
        "columns": sorted(cols),
        "total_rows": None,
        "content_type_counts": [],
        "type_counts": [],
        "recent_samples": [],
        "expensive_counts_skipped": True,
        "reason": "skip_count_star_on_learn_list",
    }
    sample_cols = [
        col
        for col in ("id", "subject", "content", "content_type", "type", "cate1", "cate2", "source_page")
        if col in cols
    ]
    if sample_cols:
        order_sql = "`id` DESC" if "id" in cols else "1"
        try:
            debug["recent_samples"] = await mysql_execute_query(
                f"""
                SELECT {', '.join(f'`{col}`' for col in sample_cols)}
                FROM `{learn_list_table}`
                ORDER BY {order_sql}
                LIMIT 10
                """,
                fetch=True,
                dbname=db_name,
            ) or []
        except Exception as exc:
            debug["recent_samples_error"] = str(exc)
    return debug


@router.post("/backend/learn-list/file-category-dry-run")
async def learn_list_file_category_dry_run(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        db_name = resolve_db_name(body, default="dev_user") or "dev_user"
        chat_bot_id = str(
            body.get("chat_bot_id")
            or (body.get("metadata") or {}).get("chat_bot_id")
            or ""
        ).strip()
        if not chat_bot_id:
            return JSONResponse({"status": "error", "message": "chat_bot_id is required"}, status_code=400)
        try:
            limit = int(body.get("limit") or 500)
        except Exception:
            limit = 500
        limit = max(1, min(limit, 5000))
        raw_ids = body.get("row_ids") or body.get("ids") or []
        row_ids: List[int] = []
        if isinstance(raw_ids, list):
            for raw in raw_ids:
                try:
                    rid = int(raw)
                except Exception:
                    continue
                if rid > 0 and rid not in row_ids:
                    row_ids.append(rid)
        row_ids = row_ids[:5000]

        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        learn_list_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
        if not learn_list_table:
            learn_list_table = get_learn_list_table_name(account_identifier)
        cols = await ensure_learn_list_standard_columns(db_name, learn_list_table)
        if "id" not in cols or "cate1" not in cols:
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list requires id and cate1 columns",
                    "table": learn_list_table,
                    "columns": sorted(cols),
                },
                status_code=400,
            )
        if "content_type" in cols:
            file_where = "LOWER(COALESCE(`content_type`, '')) = 'file'"
        elif "type" in cols:
            file_where = "LOWER(COALESCE(`type`, '')) = 'file'"
        else:
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list has no content_type/type column",
                    "table": learn_list_table,
                    "columns": sorted(cols),
                },
                status_code=400,
            )
        select_candidates = ["id", "subject", "content", "content_type", "type", "cate1", "cate2", "source_page"]
        select_cols = [col for col in select_candidates if col in cols]
        params: List[Any] = []
        where_parts = [file_where]
        if row_ids:
            placeholders = ", ".join(["%s"] * len(row_ids))
            where_parts.append(f"`id` IN ({placeholders})")
            params.extend(row_ids)
        params.append(limit)
        rows = await mysql_execute_query(
            f"""
            SELECT {', '.join(f'`{col}`' for col in select_cols)}
            FROM `{learn_list_table}`
            WHERE {' AND '.join(where_parts)}
            ORDER BY `id` DESC
            LIMIT %s
            """,
            tuple(params),
            fetch=True,
            dbname=db_name,
        )
        rows_list = [row for row in list(rows or []) if isinstance(row, dict)]
        fallback_source_url_rows = False
        if not rows_list and not row_ids and "content_type" in cols:
            url_select_candidates = [
                "id",
                "subject",
                "content",
                "content_type",
                "cate1",
                "cate2",
                "source_page",
                "created_at",
                "content_created_at",
            ]
            url_select_cols = [col for col in url_select_candidates if col in cols]
            rows = await mysql_execute_query(
                f"""
                SELECT {', '.join(f'`{col}`' for col in url_select_cols)}
                FROM `{learn_list_table}`
                WHERE LOWER(COALESCE(`content_type`, '')) = 'url'
                  AND COALESCE(NULLIF(`cate2`, ''), '') <> ''
                ORDER BY `id` DESC
                LIMIT %s
                """,
                (limit,),
                fetch=True,
                dbname=db_name,
            )
            rows_list = [row for row in list(rows or []) if isinstance(row, dict)]
            fallback_source_url_rows = bool(rows_list)
        debug_counts: Dict[str, Any] = {}
        if not rows_list:
            if _learn_list_file_category_debug_counts_enabled():
                debug_counts = await _learn_list_file_category_debug_counts(
                    db_name=db_name,
                    learn_list_table=learn_list_table,
                    cols=cols,
                )
            else:
                debug_counts = {
                    "skipped": True,
                    "reason": "disabled",
                    "enable_env": "LEARN_LIST_FILE_CATEGORY_DEBUG_COUNTS=1",
                }
        target_source_page = str(
            body.get("contents_url")
            or body.get("access_url")
            or body.get("target_url")
            or body.get("url")
            or ""
        ).strip()
        pg_source_url_meta: Dict[str, Any] = {}
        try:
            pg_source_url_meta = await _load_pg_text_data_for_file_rows(
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                account_identifier=account_identifier,
                rows=rows_list,
            )
        except Exception as exc:
            logger.info(
                "[LearnListFileCategoryDryRun] PostgreSQL content_metadata source_url lookup skipped | db=%s table=%s err=%s",
                db_name,
                learn_list_table,
                exc,
            )
            pg_source_url_meta = {"training_table": "", "source_url_matched_rows": 0, "reason": str(exc)}

        category_rule_obj: Optional[Dict[str, Any]] = None
        try:
            category_rule_obj = await _load_category_url_pattern_object(
                chat_bot_id,
                db_name,
                contents_url=target_source_page or None,
                require_nonempty_rules=True,
            )
        except Exception as exc:
            logger.info(
                "[LearnListFileCategoryDryRun] CATEGORY rule load skipped | db=%s table=%s err=%s",
                db_name,
                learn_list_table,
                exc,
            )
            category_rule_obj = None

        results: List[Dict[str, Any]] = []
        for row in rows_list:
            if not isinstance(row, dict):
                continue
            inferred_c1, inferred_c2, inferred_reason = await _infer_file_source_category_from_learn_list(
                db_name=db_name,
                learn_list_table=learn_list_table,
                cols=cols,
                file_where=file_where,
                row=row,
                category_rule_obj=category_rule_obj,
            )
            source_c1 = inferred_c1 or str(row.get("cate1") or "").strip()
            source_c2 = inferred_c2 or str(row.get("cate2") or "").strip()
            mapped_c1 = ""
            mapped_c2 = ""
            reason = ""
            if not (source_c1 or source_c2):
                reason = "source_cate_empty"
            else:
                reason = inferred_reason if (inferred_c1 or inferred_c2) else "source_row_cate"
            if source_c1 or source_c2:
                mapped_c1, mapped_c2 = await _ensure_file_learning_category_mapping(
                    chat_bot_id=chat_bot_id,
                    db_name=db_name,
                    source_cate1=source_c1,
                    source_cate2=source_c2,
                    create_missing=False,
                )
                if mapped_c1 or mapped_c2:
                    reason = "mapped" if reason == "source_row_cate" else reason
                else:
                    reason = "file_learning_mapping_empty"
            results.append(
                {
                    "id": row.get("id"),
                    "subject": row.get("subject"),
                    "content": row.get("content"),
                    "source_url": row.get("pg_source_url") or row.get("source_url") or row.get("content") or "",
                    "source_cate1": source_c1,
                    "source_cate2": source_c2,
                    "dry_run_cate1": mapped_c1,
                    "dry_run_cate2": mapped_c2,
                    "would_update": bool(mapped_c1 and mapped_c2 and not fallback_source_url_rows),
                    "reason": reason,
                    "preview_source": "url_rows_fallback" if fallback_source_url_rows else "file_rows",
                }
            )
        category_names = await _load_category_names_by_code(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            codes=[
                str(item.get(key) or "")
                for item in results
                for key in ("source_cate1", "source_cate2", "dry_run_cate1", "dry_run_cate2")
            ],
        )
        for item in results:
            item["source_cate1_name"] = category_names.get(str(item.get("source_cate1") or "").strip(), "")
            item["source_cate2_name"] = category_names.get(str(item.get("source_cate2") or "").strip(), "")
            item["dry_run_cate1_name"] = category_names.get(str(item.get("dry_run_cate1") or "").strip(), "")
            item["dry_run_cate2_name"] = category_names.get(str(item.get("dry_run_cate2") or "").strip(), "")
        return JSONResponse(
            jsonable_encoder(
                {
                    "status": "success",
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "table": learn_list_table,
                    "count": len(results),
                    "pg_source_url": pg_source_url_meta,
                    "debug_counts": debug_counts,
                    "fallback_source_url_rows": fallback_source_url_rows,
                    "results": results,
                }
            ),
            status_code=200,
        )
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[LearnListFileCategoryDryRun] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/learn-list/file-category-apply-dry-run")
async def learn_list_file_category_apply_dry_run(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        db_name = resolve_db_name(body, default="dev_user") or "dev_user"
        chat_bot_id = str(
            body.get("chat_bot_id")
            or (body.get("metadata") or {}).get("chat_bot_id")
            or ""
        ).strip()
        if not chat_bot_id:
            return JSONResponse({"status": "error", "message": "chat_bot_id is required"}, status_code=400)

        raw_results = body.get("results") or body.get("dry_run_results") or []
        if not isinstance(raw_results, list):
            raw_results = []
        updates: List[Dict[str, Any]] = []
        seen_ids: set[int] = set()
        for item in raw_results:
            if not isinstance(item, dict):
                continue
            try:
                row_id = int(item.get("id") or 0)
            except Exception:
                continue
            cate1 = str(item.get("dry_run_cate1") or item.get("cate1") or "").strip()
            cate2 = str(item.get("dry_run_cate2") or item.get("cate2") or "").strip()
            if row_id <= 0 or row_id in seen_ids or not (cate1 and cate2):
                continue
            if item.get("would_update") is False:
                continue
            seen_ids.add(row_id)
            updates.append({"id": row_id, "cate1": cate1, "cate2": cate2})
            if len(updates) >= 5000:
                break
        if not updates:
            return JSONResponse(
                {
                    "status": "success",
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "updated": 0,
                    "reason": "no_applicable_dry_run_results",
                },
                status_code=200,
        )

        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        learn_list_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
        if not learn_list_table:
            learn_list_table = get_learn_list_table_name(account_identifier)
        cols = await ensure_learn_list_standard_columns(db_name, learn_list_table)
        if not {"id", "cate1", "cate2"}.issubset(cols):
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list requires id, cate1, cate2 columns",
                    "table": learn_list_table,
                    "columns": sorted(cols),
                },
                status_code=400,
            )
        if "content_type" in cols:
            file_where = "LOWER(COALESCE(`content_type`, '')) = 'file'"
        elif "type" in cols:
            file_where = "LOWER(COALESCE(`type`, '')) = 'file'"
        else:
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list has no content_type/type column",
                    "table": learn_list_table,
                    "columns": sorted(cols),
                },
                status_code=400,
            )

        updated = 0
        details: List[Dict[str, Any]] = []
        for item in updates:
            affected = await mysql_execute_query(
                f"""
                UPDATE `{learn_list_table}`
                SET `cate1` = %s, `cate2` = %s
                WHERE `id` = %s AND {file_where}
                """,
                (item["cate1"], item["cate2"], item["id"]),
                fetch=False,
                dbname=db_name,
            )
            affected_int = 0
            try:
                affected_int = int(affected or 0)
            except Exception:
                affected_int = 0
            updated += affected_int
            if len(details) < 50:
                details.append(
                    {
                        "id": item["id"],
                        "cate1": item["cate1"],
                        "cate2": item["cate2"],
                        "affected": affected_int,
                    }
                )

        logger.info(
            "[LearnListFileCategoryApplyDryRun] applied | db=%s chat_bot_id=%s table=%s requested=%s updated=%s",
            db_name,
            chat_bot_id,
            learn_list_table,
            len(updates),
            updated,
        )
        return JSONResponse(
            jsonable_encoder(
                {
                    "status": "success",
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "table": learn_list_table,
                    "requested": len(updates),
                    "updated": updated,
                    "details": details,
                }
            ),
            status_code=200,
        )
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[LearnListFileCategoryApplyDryRun] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/learn-list/file-category-bulk-apply-by-created-at")
async def learn_list_file_category_bulk_apply_by_created_at(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        db_name = resolve_db_name(body, default="dev_user") or "dev_user"
        chat_bot_id = str(
            body.get("chat_bot_id")
            or (body.get("metadata") or {}).get("chat_bot_id")
            or ""
        ).strip()
        if not chat_bot_id:
            return JSONResponse({"status": "error", "message": "chat_bot_id is required"}, status_code=400)

        start_raw = str(body.get("start_date") or body.get("created_at_from") or "").strip()
        end_raw = str(body.get("end_date") or body.get("created_at_to") or "").strip()
        if not start_raw or not end_raw:
            return JSONResponse(
                {"status": "error", "message": "start_date and end_date are required"},
                status_code=400,
            )
        try:
            start_dt = datetime.strptime(start_raw[:10], "%Y-%m-%d")
            end_dt = datetime.strptime(end_raw[:10], "%Y-%m-%d") + timedelta(days=1)
        except Exception:
            return JSONResponse(
                {"status": "error", "message": "date format must be YYYY-MM-DD"},
                status_code=400,
            )
        if end_dt <= start_dt:
            return JSONResponse(
                {"status": "error", "message": "end_date must be same as or after start_date"},
                status_code=400,
            )
        try:
            limit = int(body.get("limit") or 5000)
        except Exception:
            limit = 5000
        limit = max(1, min(limit, 20000))

        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        learn_list_table = get_learn_list_table_name(account_identifier)
        cols = await ensure_learn_list_standard_columns(db_name, learn_list_table)
        required = {"id", "cate1", "cate2", "created_at"}
        if not required.issubset(cols):
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list requires id, cate1, cate2, created_at columns",
                    "table": learn_list_table,
                    "columns": sorted(cols),
                },
                status_code=400,
            )
        if "content_type" in cols:
            file_where = "LOWER(COALESCE(`content_type`, '')) = 'file'"
        elif "type" in cols:
            file_where = "LOWER(COALESCE(`type`, '')) = 'file'"
        else:
            return JSONResponse(
                {
                    "status": "error",
                    "message": "learn_list has no content_type/type column",
                    "table": learn_list_table,
                    "columns": sorted(cols),
                },
                status_code=400,
            )

        select_cols = ["id", "subject", "cate1", "cate2", "created_at"]
        rows = await mysql_execute_query(
            f"""
            SELECT {', '.join(f'`{col}`' for col in select_cols)}
            FROM `{learn_list_table}`
            WHERE {file_where}
              AND `created_at` >= %s
              AND `created_at` < %s
            ORDER BY `id` DESC
            LIMIT %s
            """,
            (start_dt, end_dt, limit),
            fetch=True,
            dbname=db_name,
        )

        scanned = 0
        mapped = 0
        updated = 0
        skipped = 0
        details: List[Dict[str, Any]] = []
        for row in list(rows or []):
            if not isinstance(row, dict):
                continue
            scanned += 1
            row_id = row.get("id")
            source_c1 = str(row.get("cate1") or "").strip()
            source_c2 = str(row.get("cate2") or "").strip()
            if not source_c2:
                skipped += 1
                if len(details) < 80:
                    details.append(
                        {
                            "id": row_id,
                            "subject": row.get("subject"),
                            "affected": 0,
                            "reason": "source_cate2_empty",
                        }
                    )
                continue
            mapped_c1, mapped_c2 = await _ensure_file_learning_category_mapping(
                chat_bot_id=chat_bot_id,
                db_name=db_name,
                source_cate1=source_c1,
                source_cate2=source_c2,
                create_missing=False,
            )
            if not (mapped_c1 and mapped_c2):
                skipped += 1
                if len(details) < 80:
                    details.append(
                        {
                            "id": row_id,
                            "subject": row.get("subject"),
                            "source_cate1": source_c1,
                            "source_cate2": source_c2,
                            "affected": 0,
                            "reason": "file_learning_mapping_empty",
                        }
                    )
                continue
            mapped += 1
            affected = await mysql_execute_query(
                f"""
                UPDATE `{learn_list_table}`
                SET `cate1` = %s, `cate2` = %s
                WHERE `id` = %s
                  AND {file_where}
                  AND `created_at` >= %s
                  AND `created_at` < %s
                """,
                (mapped_c1, mapped_c2, row_id, start_dt, end_dt),
                fetch=False,
                dbname=db_name,
            )
            affected_int = 0
            try:
                affected_int = int(affected or 0)
            except Exception:
                affected_int = 0
            updated += affected_int
            if len(details) < 80:
                details.append(
                    {
                        "id": row_id,
                        "subject": row.get("subject"),
                        "source_cate1": source_c1,
                        "source_cate2": source_c2,
                        "cate1": mapped_c1,
                        "cate2": mapped_c2,
                        "affected": affected_int,
                        "reason": "mapped",
                    }
                )

        logger.info(
            "[LearnListFileCategoryBulkApplyByCreatedAt] applied | db=%s chat_bot_id=%s table=%s start=%s end=%s scanned=%s mapped=%s updated=%s skipped=%s",
            db_name,
            chat_bot_id,
            learn_list_table,
            start_raw,
            end_raw,
            scanned,
            mapped,
            updated,
            skipped,
        )
        return JSONResponse(
            jsonable_encoder(
                {
                    "status": "success",
                    "db_name": db_name,
                    "chat_bot_id": chat_bot_id,
                    "table": learn_list_table,
                    "start_date": start_raw[:10],
                    "end_date": end_raw[:10],
                    "limit": limit,
                    "scanned": scanned,
                    "mapped": mapped,
                    "updated": updated,
                    "skipped": skipped,
                    "details": details,
                }
            ),
            status_code=200,
        )
    except ValueError as exc:
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=400)
    except Exception as exc:
        logger.exception("[LearnListFileCategoryBulkApplyByCreatedAt] endpoint failed: %s", exc)
        return JSONResponse({"status": "error", "message": str(exc)}, status_code=500)


@router.post("/backend/session/start")
async def crawl_start(request: Request, background_tasks: BackgroundTasks):
    dedupe_key: Optional[str] = None
    dedupe_locked = False
    job_id = ""
    try:
        body = await request.json()
        _normalize_file_crawl_route_hint(body, stage="request")
        body["stream_matched_rules_only"] = resolve_stream_matched_rules_only(body)
        body["_category_sync_request_cookies"] = dict(request.cookies or {})
        job_id = body.get("job_id")
        db_name = resolve_db_name(body, default="dev_user")
        _trace_url = ""
        try:
            _contents = body.get("contents")
            if isinstance(_contents, list) and _contents:
                _trace_url = str(_contents[0] or "").strip()
            elif isinstance(_contents, str):
                _trace_url = _contents.strip()
            _trace_url = str(body.get("contents_url") or body.get("access_url") or body.get("target_url") or _trace_url or "").strip()
        except Exception:
            _trace_url = ""
        _songpa_title_trace(
            "session_start_request",
            url=_trace_url,
            job_id=job_id,
            db_name=db_name,
            colle=body.get("colle"),
            content_type=body.get("content_type"),
            crawl_mode=body.get("crawl_mode"),
            method=body.get("method"),
            contents=body.get("contents"),
        )
        try:
            logger.info(
                "%s[start_request] job_id=%s db=%s colle=%s slot=%s active=%s env_max=%s use_celery=%s burst_dedupe=%s",
                CONCURRENT_CRAWL_LOG_PREFIX,
                job_id,
                db_name,
                str(body.get("colle") or "").strip().lower(),
                crawler_state.get_workflow_slot_snapshot(),
                crawler_state.get_workflow_debug_snapshot(),
                os.getenv("CRAWL_MAX_ACTIVE_WORKFLOWS"),
                os.getenv("CRAWL_WORKFLOW_USE_CELERY"),
                os.getenv("CRAWL_START_BURST_DEDUPE"),
            )
        except Exception:
            logger.debug("%s[start_request] snapshot failed", CONCURRENT_CRAWL_LOG_PREFIX, exc_info=True)
        logger.debug(
            "[PartialCategory][StartRequest] job_id=%s db=%s decision=%s payload_summary=%s",
            job_id,
            db_name,
            partial_category_debug_reason(body),
            {
                "colle": body.get("colle"),
                "content_type": body.get("content_type"),
                "crawl_mode": body.get("crawl_mode"),
                "partial_update_fields": body.get("partial_update_fields"),
                "partial_target_filter": body.get("partial_target_filter"),
                "contents": body.get("contents"),
            },
        )

        redis = await get_redis()

        try:
            active_workers = getattr(crawler_state, "active_worker_tasks", {}) or {}
            existing_worker_task = active_workers.get(job_id)
            existing_workflow_task = crawler_state.workflow_tasks.get(job_id)
            existing_workflow = crawler_state.workflows.get(job_id)
            repeat_reasons = []
            if existing_worker_task is not None and not existing_worker_task.done():
                repeat_reasons.append("active_worker_task")
            if existing_workflow_task is not None and not existing_workflow_task.done():
                repeat_reasons.append("active_workflow_task")
            if existing_workflow is not None:
                repeat_reasons.append("workflow_registered")
            if job_id in crawler_state.job_history:
                repeat_reasons.append(f"history:{crawler_state.job_history.get(job_id, {}).get('status')}")
            if repeat_reasons:
                logger.warning(
                    "[CrawlRegistrationRepeat] repeated crawl registration | job_id=%s db=%s reasons=%s "
                    "worker_done=%s workflow_task_done=%s workflow_type=%s active=%s payload_summary=%s",
                    job_id,
                    db_name,
                    repeat_reasons,
                    None if existing_worker_task is None else existing_worker_task.done(),
                    None if existing_workflow_task is None else existing_workflow_task.done(),
                    None if existing_workflow is None else type(existing_workflow).__name__,
                    crawler_state.get_workflow_debug_snapshot(),
                    {
                        "colle": body.get("colle"),
                        "content_type": body.get("content_type"),
                        "crawl_mode": body.get("crawl_mode"),
                        "contents": body.get("contents"),
                        "target_date": body.get("target_date"),
                    },
                )
            active_collision = any(
                reason in repeat_reasons
                for reason in {"active_worker_task", "active_workflow_task", "workflow_registered"}
            )
            if active_collision:
                original_job_id = str(job_id or "").strip()
                job_id = f"{original_job_id or 'crawl'}-{uuid4().hex[:8]}"
                body["job_id"] = job_id
                logger.warning(
                    "[CrawlRegistrationRepeat] active job_id collision remapped | original_job_id=%s new_job_id=%s db=%s reasons=%s",
                    original_job_id,
                    job_id,
                    db_name,
                    repeat_reasons,
                )
        except Exception:
            logger.debug("[CrawlRegistrationRepeat] repeat check failed", exc_info=True)
        
        # [시작 상태 1] job metadata를 Redis에 먼저 저장해 SSE 연결 직후 404/누락을 줄인다.
        # 프론트엔드가 빠르게 구독을 시작해도 job 상태를 찾을 수 있도록 최소 메타데이터를 먼저 남긴다.
        await cache_job_metadata(job_id, db_name)
        
        # [시작 상태 2] 이전 상태를 지우고 새 크롤링 시작 상태를 기록한다.
        await redis.delete(f"crawl:{db_name}:{job_id}:state")
        initial_start_payload = {
            "status": "start",
            "event": "crawl_requested",
            "job_id": job_id,
            "account_name": db_name,
            "scan_count": 0,
            "total_count": 0,
            "collection_count": 0,
            "save_count": 0,
            "study_count": 0,
            "h3": "크롤링을 준비하고 있습니다.",
            "source": "crawl_start_initial",
            "timestamp": datetime.now().isoformat(),
        }
        await update_state_only(
            job_id=job_id,
            account_name=db_name,
            payload=initial_start_payload,
        )
        try:
            await send_message_to_redis_sse(
                job_id=job_id,
                dbname=db_name,
                message=initial_start_payload,
            )
        except Exception:
            pass
        try:
            bootstrap_task = asyncio.create_task(
                bootstrap_job_state(job_id, db_name, "start_request"),
                name=f"initial-redis-bootstrap:{job_id}",
            )
            bootstrap_task.add_done_callback(swallow_task_exception)
        except Exception:
            pass
        if _burst_dedupe_enabled():
            acquired, dedupe_key, existing_job_id, dedupe_ttl = await _acquire_burst_dedupe_lock(
                redis=redis,
                data=body,
                db_name=db_name,
                job_id=str(job_id or "").strip(),
            )
            if not acquired:
                logger.warning(
                    "[StartDedupe] burst duplicate blocked | job_id=%s existing_job_id=%s ttl=%s colle=%s active=%s",
                    job_id,
                    existing_job_id,
                    dedupe_ttl,
                    str(body.get("colle") or "").strip().lower(),
                    crawler_state.get_workflow_debug_snapshot(),
                )
                duplicate_payload = {
                    "status": "duplicate",
                    "event": "duplicate_blocked",
                    "scan_count": 0,
                    "total_count": 0,
                    "message": f"동일한 크롤링 요청이 이미 처리 중입니다. {dedupe_ttl}초 후 다시 시도하세요.",
                }
                if existing_job_id:
                    duplicate_payload["duplicate_of"] = existing_job_id
                await update_state_only(
                    job_id=job_id,
                    account_name=db_name,
                    payload=duplicate_payload,
                )
                await send_message_to_redis_sse(
                    job_id=job_id,
                    dbname=db_name,
                    message=duplicate_payload,
                )
                return JSONResponse(
                    {
                        "status": "accepted",
                        "job_id": job_id,
                        "duplicate": True,
                        "duplicate_of": existing_job_id,
                    },
                    status_code=200,
                )
            await _remember_burst_dedupe_lock(
                redis=redis,
                job_id=str(job_id or "").strip(),
                dedupe_key=dedupe_key,
                ttl=dedupe_ttl,
            )
            dedupe_locked = True

        # 중복 방지 락을 현재 job에 연결해 후속 정리에서 해제할 수 있게 한다.
        worker_task = asyncio.create_task(_crawl_file_worker(body, background_tasks), name=f"worker:{job_id}")
        
        if not hasattr(crawler_state, "active_worker_tasks"):
            crawler_state.active_worker_tasks = {}
        crawler_state.active_worker_tasks[job_id] = worker_task
        try:
            logger.info(
                "%s[worker_scheduled] job_id=%s task_name=%s active=%s",
                CONCURRENT_CRAWL_LOG_PREFIX,
                job_id,
                worker_task.get_name(),
                crawler_state.get_workflow_debug_snapshot(),
            )
        except Exception:
            logger.debug("%s[worker_scheduled] snapshot failed", CONCURRENT_CRAWL_LOG_PREFIX, exc_info=True)

        return JSONResponse({"status": "accepted", "job_id": job_id}, status_code=200)

    except Exception as e:
        if dedupe_locked and dedupe_key:
            try:
                redis = await get_redis()
                await release_burst_dedupe_lock(job_id, redis=redis)
            except Exception:
                pass
        logger.exception(f"Start API Error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@router.post("/backend/session/accelerated-start")
async def crawl_accelerated_start(request: Request):
    """Create start_urls, filter duplicates from the warmed learn_list cache, then stop before crawling."""
    request_started_at = time.perf_counter()
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        _normalize_file_crawl_route_hint(body, stage="accelerated_request")
        body["stream_matched_rules_only"] = resolve_stream_matched_rules_only(body)
        body["crawl_mode"] = "accelerated"
        body["accelerated_crawl"] = True
        sse_publish_enabled = _accelerated_sse_publish_enabled(body)
        redis_state_enabled = _accelerated_redis_state_enabled(body)

        job_id = str(body.get("job_id") or "").strip()
        db_name = resolve_db_name(body, default="dev_user") or "dev_user"
        chat_bot_id = str(
            body.get("chat_bot_id")
            or (body.get("metadata") or {}).get("chat_bot_id")
            or ""
        ).strip()
        if not job_id:
            return JSONResponse({"status": "error", "message": "job_id is required"}, status_code=400)

        if is_file_category_update_only_request(body):
            logger.info("[FileCategorySync][AcceleratedRoute] job_id=%s route=file_category_update_only", job_id)
            result = await _run_file_category_sync_for_request(body, update_only=True)
            return JSONResponse(
                {
                    "status": "completed",
                    "job_id": job_id,
                    "db_name": db_name,
                    "file_category_sync": result,
                }
            )

        _log_accelerated_parse_save(
            "request_received",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            colle=body.get("colle"),
            content_type=body.get("content_type"),
            crawl_mode=body.get("crawl_mode"),
            contents=body.get("contents"),
            target_date=body.get("target_date"),
            start_urls_order=body.get("start_urls_order"),
        )
        redis = await get_redis()
        await cache_job_metadata(job_id, db_name)
        await redis.delete(f"crawl:{db_name}:{job_id}:state")
        started_payload = {
            "status": "start",
            "event": "accelerated_start_urls",
            "scan_count": 0,
            "total_count": 0,
            "collection_count": 0,
            "h3": "crawl status",
            "message": "crawl status",
        }
        _set_accelerated_memory_state(job_id, started_payload)
        if redis_state_enabled:
            await update_state_only(job_id=job_id, account_name=db_name, payload=started_payload)
        if sse_publish_enabled:
            await send_message_to_redis_sse(
                job_id=job_id,
                dbname=db_name,
                message=started_payload,
            )
        await _update_accelerated_crawling_log(
            job_id=job_id,
            db_name=db_name,
            data=body,
            scan=0,
            collection=0,
            saved=0,
            study=0,
            status="running",
            reason="accelerated_request_started",
        )

        warm_meta: Dict[str, Any] = {"loaded": 0, "cache_warmed": False}
        try:
            limit = int(
                body.get("learn_list_duplicate_exclude_limit")
                or body.get("learnListDuplicateExcludeLimit")
                or os.getenv("LEARN_LIST_START_URL_DEDUPE_LIMIT", "200000")
                or "200000"
            )
        except Exception:
            limit = 200000
        if chat_bot_id:
            warm_started_at = time.perf_counter()
            _log_accelerated_parse_save(
                "learn_list_cache_warm_begin",
                job_id=job_id,
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                limit=limit,
            )
            _, warm_meta = await load_learn_list_url_keys(
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                job_id=job_id,
                limit=limit,
            )
            warm_meta["cache_warmed"] = True
            _log_accelerated_parse_save(
                "learn_list_cache_warm_done",
                job_id=job_id,
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                elapsed_ms=int((time.perf_counter() - warm_started_at) * 1000),
                table=warm_meta.get("table"),
                loaded=warm_meta.get("loaded"),
                rows=warm_meta.get("rows"),
                cache_hit=warm_meta.get("cache_hit"),
                error=warm_meta.get("error"),
            )
        else:
            _log_accelerated_parse_save(
                "learn_list_cache_warm_skip",
                job_id=job_id,
                db_name=db_name,
                reason="chat_bot_id_missing",
            )

        prepare_body = dict(body)
        prepare_body["learn_list_duplicate_exclude_enabled"] = False
        prepare_body["learnListDuplicateExcludeEnabled"] = False
        prepare_started_at = time.perf_counter()
        _log_accelerated_parse_save(
            "start_urls_prepare_begin",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
        )
        header_response, db_name, job_id, chat_bot_id = await _prepare_crawl(prepare_body)
        del header_response

        start_urls = list(prepare_body.get("start_urls_override") or [])
        actual_start_urls_count = len(start_urls)
        total_found = int(prepare_body.get("pre_explored_start_urls_count") or actual_start_urls_count)
        redis_total_found = _resolve_redis_exploration_count(prepare_body, total_found)
        crawling_log_scan_count = max(int(total_found or 0), int(actual_start_urls_count or 0))
        prepare_body["crawling_log_scan_count"] = crawling_log_scan_count
        prepare_body["original_exploration_scan_count"] = crawling_log_scan_count
        _log_accelerated_parse_save(
            "start_urls_prepare_done",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            elapsed_ms=int((time.perf_counter() - prepare_started_at) * 1000),
            count=total_found,
            actual_target_urls=actual_start_urls_count,
            override_source=prepare_body.get("start_urls_override_source"),
            sample=start_urls[:5],
        )
        prefilter_started_at = time.perf_counter()
        start_urls, accelerated_prefilter_meta = _prefilter_accelerated_start_urls(
            start_urls,
            contents_url=_resolve_primary_contents_url(prepare_body) or "",
        )
        _log_accelerated_parse_save(
            "accelerated_prefilter_done",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            elapsed_ms=int((time.perf_counter() - prefilter_started_at) * 1000),
            **accelerated_prefilter_meta,
        )
        dedupe_started_at = time.perf_counter()
        _log_accelerated_parse_save(
            "cache_dedupe_begin",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            start_urls_count=len(start_urls),
        )
        filtered_urls, dedupe_meta = filter_start_urls_against_loaded_learn_list_cache(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            job_id=job_id,
            start_urls=start_urls,
        )
        filtered_urls, fast_filter_meta = _filter_accelerated_fast_urls(
            filtered_urls,
            contents_url=_resolve_primary_contents_url(prepare_body) or "",
        )
        dedupe_meta["accelerated_prefilter"] = accelerated_prefilter_meta
        dedupe_meta["accelerated_fast_filter"] = fast_filter_meta
        selected_count = len(filtered_urls)
        crawl_target_count = selected_count
        prepare_body["start_urls_override"] = filtered_urls
        prepare_body["pre_explored_start_urls_count"] = redis_total_found
        prepare_body["crawling_log_scan_count"] = crawling_log_scan_count
        prepare_body["original_exploration_scan_count"] = crawling_log_scan_count
        prepare_body["learn_list_duplicate_exclude_selected_count"] = crawl_target_count
        prepare_body["selected_start_urls_count"] = crawl_target_count
        logger.info(
            "[LargeModeTargetUrls] accelerated actual target URLs | job_id=%s db=%s chat_bot_id=%s exploration_post_total=%s prepared=%s selected=%s duplicates=%s prefilter_skipped=%s fast_filter_skipped=%s",
            job_id,
            db_name,
            chat_bot_id,
            redis_total_found,
            actual_start_urls_count,
            selected_count,
            dedupe_meta.get("duplicates"),
            accelerated_prefilter_meta.get("skipped"),
            fast_filter_meta.get("skipped"),
        )
        try:
            parse_concurrency = max(1, min(int(os.getenv("ACCELERATED_PARSE_CONCURRENCY", "5") or "5"), 50))
        except Exception:
            parse_concurrency = 10
        mariadb_paths = _accelerated_mariadb_payload_paths(job_id)
        _log_accelerated_parse_save(
            "cache_dedupe_done",
            job_id=job_id,
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            elapsed_ms=int((time.perf_counter() - dedupe_started_at) * 1000),
            before=dedupe_meta.get("before"),
            after=dedupe_meta.get("after"),
            duplicates=dedupe_meta.get("duplicates"),
            prefilter=accelerated_prefilter_meta,
            fast_filter=fast_filter_meta,
            lookup=dedupe_meta.get("lookup"),
            table=dedupe_meta.get("table"),
            loaded=dedupe_meta.get("loaded"),
            duplicate_samples=dedupe_meta.get("duplicate_samples") or [],
            selected_sample=filtered_urls[:5],
        )

        selected_payload = {
            "status": "running",
            "event": "accelerated_crawl_selected",
            "source": "accelerated_crawl",
            "scan_count": redis_total_found,
            "total_count": redis_total_found,
            "actual_scan_count": crawl_target_count,
            "collection_count": 0,
            "save_count": 0,
            "study_count": 0,
            "start_urls_count": crawl_target_count,
            "actual_start_urls_count": crawl_target_count,
            "pre_explored_start_urls_count": redis_total_found,
            "prepared_start_urls_count": actual_start_urls_count,
            "exploration_post_total_count": redis_total_found,
            "crawling_log_scan_count": crawling_log_scan_count,
            "selected_start_urls_count": selected_count,
            "parse_concurrency": parse_concurrency,
            "mariadb_json_path": mariadb_paths["json"],
            "mariadb_jsonl_path": mariadb_paths["jsonl"],
            "duplicate_count": int(dedupe_meta.get("duplicates") or 0),
            "accelerated_fast_filter": fast_filter_meta,
            "learn_list_duplicate_exclude_result": {
                **dedupe_meta,
                "warm_meta": warm_meta,
            },
            "start_urls_sample": filtered_urls[:10],
            "h3": "crawl status",
            "message": "crawl status",
        }
        _set_accelerated_memory_state(job_id, selected_payload)
        if redis_state_enabled:
            await update_state_only(job_id=job_id, account_name=db_name, payload=selected_payload)
        if sse_publish_enabled:
            await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=selected_payload)
        await _update_accelerated_crawling_log(
            job_id=job_id,
            db_name=db_name,
            data=prepare_body,
            scan=crawl_target_count,
            collection=0,
            saved=0,
            study=0,
            status="running",
            reason="accelerated_selection_done",
        )
        _log_accelerated_parse_save(
            "selection_published",
            job_id=job_id,
            db_name=db_name,
            scan_count=crawl_target_count,
            crawling_log_scan_count=crawling_log_scan_count,
            actual_target_urls=crawl_target_count,
            prepared_target_urls=actual_start_urls_count,
            selected_target_urls=selected_count,
            collection_count=0,
            save_count=0,
        )
        crawler_state.record_history(
            job_id,
            "running",
            "accelerated_crawl_selected",
            db_name,
            chat_bot_id=chat_bot_id,
        )
        logger.info(
            "[AcceleratedCrawl] selected | job_id=%s db=%s chat_bot_id=%s before=%s after=%s duplicates=%s loaded=%s",
            job_id,
            db_name,
            chat_bot_id,
            actual_start_urls_count,
            selected_count,
            dedupe_meta.get("duplicates"),
            warm_meta.get("loaded"),
        )
        parse_task = asyncio.create_task(
            _run_accelerated_parse_selected(
                data=prepare_body,
                db_name=db_name,
                job_id=job_id,
                chat_bot_id=chat_bot_id,
                selected_urls=filtered_urls,
                scan_count=crawl_target_count,
                redis_scan_count=crawl_target_count,
                dedupe_meta=dedupe_meta,
                warm_meta=warm_meta,
            ),
            name=f"accelerated-parse:{job_id}",
        )
        if not hasattr(crawler_state, "active_worker_tasks"):
            crawler_state.active_worker_tasks = {}
        crawler_state.active_worker_tasks[job_id] = parse_task

        def _on_accelerated_parse_done(_task: asyncio.Task, _job_id: str = job_id, _db_name: str = db_name, _body: Dict[str, Any] = prepare_body) -> None:
            getattr(crawler_state, "active_worker_tasks", {}).pop(_job_id, None)
            try:
                if _task.cancelled():
                    asyncio.create_task(
                        _update_accelerated_crawling_log(
                            job_id=_job_id,
                            db_name=_db_name,
                            data=_body,
                            status="stop",
                            reason="accelerated_parse_cancelled",
                        )
                    )
                    return
                exc = _task.exception()
                if exc is not None:
                    asyncio.create_task(
                        _update_accelerated_crawling_log(
                            job_id=_job_id,
                            db_name=_db_name,
                            data=_body,
                            status="error",
                            reason="accelerated_parse_failed",
                        )
                    )
            except Exception:
                pass

        parse_task.add_done_callback(_on_accelerated_parse_done)
        _log_accelerated_parse_save(
            "parse_task_queued",
            job_id=job_id,
            db_name=db_name,
            selected_count=selected_count,
            concurrency=parse_concurrency,
            mariadb_json=mariadb_paths["json"],
            mariadb_jsonl=mariadb_paths["jsonl"],
            request_elapsed_ms=int((time.perf_counter() - request_started_at) * 1000),
            task_name=f"accelerated-parse:{job_id}",
        )
        return JSONResponse(
            {
                "status": "accepted",
                "job_id": job_id,
                "db_name": db_name,
                "scan_count": crawl_target_count,
                "total_count": crawl_target_count,
                "start_urls_count": crawl_target_count,
                "actual_start_urls_count": crawl_target_count,
                "prepared_start_urls_count": actual_start_urls_count,
                "crawling_log_scan_count": crawling_log_scan_count,
                "collection_count": 0,
                "selected_start_urls_count": selected_count,
                "parse_concurrency": parse_concurrency,
                "mariadb_json_path": mariadb_paths["json"],
                "mariadb_jsonl_path": mariadb_paths["jsonl"],
                "duplicate_count": int(dedupe_meta.get("duplicates") or 0),
                "learn_list_duplicate_exclude_result": selected_payload["learn_list_duplicate_exclude_result"],
            },
            status_code=200,
        )
    except Exception as e:
        _log_accelerated_parse_save(
            "request_error",
            job_id=str((locals().get("job_id") or "")),
            db_name=str((locals().get("db_name") or "")),
            elapsed_ms=int((time.perf_counter() - request_started_at) * 1000),
            error=str(e),
        )
        try:
            await _update_accelerated_crawling_log(
                job_id=str((locals().get("job_id") or "")),
                db_name=str((locals().get("db_name") or "")),
                data=locals().get("body") if isinstance(locals().get("body"), dict) else {},
                status="error",
                reason="accelerated_request_error",
            )
        except Exception:
            pass
        logger.exception("[AcceleratedCrawl] start failed: %s", e)
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


@router.post("/backend/session/heartbeat")
async def crawl_client_heartbeat(request: Request):
    """
    프론트엔드 세션 heartbeat를 받아 Redis에 PING + client_heartbeat를 기록한다.
    Body: { "job_id", "db_name" | "account_name" | "dbname" } (resolve_db_name으로 계정명을 정규화)
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    job_id = str(body.get("job_id") or body.get("jobId") or "").strip()
    db_name = resolve_db_name(body, default="") or ""
    if not db_name:
        db_name = str(
            body.get("db_name") or body.get("dbname") or body.get("account_name") or ""
        ).strip()
    if not job_id or not db_name:
        return JSONResponse(
            {"ok": False, "error": "job_id_and_db_name_required"},
            status_code=400,
        )
    out = await publish_client_redis_heartbeat(job_id, db_name)
    status = 200 if out.get("ok") else 503
    return JSONResponse(out, status_code=status)


async def _run_file_category_sync_for_request(data: Dict[str, Any], *, update_only: bool) -> Dict[str, Any]:
    db_name = resolve_db_name(data, default="dev_user")
    job_id = str(data.get("job_id") or "").strip()
    chat_bot_id = str(data.get("chat_bot_id") or (data.get("metadata") or {}).get("chat_bot_id") or "").strip()
    if not chat_bot_id:
        raise RuntimeError("file category sync requires chat_bot_id")

    event_name = "file_category_update_only" if update_only else "file_category_sync"
    message = "파일 카테고리만 갱신하는 중입니다.." if update_only else "파일 카테고리를 동기화하는 중입니다.."
    await send_message_to_redis_sse(
        job_id=job_id,
        dbname=db_name,
        message={
            "status": "running",
            "event": event_name,
            "scan_count": 0,
            "total_count": 0,
            "h3": message,
        },
    )

    if update_only:
        result = {
            "ok": True,
            "preview_summary": {},
            "stats": {
                "updated_file_rows_by_cate1_only": 0,
                "updated_file_rows_by_cate2": 0,
            },
            "reason": "update_only_skips_category_creation_sync",
        }
    else:
        result = await sync_existing_file_categories_from_homepage_learning(
            chat_bot_id=chat_bot_id,
            db_name=db_name,
            access_url=str(data.get("access_url") or "").strip() or None,
            request_cookies=dict(data.get("_category_sync_request_cookies") or {}),
        )
    stats = dict(result.get("stats") or {})
    updated = int(stats.get("updated_file_rows_by_cate1_only") or 0) + int(
        stats.get("updated_file_rows_by_cate2") or 0
    )
    detail_update: Dict[str, Any] = {}
    if update_only:
        try:
            logger.info(
                "[FileCategorySync][update_only] detail update start | job_id=%s db=%s chat_bot_id=%s",
                job_id,
                db_name,
                chat_bot_id,
            )
            detail_update = await run_file_category_update_only(data)
            updated += int(detail_update.get("updated") or 0)
            logger.info(
                "[FileCategorySync][update_only] detail update result | job_id=%s result=%s",
                job_id,
                detail_update,
            )
        except Exception as exc:
            detail_update = {"ok": False, "updated": 0, "error": str(exc)}
            logger.warning(
                "[FileCategorySync][update_only] detail update failed | job_id=%s db=%s err=%s",
                job_id,
                db_name,
                exc,
            )
    payload = {
        "status": "completed" if update_only else "running",
        "event": "file_category_update_completed" if update_only else "file_category_sync_completed",
        "scan_count": updated,
        "total_count": updated,
        "collection_count": updated,
        "save_count": updated,
        "h3": "crawl status",
        "file_category_sync": {
            "ok": bool(result.get("ok")),
            "preview_summary": dict(result.get("preview_summary") or {}),
            "stats": stats,
            "detail_update": detail_update,
        },
    }
    await update_state_only(job_id=job_id, account_name=db_name, payload=payload)
    await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=payload)
    logger.info(
        "[FileCategorySync] %s | job_id=%s db=%s chat_bot_id=%s stats=%s",
        "update_only_completed" if update_only else "completed",
        job_id,
        db_name,
        chat_bot_id,
        stats,
    )
    if update_only:
        result = dict(result or {})
        result["updated"] = updated
        result["field_save_counts"] = _empty_field_save_counts()
        result["field_save_counts"]["cate"] = updated
        result["detail_update"] = detail_update
    return result


async def _run_partial_internal_sequence(
    data: Dict[str, Any],
    background_tasks: BackgroundTasks,
    *,
    db_name: str,
    job_id: str,
    fields: List[str],
    data_by_field: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    ordered = [field for field in fields if field in {"title", "symmary", "content", "type", "cate"}]
    logger.info("[PartialSequence] start | job_id=%s db=%s fields=%s", job_id, db_name, ordered)
    results: List[Dict[str, Any]] = []
    aggregate_counts = _empty_field_save_counts()
    data["_partial_sequence_aggregate_counts"] = aggregate_counts
    await send_message_to_redis_sse(
        job_id=job_id,
        dbname=db_name,
        message={
            "status": "running",
            "event": "partial_sequence_started",
            "job_id": job_id,
            "account_name": db_name,
            "total_count": len(ordered),
            "scan_count": len(ordered),
            "collection_count": 0,
            "save_count": 0,
            "study_count": 0,
            "field_save_counts": _empty_field_save_counts(),
            "source": "partial_sequence",
            "partial_sequence_running": True,
            "message": "crawl status",
        },
    )

    for index, field in enumerate(ordered, start=1):
        step_data = dict((data_by_field or {}).get(field) or data)
        step_data["partial_update_fields"] = [field]
        step_data["_suppress_terminal_sse"] = True
        step_data["_partial_sequence_aggregate_counts"] = aggregate_counts
        step_data["_partial_sequence_running"] = True

        if field == "title":
            if await partial_title_change_enabled(step_data, dbname=db_name):
                from backend.shared.title_only_mode import run_title_only

                results.append(await run_title_only(step_data))
            else:
                results.append({"field_save_counts": _empty_field_save_counts(), "source": "title_only"})
        elif field == "symmary":
            from backend.shared.summary_only_mode import run_summary_only

            step_data["crawl_mode"] = "summary_only"
            step_data["summary_only"] = True
            results.append(await run_summary_only(step_data))
        elif field == "content":
            if _is_partial_content_relearn_request(step_data):
                from backend.shared.partial_content_relearn import load_partial_content_relearn_targets

                target_result = await load_partial_content_relearn_targets(
                    step_data,
                    db_name=db_name,
                    chat_bot_id=str(step_data.get("chat_bot_id") or ""),
                )
                targets = target_result.get("rows") if isinstance(target_result, dict) else []
                if targets:
                    step_data["start_urls_override"] = targets
                    step_data["pre_explored_start_urls_count"] = len(targets)
                    step_data["start_urls_override_source"] = "partial_content_relearn"
                    step_data["content_relearn_mode"] = True
                    step_data["crawl_mode"] = "crawling"
                    step_data["duplicate_repair_mode"] = "category"
                    step_data["duplicate_summary_mode"] = "off"
                    step_data["duplicate_title_mode"] = "off"
                    step_data["stream_matched_rules_only"] = False
                    step_data["enable_post_job_cate_update"] = bool(step_data.get("enable_post_job_cate_update", True))
                    await _schedule_and_monitor(step_data, background_tasks, None, db_name, job_id, step_data.get("chat_bot_id"))
                    task = getattr(crawler_state, "workflow_tasks", {}).get(job_id)
                    if task is not None:
                        await task
            else:
                targets = []
                logger.info(
                    "[PartialContent][Blocked] job_id=%s db=%s reason=partial_content_relearn_disabled",
                    job_id,
                    db_name,
                )
            counts = _empty_field_save_counts()
            counts["content"] = len(targets or [])
            results.append({"field_save_counts": counts, "source": "partial_content_relearn"})
        elif field == "type":
            step_data["type_postprocess_enabled"] = True
            results.append(await run_type_postprocess(step_data))
        elif field == "cate":
            if file_category_mode(step_data) == "category_only" or is_file_category_update_only_request(step_data):
                results.append(await _run_file_category_sync_for_request(step_data, update_only=True))
            else:
                results.append(await run_partial_category_postprocess(step_data))

        aggregate_counts = _merge_field_save_counts(results)
        data["_partial_sequence_aggregate_counts"] = aggregate_counts
        await send_message_to_redis_sse(
            job_id=job_id,
            dbname=db_name,
            message={
                "status": "running",
                "event": "partial_step_completed",
                "job_id": job_id,
                "account_name": db_name,
                "total_count": len(ordered),
                "scan_count": len(ordered),
                "collection_count": index,
                "save_count": index,
                "study_count": 0,
                "field_save_counts": aggregate_counts,
                "source": "partial_sequence",
                "partial_sequence_running": True,
                "partial_step": field,
                "message": f"부분 업데이트 {index}/{len(ordered)} 완료: {field}",
            },
        )
        for key in aggregate_counts:
            data["_partial_sequence_aggregate_counts"][key] = aggregate_counts.get(key, 0)

    merged_counts = _merge_field_save_counts(results)
    updated_total = sum(int(merged_counts.get(key, 0) or 0) for key in ("title", "content", "cate", "symmary", "type"))
    final_payload = {
        "status": "completed",
        "event": "workflow_completed",
        "job_id": job_id,
        "account_name": db_name,
        "total_count": len(ordered),
        "scan_count": len(ordered),
        "collection_count": updated_total,
        "save_count": updated_total,
        "study_count": 0,
        "updated_count": updated_total,
        "field_save_counts": merged_counts,
        "source": "partial_sequence",
        "partial_sequence_running": False,
        "partial_sequence": ordered,
        "message": "crawl status",
    }
    await update_state_only(job_id=job_id, account_name=db_name, payload=final_payload)
    await send_message_to_redis_sse(job_id=job_id, dbname=db_name, message=final_payload)
    return final_payload


# ==========================================
# 2. 실제 크롤링 worker: 요청 유형을 분기하고 워크플로우를 스케줄링한다.
# ==========================================
async def _crawl_file_worker(data: Dict[str, Any], background_tasks: BackgroundTasks):
    job_id, db_name = data.get("job_id"), resolve_db_name(data, default="dev_user")
    worker_t0 = time.perf_counter()
    _worker_colle = str(data.get("colle") or "").strip().lower()
    _worker_flow_log = logger.debug if _worker_colle == "file" else logger.info
    
    try:
        _worker_flow_log(
            "[BottleneckTrace][worker_entry] job_id=%s db=%s colle=%s elapsed_ms=0",
            job_id,
            db_name,
            _worker_colle,
        )
        # 이미 terminal 상태인 job이면 중복 실행을 막기 위해 worker를 종료한다.
        if job_id and _is_job_terminal(job_id):
            return

        if await _enqueue_partial_debounce(data, background_tasks, db_name=db_name, job_id=job_id):
            return

        ordered_partial_fields = _ordered_partial_internal_fields(data)
        if len(ordered_partial_fields) > 1:
            await _run_partial_internal_sequence(
                data,
                background_tasks,
                db_name=db_name,
                job_id=job_id,
                fields=ordered_partial_fields,
            )
            return

        if is_type_postprocess_request(data):
            await run_type_postprocess(data)
            return

        partial_fields = _partial_update_fields(data)
        partial_content_requested = (
            str(data.get("colle") or "").strip().lower() == "content"
            and "content" in partial_fields
        )
        _worker_flow_log(
            "[PartialCategory][WorkerDecision] job_id=%s db=%s decision=%s partial_fields=%s partial_content_requested=%s",
            job_id,
            db_name,
            partial_category_debug_reason(data),
            sorted(partial_fields),
            partial_content_requested,
        )

        partial_title_requested = is_partial_title_change_request(data)
        partial_title_config_enabled = await partial_title_change_enabled(data, dbname=db_name) if partial_title_requested else False
        partial_category_requested = is_partial_category_postprocess_request(data)
        partial_category_only_fields = "cate" in partial_fields and "content" not in partial_fields
        partial_summary_requested = _is_partial_summary_request(data)
        partial_summary_only_fields = bool(partial_fields & {"symmary", "summary"}) and not bool(
            partial_fields & {"title", "cate", "content"}
        )
        partial_content_relearn_requested = _is_partial_content_relearn_request(data)
        if partial_content_relearn_requested:
            logger.info(
                "[PartialContent][WorkerDecision] job_id=%s db=%s requested=%s fields=%s filter=%s",
                job_id,
                db_name,
                partial_content_relearn_requested,
                sorted(partial_fields),
                data.get("partial_target_filter") if isinstance(data.get("partial_target_filter"), dict) else {},
            )
            from backend.shared.partial_content_relearn import load_partial_content_relearn_targets

            target_result = await load_partial_content_relearn_targets(
                data,
                db_name=db_name,
                chat_bot_id=str(data.get("chat_bot_id") or ""),
            )
            targets = target_result.get("rows") if isinstance(target_result, dict) else []
            if not targets:
                await send_message_to_redis_sse(
                    job_id=job_id,
                    dbname=db_name,
                    message={
                        "status": "completed",
                        "event": "workflow_completed",
                        "job_id": job_id,
                        "account_name": db_name,
                        "total_count": 0,
                        "scan_count": 0,
                        "collection_count": 0,
                        "save_count": 0,
                        "study_count": 0,
                        "field_save_counts": {
                            "title": 0,
                            "content": 0,
                            "cate": 0,
                            "symmary": 0,
                            "type": 0,
                            "url": 0,
                            "web_de": 0,
                        },
                        "source": "partial_content_relearn",
                        "message": f"부분 본문 재학습 대상이 없습니다: {target_result.get('reason') if isinstance(target_result, dict) else 'empty'}",
                    },
                )
                return
            data["start_urls_override"] = targets
            data["pre_explored_start_urls_count"] = len(targets)
            data["start_urls_override_source"] = "partial_content_relearn"
            data["content_relearn_mode"] = True
            data["crawl_mode"] = "crawling"
            data["duplicate_repair_mode"] = "category"
            data["duplicate_summary_mode"] = "off"
            data["duplicate_title_mode"] = "off"
            data["stream_matched_rules_only"] = False
            data["enable_post_job_cate_update"] = bool(data.get("enable_post_job_cate_update", True))
            await send_message_to_redis_sse(
                job_id=job_id,
                dbname=db_name,
                message={
                    "status": "running",
                    "event": "partial_content_targets_loaded",
                    "job_id": job_id,
                    "account_name": db_name,
                    "total_count": len(targets),
                    "scan_count": len(targets),
                    "collection_count": 0,
                    "save_count": 0,
                    "study_count": 0,
                    "field_save_counts": {
                        "title": 0,
                        "content": 0,
                        "cate": 0,
                        "symmary": 0,
                        "type": 0,
                        "url": 0,
                        "web_de": 0,
                    },
                    "source": "partial_content_relearn",
                    "h3": "crawl status",
                    "message": f"partial content relearn targets={len(targets)}",
                },
            )
            logger.info(
                "[PartialContent][WorkerRoute] job_id=%s route=content_relearn start_urls=%s table=%s",
                job_id,
                len(targets),
                target_result.get("table") if isinstance(target_result, dict) else "",
            )
            await _schedule_and_monitor(data, background_tasks, None, db_name, job_id, data.get("chat_bot_id"))
            return
        if partial_summary_requested:
            logger.info(
                "[SummaryOnly][WorkerDecision] job_id=%s db=%s requested=%s only_fields=%s fields=%s content_requested=%s filter=%s",
                job_id,
                db_name,
                partial_summary_requested,
                partial_summary_only_fields,
                sorted(partial_fields),
                partial_content_requested,
                data.get("partial_target_filter") if isinstance(data.get("partial_target_filter"), dict) else {},
            )
        if partial_summary_requested and partial_summary_only_fields and not partial_content_requested:
            from backend.shared.summary_only_mode import run_summary_only

            data["crawl_mode"] = "summary_only"
            data["summary_only"] = True
            logger.info("[SummaryOnly][WorkerRoute] job_id=%s route=summary_only", job_id)
            await run_summary_only(data)
            return
        if (
            file_category_mode(data) == "category_only"
            and "cate" in partial_fields
            and not partial_content_requested
        ):
            logger.debug("[FileCategorySync][WorkerRoute] job_id=%s route=file_category_update_only", job_id)
            await _run_file_category_sync_for_request(data, update_only=True)
            return
        if partial_title_requested:
            logger.info(
                "[PartialTitle][WorkerDecision] job_id=%s db=%s requested=%s config_enabled=%s fields=%s content_requested=%s",
                job_id,
                db_name,
                partial_title_requested,
                partial_title_config_enabled,
                sorted(partial_fields),
                partial_content_requested,
            )

        if (
            partial_title_requested
            and partial_title_config_enabled
            and partial_category_requested
            and partial_category_only_fields
            and not partial_content_requested
        ):
            from backend.shared.title_only_mode import run_title_only

            title_data = dict(data)
            title_data["partial_update_fields"] = ["title"]
            title_data["_suppress_terminal_sse"] = True
            category_data = dict(data)
            category_data["partial_update_fields"] = partial_update_fields_without_title(data)
            logger.info(
                "[PartialRoute] job_id=%s route=title_then_category fields=%s",
                job_id,
                sorted(partial_fields),
            )
            await run_title_only(title_data)
            await run_partial_category_postprocess(category_data)
            return

        if partial_category_requested and not partial_title_requested and not partial_content_requested:
            logger.info("[PartialCategory][WorkerRoute] job_id=%s route=partial_category_only", job_id)
            await run_partial_category_postprocess(data)
            return

        if partial_title_requested and partial_title_config_enabled and not partial_content_requested:
            from backend.shared.title_only_mode import run_title_only

            logger.info("[PartialTitle][WorkerRoute] job_id=%s route=title_only", job_id)
            await run_title_only(data)
            return
        elif partial_title_requested and is_partial_title_only_request(data) and not partial_content_requested:
            await send_message_to_redis_sse(
                job_id=job_id,
                dbname=db_name,
                message={
                    "status": "completed",
                    "event": "workflow_completed",
                    "job_id": job_id,
                    "account_name": db_name,
                    "total_count": 0,
                    "collection_count": 0,
                    "save_count": 0,
                    "study_count": 0,
                    "source": "title_only",
                    "message": "sub_change=off: 제목 변경이 비활성화되어 제목만 처리하지 않고 종료합니다.",
                },
            )
            return
        elif partial_title_requested and not partial_title_config_enabled:
            data["partial_update_fields"] = partial_update_fields_without_title(data)
            if is_partial_category_postprocess_request(data) and not partial_content_requested:
                logger.info("[PartialCategory][WorkerRoute] job_id=%s route=partial_category_after_title_disabled", job_id)
                await run_partial_category_postprocess(data)
                return

        if is_file_category_update_only_request(data):
            await _run_file_category_sync_for_request(data, update_only=True)
            return

        # 탐색 시작 상태를 먼저 전송해 UI가 준비 단계에서 멈춘 것처럼 보이지 않게 한다.
        await send_message_to_redis_sse(
            job_id=job_id, dbname=db_name,
            message={
                "status": "running", "event": "exploring",
                "scan_count": 0, "total_count": 0,
                "h3": "crawl status",
            }
        )
        _worker_flow_log(
            "[BottleneckTrace][worker_exploring_sent] job_id=%s elapsed_ms=%s",
            job_id,
            int((time.perf_counter() - worker_t0) * 1000),
        )

        # 1. URL/요청 정보 준비
        prepare_t0 = time.perf_counter()
        _worker_flow_log(
            "[BottleneckTrace][prepare_start] job_id=%s elapsed_ms=%s",
            job_id,
            int((prepare_t0 - worker_t0) * 1000),
        )
        header_response, db_name, job_id, chat_bot_id = await _prepare_crawl(data)
        _worker_flow_log(
            "[BottleneckTrace][prepare_done] job_id=%s prepare_ms=%s elapsed_ms=%s start_urls=%s source=%s",
            job_id,
            int((time.perf_counter() - prepare_t0) * 1000),
            int((time.perf_counter() - worker_t0) * 1000),
            len(data.get("start_urls_override") or []) if isinstance(data.get("start_urls_override"), list) else 0,
            data.get("start_urls_override_source"),
        )

        from backend.shared.parsed_fields_only_mode import (
            is_duplicate_repair_only_request,
            run_duplicate_repair_only,
        )

        if is_duplicate_repair_only_request(data):
            await run_duplicate_repair_only(data)
            return

        if _is_summary_only_request(data):
            from backend.shared.summary_only_mode import run_summary_only

            await run_summary_only(data)
            return

        if _is_title_only_request(data):
            if is_partial_title_change_request(data) and not await partial_title_change_enabled(data, dbname=db_name):
                return
            from backend.shared.title_only_mode import run_title_only

            await run_title_only(data)
            return

        # prepare 이후에도 job이 terminal 상태가 되었으면 스케줄링 전에 종료한다.
        if job_id and _is_job_terminal(job_id):
            return
        
        urls = data.get("start_urls_override", [])
        total_found = int(data.get("pre_explored_start_urls_count") or len(urls or []))
        redis_total_found = _resolve_redis_exploration_count(data, total_found)
        _worker_flow_log("[CrawlStart] file worker start_urls override | job_id=%s count=%s sample=%s", job_id, total_found, (urls or [])[:3])
       
        if total_found > 0:
            await send_message_to_redis_sse(
                job_id=job_id, dbname=db_name,
                message={
                    "status": "running", 
                    "event": "exploring",
                    "scan_count": redis_total_found, # Redis/SSE display count
                    "total_count": redis_total_found,
                    "actual_scan_count": total_found,
                    "h3": "crawl status",
                }
            )

        # 2. 준비된 start_urls를 기반으로 워크플로우를 스케줄링한다.
        if _worker_colle != "file":
            _safe_print(f"[TIMING] start_urls 준비 완료(total={total_found})")
        schedule_t0 = time.perf_counter()
        _worker_flow_log(
            "[BottleneckTrace][schedule_start] job_id=%s elapsed_ms=%s total_found=%s",
            job_id,
            int((schedule_t0 - worker_t0) * 1000),
            total_found,
        )
        await _schedule_and_monitor(data, background_tasks, header_response, db_name, job_id, chat_bot_id)
        _worker_flow_log(
            "[BottleneckTrace][schedule_done] job_id=%s schedule_ms=%s elapsed_ms=%s",
            job_id,
            int((time.perf_counter() - schedule_t0) * 1000),
            int((time.perf_counter() - worker_t0) * 1000),
        )
        
    except Exception as e:
        logger.exception(f"Worker Error: {e}")
    finally:
        if hasattr(crawler_state, "active_worker_tasks"):
            crawler_state.active_worker_tasks.pop(job_id, None)
        try:
            _worker_flow_log(
                "%s[worker_finished] job_id=%s active=%s",
                CONCURRENT_CRAWL_LOG_PREFIX,
                job_id,
                crawler_state.get_workflow_debug_snapshot(),
            )
        except Exception:
            logger.debug("%s[worker_finished] snapshot failed", CONCURRENT_CRAWL_LOG_PREFIX, exc_info=True)
        try:
            await release_burst_dedupe_lock(job_id)
        except Exception:
            pass
        try:
            if hasattr(crawler_state, "active_animation_tasks"):
                crawler_state.active_animation_tasks.pop(job_id, None)
        except Exception:
            pass
        # worker가 종료될 때 남은 애니메이션/진행 상태 task를 정리해 다음 요청에 영향을 주지 않게 한다.
        try:
            if isinstance(animation_task, asyncio.Task) and not animation_task.done():
                animation_task.cancel()
        except Exception:
            pass
            
# ==========================================
# 4. 워크플로우 스케줄링 및 모니터링
# ==========================================
async def _schedule_and_monitor(data, background_tasks, header_response, db_name, job_id, chat_bot_id):
    """Schedule and monitor crawl workflow."""
    t0 = time.perf_counter()
    schedule_log = logger.info if (data or {}).get("learn_list_duplicate_exclude_result") else (logger.debug if str((data or {}).get("colle") or "").strip().lower() == "file" else logger.info)
    try:
        schedule_log(
            "[BottleneckTrace][dispatch_call_start] job_id=%s db=%s start_urls=%s source=%s",
            job_id,
            db_name,
            len((data or {}).get("start_urls_override") or [])
            if isinstance((data or {}).get("start_urls_override"), list)
            else 0,
            (data or {}).get("start_urls_override_source"),
        )
        return await dispatch_and_schedule_workflow(data, background_tasks, header_response=header_response)
    finally:
        schedule_log(
            "[BottleneckTrace][dispatch_call_done] job_id=%s dispatch_ms=%s",
            job_id,
            int((time.perf_counter() - t0) * 1000),
        )


def _force_direct_detail_enabled(data: Dict[str, Any]) -> bool:
    return str(
        data.get("probe_direct_detail")
        or data.get("file_probe_direct_detail")
        or data.get("force_direct_detail")
        or data.get("direct_detail")
        or data.get("single_detail_mode")
        or data.get("direct_detail_url")
        or ""
    ).strip().lower() in {"1", "true", "y", "yes", "on"}


_FILE_DIRECT_DETAIL_QUERY_KEYS = {
    "nttno",
    "nttid",
    "ntt_id",
    "articleno",
    "article_no",
    "postno",
    "post_no",
    "postid",
    "post_id",
    "docid",
    "doc_id",
    "idx",
    "seq",
    "num",
    "no",
}


def _is_file_direct_detail_url(url: str) -> bool:
    try:
        parsed = urlparse(ensure_url_scheme(str(url or "").strip()))
        path = (parsed.path or "").lower()
        query_keys = {
            str(key or "").strip().lower()
            for key, value in parse_qsl(parsed.query or "", keep_blank_values=False)
            if str(key or "").strip() and str(value or "").strip()
        }
    except Exception:
        path = str(url or "").lower()
        query_keys = set()
    if any(
        token in path
        for token in (
            "list.do",
            "list.jsp",
            "list.asp",
            "list.php",
            "list.html",
            "selectbbsnttlist.do",
            "bd_selectbbslist.do",
            "bd_selectnftcbbslist.do",
        )
    ):
        return False
    if not is_detail_page_url(url):
        return False
    # Generic View URLs with only board/menu ids are scopes, not concrete posts.
    if "view" in path and not (query_keys & _FILE_DIRECT_DETAIL_QUERY_KEYS):
        return False
    return True


async def _prepare_crawl(data: Dict[str, Any]):
    prepare_t0 = time.perf_counter()
    _normalize_file_crawl_route_hint(data, stage="prepare")
    data["stream_matched_rules_only"] = resolve_stream_matched_rules_only(data)
    db = resolve_db_name(data, default="dev_user") or "dev_user"
    contents_url = _resolve_primary_contents_url(data) or data.get("contents")
    target_domains = data.get("target_domains")
    requested_scope_path_prefix = _resolve_requested_scope_path_prefix(data)
    request_cate1, request_cate2 = _resolve_request_categories(data)
    start_urls_date_filter_enabled = _resolve_start_urls_date_filter_enabled(data)
    if ignore_period_enabled():
        start_urls_date_filter_enabled = False
        data["start_urls_target_date"] = None
    normalized_start_urls_target_date = _normalize_start_urls_target_date(
        data,
        enabled=start_urls_date_filter_enabled,
    )
    urls = []
    incoming_override_urls = (
        list(data.get("start_urls_override") or [])
        if isinstance(data.get("start_urls_override"), list) and data.get("start_urls_override")
        else []
    )
    incoming_override_source = str(data.get("start_urls_override_source") or "").strip()
    incoming_override_has_direct_attachments = any(
        isinstance(item, dict)
        and (
            isinstance(item.get("attachments"), list)
            or isinstance(item.get("direct_attachments"), list)
        )
        for item in incoming_override_urls
    )
    preserve_exact_override = bool(
        incoming_override_urls
        and (
            incoming_override_source in {"contents_detail_direct", "partial_content_relearn"}
            or (
                incoming_override_source in {"file_crawl_post_db", "file_crawl_post_db_stream"}
                and (bool(data.get("file_dashboard")) or incoming_override_has_direct_attachments)
            )
        )
    )
    prepare_flow_log = logger.debug if str(data.get("colle") or "").strip().lower() == "file" else logger.info
    prepare_flow_log(
        "[START_URLS_DATE_FILTER] request | job_id=%s colle=%s enabled=%s start_urls_target_date=%s crawl_target_date=%s",
        data.get("job_id"),
        str(data.get("colle") or "").strip().lower() or "board",
        start_urls_date_filter_enabled,
        normalized_start_urls_target_date or data.get("start_urls_target_date"),
        data.get("target_date"),
    )
    prepare_flow_log(
        "[START_URLS_SCOPE] request | job_id=%s colle=%s scope_path_prefix=%s contents_url=%s",
        data.get("job_id"),
        str(data.get("colle") or "").strip().lower() or "board",
        requested_scope_path_prefix,
        contents_url,
    )
    try:
        target_domains = data.get("target_domains")
        if isinstance(target_domains, str):
            target_domains = [x.strip() for x in target_domains.split(",") if x.strip()]

        _colle_lc = str(data.get("colle") or "").strip().lower()
        _flow_log = logger.debug if _colle_lc == "file" else logger.info
        load_t0 = time.perf_counter()
        _flow_log(
            "[BottleneckTrace][prepare_load_start] job_id=%s db=%s colle=%s source=%s elapsed_ms=%s",
            data.get("job_id"),
            db,
            _colle_lc or "board",
            "load_file_crawl_post_url_strings" if _colle_lc == "file" else "stream_asadal_urls_from_db",
            int((load_t0 - prepare_t0) * 1000),
        )
        if _colle_lc == "file":
            direct_detail_url = ensure_url_scheme(str(contents_url or "").strip()) if contents_url else ""
            force_direct_detail = _force_direct_detail_enabled(data)
            direct_detail_is_detail = bool(
                direct_detail_url and (force_direct_detail or _is_file_direct_detail_url(direct_detail_url))
            )
            if preserve_exact_override:
                urls = incoming_override_urls
                try:
                    existing_override_count = int(data.get("pre_explored_start_urls_count") or 0)
                except Exception:
                    existing_override_count = 0
                if direct_detail_url:
                    existing_url_keys = {
                        str((item.get("url") if isinstance(item, dict) else item) or "").strip().lower()
                        for item in (urls or [])
                    }
                    direct_detail_key = str(direct_detail_url or "").strip().lower()
                    if direct_detail_key and direct_detail_key not in existing_url_keys:
                        urls = [{"url": direct_detail_url, "type": "contents_scope", "source": "contents_url"}] + list(urls or [])
                data["start_urls_override"] = urls
                data["pre_explored_start_urls_count"] = max(existing_override_count, len(urls))
                data["selected_start_urls_count"] = len(urls)
                data["actual_start_urls_count"] = len(urls)
                data["start_urls_override_source"] = incoming_override_source
                data["file_crawl_stream_config"] = {}
                logger.info(
                    "[CrawlStart] preserved file start_urls_override | job_id=%s source=%s count=%s has_direct_attachments=%s sample=%s",
                    data.get("job_id"),
                    incoming_override_source,
                    len(urls),
                    incoming_override_has_direct_attachments,
                    urls[:3],
                )
            elif direct_detail_is_detail:
                direct_detail_in_scope = _direct_url_matches_requested_scope(
                    direct_detail_url,
                    requested_scope_path_prefix,
                )
                direct_detail_item = (
                    await build_direct_detail_start_url_item(
                        data,
                        direct_detail_url,
                        db_name=db,
                        chat_bot_id=data.get("chat_bot_id"),
                    )
                    if direct_detail_in_scope
                    else None
                )
                data["start_urls_override"] = [direct_detail_item] if direct_detail_item else []
                data["pre_explored_start_urls_count"] = 1 if direct_detail_in_scope else 0
                data["start_urls_override_source"] = "contents_detail_direct"
                data["file_crawl_stream_config"] = {}
                logger.debug(
                    "[CrawlStart] direct detail bypass | job_id=%s url=%s scope_path_prefix=%s in_scope=%s",
                    data.get("job_id"),
                    direct_detail_url,
                    requested_scope_path_prefix,
                    direct_detail_in_scope,
                )
                urls = [direct_detail_item] if direct_detail_item else []
            else:
                stream_target_domains = target_domains if isinstance(target_domains, list) else None
                stream_method = str(data.get("method") or "period")
                start_urls_order = _resolve_start_urls_order(data)
                learn_list_id_scope = _resolve_learn_list_id_scope(data)
                urls = await load_file_crawl_post_url_strings(
                    db_name=db,
                    target_domains=stream_target_domains,
                    contents_url=contents_url,
                    chat_bot_id=data.get("chat_bot_id"),
                    method=stream_method,
                    target_date=data.get("start_urls_target_date"),
                    exploration_date_filter_enabled=start_urls_date_filter_enabled,
                    scope_path_prefix=data.get("scope_path_prefix"),
                    start_urls_order=start_urls_order,
                    use_category_rules=False,
                    dedupe_urls=True,
                    learn_list_id_scope=learn_list_id_scope,
                    scope_by_contents_learn_list_id=True,
                )
                data["start_urls_override"] = urls
                data["pre_explored_start_urls_count"] = len(urls)
                data["start_urls_override_source"] = "file_crawl_post_db"
                data["file_crawl_stream_config"] = {}
                data["_file_start_urls_db_branch_applied"] = True
                if not urls:
                    data["_file_start_urls_db_branch_failure_reason"] = "file_learn_list_scope_start_urls_empty"
                    data["_file_start_urls_db_branch_failure_message"] = (
                        "learn_list_id에 매칭되는 exploration row를 찾지 못했습니다. URL scope fallback 또는 LEARN_LIST content 값을 확인하세요."
                    )
                    data["_file_start_urls_db_branch_failure_contents_url"] = contents_url
        else:
            direct_detail_url = ensure_url_scheme(str(contents_url or "").strip()) if contents_url else ""
            force_direct_detail = _force_direct_detail_enabled(data)
            direct_detail_is_detail = bool(
                direct_detail_url and (force_direct_detail or is_detail_page_url(direct_detail_url))
            )
            if preserve_exact_override:
                urls = incoming_override_urls
                try:
                    existing_override_count = int(data.get("pre_explored_start_urls_count") or 0)
                except Exception:
                    existing_override_count = 0
                data["start_urls_override"] = urls
                data["pre_explored_start_urls_count"] = max(existing_override_count, len(urls))
                data["selected_start_urls_count"] = len(urls)
                data["actual_start_urls_count"] = len(urls)
                data["start_urls_override_source"] = incoming_override_source
                logger.info(
                    "[CrawlStart] preserved exact start_urls_override | job_id=%s source=%s count=%s sample=%s",
                    data.get("job_id"),
                    incoming_override_source,
                    len(urls),
                    urls[:3],
                )
            elif direct_detail_is_detail:
                direct_detail_in_scope = _direct_url_matches_requested_scope(
                    direct_detail_url,
                    requested_scope_path_prefix,
                )
                direct_detail_item = (
                    await build_direct_detail_start_url_item(
                        data,
                        direct_detail_url,
                        db_name=db,
                        chat_bot_id=data.get("chat_bot_id"),
                    )
                    if direct_detail_in_scope
                    else None
                )
                urls = [direct_detail_item] if direct_detail_item else []
                data["start_urls_override"] = urls
                data["pre_explored_start_urls_count"] = 1 if direct_detail_in_scope else 0
                data["start_urls_override_source"] = "contents_detail_direct"
                logger.info(
                    "[CrawlStart] board direct detail bypass | job_id=%s url=%s scope_path_prefix=%s in_scope=%s",
                    data.get("job_id"),
                    direct_detail_url,
                    requested_scope_path_prefix,
                    direct_detail_in_scope,
                )
            else:
                async for url_batch in stream_asadal_urls_from_db(
                    db_name=db,
                    target_domains=target_domains if isinstance(target_domains, list) else None,
                    contents_url=contents_url,
                    chat_bot_id=data.get("chat_bot_id"),
                    cate1=request_cate1,
                    cate2=request_cate2,
                    target_date=data.get("start_urls_target_date"),
                    exploration_date_filter_enabled=start_urls_date_filter_enabled,
                    stream_matched_rules_only=resolve_stream_matched_rules_only(data),
                    scope_path_prefix=data.get("scope_path_prefix"),
                    start_urls_order=_resolve_start_urls_order(data),
                ):
                    if url_batch:
                        urls.extend(url_batch)
        _flow_log(
            "[BottleneckTrace][prepare_load_done] job_id=%s load_ms=%s elapsed_ms=%s urls=%s",
            data.get("job_id"),
            int((time.perf_counter() - load_t0) * 1000),
            int((time.perf_counter() - prepare_t0) * 1000),
            len(urls or []),
        )

    except Exception as e:
        logger.error(f"DB Stream Error: {e}")

    if str(data.get("colle") or "").strip().lower() != "file":
        data["start_urls_override"] = urls
    try:
        if (
            urls
            and not preserve_exact_override
            and not _force_direct_detail_enabled(data)
            and str(data.get("colle") or "").strip().lower() != "file"
        ):
            dedupe_t0 = time.perf_counter()
            _dedupe_flow_log = logger.debug if str(data.get("colle") or "").strip().lower() == "file" else logger.info
            _dedupe_flow_log(
                "[BottleneckTrace][prepare_dedupe_start] job_id=%s urls=%s elapsed_ms=%s",
                data.get("job_id"),
                len(urls or []),
                int((dedupe_t0 - prepare_t0) * 1000),
            )
            filtered_urls, learn_list_dedupe_meta = await apply_learn_list_start_url_dedupe(
                data=data,
                start_urls=urls,
                db_name=db,
                chat_bot_id=data.get("chat_bot_id"),
            )
            if learn_list_dedupe_meta.get("enabled"):
                urls = filtered_urls
                data["start_urls_override"] = urls
                try:
                    before_count = int(learn_list_dedupe_meta.get("before") or 0)
                    after_count = int(learn_list_dedupe_meta.get("after") or len(urls))
                    data["learn_list_duplicate_exclude_scan_count"] = before_count
                    data["learn_list_duplicate_exclude_selected_count"] = after_count
                    data["selected_start_urls_count"] = after_count
                    data["actual_start_urls_count"] = after_count
                    data["pre_explored_start_urls_count"] = max(
                        int(data.get("pre_explored_start_urls_count") or 0),
                        before_count,
                    )
                except Exception:
                    data["selected_start_urls_count"] = len(urls)
                    data["actual_start_urls_count"] = len(urls)
                    data["pre_explored_start_urls_count"] = max(
                        int(data.get("pre_explored_start_urls_count") or 0),
                        len(urls),
                    )
                data["learn_list_duplicate_exclude_result"] = learn_list_dedupe_meta
                logger.debug(
                    "[CrawlStart] learn-list duplicate exclude applied | job_id=%s before=%s after=%s duplicates=%s table=%s",
                    data.get("job_id"),
                    learn_list_dedupe_meta.get("before"),
                    learn_list_dedupe_meta.get("after"),
                    learn_list_dedupe_meta.get("duplicates"),
                    learn_list_dedupe_meta.get("table"),
                )
                try:
                    _before = int(learn_list_dedupe_meta.get("before") or 0)
                    _selected = int(learn_list_dedupe_meta.get("after") or len(urls or []))
                    _duplicates = int(learn_list_dedupe_meta.get("duplicates") or max(0, _before - _selected))
                    _excluded_pct = (_duplicates / _before * 100.0) if _before > 0 else 0.0
                except Exception:
                    _before = int(learn_list_dedupe_meta.get("before") or 0)
                    _selected = len(urls or [])
                    _duplicates = int(learn_list_dedupe_meta.get("duplicates") or 0)
                    _excluded_pct = 0.0
                logger.info(
                    "[CrawlStart] target URLs after duplicate exclude | before=%s duplicates=%s selected=%s excluded=%.1f%% job_id=%s db=%s chat_bot_id=%s",
                    _before,
                    _duplicates,
                    _selected,
                    _excluded_pct,
                    data.get("job_id"),
                    db,
                    data.get("chat_bot_id"),
                )
            logger.debug(
                "[BottleneckTrace][prepare_dedupe_done] job_id=%s dedupe_ms=%s elapsed_ms=%s before=%s after=%s enabled=%s",
                data.get("job_id"),
                int((time.perf_counter() - dedupe_t0) * 1000),
                int((time.perf_counter() - prepare_t0) * 1000),
                learn_list_dedupe_meta.get("before") if isinstance(learn_list_dedupe_meta, dict) else len(urls or []),
                len(urls or []),
                learn_list_dedupe_meta.get("enabled") if isinstance(learn_list_dedupe_meta, dict) else None,
            )
        elif urls and _force_direct_detail_enabled(data):
            (logger.debug if str(data.get("colle") or "").strip().lower() == "file" else logger.info)(
                "[CrawlStart] direct detail skips learn-list duplicate exclude | job_id=%s count=%s",
                data.get("job_id"),
                len(urls),
            )
    except Exception as exc:
        logger.warning(
            "[CrawlStart] learn-list duplicate exclude failed open | job_id=%s db=%s err=%s",
            data.get("job_id"),
            db,
            exc,
            exc_info=True,
        )
    if str(data.get("colle") or "").strip().lower() != "file":
        try:
            exploration_post_total = await count_exploration_post_urls(
                db_name=db,
                chat_bot_id=data.get("chat_bot_id"),
                target_domains=target_domains if isinstance(target_domains, list) else None,
                contents_url=contents_url,
                scope_path_prefix=data.get("scope_path_prefix"),
            )
        except Exception:
            exploration_post_total = 0
        if exploration_post_total > 0:
            data["exploration_post_total_count"] = int(exploration_post_total)
            data["exploration_display_count_fixed"] = True
            data["exploration_display_max_count"] = int(exploration_post_total)
            data["actual_start_urls_count"] = len(urls or [])
            if data.get("learn_list_duplicate_exclude_result"):
                data["pre_explored_start_urls_count"] = int(exploration_post_total)
                data["learn_list_duplicate_exclude_selected_count"] = len(urls or [])
                data["selected_start_urls_count"] = len(urls or [])
            logger.debug(
                "[CrawlStart] exploration display total fixed | job_id=%s db=%s chat_bot_id=%s post_total=%s actual_start_urls=%s target_count=%s",
                data.get("job_id"),
                db,
                data.get("chat_bot_id"),
                exploration_post_total,
                len(urls or []),
                int(data.get("pre_explored_start_urls_count") or len(urls or [])),
            )
    # dispatch에서 생성된 override start_urls를 data에 반영한다.
    if not data.get("start_urls_override_source"):
        data["start_urls_override_source"] = (
            "file_crawl_post_db" if str(data.get("colle") or "").strip().lower() == "file" else "pre_explored_db"
        )
    job_id = data.get("job_id", "")
    source_name = (
        "load_file_crawl_post_url_strings"
        if str(data.get("colle") or "").strip().lower() == "file"
        else "stream_asadal_urls_from_db"
    )
    final_flow_log = (
        logger.debug
        if str(data.get("colle") or "").strip().lower() == "file" or data.get("learn_list_duplicate_exclude_result")
        else logger.info
    )
    final_flow_log(
        "[CrawlStart] start_urls prepared | job_id=%s source=%s override_source=%s count=%s sample=%s",
        job_id,
        source_name,
        data.get("start_urls_override_source"),
        int(data.get("pre_explored_start_urls_count") or len(urls)),
        (urls or [])[:3],
    )
    final_flow_log(
        "[BottleneckTrace][prepare_final] job_id=%s total_ms=%s source=%s override_source=%s urls=%s",
        job_id,
        int((time.perf_counter() - prepare_t0) * 1000),
        source_name,
        data.get("start_urls_override_source"),
        len(urls or []),
    )
    if not data.get("colle"):
        data["colle"] = detect_board_crawl(data).get("mode", "file")
        
    return None, db, data.get("job_id"), data.get("chat_bot_id")

