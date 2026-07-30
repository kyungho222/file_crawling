from __future__ import annotations

import json
import logging
import asyncio
import time
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field
from redis.asyncio.client import Redis

from db.db_redis import get_redis
from backend.shared.crawl_trace import crawl_trace, elapsed_ms

try:
    from config import Config  # type: ignore
except Exception:  # pragma: no cover
    class _RedisSSEFallbackConfig:
        REDIS_JOB_META_TTL_SEC = 24 * 3600

    Config = _RedisSSEFallbackConfig()  # type: ignore

logger = logging.getLogger("redis_sse_service")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

STOP_STATUSES = {"stop", "stopped", "cancelled", "cancel", "coll_stop"}
COMPLETED_STATUSES = {"completed", "complete", "crawled", "ok", "finished"}


def _local_file_crawl_no_redis_enabled() -> bool:
    return str(os.getenv("LOCAL_FILE_CRAWL_NO_REDIS", "1") or "1").strip().lower() in {
        "1", "true", "yes", "y", "on"
    }

try:
    RUNNING_STATE_TTL = int(os.getenv("SSE_RUNNING_STATE_TTL_SEC", "86400") or "86400")
except Exception:
    RUNNING_STATE_TTL = 24 * 60 * 60
try:
    RUNNING_TTL_REFRESH_THRESHOLD = int(os.getenv("SSE_RUNNING_TTL_REFRESH_THRESHOLD_SEC", "3600") or "3600")
except Exception:
    RUNNING_TTL_REFRESH_THRESHOLD = 60 * 60
try:
    COMPLETED_STATE_TTL = int(os.getenv("SSE_COMPLETED_STATE_TTL_SEC", "86400") or "86400")
except Exception:
    COMPLETED_STATE_TTL = 24 * 60 * 60

# ??[?섏젙] SSE ?대깽??諛쒗뻾 鍮덈룄 ?쒗븳 ?⑥텞 (5珥?-> 0.2珥?
# ?좊땲硫붿씠???곗텧???ㅼ떆媛꾩쑝濡??ъ슜???붾㈃??諛섏쁺?섎룄濡??⑸땲??
try:
    SSE_RATE_LIMIT_INTERVAL = float(os.getenv("SSE_RATE_LIMIT_INTERVAL", "0.2"))
except Exception:
    SSE_RATE_LIMIT_INTERVAL = 0.2

_rate_limit_timestamps: Dict[str, float] = defaultdict(lambda: 0.0)
_rate_limit_lock = asyncio.Lock()
_pending_requests: Dict[str, "RedisSSEPublishRequest"] = {}
_pending_tasks: Dict[str, asyncio.Task] = {}
_pubsub_publish_tasks: set[asyncio.Task] = set()
_state_write_pending: Dict[str, Dict[str, Any]] = {}
_state_write_pending_events: int = 0
_state_write_flush_task: Optional[asyncio.Task] = None
_state_write_lock = asyncio.Lock()

_seq_by_job: Dict[str, int] = defaultdict(int)
_last_publish_meta: Dict[str, Dict[str, Any]] = defaultdict(dict)
_seq_lock = asyncio.Lock()
_smooth_target_by_job: Dict[str, Dict[str, Any]] = {}
_smooth_task_by_job: Dict[str, asyncio.Task] = {}

_COUNT_KEYS = (
    "scan_count", "total_count", "collection_count", "save_count", 
    "save_done_count", "save_success_count", "save_failed_count",
    "study_count", "study_done_count", "study_success_count",
    "study_failed_count", "study_skipped_count",
    "file_study_count", "file_study_done_count", "file_study_success_count",
    "file_study_failed_count", "file_study_skipped_count",
    "pending_collection_count", "pending_save_count", "error_count",
)

# --- ?좏떥由ы떚 諛?蹂묓빀 濡쒖쭅 (湲곗〈 ?좎?) ---
def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None: return default
        if isinstance(v, bool): return int(v)
        return int(float(str(v).strip()))
    except: return default

def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None: return default
        if isinstance(v, bool): return float(int(v))
        return float(str(v).strip())
    except: return default

def _extract_counts(d: Dict[str, Any]) -> Dict[str, int]:
    """移댁슫??異붿텧. scan_count/total_count???쒖そ留??덉쑝硫??쒕줈 梨꾩?.
    ?????덉쑝硫?洹몃?濡????먯깋 ?곗텧?먯꽌 scan_count < total_count ?덉슜)."""
    out: Dict[str, int] = {}
    for k in _COUNT_KEYS:
        if k in d: out[k] = _to_int(d.get(k), 0)
    try:
        if "scan_count" in out and "total_count" not in out:
            out["total_count"] = out["scan_count"]
        if "total_count" in out and "scan_count" not in out:
            out["scan_count"] = out["total_count"]
        # ?????덉쑝硫?max濡?留욎텛吏 ?딆쓬 ???먯깋 ?④퀎?먯꽌 0/1369媛 1369/1369濡?諛붾뚮뒗 寃?諛⑹?
    except: pass
    return out

def _progress_smoothing_enabled() -> bool:
    return str(os.getenv("SSE_PROGRESS_SMOOTH_ENABLED", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

def _smooth_interval_seconds() -> float:
    try:
        return max(0.05, min(float(os.getenv("SSE_PROGRESS_SMOOTH_INTERVAL_SEC", "0.2") or "0.2"), 2.0))
    except Exception:
        return 0.2

def _smooth_fraction() -> float:
    try:
        return max(0.05, min(float(os.getenv("SSE_PROGRESS_SMOOTH_FRACTION", "0.35") or "0.35"), 1.0))
    except Exception:
        return 0.35

def _smooth_min_step() -> int:
    try:
        return max(1, min(int(os.getenv("SSE_PROGRESS_SMOOTH_MIN_STEP", "1") or "1"), 1000))
    except Exception:
        return 1

def _smooth_max_step() -> int:
    try:
        return max(1, min(int(os.getenv("SSE_PROGRESS_SMOOTH_MAX_STEP", "25") or "25"), 100000))
    except Exception:
        return 25

def _is_terminal_message(message: Dict[str, Any]) -> bool:
    return _normalize_status_for_sse(message.get("status")) in {"completed", "cancelled", "error"}


def _is_unit_progress_message(message: Dict[str, Any]) -> bool:
    event = str((message or {}).get("event") or "").strip().lower()
    return event in {"title_only_progress", "partial_category_progress", "summary_only_progress"}

def _smooth_next_int(prev: int, target: int) -> int:
    if target <= prev:
        return target
    delta = target - prev
    step = max(_smooth_min_step(), int(delta * _smooth_fraction()))
    step = min(step, _smooth_max_step(), delta)
    return prev + step

def _apply_progress_smoothing(job_id: str, message: Dict[str, Any]) -> tuple[Dict[str, Any], bool]:
    if not _progress_smoothing_enabled() or _is_terminal_message(message) or _is_unit_progress_message(message):
        _smooth_target_by_job.pop(job_id, None)
        task = _smooth_task_by_job.pop(job_id, None)
        if task is not None and not task.done():
            task.cancel()
        return message, False

    prev_msg = _last_publish_meta.get(job_id, {}).get("message") or {}
    if not prev_msg:
        return message, False

    smoothed = dict(message)
    needs_more = False
    prev_counts = _extract_counts(prev_msg)
    target_counts = _extract_counts(message)
    for key, target in target_counts.items():
        if key in {"scan_count", "total_count"}:
            continue
        prev = prev_counts.get(key, target)
        if target > prev:
            next_value = _smooth_next_int(prev, target)
            smoothed[key] = next_value
            if next_value < target:
                needs_more = True
        elif target < prev:
            smoothed[key] = target

    try:
        prev_pct = _to_float(prev_msg.get("progress_percentage"), 0.0)
        target_pct = _to_float(message.get("progress_percentage"), 0.0)
        if target_pct > prev_pct:
            next_pct = prev_pct + max(0.1, min((target_pct - prev_pct) * _smooth_fraction(), 10.0))
            next_pct = min(next_pct, target_pct)
            smoothed["progress_percentage"] = round(next_pct, 2)
            if next_pct < target_pct:
                needs_more = True
        elif target_pct < prev_pct:
            smoothed["progress_percentage"] = target_pct
    except Exception:
        pass

    if needs_more:
        target_message = dict(message)
        target_message["smooth_target"] = True
        _smooth_target_by_job[job_id] = target_message
    else:
        _smooth_target_by_job.pop(job_id, None)
    return smoothed, needs_more

def _payload_and_extra_from_message(message: Dict[str, Any]) -> tuple[RedisSSEPayload, Dict[str, Any]]:
    payload = _build_payload(message)
    extra = {k: v for k, v in message.items() if k not in RedisSSEPayload.model_fields}
    return payload, extra

def _schedule_smooth_frame(job_id: str, account_name: str) -> None:
    if not _progress_smoothing_enabled():
        return
    existing = _smooth_task_by_job.get(job_id)
    if existing is not None and not existing.done():
        return
    async def _run() -> None:
        await asyncio.sleep(_smooth_interval_seconds())
        target = _smooth_target_by_job.get(job_id)
        if not target:
            return
        payload, extra = _payload_and_extra_from_message(target)
        await publish_sse_event(
            RedisSSEPublishRequest(job_id=job_id, account_name=account_name, payload=payload, extra=extra),
            bypass_throttle=True,
        )
    try:
        _smooth_task_by_job[job_id] = asyncio.create_task(_run(), name=f"sse-progress-smooth:{job_id}")
    except Exception:
        pass

def _merge_monotonic(prev: Dict[str, Any], cur: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(cur)
    prev_was_exploring = str((prev or {}).get("event") or "").strip().lower() == "exploring"
    prev_total_zero = _to_int((prev or {}).get("total_count"), -1) == 0
    is_exploring = str(cur.get("event") or "").strip().lower() == "exploring"
    # ?먯깋 ?곗텧 以?total_count==0)???뚰겕?뚮줈媛 scan_count=20 ?깆쑝濡?蹂대궡硫??곗텧???誘濡? 洹몃룞?덉? ?곗텧 媛??좎?
    animation_phase = False
    prev_counts = _extract_counts(prev or {})
    cur_counts = _extract_counts(cur or {})
    allow_scan_count_decrease = bool(cur.get("allow_scan_count_decrease"))
    skip_keys = {"scan_count", "total_count"} if (is_exploring or allow_scan_count_decrease) else set()
    if bool(cur.get("allow_counter_decrease")):
        skip_keys.update(_extract_counts(cur or {}).keys())
    for k, pv in prev_counts.items():
        if k in skip_keys:
            continue
        cv = cur_counts.get(k, 0)
        if cv < pv: merged[k] = pv
    try:
        pv = _to_float((prev or {}).get("progress_percentage"), 0.0)
        cv = _to_float((cur or {}).get("progress_percentage"), 0.0)
        if cv < pv: merged["progress_percentage"] = pv
    except: pass
    try:
        prev_status = _normalize_status_for_sse((prev or {}).get("status"))
        cur_status = _normalize_status_for_sse((cur or {}).get("status"))
        partial_sequence_running = str(cur.get("partial_sequence_running") or "").strip().lower() in {"1", "true", "yes", "on", "y"}
        if prev_status in {"completed", "cancelled", "error"} and cur_status == "running" and not partial_sequence_running:
            merged["status"] = prev_status
    except: pass
    try:
        prev_field_counts = (prev or {}).get("field_save_counts")
        cur_field_counts = cur.get("field_save_counts")
        if isinstance(prev_field_counts, str) and prev_field_counts.strip():
            prev_field_counts = json.loads(prev_field_counts)
        if isinstance(cur_field_counts, str) and cur_field_counts.strip():
            cur_field_counts = json.loads(cur_field_counts)
        if isinstance(prev_field_counts, dict) or isinstance(cur_field_counts, dict):
            merged_field_counts: Dict[str, int] = {}
            prev_dict = prev_field_counts if isinstance(prev_field_counts, dict) else {}
            cur_dict = cur_field_counts if isinstance(cur_field_counts, dict) else {}
            for key in ("title", "content", "cate", "symmary", "type", "url", "web_de"):
                pv = _to_int(prev_dict.get(key), 0)
                cv = _to_int(cur_dict.get(key), 0)
                merged_field_counts[key] = max(pv, cv)
            merged["field_save_counts"] = merged_field_counts
    except Exception:
        pass
    for k in ("h3", "subject", "details", "colle"):
        if k in (prev or {}) and (k not in merged or str(merged.get(k) or "").strip() == ""):
            merged[k] = (prev or {}).get(k)
    return merged

# --- ?곗씠??紐⑤뜽 諛??곹깭 ?뺢퇋??(湲곗〈 ?좎?) ---
class RedisSSEPayload(BaseModel):
    status: str = Field(default="running")
    total_count: int = 0
    collection_count: int = 0
    save_count: int = 0
    progress_percentage: float = 0.0
    timestamp: Optional[str] = None

class RedisSSEPublishRequest(BaseModel):
    job_id: str
    account_name: Optional[str] = None
    payload: RedisSSEPayload = Field(default_factory=RedisSSEPayload)
    extra: Dict[str, Any] = Field(default_factory=dict)

class RedisSSEPublishResponse(BaseModel):
    job_id: str
    account_name: Optional[str]
    published: bool = False
    state_updated: bool = False

def _now_iso() -> str: return datetime.now(timezone.utc).isoformat()
def _channel_name(account_name: str, job_id: str) -> str: return f"crawl:{account_name}:{job_id}:progress"
def _state_key(account_name: str, job_id: str) -> str: return f"crawl:{account_name}:{job_id}:state"
def _normalize_status_for_sse(status: Optional[str]) -> str:
    normalized = (status or "running").strip().lower()
    if normalized in STOP_STATUSES: return "cancelled"
    if normalized in COMPLETED_STATUSES: return "completed"
    if normalized in {"error", "failed", "fail"}: return "error"
    return "running"

def _redis_publish_timeout_sec() -> float:
    try:
        value = float(os.getenv("SSE_REDIS_PUBLISH_OP_TIMEOUT_SECONDS", "1.5") or "1.5")
    except Exception:
        value = 1.5
    return max(0.1, min(value, 10.0))

def _redis_state_timeout_sec() -> float:
    try:
        value = float(os.getenv("SSE_REDIS_STATE_OP_TIMEOUT_SECONDS", "3.0") or "3.0")
    except Exception:
        value = 3.0
    return max(0.1, min(value, 10.0))

def _redis_state_batch_enabled() -> bool:
    return str(os.getenv("SSE_REDIS_STATE_BATCH_ENABLED", "1") or "1").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

def _redis_state_batch_size() -> int:
    try:
        return max(1, min(int(os.getenv("SSE_REDIS_STATE_BATCH_SIZE", "6") or "6"), 100))
    except Exception:
        return 6

def _redis_state_batch_wait_sec() -> float:
    try:
        wait_ms = float(os.getenv("SSE_REDIS_STATE_BATCH_WAIT_MS", "100") or "100")
    except Exception:
        wait_ms = 100.0
    return max(0.0, min(wait_ms / 1000.0, 2.0))


def _redis_state_min_write_interval_sec() -> float:
    try:
        value = float(os.getenv("SSE_REDIS_STATE_MIN_WRITE_INTERVAL_SEC", "5.0") or "5.0")
    except Exception:
        value = 5.0
    return max(0.0, min(value, 60.0))

def _redis_get_timeout_sec() -> float:
    try:
        value = float(os.getenv("SSE_REDIS_GET_CLIENT_TIMEOUT_SECONDS", "2.0") or "2.0")
    except Exception:
        value = 2.0
    return max(0.1, min(value, 10.0))


def _redis_state_write_retry_limit() -> int:
    try:
        value = int(os.getenv("SSE_REDIS_STATE_WRITE_RETRY_LIMIT", "2") or "2")
    except Exception:
        value = 2
    return max(0, min(value, 10))


def _redis_state_write_retry_delay_sec() -> float:
    try:
        value = float(os.getenv("SSE_REDIS_STATE_WRITE_RETRY_DELAY_MS", "250") or "250") / 1000.0
    except Exception:
        value = 0.25
    return max(0.0, min(value, 5.0))

def _redis_publish_async_enabled() -> bool:
    return str(os.getenv("SSE_REDIS_PUBLISH_ASYNC", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _redis_state_update_on_throttle_enabled() -> bool:
    return str(os.getenv("SSE_REDIS_STATE_UPDATE_ON_THROTTLE", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

def _redis_pubsub_task_limit() -> int:
    try:
        value = int(os.getenv("SSE_REDIS_PUBSUB_TASK_LIMIT", "100") or "100")
    except Exception:
        value = 100
    return max(1, min(value, 1000))

async def _run_pubsub_publish_task(
    *,
    redis_client: Redis,
    channel: str,
    message: Dict[str, Any],
    job_id: str,
    account_name: str,
    status: str,
) -> None:
    publish_t0 = time.perf_counter()
    try:
        pub_res = await asyncio.wait_for(
            redis_client.publish(channel, json.dumps(message, ensure_ascii=False)),
            timeout=_redis_publish_timeout_sec(),
        )
        publish_ms = (time.perf_counter() - publish_t0) * 1000.0
        try:
            slow_ms = float(os.getenv("REDIS_SSE_STEP_SLOW_MS", "500") or "500")
        except Exception:
            slow_ms = 500.0
        crawl_trace(
            logger,
            phase="redis",
            action="pubsub_publish",
            state="slow" if publish_ms >= max(0.0, slow_ms) else "end",
            level=logging.INFO,
            job_id=job_id,
            elapsed_ms=publish_ms,
            account=account_name,
            status=status,
            channel=channel,
            published=bool(pub_res),
            mode="async_task",
            slow_ms=slow_ms,
        )
    except asyncio.TimeoutError:
        crawl_trace(
            logger,
            phase="redis",
            action="pubsub_publish",
            state="timeout",
            job_id=job_id,
            level=logging.WARNING,
            elapsed_ms=(time.perf_counter() - publish_t0) * 1000.0,
            account=account_name,
            status=status,
            channel=channel,
            timeout_sec=_redis_publish_timeout_sec(),
            mode="async_task",
        )
    except Exception as exc:
        crawl_trace(
            logger,
            phase="redis",
            action="pubsub_publish",
            state="fail",
            job_id=job_id,
            level=logging.WARNING,
            elapsed_ms=(time.perf_counter() - publish_t0) * 1000.0,
            account=account_name,
            status=status,
            channel=channel,
            error=exc,
            mode="async_task",
        )

def _schedule_pubsub_publish(
    *,
    redis_client: Redis,
    channel: str,
    message: Dict[str, Any],
    job_id: str,
    account_name: str,
    status: str,
) -> bool:
    active = {task for task in _pubsub_publish_tasks if task and not task.done()}
    _pubsub_publish_tasks.clear()
    _pubsub_publish_tasks.update(active)
    limit = _redis_pubsub_task_limit()
    if len(_pubsub_publish_tasks) >= limit:
        crawl_trace(
            logger,
            phase="redis",
            action="pubsub_publish",
            state="drop",
            job_id=job_id,
            level=logging.WARNING,
            counts={"active_tasks": len(_pubsub_publish_tasks), "limit": limit},
            account=account_name,
            status=status,
            channel=channel,
            mode="async_task",
        )
        return False
    task = asyncio.create_task(
        _run_pubsub_publish_task(
            redis_client=redis_client,
            channel=channel,
            message=dict(message or {}),
            job_id=job_id,
            account_name=account_name,
            status=status,
        ),
        name=f"redis-pubsub:{job_id}",
    )
    _pubsub_publish_tasks.add(task)

    def _cleanup(done: asyncio.Task) -> None:
        _pubsub_publish_tasks.discard(done)
        try:
            if not done.cancelled():
                done.exception()
        except Exception:
            pass

    task.add_done_callback(_cleanup)
    return True

async def _resolve_db_name(redis_client: Redis, job_id: str, provided: Optional[str]) -> Optional[str]:
    if provided: return provided
    try:
        meta = await redis_client.hgetall(f"job_meta:{job_id}")
        return meta.get("dbname") or meta.get(b"dbname", b"").decode()
    except: return None

def _build_message(request: RedisSSEPublishRequest, account_name: str) -> dict:
    payload_dict = request.payload.model_dump()
    payload_dict.update({"timestamp": _now_iso(), "account_name": account_name, "job_id": request.job_id})
    payload_dict["status"] = _normalize_status_for_sse(payload_dict.get("status"))
    if request.extra: payload_dict.update(request.extra)
    _ensure_field_save_counts(payload_dict)
    return payload_dict

def _ensure_field_save_counts(message: Dict[str, Any]) -> None:
    current = message.get("field_save_counts")
    if isinstance(current, dict):
        counts = dict(current)
    else:
        counts = {}
    aggregate = message.get("_partial_sequence_aggregate_counts")
    if isinstance(aggregate, str) and aggregate.strip():
        try:
            aggregate = json.loads(aggregate)
        except Exception:
            aggregate = {}
    if isinstance(aggregate, dict):
        for key, value in aggregate.items():
            try:
                counts[key] = max(_to_int(counts.get(key), 0), _to_int(value, 0))
            except Exception:
                pass
    fallback = _to_int(
        message.get("updated_count", message.get("save_count", message.get("collection_count", message.get("total_count", 0)))),
        0,
    )
    source = str(message.get("source") or "").strip().lower()
    focused_key = ""
    if source == "title_only":
        focused_key = "title"
    elif source == "partial_category_postprocess":
        focused_key = "cate"
    elif source == "summary_only":
        focused_key = "symmary"
    elif source == "partial_content_relearn":
        focused_key = "content"
    elif source == "type_postprocess":
        focused_key = "type"
    if focused_key:
        counts.setdefault(focused_key, fallback)
    for key in ("title", "content", "cate", "symmary", "type", "url", "web_de"):
        counts.setdefault(key, 0 if focused_key else fallback)
    message["field_save_counts"] = counts

def _sync_scan_and_scan_count(message: Dict[str, Any]) -> None:
    """SSE 硫붿떆吏?먯꽌 scan怨?scan_count瑜??숈씪 媛믪쑝濡?留욎땄 (?꾨줎???덇굅???명솚)."""
    raw = message.get("scan_count")
    if raw in (None, ""):
        raw = message.get("scan")
    if raw in (None, ""):
        raw = message.get("total_count")
    scan_val = _to_int(raw, 0)
    message["scan_count"] = scan_val
    message["scan"] = scan_val

def _redis_state_mapping(message: Dict[str, Any]) -> Dict[str, str]:
    try:
        source = str((message or {}).get("source") or "").strip()
        event = str((message or {}).get("event") or "").strip()
        collection = _to_int((message or {}).get("collection_count"), 0)
        counts = (message or {}).get("field_save_counts")
        title_count = _to_int((counts or {}).get("title"), 0) if isinstance(counts, dict) else 0
        if source == "title_only" and collection > 0 and title_count == 0:
            logger.warning(
                "[RedisSSE][ZeroDebug] state write title count zero | job_id=%s event=%s collection=%s updated=%s field_counts=%s message=%s",
                (message or {}).get("job_id"),
                event,
                (message or {}).get("collection_count"),
                (message or {}).get("updated_count"),
                counts,
                (message or {}).get("message"),
            )
    except Exception:
        pass
    out: Dict[str, str] = {}
    for key, value in message.items():
        if isinstance(value, (dict, list)):
            try:
                out[key] = json.dumps(value, ensure_ascii=False)
                continue
            except Exception:
                pass
        out[key] = str(value)
    return out


def _count_snapshot(message: Dict[str, Any]) -> Dict[str, Any]:
    counts = {
        "scan_count": _to_int(message.get("scan_count", message.get("total_count", 0)), 0),
        "total_count": _to_int(message.get("total_count", message.get("scan_count", 0)), 0),
        "collection_count": _to_int(message.get("collection_count", 0), 0),
        "save_count": _to_int(message.get("save_count", 0), 0),
        "study_count": _to_int(message.get("study_count", 0), 0),
    }
    field_counts = message.get("field_save_counts")
    if isinstance(field_counts, dict):
        counts["field_save_counts"] = dict(field_counts)
    return counts


def _should_skip_state_write(job_id: str, message: Dict[str, Any]) -> bool:
    if _is_terminal_message(message):
        return False
    min_interval = _redis_state_min_write_interval_sec()
    if min_interval <= 0:
        return False
    prev_meta = _last_publish_meta.get(job_id) or {}
    prev_message = prev_meta.get("message") or {}
    try:
        last_ts = float(prev_meta.get("state_write_scheduled_at_ts") or prev_meta.get("updated_at_ts") or 0.0)
    except Exception:
        last_ts = 0.0
    if last_ts <= 0 or (time.time() - last_ts) >= min_interval:
        return False
    if _normalize_status_for_sse(prev_message.get("status")) != _normalize_status_for_sse(message.get("status")):
        return False
    if str(prev_message.get("event") or "") != str(message.get("event") or ""):
        return False
    return _count_snapshot(prev_message) == _count_snapshot(message)

# --- 諛쒗뻾 諛??낅뜲?댄듃 濡쒖쭅 (湲곗〈 ?좎?) ---
async def update_state_only(*, job_id: str, account_name: str, payload: Dict[str, Any], extra: Optional[Dict[str, Any]] = None) -> bool:
    if _local_file_crawl_no_redis_enabled() and str(job_id or "").startswith("local-file-crawl-"):
        _last_publish_meta[job_id] = {"message": dict(payload or {}), "counts": _count_snapshot(payload or {}), "state_key": "", "updated_at_ts": time.time()}
        return True
    state_t0 = time.perf_counter()
    try:
        redis_client: Redis = await asyncio.wait_for(get_redis(), timeout=_redis_get_timeout_sec())
        message = _build_message(RedisSSEPublishRequest(job_id=job_id, account_name=account_name, payload=RedisSSEPayload(**payload), extra=extra or {}), account_name)
        prev_msg = _last_publish_meta.get(job_id, {}).get("message") or {}
        message = _merge_monotonic(prev_msg, message)
        message, _needs_more = _apply_progress_smoothing(job_id, message)
        _sync_scan_and_scan_count(message)
        state_key = _state_key(account_name, job_id)
        pipe = redis_client.pipeline(transaction=False)
        pipe.hset(state_key, mapping=_redis_state_mapping(message))
        pipe.expire(state_key, COMPLETED_STATE_TTL if _normalize_status_for_sse(message.get("status")) != "running" else RUNNING_STATE_TTL)
        await asyncio.wait_for(pipe.execute(), timeout=_redis_state_timeout_sec())
        _last_publish_meta[job_id] = {"message": message, "counts": _count_snapshot(message), "state_key": state_key, "updated_at_ts": time.time()}
        state_ms = elapsed_ms(state_t0)
        try:
            slow_ms = float(os.getenv("REDIS_SSE_STATE_SLOW_MS", "500") or "500")
        except Exception:
            slow_ms = 500.0
        crawl_trace(
            logger,
            phase="redis",
            action="state_update",
            state="slow" if state_ms >= slow_ms else "end",
            level=logging.DEBUG,
            job_id=job_id,
            elapsed_ms=state_ms,
            account=account_name,
            status=message.get("status"),
            state_key=state_key,
        )
        return True
    except Exception as exc:
        crawl_trace(
            logger,
            phase="redis",
            action="state_update",
            state="fail",
            job_id=job_id,
            level=logging.DEBUG,
            elapsed_ms=elapsed_ms(state_t0),
            account=account_name,
            error=exc,
        )
        logger.debug("[RedisSSE] state update skipped | job_id=%s err=%s", job_id, exc)
        return False


async def _write_state_for_publish(
    *,
    redis_client: Redis,
    job_id: str,
    account_name: str,
    message: Dict[str, Any],
    state_key: str,
) -> bool:
    state_t0 = time.perf_counter()
    try:
        pipe = redis_client.pipeline(transaction=False)
        pipe.hset(state_key, mapping=_redis_state_mapping(message))
        pipe.expire(
            state_key,
            COMPLETED_STATE_TTL
            if _normalize_status_for_sse(message.get("status")) != "running"
            else RUNNING_STATE_TTL,
        )
        await asyncio.wait_for(pipe.execute(), timeout=_redis_state_timeout_sec())
        state_ms = elapsed_ms(state_t0)
        try:
            slow_ms = float(os.getenv("REDIS_SSE_STATE_SLOW_MS", "500") or "500")
        except Exception:
            slow_ms = 500.0
        crawl_trace(
            logger,
            phase="redis",
            action="async_state_write",
            state="slow" if state_ms >= slow_ms else "end",
            level=logging.DEBUG,
            job_id=job_id,
            elapsed_ms=state_ms,
            account=account_name,
            status=(message or {}).get("status"),
            state_key=state_key,
        )
        return True
    except Exception as exc:
        crawl_trace(
            logger,
            phase="redis",
            action="async_state_write",
            state="fail",
            job_id=job_id,
            level=logging.DEBUG,
            elapsed_ms=elapsed_ms(state_t0),
            account=account_name,
            error=repr(exc),
            error_type=type(exc).__name__,
            state_key=state_key,
            status=(message or {}).get("status"),
        )
        logger.debug(
            "[RedisSSE] async state write failed | job_id=%s account=%s status=%s state_key=%s err_type=%s err=%r",
            job_id,
            account_name,
            (message or {}).get("status"),
            state_key,
            type(exc).__name__,
            exc,
        )
        return False


async def _flush_state_write_batch(*, delay_sec: float = 0.0) -> None:
    global _state_write_flush_task, _state_write_pending_events
    task_started_at = time.perf_counter()
    sleep_ms = 0.0
    sleep_overrun_ms = 0.0
    if delay_sec > 0:
        try:
            sleep_started_at = time.perf_counter()
            await asyncio.sleep(delay_sec)
            sleep_ms = elapsed_ms(sleep_started_at)
            sleep_overrun_ms = max(0.0, sleep_ms - (delay_sec * 1000.0))
        except asyncio.CancelledError:
            return

    batch_size = _redis_state_batch_size()
    lock_wait_started_at = time.perf_counter()
    async with _state_write_lock:
        lock_wait_ms = elapsed_ms(lock_wait_started_at)
        batch = list(_state_write_pending.values())[:batch_size]
        for item in batch:
            _state_write_pending.pop(str(item.get("state_key") or ""), None)
        event_count = _state_write_pending_events
        _state_write_pending_events = 0
        has_more = bool(_state_write_pending)
        _state_write_flush_task = None

    if not batch:
        return

    state_t0 = time.perf_counter()
    now_perf = time.perf_counter()
    queued_ages_ms = []
    scheduled_ages_ms = []
    enqueue_task_lag_ms_values = []
    enqueue_lock_wait_ms_values = []
    for item in batch:
        try:
            enqueued_at = float(item.get("enqueued_at_perf") or 0.0)
        except Exception:
            enqueued_at = 0.0
        if enqueued_at > 0:
            queued_ages_ms.append((now_perf - enqueued_at) * 1000.0)
        try:
            scheduled_at = float(item.get("scheduled_at_perf") or 0.0)
        except Exception:
            scheduled_at = 0.0
        if scheduled_at > 0:
            scheduled_ages_ms.append((now_perf - scheduled_at) * 1000.0)
        try:
            enqueue_task_lag_ms_values.append(float(item.get("enqueue_task_lag_ms") or 0.0))
        except Exception:
            pass
        try:
            enqueue_lock_wait_ms_values.append(float(item.get("enqueue_lock_wait_ms") or 0.0))
        except Exception:
            pass
    oldest_queue_wait_ms = max(queued_ages_ms) if queued_ages_ms else None
    newest_queue_wait_ms = min(queued_ages_ms) if queued_ages_ms else None
    oldest_schedule_wait_ms = max(scheduled_ages_ms) if scheduled_ages_ms else None
    enqueue_task_lag_ms = max(enqueue_task_lag_ms_values) if enqueue_task_lag_ms_values else 0.0
    enqueue_lock_wait_ms = max(enqueue_lock_wait_ms_values) if enqueue_lock_wait_ms_values else 0.0
    flush_start_lag_ms = elapsed_ms(task_started_at)
    redis_get_ms = 0.0
    pipe_build_ms = 0.0
    pipe_execute_ms = 0.0
    timeout_overrun_ms = 0.0
    redis_client = batch[0].get("redis_client")
    try:
        if redis_client is None:
            get_t0 = time.perf_counter()
            redis_client = await asyncio.wait_for(get_redis(), timeout=_redis_get_timeout_sec())
            redis_get_ms = elapsed_ms(get_t0)
        build_t0 = time.perf_counter()
        pipe = redis_client.pipeline(transaction=False)
        for item in batch:
            message = dict(item.get("message") or {})
            state_key = str(item.get("state_key") or "")
            if not state_key:
                continue
            pipe.hset(state_key, mapping=_redis_state_mapping(message))
            pipe.expire(
                state_key,
                COMPLETED_STATE_TTL
                if _normalize_status_for_sse(message.get("status")) != "running"
                else RUNNING_STATE_TTL,
            )
        pipe_build_ms = elapsed_ms(build_t0)
        execute_t0 = time.perf_counter()
        await asyncio.wait_for(pipe.execute(), timeout=_redis_state_timeout_sec())
        pipe_execute_ms = elapsed_ms(execute_t0)
        timeout_overrun_ms = max(0.0, pipe_execute_ms - (_redis_state_timeout_sec() * 1000.0))
        state_ms = elapsed_ms(state_t0)
        try:
            slow_ms = float(os.getenv("REDIS_SSE_STATE_SLOW_MS", "500") or "500")
        except Exception:
            slow_ms = 500.0
        sample = batch[-1]
        crawl_trace(
            logger,
            phase="redis",
            action="async_state_write_batch",
            state="slow" if state_ms >= slow_ms else "end",
            level=logging.DEBUG,
            job_id=sample.get("job_id"),
            elapsed_ms=state_ms,
            account=sample.get("account_name"),
            status=(sample.get("message") or {}).get("status"),
            counts={"batch": len(batch), "events": event_count, "pending": len(_state_write_pending)},
            slow_ms=slow_ms,
            lock_wait_ms=lock_wait_ms,
            flush_start_lag_ms=flush_start_lag_ms,
            sleep_ms=sleep_ms,
            sleep_overrun_ms=sleep_overrun_ms,
            oldest_queue_wait_ms=oldest_queue_wait_ms,
            newest_queue_wait_ms=newest_queue_wait_ms,
            oldest_schedule_wait_ms=oldest_schedule_wait_ms,
            enqueue_task_lag_ms=enqueue_task_lag_ms,
            enqueue_lock_wait_ms=enqueue_lock_wait_ms,
            redis_get_ms=redis_get_ms,
            pipe_build_ms=pipe_build_ms,
            pipe_execute_ms=pipe_execute_ms,
            timeout_overrun_ms=timeout_overrun_ms,
        )
    except Exception as exc:
        if pipe_execute_ms <= 0.0 and "execute_t0" in locals():
            pipe_execute_ms = elapsed_ms(execute_t0)
            timeout_overrun_ms = max(0.0, pipe_execute_ms - (_redis_state_timeout_sec() * 1000.0))
        if redis_get_ms <= 0.0 and "get_t0" in locals():
            redis_get_ms = elapsed_ms(get_t0)
        if pipe_build_ms <= 0.0 and "build_t0" in locals():
            pipe_build_ms = elapsed_ms(build_t0)
        sample = batch[-1] if batch else {}
        job_ids = []
        statuses = []
        state_keys = []
        try:
            for item in batch:
                job_ids.append(str(item.get("job_id") or ""))
                statuses.append(str((item.get("message") or {}).get("status") or ""))
                state_keys.append(str(item.get("state_key") or ""))
        except Exception:
            pass
        crawl_trace(
            logger,
            phase="redis",
            action="async_state_write_batch",
            state="fail",
            level=logging.WARNING,
            job_id=sample.get("job_id"),
            elapsed_ms=elapsed_ms(state_t0),
            counts={"batch": len(batch), "events": event_count, "pending": len(_state_write_pending)},
            error=repr(exc),
            error_type=type(exc).__name__,
            jobs=job_ids[:10],
            statuses=statuses[:10],
            state_keys=state_keys[:10],
            timeout_sec=_redis_state_timeout_sec(),
            lock_wait_ms=lock_wait_ms,
            flush_start_lag_ms=flush_start_lag_ms,
            sleep_ms=sleep_ms,
            sleep_overrun_ms=sleep_overrun_ms,
            oldest_queue_wait_ms=oldest_queue_wait_ms,
            newest_queue_wait_ms=newest_queue_wait_ms,
            oldest_schedule_wait_ms=oldest_schedule_wait_ms,
            enqueue_task_lag_ms=enqueue_task_lag_ms,
            enqueue_lock_wait_ms=enqueue_lock_wait_ms,
            redis_get_ms=redis_get_ms,
            pipe_build_ms=pipe_build_ms,
            pipe_execute_ms=pipe_execute_ms,
            timeout_overrun_ms=timeout_overrun_ms,
        )
        logger.warning(
            "[RedisSSE] async state write batch failed | jobs=%s statuses=%s keys=%s batch=%s events=%s pending=%s timeout=%.2fs lock_wait=%.1fms enqueue_lag=%.1fms enqueue_lock=%.1fms schedule_oldest=%sms queue_oldest=%sms sleep_overrun=%.1fms redis_get=%.1fms pipe_build=%.1fms pipe_execute=%.1fms timeout_overrun=%.1fms err_type=%s err=%r",
            job_ids[:10],
            statuses[:10],
            state_keys[:10],
            len(batch),
            event_count,
            len(_state_write_pending),
            _redis_state_timeout_sec(),
            lock_wait_ms,
            enqueue_task_lag_ms,
            enqueue_lock_wait_ms,
            f"{oldest_schedule_wait_ms:.1f}" if oldest_schedule_wait_ms is not None else "n/a",
            f"{oldest_queue_wait_ms:.1f}" if oldest_queue_wait_ms is not None else "n/a",
            sleep_overrun_ms,
            redis_get_ms,
            pipe_build_ms,
            pipe_execute_ms,
            timeout_overrun_ms,
            type(exc).__name__,
            exc,
        )
        retry_limit = _redis_state_write_retry_limit()
        retry_delay_sec = _redis_state_write_retry_delay_sec()
        requeued = 0
        dropped = 0
        if retry_limit > 0:
            async with _state_write_lock:
                for item in batch:
                    state_key = str(item.get("state_key") or "")
                    if not state_key:
                        dropped += 1
                        continue
                    retry_count = int(item.get("retry_count") or 0)
                    if retry_count >= retry_limit:
                        dropped += 1
                        continue
                    if state_key in _state_write_pending:
                        continue
                    revived = dict(item)
                    revived["redis_client"] = None
                    revived["retry_count"] = retry_count + 1
                    _state_write_pending[state_key] = revived
                    requeued += 1
                if requeued and (_state_write_flush_task is None or _state_write_flush_task.done()):
                    _state_write_flush_task = asyncio.create_task(
                        _flush_state_write_batch(delay_sec=retry_delay_sec),
                        name="redis-sse-state-batch-retry",
                    )
        if requeued or dropped:
            crawl_trace(
                logger,
                phase="redis",
                action="async_state_write_batch",
                state="retry_scheduled" if requeued else "retry_drop",
                level=logging.WARNING if dropped else logging.INFO,
                job_id=sample.get("job_id"),
                counts={
                    "requeued": requeued,
                    "dropped": dropped,
                    "retry_limit": retry_limit,
                    "pending": len(_state_write_pending),
                },
                retry_delay_ms=round(retry_delay_sec * 1000.0, 1),
            )

    if has_more:
        async with _state_write_lock:
            if _state_write_pending and (_state_write_flush_task is None or _state_write_flush_task.done()):
                _state_write_flush_task = asyncio.create_task(
                    _flush_state_write_batch(delay_sec=0.0),
                    name="redis-sse-state-batch-flush",
                )


async def _enqueue_state_write(
    *,
    redis_client: Redis,
    job_id: str,
    account_name: str,
    message: Dict[str, Any],
    state_key: str,
    scheduled_at_perf: float,
) -> None:
    global _state_write_flush_task, _state_write_pending_events
    batch_size = _redis_state_batch_size()
    enqueue_started_at = time.perf_counter()
    enqueue_task_lag_ms = max(0.0, (enqueue_started_at - scheduled_at_perf) * 1000.0)
    enqueue_lock_started_at = time.perf_counter()
    async with _state_write_lock:
        enqueue_lock_wait_ms = elapsed_ms(enqueue_lock_started_at)
        _state_write_pending_events += 1
        _state_write_pending[state_key] = {
            "redis_client": redis_client,
            "job_id": job_id,
            "account_name": account_name,
            "message": dict(message or {}),
            "state_key": state_key,
            "retry_count": 0,
            "enqueued_at_perf": time.perf_counter(),
            "scheduled_at_perf": scheduled_at_perf,
            "enqueue_task_lag_ms": enqueue_task_lag_ms,
            "enqueue_lock_wait_ms": enqueue_lock_wait_ms,
        }
        pending_count = len(_state_write_pending)
        should_flush_now = pending_count >= batch_size or _state_write_pending_events >= batch_size
        delay_sec = 0.0 if should_flush_now else _redis_state_batch_wait_sec()
        if should_flush_now and _state_write_flush_task and not _state_write_flush_task.done():
            _state_write_flush_task.cancel()
            _state_write_flush_task = None
        if _state_write_flush_task is None or _state_write_flush_task.done():
            _state_write_flush_task = asyncio.create_task(
                _flush_state_write_batch(delay_sec=delay_sec),
                name="redis-sse-state-batch-flush",
            )


def _schedule_state_write(
    *,
    redis_client: Redis,
    job_id: str,
    account_name: str,
    message: Dict[str, Any],
    state_key: str,
) -> None:
    try:
        if _should_skip_state_write(job_id, message):
            crawl_trace(
                logger,
                phase="redis",
                action="async_state_write_batch",
                state="skip_unchanged",
                level=logging.DEBUG,
                job_id=job_id,
                account=account_name,
                status=(message or {}).get("status"),
                min_interval_sec=_redis_state_min_write_interval_sec(),
            )
            return
        prev_meta = _last_publish_meta.get(job_id) or {}
        scheduled_meta = dict(prev_meta)
        scheduled_meta["state_write_scheduled_at_ts"] = time.time()
        _last_publish_meta[job_id] = scheduled_meta
        if _redis_state_batch_enabled():
            scheduled_at_perf = time.perf_counter()
            task = asyncio.create_task(
                _enqueue_state_write(
                    redis_client=redis_client,
                    job_id=job_id,
                    account_name=account_name,
                    message=dict(message or {}),
                    state_key=state_key,
                    scheduled_at_perf=scheduled_at_perf,
                ),
                name=f"redis-sse-state-enqueue:{job_id}",
            )
        else:
            task = asyncio.create_task(
                _write_state_for_publish(
                    redis_client=redis_client,
                    job_id=job_id,
                    account_name=account_name,
                    message=dict(message or {}),
                    state_key=state_key,
                ),
                name=f"redis-sse-state:{job_id}",
            )
        task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
    except Exception:
        pass

async def publish_sse_event(request: RedisSSEPublishRequest, bypass_throttle: bool = False) -> RedisSSEPublishResponse:
    job_id = request.job_id
    status = _normalize_status_for_sse(request.payload.status)
    request_message = request.payload.model_dump()
    if request.extra:
        request_message.update(request.extra)
    if _is_unit_progress_message(request_message):
        bypass_throttle = True
        logger.debug(
            "[RedisSSE][UnitProgressDebug] bypass | job_id=%s event=%s collection=%s updated=%s field_counts=%s",
            job_id,
            request_message.get("event"),
            request_message.get("collection_count"),
            request_message.get("updated_count"),
            request_message.get("field_save_counts"),
        )
    
    if status in {"completed", "cancelled", "error"}:
        bypass_throttle = True
        _pending_requests.pop(job_id, None)

    if not bypass_throttle:
        wait_s = await _reserve_rate_slot(job_id)
        if wait_s > 0:
            _pending_requests[job_id] = request
            if job_id not in _pending_tasks or _pending_tasks[job_id].done():
                _pending_tasks[job_id] = asyncio.create_task(_publish_pending_after(job_id, wait_s))
            crawl_trace(
                logger,
                phase="redis",
                action="publish_sse_event",
                state="throttled",
                job_id=job_id,
                queue_wait_ms=wait_s * 1000.0,
                status=status,
                source=request.extra.get("source") if request.extra else None,
                state_update_on_throttle=_redis_state_update_on_throttle_enabled(),
            )
            if _redis_state_update_on_throttle_enabled():
                await update_state_only(
                    job_id=job_id,
                    account_name=request.account_name or "dev_user",
                    payload=request.payload.model_dump(),
                    extra=request.extra,
                )
            return RedisSSEPublishResponse(job_id=job_id, account_name=request.account_name)

    try:
        op_t0 = time.perf_counter()
        redis_client: Redis = await asyncio.wait_for(get_redis(), timeout=_redis_get_timeout_sec())
        get_ms = (time.perf_counter() - op_t0) * 1000.0
        account_name = request.account_name
        resolve_ms = 0.0
        if not account_name:
            resolve_t0 = time.perf_counter()
            account_name = await asyncio.wait_for(
                _resolve_db_name(redis_client, job_id, request.account_name),
                timeout=_redis_state_timeout_sec(),
            )
            resolve_ms = (time.perf_counter() - resolve_t0) * 1000.0
        account_name = account_name or "dev_user"
        message = _build_message(request, account_name)
        prev_msg = _last_publish_meta.get(job_id, {}).get("message") or {}
        message = _merge_monotonic(prev_msg, message)
        message, needs_more = _apply_progress_smoothing(job_id, message)
        _sync_scan_and_scan_count(message)
        channel, state_key = _channel_name(account_name, job_id), _state_key(account_name, job_id)
        if _is_unit_progress_message(message):
            logger.debug(
                "[RedisSSE][UnitProgressDebug] publish | job_id=%s event=%s collection=%s updated=%s field_counts=%s needs_more=%s channel=%s",
                job_id,
                message.get("event"),
                message.get("collection_count"),
                message.get("updated_count"),
                message.get("field_save_counts"),
                needs_more,
                channel,
            )
        if _redis_publish_async_enabled():
            scheduled = _schedule_pubsub_publish(
                redis_client=redis_client,
                channel=channel,
                message=message,
                job_id=job_id,
                account_name=account_name,
                status=status,
            )
            _schedule_state_write(
                redis_client=redis_client,
                job_id=job_id,
                account_name=account_name,
                message=message,
                state_key=state_key,
            )
            _last_publish_meta[job_id] = {"message": message, "counts": _count_snapshot(message), "channel": channel, "state_key": state_key, "updated_at_ts": time.time()}
            crawl_trace(
                logger,
                phase="redis",
                action="publish_sse_event",
                state="scheduled" if scheduled else "drop",
                level=logging.INFO if scheduled else logging.WARNING,
                job_id=job_id,
                elapsed_ms=(time.perf_counter() - op_t0) * 1000.0,
                get_ms=round(get_ms, 1),
                resolve_ms=round(resolve_ms, 1),
                publish_ms=0.0,
                channel=channel,
                publish_async=True,
            )
            if needs_more:
                _schedule_smooth_frame(job_id, account_name)
            return RedisSSEPublishResponse(job_id=job_id, account_name=account_name, published=scheduled, state_updated=False)
        publish_t0 = time.perf_counter()
        pub_res = await asyncio.wait_for(
            redis_client.publish(channel, json.dumps(message, ensure_ascii=False)),
            timeout=_redis_publish_timeout_sec(),
        )
        publish_ms = (time.perf_counter() - publish_t0) * 1000.0
        _schedule_state_write(
            redis_client=redis_client,
            job_id=job_id,
            account_name=account_name,
            message=message,
            state_key=state_key,
        )
        
        _last_publish_meta[job_id] = {
            "message": message,
            "counts": _count_snapshot(message),
            "channel": channel,
            "state_key": state_key,
            "updated_at_ts": time.time(),
        }
        try:
            slow_ms = float(os.getenv("REDIS_SSE_STEP_SLOW_MS", "500") or "500")
        except Exception:
            slow_ms = 500.0
        max_step_ms = max(get_ms, resolve_ms, publish_ms)
        if max_step_ms >= max(0.0, slow_ms):
            crawl_trace(
                logger,
                phase="redis",
                action="publish_sse_event",
                state="slow",
                level=logging.INFO,
                job_id=job_id,
                elapsed_ms=(time.perf_counter() - op_t0) * 1000.0,
                get_ms=round(get_ms, 1),
                resolve_ms=round(resolve_ms, 1),
                publish_ms=round(publish_ms, 1),
                channel=channel,
                slow_ms=slow_ms,
            )
            logger.info(
                "[RedisSSE][slow_step] job_id=%s get_ms=%.1f resolve_ms=%.1f publish_ms=%.1f channel=%s",
                job_id,
                get_ms,
                resolve_ms,
                publish_ms,
                channel,
            )
        else:
            crawl_trace(
                logger,
                phase="redis",
                action="publish_sse_event",
                state="end",
                job_id=job_id,
                elapsed_ms=(time.perf_counter() - op_t0) * 1000.0,
                get_ms=round(get_ms, 1),
                resolve_ms=round(resolve_ms, 1),
                publish_ms=round(publish_ms, 1),
                channel=channel,
            )
        if needs_more:
            _schedule_smooth_frame(job_id, account_name)
        return RedisSSEPublishResponse(job_id=job_id, account_name=account_name, published=bool(pub_res), state_updated=False)
    except asyncio.TimeoutError:
        crawl_trace(
            logger,
            phase="redis",
            action="publish_sse_event",
            state="timeout",
            job_id=job_id,
            level=logging.WARNING,
            account=request.account_name,
            timeout_sec=_redis_publish_timeout_sec(),
            status=status,
        )
        logger.warning(
            "[Redis SSE] publish timeout | job_id=%s account=%s timeout=%.1fs status=%s",
            job_id,
            request.account_name,
            _redis_publish_timeout_sec(),
            status,
        )
        return RedisSSEPublishResponse(job_id=job_id, account_name=request.account_name)
    except Exception as e:
        crawl_trace(
            logger,
            phase="redis",
            action="publish_sse_event",
            state="fail",
            job_id=job_id,
            level=logging.WARNING,
            account=request.account_name,
            status=status,
            error=e,
        )
        logger.warning(f"[Redis SSE] publish ?ㅽ뙣: {e}")
        return RedisSSEPublishResponse(job_id=job_id, account_name=request.account_name)

async def _reserve_rate_slot(job_id: str) -> float:
    async with _rate_limit_lock:
        now = time.time()
        delta = now - _rate_limit_timestamps.get(job_id, 0.0)
        if delta >= SSE_RATE_LIMIT_INTERVAL:
            _rate_limit_timestamps[job_id] = now
            return 0.0
        return max(SSE_RATE_LIMIT_INTERVAL - delta, 0.0)

async def _publish_pending_after(job_id: str, delay: float):
    await asyncio.sleep(delay)
    if req := _pending_requests.pop(job_id, None):
        crawl_trace(
            logger,
            phase="redis",
            action="pending_publish",
            state="start",
            job_id=job_id,
            queue_wait_ms=delay * 1000.0,
        )
        await publish_sse_event(req, bypass_throttle=True)

# ??[蹂듦뎄] ImportError 諛⑹????꾩닔 ?⑥닔
def get_last_publish_meta(job_id: str) -> Dict[str, Any]:
    return _last_publish_meta.get(job_id, {})

async def send_message_to_redis_sse(job_id: str, message: Dict[str, Any], dbname: Optional[str] = None):
    if _local_file_crawl_no_redis_enabled() and str(job_id or "").startswith("local-file-crawl-"):
        _last_publish_meta[job_id] = {"message": dict(message or {}), "counts": _count_snapshot(message or {}), "state_key": "", "updated_at_ts": time.time()}
        return RedisSSEPublishResponse(job_id=job_id, account_name=dbname)
    try:
        payload = _build_payload(message)
        extra = {k: v for k, v in message.items() if k not in RedisSSEPayload.model_fields}
        return await publish_sse_event(RedisSSEPublishRequest(job_id=job_id, account_name=dbname, payload=payload, extra=extra))
    except Exception as e:
        logger.warning(f"[Redis SSE] send_message ?ㅽ뙣: {e}")
        return RedisSSEPublishResponse(job_id=job_id, account_name=dbname)

def _build_payload(message: Dict[str, Any]) -> RedisSSEPayload:
    # ?덇굅????蹂댁젙
    total = int(message.get("total_count", message.get("scan_count", 0)) or 0)
    return RedisSSEPayload(
        status=message.get("status", "running"),
        total_count=total,
        collection_count=int(message.get("collection_count", 0) or 0),
        save_count=int(message.get("save_count", 0) or 0),
        progress_percentage=float(message.get("progress_percentage", 0.0) or 0.0),
        timestamp=message.get("timestamp"),
    )

