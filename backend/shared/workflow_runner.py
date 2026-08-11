import asyncio
import hashlib
import json
import logging
import os
import time
import inspect
from datetime import datetime, timezone
from typing import Any, Dict, List

from backend.shared.crawl_monitor import monitor_auto_stop
from backend.shared.crawler_state import crawler_state
from backend.shared.job_result_report import schedule_job_result_report
from backend.shared.pre_explored_url import save_crawled_urls_report
from backend.shared.crawl_shared import (
    COMPLETE_SSE_STATUSES,
    STOP_SSE_STATUSES,
    normalize_status_for_sse,
    publish_client_redis_heartbeat,
)
from backend.shared.sse_publish_queue import enqueue_sse_message, await_sse_publish_idle
from backend.shared.redis_sse_service import update_state_only, get_last_publish_meta
from backend.shared.crawl_trace import crawl_trace, elapsed_ms
from backend.shared.crawl_redis_keys import crawl_client_heartbeat_key, crawl_state_key
from db.crawl_db_manager import update_crawling_log_counters
try:
    from db.mariadb_save_update import begin_crawl_db_cache, end_crawl_db_cache, prewarm_crawl_db_cache
except Exception:  # pragma: no cover - keep workflow runner import-safe
    begin_crawl_db_cache = None  # type: ignore
    end_crawl_db_cache = None  # type: ignore
    prewarm_crawl_db_cache = None  # type: ignore
from core.crawler.queues import dispose_job_queues
from core.crawler.global_pool import get_global_worker_pool
from utils.timezone_utils import get_local_now

logger = logging.getLogger("backend.shared.workflow_runner")
CONCURRENT_CRAWL_LOG_PREFIX = "[ConcurrentCrawlStartDebug]"
_EMPTY_UI_TOKENS = {"", "undefined", "null", "none", "nan"}


def _distributed_duplicate_workflow_lock_enabled() -> bool:
    return str(os.getenv("CRAWL_DISTRIBUTED_DUPLICATE_WORKFLOW_LOCK", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _duplicate_lock_blocks_different_job_ids() -> bool:
    return str(os.getenv("CRAWL_DUPLICATE_LOCK_BLOCK_DIFFERENT_JOB_IDS", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _board_mariadb_minimal_enabled() -> bool:
    return str(os.getenv("BOARD_CRAWL_MARIADB_MINIMAL", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _filter_start_workflow_kwargs(workflow: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        signature = inspect.signature(workflow.start_workflow)
    except Exception:
        return dict(kwargs)
    parameters = signature.parameters
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values()):
        return dict(kwargs)
    return {key: value for key, value in kwargs.items() if key in parameters}


def _distributed_duplicate_workflow_lock_ttl_sec() -> int:
    try:
        ttl = int(os.getenv("CRAWL_DISTRIBUTED_DUPLICATE_WORKFLOW_LOCK_TTL_SEC", "10800") or "10800")
    except Exception:
        ttl = 10800
    return max(60, min(ttl, 43200))


def _duplicate_lock_stale_release_enabled() -> bool:
    return str(os.getenv("CRAWL_DUPLICATE_LOCK_STALE_RELEASE", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _duplicate_lock_stale_seconds() -> float:
    try:
        value = float(os.getenv("CRAWL_DUPLICATE_LOCK_STALE_SEC", "600") or "600")
    except Exception:
        value = 600.0
    return max(60.0, min(value, 43200.0))


def _duplicate_lock_missing_state_stale_seconds() -> float:
    try:
        value = float(os.getenv("CRAWL_DUPLICATE_LOCK_MISSING_STATE_STALE_SEC", "120") or "120")
    except Exception:
        value = 120.0
    return max(30.0, min(value, 43200.0))


def _duplicate_lock_heartbeat_stale_seconds() -> float:
    try:
        value = float(os.getenv("CRAWL_DUPLICATE_LOCK_HEARTBEAT_STALE_SEC", "180") or "180")
    except Exception:
        value = 180.0
    return max(60.0, min(value, 43200.0))


def _decode_redis_hash(raw: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    for key, value in raw.items():
        try:
            k = key.decode("utf-8", errors="replace") if isinstance(key, (bytes, bytearray)) else str(key)
            v = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
            out[k] = v
        except Exception:
            continue
    return out


def _parse_state_timestamp_age_sec(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None


def _decode_redis_value(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if value is None:
        return ""
    return str(value)


def _local_workflow_owner_snapshot(existing_job_id: str) -> Dict[str, Any]:
    task = None
    workflow_obj = None
    try:
        task = crawler_state.workflow_tasks.get(existing_job_id)
    except Exception:
        task = None
    try:
        workflow_obj = crawler_state.workflows.get(existing_job_id)
    except Exception:
        workflow_obj = None
    try:
        task_active = bool(task and not task.done())
    except Exception:
        task_active = False
    try:
        workflow_running = bool(workflow_obj and getattr(workflow_obj, "is_running", False))
    except Exception:
        workflow_running = False
    try:
        active_slot = bool(crawler_state.has_active_workflow_slot(existing_job_id))
    except Exception:
        active_slot = False
    try:
        final_status = str(getattr(workflow_obj, "final_status", "") or "").strip().lower() if workflow_obj else ""
    except Exception:
        final_status = ""
    return {
        "task_present": task is not None,
        "task_active": task_active,
        "workflow_present": workflow_obj is not None,
        "workflow_running": workflow_running,
        "active_slot": active_slot,
        "final_status": final_status,
        "local_active": bool(task_active or workflow_running or active_slot),
    }


async def _inspect_duplicate_lock_owner(
    *,
    redis: Any,
    db_name: str,
    lock_key: str,
    lock_ttl: int,
    existing_job_id: str,
) -> Dict[str, Any]:
    local = _local_workflow_owner_snapshot(existing_job_id)
    state_key = crawl_state_key(db_name, existing_job_id)
    heartbeat_key = crawl_client_heartbeat_key(db_name, existing_job_id)
    state: Dict[str, str] = {}
    heartbeat_present = False
    heartbeat_ttl = None
    heartbeat_age_sec = None
    ttl_remaining = None
    try:
        raw_ttl = await redis.ttl(lock_key)
        ttl_remaining = int(raw_ttl)
    except Exception:
        ttl_remaining = None
    try:
        state = _decode_redis_hash(await redis.hgetall(state_key))
    except Exception:
        state = {}
    try:
        raw_hb = await redis.get(heartbeat_key)
        heartbeat_present = bool(raw_hb)
        if raw_hb:
            try:
                hb_payload = json.loads(_decode_redis_value(raw_hb))
                heartbeat_age_sec = _parse_state_timestamp_age_sec(hb_payload.get("ts"))
            except Exception:
                heartbeat_age_sec = None
        raw_hb_ttl = await redis.ttl(heartbeat_key)
        heartbeat_ttl = int(raw_hb_ttl)
    except Exception:
        heartbeat_present = False
        heartbeat_ttl = None
        heartbeat_age_sec = None
    status = str(state.get("status") or "").strip().lower()
    timestamp_age_sec = _parse_state_timestamp_age_sec(state.get("timestamp"))
    terminal = status in {"completed", "cancelled", "error", "failed", "fail", "stopped", "stop", "duplicate"}
    lock_age_sec = None
    if isinstance(ttl_remaining, int) and ttl_remaining >= 0:
        lock_age_sec = max(0, int(lock_ttl) - ttl_remaining)
    stale_reason = ""
    if not local.get("local_active"):
        if terminal:
            stale_reason = f"state_terminal:{status or '-'}"
        elif (
            heartbeat_present
            and heartbeat_age_sec is not None
            and heartbeat_age_sec >= _duplicate_lock_heartbeat_stale_seconds()
        ):
            stale_reason = f"heartbeat_stale:{heartbeat_age_sec:.1f}s"
        elif timestamp_age_sec is not None and timestamp_age_sec >= _duplicate_lock_stale_seconds():
            stale_reason = f"state_stale:{timestamp_age_sec:.1f}s"
        elif not state and lock_age_sec is not None and lock_age_sec >= _duplicate_lock_missing_state_stale_seconds():
            stale_reason = f"state_missing_lock_age:{lock_age_sec}s"
    return {
        **local,
        "state_key": state_key,
        "heartbeat_key": heartbeat_key,
        "state_present": bool(state),
        "state_status": status,
        "state_timestamp_age_sec": timestamp_age_sec,
        "heartbeat_present": heartbeat_present,
        "heartbeat_age_sec": heartbeat_age_sec,
        "heartbeat_ttl_sec": heartbeat_ttl,
        "lock_ttl_remaining_sec": ttl_remaining,
        "lock_age_sec": lock_age_sec,
        "stale": bool(stale_reason),
        "stale_reason": stale_reason,
    }


def _workflow_duplicate_fingerprint(
    *,
    workflow: Any,
    start_urls: List[str],
    start_date: Any,
    end_date: Any,
    db_name: str,
    chat_bot_id: str | None,
    use_query_links_only: bool,
) -> str:
    normalized_urls: List[str] = []
    for raw in list(start_urls or [])[:5000]:
        if isinstance(raw, dict):
            text = str(raw.get("url") or raw.get("content") or raw.get("source_url") or raw.get("href") or "").strip()
        else:
            text = str(raw or "").strip()
        if text:
            normalized_urls.append(text.split("#", 1)[0].strip().lower())
    payload = {
        "workflow": type(workflow).__name__,
        "db_name": str(db_name or "").strip().lower(),
        "chat_bot_id": str(chat_bot_id or getattr(workflow, "chat_bot_id", "") or "").strip(),
        "start_date": str(start_date or ""),
        "end_date": str(end_date or ""),
        "use_query_links_only": bool(use_query_links_only),
        "count": len(start_urls or []),
        "urls": sorted(set(normalized_urls)),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _clean_ui_text(value: Any) -> str:
    try:
        text = str(value if value is not None else "").strip()
    except Exception:
        return ""
    if text.lower() in _EMPTY_UI_TOKENS:
        return ""
    return text


def _field_save_counts_for_source(source: str, count: int) -> Dict[str, int]:
    value = max(0, int(count or 0))
    counts = {
        "title": 0,
        "content": 0,
        "cate": 0,
        "symmary": 0,
        "type": 0,
        "url": 0,
        "web_de": 0,
    }
    if source == "partial_content_relearn":
        counts["content"] = value
    return counts

# ------------------------------------------------------------------
# Stage JSON 疫꿸퀡而???????덈뮸) 燁삳똻???癰귣똻??
# - ?醫딇??癒?퉳 燁삳똻??紐껊뮉 域밸챶?嚥??癒?? "??????덈뮸"筌?筌ㅼ뮇伊?筌뤴뫀????類μ넇????뽯뻻??띾┛ ?袁る맙
# ------------------------------------------------------------------
def _downloads_root_dir() -> str:
    try:
        # backend/shared -> backend -> project_root -> downloads
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "downloads"))
    except Exception:
        return os.path.abspath(os.path.join(os.getcwd(), "downloads"))


def _find_stage_json_path(*, stage: str, db_name: str, job_id: str) -> str | None:
    """downloads/ ??꾨릭(??륁맄 ??????釉??癒?퐣 stage JSON ???뵬??筌≪뼔???"""
    if not stage or not db_name or not job_id:
        return None
    filename = f"stage_{stage}_{db_name}_{job_id}.json"
    root = _downloads_root_dir()
    try:
        direct = os.path.join(root, filename)
        if os.path.isfile(direct):
            return direct
    except Exception:
        pass
    try:
        for dirpath, _dirnames, filenames in os.walk(root):
            if filename in filenames:
                return os.path.join(dirpath, filename)
    except Exception:
        return None
    return None


def _read_json_file(path: str | None) -> Dict[str, Any] | None:
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _stage_save_unique_files(*, db_name: str, job_id: str) -> int | None:
    """stage_save?癒?퐣 ??쇱젫 ???貫留??醫딅빍?????뵬 ??file_path ?怨쀪퐨)???④쑴沅?"""
    p = _find_stage_json_path(stage="save", db_name=db_name, job_id=job_id)
    obj = _read_json_file(p)
    urls = (obj or {}).get("urls")
    if not isinstance(urls, list):
        return None
    keys: set[str] = set()
    for e in urls:
        if not isinstance(e, dict):
            continue
        fp = str(e.get("file_path") or e.get("local_path") or "").strip()
        u = str(e.get("url") or "").strip()
        k = fp or u
        if k:
            keys.add(k)
    return len(keys)


def _stage_study_success_count(*, db_name: str, job_id: str) -> int | None:
    """stage_study???源껊궗 ??뽰젎?癒?춸 疫꿸퀡以???嚥? len(urls)???源껊궗 燁삳똻??紐껋쨮 ????"""
    p = _find_stage_json_path(stage="study", db_name=db_name, job_id=job_id)
    obj = _read_json_file(p)
    urls = (obj or {}).get("urls")
    if isinstance(urls, list):
        return len(urls)
    try:
        return int((obj or {}).get("total"))
    except Exception:
        return None


def _is_file_mode_workflow(workflow: Any) -> bool:
    try:
        if bool(getattr(workflow, "is_attachment_file_crawl_workflow", False)):
            return True
    except Exception:
        pass
    try:
        if bool(getattr(workflow, "file_mode", False)):
            return True
    except Exception:
        pass
    try:
        ui_colle = str(getattr(workflow, "ui_colle", "") or "").strip().lower()
        if ui_colle == "file":
            return True
    except Exception:
        pass
    try:
        colle = str(getattr(workflow, "colle", "") or "").strip().lower()
        if colle == "file":
            return True
    except Exception:
        pass
    try:
        colle_mode = str(getattr(workflow, "colle_mode", "") or "").strip().lower()
        if colle_mode == "file":
            return True
    except Exception:
        pass
    return False


def _check_terminal_save_study_counts(
    stats: Dict[str, Any],
    *,
    is_file_mode: bool = False,
) -> tuple[bool, str]:
    """怨듭슜 ?꾨즺 湲곗? 寃利? save == study_success(?놁쑝硫?study_count ?ъ슜)."""
    try:
        save = int(stats.get("save_count", 0) or 0)
    except Exception:
        save = 0
    try:
        study = int(
            stats.get("study_success_count", stats.get("study_count", 0)) or 0
        )
    except Exception:
        study = 0
    try:
        study_done = int(stats.get("study_done_count", 0) or 0)
    except Exception:
        study_done = 0
    try:
        study_failed = int(stats.get("study_failed_count", 0) or 0)
    except Exception:
        study_failed = 0
    try:
        study_skipped = int(stats.get("study_skipped_count", 0) or 0)
    except Exception:
        study_skipped = 0
    if is_file_mode:
        # ???뵬 筌뤴뫀諭??揶쏆뮇??類ｋ궖 ???쉘 筌△뫀???源놁몵嚥?skipped/failed揶쎛 ?類ㅺ맒 ?ル굝利뷴첎? ??????덈뼄.
        try:
            save_done = int(stats.get("save_done_count", stats.get("save_success_count", save)) or 0)
        except Exception:
            save_done = 0
        try:
            save_success = int(stats.get("save_success_count", save_done) or 0)
        except Exception:
            save_success = save_done
        ok = max(save_done, save_success) >= save
    else:
        ok = study_done >= save
    pending_study = max(0, save - study_done)
    detail = (
        f"save={save} study_success={study} study_done={study_done} "
        f"study_failed={study_failed} study_skipped={study_skipped} "
        f"pending_study={pending_study}"
    )
    detail += " rule=save_done>=save" if is_file_mode else " rule=study_done>=save"
    try:
        fail_reason = str(stats.get("study_fail_reason") or "").strip()
    except Exception:
        fail_reason = ""
    try:
        fail_url = str(stats.get("study_fail_url") or "").strip()
    except Exception:
        fail_url = ""
    try:
        fail_detail = str(stats.get("study_fail_detail") or "").strip()
    except Exception:
        fail_detail = ""
    if fail_reason:
        detail += f" last_reason={fail_reason}"
    if fail_url:
        detail += f" last_url={fail_url[:140]}"
    if fail_detail:
        detail += f" last_detail={fail_detail[:160]}"
    try:
        issue_samples = list(stats.get("study_issue_samples") or [])
    except Exception:
        issue_samples = []
    if issue_samples:
        sample_parts = []
        for sample in issue_samples[-3:]:
            try:
                s_reason = str((sample or {}).get("reason") or "").strip()
                s_path = str((sample or {}).get("path") or "").strip()
                s_url = str((sample or {}).get("url") or "").strip()
                label = s_path or s_url or "-"
                sample_parts.append(f"{s_reason}:{label[:80]}")
            except Exception:
                continue
        if sample_parts:
            detail += " recent=" + " || ".join(sample_parts)
    return ok, detail


def _safe_count_value(value: Any) -> int | None:
    try:
        return max(0, int(value or 0))
    except Exception:
        return None


def _resolve_effective_study_count(
    stats: Dict[str, Any],
    *,
    is_file_mode: bool = False,
) -> int:
    """Return the actually learned count used for UI and crawl-log updates."""
    save = _safe_count_value(stats.get("save_count", 0)) or 0
    if is_file_mode:
        for key in (
            "file_study_success_count",
            "study_success_count",
            "file_study_count",
            "study_count",
        ):
            if key not in stats:
                continue
            study = _safe_count_value(stats.get(key))
            if study is not None:
                return min(study, save)
        return 0

    study = _safe_count_value(stats.get("study_success_count", stats.get("study_count", 0))) or 0
    return min(study, save)


def _apply_stage_terminal_count_adjustments(
    stats: Dict[str, Any],
    *,
    db_name: str,
    job_id: str,
    is_file_mode: bool = False,
) -> Dict[str, Any]:
    """
    Apply final stage JSON corrections before any terminal completion decision.

    This prevents a workflow from being marked completed using stale in-memory
    counters while the persisted save/study stage data already shows unfinished
    learning.
    """
    adjusted = dict(stats or {})

    try:
        stage_save_files = _stage_save_unique_files(db_name=db_name, job_id=job_id)
    except Exception:
        stage_save_files = None
    try:
        stage_study_success = _stage_study_success_count(db_name=db_name, job_id=job_id)
    except Exception:
        stage_study_success = None

    if stage_save_files is not None:
        if is_file_mode:
            # ???뵬 筌뤴뫀諭?筌ㅼ뮇伊???????롫뮉 餓λ쵌而?筌롫뗀?덄뵳?燁삳똻??怨뺣궖??            # stage_save(JSON)??疫꿸퀡以????쇱젫 野껉퀗?든몴??怨쀪퐨??뺣뼄.
            try:
                actual_saved = max(0, int(stage_save_files))
            except Exception:
                actual_saved = 0
            adjusted["save_count"] = actual_saved
            adjusted["collection_count"] = actual_saved
            try:
                adjusted["save_success_count"] = min(
                    int(adjusted.get("save_success_count", 0) or 0),
                    actual_saved,
                )
            except Exception:
                adjusted["save_success_count"] = actual_saved
        else:
            try:
                adjusted["save_count"] = max(int(adjusted.get("save_count", 0) or 0), int(stage_save_files))
            except Exception:
                adjusted["save_count"] = stage_save_files

    if stage_study_success is not None:
        try:
            adjusted["study_success_count"] = max(
                int(adjusted.get("study_success_count", 0) or 0),
                int(stage_study_success),
            )
        except Exception:
            adjusted["study_success_count"] = stage_study_success
        try:
            adjusted["study_done_count"] = max(
                int(adjusted.get("study_done_count", 0) or 0),
                int(stage_study_success),
            )
        except Exception:
            adjusted["study_done_count"] = stage_study_success
        try:
            adjusted["study_count"] = max(
                int(adjusted.get("study_count", 0) or 0),
                int(stage_study_success),
            )
        except Exception:
            adjusted["study_count"] = stage_study_success

    return adjusted


def _agent_debug_log(hypothesis_id: str, location: str, message: str, data: Dict[str, Any]) -> None:
    return


def _env_bool(name: str, default: str = "1") -> bool:
    try:
        return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default == "1"


def _file_workflow_saved_learning_accounted(workflow: Any) -> bool:
    if not _is_file_mode_workflow(workflow):
        return False
    try:
        dispatch_complete = getattr(workflow, "file_learning_request_dispatch_complete", None)
        if callable(dispatch_complete) and bool(dispatch_complete()):
            return True
    except Exception:
        pass
    try:
        stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
    except Exception:
        stats = {}
    if not stats:
        stats = getattr(workflow, "stats", {}) or {}
    try:
        save = int(stats.get("save_count", 0) or 0)
    except Exception:
        save = 0
    if save <= 0:
        return False
    try:
        success = int(stats.get("file_study_success_count", stats.get("study_success_count", 0)) or 0)
    except Exception:
        success = 0
    try:
        failed = int(stats.get("file_study_failed_count", stats.get("study_failed_count", 0)) or 0)
    except Exception:
        failed = 0
    return (success + failed) >= save

def _collect_background_async_tasks(workflow: Any) -> List[asyncio.Task]:
    """??곌쾿???쨮揶쎛 ?곕뗄???롫뮉 獄쏄퉫???깆뒲??asyncio.Task筌???륁춿??뺣뼄."""
    out: List[asyncio.Task] = []
    for attr in ("_post_download_tasks", "_trigger_tasks", "_learn_tasks", "_meta_extraction_tasks"):
        s = getattr(workflow, attr, None)
        if isinstance(s, (set, list, frozenset)):
            for t in list(s):
                try:
                    if isinstance(t, asyncio.Task) and not t.done():
                        out.append(t)
                except Exception:
                    pass
    extra = getattr(workflow, "_background_misc_tasks", None)
    if isinstance(extra, (set, list, frozenset)):
        for t in list(extra):
            try:
                if isinstance(t, asyncio.Task) and not t.done():
                    out.append(t)
            except Exception:
                pass
    # ???뵬 ???뵠?袁⑥뵬?? collection_batch join ???袁⑸툡???源놁몵嚥?finalize揶쎛 shutdown ??곸뵠 ??멸돌筌?    # _file_progress_task 揶쎛 ??덈뮸 餓λ쵐??怨뺣즲 pending??곗쨮 ??レ뿳筌왖 ??녿툡 Redis筌??믪눘? completed ??????됱벉.
    file_learning_accounted = False
    try:
        file_learning_accounted = _file_workflow_saved_learning_accounted(workflow)
    except Exception:
        file_learning_accounted = False
    try:
        dedicated_file_request_dispatch = callable(
            getattr(workflow, "file_learning_request_dispatch_complete", None)
        )
    except Exception:
        dedicated_file_request_dispatch = False
    for _attr in ("_file_progress_task", "_file_worker_task"):
        try:
            _t = getattr(workflow, _attr, None)
            if isinstance(_t, asyncio.Task) and not _t.done():
                if file_learning_accounted or dedicated_file_request_dispatch:
                    continue
                out.append(_t)
        except Exception:
            pass
    _fpl = getattr(workflow, "_file_parallel_learn_tasks", None)
    if isinstance(_fpl, (set, list, frozenset)):
        for _t in list(_fpl):
            try:
                if isinstance(_t, asyncio.Task) and not _t.done():
                    out.append(_t)
            except Exception:
                pass
    # 野껊슣???BoardContentWorkflow): 燁삳똾?믤⑥쥓???袁⑹퓗??猷???꿸숲 ??덈뮸夷똡iscover overflow ?源놁뵠
    # start_workflow 獄쏆꼹???袁⑸퓠????λ툡 SSE/?袁⑥쨴?紐껋춸 ?믪눘? ??멸돌???袁⑷맒??筌띾슢諭??
    for _attr in (
        "_post_job_cate_update_task",
        "_selector_learning_task",
        "_discover_overflow_task",
    ):
        try:
            _t = getattr(workflow, _attr, None)
            if isinstance(_t, asyncio.Task) and not _t.done():
                out.append(_t)
        except Exception:
            pass
    return out



def _is_non_failure_study_progress_event(event: Any, message: Any) -> bool:
    event_text = str(event or "").strip().lower()
    message_text = str(message or "").strip().lower()
    if event_text in {"file_study_skipped", "study_skipped"}:
        return True
    if message_text in {"duplicate_reuse_learned", "duplicate_existing", "already_learned", "skip_learning"}:
        return True
    return False

def _workflow_has_pending_background_tasks(workflow: Any) -> bool:
    return len(_collect_background_async_tasks(workflow)) > 0


def _unfinished_asyncio_queue(q: Any) -> int:
    try:
        return int(getattr(q, "_unfinished_tasks", 0) or 0)
    except Exception:
        return 0


def _batch_queue_work_pending(bq: Any) -> bool:
    try:
        buf = getattr(bq, "buffer", None) or []
        if len(buf) > 0:
            return True
        inner = getattr(bq, "queue", None)
        if inner is None:
            return False
        try:
            if not inner.empty():
                return True
        except Exception:
            pass
        return _unfinished_asyncio_queue(inner) > 0
    except Exception:
        return False


def _file_job_queues_have_pending_work(workflow: Any) -> bool:
    """???뵬/筌ｂ뫀? ???뵠?袁⑥뵬??JobQueues??沃섎챷荑귞뵳??臾믩씜????됱몵筌?True (join ??Redis ?袁⑥┷ 獄쎻뫗?)."""
    jq = getattr(workflow, "_file_job_queues", None)
    if jq is None:
        return False
    try:
        if int(getattr(jq.scan_queue, "_unfinished_tasks", 0) or 0) > 0:
            return True
    except Exception:
        pass
    try:
        if _batch_queue_work_pending(jq.scan_batch_queue):
            return True
        if _batch_queue_work_pending(jq.collection_batch_queue):
            return True
        if _batch_queue_work_pending(jq.save_batch_queue):
            return True
        if _batch_queue_work_pending(jq.study_batch_queue):
            return True
        pq = jq.progress_queue
        try:
            if not pq.empty():
                return True
        except Exception:
            pass
        if _unfinished_asyncio_queue(pq) > 0:
            return True
    except Exception:
        return False
    return False


def _workflow_has_pending_pipeline_work(workflow: Any) -> bool:
    try:
        dispatch_complete = getattr(workflow, "file_learning_request_dispatch_complete", None)
        if callable(dispatch_complete) and not bool(dispatch_complete()):
            return True
    except Exception:
        pass
    if _workflow_has_pending_background_tasks(workflow):
        return True
    return _file_job_queues_have_pending_work(workflow)


def _file_learning_request_dispatch_pending(workflow: Any) -> bool:
    if not _is_file_mode_workflow(workflow):
        return False
    try:
        dispatch_complete = getattr(workflow, "file_learning_request_dispatch_complete", None)
        return callable(dispatch_complete) and not bool(dispatch_complete())
    except Exception:
        return False


def _file_learning_callbacks_detached(workflow: Any) -> bool:
    if not _is_file_mode_workflow(workflow):
        return False
    try:
        detached = getattr(workflow, "file_learning_callbacks_detached", None)
        return callable(detached) and bool(detached())
    except Exception:
        return False


def _file_workflow_save_stage_complete(workflow: Any) -> bool:
    if not _is_file_mode_workflow(workflow):
        return False
    try:
        stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
    except Exception:
        stats = {}
    if not stats:
        stats = getattr(workflow, "stats", {}) or {}
    try:
        save = int(stats.get("save_count", 0) or 0)
    except Exception:
        save = 0
    if save <= 0:
        return False
    try:
        save_done = int(stats.get("save_done_count", stats.get("save_success_count", 0)) or 0)
    except Exception:
        save_done = 0
    try:
        save_success = int(stats.get("save_success_count", 0) or 0)
    except Exception:
        save_success = 0
    try:
        collection = int(stats.get("collection_count", 0) or 0)
    except Exception:
        collection = 0
    return max(save_done, save_success, collection) >= save


async def _join_file_job_queues_slice(workflow: Any, timeout: float) -> None:
    """JobQueues ???닌덉퍢 join?????????곷뮞?癒?퐣 筌욊쑵六???묐뱜??쑵????뽯뮞??? 癰귣쵑六??롫즲嚥?筌욁룂? ???袁⑸툡??."""
    jq = getattr(workflow, "_file_job_queues", None)
    if jq is None or timeout <= 0:
        return
    flush_cap = max(1.0, min(timeout * 0.25, 30.0))
    for qn in ("collection_batch_queue", "save_batch_queue", "study_batch_queue", "scan_batch_queue"):
        bq = getattr(jq, qn, None)
        if bq is None:
            continue
        try:
            await asyncio.wait_for(bq.flush(), timeout=flush_cap)
        except Exception:
            pass
    t_remain = max(0.5, timeout - flush_cap * 4)
    coros: List[Any] = []
    for obj in (
        getattr(jq, "collection_batch_queue", None),
        getattr(jq, "progress_queue", None),
        getattr(jq, "save_batch_queue", None),
        getattr(jq, "study_batch_queue", None),
        getattr(jq, "scan_batch_queue", None),
    ):
        if obj is None:
            continue
        try:
            coros.append(obj.join())
        except Exception:
            pass
    sq = getattr(jq, "scan_queue", None)
    if sq is not None:
        try:
            coros.append(sq.join())
        except Exception:
            pass
    if not coros:
        return
    try:
        await asyncio.wait_for(asyncio.gather(*coros), timeout=t_remain)
    except asyncio.TimeoutError:
        pass
    except Exception:
        pass


async def _drain_workflow_background_tasks(workflow: Any, job_id: str) -> bool:
    """
    start_workflow 獄쏆꼹??筌욊낱??癒?즲 ??쇱뒲嚥≪뮆諭???덈뮸 ?紐꺿봺椰???asyncio.Task揶쎛 ??μ뱽 ????덈뼄.
    Redis/SSE夷똃B??'?袁⑥┷'嚥?筌〓씧由??袁⑸퓠 ??諭????멸텊 ???돱筌왖 疫꿸퀡?롧뵳怨뺣뼄.
    ???뵬 ???뵠?袁⑥뵬?紐꾩뵠 ??됱몵筌?JobQueues(scan/collection/progress/save/study)??join ?????돱筌왖 ??덉뵬 ??됯텦 ??됰퓠????疫꿸퀬釉??
    - 獄쏆꼹??True: ??疫?甕곕뗄????됰퓠??pending ??곸벉(?癒?뮉 餓λ쵎?믪쮯?癒?쑎 筌뤴뫀諭?癒?퐣 ??됯텦 ???춭).
    - 獄쏆꼹??False: ????됯텦 ??곷퓠??Task揶쎛 ??μ벉 ???袁⑥┷嚥??띯몿???? 筌?野?
    """
    raw_fs = (getattr(workflow, "final_status", None) or "").strip().lower()
    stopped = raw_fs in STOP_SSE_STATUSES
    errored = raw_fs in {"error", "failed", "fail", "exception"}

    try:
        total_cap = float(
            os.getenv(
                "WORKFLOW_BACKGROUND_DRAIN_ON_STOP_TOTAL_SEC" if stopped else "WORKFLOW_BACKGROUND_DRAIN_TOTAL_SEC",
                "300" if stopped else "7200",
            )
            or ("300" if stopped else "7200")
        )
    except Exception:
        total_cap = 300.0 if stopped else 7200.0
    if errored:
        try:
            total_cap = min(total_cap, float(os.getenv("WORKFLOW_BACKGROUND_DRAIN_ON_ERROR_TOTAL_SEC", "120") or "120"))
        except Exception:
            total_cap = min(total_cap, 120.0)
    total_cap = max(0.0, min(total_cap, 86400.0))

    try:
        slice_sec = float(os.getenv("WORKFLOW_BACKGROUND_DRAIN_SLICE_SEC", "60") or "60")
    except Exception:
        slice_sec = 60.0
    slice_sec = max(5.0, min(slice_sec, 600.0))

    deadline = None if total_cap <= 0 else time.monotonic() + total_cap
    loop_count = 0

    while True:
        wait_budget = slice_sec
        if deadline is not None:
            wait_budget = min(slice_sec, max(0.5, deadline - time.monotonic()))

        pending = _collect_background_async_tasks(workflow)
        if not pending:
            hook = getattr(workflow, "await_background_completion", None)
            if callable(hook):
                try:
                    rem = 300.0
                    if deadline is not None:
                        rem = max(1.0, deadline - time.monotonic())
                    co = hook()
                    if inspect.isawaitable(co):
                        await asyncio.wait_for(co, timeout=min(rem, slice_sec * 2))
                except asyncio.TimeoutError:
                    if deadline is not None and time.monotonic() >= deadline:
                        logger.warning(
                            "[RunWorkflowTask] await_background_completion timeout | job_id=%s",
                            job_id,
                        )
                        return False
                    continue
                except Exception as e:
                    logger.warning(
                        "[RunWorkflowTask] await_background_completion failed | job_id=%s err=%s",
                        job_id,
                        e,
                    )
            pending = _collect_background_async_tasks(workflow)
            if not pending:
                if _file_job_queues_have_pending_work(workflow):
                    try:
                        await asyncio.sleep(0)
                    except Exception:
                        pass
                    try:
                        dbn = getattr(workflow, "db_name", None) or ""
                        try:
                            await publish_client_redis_heartbeat(job_id, dbn)
                        except Exception:
                            pass
                        await _join_file_job_queues_slice(workflow, wait_budget)
                    except Exception:
                        pass
                    if _file_job_queues_have_pending_work(workflow):
                        if deadline is not None and time.monotonic() >= deadline:
                            logger.warning(
                                "[RunWorkflowTask] file JobQueues drain budget exhausted | job_id=%s",
                                job_id,
                            )
                            return False
                        loop_count += 1
                        continue

        if deadline is not None and time.monotonic() >= deadline:
            logger.warning(
                "[RunWorkflowTask] background drain budget exhausted | job_id=%s pending_tasks=%s stopped=%s",
                job_id,
                len(pending),
                stopped,
            )
            return False

        loop_count += 1
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True),
                timeout=wait_budget,
            )
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.debug(
                "[RunWorkflowTask] background gather pass err | job_id=%s err=%s",
                job_id,
                e,
            )


async def _continue_file_background_after_frontend_complete(
    *,
    workflow: Any,
    job_id: str,
    db_name: str,
) -> None:
    """Finish file learning/queue drain after the frontend already received completed."""
    try:
        ok = await _drain_workflow_background_tasks(workflow, job_id)
        try:
            reconcile_file_counts = getattr(workflow, "_reconcile_file_study_counts_from_learn_list", None)
            if callable(reconcile_file_counts):
                ret = reconcile_file_counts()
                if inspect.isawaitable(ret):
                    await ret
        except Exception as exc:
            logger.debug(
                "[RunWorkflowTask] async file learn count reconcile skipped | job_id=%s err=%s",
                job_id,
                exc,
            )
        if ok:
            try:
                await _force_terminate_job_after_finish(workflow=workflow, job_id=job_id, db_name=db_name)
            except Exception as exc:
                logger.debug(
                    "[RunWorkflowTask] async file cleanup after frontend complete failed | job_id=%s err=%s",
                    job_id,
                    exc,
                )
        else:
            logger.warning(
                "[RunWorkflowTask] async file backend work still pending after drain budget | job_id=%s",
                job_id,
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "[RunWorkflowTask] async file backend continuation failed | job_id=%s err=%s",
            job_id,
            exc,
        )


async def _redis_stop_poll_loop(job_id: str, workflow: Any) -> None:
    """
    uvicorn stop API揶쎛 ??곌쾿???쨮 ?紐꾨뮞??곷뮞 ??곸뵠 Redis?癒?춸 餓λ쵐? ???삋域밸챶? ??ｋ┸ 野껋럩??Celery ??쨌),
    ???묽 ?袁⑥쨮?紐꾨뮞?癒?퐣 雅뚯눊由?怨몄몵嚥???뚮선 ??롫굡 ??쎈꽧??椰꾨???
    """
    try:
        from db.db_redis import get_redis
    except Exception:
        return
    try:
        poll_sec = float(os.getenv("CRAWL_REDIS_STOP_POLL_SEC", "0.5") or "0.5")
    except Exception:
        poll_sec = 2.0
    poll_sec = max(0.5, min(poll_sec, 30.0))
    try:
        r = await get_redis()
        while True:
            # 筌??룐뫂遊썽겫???筌앸맩??野꺜??(??곸읈?癒?뮉 sleep ?袁⑸퓠筌??딅Ŋ苑?stop 筌욊낱??revoke?? 野껋럩???띻탢??筌왖?怨쀬뵠 ?뚮챷??
            try:
                v = await r.get(f"crawl_stop_request:{job_id}")
            except Exception:
                v = None
            if not v:
                await asyncio.sleep(poll_sec)
                continue
            try:
                if hasattr(workflow, "_force_hard_stop"):
                    co = workflow._force_hard_stop(reason="redis_stop")
                    if inspect.isawaitable(co):
                        await co
                else:
                    getattr(workflow, "stop_event", None) and workflow.stop_event.set()
            except Exception:
                try:
                    workflow.stop_event.set()
                except Exception:
                    pass
            break
    except asyncio.CancelledError:
        raise
    except asyncio.CancelledError:
        try:
            cancel_stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
        except Exception:
            cancel_stats = {}
        if not cancel_stats:
            cancel_stats = getattr(workflow, "stats", {}) or {}
        logger.warning(
            "%s[run_workflow_task_cancelled] job_id=%s db=%s workflow=%s final_status=%s state=%s "
            "stop_event=%s hard_stop=%s stop_requested=%s stats=%s history=%s",
            CONCURRENT_CRAWL_LOG_PREFIX,
            job_id,
            db_name,
            type(workflow).__name__ if workflow is not None else "",
            getattr(workflow, "final_status", None),
            getattr(getattr(workflow, "state", None), "name", getattr(workflow, "state", None)),
            bool(getattr(getattr(workflow, "stop_event", None), "is_set", lambda: False)()),
            bool(getattr(workflow, "_hard_stop", False)),
            bool(getattr(workflow, "_stop_requested", False)),
            {
                "scan": cancel_stats.get("scan_count") or cancel_stats.get("total_count"),
                "collection": cancel_stats.get("collection_count"),
                "save": cancel_stats.get("save_count"),
                "study": cancel_stats.get("study_count"),
                "study_success": cancel_stats.get("study_success_count"),
                "stage": cancel_stats.get("stage"),
                "event": cancel_stats.get("event"),
                "message": cancel_stats.get("message"),
            },
            crawler_state.job_history.get(job_id),
            exc_info=True,
        )
        raise
    except Exception as e:
        logger.debug("[RunWorkflowTask] redis stop poll ended | job_id=%s err=%s", job_id, e)


async def _force_terminate_job_after_finish(*, workflow: Any, job_id: str, db_name: str) -> None:
    """
    ?臾믩씜 ?ル굝利?嚥≪뮄??筌욊낱?? job ?온???귐딅꺖??? 揶쎛?館釉???筌앸맩????곸젫/餓λ쵎???뺣뼄.
    - ??곌쾿???쨮????? task/worker_manager cancel
    - job queues drain+dispose
    - global pool job ?源낆쨯 ??곸젫
    - crawler_state?癒?퐣 筌앸맩????볤탢
    """
    cleanup_started = time.monotonic()
    cleanup_flags: dict[str, Any] = {
        "workflow_stop": False,
        "worker_manager_stop": False,
        "file_worker_manager_stop": False,
        "global_pool_unregister": False,
        "global_pool_idle_close": False,
        "job_queues_dispose": False,
        "crawler_state_removed": False,
        "db_pool_cleanup": False,
    }
    # 1) best-effort: workflow??stop ?醫륁깈 (??? ?袁⑥┷??野껋럩?????됱몵沃샕嚥?筌욁룓苡띰쭕???疫?
    try:
        raw_final_before_cleanup = str(getattr(workflow, "final_status", "") or "").strip().lower()
        cleanup_only = raw_final_before_cleanup in COMPLETE_SSE_STATUSES
        if cleanup_only:
            cleanup_fn = getattr(workflow, "_cleanup_stop_resources", None)
            if callable(cleanup_fn):
                ret = cleanup_fn()
                if inspect.isawaitable(ret):
                    await asyncio.wait_for(ret, timeout=1.5)
                cleanup_flags["workflow_resource_cleanup"] = True
        stop_fn = None if cleanup_only else getattr(workflow, "stop", None)
        if callable(stop_fn):
            ret = stop_fn()
            if inspect.isawaitable(ret):
                try:
                    await asyncio.wait_for(ret, timeout=1.5)
                    cleanup_flags["workflow_stop"] = True
                except Exception:
                    pass
            else:
                cleanup_flags["workflow_stop"] = True
    except Exception:
        pass

    # 2) worker_manager 揶쏅벡??餓λ쵎??(idle ???묽 ?얜똾釉???疫?獄쎻뫗?)
    try:
        wm = getattr(workflow, "worker_manager", None)
        if wm is not None and hasattr(wm, "stop"):
            ret = wm.stop(graceful=False)  # type: ignore[call-arg]
            if inspect.isawaitable(ret):
                try:
                    await asyncio.wait_for(ret, timeout=3.0)
                    cleanup_flags["worker_manager_stop"] = True
                except Exception:
                    pass
            else:
                cleanup_flags["worker_manager_stop"] = True
    except Exception:
        pass

    # 3) workflow ????癒?퐣 ?곕뗄??餓λ쵐??task??cancel (best-effort)
    try:
        for attr in ("_post_download_tasks", "_trigger_tasks"):
            s = getattr(workflow, attr, None)
            if isinstance(s, set) and s:
                for t in list(s):
                    try:
                        if isinstance(t, asyncio.Task) and not t.done():
                            t.cancel()
                    except Exception:
                        pass
    except Exception:
        pass

    # 揶쏆뮆??task ?紐껊굶(??됱몵筌?cancel)
    try:
        file_wm = getattr(workflow, "_file_worker_manager_cleanup_pending", None)
        if file_wm is not None and hasattr(file_wm, "stop"):
            ret = file_wm.stop(
                graceful=False,
                stop_scan=False,
                stop_collection=False,
                stop_download=False,
                stop_study=False,
                stop_flush_task=False,
                close_browser=True,
                stop_playwright=True,
                reset_deduplicator=False,
            )
            if inspect.isawaitable(ret):
                try:
                    await asyncio.wait_for(ret, timeout=5.0)
                    cleanup_flags["file_worker_manager_stop"] = True
                except Exception:
                    pass
            else:
                cleanup_flags["file_worker_manager_stop"] = True
            try:
                setattr(workflow, "_file_worker_manager_cleanup_pending", None)
            except Exception:
                pass
    except Exception:
        pass

    for attr in ("_worker_manager_start_task", "_stop_grace_enforcer_task", "_auto_out_of_range_task"):
        try:
            t = getattr(workflow, attr, None)
            if isinstance(t, asyncio.Task) and not t.done():
                t.cancel()
        except Exception:
            pass

    # 4) 疫꼲嚥≪뮆苡???: job_id癰?context ?ル굝利????源낆쨯 ??곸젫 (job_id癰??類ｂ봺)
    try:
        if bool(getattr(workflow, "use_global_pool", False)):
            pool = get_global_worker_pool()
            await pool.unregister_job(job_id)
            cleanup_flags["global_pool_unregister"] = True
            await pool.close_resources_if_no_jobs()
            cleanup_flags["global_pool_idle_close"] = True
    except Exception:
        pass
    
    # 4-b) 湲濡쒕쾶 ? best-effort ?뺣━:
    # - use_global_pool ???삋域밸㈇? ??용쐭??곕즲, ?⑥눊援??臾믩씜??곗쨮 ?? worker揶쎛 ????됱뱽 ????덈뼄.
    # - ?源낆쨯 job??0椰꾨똻?좑쭖??????袁⑹읈??stop()??뤿연 worker idle ??疫꿸퀡? ??곷말??
    try:
        pool = get_global_worker_pool()
        await pool.close_resources_if_no_jobs()
        cleanup_flags["global_pool_idle_close"] = True
    except Exception:
        pass

    # 5) job queue dispose (drain+remove)
    try:
        key = getattr(workflow, "_job_queue_key", None) or getattr(workflow, "job_id", None) or job_id
        await dispose_job_queues(str(key))
        cleanup_flags["job_queues_dispose"] = True
    except Exception:
        pass

    # 6) 筌롫뗀?덄뵳?肉??筌앸맩????볤탢
    try:
        crawler_state.workflows.pop(job_id, None)
        cleanup_flags["crawler_state_removed"] = True
        prev_status = crawler_state.job_history.get(job_id, {}).get("status")
        if prev_status not in {"failed_to_start", "creation_failed"}:
            crawler_state.record_history(job_id, "cleaned", "force_cleanup_after_finish", db_name, chat_bot_id=getattr(workflow, "chat_bot_id", None))
    except Exception:
        pass

    # 7) DB ?? ?類ｂ봺:
    # - ??쇱㉦ job ??덈뻻 ??쎈뻬 ??띻펾?癒?퐣 ??삘뀲 ?臾믩씜???怨밸샨 雅뚯눘? ??낅즲嚥?
    #   疫꿸퀡??? "??뽮쉐 ??곌쾿???쨮?怨? 0椰??????춸 ?袁⑷퍥 ??????ル뮉??
    # - ?? PostgreSQL ???? job癰?db_name ??? ??????嚥?    #   ??덉뵬 db_name???怨뺣뮉 ??삘뀲 ??뽮쉐 workflow揶쎛 ??곸몵筌???????筌??醫롮젫?怨몄몵嚥???ル뮉??
    try:
        workflows_map = getattr(crawler_state, "workflows", None)
        workflows_vals = list(workflows_map.values()) if isinstance(workflows_map, dict) else []

        # 7-a) PostgreSQL(asyncpg) per-db pool close (safe when no other active workflow uses same db)
        try:
            from config.settings import DatabasePool as PostgresPool  # type: ignore

            same_db_active = False
            for w in workflows_vals:
                try:
                    if str(getattr(w, "db_name", "") or "").strip() == str(db_name).strip():
                        same_db_active = True
                        break
                except Exception:
                    continue
            if not same_db_active:
                await PostgresPool.close_pool(db_name)
        except Exception:
            pass

        # 7-b) If no active workflows remain, close all pools (Postgres/MySQL/Maria)
        if not workflows_vals:
            try:
                from config.settings import DatabasePool as PostgresPool  # type: ignore
                await PostgresPool.close_all_pools()
            except Exception:
                pass

            # MySQL(asyncmy/aiomysql) pools
            try:
                from db.mysql_db_config import MYSQL_DatabasePool  # type: ignore
                await MYSQL_DatabasePool.close_all_pools()
            except Exception:
                pass

            # MariaDB(aiomysql) pools
            try:
                from db.maria_db_config import DatabasePool as MariaPool  # type: ignore
                await MariaPool.close_all_pools()
            except Exception:
                pass
            cleanup_flags["db_pool_cleanup"] = True
    except Exception:
        pass
    try:
        pool = get_global_worker_pool()
        registered_jobs = getattr(pool, "registered_jobs", None)
        pool_started = bool(getattr(pool, "_started", False))
    except Exception:
        registered_jobs = None
        pool_started = None


async def _cleanup_workflow_state_after_finish(
    *,
    workflow: Any,
    job_id: str,
    db_name: str,
    delay_sec: int = 0,
    history_detail: str = "cleanup_after_finish",
) -> None:
    """??곌쾿???쨮??筌롫뗀?덄뵳??怨밴묶???ル굝利?筌욊낱???類ｂ봺??뺣뼄."""
    wait_sec = max(0, min(int(delay_sec or 0), 3600))
    try:
        if wait_sec > 0:
            await asyncio.sleep(wait_sec)

        try:
            from backend.shared.batch_embedding_scheduler import get_pending_embedding_callback_count
        except Exception:
            get_pending_embedding_callback_count = None  # type: ignore[assignment]

        try:
            pending_retry_sec = int(os.getenv("WORKFLOW_CLEANUP_PENDING_EMBEDDING_RETRY_SEC", "30") or "30")
        except Exception:
            pending_retry_sec = 30
        pending_retry_sec = max(1, min(pending_retry_sec, 300))
        try:
            pending_max_wait_sec = int(os.getenv("WORKFLOW_CLEANUP_PENDING_EMBEDDING_MAX_WAIT_SEC", "600") or "600")
        except Exception:
            pending_max_wait_sec = 600
        pending_max_wait_sec = max(0, min(pending_max_wait_sec, 7200))
        pending_waited_sec = 0
        wait_for_embedding_callback = not _file_learning_callbacks_detached(workflow)
        if not wait_for_embedding_callback:
            logger.info(
                "[FileLearnRequest][callback_wait_detached] job_id=%s db=%s history_detail=%s",
                job_id,
                db_name,
                history_detail,
            )
        while wait_for_embedding_callback and callable(get_pending_embedding_callback_count):
            try:
                pending_embedding = int(get_pending_embedding_callback_count(job_id) or 0)
            except Exception:
                pending_embedding = 0
            if pending_embedding <= 0:
                break
            if pending_waited_sec >= pending_max_wait_sec:
                logger.warning(
                    "[FileStudyDebug][workflow_cleanup_pending_embedding_timeout] job_id=%s db=%s pending=%s waited_sec=%s max_wait_sec=%s history_detail=%s",
                    job_id,
                    db_name,
                    pending_embedding,
                    pending_waited_sec,
                    pending_max_wait_sec,
                    history_detail,
                )
                break
            sleep_for = min(pending_retry_sec, pending_max_wait_sec - pending_waited_sec)
            await asyncio.sleep(sleep_for)
            pending_waited_sec += sleep_for

        try:
            pending_pipeline = _workflow_has_pending_pipeline_work(workflow)
        except Exception:
            pending_pipeline = None
        existed_before_pop = job_id in crawler_state.workflows
        crawler_state.workflows.pop(job_id, None)
        prev_status = crawler_state.job_history.get(job_id, {}).get("status")
        if prev_status not in {"failed_to_start", "creation_failed"}:
            crawler_state.record_history(
                job_id,
                "cleaned",
                history_detail,
                db_name,
                chat_bot_id=getattr(workflow, "chat_bot_id", None),
            )
    except Exception as exc:
        logger.debug(
            "[RunWorkflowTask] workflow state cleanup failed (ignore) | job_id=%s err=%s",
            job_id,
            exc,
        )


async def run_workflow_task(
    workflow: Any,
    start_urls: List[str],
    start_date,
    end_date,
    job_id: str,
    craw_id: str,
    db_name: str = "default",
    chat_bot_id: str | None = None,
    use_query_links_only: bool = False,
):
    """??곌쾿???쨮????쎈뻬 ??묐쓠 (router.py?癒?퐣 ?브쑬??"""
    workflow.craw_id = craw_id
    workflow.db_name = db_name
    workflow.job_id = job_id
    workflow.chat_bot_id = chat_bot_id
    workflow_slot_acquired = False
    workflow_slot_waited = False
    distributed_lock_key = ""
    distributed_lock_acquired = False
    workflow_t0 = time.perf_counter()
    prestart_t0 = workflow_t0
    crawl_trace(
        logger,
        phase="workflow",
        action="run_workflow_task",
        state="start",
        job_id=job_id,
        counts={"start_urls": len(start_urls or [])},
        db=db_name,
        craw_id=craw_id,
        workflow=type(workflow).__name__,
        use_query_links_only=bool(use_query_links_only),
    )

    # region agent log
    try:
        logger.debug(
            "[DBG][RUN] job_id=%s workflow=%s start_urls=%s use_query_links_only=%s",
            job_id,
            type(workflow).__name__,
            len(start_urls or []),
            bool(use_query_links_only),
        )
    except Exception:
        pass
    # endregion

    dashboard_job = str(job_id or "").startswith("board-dashboard-")
    local_file_crawl_job = str(job_id or "").startswith("local-file-crawl-")
    if _distributed_duplicate_workflow_lock_enabled() and not dashboard_job and not local_file_crawl_job:
        duplicate_t0 = time.perf_counter()
        try:
            from db.db_redis import get_redis

            fingerprint = _workflow_duplicate_fingerprint(
                workflow=workflow,
                start_urls=start_urls,
                start_date=start_date,
                end_date=end_date,
                db_name=db_name,
                chat_bot_id=chat_bot_id,
                use_query_links_only=use_query_links_only,
            )
            distributed_lock_key = f"crawl:workflow:duplicate_lock:{fingerprint}"
            ttl = _distributed_duplicate_workflow_lock_ttl_sec()
            redis = await get_redis()
            distributed_lock_acquired = bool(await redis.set(distributed_lock_key, job_id, ex=ttl, nx=True))
            if not distributed_lock_acquired:
                raw_existing = await redis.get(distributed_lock_key)
                if isinstance(raw_existing, (bytes, bytearray)):
                    raw_existing = raw_existing.decode("utf-8", errors="replace")
                existing_job_id = str(raw_existing or "").strip()
                logger.info(
                    "[RunWorkflowTask][DuplicateLock] same fingerprint; allow concurrent run and rely on URL dedup | "
                    "job_id=%s existing_job_id=%s key=%s start_urls=%s db=%s ttl=%s",
                    job_id,
                    existing_job_id,
                    distributed_lock_key,
                    len(start_urls or []),
                    db_name,
                    ttl,
                )
        except Exception as exc:
            logger.warning(
                "[RunWorkflowTask][DuplicateLock] lock check failed open | job_id=%s err=%s",
                job_id,
                exc,
            )
        finally:
            logger.debug(
                "[BottleneckTrace][runner_duplicate_done] job_id=%s duplicate_ms=%s elapsed_ms=%s acquired=%s",
                job_id,
                int((time.perf_counter() - duplicate_t0) * 1000),
                int((time.perf_counter() - prestart_t0) * 1000),
                distributed_lock_acquired,
            )

    try:
        slot_snapshot = crawler_state.get_workflow_slot_snapshot(workflow=workflow)
    except Exception:
        slot_snapshot = {"limit": 0, "active": 0, "waiting": 0}
    try:
        slot_limit = int(slot_snapshot.get("limit", 0) or 0)
    except Exception:
        slot_limit = 0
    try:
        slot_active = int(slot_snapshot.get("active", 0) or 0)
    except Exception:
        slot_active = 0
    try:
        slot_waiting = int(slot_snapshot.get("waiting", 0) or 0)
    except Exception:
        slot_waiting = 0

    if slot_limit > 0 and slot_active >= slot_limit:
        crawl_trace(
            logger,
            phase="workflow",
            action="workflow_slot",
            state="wait",
            job_id=job_id,
            counts={"active": slot_active, "limit": slot_limit, "waiting": slot_waiting + 1},
        )
        waiting_payload = {
            "status": "running",
            "event": "workflow_queued",
            "job_id": job_id,
            "account_name": db_name,
            "message": f"?ㅽ뻾 ?湲?以?.. active={slot_active}/{slot_limit} waiting={slot_waiting + 1}",
            "total_count": 0,
            "collection_count": 0,
            "save_count": 0,
            "study_count": 0,
            "timestamp": datetime.now().isoformat(),
        }
        try:
            await update_state_only(job_id=job_id, account_name=db_name, payload=waiting_payload)
        except Exception:
            pass
        try:
            enqueue_sse_message(job_id, waiting_payload, db_name, "workflow_queued", priority=0)
        except Exception:
            pass
        try:
            crawler_state.record_history(job_id, "queued", f"waiting_for_workflow_slot:{slot_active}/{slot_limit}", db_name, chat_bot_id=chat_bot_id)
        except Exception:
            pass

    slot_t0 = time.perf_counter()
    slot_result = await crawler_state.acquire_workflow_slot(job_id, workflow=workflow)
    workflow_slot_acquired = bool(slot_result.get("granted"))
    workflow_slot_waited = bool(slot_result.get("waited"))
    workflow_slot_cancelled = bool(slot_result.get("cancelled"))

    if not workflow_slot_acquired:
        if distributed_lock_acquired and distributed_lock_key:
            try:
                from db.db_redis import get_redis

                redis = await get_redis()
                raw_owner = await redis.get(distributed_lock_key)
                if isinstance(raw_owner, (bytes, bytearray)):
                    raw_owner = raw_owner.decode("utf-8", errors="replace")
                if str(raw_owner or "").strip() == str(job_id or "").strip():
                    await redis.delete(distributed_lock_key)
            except Exception:
                pass
            distributed_lock_acquired = False
        if workflow_slot_cancelled:
            try:
                crawler_state.record_history(
                    job_id,
                    "cancelled",
                    "workflow_slot_wait_cancelled",
                    db_name,
                    chat_bot_id=chat_bot_id,
                )
            except Exception:
                pass
            try:
                crawler_state.workflows.pop(job_id, None)
            except Exception:
                pass
        return

    try:
        start_payload = {
            "status": "running",
            "event": "workflow_slot_acquired" if workflow_slot_waited else "workflow_started",
            "job_id": job_id,
            "account_name": db_name,
            "message": "Crawling started.",
            "scan_count": 0,
            "total_count": 0,
            "collection_count": 0,
            "save_count": 0,
            "study_count": 0,
            "timestamp": datetime.now().isoformat(),
        }
        try:
            if _is_file_mode_workflow(workflow):
                start_payload["colle"] = "file"
                start_payload["source"] = "file_crawl"
                start_payload["file_crawl"] = True
        except Exception:
            pass
        await update_state_only(job_id=job_id, account_name=db_name, payload=start_payload)
        enqueue_sse_message(
            job_id,
            start_payload,
            db_name,
            "workflow_slot_acquired" if workflow_slot_waited else "workflow_started",
            priority=-5,
        )
    except Exception:
        pass

    # unique_id揶쎛 ?袁⑹춦 ??쇱젟??? ??녿릭??겹늺 鈺곌퀬???뤿연 ??쇱젟
    unique_t0 = time.perf_counter()
    if not getattr(workflow, "unique_id", None) and chat_bot_id and db_name and not _board_mariadb_minimal_enabled():
        for attempt in range(3):
            try:
                from db.mariadb_save_update import get_account_identifier_from_chatbot_setup

                unique_id = await asyncio.wait_for(
                    get_account_identifier_from_chatbot_setup(chat_bot_id=chat_bot_id, db_name=db_name),
                    timeout=5.0,
                )
                if unique_id:
                    unique_id = str(unique_id).upper().strip()
                    workflow.unique_id = unique_id
                    break
                if attempt < 2:
                    await asyncio.sleep(1)
            except (asyncio.TimeoutError, Exception) as exc:
                if attempt < 2:
                    logger.warning("[RunWorkflowTask] d_t 鈺곌퀬???????餓?.. (%d/3): %s", attempt + 1, exc)
                    await asyncio.sleep(1.5)
                else:
                    logger.warning("[RunWorkflowTask] d_t 議고쉶 理쒖쥌 ?ㅽ뙣, chat_bot_id?먯꽌 異붿텧 ?쒕룄: %s", exc)
                    if chat_bot_id:
                        from db.mariadb_save_update import _extract_identifier_from_chat_bot_id

                        workflow.unique_id = _extract_identifier_from_chat_bot_id(chat_bot_id)
    logger.debug(
        "[BottleneckTrace][runner_unique_id_done] job_id=%s unique_ms=%s elapsed_ms=%s has_unique_id=%s",
        job_id,
        int((time.perf_counter() - unique_t0) * 1000),
        int((time.perf_counter() - prestart_t0) * 1000),
        bool(getattr(workflow, "unique_id", None)),
    )

    logger.debug(
        "[START_URLS_TRACE] workflow_runner.run_workflow_task received | job_id=%s start_urls_count=%s sample=%s",
        job_id, len(start_urls or []), (start_urls or [])[:5],
    )
    try:
        logger.debug(
            "[RunWorkflowTask] start_urls sample | job_id=%s count=%s sample=%s",
            job_id,
            len(start_urls or []),
            (start_urls or [])[:5],
        )
    except Exception:
        pass
    logger.debug(
        "[RunWorkflowTask] input dates | job_id=%s start_date=%s end_date=%s craw_id=%s db=%s",
        job_id,
        start_date,
        end_date,
        craw_id,
        db_name,
    )
    crawler_state.record_history(job_id, "starting", "workflow_task_started", db_name, chat_bot_id=getattr(workflow, "chat_bot_id", None))

    if _env_bool("CRAWL_REDIS_STOP_POLL", "1"):
        try:
            asyncio.create_task(
                _redis_stop_poll_loop(job_id, workflow),
                name=f"redis-stop-poll:{job_id}",
            )
        except Exception:
            pass

    # ???醫뤿뻬 runtime_tab_view: 揶쎛?館釉???쥓?ㅵ칰??怨멸쉭??륁뵠筌왖 URL???類ｋ궖
    # - start_urls揶쎛 ?猷몃뼊?癒?퐣 餓Β??쑬由???냈??곷뮞(use_query_links_only)?癒?퐣???醫뤿뻬 ??쎈뻬??椰꾨?瑗????
    if start_urls and use_query_links_only and _env_bool("WORKFLOW_PRESTART_RUNTIME_TAB_VIEW", "1"):
        logger.debug(
            "[RunWorkflowTask] prestart runtime_tab_view skipped (query links expand later) | job_id=%s",
            job_id,
        )
    try:
        _start_urls_override_source = str(getattr(workflow, "start_urls_override_source", "") or "").strip()
    except Exception:
        _start_urls_override_source = ""
    _pre_explored_exact_sources = {
        "pre_explored_db",
        "file_crawl_post_db",
        "file_crawl_post_db_stream",
        "partial_content_relearn",
        "contents_detail_direct",
        "contents_url_fallback",
        "accelerated_crawl",
        "board_gap_dashboard",
    }
    if (
        start_urls
        and (not use_query_links_only)
        and _env_bool("WORKFLOW_PRESTART_RUNTIME_TAB_VIEW", "1")
        and _start_urls_override_source in _pre_explored_exact_sources
    ):
        logger.debug(
            "[RunWorkflowTask] prestart runtime_tab_view skipped (pre-explored exact targets) | job_id=%s source=%s count=%s",
            job_id,
            _start_urls_override_source,
            len(start_urls or []),
        )
    elif start_urls and (not use_query_links_only) and _env_bool("WORKFLOW_PRESTART_RUNTIME_TAB_VIEW", "1"):
        runtime_t0 = time.perf_counter()
        runtime_applied = False
        try:
            try:
                max_prestart = int(os.getenv("WORKFLOW_PRESTART_RUNTIME_TAB_VIEW_MAX_URLS", "300") or "300")
            except Exception:
                max_prestart = 300
            if len(start_urls or []) > max_prestart:
                logger.debug(
                    "[RunWorkflowTask] prestart runtime_tab_view skipped (too many urls) | job_id=%s count=%s limit=%s",
                    job_id,
                    len(start_urls or []),
                    max_prestart,
                )
                raise RuntimeError("prestart_runtime_tab_view_skipped_for_many_urls")
            try:
                is_file_mode = bool(getattr(workflow, "file_mode", False))
                prestart_mode = "file" if is_file_mode else "board"
            except Exception:
                is_file_mode = False
                prestart_mode = "board"
            try:
                from backend.shared.seed_urls import _is_list_page_url
            except Exception:
                _is_list_page_url = None
            if _is_list_page_url:
                try:
                    # 野껊슣???list) seed????륁뵠筌욌벡???袁る퉸 prestart??椰꾨?瑗????
                    # ???뵬 筌뤴뫀諭???怨멸쉭 URL ?醫뤿뻬 ?곕뗄???筌뤴뫗????嚥?list seed??곕즲 ??됱뒠??뺣뼄.
                    if (not is_file_mode) and any(_is_list_page_url(u) for u in (start_urls or [])):
                        logger.debug(
                            "[RunWorkflowTask] prestart runtime_tab_view skipped (list seed for paging) | job_id=%s",
                            job_id,
                        )
                        raise RuntimeError("prestart_runtime_tab_view_skipped_for_paging")
                except RuntimeError:
                    # propagate intentional control Flow to outer handler
                    raise
                except Exception:
                    # non-fatal: ignore errors in list-detection
                    pass
            from backend.shared.runtime_tab_view import resolve_runtime_start_urls

            try:
                timeout_sec = float(os.getenv("WORKFLOW_PRESTART_RUNTIME_TAB_VIEW_TIMEOUT_SEC", "20") or "20")
            except Exception:
                timeout_sec = 20.0
            timeout_sec = max(3.0, min(timeout_sec, 120.0))
            resolved = await asyncio.wait_for(
                resolve_runtime_start_urls(
                    list(start_urls),
                    mode=prestart_mode,
                    strict_view_only=False,
                ),
                timeout=timeout_sec,
            )
            if resolved:
                start_urls = list(resolved)
                runtime_applied = True
                try:
                    setattr(workflow, "_prestart_runtime_tab_view_applied", True)
                except Exception:
                    pass
                logger.debug(
                    "[RunWorkflowTask] prestart runtime_tab_view applied | job_id=%s mode=%s resolved=%s",
                    job_id,
                    prestart_mode,
                    len(start_urls),
                )
            else:
                logger.debug(
                    "[RunWorkflowTask] prestart runtime_tab_view empty; keep original | job_id=%s",
                    job_id,
                )
        except asyncio.TimeoutError:
            logger.debug(
                "[RunWorkflowTask] prestart runtime_tab_view timeout; keep original | job_id=%s timeout=%s",
                job_id,
                timeout_sec,
            )
        except Exception as exc:
            logger.debug(
                "[RunWorkflowTask] prestart runtime_tab_view failed; keep original | job_id=%s err=%s",
                job_id,
                exc,
            )
        finally:
            logger.debug(
                "[BottleneckTrace][runner_runtime_tab_done] job_id=%s runtime_ms=%s elapsed_ms=%s applied=%s start_urls=%s",
                job_id,
                int((time.perf_counter() - runtime_t0) * 1000),
                int((time.perf_counter() - prestart_t0) * 1000),
                runtime_applied,
                len(start_urls or []),
            )

    terminal_sse_sent = False
    pending_terminal_message: Dict[str, Any] | None = None
    pending_terminal_source = "workflow_completed"
    pending_terminal_status: Optional[str] = None
    pending_db_status: Optional[str] = None
    pending_db_stats: Dict[str, Any] = {}
    pending_db_craw_id: Any = None
    pending_db_flushed = False
    progress_started = asyncio.Event()
    prestart_stop = asyncio.Event()
    prestart_task: asyncio.Task | None = None
    heartbeat_stop = asyncio.Event()
    heartbeat_task: asyncio.Task | None = None
    monitor_stop = asyncio.Event()
    monitor_task: asyncio.Task | None = None
    live_db_update_tasks: set[asyncio.Task] = set()
    live_db_last_counts: tuple[int, int, int, int] | None = None
    live_db_last_update_at = 0.0
    try:
        _worker_file_mode = bool(getattr(workflow, "file_mode", False))
        _worker_attachment = bool(getattr(workflow, "is_attachment_file_crawl_workflow", False))
    except Exception:
        _worker_file_mode = False
        _worker_attachment = False
    _dense_redis_hb = bool(_worker_file_mode or _worker_attachment)
    try:
        # ???뵬/筌ｂ뫀?: ??쇱뒲嚥≪뮆諭띠쮯??덈뮸夷??join ??疫꿸퀗? 疫뀀챷堉???뺤쒔揶쎛 Redis state夷똠lient_heartbeat???癒?폒 揶쏄퉮??
        if _dense_redis_hb:
            heartbeat_interval = float(
                os.getenv("FILE_CRAWL_REDIS_HEARTBEAT_SEC", "30") or "30"
            )
        else:
            heartbeat_interval = float(os.getenv("SSE_KEEPALIVE_SEC", "60") or "60")
    except Exception:
        heartbeat_interval = 60.0
    _hb_floor = 5.0 if _dense_redis_hb else 10.0
    heartbeat_interval = max(_hb_floor, min(heartbeat_interval, 120.0))

    async def _heartbeat_loop():
        nonlocal terminal_sse_sent
        # 筌??룐뫂遊?癒?퐣 筌앸맩??Redis 揶쏄퉮????interval 筌띾슦寃???疫?(????몄쎗 ???뵬 筌ｌ꼶??餓λ쵐肉??雅뚯눊由????밤??醫륁깈)
        while not heartbeat_stop.is_set():
            if terminal_sse_sent:
                break

            stats: Dict[str, Any] = {}
            try:
                stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
            except Exception:
                stats = {}

            scan_val = int(stats.get("scan_count", stats.get("total_count", 0)) or 0)
            total_val = int(stats.get("total_count", stats.get("scan_count", 0)) or 0)
            heartbeat_study_count = int(stats.get("study_count", 0) or 0)

            state_payload: Dict[str, Any] = {
                "status": "running",
                "scan_count": scan_val,
                "total_count": total_val,
                "collection_count": int(stats.get("collection_count", 0) or 0),
                "save_count": int(stats.get("save_count", 0) or 0),
                "save_done_count": int(stats.get("save_done_count", 0) or 0),
                "save_success_count": int(stats.get("save_success_count", 0) or 0),
                "save_failed_count": int(stats.get("save_failed_count", 0) or 0),
                "study_count": int(stats.get("study_count", 0) or 0),
                "study_done_count": int(stats.get("study_done_count", 0) or 0),
                "study_success_count": int(stats.get("study_success_count", 0) or 0),
                "study_failed_count": int(stats.get("study_failed_count", 0) or 0),
                "study_skipped_count": int(stats.get("study_skipped_count", 0) or 0),
                "file_study_count": int(stats.get("file_study_count", 0) or 0),
                "file_study_done_count": int(stats.get("file_study_done_count", 0) or 0),
                "file_study_success_count": int(stats.get("file_study_success_count", 0) or 0),
                "file_study_failed_count": int(stats.get("file_study_failed_count", 0) or 0),
                "file_study_skipped_count": int(stats.get("file_study_skipped_count", 0) or 0),
                "pending_collection_count": int(stats.get("pending_collection_count", 0) or 0),
                "pending_save_count": int(stats.get("pending_save_count", 0) or 0),
                "stats_revision": int(stats.get("stats_revision", 0) or 0),
                "progress_percentage": float(stats.get("progress_percentage", 0) or 0),
                "timestamp": datetime.now().isoformat(),
            }
            extra_keepalive = {
                "event": "keepalive",
                "source": "heartbeat_loop",
                "job_id": job_id,
                "account_name": db_name,
                "scan_count": scan_val,
                "study_count": heartbeat_study_count,
            }

            try:
                meta = get_last_publish_meta(job_id) or {}
                last_state_ts = float(meta.get("updated_at_ts") or 0.0)
                recent_state_window = max(5.0, min(float(heartbeat_interval) * 0.75, 30.0))
                if last_state_ts <= 0 or (time.time() - last_state_ts) >= recent_state_window:
                    await update_state_only(
                        job_id=job_id,
                        account_name=db_name,
                        payload=state_payload,
                        extra=extra_keepalive,
                    )
            except Exception as exc:
                logger.debug(
                    "[HEARTBEAT] update_state_only ??쎈솭 | job_id=%s err=%s", job_id, exc
                )

            try:
                await publish_client_redis_heartbeat(job_id, db_name)
            except Exception as exc:
                logger.debug(
                    "[HEARTBEAT] publish_client_redis_heartbeat ??쎈솭 | job_id=%s err=%s",
                    job_id,
                    exc,
                )

            try:
                logger.debug(
                    "[HEARTBEAT] Redis 揶쏄퉮??| job_id=%s interval=%ss dense_hb=%s scan=%s save=%s study=%s",
                    job_id,
                    heartbeat_interval,
                    _dense_redis_hb,
                    scan_val,
                    int(stats.get("save_count", 0) or 0),
                    heartbeat_study_count,
                )
            except Exception:
                pass

            try:
                await asyncio.wait_for(
                    asyncio.sleep(heartbeat_interval), timeout=heartbeat_interval
                )
            except asyncio.CancelledError:
                break
            except Exception:
                pass
            if terminal_sse_sent or heartbeat_stop.is_set():
                break

    heartbeat_task = asyncio.create_task(_heartbeat_loop(), name=f"heartbeat:{job_id}")
    try:
        from backend.shared.heartbeat_registry import register_heartbeat
        await register_heartbeat(job_id=job_id, db_name=db_name, stop_event=heartbeat_stop, task=heartbeat_task)
    except Exception:
        pass
    # endregion heartbeat
    # region prestart keepalive (state-only)
    try:
        try:
            prestart_interval = float(os.getenv("SSE_PRESTART_KEEPALIVE_SEC", "2") or "2")
        except Exception:
            prestart_interval = 2.0
        prestart_interval = max(0.5, min(prestart_interval, 10.0))

        async def _prestart_keepalive_loop():
            while not prestart_stop.is_set() and not progress_started.is_set():
                payload = {
                    "status": "running",
                    "scan_count": 0,
                    "total_count": 0,
                    "collection_count": 0,
                    "save_count": 0,
                    "study_count": 0,
                    "timestamp": datetime.now().isoformat(),
                }
                extra = {
                    "event": "prestart_keepalive",
                    "source": "prestart_keepalive",
                    "job_id": job_id,
                    "account_name": db_name,
                }
                try:
                    await update_state_only(
                        job_id=job_id,
                        account_name=db_name,
                        payload=payload,
                        extra=extra,
                    )
                except Exception:
                    pass
                try:
                    publish_payload = {**payload, **extra}
                    enqueue_sse_message(
                        job_id,
                        publish_payload,
                        db_name,
                        "prestart_keepalive",
                        priority=-4,
                    )
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(asyncio.sleep(prestart_interval), timeout=prestart_interval)
                except asyncio.CancelledError:
                    break
                except Exception:
                    pass
                if terminal_sse_sent:
                    break

        prestart_task = asyncio.create_task(_prestart_keepalive_loop(), name=f"prestart-keepalive:{job_id}")
    except Exception:
        prestart_task = None
    # endregion prestart keepalive
    background_drain_ok = True
    strict_counts_fail_detail: str = ""
    zero_scan_fail_detail: str = ""
    crawl_db_cache_token = None
    try:
        if begin_crawl_db_cache is not None:
            crawl_db_cache_token = begin_crawl_db_cache(
                job_id=job_id,
                db_name=db_name,
                chat_bot_id=str(getattr(workflow, "chat_bot_id", "") or ""),
            )
            if prewarm_crawl_db_cache is not None:
                await prewarm_crawl_db_cache(
                    chat_bot_id=str(getattr(workflow, "chat_bot_id", "") or ""),
                    db_name=db_name,
                )
    except Exception:
        crawl_db_cache_token = None
    try:
        from backend.file.integrated_workflow import WorkflowState

        # UI ??뽯뻻???袁⑥쨴???紐낆넎): ??곸몵筌???揶쏅??앮에?????'undefined' ?곗뮆??獄쎻뫗?
        ui_subject = getattr(workflow, "ui_subject", None)
        ui_h3 = getattr(workflow, "ui_h3", None)
        ui_details = getattr(workflow, "ui_details", None)
        ui_colle = getattr(workflow, "ui_colle", None)

        def _file_live_db_progress_enabled() -> bool:
            return True

        def _live_db_progress_flush_interval() -> float:
            return 2.0

        def _schedule_live_db_progress_update(message: Dict[str, Any]) -> None:
            nonlocal live_db_last_counts, live_db_last_update_at
            try:
                if not (_is_file_mode_workflow(workflow) and _file_live_db_progress_enabled()):
                    return
                counts = (
                    int(message.get("scan_count", 0) or 0),
                    int(message.get("collection_count", 0) or 0),
                    int(message.get("save_count", 0) or 0),
                    int(message.get("study_count", 0) or 0),
                )
                now = time.monotonic()
                first_nonzero = live_db_last_counts is None and any(v > 0 for v in counts)
                changed_to_nonzero = bool(live_db_last_counts) and any(
                    int(prev or 0) <= 0 and int(cur or 0) > 0
                    for prev, cur in zip(live_db_last_counts, counts)
                )
                if not (first_nonzero or changed_to_nonzero):
                    if counts == live_db_last_counts:
                        return
                    if now - live_db_last_update_at < _live_db_progress_flush_interval():
                        return
                live_db_last_counts = counts
                live_db_last_update_at = now
            except Exception:
                return

            async def _run() -> None:
                nonlocal live_db_last_counts, live_db_last_update_at
                try:
                    ok = await update_crawling_log_counters(
                        job_id=job_id,
                        scan=counts[0],
                        collection=counts[1],
                        saved=counts[2],
                        study=counts[3],
                        dbname=db_name,
                        log_id=getattr(workflow, "craw_id", None),
                        colle="file",
                        status="start",
                    )
                    if not ok:
                        live_db_last_counts = None
                        live_db_last_update_at = 0.0
                        logger.warning(
                            "[RunWorkflowTask] live crawling_log progress update not applied | "
                            "job_id=%s db=%s craw_id=%s counts=%s",
                            job_id,
                            db_name,
                            getattr(workflow, "craw_id", None),
                            counts,
                        )
                except Exception as exc:
                    live_db_last_counts = None
                    live_db_last_update_at = 0.0
                    logger.debug(
                        "[RunWorkflowTask] live crawling_log progress update failed | job_id=%s err=%s",
                        job_id,
                        exc,
                    )

            task = asyncio.create_task(_run(), name=f"crawling-log-live:{job_id}")
            live_db_update_tasks.add(task)
            task.add_done_callback(lambda t: live_db_update_tasks.discard(t))
        debug_progress_logged = False
        # ??덉뵬 筌욊쑵六???산퉬?猷뱀몵嚥?progress_callback???怨쀫꺗 ?紐꾪뀱????[FILE-STUDY-FAIL] progress 餓λ쵎??獄쎻뫗?
        _last_study_fail_progress_sig: Any = None

        def progress_callback(stats: Dict[str, Any]):
            nonlocal debug_progress_logged, _last_study_fail_progress_sig
            if not progress_started.is_set():
                progress_started.set()
            try:
                is_partial_content_relearn = bool(getattr(workflow, "content_relearn_mode", False))
                save_count = stats.get("save_count", 0)
                collection_count = stats.get("collection_count", 0)
                is_stopping_mode = getattr(workflow, "final_status", None) == "stopped" or getattr(workflow, "state", None) == WorkflowState.STOPPING
                is_saving_in_progress = is_stopping_mode and (save_count < collection_count)
                has_pending_pipeline_work = _workflow_has_pending_pipeline_work(workflow)

                if is_saving_in_progress:
                    current_status = "running"
                elif has_pending_pipeline_work:
                    current_status = "running"
                elif getattr(workflow, "final_status", None):
                    current_status = normalize_status_for_sse(getattr(workflow, "final_status", None))
                elif not getattr(workflow, "is_running", True):
                    current_status = normalize_status_for_sse("complete")
                else:
                    current_status = "running"

                # File crawling learns after save, so the modal must show actual learned count.
                is_file_mode_progress = _is_file_mode_workflow(workflow)
                ui_study_count = stats.get("study_count", 0)
                ui_study_done_count = stats.get("study_done_count", stats.get("study_count", 0))
                ui_study_success_count = stats.get("study_success_count", 0)
                if is_file_mode_progress:
                    ui_study_count = _resolve_effective_study_count(
                        stats,
                        is_file_mode=True,
                    )
                    ui_study_success_count = ui_study_count
                    ui_study_done_count = min(
                        _safe_count_value(stats.get("file_study_done_count", stats.get("study_done_count", 0))) or 0,
                        _safe_count_value(stats.get("save_count", 0)) or 0,
                    )
                if is_file_mode_progress:
                    try:
                        ui_save_success_count = max(0, int(stats.get("save_count", 0) or 0))
                    except Exception:
                        ui_save_success_count = stats.get("save_count", 0)
                    ui_save_done_count = ui_save_success_count
                else:
                    ui_save_success_count = stats.get("save_success_count", stats.get("save_count", 0))
                    ui_save_done_count = stats.get("save_done_count", stats.get("save_count", 0))
                message: Dict[str, Any] = {
                    "status": current_status,
                    "scan_count": stats.get("scan_count", 0),
                    "total_count": stats.get("scan_count", 0),
                    "collection_count": stats.get("collection_count", 0),
                    "save_count": stats.get("save_count", 0),
                    "save_success_count": ui_save_success_count,
                    "save_done_count": ui_save_done_count,
                    "save_failed_count": stats.get("save_failed_count", 0),
                    "study_count": ui_study_count,
                    "study_done_count": ui_study_done_count,
                    "study_success_count": ui_study_success_count,
                    "study_failed_count": stats.get("study_failed_count", 0),
                    "study_skipped_count": stats.get("study_skipped_count", 0),
                    "stats_revision": int(stats.get("stats_revision", 0) or 0),
                    "timestamp": datetime.now().isoformat(),
                    "job_id": job_id,
                    "account_name": db_name,
                    "pending_collection_count": stats.get("pending_collection_count", 0),
                    "pending_save_count": stats.get("pending_save_count", 0),
                    "duplicate_runtime_excluded_count": stats.get("duplicate_runtime_excluded_count", 0),
                    "allow_counter_decrease": bool(stats.get("allow_counter_decrease", False)),
                    "allow_scan_count_decrease": bool(stats.get("allow_scan_count_decrease", False)),
                    "last_counter_decrease_reason": stats.get("last_counter_decrease_reason", ""),
                    "in_flight": stats.get("in_flight", {}),
                    "study_fail_url": stats.get("study_fail_url", ""),
                    "study_issue_samples": stats.get("study_issue_samples", []),
                    "study_skip_samples": stats.get("study_skip_samples", []),
                    # ?醫딇?誘?????쎄땁 ??? (?遺얠쒔域?UI: ?????關???醫딇롨퉪????怨?筌왖)
                    "skip_reasons": {
                        "skip_file_url": int(stats.get("skip_file_url", 0) or 0),
                        "skip_fetch_fail": int(stats.get("skip_fetch_fail", 0) or 0),
                        "skip_date_out_of_range": int(stats.get("skip_date_out_of_range", 0) or 0),
                        "skip_content_too_short": int(stats.get("skip_content_too_short", 0) or 0),
                        "skip_save_fail": int(stats.get("skip_save_fail", 0) or 0),
                    },
                    # ?袁⑥쨴????뽯뻻??筌롫?? (??곸몵筌?''嚥?揶쏅벡??
                    "subject": _clean_ui_text(ui_subject),
                    "h3": _clean_ui_text(ui_h3),
                    "details": _clean_ui_text(ui_details),
                    "colle": _clean_ui_text(ui_colle),
                }
                if is_partial_content_relearn:
                    content_done = int(
                        stats.get("study_success_count", stats.get("study_count", stats.get("save_count", 0))) or 0
                    )
                    message["source"] = "partial_content_relearn"
                    message["event"] = stats.get("event") or "partial_content_progress"
                    message["h3"] = "partial content relearn"
                    message["message"] = f"partial content relearn {content_done}/{stats.get('scan_count', 0) or 0}"
                    message["updated_count"] = content_done
                    message["field_save_counts"] = _field_save_counts_for_source("partial_content_relearn", content_done)
                    message["partial_content_relearn"] = {
                        "scan": int(stats.get("scan_count", 0) or 0),
                        "collection": int(stats.get("collection_count", 0) or 0),
                        "save": int(stats.get("save_count", 0) or 0),
                        "study_success": int(stats.get("study_success_count", 0) or 0),
                        "study_failed": int(stats.get("study_failed_count", 0) or 0),
                        "study_skipped": int(stats.get("study_skipped_count", 0) or 0),
                    }
                # ?醫롮? ?袁り숲 ?類ｋ궖 ??釉?(??덉읅 疫꿸퀗而??뽱뀱??
                try:
                    start_date_str = stats.get("start_date") if stats.get("start_date") is not None else None
                    end_date_str = stats.get("end_date") if stats.get("end_date") is not None else None
                    if start_date_str or end_date_str:
                        message["date_filter_active"] = bool(stats.get("date_filter_active", True))
                        # ???? START ~ END (??餓???롪돌筌???됱몵筌?????揶쏅?彛???뽯뻻)
                        if start_date_str and end_date_str:
                            message["date_range"] = f"{start_date_str} ~ {end_date_str}"
                        elif start_date_str:
                            message["date_range"] = f"{start_date_str} ~ "
                        elif end_date_str:
                            message["date_range"] = f" ~ {end_date_str}"
                        message["start_date"] = start_date_str
                        message["end_date"] = end_date_str
                except Exception:
                    pass

                # ??stop/?袁⑹읈餓λ쵎????뽯뻻 癰귣떯而?(?袁⑥쨴?紐꾨퓠??筌뤿굟????닌됲뀋 揶쎛??
                try:
                    if stats.get("stop_requested") is not None:
                        message["stop_requested"] = bool(stats.get("stop_requested"))
                    if stats.get("stop_level"):
                        message["stop_level"] = stats.get("stop_level")
                    if stats.get("hard_stop") is not None:
                        message["hard_stop"] = bool(stats.get("hard_stop"))
                    if stats.get("stop_reason"):
                        message["stop_reason"] = stats.get("stop_reason")
                    if stats.get("stop_grace_seconds") is not None:
                        message["stop_grace_seconds"] = stats.get("stop_grace_seconds")
                    if stats.get("stop_elapsed_seconds") is not None:
                        message["stop_elapsed_seconds"] = stats.get("stop_elapsed_seconds")
                    # workflow揶쎛 癰귣?沅????源??筌롫뗄?놅쭪?揶쎛 ??됱몵筌??袁⑤뼎(??롫굡餓λ쵎????
                    if stats.get("event"):
                        message["event"] = stats.get("event")
                    if stats.get("message"):
                        message["message"] = stats.get("message")
                except Exception:
                    pass

                if is_saving_in_progress:
                    message["stop_requested"] = True
                    message["status_hint"] = "stopped"
                    message["event"] = "stopping_save"
                    message["message"] = f"餓λ쵎????????筌욊쑵六?餓?.. (??륁춿: {collection_count}, ???? {save_count})"
                elif has_pending_pipeline_work and getattr(workflow, "final_status", None):
                    message["status_hint"] = normalize_status_for_sse(getattr(workflow, "final_status", None))
                    message["event"] = "pipeline_draining"

                if not debug_progress_logged:
                    debug_progress_logged = True

                # ??SSE count ?遺얠쒔繹? progress_callback?癒?퐣 燁삳똻????곕뗄??獄???쑨??
                scan_count = int(message.get("scan_count", 0) or 0)
                collection_count = int(message.get("collection_count", 0) or 0)
                save_count = int(message.get("save_count", 0) or 0)
                study_count = int(message.get("study_count", 0) or 0)
                study_success_count = int(message.get("study_success_count", 0) or 0)
                study_done_count = int(message.get("study_done_count", 0) or 0)
                study_failed_count = int(message.get("study_failed_count", 0) or 0)
                study_skipped_count = int(message.get("study_skipped_count", 0) or 0)
                
                # ??燁삳똻?????쑨???브쑴苑?
                count_comparison = {
                    "scan": scan_count,
                    "collection": collection_count,
                    "save": save_count,
                    "study": study_count,
                    "study_success": study_success_count,
                    "study_done": study_done_count,
                    "study_failed": study_failed_count,
                    "study_skipped": study_skipped_count,
                }
                
                # 燁삳똻????온??野꺜筌?
                warnings = []
                if collection_count > scan_count:
                    warnings.append(f"collection({collection_count}) > scan({scan_count})")
                if save_count > collection_count:
                    warnings.append(f"save({save_count}) > collection({collection_count})")
                if study_count > save_count:
                    warnings.append(f"study({study_count}) > save({save_count})")
                if study_success_count > study_done_count:
                    warnings.append(f"study_success({study_success_count}) > study_done({study_done_count})")
                _done_sum = study_success_count + study_failed_count + study_skipped_count
                if study_done_count != _done_sum:
                    warnings.append(
                        f"study_done({study_done_count}) != study_success({study_success_count}) + "
                        f"study_failed({study_failed_count}) + study_skipped({study_skipped_count})"
                    )
                
                # 筌△뫁???④쑴沅?
                diff_collection_scan = collection_count - scan_count
                diff_save_collection = save_count - collection_count
                diff_study_save = study_count - save_count
                diff_study_success_done = study_success_count - study_done_count
                
                try:
                    if study_failed_count > 0 and not _is_non_failure_study_progress_event(stats.get("event"), stats.get("message")):
                        _colle_ui = str(ui_colle or "").strip().lower()
                        _fail_tag = "[FILE-STUDY-FAIL]" if _colle_ui == "file" else "[STUDY-FAIL]"
                        _fail_sig = (
                            _fail_tag,
                            study_failed_count,
                            study_done_count,
                            study_success_count,
                            current_status,
                            stats.get("stage"),
                            stats.get("event"),
                            stats.get("message"),
                        )
                        if _fail_sig != _last_study_fail_progress_sig:
                            _last_study_fail_progress_sig = _fail_sig
                            logger.warning(
                                "%s progress | job_id=%s status=%s failed=%s done=%s success=%s stage=%s event=%s message=%s",
                                _fail_tag,
                                job_id,
                                current_status,
                                study_failed_count,
                                study_done_count,
                                study_success_count,
                                stats.get("stage"),
                                stats.get("event"),
                                stats.get("message"),
                            )
                except Exception:
                    pass
                
                # ?곕떽? ?遺얠쒔繹? 筌ㅼ뮄??獄쏆뮉六?筌롫??(logged in redis_sse_service) ?類ㅼ뵥
                try:
                    try:
                        meta = get_last_publish_meta(job_id) or {}
                    except Exception:
                        meta = {}
                    if meta:
                        crawl_trace(
                            logger,
                            phase="sse",
                            action="last_publish_meta",
                            state="found",
                            job_id=job_id,
                            level=logging.DEBUG,
                            counts=meta.get("counts"),
                            published_raw=meta.get("published_raw"),
                            state_updated=meta.get("state_updated"),
                            status=meta.get("status"),
                            timestamp=meta.get("timestamp"),
                            channel=meta.get("channel"),
                        )
                        # ?닌됰즴????곸벉??곗쨮 獄쏆뮉六??0??野껋럩??野껋럡??嚥≪뮄?????
                    else:
                        crawl_trace(
                            logger,
                            phase="sse",
                            action="last_publish_meta",
                            state="missing",
                            job_id=job_id,
                            level=logging.DEBUG,
                        )
                except Exception as dbg_exc:
                    crawl_trace(
                        logger,
                        phase="sse",
                        action="last_publish_meta",
                        state="error",
                        job_id=job_id,
                        level=logging.DEBUG,
                        error=str(dbg_exc),
                    )
                
                crawl_trace(
                    logger,
                    phase="sse",
                    action="count_compare",
                    state="end",
                    job_id=job_id,
                    level=logging.DEBUG,
                    counts={
                        "diff_collection_scan": diff_collection_scan,
                        "diff_save_collection": diff_save_collection,
                        "diff_study_save": diff_study_save,
                        "diff_study_success_done": diff_study_success_done,
                    },
                    warnings=warnings if warnings else "none",
                )
                
                sse_priority = -10 if (
                    bool(message.get("stop_requested"))
                    or str(message.get("status") or "").strip().lower() in {"cancelled", "error"}
                    or str(message.get("status_hint") or "").strip().lower() == "stopped"
                    or str(message.get("event") or "").strip().lower() in {"stopping_save", "stop_requested", "hard_stop"}
                ) else 0
                _schedule_live_db_progress_update(message)
                progress_enqueue_t0 = time.perf_counter()
                enqueue_sse_message(job_id, message, db_name, "workflow_progress", priority=sse_priority)
                progress_enqueue_ms = (time.perf_counter() - progress_enqueue_t0) * 1000.0
                try:
                    progress_enqueue_warn_ms = float(os.getenv("WORKFLOW_PROGRESS_ENQUEUE_WARN_MS", "1000") or "1000")
                except Exception:
                    progress_enqueue_warn_ms = 1000.0
                crawl_trace(
                    logger,
                    phase="sse",
                    action="workflow_progress_enqueue",
                    state="end",
                    job_id=job_id,
                    level=logging.WARNING if progress_enqueue_ms >= max(0.0, progress_enqueue_warn_ms) else logging.DEBUG,
                    elapsed_ms=progress_enqueue_ms,
                    counts={**count_comparison, "payload_keys": len(message or {})},
                    status=message.get("status"),
                    priority=sse_priority,
                    source="workflow_progress",
                    warn_ms=max(0.0, progress_enqueue_warn_ms),
                )
            except Exception as e:
                logger.warning("[RunWorkflowTask] Redis send error: %s", e)

        try:
            start_local_time = get_local_now()
        except Exception:
            start_local_time = None

        # ??STOP 疫꿸퀡??筌△뫀??? monitor_auto_stop ??쑵??源딆넅
        # monitor_task = asyncio.create_task(
        #     monitor_auto_stop(
        #         workflow=workflow,
        #         job_id=job_id,
        #         db_name=db_name,
        #         chat_bot_id=chat_bot_id,
        #         stop_signal=monitor_stop,
        #         start_time=start_local_time,
        #     ),
        #     name=f"auto_stop:{job_id}",
        # )
        # 筌뤴뫀??怨뺤춦 ??뽮쉐?? ?癒?짗 餓λ쵎??疫꿸퀗而?燁삳똻????袁⑤뼎) 揶쏅Ŋ????뽰삂
        monitor_task = None
        try:
            if _env_bool("WORKFLOW_AUTO_STOP_ENABLED", "1") and chat_bot_id and db_name:
                try:
                    monitor_task = asyncio.create_task(
                        monitor_auto_stop(
                            workflow=workflow,
                            job_id=job_id,
                            db_name=db_name,
                            chat_bot_id=chat_bot_id,
                            stop_signal=monitor_stop,
                            start_time=start_local_time,
                            source="workflow_runner",
                        ),
                        name=f"auto-stop-monitor:{job_id}",
                    )
                except Exception as exc:
                    logger.warning("[RunWorkflowTask] Failed to start auto-stop monitor | job_id=%s err=%s", job_id, exc)
        except Exception:
            monitor_task = None

        _start_urls_count_log = len(start_urls or [])
        if _start_urls_count_log <= 0:
            try:
                _start_urls_count_log = int(getattr(workflow, "pre_explored_start_urls_count", 0) or 0)
            except Exception:
                _start_urls_count_log = 0
        logger.debug(
            "[START_URLS_TRACE] workflow_runner calling start_workflow | job_id=%s start_urls_count=%s sample=%s",
            job_id, _start_urls_count_log, (start_urls or [])[:5],
        )
        logger.debug(
            "[RunWorkflowTask] calling start_workflow | job_id=%s urls=%s",
            job_id,
            _start_urls_count_log,
        )
        logger.debug(
            "[BottleneckTrace][runner_start_workflow_call] job_id=%s elapsed_ms=%s urls=%s",
            job_id,
            int((time.perf_counter() - prestart_t0) * 1000),
            _start_urls_count_log,
        )
        # ?袁⑥쨴?紐꾨퓠???袁⑤뼎??colle=file ????륁춿 筌뤴뫀諭띄몴???곌쾿???쨮?怨쀫퓠 ?袁⑤뼎 (BoardContentWorkflow ??
        colle_for_start = getattr(workflow, "colle", None) or getattr(workflow, "colle_mode", None) or "board"
        start_workflow_t0 = time.perf_counter()
        crawl_trace(
            logger,
            phase="workflow",
            action="start_workflow_call",
            state="start",
            job_id=job_id,
            counts={"start_urls": _start_urls_count_log},
            colle=colle_for_start,
        )
        start_workflow_kwargs = _filter_start_workflow_kwargs(
            workflow,
            {
                "progress_callback": progress_callback,
                "start_date": start_date,
                "end_date": end_date,
                "use_query_links_only": use_query_links_only,
                "target_domains": getattr(workflow, "target_domains", None),
                "colle": colle_for_start,
            },
        )
        await workflow.start_workflow(start_urls, **start_workflow_kwargs)
        crawl_trace(
            logger,
            phase="workflow",
            action="start_workflow_call",
            state="end",
            job_id=job_id,
            elapsed_ms=elapsed_ms(start_workflow_t0),
            counts=workflow.get_stats() if hasattr(workflow, "get_stats") else {},
        )
        logger.debug(
            "[BottleneckTrace][runner_start_workflow_returned] job_id=%s start_workflow_ms=%s elapsed_ms=%s",
            job_id,
            int((time.perf_counter() - start_workflow_t0) * 1000),
            int((time.perf_counter() - prestart_t0) * 1000),
        )

        background_drain_t0 = time.perf_counter()
        crawl_trace(
            logger,
            phase="workflow",
            action="background_drain",
            state="start",
            job_id=job_id,
        )
        file_frontend_complete_now = (
            _file_workflow_save_stage_complete(workflow)
            and _workflow_has_pending_pipeline_work(workflow)
            and not _file_learning_request_dispatch_pending(workflow)
        )
        if file_frontend_complete_now:
            asyncio.create_task(
                _continue_file_background_after_frontend_complete(
                    workflow=workflow,
                    job_id=job_id,
                    db_name=db_name,
                )
            )
            background_drain_ok = False
        else:
            background_drain_ok = await _drain_workflow_background_tasks(workflow, job_id)
        crawl_trace(
            logger,
            phase="workflow",
            action="background_drain",
            state="end" if background_drain_ok else "slow",
            level=logging.INFO if background_drain_ok else logging.WARNING,
            job_id=job_id,
            elapsed_ms=elapsed_ms(background_drain_t0),
            ok=background_drain_ok,
        )
        if not background_drain_ok:
            logger.warning(
                "[RunWorkflowTask] background work is still running after drain budget; keeping completed status | job_id=%s",
                job_id,
            )
        try:
            reconcile_file_counts = getattr(workflow, "_reconcile_file_study_counts_from_learn_list", None)
            if callable(reconcile_file_counts):
                ret = reconcile_file_counts()
                if inspect.isawaitable(ret):
                    await ret
        except Exception as exc:
            logger.debug(
                "[RunWorkflowTask] file learn count reconcile skipped | job_id=%s err=%s",
                job_id,
                exc,
            )

        final_status = getattr(workflow, "final_status", None) or ("running" if getattr(workflow, "is_running", False) else "completed")
        # ??곌쾿???쨮?怨? final_status??揶쏄퉮???? ??꾪?"running"??곗쨮 ??ｋ┸ 野껋럩?? is_running=False?????袁⑥┷嚥?揶쏄쑴竊?
        if (final_status or "").strip().lower() == "running" and not getattr(workflow, "is_running", False):
            final_status = "completed"

        final_stats = {}
        try:
            final_stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
        except Exception:
            final_stats = {}
        if not final_stats:
            final_stats = getattr(workflow, "stats", {}) or {}
        is_file_mode = _is_file_mode_workflow(workflow)
        final_stats = _apply_stage_terminal_count_adjustments(
            final_stats,
            db_name=db_name,
            job_id=job_id,
            is_file_mode=is_file_mode,
        )

        # ?類ㅼ퐠: ???뵬/野껊슣?????쨌筌?筌뤴뫀紐?scan_count揶쎛 0??곗쨮 ?ル굝利??롢늺 ?얜똻?쒎쳞???쎈솭 筌ｌ꼶??
        try:
            final_scan_count = int(final_stats.get("scan_count", final_stats.get("total_count", 0)) or 0)
        except Exception:
            final_scan_count = 0
        if final_scan_count <= 0:
            zero_scan_fail_detail = f"scan_count={final_scan_count}"
            if (final_status or "").strip().lower() != "error":
                logger.warning(
                    "[RunWorkflowTask] scan_count=0 terminal override -> error | job_id=%s prev_status=%s",
                    job_id,
                    final_status,
                )
            final_status = "error"
            try:
                setattr(workflow, "final_status", "error")
            except Exception:
                pass

        # ???뵬 ??쨌 ?袁⑥┷ 疫꿸퀣? ?紐껎뀋??
        # ?醫딇??袁⑥┷ > ????筌ｋ똾寃?> ?????袁⑥┷ > ??덈뮸 ?袁⑥┷
        # => completed ??됱뒠 鈺곌퀗援? ?醫딇???????덈뮸(?源껊궗)
        try:
            require_equal_on_complete = _env_bool(
                "WORKFLOW_REQUIRE_EQUAL_COUNTS_ON_COMPLETE", "1"
            )
        except Exception:
            require_equal_on_complete = True
        if require_equal_on_complete:
            ok_counts, detail = _check_terminal_save_study_counts(
                final_stats,
                is_file_mode=is_file_mode,
            )
            if not ok_counts:
                strict_counts_fail_detail = detail
                _fsl = (final_status or "").strip().lower()
                if _fsl in STOP_SSE_STATUSES:
                    logger.warning(
                        "[RunWorkflowTask] save/study 燁삳똻????븍뜆?ょ㎉?륁뵠??stop ?怨밴묶???醫? | job_id=%s %s",
                        job_id,
                        detail,
                    )
                else:
                    logger.warning(
                        "[RunWorkflowTask] save/study 移댁슫??遺덉씪移?-> stop 媛뺤젣 | job_id=%s prev_status=%s %s",
                        job_id,
                        final_status,
                        detail,
                    )
                    final_status = "stop"
                    try:
                        setattr(workflow, "final_status", "stop")
                    except Exception:
                        pass

        crawler_state.record_history(
            job_id,
            final_status,
            "workflow_completed",
            db_name,
            chat_bot_id=getattr(workflow, "chat_bot_id", None),
        )
        try:
            hist = crawler_state.job_history.get(job_id)
            if isinstance(hist, dict):
                hist["final_stats"] = dict(final_stats or {})
                hist["terminal_status"] = final_status
            events = crawler_state.job_history_events.get(job_id) or []
            if events and isinstance(events[-1], dict):
                events[-1]["final_stats"] = dict(final_stats or {})
                events[-1]["terminal_status"] = final_status
        except Exception:
            pass

        # DB ?怨밴묶: COMPLETE_SSE_STATUSES/STOP_SSE_STATUSES?? ??덉뵬 疫꿸퀣? ????(?類ㅺ맒?袁⑥┷ ??update_crawling_log_counters ?紐꾪뀱 癰귣똻??
        raw_final_for_db = (final_status or "").strip().lower()
        db_status = None
        if raw_final_for_db in COMPLETE_SSE_STATUSES:
            db_status = "completed"
        elif raw_final_for_db in STOP_SSE_STATUSES:
            db_status = "stop"
        elif raw_final_for_db in {"error", "failed", "fail", "exception"}:
            db_status = "error"

        raw_final_status = getattr(workflow, "final_status", "NOT_SET")

        if db_status:
            pending_db_status = db_status
            pending_db_stats = dict(final_stats or {})
            pending_db_craw_id = getattr(workflow, "craw_id", None) or None

        try:
            raw_final = (final_status or getattr(workflow, "final_status", None) or "").strip().lower()
            if raw_final in STOP_SSE_STATUSES:
                terminal_status = "cancelled"
            elif raw_final in COMPLETE_SSE_STATUSES:
                terminal_status = "completed"
            elif raw_final in {"error", "failed", "fail", "exception"}:
                terminal_status = "error"
            else:
                terminal_status = "completed"

            if zero_scan_fail_detail:
                terminal_status = "error"

            # Modal count: file crawling must show actual learned count, not saved count.
            ui_final_study = _resolve_effective_study_count(
                final_stats,
                is_file_mode=is_file_mode,
            )
            try:
                final_save_cap = max(0, int(final_stats.get("save_count", 0) or 0))
            except Exception:
                final_save_cap = 0
            if is_file_mode:
                ui_final_save_success = final_save_cap
                ui_final_save_done = final_save_cap
                ui_final_study_done = min(
                    _safe_count_value(final_stats.get("file_study_done_count", final_stats.get("study_done_count", 0))) or 0,
                    final_save_cap,
                )
                ui_final_study_success = ui_final_study
            else:
                ui_final_save_success = final_stats.get("save_success_count", final_stats.get("save_count", 0))
                ui_final_save_done = final_stats.get("save_done_count", final_stats.get("save_count", 0))
                ui_final_study_done = final_stats.get("study_done_count", final_stats.get("study_count", 0))
                ui_final_study_success = final_stats.get("study_success_count", 0)

            # Final modal values are aligned with the stage counters used for crawling_log.
            final_message = {
                "status": terminal_status,
                "scan_count": final_stats.get("scan_count", 0),
                "total_count": final_stats.get("scan_count", 0),
                "collection_count": final_stats.get("collection_count", 0),
                "save_count": final_stats.get("save_count", 0),
                "save_success_count": ui_final_save_success,
                "save_done_count": ui_final_save_done,
                "save_failed_count": final_stats.get("save_failed_count", 0),
                "study_count": ui_final_study,
                "study_done_count": ui_final_study_done,
                "study_success_count": ui_final_study_success,
                "study_failed_count": final_stats.get("study_failed_count", 0),
                "study_skipped_count": final_stats.get("study_skipped_count", 0),
                "study_skip_reason": final_stats.get("study_skip_reason", ""),
                "study_skip_url": final_stats.get("study_skip_url", ""),
                "study_skip_detail": final_stats.get("study_skip_detail", ""),
                "study_skip_samples": final_stats.get("study_skip_samples", []),
                "attachment_count": final_stats.get("attachment_count", 0),
                "file_attachment_found_count": final_stats.get("file_attachment_found_count", 0),
                "file_attachment_raw_count": final_stats.get("file_attachment_raw_count", 0),
                "file_attachment_candidate_count": final_stats.get("file_attachment_candidate_count", 0),
                "file_attachment_enqueued_count": final_stats.get("file_attachment_enqueued_count", 0),
                "file_attachment_found_samples": final_stats.get("file_attachment_found_samples", []),
                "file_download_skipped_count": final_stats.get("file_download_skipped_count", 0),
                "file_download_skipped_samples": final_stats.get("file_download_skipped_samples", []),
                "file_duplicate_reuse_learned_count": final_stats.get("file_duplicate_reuse_learned_count", 0),
                "enable_db_save": final_stats.get("enable_db_save", True),
                "file_pipeline_skip_learning": final_stats.get("file_pipeline_skip_learning", False),
                "duplicate_runtime_excluded_count": final_stats.get("duplicate_runtime_excluded_count", 0),
                "allow_counter_decrease": bool(final_stats.get("allow_counter_decrease", False)),
                "allow_scan_count_decrease": bool(final_stats.get("allow_scan_count_decrease", False)),
                "last_counter_decrease_reason": final_stats.get("last_counter_decrease_reason", ""),
                "timestamp": datetime.now().isoformat(),
                "event": "workflow_completed",
                "job_id": job_id,
                "account_name": db_name,
                "subject": _clean_ui_text(ui_subject),
                "h3": _clean_ui_text(ui_h3),
                "details": _clean_ui_text(ui_details),
                "colle": _clean_ui_text(ui_colle),
            }
            try:
                if bool(getattr(workflow, "content_relearn_mode", False)):
                    content_done = int(
                        final_stats.get(
                            "study_success_count",
                            final_stats.get("study_count", final_stats.get("save_count", 0)),
                        )
                        or 0
                    )
                    final_message["source"] = "partial_content_relearn"
                    final_message["h3"] = "partial content relearn"
                    final_message["message"] = f"partial content relearn completed: total={final_stats.get('scan_count', 0) or 0} saved={final_stats.get('save_count', 0) or 0} study={content_done}"
                    final_message["updated_count"] = content_done
                    final_message["field_save_counts"] = _field_save_counts_for_source("partial_content_relearn", content_done)
                    final_message["updated_count"] = content_done
                    final_message["field_save_counts"] = _field_save_counts_for_source(
                        "partial_content_relearn",
                        content_done,
                    )
                    final_message["partial_content_relearn"] = {
                        "scan": int(final_stats.get("scan_count", 0) or 0),
                        "collection": int(final_stats.get("collection_count", 0) or 0),
                        "save": int(final_stats.get("save_count", 0) or 0),
                        "study_success": int(final_stats.get("study_success_count", 0) or 0),
                        "study_failed": int(final_stats.get("study_failed_count", 0) or 0),
                        "study_skipped": int(final_stats.get("study_skipped_count", 0) or 0),
                    }
            except Exception:
                pass
            if not background_drain_ok and not file_frontend_complete_now:
                final_message["background_drain_pending"] = True
                final_message["message"] = (
                    "筌롫뗄???臾믩씜?? ?袁⑥┷??뤿??? ??? 獄쏄퉫???깆뒲???臾믩씜???④쑴???類ｂ봺 餓λ쵐???덈뼄."
                )
            if file_frontend_complete_now:
                final_message["background_followup_pending"] = True
            if zero_scan_fail_detail and final_message.get("status") == "error":
                final_message["event"] = "workflow_zero_scan"
                final_message["message"] = (
                    "?癒?퉳 ??롮쎗??0??곗쨮 ?ル굝利??뤿선 ??쎈솭 筌ｌ꼶???뤿???щ빍?? | "
                    + zero_scan_fail_detail
                )
            elif strict_counts_fail_detail and final_message.get("status") in {"error", "cancelled"}:
                final_message["event"] = "workflow_incomplete_counts"
                final_message["message"] = (
                    "?袁⑥┷ 疫꿸퀣? 沃섎챷?먫?? ??????????덈뮸 ?袁⑥┷揶쎛 ?봔鈺곌퉲釉??stop 筌ｌ꼶???뤿???щ빍?? | "
                    + strict_counts_fail_detail
                )
            # stop level/hard stop ??뽯뻻
            try:
                for k in ("stop_requested", "stop_level", "hard_stop", "stop_reason", "stop_grace_seconds", "stop_elapsed_seconds"):
                    if k in final_stats:
                        final_message[k] = final_stats.get(k)
            except Exception:
                pass
            # ??SSE count ?遺얠쒔繹? ?袁⑥┷ 筌롫뗄?놅쭪? 燁삳똻????곕뗄??(??湲??곗뮆??
            # ???袁⑥┷ ??뽰젎????덈뮸 ??쎈솭 ?遺용튋 嚥≪뮄??
            try:
                final_failed = int(final_message.get("study_failed_count", 0) or 0)
                if final_failed > 0 and not _is_non_failure_study_progress_event(final_stats.get("event"), final_stats.get("message")):
                    _colle_done = str(ui_colle or "").strip().lower()
                    _done_fail_tag = "[FILE-STUDY-FAIL]" if _colle_done == "file" else "[STUDY-FAIL]"
                    logger.warning(
                        "%s completed | job_id=%s status=%s failed=%s done=%s success=%s stage=%s event=%s message=%s",
                        _done_fail_tag,
                        job_id,
                        terminal_status,
                        final_failed,
                        final_message.get("study_done_count", 0),
                        final_message.get("study_success_count", 0),
                        final_stats.get("stage"),
                        final_stats.get("event"),
                        final_stats.get("message"),
                    )
            except Exception:
                pass
            # The terminal DB row must receive exactly the counters sent in the terminal Redis/SSE payload.
            if pending_db_status:
                terminal_payload_study = _safe_count_value(final_message.get("study_count")) or 0
                pending_db_stats = {
                    "scan_count": _safe_count_value(final_message.get("scan_count")) or 0,
                    "collection_count": _safe_count_value(final_message.get("collection_count")) or 0,
                    "save_count": _safe_count_value(final_message.get("save_count")) or 0,
                    "study_count": terminal_payload_study,
                    "file_study_success_count": terminal_payload_study,
                    "study_success_count": terminal_payload_study,
                }
            pending_terminal_message = dict(final_message)
            pending_terminal_source = "workflow_completed"
            pending_terminal_status = terminal_status
            # ???袁⑥┷/餓λ쵎???癒?쑎 "?遺용튋 嚥≪뮄??????湲???ｋ┸??(??????遺욧퍕)
            # ???袁⑸뻻 筌△뫀?? COMPLETED_EVENT 嚥≪뮄???곗뮆??筌△뫀??
            # Capture crawled URLs if available.

        except Exception as e:
            logger.exception("[RunWorkflowTask] Workflow error for job_id=%s: %s", job_id, e)
            crawler_state.record_history(job_id, "failed_to_start", str(e), db_name, chat_bot_id=getattr(workflow, "chat_bot_id", None))
            # Capture crawled URLs if available.
            try:
                crawled_list = getattr(workflow, "_crawled_urls_snapshot", None) or []
                if not crawled_list:
                    seen_scan = getattr(workflow, "_seen_scan", None)
                    crawled_list = list(seen_scan) if seen_scan else []
                if not crawled_list:
                    try:
                        ss = getattr(workflow, "seen_scan_urls", None)
                        if ss:
                            crawled_list = list(ss)
                    except Exception:
                        pass
                if crawled_list:
                    save_crawled_urls_report(
                        db_name=db_name,
                        crawled_urls=crawled_list,
                        job_id=job_id,
                        target_domains=getattr(workflow, "target_domains", None),
                        status="error",
                    )
            except Exception:
                pass
            try:
                final_stats = {}
                try:
                    final_stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
                except Exception:
                    final_stats = {}
                if not final_stats:
                    final_stats = getattr(workflow, "stats", {}) or {}
                db_study = _resolve_effective_study_count(
                    final_stats,
                    is_file_mode=_is_file_mode_workflow(workflow),
                )
            
                crawl_trace(
                    logger,
                    phase="db",
                    action="error_status_update",
                    state="before",
                    job_id=job_id,
                    level=logging.DEBUG,
                    counts={
                        "scan": final_stats.get("scan_count", 0),
                        "collection": final_stats.get("collection_count", 0),
                        "save": final_stats.get("save_count", 0),
                        "study": final_stats.get("study_count", 0),
                        "study_success": final_stats.get("study_success_count", 0),
                        "db_study": db_study,
                    },
                    status="error",
                )
            
                await update_crawling_log_counters(
                    job_id=job_id,
                    scan=final_stats.get("scan_count"),
                    collection=final_stats.get("collection_count"),
                    saved=final_stats.get("save_count"),
                    study=db_study,
                    status="error",
                    dbname=db_name,
                    log_id=getattr(workflow, "craw_id", None),
                )
            
                crawl_trace(
                    logger,
                    phase="db",
                    action="error_status_update",
                    state="after",
                    job_id=job_id,
                    level=logging.DEBUG,
                    counts={
                        "scan": final_stats.get("scan_count", 0),
                        "collection": final_stats.get("collection_count", 0),
                        "save": final_stats.get("save_count", 0),
                        "db_study": db_study,
                    },
                    status="error",
                )
            except Exception as db_err:
                logger.error("[RunWorkflowTask] Failed to update DB status to 'error' for job_id=%s: %s", job_id, db_err)
    finally:
        try:
            if end_crawl_db_cache is not None:
                end_crawl_db_cache(crawl_db_cache_token)
        except Exception:
            pass
        prestart_stop.set()
        if prestart_task:
            prestart_task.cancel()
            try:
                await prestart_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        heartbeat_stop.set()
        if heartbeat_task:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        try:
            from backend.shared.heartbeat_registry import unregister_heartbeat
            await unregister_heartbeat(job_id)
        except Exception:
            pass
        monitor_stop.set()
        if monitor_task:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        if live_db_update_tasks:
            try:
                await asyncio.gather(*list(live_db_update_tasks), return_exceptions=True)
            except Exception:
                pass
        # print(f"\n" + "=" * 50)
        # print(f"?猶??臾믩씜 ?ル굝利?workflow_runner (Job ID: {job_id})")
        # print(f"=" * 50 + "\n", flush=True)
        crawl_trace(
            logger,
            phase="workflow",
            action="run_workflow_task",
            state="end",
            job_id=job_id,
            elapsed_ms=elapsed_ms(workflow_t0),
            db=db_name,
            final_status=getattr(workflow, "final_status", None),
        )
        # ???ル굝利???뽰젎 ?怨멸쉭 ?遺얠쒔繹?筌ㅼ뮇伊??怨밴묶/燁삳똻?????쎈???)
        try:
            raw_final = (getattr(workflow, "final_status", None) or "").strip().lower()
            try:
                final_stats_debug = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
            except Exception:
                final_stats_debug = {}
            if not final_stats_debug:
                final_stats_debug = getattr(workflow, "stats", {}) or {}
            reason_parts = []
            try:
                if final_stats_debug.get("stop_reason"):
                    reason_parts.append(f"stop_reason={final_stats_debug.get('stop_reason')}")
                if final_stats_debug.get("event"):
                    reason_parts.append(f"event={final_stats_debug.get('event')}")
                if final_stats_debug.get("message"):
                    reason_parts.append(f"message={final_stats_debug.get('message')}")
                if getattr(workflow, "_auto_stop_reason", None):
                    reason_parts.append(f"auto_stop_reason={getattr(workflow, '_auto_stop_reason', None)}")
                if getattr(workflow, "_hard_stop_reason", None):
                    reason_parts.append(f"hard_stop_reason={getattr(workflow, '_hard_stop_reason', None)}")
                if getattr(workflow, "_auto_stop_as_completed", None):
                    reason_parts.append("auto_stop_as_completed=true")
                if getattr(workflow, "_stop_requested", None):
                    reason_parts.append("stop_requested=true")
            except Exception:
                pass
            finish_reason = " | ".join([p for p in reason_parts if p]) or "n/a"
            logger.warning(
                "[BOARD_DASHBOARD_DEBUG][Finish] job_id=%s final_status=%s state=%s reason=%s "
                "scan=%s coll=%s save=%s save_done=%s save_succ=%s save_fail=%s "
                "study=%s study_done=%s study_succ=%s study_fail=%s skipped=%s stage=%s override_source=%s workflow=%s",
                job_id,
                raw_final,
                getattr(getattr(workflow, "state", None), "name", getattr(workflow, "state", None)),
                finish_reason,
                final_stats_debug.get("scan_count"),
                final_stats_debug.get("collection_count"),
                final_stats_debug.get("save_count"),
                final_stats_debug.get("save_done_count"),
                final_stats_debug.get("save_success_count"),
                final_stats_debug.get("save_failed_count"),
                final_stats_debug.get("study_count"),
                final_stats_debug.get("study_done_count"),
                final_stats_debug.get("study_success_count"),
                final_stats_debug.get("study_failed_count"),
                final_stats_debug.get("study_skipped_count"),
                final_stats_debug.get("stage"),
                getattr(workflow, "start_urls_override_source", ""),
                type(workflow).__name__ if workflow is not None else "",
            )
        except Exception as debug_exc:
            logger.warning(
                "[BOARD_DASHBOARD_DEBUG][Finish] logging failed | job_id=%s err=%s",
                job_id,
                debug_exc,
            )

        if pending_db_status:
            try:
                pending_study_value = _resolve_effective_study_count(
                    pending_db_stats,
                    is_file_mode=_is_file_mode_workflow(workflow),
                )
                terminal_db_t0 = time.perf_counter()
                crawl_trace(
                    logger,
                    phase="db",
                    action="terminal_crawling_log_update",
                    state="start",
                    job_id=job_id,
                    status=pending_db_status,
                    counts={
                        "scan": pending_db_stats.get("scan_count", 0),
                        "collection": pending_db_stats.get("collection_count", 0),
                        "save": pending_db_stats.get("save_count", 0),
                        "study": pending_study_value,
                    },
                )
                async def _deferred_terminal_db_update() -> None:
                    await update_crawling_log_counters(
                        job_id=job_id,
                        scan=pending_db_stats.get("scan_count"),
                        collection=pending_db_stats.get("collection_count"),
                        saved=pending_db_stats.get("save_count"),
                        study=pending_study_value,
                        status=pending_db_status,
                        log_id=pending_db_craw_id,
                        dbname=db_name,
                    )

                await _deferred_terminal_db_update()
                pending_db_flushed = True
                crawl_trace(
                    logger,
                    phase="db",
                    action="terminal_crawling_log_update",
                    state="end",
                    job_id=job_id,
                    elapsed_ms=elapsed_ms(terminal_db_t0),
                    status=pending_db_status,
                )
            except Exception as db_err:
                crawl_trace(
                    logger,
                    phase="db",
                    action="terminal_crawling_log_update",
                    state="fail",
                    job_id=job_id,
                    level=logging.WARNING,
                    status=pending_db_status,
                    error=db_err,
                )
                logger.error(
                    "[RunWorkflowTask] Deferred DB status update failed | job_id=%s status=%s err=%s",
                    job_id,
                    pending_db_status,
                    db_err,
                )

        if _env_bool("WORKFLOW_FORCE_TERMINATE_AFTER_FINISH", "1"):
            if background_drain_ok:
                try:
                    await _force_terminate_job_after_finish(workflow=workflow, job_id=job_id, db_name=db_name)
                except Exception:
                    pass
            else:
                logger.warning(
                    "[RunWorkflowTask] skip force terminate because background drain is still pending | job_id=%s",
                    job_id,
                )

        if pending_db_status and not pending_db_flushed:
            try:
                pending_study_value = _resolve_effective_study_count(
                    pending_db_stats,
                    is_file_mode=_is_file_mode_workflow(workflow),
                )
                terminal_db_t0 = time.perf_counter()
                await update_crawling_log_counters(
                    job_id=job_id,
                    scan=pending_db_stats.get("scan_count"),
                    collection=pending_db_stats.get("collection_count"),
                    saved=pending_db_stats.get("save_count"),
                    study=pending_study_value,
                    status=pending_db_status,
                    log_id=pending_db_craw_id,
                    dbname=db_name,
                )
                pending_db_flushed = True
                crawl_trace(
                    logger,
                    phase="db",
                    action="terminal_crawling_log_update",
                    state="end",
                    job_id=job_id,
                    elapsed_ms=elapsed_ms(terminal_db_t0),
                    status=pending_db_status,
                    retry=True,
                )
            except Exception as db_err:
                logger.error(
                    "[RunWorkflowTask] terminal DB update retry failed | job_id=%s status=%s err=%s",
                    job_id,
                    pending_db_status,
                    db_err,
                )

        sync_terminal_with_backend = _env_bool("WORKFLOW_SYNC_FRONTEND_BACKEND_COMPLETION", "1")
        force_terminate_after_finish = _env_bool("WORKFLOW_FORCE_TERMINATE_AFTER_FINISH", "1")

        if workflow_slot_acquired:
            try:
                await crawler_state.release_workflow_slot(job_id)
                workflow_slot_acquired = False
            except Exception as exc:
                logger.debug("[RunWorkflowTask] workflow slot release failed | job_id=%s err=%s", job_id, exc)

        if distributed_lock_acquired and distributed_lock_key:
            try:
                from db.db_redis import get_redis

                redis = await get_redis()
                raw_owner = await redis.get(distributed_lock_key)
                if isinstance(raw_owner, (bytes, bytearray)):
                    raw_owner = raw_owner.decode("utf-8", errors="replace")
                if str(raw_owner or "").strip() == str(job_id or "").strip():
                    await redis.delete(distributed_lock_key)
            except Exception as exc:
                logger.debug(
                    "[RunWorkflowTask][DuplicateLock] release failed | job_id=%s err=%s",
                    job_id,
                    exc,
                )
            distributed_lock_acquired = False

        try:
            from backend.shared.crawl_start import release_burst_dedupe_lock

            await release_burst_dedupe_lock(job_id)
        except Exception as exc:
            logger.debug("[RunWorkflowTask] burst dedupe release failed | job_id=%s err=%s", job_id, exc)

        if sync_terminal_with_backend and not force_terminate_after_finish:
            try:
                await _cleanup_workflow_state_after_finish(
                    workflow=workflow,
                    job_id=job_id,
                    db_name=db_name,
                    delay_sec=0,
                    history_detail="sync_cleanup_after_finish",
                )
            except Exception:
                pass

        if not terminal_sse_sent:
            try:
                if pending_terminal_message is not None:
                    terminal_status = str(
                        pending_terminal_status
                        or pending_terminal_message.get("status")
                        or "completed"
                    ).strip().lower()
                    final_message = dict(pending_terminal_message)
                    if zero_scan_fail_detail and terminal_status == "error":
                        final_message["event"] = "workflow_zero_scan"
                        final_message["message"] = (
                            "?癒?퉳 ??롮쎗??0??곗쨮 ?ル굝利??뤿선 ??쎈솭 筌ｌ꼶???뤿???щ빍?? | "
                            + zero_scan_fail_detail
                        )
                    elif strict_counts_fail_detail and terminal_status in {"error", "cancelled"}:
                        final_message["event"] = "workflow_incomplete_counts"
                        final_message["message"] = (
                            "?袁⑥┷ 疫꿸퀣? 沃섎챷?먫?? ??????????덈뮸 ?袁⑥┷揶쎛 ?봔鈺곌퉲釉??stop 筌ｌ꼶???뤿???щ빍?? | "
                            + strict_counts_fail_detail
                        )
                else:
                    raw_final = (getattr(workflow, "final_status", None) or "").strip().lower()
                    if raw_final in STOP_SSE_STATUSES:
                        terminal_status = "cancelled"
                    elif raw_final in COMPLETE_SSE_STATUSES:
                        terminal_status = "completed"
                    elif raw_final in {"error", "failed", "fail", "exception"}:
                        terminal_status = "error"
                    else:
                        terminal_status = "completed"

                    try:
                        final_stats = workflow.get_stats() if hasattr(workflow, "get_stats") else {}
                    except Exception:
                        final_stats = {}
                    if not final_stats:
                        final_stats = getattr(workflow, "stats", {}) or {}
                    final_stats = _apply_stage_terminal_count_adjustments(
                        final_stats,
                        db_name=db_name,
                        job_id=job_id,
                        is_file_mode=_is_file_mode_workflow(workflow),
                    )
                    ok_counts, detail = _check_terminal_save_study_counts(
                        final_stats,
                        is_file_mode=_is_file_mode_workflow(workflow),
                    )
                    if not ok_counts and terminal_status == "completed":
                        terminal_status = "cancelled"
                        strict_counts_fail_detail = strict_counts_fail_detail or detail

                    # ??study_count: study_success_count ?怨쀪퐨, ??곸몵筌?study_count ????
                    finally_study_count = _resolve_effective_study_count(
                        final_stats,
                        is_file_mode=_is_file_mode_workflow(workflow),
                    )

                    final_message = {
                        "status": terminal_status,
                        "scan_count": final_stats.get("scan_count", 0),
                        "total_count": final_stats.get("scan_count", 0),
                        "collection_count": final_stats.get("collection_count", 0),
                        "save_count": final_stats.get("save_count", 0),
                        "study_count": finally_study_count,
                        "timestamp": datetime.now().isoformat(),
                        "event": "workflow_completed",
                        "job_id": job_id,
                        "account_name": db_name,
                        "subject": _clean_ui_text(ui_subject),
                        "h3": _clean_ui_text(ui_h3),
                        "details": _clean_ui_text(ui_details),
                        "colle": _clean_ui_text(ui_colle),
                    }
                    try:
                        finally_scan_count = int(
                            final_stats.get("scan_count", final_stats.get("total_count", 0)) or 0
                        )
                    except Exception:
                        finally_scan_count = 0
                    if finally_scan_count <= 0:
                        terminal_status = "error"
                        if not zero_scan_fail_detail:
                            zero_scan_fail_detail = f"scan_count={finally_scan_count}"
                    final_message["status"] = terminal_status

                backend_pending_after_cleanup = _workflow_has_pending_pipeline_work(workflow)
                if sync_terminal_with_backend and backend_pending_after_cleanup:
                    raw_terminal_status = str(final_message.get("status") or terminal_status or "").strip().lower()
                    if raw_terminal_status == "completed":
                        terminal_status = "error"
                        final_message["status"] = terminal_status
                        final_message["event"] = "workflow_backend_pending"
                        final_message["message"] = (
                            "獄쏄퉮肉???類ｂ봺揶쎛 ?袁⑹춦 ??멸돌筌왖 ??녿툡 ?袁⑥┷ 筌ｌ꼶???? ??녿릭??щ빍?? "
                            "??? 獄쏄퉫???깆뒲???臾믩씜???ル굝利????쇰퓠筌??袁⑥┷嚥?揶쏄쑴竊??몃빍??"
                        )
                        final_message["backend_pending_after_cleanup"] = True

                try:
                    _terminal_status_for_ui = str(final_message.get("status") or terminal_status or "").strip().lower()
                    _terminal_event_for_ui = str(final_message.get("event") or "").strip()
                    if _terminal_status_for_ui in {"completed", "cancelled", "error"} and _terminal_event_for_ui != "workflow_completed":
                        if _terminal_event_for_ui:
                            final_message.setdefault("terminal_reason_event", _terminal_event_for_ui)
                        final_message["event"] = "workflow_completed"
                except Exception:
                    pass

                try:
                    await update_state_only(
                        job_id=job_id,
                        account_name=db_name,
                        payload=final_message,
                        extra={"source": "workflow_terminal_state", "event": "workflow_completed"},
                    )
                except Exception as state_exc:
                    logger.warning(
                        "[RunWorkflowTask] terminal state update failed before SSE publish | job_id=%s err=%s",
                        job_id,
                        state_exc,
                    )

                if not bool(getattr(workflow, "suppress_terminal_sse", False)):
                    terminal_sse_t0 = time.perf_counter()
                    crawl_trace(
                        logger,
                        phase="redis",
                        action="terminal_sse_publish",
                        state="start",
                        job_id=job_id,
                        mode="async_queue",
                        status=final_message.get("status"),
                    )
                    enqueue_sse_message(
                        job_id,
                        final_message,
                        db_name,
                        pending_terminal_source if pending_terminal_message is not None else "workflow_completed:finally",
                        priority=-10,
                    )
                    terminal_sse_sent = True
                    try:
                        await await_sse_publish_idle(
                            job_id,
                            timeout_sec=float(os.getenv("WORKFLOW_TERMINAL_SSE_FLUSH_TIMEOUT_SEC", "5") or "5"),
                        )
                    except Exception:
                        pass
                    crawl_trace(
                        logger,
                        phase="redis",
                        action="terminal_sse_publish",
                        state="queued",
                        job_id=job_id,
                        elapsed_ms=elapsed_ms(terminal_sse_t0),
                        status=final_message.get("status"),
                        mode="async_queue",
                    )
                # ??餓λ쵎???源놁몵嚥??類ㅺ맒 野껋럥以덄몴???筌왖 筌륁궢六?????즲 crawled_*.json ??밴쉐
                try:
                    crawled_list = getattr(workflow, "_crawled_urls_snapshot", None) or []
                    if not crawled_list:
                        seen_scan = getattr(workflow, "_seen_scan", None)
                        crawled_list = list(seen_scan) if seen_scan else []
                    if crawled_list:
                        save_crawled_urls_report(
                            db_name=db_name,
                            crawled_urls=crawled_list,
                            job_id=job_id,
                            target_domains=getattr(workflow, "target_domains", None),
                            status=terminal_status,
                        )
                except Exception as crawl_report_err:
                    logger.warning("[RunWorkflowTask] Crawled URL report save failed (finally) | job_id=%s err=%s", job_id, crawl_report_err)
                try:
                    schedule_job_result_report(
                        workflow=workflow,
                        job_id=job_id,
                        db_name=db_name,
                        status=terminal_status,
                    )
                except Exception as job_report_err:
                    logger.warning("[RunWorkflowTask] Job result report schedule failed | job_id=%s err=%s", job_id, job_report_err)
            except Exception as exc:
                logger.warning("[RunWorkflowTask] Failed to ensure terminal SSE in finally (ignore) | job_id=%s err=%s", job_id, exc)

        if not force_terminate_after_finish and not sync_terminal_with_backend:
            # ?袁⑥┷ ???類ｂ봺 ?臾믩씜?? detach (揶쏅벡???ル굝利?筌뤴뫀諭뜹첎? ?袁⑤빜 ???춸)
            try:
                delay_sec_default = 60
                try:
                    delay_sec = int(os.getenv("WORKFLOW_CLEANUP_DELAY_SEC", str(delay_sec_default)) or delay_sec_default)
                except Exception:
                    delay_sec = delay_sec_default
                delay_sec = max(0, min(int(delay_sec), 3600))
            except Exception:
                delay_sec = 60

            async def _cleanup_after_delay():
                await _cleanup_workflow_state_after_finish(
                    workflow=workflow,
                    job_id=job_id,
                    db_name=db_name,
                    delay_sec=delay_sec,
                    history_detail="auto_cleanup_after_finish",
                )

            try:
                asyncio.create_task(_cleanup_after_delay(), name=f"workflow-cleanup:{job_id}")
            except Exception:
                await _cleanup_workflow_state_after_finish(
                    workflow=workflow,
                    job_id=job_id,
                    db_name=db_name,
                    delay_sec=delay_sec,
                    history_detail="auto_cleanup_after_finish_fallback",
                )



