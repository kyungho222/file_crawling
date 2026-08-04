"""MariaDB persistence helpers for crawler learning data."""
import asyncio
import contextvars
import hashlib
import json
import os
import logging
import re
import time
import socket
import ipaddress
import ssl
from datetime import datetime
from typing import Optional, Dict, Tuple, Set, Any, Iterable
from urllib.parse import parse_qsl, urlencode, unquote, unquote_plus, urlparse, urlunparse
from urllib.request import Request as UrlRequest, urlopen
from backend.shared.sub_cate_mode import (
    get_sub_cate_mode_from_config,
    is_sub_cate_overwrite,
    should_update_category_field,
)
from backend.shared.learn_list_url_row_cache import (
    find_loaded_learn_list_row_in_url_cache,
    find_learn_list_row_in_url_cache,
    remember_learn_list_url_row,
)
from backend.shared.crawl_trace import crawl_trace
from db.crawl_db_manager import update_crawling_log_counters
from db.mysql_db_config import (
    mysql_execute_query,
    mysql_upsert_then_last_insert_id,
    mysql_user_lock_run,
)
from db.maria_operations import maria_insert_data, maria_execute_query
from config.settings import (
    get_storage_domain_for_db_name,
    normalize_access_url,
    get_uploaded_files_web_url,
    get_file_download_path,
    get_file_upload_content_url,
)
from utils.attachment_url_normalize import (
    canonicalize_attachment_url_for_learn_list,
    extract_attachment_key_candidates,
    sql_like_contains_pattern,
)
from utils.url import (
    build_dedup_candidate_terms,
    canonicalize_url_for_dedup,
    urls_match_for_dedup,
)

_LEARN_LIST_HASH_KEYS_BLOCKED = frozenset({
    "file_hash", "content_hash", "url_hash", "chunk_hash",
})

_LEARN_LIST_DB_COLUMNS = frozenset({
    "id",
    "content",
    "subject",
    "content_type",
    "status",
    "size",
    "chunk",
    "cate1",
    "cate2",
    "memo1",
    "content_address",
    "created_at",
    "content_created_at",
    "content_updated_at",
    "content_author",
    "img_describe",
    "video_summary",
    "video_text",
    "video_thumbnail",
    "video_time",
    "hash",
    "segments",
    "embedding_tokens",
    "source_url",
    "keyword1",
    "keyword2",
    "keyword3",
    "keyword4",
    "keyword5",
    "keyword6",
    "keyword7",
    "keyword8",
    "keyword9",
    "keyword10",
})

_LEARN_LIST_VISIBLE_WRITE_COLUMNS = _LEARN_LIST_DB_COLUMNS - {"id"}
_LEARN_LIST_OPERATIONAL_WRITE_COLUMNS = frozenset()

_CATEGORY_CHILD_TREECODE_STEPS = (3, 4)
_FILE_UPDATE_ONLY_MAPPING_CACHE: Dict[Tuple[str, str, str], Tuple[str, str]] = {}
_learn_list_table_exists_cache: Dict[Tuple[str, str], bool] = {}
_existing_table_name_cache: Dict[Tuple[str, Tuple[str, ...]], Optional[str]] = {}
_learn_list_status_success_cache: Dict[Tuple[str, str, str], float] = {}
_crawl_db_cache_var: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar("mariadb_save_update_crawl_cache", default=None)


def begin_crawl_db_cache(*, job_id: str = "", db_name: str = "", chat_bot_id: str = "") -> contextvars.Token:
    cache: Dict[str, Any] = {
        "job_id": str(job_id or ""),
        "db_name": str(db_name or ""),
        "chat_bot_id": str(chat_bot_id or ""),
        "category_rows": {},
        "table_columns": {},
        "table_exists": {},
        "existing_table_name": {},
        "config": {},
    }
    return _crawl_db_cache_var.set(cache)


def end_crawl_db_cache(token: Optional[contextvars.Token] = None) -> None:
    try:
        if token is not None:
            _crawl_db_cache_var.reset(token)
        else:
            _crawl_db_cache_var.set(None)
    except Exception:
        try:
            _crawl_db_cache_var.set(None)
        except Exception:
            pass


def _current_crawl_db_cache() -> Optional[Dict[str, Any]]:
    try:
        cache = _crawl_db_cache_var.get()
    except Exception:
        cache = None
    return cache if isinstance(cache, dict) else None


async def prewarm_crawl_db_cache(*, chat_bot_id: str = "", db_name: str = "") -> None:
    cache = _current_crawl_db_cache()
    if cache is None or not db_name:
        return
    try:
        if chat_bot_id:
            category_table = get_category_table_name(chat_bot_id)
            await _fetch_category_rows_cached(category_table=category_table, db_name=db_name)
    except Exception as exc:
        logger.debug("[CrawlDBCache] category prewarm skipped | db=%s chat_bot_id=%s err=%s", db_name, chat_bot_id, exc)
    try:
        if chat_bot_id:
            account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
            learn_list_table = get_learn_list_table_name(account_identifier)
            await _learn_list_table_exists(db_name, learn_list_table)
            await _get_table_columns(db_name, learn_list_table)
    except Exception as exc:
        logger.debug("[CrawlDBCache] learn_list prewarm skipped | db=%s chat_bot_id=%s err=%s", db_name, chat_bot_id, exc)


def _learn_list_status_verify_enabled() -> bool:
    raw = str(os.getenv("BOARD_LEARN_LIST_STATUS_VERIFY", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _learn_list_status_success_cache_ttl_sec() -> float:
    try:
        value = float(os.getenv("BOARD_LEARN_LIST_STATUS_SUCCESS_CACHE_TTL_SEC", "300") or "300")
    except Exception:
        value = 300.0
    return max(0.0, min(value, 3600.0))


def _learn_list_status_success_cache_key(db_name: str, table_name: str, db_id: str) -> Tuple[str, str, str]:
    return (str(db_name or ""), str(table_name or ""), str(db_id or ""))


def _learn_list_status_success_cache_hit(db_name: str, table_name: str, db_id: str) -> bool:
    ttl = _learn_list_status_success_cache_ttl_sec()
    if ttl <= 0:
        return False
    key = _learn_list_status_success_cache_key(db_name, table_name, db_id)
    ts = _learn_list_status_success_cache.get(key)
    if not ts:
        return False
    if time.time() - ts <= ttl:
        return True
    _learn_list_status_success_cache.pop(key, None)
    return False


def _remember_learn_list_status_success(db_name: str, table_name: str, db_id: str) -> None:
    ttl = _learn_list_status_success_cache_ttl_sec()
    if ttl <= 0:
        return
    if len(_learn_list_status_success_cache) > 10000:
        cutoff = time.time() - ttl
        stale_keys = [key for key, ts in _learn_list_status_success_cache.items() if ts < cutoff]
        for key in stale_keys[:2000]:
            _learn_list_status_success_cache.pop(key, None)
    _learn_list_status_success_cache[_learn_list_status_success_cache_key(db_name, table_name, db_id)] = time.time()


def _category_direct_child_lengths(parent_treecode: str) -> Tuple[int, ...]:
    parent = str(parent_treecode or "").strip()
    if not parent:
        return tuple()
    return tuple(len(parent) + step for step in _CATEGORY_CHILD_TREECODE_STEPS)


def _filter_learn_list_visible_write_data(
    data: Dict[str, Any],
    cols: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Keep UI-visible fields plus required LEARN_LIST operational columns."""
    allowed = _LEARN_LIST_VISIBLE_WRITE_COLUMNS | _LEARN_LIST_OPERATIONAL_WRITE_COLUMNS
    if cols:
        allowed = allowed.intersection(cols)
    return {k: v for k, v in (data or {}).items() if k in allowed}


def _learn_list_visible_column_allowed(column: str, cols: Optional[Set[str]] = None) -> bool:
    return column in _LEARN_LIST_VISIBLE_WRITE_COLUMNS and (not cols or column in cols)


def _extract_unknown_column_from_db_error(exc: Exception) -> str:
    try:
        text = str(exc or "")
    except Exception:
        text = ""
    match = re.search(r"Unknown column ['`\"]?([^'`\"\s]+)['`\"]?", text, flags=re.IGNORECASE)
    if match:
        return str(match.group(1) or "").strip("`'\" ")
    return ""

# Legacy comment removed because the original text was encoding-corrupted.
_CATE_ARG_UNSET = object()

logger = logging.getLogger("db.mariadb_save_update")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(levelname)s] %(name)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
_LOG_LEVEL_NAME = str(os.getenv("MARIADB_SAVE_LOG_LEVEL", "INFO") or "INFO").strip().upper()
logger.setLevel(getattr(logging, _LOG_LEVEL_NAME, logging.INFO))
logger.propagate = False
SONGPA_TITLE_TRACE_PREFIX = "[SongpaTitleTrace]"
FILE_CATEGORY_TRACE_PREFIX = "[파일분류추적]"


def _clip_category_trace_value(value: Any, limit: int = 180) -> str:
    try:
        text = str(value if value is not None else "")
    except Exception:
        text = repr(value)
    text = " ".join(text.split())
    if len(text) > limit:
        return text[:limit] + "..."
    return text


def log_file_category_trace(stage: str, **fields: Any) -> None:
    try:
        parts = [f"{key}={_clip_category_trace_value(value)}" for key, value in (fields or {}).items()]
        logger.info("%s 단계=%s %s", FILE_CATEGORY_TRACE_PREFIX, stage, " ".join(parts))
    except Exception:
        pass


def _is_songpa_title_trace_url(url: str = "") -> bool:
    return "songpa.go.kr" in str(url or "").lower()


def _songpa_title_trace(stage: str, *, url: str = "", **fields: Any) -> None:
    if not _is_songpa_title_trace_url(url):
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
                fp.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {SONGPA_TITLE_TRACE_PREFIX} module=mariadb_save_update stage={stage} url={str(url or '')[:240]} fields={compact}\n")
        except Exception:
            pass
    except Exception:
        pass


def _content_author_debug_enabled() -> bool:
    return str(
        os.getenv("CONTENT_AUTHOR_DEBUG", os.getenv("FILE_CONTENT_AUTHOR_DEBUG", "0"))
        or "0"
    ).strip().lower() in {"1", "true", "yes", "on"}


def _content_author_debug(message: str, *args: Any) -> None:
    try:
        logger.debug(message, *args)
    except Exception:
        pass


def _content_author_debug_value(value: Any, limit: int = 180) -> str:
    try:
        text = str(value or "").strip()
    except Exception:
        text = ""
    return text[:limit]


def _db_load_debug_enabled() -> bool:
    return str(os.getenv("DB_LOAD_DEBUG", "0") or "0").strip().lower() in ("1", "true", "yes", "on")


def _db_load_slow_ms() -> float:
    try:
        return max(0.0, float(os.getenv("DB_LOAD_SLOW_QUERY_MS", "300") or "300"))
    except Exception:
        return 300.0

# Legacy comment removed because the original text was encoding-corrupted.
_file_insert_backpressure_state: Dict[str, Any] = {
    "level": 0,
    "last_update_ts": 0.0,
    "hold_until_ts": 0.0,
}


def _file_insert_backpressure_enabled() -> bool:
    v = (os.getenv("FILE_LEARN_LIST_BACKPRESSURE_ENABLE") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def _file_insert_backpressure_base_ms() -> int:
    try:
        return max(0, int(os.getenv("FILE_LEARN_LIST_BACKPRESSURE_BASE_MS", "80") or "80"))
    except Exception:
        return 80


def _file_insert_backpressure_max_ms() -> int:
    try:
        return max(0, int(os.getenv("FILE_LEARN_LIST_BACKPRESSURE_MAX_MS", "1200") or "1200"))
    except Exception:
        return 1200


def _file_insert_backpressure_decay_sec() -> float:
    try:
        return max(1.0, float(os.getenv("FILE_LEARN_LIST_BACKPRESSURE_DECAY_SEC", "20") or "20"))
    except Exception:
        return 20.0


def _file_insert_backpressure_delay_ms_from_level(level: int) -> int:
    if level <= 0:
        return 0
    base = _file_insert_backpressure_base_ms()
    cap = _file_insert_backpressure_max_ms()
    # 80, 160, 320, 640, 1200, ...
    return min(cap, int(base * (2 ** max(0, level - 1))))


def _decay_file_insert_backpressure_state(now_ts: float) -> None:
    st = _file_insert_backpressure_state
    last_ts = float(st.get("last_update_ts") or 0.0)
    level = int(st.get("level") or 0)
    if level <= 0:
        st["last_update_ts"] = now_ts
        st["hold_until_ts"] = 0.0
        return
    elapsed = max(0.0, now_ts - last_ts)
    decay_sec = _file_insert_backpressure_decay_sec()
    if elapsed >= decay_sec:
        drop = int(elapsed // decay_sec)
        level = max(0, level - drop)
        st["level"] = level
    st["last_update_ts"] = now_ts
    if level <= 0:
        st["hold_until_ts"] = 0.0


def _observe_file_insert_latency_for_backpressure(total_ms: int, url: Any) -> None:
    if not _file_insert_backpressure_enabled() or not _is_file_download_like_url(url):
        return
    now_ts = time.monotonic()
    _decay_file_insert_backpressure_state(now_ts)
    st = _file_insert_backpressure_state
    prev_level = int(st.get("level") or 0)
    level = prev_level

    slow_ms = _slow_insert_warn_threshold_ms(url)
    recover_ms = max(300, int(slow_ms * 0.5))

    if total_ms > slow_ms:
        level = min(6, level + 1)
    elif total_ms < recover_ms and level > 0:
        level = max(0, level - 1)

    st["level"] = level
    st["last_update_ts"] = now_ts
    if total_ms > slow_ms and level > 0:
        hold_sec = min(3.0, _file_insert_backpressure_delay_ms_from_level(level) / 1000.0)
        st["hold_until_ts"] = max(float(st.get("hold_until_ts") or 0.0), now_ts + hold_sec)

    if level != prev_level:
        learn_list_file_dup_debug_log(
            "[Backpressure] level changed | previous=%s current=%s total_ms=%s url=%s",
            prev_level,
            level,
            total_ms,
            _debug_insert_value(url),
        )


async def _maybe_apply_file_insert_backpressure(url: Any) -> None:
    if not _file_insert_backpressure_enabled() or not _is_file_download_like_url(url):
        return
    now_ts = time.monotonic()
    _decay_file_insert_backpressure_state(now_ts)
    st = _file_insert_backpressure_state
    level = int(st.get("level") or 0)
    if level <= 0:
        return

    delay_ms = _file_insert_backpressure_delay_ms_from_level(level)
    hold_until = float(st.get("hold_until_ts") or 0.0)
    if hold_until > now_ts:
        delay_ms = max(delay_ms, int((hold_until - now_ts) * 1000))
    if delay_ms <= 0:
        return

    # Legacy comment removed because the original text was encoding-corrupted.
    await asyncio.sleep(delay_ms / 1000.0)


def learn_list_file_dup_debug_enabled() -> bool:
    """LEARN_LIST 중복 처리 디버그 여부를 반환한다."""
    v = (os.getenv("FILE_CRAWL_DUP_DEBUG") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def learn_list_file_dup_debug_log(msg: str, *args: Any) -> None:
    if not learn_list_file_dup_debug_enabled():
        return
    logger.debug("[LEARN_LIST][file_dup] " + msg, *args)

def learn_list_insert_debug_enabled() -> bool:
    """LEARN_LIST insert path tracing. Enabled by default while debugging input failures."""
    v = (os.getenv("DEBUG_LEARN_LIST_INSERT") or "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def learn_list_insert_debug_log(msg: str, *args: Any, level: int = logging.DEBUG) -> None:
    if learn_list_insert_debug_enabled():
        logger.log(level, "[LEARN_LIST][insert_debug] " + msg, *args)


def db_save_trace_enabled() -> bool:
    v = (os.getenv("DEBUG_DB_SAVE_TRACE") or "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def db_save_trace_log(msg: str, *args: Any, level: int = logging.DEBUG, exc_info: bool = False) -> None:
    if db_save_trace_enabled():
        logger.log(level, "[DB-SAVE-TRACE] " + msg, *args, exc_info=exc_info)


def board_save_flow_trace_enabled() -> bool:
    v = (os.getenv("BOARD_SAVE_FLOW_TRACE") or "1").strip().lower()
    return v in ("1", "true", "yes", "on")


def _board_save_flow_info_all_enabled() -> bool:
    v = (os.getenv("BOARD_SAVE_FLOW_INFO_ALL") or "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def _board_save_flow_slow_ms() -> float:
    try:
        return max(0.0, float(os.getenv("BOARD_SAVE_FLOW_SLOW_MS", "1000") or "1000"))
    except Exception:
        return 1000.0


def board_save_flow_trace(
    action: str,
    state: str,
    *,
    started_at: Optional[float] = None,
    level: Optional[int] = None,
    **fields: Any,
) -> None:
    if not board_save_flow_trace_enabled():
        return
    elapsed_value: Optional[float] = None
    if started_at is not None:
        elapsed_value = (time.perf_counter() - started_at) * 1000.0
    effective_level = level
    if effective_level is None:
        is_slow = elapsed_value is not None and elapsed_value >= _board_save_flow_slow_ms()
        if is_slow:
            effective_level = logging.WARNING
        else:
            effective_level = logging.DEBUG
    if elapsed_value is not None:
        fields.setdefault("elapsed_ms", elapsed_value)
        fields.setdefault("slow", elapsed_value >= _board_save_flow_slow_ms())
    crawl_trace(
        logger,
        phase="save",
        action=action,
        state=state,
        level=effective_level,
        **fields,
    )


def _debug_insert_value(value: Any, limit: int = 220) -> str:
    try:
        text = str(value or "")
    except Exception:
        text = ""
    if len(text) > limit:
        return text[:limit] + "..."
    return text


def _learn_list_batch_debug_sample(values: Iterable[Any], limit: int = 5) -> list[str]:
    sample: list[str] = []
    try:
        for value in values or []:
            sample.append(_debug_insert_value(value, 180))
            if len(sample) >= max(1, int(limit or 5)):
                break
    except Exception:
        pass
    return sample


def _learn_list_batch_debug_log(event: str, **fields: Any) -> None:
    if not learn_list_file_dup_debug_enabled():
        return
    safe_fields = {
        str(key): _debug_insert_value(value, 300)
        for key, value in (fields or {}).items()
    }
    logger.debug(
        "[LEARN_LIST][batch_debug] event=%s fields=%s",
        event,
        safe_fields,
    )


def _insert_value_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


async def _prepare_learn_list_insert_payload(
    table_name: str,
    data: Dict[str, Any],
    db_name: str,
) -> Dict[str, Any]:
    """Fill required LEARN_LIST operational columns after visible-column filtering."""
    working = dict(data or {})
    if "LEARN_LIST" not in str(table_name or "").upper():
        return working

    try:
        cols = await _get_table_columns(db_name, table_name)
    except Exception:
        cols = set()

    def has_col(name: str) -> bool:
        return not cols or name in cols

    added: list[str] = []

    if has_col("status") and _insert_value_missing(working.get("status")):
        working["status"] = "N"
        added.append("status=N")

    if has_col("created_at") and _insert_value_missing(working.get("created_at")):
        working["created_at"] = datetime.now()
        added.append("created_at=now")

    if has_col("content_type") and _insert_value_missing(working.get("content_type")):
        working["content_type"] = "url"
        added.append(f"content_type={working['content_type']}")

    if added:
        learn_list_insert_debug_log(
            "payload_defaults table=%s added=%s keys=%s content=%s",
            table_name,
            ",".join(added),
            ",".join(sorted(str(k) for k in working.keys())),
            _debug_insert_value(working.get("content")),
        )

    return working


def _is_file_download_like_url(url: Any) -> bool:
    try:
        s = str(url or "").strip().lower()
    except Exception:
        return False
    if not s:
        return False
    return any(tok in s for tok in ("filedown", "downloadbbsfile", "atchmnflno", "atchfileid", "atchfile"))


def _slow_insert_warn_threshold_ms(url: Any) -> int:
    env_name = "FILE_LEARN_LIST_SLOW_INSERT_WARN_MS" if _is_file_download_like_url(url) else "LEARN_LIST_SLOW_INSERT_WARN_MS"
    default_val = "10000" if _is_file_download_like_url(url) else "1500"
    try:
        return max(0, int(os.getenv(env_name, default_val) or default_val))
    except Exception:
        return int(default_val)


def _log_learn_list_insert_slow(total_ms: int, url: Any, message: str, *args: Any) -> None:
    _observe_file_insert_latency_for_backpressure(total_ms, url)
    if total_ms <= _slow_insert_warn_threshold_ms(url):
        return
    logger.warning(message, *args)


def _learn_list_insert_slow_ms_segments(
    started: float,
    cp_after_cols: float,
    cp_after_unique: Optional[float],
    before_upsert: float,
    ended: float,
) -> str:
    """?占?占쏙옙 INSERT ?占?占쏙옙 ?占쎈뗄???ms)."""
    try:
        parts = [
            f"setup_to_cols={int((cp_after_cols - started) * 1000)}ms",
        ]
        if cp_after_unique is not None:
            parts.append(f"unique_idx={int((cp_after_unique - cp_after_cols) * 1000)}ms")
            parts.append(f"pre_upsert={int((before_upsert - cp_after_unique) * 1000)}ms")
        else:
            parts.append(f"pre_upsert={int((before_upsert - cp_after_cols) * 1000)}ms")
        parts.append(f"upsert_stmt={int((ended - before_upsert) * 1000)}ms")
        return " | ".join(parts)
    except Exception:
        return "(timing n/a)"


# =================================================================
# Legacy comment removed because the original text was encoding-corrupted.
# =================================================================
def _normalize_url_for_db(url: str) -> str:
    """DB ????占?鈺곌퀬?????????占쎈뮉 ??? ?占?占쏙옙??"""
    if not url: return ""
    try:
        return canonicalize_url_for_dedup(url)
    except Exception:
        return url


def _pick_board_url_key_column(cols: Set[str]) -> str:
    if "content" in cols:
        return "content"
    return "content"


def _board_strong_key_skip_like_enabled() -> bool:
    raw = str(os.getenv("BOARD_DEDUP_STRONG_KEY_SKIP_LIKE", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _board_save_duplicate_skip_like_enabled() -> bool:
    raw = str(os.getenv("BOARD_SAVE_DEDUP_SKIP_LIKE", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _board_strong_key_skip_unindexed_exact_enabled() -> bool:
    raw = str(os.getenv("BOARD_DEDUP_STRONG_KEY_SKIP_UNINDEXED_EXACT", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _board_dedup_lookup_index_check_enabled() -> bool:
    raw = str(os.getenv("BOARD_DEDUP_LOOKUP_INDEX_CHECK", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _board_dedup_allow_unindexed_exact_enabled() -> bool:
    raw = str(os.getenv("BOARD_DEDUP_ALLOW_UNINDEXED_EXACT", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _board_dedup_content_like_enabled() -> bool:
    raw = str(os.getenv("BOARD_DEDUP_CONTENT_LIKE_ENABLED", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _file_category_count_query_enabled() -> bool:
    raw = str(os.getenv("FILE_CATEGORY_COUNT_QUERY_ENABLED", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _board_contract_skip_exact_enabled() -> bool:
    raw = str(os.getenv("BOARD_DEDUP_CONTRACT_SKIP_EXACT", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _board_learn_list_batch_normalized_lookup_enabled() -> bool:
    raw = str(os.getenv("BOARD_LEARN_LIST_BATCH_NORMALIZED_LOOKUP", "0") or "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _extract_board_post_identity(url: Any) -> Optional[Dict[str, str]]:
    try:
        raw = str(url or "").strip()
        parsed = urlparse(raw)
        pairs = parse_qsl(parsed.query or "", keep_blank_values=True)
    except Exception:
        return None
    if not pairs:
        return None
    params: Dict[str, str] = {}
    for key, value in pairs:
        k = str(key or "").strip().lower()
        if k and k not in params:
            params[k] = str(value or "").strip()
    identity_kind = "bbs"
    board_code = params.get("q_bbscode") or params.get("bbscode") or params.get("bbs_code")
    post_sn = (
        params.get("q_bbscttsn")
        or params.get("bbscttsn")
        or params.get("bbsctt_sn")
        or params.get("nttno")
        or params.get("nttid")
    )
    if not board_code or not post_sn:
        nftc_code = params.get("q_nftcbbscode") or params.get("nftcbbscode")
        nftc_mgtno = params.get("q_nftcbbsmgtno") or params.get("nftcbbsmgtno") or params.get("q_nftcbbsmgt_no")
        if nftc_code and nftc_mgtno:
            identity_kind = "nftc"
            board_code = nftc_code
            post_sn = nftc_mgtno
    if not board_code or not post_sn:
        contract_mng_no = (
            params.get("ctrtacctbookmngno")
            or params.get("ctrt_acct_book_mng_no")
            or params.get("ctrtacctbookmng_no")
        )
        if contract_mng_no:
            identity_kind = "contract"
            board_code = "contract"
            post_sn = contract_mng_no
    if not board_code or not post_sn:
        lobas_mng_no = params.get("q_cntrctmngno") or params.get("cntrctmngno")
        lobas_code = params.get("q_lobastcmbbscode") or params.get("lobastcmbbscode")
        lobas_full_ty = params.get("q_fullty") or params.get("fullty") or ""
        if lobas_mng_no and lobas_code:
            identity_kind = "lobas_tcm"
            board_code = lobas_code
            post_sn = lobas_mng_no
            if lobas_full_ty:
                board_code = f"{board_code}:{lobas_full_ty}"
    if not board_code or not post_sn:
        return None
    host = str(parsed.netloc or "").strip().lower()
    if "@" in host:
        host = host.rsplit("@", 1)[-1]
    if ":" in host:
        host = host.split(":", 1)[0]
    return {
        "scheme": parsed.scheme or "https",
        "host": host,
        "path": parsed.path or "",
        "board_code": board_code,
        "post_sn": post_sn,
        "kind": identity_kind,
    }


def _build_board_post_exact_url_variants(url: Any, *, limit: int = 40) -> list[str]:
    raw = str(url or "").strip()
    ident = _extract_board_post_identity(raw)
    if not ident:
        return [raw] if raw else []

    scheme = ident.get("scheme") or "https"
    host = ident.get("host") or ""
    path = ident.get("path") or ""
    board_code = ident.get("board_code") or ""
    post_sn = ident.get("post_sn") or ""
    kind = ident.get("kind") or "bbs"

    hosts: list[str] = []
    for candidate_host in (host, host[4:] if host.startswith("www.") else f"www.{host}" if host else ""):
        if candidate_host and candidate_host not in hosts:
            hosts.append(candidate_host)

    paths: list[str] = []
    path_candidates = (
        (
            path,
            "/pt/disclosure/bidContractInfo/contractInfo/contractView.do",
            "/pt/disclosure/bidContractInfo/contractInfo/accountView.do",
            "/disclosure/bidContractInfo/contractInfo/contractView.do",
            "/disclosure/bidContractInfo/contractInfo/accountView.do",
        )
        if kind == "contract"
        else (
            path,
            "/pt/user/lobasTcm/BD_selectLobasTcmBbsDetail.do",
            "/user/lobasTcm/BD_selectLobasTcmBbsDetail.do",
        )
        if kind == "lobas_tcm"
        else (
            path,
            "/__bbs_detail__/BD_selectNftcBbsDetail.do",
            "/pt/user/nftcBbs/BD_selectNftcBbsDetail.do",
            "/user/nftcBbs/BD_selectNftcBbsDetail.do",
            "/www/user/nftcBbs/BD_selectNftcBbsDetail.do",
        )
        if kind == "nftc"
        else (
            path,
            "/__bbs_detail__/BD_selectBbs.do",
            "/pt/user/bbs/BD_selectBbs.do",
            "/user/bbs/BD_selectBbs.do",
            "/www/user/bbs/BD_selectBbs.do",
        )
    )
    for candidate_path in path_candidates:
        if candidate_path and candidate_path not in paths:
            paths.append(candidate_path)

    if kind == "contract":
        query_variants = [
            urlencode((("ctrtAcctBookMngNo", post_sn),)),
            urlencode((("ctrtacctbookmngno", post_sn),)),
        ]
    elif kind == "lobas_tcm":
        lobas_code, _, lobas_full_ty = board_code.partition(":")
        query_variants = [
            urlencode((("q_cntrctMngNo", post_sn), ("q_fullTy", lobas_full_ty or "1001"), ("q_lobasTcmBbsCode", lobas_code))),
            urlencode((("q_cntrctmngno", post_sn), ("q_fullty", lobas_full_ty or "1001"), ("q_lobastcmbbscode", lobas_code))),
            urlencode((("q_lobasTcmBbsCode", lobas_code), ("q_fullTy", lobas_full_ty or "1001"), ("q_cntrctMngNo", post_sn))),
        ]
    elif kind == "nftc":
        query_variants = [
            urlencode((("q_nftcBbsCode", board_code), ("q_nftcBbsMgtno", post_sn))),
            urlencode((("q_nftcbbscode", board_code), ("q_nftcbbsmgtno", post_sn))),
        ]
    else:
        query_variants = [
            urlencode((("q_bbsCode", board_code), ("q_bbscttSn", post_sn))),
            urlencode((("q_bbscode", board_code), ("q_bbscttsn", post_sn))),
        ]

    variants: list[str] = []

    def add(value: str) -> None:
        value = str(value or "").strip()
        if value and value not in variants:
            variants.append(value)

    add(raw)
    try:
        add(canonicalize_url_for_dedup(raw) or raw)
    except Exception:
        pass
    for candidate_host in hosts:
        for candidate_path in paths:
            for query in query_variants:
                add(urlunparse((scheme, candidate_host, candidate_path, "", query, "")))
                if scheme != "https":
                    add(urlunparse(("https", candidate_host, candidate_path, "", query, "")))
    return variants[: max(1, int(limit or 40))]


async def _ensure_learn_list_duplicate_lookup_index(
    *,
    db_name: str,
    table_name: str,
    key_col: str,
) -> bool:
    """野껊슣?占썸묾? 餓λ쵎?占썲칰???WHERE key_col = ? ORDER BY id DESC LIMIT 1 鈺곌퀬????紐껊쑔??? 癰귣똻???占쎈뼄."""
    if not db_name or not table_name or not key_col:
        return False
    cache_key = (str(db_name), str(table_name), str(key_col))
    if cache_key in _learn_list_duplicate_lookup_index_ready:
        return _learn_list_duplicate_lookup_index_ready[cache_key]
    if not _board_dedup_lookup_index_check_enabled():
        _learn_list_duplicate_lookup_index_ready[cache_key] = False
        return False

    async with _get_table_lock(str(db_name), str(table_name)):
        if cache_key in _learn_list_duplicate_lookup_index_ready:
            return _learn_list_duplicate_lookup_index_ready[cache_key]
        try:
            existing = await mysql_execute_query(
                """
                SELECT index_name
                FROM information_schema.statistics
                WHERE table_schema = %s
                  AND table_name = %s
                  AND column_name = %s
                LIMIT 1
                """,
                (db_name, table_name, key_col),
                fetch=True,
                dbname=db_name,
                op_name="schema_index_check:learn_list_duplicate_lookup",
            )
            if existing:
                _learn_list_duplicate_lookup_index_ready[cache_key] = True
                return True
        except Exception as exc:
            logger.debug(
                "[LearnList][DuplicateIndex] existing index check failed | db=%s table=%s col=%s err=%s",
                db_name,
                table_name,
                key_col,
                exc,
            )

        _learn_list_duplicate_lookup_index_ready[cache_key] = False
        logger.debug(
            "[LearnList][DuplicateIndex] runtime index creation disabled | db=%s table=%s col=%s",
            db_name,
            table_name,
            key_col,
        )
        return False


async def _find_existing_board_row_by_normalized_url(
    *,
    db_name: str,
    table_name: str,
    cols: Set[str],
    candidate_url: str,
    select_columns: Iterable[str],
    limit: int = 80,
    cache_only: bool = False,
    skip_like: bool = False,
    job_id: str = "",
) -> Optional[Dict[str, Any]]:
    lookup_t0 = time.perf_counter()
    candidate = str(candidate_url or "").strip()
    if not candidate or not table_name or not cols:
        return None
    if "content" not in cols:
        return None

    key_col = _pick_board_url_key_column(cols)
    crawl_trace(
        logger,
        phase="selection",
        action="learn_list_duplicate_lookup",
        state="start",
        level=logging.DEBUG,
        db=db_name,
        table=table_name,
        key_col=key_col,
        cache_only=cache_only,
        skip_like=skip_like,
        url=candidate,
    )
    index_ready = False
    if not cache_only:
        try:
            index_ready = await _ensure_learn_list_duplicate_lookup_index(
                db_name=db_name,
                table_name=table_name,
                key_col=key_col,
            )
        except Exception:
            logger.debug(
                "[LearnList][DuplicateIndex] ensure failed before lookup | db=%s table=%s col=%s",
                db_name,
                table_name,
                key_col,
                exc_info=True,
            )
    select_list: list[str] = []
    seen_cols: set[str] = set()
    date_like_select_cols = {"created_at", "updated_at", "content_created_at", "content_updated_at"}
    for col in [*list(select_columns or ()), key_col]:
        name = str(col or "").strip()
        if not name or name in seen_cols or name not in cols:
            continue
        seen_cols.add(name)
        if name in date_like_select_cols:
            select_list.append(f"CAST(`{name}` AS CHAR) AS `{name}`")
        else:
            select_list.append(f"`{name}`")
    if not select_list:
        select_list.append(f"`{key_col}`")

    try:
        if cache_only:
            cache_row = find_loaded_learn_list_row_in_url_cache(
                db_name=db_name,
                table_name=table_name,
                candidate_url=candidate,
                job_id=job_id,
                ignore_ttl=True,
            )
        else:
            cache_row = await find_learn_list_row_in_url_cache(
                db_name=db_name,
                table_name=table_name,
                columns=tuple(select_columns or ()),
                candidate_url=candidate,
                available_cols=cols,
                job_id=job_id,
            )
        if cache_row:
            lookup_total_ms = (time.perf_counter() - lookup_t0) * 1000.0
            crawl_trace(
                logger,
                phase="selection",
                action="learn_list_duplicate_lookup",
                state="end",
                level=logging.DEBUG,
                elapsed_ms=lookup_total_ms,
                db=db_name,
                table=table_name,
                source="memory",
                hit=True,
                row_id=_safe_row_get(cache_row, "id"),
                url=candidate,
            )
            if _db_load_debug_enabled():
                logger.debug(
                    "[DBLoad][BoardDedup] memory hit | db=%s table=%s col=%s url=%s row_id=%s",
                    db_name,
                    table_name,
                    key_col,
                    candidate[:180],
                    _safe_row_get(cache_row, "id"),
                )
            return cache_row
    except Exception as cache_exc:
        logger.debug(
            "[DBLoad][BoardDedup] memory lookup skipped | db=%s table=%s url=%s err=%s",
            db_name,
            table_name,
            candidate[:180],
            cache_exc,
        )
    if cache_only:
        crawl_trace(
            logger,
            phase="selection",
            action="learn_list_duplicate_lookup",
            state="end",
            level=logging.DEBUG,
            elapsed_ms=(time.perf_counter() - lookup_t0) * 1000.0,
            db=db_name,
            table=table_name,
            source="memory",
            hit=False,
            cache_only=True,
            url=candidate,
        )
        return None

    exact_variants = _build_board_post_exact_url_variants(candidate)
    identity = _extract_board_post_identity(candidate)
    strong_identity = bool(identity)
    skip_unindexed_exact = (
        (not index_ready)
        and _board_strong_key_skip_unindexed_exact_enabled()
        and (strong_identity or not _board_dedup_allow_unindexed_exact_enabled())
    )
    skip_contract_exact = (
        bool(identity and identity.get("kind") == "contract")
        and _board_contract_skip_exact_enabled()
    )
    exact_ms = 0.0
    exact_variant_count = len(exact_variants or [])
    if not (skip_unindexed_exact or skip_contract_exact):
        exact_t0 = time.perf_counter()
        exact_candidates = [v for v in exact_variants if v]
        if candidate and candidate not in exact_candidates:
            exact_candidates.insert(0, candidate)
        placeholders = ", ".join(["%s"] * len(exact_candidates))
        exact_rows = await mysql_execute_query(
            (
                f"SELECT {', '.join(select_list)} FROM `{table_name}` "
                f"WHERE `{key_col}` IN ({placeholders}) ORDER BY `id` DESC LIMIT 1"
            ),
            tuple(exact_candidates),
            fetch=True,
            dbname=db_name,
        ) if exact_candidates else []
        exact_ms = (time.perf_counter() - exact_t0) * 1000.0
        if exact_rows:
            row = exact_rows[0]
            if isinstance(row, dict):
                try:
                    remember_learn_list_url_row(db_name=db_name, table_name=table_name, row=row, job_id=job_id)
                except Exception:
                    pass
                total_ms = (time.perf_counter() - lookup_t0) * 1000.0
                crawl_trace(
                    logger,
                    phase="selection",
                    action="learn_list_duplicate_lookup",
                    state="slow" if total_ms >= _db_load_slow_ms() else "end",
                    level=logging.WARNING if total_ms >= _db_load_slow_ms() else logging.DEBUG,
                    elapsed_ms=total_ms,
                    db=db_name,
                    table=table_name,
                    source="exact",
                    hit=True,
                    exact_ms=round(exact_ms, 1),
                    variant_count=len(exact_candidates),
                    row_id=_safe_row_get(row, "id"),
                    url=candidate,
                )
                if _db_load_debug_enabled() or exact_ms >= _db_load_slow_ms():
                    logger.log(
                        logging.WARNING if exact_ms >= _db_load_slow_ms() else logging.DEBUG,
                        "[DBLoad][BoardDedup] exact hit | db=%s table=%s col=%s exact_ms=%.1f total_ms=%.1f variants=%s url=%s",
                        db_name,
                        table_name,
                        key_col,
                        exact_ms,
                        total_ms,
                        len(exact_candidates),
                        candidate[:180],
                    )
                return row
        if _board_strong_key_skip_like_enabled() and strong_identity:
            total_ms = (time.perf_counter() - lookup_t0) * 1000.0
            crawl_trace(
                logger,
                phase="selection",
                action="learn_list_duplicate_lookup",
                state="end",
                level=logging.DEBUG,
                elapsed_ms=total_ms,
                db=db_name,
                table=table_name,
                source="exact_variant",
                hit=False,
                reason="strong_key_skip_like",
                exact_ms=round(exact_ms, 1),
                variant_count=len(exact_candidates),
                url=candidate,
            )
            if _db_load_debug_enabled():
                logger.debug(
                    "[DBLoad][BoardDedup] strong key LIKE skipped | db=%s table=%s col=%s exact_ms=%.1f variants=%s url=%s",
                    db_name,
                    table_name,
                    key_col,
                    exact_ms,
                    len(exact_candidates),
                    candidate[:180],
                )
            return None
    elif _db_load_debug_enabled():
        logger.debug(
            "[DBLoad][BoardDedup] exact skipped for strong identity | db=%s table=%s col=%s reason=%s variants=%s url=%s",
            db_name,
            table_name,
            key_col,
            "contract" if skip_contract_exact else "unindexed",
            exact_variant_count,
            candidate[:180],
        )

    exact_lookup_skipped = bool(skip_unindexed_exact or skip_contract_exact)

    if exact_lookup_skipped and strong_identity and _board_strong_key_skip_like_enabled():
        total_ms = (time.perf_counter() - lookup_t0) * 1000.0
        crawl_trace(
            logger,
            phase="selection",
            action="learn_list_duplicate_lookup",
            state="end",
            level=logging.DEBUG,
            elapsed_ms=total_ms,
            db=db_name,
            table=table_name,
            source="exact_skipped",
            hit=False,
            reason="strong_key_skip_like",
            exact_ms=round(exact_ms, 1),
            variant_count=exact_variant_count,
            url=candidate,
        )
        if _db_load_debug_enabled():
            logger.debug(
                "[DBLoad][BoardDedup] strong key LIKE skipped after exact skip | db=%s table=%s col=%s reason=%s variants=%s url=%s",
                db_name,
                table_name,
                key_col,
                "contract" if skip_contract_exact else "unindexed",
                exact_variant_count,
                candidate[:180],
            )
        return None

    if skip_like and not exact_lookup_skipped:
        total_ms = (time.perf_counter() - lookup_t0) * 1000.0
        crawl_trace(
            logger,
            phase="selection",
            action="learn_list_duplicate_lookup",
            state="end",
            level=logging.DEBUG,
            elapsed_ms=total_ms,
            db=db_name,
            table=table_name,
            source="exact",
            hit=False,
            reason="skip_like",
            exact_ms=round(exact_ms, 1),
            variant_count=exact_variant_count,
            url=candidate,
        )
        if _db_load_debug_enabled():
            logger.debug(
                "[DBLoad][BoardDedup] LIKE skipped | db=%s table=%s col=%s exact_ms=%.1f variants=%s url=%s",
                db_name,
                table_name,
                key_col,
                exact_ms,
                exact_variant_count,
                candidate[:180],
            )
        return None

    if not _board_dedup_content_like_enabled():
        total_ms = (time.perf_counter() - lookup_t0) * 1000.0
        crawl_trace(
            logger,
            phase="selection",
            action="learn_list_duplicate_lookup",
            state="end",
            level=logging.DEBUG,
            elapsed_ms=total_ms,
            db=db_name,
            table=table_name,
            source="like_skipped",
            hit=False,
            reason="content_like_disabled",
            exact_ms=round(exact_ms, 1),
            variant_count=exact_variant_count,
            url=candidate,
        )
        return None

    probe_terms = build_dedup_candidate_terms(candidate)
    if not probe_terms:
        if _db_load_debug_enabled():
            logger.debug(
                "[DBLoad][BoardDedup] miss no_probe_terms | db=%s table=%s col=%s exact_ms=%.1f url=%s",
                db_name,
                table_name,
                key_col,
                exact_ms,
                candidate[:180],
            )
        crawl_trace(
            logger,
            phase="selection",
            action="learn_list_duplicate_lookup",
            state="end",
            level=logging.DEBUG,
            elapsed_ms=(time.perf_counter() - lookup_t0) * 1000.0,
            db=db_name,
            table=table_name,
            source="exact",
            hit=False,
            reason="no_probe_terms",
            exact_ms=round(exact_ms, 1),
            url=candidate,
        )
        return None

    where_sql = " AND ".join(
        [f"LOWER(`{key_col}`) LIKE %s ESCAPE '!'" for _ in probe_terms]
    )
    params = tuple(sql_like_contains_pattern(term) for term in probe_terms)
    like_t0 = time.perf_counter()
    rows = await mysql_execute_query(
        (
            f"SELECT {', '.join(select_list)} FROM `{table_name}` "
            f"WHERE `{key_col}` IS NOT NULL AND {where_sql} "
            f"ORDER BY `id` DESC LIMIT {max(1, int(limit or 80))}"
        ),
        params,
        fetch=True,
        dbname=db_name,
    )
    like_ms = (time.perf_counter() - like_t0) * 1000.0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        db_url = _safe_row_get(row, key_col)
        if db_url and urls_match_for_dedup(candidate, str(db_url).strip()):
            try:
                remember_learn_list_url_row(db_name=db_name, table_name=table_name, row=row, job_id=job_id)
            except Exception:
                pass
            total_ms = (time.perf_counter() - lookup_t0) * 1000.0
            crawl_trace(
                logger,
                phase="selection",
                action="learn_list_duplicate_lookup",
                state="slow" if total_ms >= _db_load_slow_ms() else "end",
                level=logging.WARNING if total_ms >= _db_load_slow_ms() else logging.DEBUG,
                elapsed_ms=total_ms,
                db=db_name,
                table=table_name,
                source="like",
                hit=True,
                exact_ms=round(exact_ms, 1),
                like_ms=round(like_ms, 1),
                candidate_rows=len(rows or []),
                row_id=_safe_row_get(row, "id"),
                url=candidate,
            )
            if _db_load_debug_enabled() or like_ms >= _db_load_slow_ms():
                logger.log(
                    logging.WARNING if like_ms >= _db_load_slow_ms() else logging.DEBUG,
                    "[DBLoad][BoardDedup] like hit | db=%s table=%s col=%s terms=%s exact_ms=%.1f like_ms=%.1f total_ms=%.1f candidate_rows=%s url=%s",
                    db_name,
                    table_name,
                    key_col,
                    probe_terms[:8],
                    exact_ms,
                    like_ms,
                    (time.perf_counter() - lookup_t0) * 1000.0,
                    len(rows or []),
                    candidate[:180],
                )
            return row
    total_ms = (time.perf_counter() - lookup_t0) * 1000.0
    crawl_trace(
        logger,
        phase="selection",
        action="learn_list_duplicate_lookup",
        state="slow" if total_ms >= _db_load_slow_ms() else "end",
        level=logging.WARNING if total_ms >= _db_load_slow_ms() else logging.DEBUG,
        elapsed_ms=total_ms,
        db=db_name,
        table=table_name,
        source="like",
        hit=False,
        exact_ms=round(exact_ms, 1),
        like_ms=round(like_ms, 1),
        candidate_rows=len(rows or []),
        url=candidate,
    )
    if _db_load_debug_enabled() or like_ms >= _db_load_slow_ms():
        logger.log(
            logging.WARNING if like_ms >= _db_load_slow_ms() else logging.DEBUG,
            "[DBLoad][BoardDedup] miss | db=%s table=%s col=%s terms=%s exact_ms=%.1f like_ms=%.1f total_ms=%.1f candidate_rows=%s url=%s",
            db_name,
            table_name,
            key_col,
            probe_terms[:8],
            exact_ms,
            like_ms,
            total_ms,
            len(rows or []),
            candidate[:180],
        )
    return None

def _strip_hash_keys_from_learn_list_input(info: Dict[str, Any]) -> None:
    """??占쎈뻻 ?占승??????占쎄탢 (in-place)."""
    if not isinstance(info, dict): return
    for k in _LEARN_LIST_HASH_KEYS_BLOCKED:
        info.pop(k, None)

def _ensure_learn_list_hash_columns_null(data: Dict[str, Any], cols: Set[str]) -> None:
    """野껊슣??????占쏙옙 野껋럥占?占?占쏙옙????占쎈뻻?????館占?占쏙옙? ??占쎈뮉?? INSERT ????占쎈뻻 ?占싼됱쓥????占쏙옙占?NULL(??占쎄텕占?nullable)占??遺얜뼄."""
    if not data or not cols:
        return
    for hc in _LEARN_LIST_HASH_KEYS_BLOCKED & cols:
        data.pop(hc, None)

def _safe_row_get(row: Any, key: str) -> Any:
    """DB 野껉퀗??row?占?占쏙옙 ??占쎌읈??占쎌쓺 ?占싼됱쓥占??占쎈뗄??"""
    if row is None: return None
    try:
        if isinstance(row, dict): return row.get(key)
        val = getattr(row, key, None)
        if val is not None: return val
        return row[key]
    except Exception: return None


def _normalize_cate_code(value: Any) -> Optional[str]:
    """cate 揶쏉옙?? ?占쎈챷????占쎈뗀諭띰쭕???占쎌뒠??占쏙옙? ?袁⑤빍占???占쏙옙占?筌ｌ꼶??"""
    try:
        if value is None:
            return None
        if isinstance(value, str):
            val = value.strip()
            if val.lower() in {"undefined", "null", "none", "nan", "array"}:
                return None
            return val if val else None

        # Legacy comment removed because the original text was encoding-corrupted.
        if isinstance(value, dict):
            # Legacy comment removed because the original text was encoding-corrupted.
            inc_list = value.get("include")
            if isinstance(inc_list, list) and inc_list:
                res = str(inc_list[0]).strip()
                return res if res else None
            
            # Legacy comment removed because the original text was encoding-corrupted.
            for key in ("value", "code", "cate_code"):
                direct = value.get(key)
                if isinstance(direct, str) and direct.strip():
                    return direct.strip()
            
            # Legacy comment removed because the original text was encoding-corrupted.
            return None
            
        return None
        
    except Exception:
        return None

# Legacy comment removed because the original text was encoding-corrupted.
_table_columns_cache: Dict[Tuple[str, str], Set[str]] = {}
_chatbot_identifier_cache: Dict[Tuple[str, str], Optional[str]] = {}
_standard_cols_checked: Set[Tuple[str, str]] = set()
_standard_cols_locks: Dict[Tuple[str, str], asyncio.Lock] = {}
# Legacy comment removed because the original text was encoding-corrupted.
_learn_list_content_unique_ready: Dict[Tuple[str, str], bool] = {}
_CONTENT_UNIQUE_INDEX_NAME = "uq_learn_list_content"
# Legacy comment removed because the original text was encoding-corrupted.
_learn_list_duplicate_lookup_index_ready: Dict[Tuple[str, str, str], bool] = {}
_DUP_LOOKUP_INDEX_PREFIX_LEN = 512
_category_rows_cache: Dict[Tuple[str, str], Tuple[float, list[Dict[str, Any]]]] = {}


def _category_rows_cache_ttl_sec() -> float:
    try:
        value = float(os.getenv("CATEGORY_ROWS_CACHE_TTL_SEC", "300") or "300")
    except Exception:
        value = 300.0
    return max(0.0, min(value, 3600.0))


def _invalidate_category_rows_cache(db_name: str, category_table: str) -> None:
    if db_name and category_table:
        key = (str(db_name), str(category_table))
        _category_rows_cache.pop(key, None)
        job_cache = _current_crawl_db_cache()
        if job_cache is not None:
            try:
                job_cache.setdefault("category_rows", {}).pop(key, None)
            except Exception:
                pass



def _learn_list_content_user_lock_name(canon_url: str) -> str:
    """MySQL GET_LOCK ??占쏙옙???占쎈립(64?? ??占쎈릭. ??占쎌뵬 ???占쏙옙 URL ??占쎈뻻 ??占쎌뿯 筌욊낮?????"""
    h = hashlib.md5((canon_url or "").encode("utf-8")).hexdigest()
    return f"llcu{h}"


def _get_table_lock(db_name: str, table_name: str) -> asyncio.Lock:
    key = (db_name, table_name)
    lock = _standard_cols_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _standard_cols_locks[key] = lock
    return lock


async def _fetch_category_rows_cached(*, category_table: str, db_name: str) -> list[Dict[str, Any]]:
    if not category_table or not db_name:
        return []
    key = (str(db_name), str(category_table))
    job_cache = _current_crawl_db_cache()
    if job_cache is not None:
        category_cache = job_cache.setdefault("category_rows", {})
        if key in category_cache:
            return [dict(row) for row in category_cache.get(key) or []]
    ttl = _category_rows_cache_ttl_sec()
    now = time.time()
    cached = _category_rows_cache.get(key)
    if cached and ttl > 0 and now - cached[0] <= ttl:
        data = [dict(row) for row in cached[1]]
        if job_cache is not None:
            job_cache.setdefault("category_rows", {})[key] = data
        return data
    async with _get_table_lock(str(db_name), str(category_table)):
        if job_cache is not None:
            category_cache = job_cache.setdefault("category_rows", {})
            if key in category_cache:
                return [dict(row) for row in category_cache.get(key) or []]
        cached = _category_rows_cache.get(key)
        now = time.time()
        if cached and ttl > 0 and now - cached[0] <= ttl:
            data = [dict(row) for row in cached[1]]
            if job_cache is not None:
                job_cache.setdefault("category_rows", {})[key] = data
            return data
        rows = await mysql_execute_query(
            f"""
            SELECT cate_code, cate_treecode, cate_name, cate_use
            FROM `{category_table}`
            WHERE cate_code IS NOT NULL
              AND cate_treecode IS NOT NULL
              AND cate_name IS NOT NULL
            """,
            (),
            fetch=True,
            dbname=db_name,
            op_name="category_rows_cache_load",
        )
        data = [dict(row) for row in (rows or []) if isinstance(row, dict)]
        if ttl > 0:
            _category_rows_cache[key] = (now, data)
        if job_cache is not None:
            job_cache.setdefault("category_rows", {})[key] = data
        return [dict(row) for row in data]


def _category_row_is_used(row: Dict[str, Any]) -> bool:
    return str((row or {}).get("cate_use") or "y").strip().lower() == "y"

def _invalidate_table_columns_cache(db_name: str, table_name: str) -> None:
    if not db_name or not table_name:
        return
    _table_columns_cache.pop((db_name, table_name), None)

def _drop_unknown_column_and_retryable(exc: Exception) -> Optional[str]:
    """INSERT ??Unknown column ?占?占쏙옙 ???占쏙옙."""
    msg = str(exc)
    m = re.search(r"Unknown column '([^']+)'", msg, flags=re.IGNORECASE) or \
        re.search(r"Unknown column ([A-Za-z0-9_]+)", msg, flags=re.IGNORECASE)
    if m: return m.group(1)
    if "1054" in msg and "unknown column" in msg.lower(): return "__UNKNOWN__"
    return None

async def _safe_maria_insert_data(table_name: str, data: Dict[str, Any], db_name: str, warning_context: Optional[Dict[str, Any]] = None) -> Any:
    """Unknown column ?占?占쏙옙 ??占쎄탢 占???占쎌뿯 ??占쎈즲 (筌ㅼ뮆? 6??."""
    working = await _prepare_learn_list_insert_payload(table_name, data, db_name)
    for attempt in range(1, 7):
        try:
            db_save_trace_log(
                "maria.insert.start db=%s table=%s attempt=%s keys=%s content=%s subject=%s status=%s",
                db_name,
                table_name,
                attempt,
                ",".join(sorted(str(k) for k in working.keys())),
                _debug_insert_value(working.get("content")),
                _debug_insert_value(working.get("subject")),
                _debug_insert_value(working.get("status")),
            )
            learn_list_insert_debug_log(
                "safe_insert attempt=%s db=%s table=%s keys=%s content=%s subject=%s content_type=%s status=%s",
                attempt,
                db_name,
                table_name,
                ",".join(sorted(str(k) for k in working.keys())),
                _debug_insert_value(working.get("content")),
                _debug_insert_value(working.get("subject")),
                _debug_insert_value(working.get("content_type")),
                _debug_insert_value(working.get("status")),
            )
            inserted_id = await maria_insert_data(table_name, working, dbname=db_name, warning_context=warning_context)
            db_save_trace_log(
                "maria.insert.done db=%s table=%s attempt=%s inserted_id=%s content=%s",
                db_name,
                table_name,
                attempt,
                inserted_id,
                _debug_insert_value(working.get("content")),
            )
            return inserted_id
        except Exception as exc:
            col = _drop_unknown_column_and_retryable(exc)
            db_save_trace_log(
                "maria.insert.error db=%s table=%s attempt=%s retry_col=%s err=%s content=%s",
                db_name,
                table_name,
                attempt,
                col,
                _debug_insert_value(exc, 800),
                _debug_insert_value(working.get("content")),
                level=logging.WARNING,
                exc_info=True,
            )
            learn_list_insert_debug_log(
                "safe_insert exception attempt=%s db=%s table=%s retry_col=%s err=%s keys=%s content=%s",
                attempt,
                db_name,
                table_name,
                col,
                _debug_insert_value(exc, 500),
                ",".join(sorted(str(k) for k in working.keys())),
                _debug_insert_value(working.get("content")),
                level=logging.WARNING,
            )
            if not col or col == "__UNKNOWN__": raise
            if col in working:
                logger.warning(f"[LearnList] Dropping unknown column: {col}")
                working.pop(col, None)
                continue
            raise
    db_save_trace_log(
        "maria.insert.final_retry db=%s table=%s keys=%s content=%s",
        db_name,
        table_name,
        ",".join(sorted(str(k) for k in working.keys())),
        _debug_insert_value(working.get("content")),
    )
    return await maria_insert_data(table_name, working, dbname=db_name, warning_context=warning_context)


def _is_missing_learn_list_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _log_file_learn_list_insert_value_gaps(
    *,
    stage: str,
    db_name: str,
    table_name: str,
    data: Dict[str, Any],
    cols: Set[str],
    file_info: Dict[str, Any],
    source_url: Any,
) -> None:
    """Log missing important values before a new file LEARN_LIST insert/upsert."""
    try:
        colset = set(cols or [])
        required = ["content", "subject", "content_type", "status"]
        optional_watch = [
            "size",
            "cate1",
            "cate2",
            "content_author",
            "content_created_at",
        ]
        missing_required = [
            key
            for key in required
            if (not colset or key in colset) and _is_missing_learn_list_value(data.get(key))
        ]
        missing_optional = [
            key
            for key in optional_watch
            if (not colset or key in colset) and _is_missing_learn_list_value(data.get(key))
        ]
        if data.get("size") in (0, "0") and "size_zero" not in missing_optional:
            missing_optional.append("size_zero")
        snapshot = {
            "content": _debug_insert_value(data.get("content")),
            "subject": _debug_insert_value(data.get("subject")),
            "content_type": data.get("content_type"),
            "status": data.get("status"),
            "size": data.get("size"),
            "cate1": data.get("cate1"),
            "cate2": data.get("cate2"),
            "content_author": _debug_insert_value(data.get("content_author")),
            "content_created_at": data.get("content_created_at"),
        }
        log = logger.warning if missing_required else logger.debug
        log(
            "[LearningDebug][learn_list.insert_value_check] stage=%s job_id=%s db=%s table=%s "
            "missing_required=%s missing_optional=%s source_url=%s payload=%s file_info_keys=%s",
            stage,
            (file_info or {}).get("job_id"),
            db_name,
            table_name,
            missing_required,
            missing_optional,
            _debug_insert_value(source_url),
            snapshot,
            ",".join(sorted(str(k) for k in (file_info or {}).keys())),
        )
    except Exception as exc:
        logger.debug("[LearningDebug][learn_list.insert_value_check] failed | err=%s", exc)


def _log_file_learn_list_cate_debug(
    *,
    stage: str,
    db_name: str,
    table_name: str = "",
    file_info: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    cols: Optional[Set[str]] = None,
    row_id: Any = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """Trace why file LEARN_LIST cate1/cate2 is or is not persisted."""
    try:
        info = file_info if isinstance(file_info, dict) else {}
        original_meta = info.get("original_meta") if isinstance(info.get("original_meta"), dict) else {}
        payload = data if isinstance(data, dict) else {}
        colset = set(cols or set())
        coalesced_c1, coalesced_c2 = coalesce_learn_list_cates(info)
        if learn_list_file_dup_debug_enabled():
            logger.debug(
                "[LearningDebug][file_cate] stage=%s db=%s table=%s row_id=%s "
                "cate1=%s cate2=%s coalesced_cate1=%s coalesced_cate2=%s "
                "columns=%s original_meta_keys=%s extra=%s",
                stage,
                db_name,
                table_name,
                row_id,
                payload.get("cate1"),
                payload.get("cate2"),
                coalesced_c1,
                coalesced_c2,
                sorted(colset),
                sorted(str(key) for key in original_meta.keys()),
                extra or {},
            )
    except Exception as exc:
        logger.debug("[LearningDebug][file_cate] log failed | stage=%s err=%s", stage, exc)


async def ensure_learn_list_type_not_blank(
    *,
    db_name: str,
    learn_list_table: str,
    default_type: str = "post",
    row_id: Optional[Any] = None,
    content: Optional[str] = None,
) -> bool:
    """Legacy no-op: current LEARN_LIST schema does not use a type column."""
    return False

# =================================================================
# Legacy comment removed because the original text was encoding-corrupted.
# =================================================================
async def _get_table_columns(db_name: str, table_name: str) -> Set[str]:
    cache_key = (str(db_name), str(table_name))
    job_cache = _current_crawl_db_cache()
    if job_cache is not None:
        table_columns = job_cache.setdefault("table_columns", {})
        if cache_key in table_columns:
            return set(table_columns.get(cache_key) or set())
    if cache_key in _table_columns_cache:
        cols = set(_table_columns_cache[cache_key])
        if job_cache is not None:
            job_cache.setdefault("table_columns", {})[cache_key] = set(cols)
        return cols
    async with _get_table_lock(str(db_name), str(table_name)):
        if job_cache is not None:
            table_columns = job_cache.setdefault("table_columns", {})
            if cache_key in table_columns:
                return set(table_columns.get(cache_key) or set())
        if cache_key in _table_columns_cache:
            cols = set(_table_columns_cache[cache_key])
            if job_cache is not None:
                job_cache.setdefault("table_columns", {})[cache_key] = set(cols)
            return cols
        cols: Set[str] = set()
        try:
            sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND LOWER(table_name) = LOWER(%s)"
            rows = await mysql_execute_query(sql, (db_name, table_name), fetch=True, dbname=db_name)
            cols = {str(r.get("column_name")) for r in rows if r.get("column_name")}
        except Exception:
            cols = set()
        _table_columns_cache[cache_key] = cols
        if job_cache is not None:
            job_cache.setdefault("table_columns", {})[cache_key] = set(cols)
        return cols

async def get_account_identifier_from_chatbot_setup(chat_bot_id: str, db_name: str) -> Optional[str]:
    if not chat_bot_id or not db_name: return None
    cache_key = (str(db_name).strip(), str(chat_bot_id).strip())
    if cache_key in _chatbot_identifier_cache: return _chatbot_identifier_cache[cache_key]
    identifier = _extract_identifier_from_chat_bot_id(chat_bot_id)
    _chatbot_identifier_cache[cache_key] = identifier
    return identifier

# Legacy comment removed because the original text was encoding-corrupted.
_latest_chat_bot_id_cache: Dict[str, Optional[str]] = {}

async def get_latest_chat_bot_id_from_chatbot_setup(db_name: str) -> Optional[str]:
    """
    chatbot_setup ???占쏙옙?占쎈뗄占??揶쎛??筌ㅼ뮇??chat_id DESC) ??占쏀맜??占쎌벥 chat_bot_id??獄쏆꼹???占쎈뼄.
    - ?袁⑥쨴???遺욧퍕?占?占쏙옙 chat_bot_id揶쎛 ?袁⑥뵭??野껋럩??fallback??占쎌쨮 ?????占쎈뼄.
    """
    if not db_name:
        return None
    key = str(db_name).strip()
    if not key:
        return None
    if key in _latest_chat_bot_id_cache:
        return _latest_chat_bot_id_cache[key]
    try:
        rows = await mysql_execute_query(
            "SELECT chat_bot_id FROM chatbot_setup ORDER BY chat_id DESC LIMIT 1",
            tuple(),
            fetch=True,
            dbname=key,
        )
        row = rows[0] if rows else None
        chat_bot_id = (row or {}).get("chat_bot_id") if isinstance(row, dict) else None
        chat_bot_id = str(chat_bot_id).strip() if chat_bot_id else None
        _latest_chat_bot_id_cache[key] = chat_bot_id
        return chat_bot_id
    except Exception:
        _latest_chat_bot_id_cache[key] = None
        return None

def _extract_identifier_from_chat_bot_id(chat_bot_id: str) -> Optional[str]:
    if not chat_bot_id: return None
    parts = chat_bot_id.split('-')
    return parts[-1].strip().upper() if parts else None

def get_learn_list_table_name(account_identifier: Optional[str]) -> str:
    if not account_identifier: return "ASADAL_CRAWLING_LEARN_LIST"
    return f"ASADAL_{str(account_identifier).strip().lower()}_LEARN_LIST"


async def _resolve_existing_table_name(db_name: str, *candidates: str) -> Optional[str]:
    ordered = _unique_preserve_order([str(name or "").strip() for name in candidates if str(name or "").strip()])
    if not db_name or not ordered:
        return None
    cache_key = (str(db_name).strip(), tuple(name.lower() for name in ordered))
    job_cache = _current_crawl_db_cache()
    if job_cache is not None:
        existing_cache = job_cache.setdefault("existing_table_name", {})
        if cache_key in existing_cache:
            return existing_cache.get(cache_key)
    if cache_key in _existing_table_name_cache:
        resolved_cached = _existing_table_name_cache[cache_key]
        if job_cache is not None:
            job_cache.setdefault("existing_table_name", {})[cache_key] = resolved_cached
        return resolved_cached
    for table_name in ordered:
        try:
            rows = await mysql_execute_query(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND LOWER(table_name) = LOWER(%s)
                LIMIT 1
                """,
                (db_name, table_name),
                fetch=True,
                dbname=db_name,
            )
        except Exception:
            rows = None
        if rows:
            actual = _safe_row_get(rows[0], "table_name")
            if actual:
                resolved = str(actual)
                _existing_table_name_cache[cache_key] = resolved
                if job_cache is not None:
                    job_cache.setdefault("existing_table_name", {})[cache_key] = resolved
                return resolved
    _existing_table_name_cache[cache_key] = None
    if job_cache is not None:
        job_cache.setdefault("existing_table_name", {})[cache_key] = None
    return None


async def resolve_learn_list_table_name_for_chatbot(chat_bot_id: str, db_name: str) -> Optional[str]:
    account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
    primary_name = get_learn_list_table_name(account_identifier)
    legacy_suffix = _extract_identifier_from_chat_bot_id(chat_bot_id)
    legacy_name = f"ASADAL_{legacy_suffix}_LEARN_LIST" if legacy_suffix else ""
    fallback_name = "ASADAL_CRAWLING_LEARN_LIST"
    resolved = await _resolve_existing_table_name(
        db_name,
        primary_name,
        legacy_name,
        fallback_name,
    )
    if resolved:
        return resolved
    return primary_name or legacy_name or fallback_name


async def _learn_list_table_exists(db_name: str, table_name: str) -> bool:
    if not db_name or not table_name:
        return False
    cache_key = (str(db_name).strip(), str(table_name).strip().lower())
    job_cache = _current_crawl_db_cache()
    if job_cache is not None:
        table_exists = job_cache.setdefault("table_exists", {})
        if cache_key in table_exists:
            return bool(table_exists.get(cache_key))
    if cache_key in _learn_list_table_exists_cache:
        exists_cached = bool(_learn_list_table_exists_cache[cache_key])
        if job_cache is not None:
            job_cache.setdefault("table_exists", {})[cache_key] = exists_cached
        return exists_cached
    try:
        resolved = await _resolve_existing_table_name(db_name, table_name)
        exists = bool(resolved)
        _learn_list_table_exists_cache[cache_key] = exists
        if job_cache is not None:
            job_cache.setdefault("table_exists", {})[cache_key] = exists
        return exists
    except Exception:
        return False


def _extract_category_table_suffix(chat_bot_id: str) -> str:
    raw = ""
    try:
        if chat_bot_id:
            parts = str(chat_bot_id).strip().split("-")
            raw = (parts[-1] if parts else "").strip()
    except Exception:
        raw = ""
    if not raw or not re.match(r"^[A-Za-z0-9]+$", raw):
        raise ValueError("invalid_chat_bot_id")
    return raw


def get_category_table_name(chat_bot_id: str) -> str:
    return f"ASADAL_{_extract_category_table_suffix(chat_bot_id)}_CATEGORY"


def get_category_config_table_name(chat_bot_id: str) -> str:
    return f"ASADAL_{_extract_category_table_suffix(chat_bot_id)}_CATEGORY_CONFIG"


_DEFAULT_CATEGORY_ROOT_CODES: Dict[str, str] = {
    "homepage_learning": "AS1729062288",
    "file_learning": "AS1729062287",
}

_CATEGORY_ROOT_FALLBACK_NAMES: Dict[str, Tuple[str, ...]] = {
    "homepage_learning": ("\ud648\ud398\uc774\uc9c0\ud559\uc2b5", "\ud648\ud398\uc774\uc9c0 \ud559\uc2b5", "homepage_learning", "homepage learning"),
    "file_learning": ("\ud30c\uc77c\ud559\uc2b5", "\ud30c\uc77c \ud559\uc2b5", "file_learning", "file learning"),
}

def _category_root_code_from_env(key: str) -> str:
    env_map = {
        "homepage_learning": "CATEGORY_SYNC_HOMEPAGE_ROOT_CODE",
        "file_learning": "CATEGORY_SYNC_FILE_ROOT_CODE",
    }
    env_name = env_map.get(str(key or "").strip(), "")
    if env_name:
        try:
            value = str(os.getenv(env_name, "") or "").strip()
            if value:
                return value
        except Exception:
            pass
    return _DEFAULT_CATEGORY_ROOT_CODES.get(str(key or "").strip(), "")


def _unique_preserve_order(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _file_category_root_candidate_names() -> tuple[str, ...]:
    return ("파일",)


def _normalize_category_match_name(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip()).lower()


async def _fetch_category_root_by_names(
    *,
    category_table: str,
    candidate_names: Iterable[str],
    db_name: str,
) -> Optional[Dict[str, Any]]:
    quoted = {str(n or "").strip() for n in candidate_names if str(n or "").strip()}
    if not quoted:
        return None
    rows = [
        row for row in await _fetch_category_rows_cached(category_table=category_table, db_name=db_name)
        if _category_row_is_used(row) and str(row.get("cate_name") or "").strip() in quoted
    ]
    rows.sort(key=lambda row: (len(str(row.get("cate_treecode") or "")), str(row.get("cate_treecode") or "")))
    return dict(rows[0]) if rows else None



async def _fetch_category_root_by_code(
    *,
    category_table: str,
    cate_code: str,
    candidate_names: Iterable[str],
    db_name: str,
) -> Optional[Dict[str, Any]]:
    code = str(cate_code or "").strip()
    if not code:
        return None
    row = next((row for row in await _fetch_category_rows_cached(category_table=category_table, db_name=db_name) if str(row.get("cate_code") or "").strip() == code), None)
    if not row:
        return None
    valid_names = {str(name or "").strip() for name in candidate_names if str(name or "").strip()}
    if valid_names:
        row_name = str((row or {}).get("cate_name") or "").strip()
        if row_name not in valid_names:
            logger.warning(
                "[CategorySync] preferred root code mismatch; falling back to root name lookup | table=%s code=%s actual_name=%s expected_names=%s",
                category_table,
                code,
                row_name,
                sorted(valid_names),
            )
            return None
    return dict(row)



async def _resolve_category_root(
    *,
    category_table: str,
    root_key: str,
    candidate_names: Iterable[str],
    db_name: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    all_candidate_names = _unique_preserve_order(
        [
            *list(candidate_names or []),
            *_CATEGORY_ROOT_FALLBACK_NAMES.get(str(root_key or "").strip(), ()),
        ]
    )
    preferred_code = _category_root_code_from_env(root_key)
    if preferred_code:
        row = await _fetch_category_root_by_code(
            category_table=category_table,
            cate_code=preferred_code,
            candidate_names=all_candidate_names,
            db_name=db_name,
        )
        if row:
            return row, "preferred_code"
    row = await _fetch_category_root_by_names(
        category_table=category_table,
        candidate_names=all_candidate_names,
        db_name=db_name,
    )
    return row, "name_fallback"


async def _fetch_category_children(
    *,
    category_table: str,
    parent_treecode: str,
    db_name: str,
) -> list[Dict[str, Any]]:
    parent = str(parent_treecode or "").strip()
    if not parent:
        return []
    child_lengths = set(_category_direct_child_lengths(parent))
    rows = [
        row for row in await _fetch_category_rows_cached(category_table=category_table, db_name=db_name)
        if _category_row_is_used(row)
        and str(row.get("cate_treecode") or "").strip().startswith(parent)
        and len(str(row.get("cate_treecode") or "").strip()) in child_lengths
    ]
    rows.sort(key=lambda row: str(row.get("cate_treecode") or ""))
    return [dict(row) for row in rows]



async def _fetch_category_direct_child_by_name(
    *,
    category_table: str,
    parent_treecode: str,
    cate_name: str,
    db_name: str,
) -> Optional[Dict[str, Any]]:
    rows = await _fetch_category_direct_children_by_name(
        category_table=category_table,
        parent_treecode=parent_treecode,
        cate_name=cate_name,
        db_name=db_name,
    )
    return dict(rows[0]) if rows else None



async def _fetch_category_direct_children_by_name(
    *,
    category_table: str,
    parent_treecode: str,
    cate_name: str,
    db_name: str,
    newest_first: bool = False,
) -> list[Dict[str, Any]]:
    parent = str(parent_treecode or "").strip()
    name = str(cate_name or "").strip()
    if not parent or not name:
        return []
    child_lengths = set(_category_direct_child_lengths(parent))
    candidates = [
        row for row in await _fetch_category_rows_cached(category_table=category_table, db_name=db_name)
        if _category_row_is_used(row)
        and str(row.get("cate_treecode") or "").strip().startswith(parent)
        and len(str(row.get("cate_treecode") or "").strip()) in child_lengths
    ]
    candidates.sort(key=lambda row: str(row.get("cate_treecode") or ""), reverse=bool(newest_first))
    rows = [dict(row) for row in candidates if str(row.get("cate_name") or "").strip() == name]
    if rows:
        return rows
    normalized_name = _normalize_category_match_name(name)
    if not normalized_name:
        return []
    return [
        dict(row) for row in candidates
        if _normalize_category_match_name((row or {}).get("cate_name")) == normalized_name
    ]



def _pick_category_child_tree_step(parent_treecode: str, children: Iterable[Dict[str, Any]]) -> int:
    parent = str(parent_treecode or "").strip()
    counts: Dict[int, int] = {}
    for row in children or []:
        tree = str((row or {}).get("cate_treecode") or "").strip()
        if not tree.startswith(parent) or tree == parent:
            continue
        step = len(tree) - len(parent)
        if step in _CATEGORY_CHILD_TREECODE_STEPS:
            counts[step] = counts.get(step, 0) + 1
    if counts:
        return sorted(counts.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][0]
    return 4 if parent.lower().startswith("c") else 3


async def _next_category_code(*, category_table: str, db_name: str) -> str:
    rows = await _fetch_category_rows_cached(category_table=category_table, db_name=db_name)
    base = 0
    existing_codes = set()
    for row in rows or []:
        raw = str((row or {}).get("cate_code") or "").strip()
        if raw:
            existing_codes.add(raw.upper())
        match = re.match(r"^AS(\d+)$", raw, flags=re.IGNORECASE)
        if match:
            try:
                base = max(base, int(match.group(1) or 0))
            except Exception:
                pass
    for offset in range(1, 500):
        code = f"AS{base + offset}"
        if code.upper() not in existing_codes:
            return code
    return f"AS{int(time.time() * 1000)}"



async def _fetch_category_by_treecode(
    *,
    category_table: str,
    cate_treecode: str,
    db_name: str,
) -> Optional[Dict[str, Any]]:
    tree = str(cate_treecode or "").strip()
    if not tree:
        return None
    row = next((row for row in await _fetch_category_rows_cached(category_table=category_table, db_name=db_name) if str(row.get("cate_treecode") or "").strip() == tree), None)
    return dict(row) if row else None



async def _next_direct_child_treecode(
    *,
    category_table: str,
    parent_treecode: str,
    db_name: str,
) -> str:
    parent = str(parent_treecode or "").strip()
    if not parent:
        raise ValueError("parent_treecode_required")
    children = await _fetch_category_children(
        category_table=category_table,
        parent_treecode=parent,
        db_name=db_name,
    )
    step = _pick_category_child_tree_step(parent, children)
    max_suffix = 0
    for row in children or []:
        tree = str((row or {}).get("cate_treecode") or "").strip()
        if not tree.startswith(parent) or len(tree) != len(parent) + step:
            continue
        suffix = tree[len(parent):]
        try:
            max_suffix = max(max_suffix, int(suffix))
        except Exception:
            continue
    for offset in range(1, 500):
        suffix_value = max_suffix + offset
        candidate = f"{parent}{suffix_value:0{step}d}"
        existing = await _fetch_category_by_treecode(
            category_table=category_table,
            cate_treecode=candidate,
            db_name=db_name,
        )
        if not existing:
            return candidate
    raise RuntimeError("category_treecode_exhausted")


async def _create_file_learning_category_child(
    *,
    category_table: str,
    parent_row: Dict[str, Any],
    cate_name: str,
    db_name: str,
) -> Optional[Dict[str, Any]]:
    name = str(cate_name or "").strip()
    parent_tree = str((parent_row or {}).get("cate_treecode") or "").strip()
    if not (name and parent_tree):
        return None

    async with _get_table_lock(str(db_name), str(category_table)):
        existing_children = await _fetch_category_direct_children_by_name(
            category_table=category_table,
            parent_treecode=parent_tree,
            cate_name=name,
            db_name=db_name,
            newest_first=True,
        )
        if existing_children:
            return dict(existing_children[0])

        for _attempt in range(1, 6):
            cate_code = await _next_category_code(category_table=category_table, db_name=db_name)
            cate_treecode = await _next_direct_child_treecode(
                category_table=category_table,
                parent_treecode=parent_tree,
                db_name=db_name,
            )
            try:
                await mysql_execute_query(
                    f"""
                    INSERT INTO `{category_table}`
                        (`code_no`, `cate_code`, `cate_treecode`, `cate_name`, `cate_ename`,
                         `cate_service`, `cate_keyword`, `cate_use`, `cate_use_part`, `conf_cate_no`, `open`)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (0, cate_code, cate_treecode, name, "", "n", "", "y", "P", 0, 0),
                    fetch=False,
                    dbname=db_name,
                    op_name="file_category_child_create",
                )
                _invalidate_category_rows_cache(db_name, category_table)
                row = await _fetch_category_by_code(
                    category_table=category_table,
                    cate_code=cate_code,
                    db_name=db_name,
                )
                return dict(row or {"cate_code": cate_code, "cate_treecode": cate_treecode, "cate_name": name})
            except Exception as exc:
                if "duplicate" in str(exc).lower() or "1062" in str(exc):
                    continue
                raise
    return None

async def _fetch_category_by_code(
    *,
    category_table: str,
    cate_code: str,
    db_name: str,
) -> Optional[Dict[str, Any]]:
    code = str(cate_code or "").strip()
    if not code:
        return None
    row = next((row for row in await _fetch_category_rows_cached(category_table=category_table, db_name=db_name) if str(row.get("cate_code") or "").strip() == code), None)
    return dict(row) if row else None



def _looks_like_category_code(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if re.match(r"^[A-Z]{1,6}\d{3,}$", text, flags=re.IGNORECASE):
        return True
    if re.match(r"^c\d{3,}$", text, flags=re.IGNORECASE):
        return True
    return False


async def _fetch_category_descendant_by_name(
    *,
    category_table: str,
    root_treecode: str,
    cate_name: str,
    db_name: str,
) -> Optional[Dict[str, Any]]:
    root = str(root_treecode or "").strip()
    name = str(cate_name or "").strip()
    if not root or not name:
        return None
    rows = [
        row for row in await _fetch_category_rows_cached(category_table=category_table, db_name=db_name)
        if _category_row_is_used(row)
        and str(row.get("cate_treecode") or "").strip().startswith(root)
        and str(row.get("cate_treecode") or "").strip() != root
        and str(row.get("cate_name") or "").strip() == name
    ]
    rows.sort(key=lambda row: (len(str(row.get("cate_treecode") or "")), str(row.get("cate_treecode") or "")))
    return dict(rows[0]) if rows else None



def _is_under_category_root(row: Optional[Dict[str, Any]], root_row: Optional[Dict[str, Any]]) -> bool:
    try:
        row_tree = str((row or {}).get("cate_treecode") or "").strip()
        root_tree = str((root_row or {}).get("cate_treecode") or "").strip()
    except Exception:
        row_tree = ""
        root_tree = ""
    if not row_tree or not root_tree:
        return False
    return row_tree == root_tree or row_tree.startswith(root_tree)


async def _fetch_category_parent_by_treecode(
    *,
    category_table: str,
    cate_treecode: str,
    db_name: str,
) -> Optional[Dict[str, Any]]:
    tree = str(cate_treecode or "").strip()
    if not tree:
        return None
    rows = [
        row for row in await _fetch_category_rows_cached(category_table=category_table, db_name=db_name)
        if _category_row_is_used(row)
        and str(row.get("cate_treecode") or "").strip()
        and tree.startswith(str(row.get("cate_treecode") or "").strip())
        and str(row.get("cate_treecode") or "").strip() != tree
    ]
    rows.sort(key=lambda row: (len(str(row.get("cate_treecode") or "")), str(row.get("cate_treecode") or "")), reverse=True)
    return dict(rows[0]) if rows else None



async def _fallback_original_board_category_mapping(
    *,
    category_table: str,
    db_name: str,
    source_cate1: str,
    source_cate2: str,
    source_c2_row: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    """Return board category names as a compatibility fail-safe for legacy DBs.

    Some existing deployments do not yet contain the fixed File-category child.
    In that known compatibility state, preserving the resolved board category
    *name* is safer than writing an opaque category code or silently blanking the
    category. A later category-repair job can deterministically map that name.
    This path does not bypass file learning, persistence, or error reporting.
    """
    fallback_c1 = str(source_cate1 or "").strip()
    fallback_c2 = str(source_cate2 or "").strip()

    async def _name_from_value(value: str, row: Optional[Dict[str, Any]] = None) -> str:
        text = str(value or "").strip()
        resolved = row
        if resolved is None and text and _looks_like_category_code(text):
            resolved = await _fetch_category_by_code(
                category_table=category_table,
                cate_code=text,
                db_name=db_name,
            )
        name = str((resolved or {}).get("cate_name") or "").strip()
        if name:
            return name
        return "" if _looks_like_category_code(text) else text

    source_c2_row = source_c2_row or None
    fallback_c2 = await _name_from_value(fallback_c2, source_c2_row)
    fallback_c1 = await _name_from_value(fallback_c1)

    if not fallback_c1 and source_c2_row:
        parent_row = await _fetch_category_parent_by_treecode(
            category_table=category_table,
            cate_treecode=str((source_c2_row or {}).get("cate_treecode") or "").strip(),
            db_name=db_name,
        )
        fallback_c1 = str((parent_row or {}).get("cate_name") or "").strip()

    return (fallback_c1, fallback_c2)

async def _ensure_file_learning_category_mapping(
    *,
    chat_bot_id: str,
    db_name: str,
    source_cate1: str,
    source_cate2: str,
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
    create_missing: bool = False,
) -> Tuple[str, str]:
    source_c1 = str(source_cate1 or "").strip()
    source_c2 = str(source_cate2 or "").strip()
    category_table = get_category_table_name(chat_bot_id)

    async def _source_category_name(value: Any, row: Optional[Dict[str, Any]] = None) -> str:
        text = str(value or "").strip()
        resolved = row
        if resolved is None and text and _looks_like_category_code(text):
            resolved = await _fetch_category_by_code(
                category_table=category_table,
                cate_code=text,
                db_name=db_name,
            )
        name = str((resolved or {}).get("cate_name") or "").strip()
        if name:
            return name
        return "" if _looks_like_category_code(text) else text
    log_file_category_trace(
        "매핑시작",
        db=db_name,
        table=category_table,
        **{"원본": f"({source_cate1},{source_cate2})"},
    )
    target_cate1_row = await _fetch_category_root_by_names(
        category_table=category_table,
        candidate_names=_file_category_root_candidate_names(),
        db_name=db_name,
    )
    if not target_cate1_row:
        fallback_c1, fallback_c2 = await _fallback_original_board_category_mapping(
            category_table=category_table,
            db_name=db_name,
            source_cate1=source_cate1,
            source_cate2=source_cate2,
            source_c2_row=None,
        )
        log_fn = logger.debug if not (str(source_cate1 or "").strip() or source_c2) else logger.warning
        log_fn(
            "[CategorySync][file-map-debug] fallback: fixed file cate1 not found; keep original board category name | db=%s chat_bot_id=%s table=%s source=(%s,%s) mapped=(%s,%s)",
            db_name,
            chat_bot_id,
            category_table,
            source_cate1,
            source_cate2,
            fallback_c1,
            fallback_c2,
        )
        return (fallback_c1, fallback_c2)
    target_cate1_code = str(target_cate1_row.get("cate_code") or "").strip()

    log_file_category_trace(
        "루트확인",
        db=db_name,
        table=category_table,
        file_cate1=target_cate1_code,
        file_tree=(target_cate1_row or {}).get("cate_treecode"),
    )

    if not source_c2:
        cate2_name = await _source_category_name(source_c1)
        if cate2_name and cate2_name != "파일" and source_c1 != target_cate1_code:
            target_c2_row = await ensure_file_learning_category_by_name(
                chat_bot_id=chat_bot_id,
                db_name=db_name,
                cate_name=cate2_name,
                parent_cate_code=target_cate1_code,
                access_url=access_url,
                request_cookies=request_cookies,
                create_missing=create_missing,
            )
            if target_c2_row:
                mapped = str(target_c2_row.get("cate_code") or "").strip()
                log_file_category_trace(
                    "하위매핑",
                    db=db_name,
                    table=category_table,
                    source=cate2_name,
                    mapped_cate1=target_cate1_code,
                    mapped_cate2=mapped,
                )
                return (target_cate1_code, mapped)
        log_file_category_trace(
            "하위매핑",
            db=db_name,
            table=category_table,
            source=cate2_name or source_cate1 or source_cate2,
            mapped_cate1=target_cate1_code,
            mapped_cate2="",
        )
        return (target_cate1_code, "")

    source_c2_row = (
        await _fetch_category_by_code(
            category_table=category_table,
            cate_code=source_c2,
            db_name=db_name,
        )
        if source_c2
        else None
    )

    if (
        source_c2_row
        and str(source_c2_row.get("cate_code") or "").strip() != target_cate1_code
        and _is_under_category_root(source_c2_row, target_cate1_row)
    ):
        return (target_cate1_code, source_c2)

    cate2_name = await _source_category_name(source_c2, source_c2_row)
    if not cate2_name:
        fallback_c1, fallback_c2 = await _fallback_original_board_category_mapping(
            category_table=category_table,
            db_name=db_name,
            source_cate1=source_cate1,
            source_cate2=source_cate2,
            source_c2_row=source_c2_row,
        )
        logger.warning(
            "[CategorySync][file-map-debug] fallback: source cate2 name not found; keep original board category name | db=%s chat_bot_id=%s table=%s source=(%s,%s) source_row=%s mapped=(%s,%s)",
            db_name,
            chat_bot_id,
            category_table,
            source_cate1,
            source_cate2,
            source_c2_row,
            fallback_c1,
            fallback_c2,
        )
        return (fallback_c1, fallback_c2)

    target_c2_row = await ensure_file_learning_category_by_name(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        cate_name=cate2_name,
        parent_cate_code=target_cate1_code,
        access_url=access_url,
        request_cookies=request_cookies,
        create_missing=create_missing,
    )
    if not target_c2_row:
        fallback_c1, fallback_c2 = await _fallback_original_board_category_mapping(
            category_table=category_table,
            db_name=db_name,
            source_cate1=source_cate1,
            source_cate2=source_cate2,
            source_c2_row=source_c2_row,
        )
        logger.warning(
            "[CategorySync][file-map-debug] fallback: file child category not found; keep original board category name | db=%s chat_bot_id=%s table=%s fixed_cate1=%s source=(%s,%s) source_name=%s create_missing=%s mapped=(%s,%s)",
            db_name,
            chat_bot_id,
            category_table,
            target_cate1_code,
            source_cate1,
            source_cate2,
            cate2_name,
            create_missing,
            fallback_c1,
            fallback_c2,
        )
        return (fallback_c1, fallback_c2)
    mapped = str(target_c2_row.get("cate_code") or "").strip()
    log_file_category_trace(
        "하위매핑",
        db=db_name,
        table=category_table,
        source=cate2_name,
        mapped_cate1=target_cate1_code,
        mapped_cate2=mapped,
    )
    return (target_cate1_code, mapped)


async def _resolve_category_names_for_file_meta(
    *,
    chat_bot_id: str,
    db_name: str,
    cate1: str,
    cate2: str,
) -> Tuple[str, str]:
    category_table = get_category_table_name(chat_bot_id)

    async def _name(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        row = await _fetch_category_by_code(
            category_table=category_table,
            cate_code=text,
            db_name=db_name,
        )
        row_name = str((row or {}).get("cate_name") or "").strip()
        if row_name:
            return row_name
        if not _looks_like_category_code(text):
            return text
        return ""

    return (await _name(cate1), await _name(cate2))


async def ensure_file_learning_category_by_name(
    *,
    chat_bot_id: str,
    db_name: str,
    cate_name: str,
    parent_cate_code: Optional[str] = None,
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
    create_missing: bool = False,
    force_create: bool = False,
) -> Optional[Dict[str, Any]]:
    name = str(cate_name or "").strip()
    if not str(chat_bot_id or "").strip():
        raise ValueError("chat_bot_id_required")
    if not str(db_name or "").strip():
        raise ValueError("db_name_required")
    if not name:
        raise ValueError("cate_name_required")

    category_table = get_category_table_name(chat_bot_id)
    parent_code = str(parent_cate_code or "").strip()
    if parent_code:
        resolved_parent = await _fetch_category_by_code(
            category_table=category_table,
            cate_code=parent_code,
            db_name=db_name,
        )
        if not resolved_parent:
            raise RuntimeError(f"file_category_parent_not_found:{parent_code}")
        parent_row: Dict[str, Any] = dict(resolved_parent)
    else:
        target_root = await _fetch_category_root_by_names(
            category_table=category_table,
            candidate_names=_file_category_root_candidate_names(),
            db_name=db_name,
        )
        if not target_root:
            raise RuntimeError("file_category_root_not_found")
        parent_row = dict(target_root)

    existing_children = await _fetch_category_direct_children_by_name(
        category_table=category_table,
        parent_treecode=str(parent_row.get("cate_treecode") or ""),
        cate_name=name,
        db_name=db_name,
    )
    existing = existing_children[0] if existing_children else None
    if existing and not force_create:
        return dict(existing)
    if not create_missing:
        return None
    return await _create_file_learning_category_child(
        category_table=category_table,
        parent_row=parent_row,
        cate_name=name,
        db_name=db_name,
    )

async def ensure_file_learning_category_code_by_name(
    *,
    chat_bot_id: str,
    db_name: str,
    cate_name: str,
    parent_cate_code: Optional[str] = None,
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
    create_missing: bool = False,
    force_create: bool = False,
) -> str:
    row = await ensure_file_learning_category_by_name(
        chat_bot_id=chat_bot_id,
        db_name=db_name,
        cate_name=cate_name,
        parent_cate_code=parent_cate_code,
        access_url=access_url,
        request_cookies=request_cookies,
        create_missing=create_missing,
        force_create=force_create,
    )
    return str((row or {}).get("cate_code") or "").strip()


async def _count_file_rows_for_cate2(
    *,
    learn_list_table: str,
    cate2_code: str,
    db_name: str,
) -> int:
    if not _file_category_count_query_enabled():
        return 0
    rows = await mysql_execute_query(
        f"SELECT COUNT(*) AS cnt FROM `{learn_list_table}` WHERE content_type = %s AND cate2 = %s",
        ("file", str(cate2_code or "").strip()),
        fetch=True,
        dbname=db_name,
    )
    return int(_safe_row_get(rows[0], "cnt") or 0) if rows else 0


async def _count_file_rows_for_cate1_only(
    *,
    learn_list_table: str,
    cate1_code: str,
    db_name: str,
) -> int:
    if not _file_category_count_query_enabled():
        return 0
    rows = await mysql_execute_query(
        f"""
        SELECT COUNT(*) AS cnt
        FROM `{learn_list_table}`
        WHERE content_type = %s
          AND cate1 = %s
          AND COALESCE(NULLIF(cate2, ''), '') = ''
        """,
        ("file", str(cate1_code or "").strip()),
        fetch=True,
        dbname=db_name,
    )
    return int(_safe_row_get(rows[0], "cnt") or 0) if rows else 0


async def preview_file_category_sync_plan(
    *,
    chat_bot_id: str,
    db_name: str,
) -> Dict[str, Any]:
    if not str(chat_bot_id or "").strip():
        raise ValueError("chat_bot_id_required")
    if not str(db_name or "").strip():
        raise ValueError("db_name_required")

    category_table = get_category_table_name(chat_bot_id)
    account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
    learn_list_table = get_learn_list_table_name(account_identifier)

    source_root, source_root_strategy = await _resolve_category_root(
        category_table=category_table,
        root_key="homepage_learning",
        candidate_names=("\ud648\ud398\uc774\uc9c0\ud559\uc2b5", "\ud648\ud398\uc774\uc9c0 \ud559\uc2b5"),
        db_name=db_name,
    )
    target_root, target_root_strategy = await _resolve_category_root(
        category_table=category_table,
        root_key="file_learning",
        candidate_names=("\ud30c\uc77c\ud559\uc2b5", "\ud30c\uc77c \ud559\uc2b5"),
        db_name=db_name,
    )
    if not source_root:
        raise RuntimeError("homepage_learning_root_not_found")
    if not target_root:
        raise RuntimeError("file_learning_root_not_found")

    plan_items: list[Dict[str, Any]] = []
    summary: Dict[str, Any] = {
        "source_cate1_total": 0,
        "target_cate1_missing": 0,
        "source_cate2_total": 0,
        "target_cate2_missing": 0,
        "matched_file_rows_by_cate1_only": 0,
        "matched_file_rows_by_cate2": 0,
    }

    source_cate1_rows = await _fetch_category_children(
        category_table=category_table,
        parent_treecode=str(source_root.get("cate_treecode") or ""),
        db_name=db_name,
    )
    summary["source_cate1_total"] = len(source_cate1_rows)

    for source_cate1 in source_cate1_rows:
        existing_target_cate1 = await _fetch_category_direct_child_by_name(
            category_table=category_table,
            parent_treecode=str(target_root.get("cate_treecode") or ""),
            cate_name=str(source_cate1.get("cate_name") or ""),
            db_name=db_name,
        )
        cate1_only_rows = await _count_file_rows_for_cate1_only(
            learn_list_table=learn_list_table,
            cate1_code=str(source_cate1.get("cate_code") or ""),
            db_name=db_name,
        )
        summary["matched_file_rows_by_cate1_only"] += cate1_only_rows
        if not existing_target_cate1:
            summary["target_cate1_missing"] += 1

        item: Dict[str, Any] = {
            "source_cate1": dict(source_cate1),
            "target_cate1": dict(existing_target_cate1) if existing_target_cate1 else None,
            "target_cate1_exists": bool(existing_target_cate1),
            "matched_file_rows_by_cate1_only": cate1_only_rows,
            "cate2_plans": [],
        }

        source_cate2_rows = await _fetch_category_children(
            category_table=category_table,
            parent_treecode=str(source_cate1.get("cate_treecode") or ""),
            db_name=db_name,
        )
        summary["source_cate2_total"] += len(source_cate2_rows)

        target_cate1_treecode = str((existing_target_cate1 or {}).get("cate_treecode") or "")
        for source_cate2 in source_cate2_rows:
            existing_target_cate2 = None
            if target_cate1_treecode:
                existing_target_cate2 = await _fetch_category_direct_child_by_name(
                    category_table=category_table,
                    parent_treecode=target_cate1_treecode,
                    cate_name=str(source_cate2.get("cate_name") or ""),
                    db_name=db_name,
                )
            matched_rows = await _count_file_rows_for_cate2(
                learn_list_table=learn_list_table,
                cate2_code=str(source_cate2.get("cate_code") or ""),
                db_name=db_name,
            )
            summary["matched_file_rows_by_cate2"] += matched_rows
            if not existing_target_cate2:
                summary["target_cate2_missing"] += 1
            item["cate2_plans"].append(
                {
                    "source_cate2": dict(source_cate2),
                    "target_cate2": dict(existing_target_cate2) if existing_target_cate2 else None,
                    "target_cate2_exists": bool(existing_target_cate2),
                    "matched_file_rows": matched_rows,
                }
            )

        plan_items.append(item)

    return {
        "ok": True,
        "chat_bot_id": chat_bot_id,
        "db_name": db_name,
        "category_table": category_table,
        "learn_list_table": learn_list_table,
        "source_root": dict(source_root),
        "target_root": dict(target_root),
        "source_root_strategy": source_root_strategy,
        "target_root_strategy": target_root_strategy,
        "summary": summary,
        "plan_items": plan_items,
    }

# =================================================================
# [筌롳옙?? ?怨쀬뵠???占쎈뗄??
# =================================================================
def _extract_meta(file_info: Dict[str, Any], key: str) -> Any:
    if not file_info: return None
    if key in file_info and file_info.get(key) is not None: return file_info.get(key)
    orig = file_info.get("original_meta", {})
    if not isinstance(orig, dict):
        return None
    value = orig.get(key)
    if value not in (None, ""):
        return value
    nested = orig.get("original_meta")
    if isinstance(nested, dict):
        return nested.get(key)
    return None


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


def _file_subject_basename(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        path = urlparse(raw).path if "://" in raw else raw
    except Exception:
        path = raw
    try:
        name = unquote(os.path.basename(str(path or "").replace("\\", "/"))).strip()
    except Exception:
        name = os.path.basename(str(path or "").replace("\\", "/")).strip()
    return name


def _normalize_file_subject_candidate(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        if "://" in text:
            text = urlparse(text).path or text
    except Exception:
        pass
    if "/" in text or "\\" in text:
        text = os.path.basename(text.replace("\\", "/"))
    try:
        text = unquote(text)
    except Exception:
        pass
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_file_subject_for_db(value: Any) -> str:
    from utils.file import preserve_file_learning_subject

    return preserve_file_learning_subject(value)


def _is_generic_file_subject(value: Any) -> bool:
    text = re.sub(r"\s+", " ", str(value or "")).strip().strip("._- ")
    if not text:
        return True
    stem = os.path.splitext(text)[0].strip().lower()
    compact = re.sub(r"[\s._\-()\[\]]+", "", stem)
    return compact in {
        "attachment",
        "attach",
        "file",
        "download",
        "untitled",
        "noname",
        "泥⑨옙?",
        "泥⑨옙??占쎌씪",
        "?占쎌슫濡쒕뱶",
        "?占쎌씪",
        "諛붾줈蹂닿린",
        "諛붾줈?占쎄린",
    }


def _resolve_file_learning_subject(file_info: Dict[str, Any]) -> str:
    if not isinstance(file_info, dict):
        return ""
    from utils.file import preserve_file_learning_subject

    file_path = file_info.get("file_path") or file_info.get("local_path") or ""
    extension_sources = [
        file_path,
        file_info.get("url"),
        file_info.get("content"),
        file_info.get("source_url"),
        file_info.get("href"),
        _extract_meta(file_info, "url"),
        _extract_meta(file_info, "content"),
        _extract_meta(file_info, "source_url"),
        _extract_meta(file_info, "href"),
    ]
    # source_filename is finalized once by the download/save workflow.  It is
    # authoritative and must not pass through legacy candidate whitespace or
    # punctuation normalization before MariaDB persistence.
    source_filename = preserve_file_learning_subject(file_info.get("source_filename"))
    if source_filename and not _is_generic_file_subject(source_filename):
        return preserve_file_learning_subject(
            _append_file_extension_if_missing(source_filename, *extension_sources)
        )
    candidates = [
        file_info.get("display_name"),
        _extract_meta(file_info, "attachment_name"),
        file_info.get("attachment_name"),
        _extract_meta(file_info, "title"),
        file_info.get("title"),
        file_info.get("subject"),
        file_info.get("storage_filename"),
        _file_subject_basename(file_path),
        _file_subject_basename(file_info.get("content")),
        _file_subject_basename(_extract_meta(file_info, "content")),
        file_info.get("filename"),
        file_info.get("name"),
    ]
    for candidate in candidates:
        text = _normalize_file_subject_candidate(candidate)
        if text and not _is_generic_file_subject(text):
            return _normalize_file_subject_for_db(_append_file_extension_if_missing(text, *extension_sources))
    return ""


def _infer_storage_domain_from_local_path(file_path: Any) -> str:
    """Infer the FileUpload storage domain from downloads/{domain}/{uuid}/file paths."""
    try:
        raw = str(file_path or "").replace("\\", "/").strip()
    except Exception:
        return ""
    if not raw:
        return ""
    parts = [p for p in raw.split("/") if p]
    lowered = [p.lower() for p in parts]
    for marker in ("downloads", "fileupload"):
        try:
            idx = lowered.index(marker)
        except ValueError:
            continue
        try:
            candidate = parts[idx + 1].strip()
        except Exception:
            candidate = ""
        if "." in candidate and "/" not in candidate and "\\" not in candidate:
            return candidate
    return ""


def _pick_first_existing_column(cols: Optional[Set[str]], candidates: Iterable[str]) -> Optional[str]:
    """??占쎌젫 ???占쏙옙?占쎈뗄占???占쎈뮉 占?甕곕뜆???袁⑤궖 ?占싼됱쓥筌뤿굞??獄쏆꼹???占쎈뼄."""
    if not cols:
        return None
    for c in candidates:
        if c and c in cols:
            return c
    return None


def _is_valid_file_content_author(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        from backend.board.board_meta_extractor import is_valid_content_author_value
        return bool(is_valid_content_author_value(text))
    except Exception:
        return len(text) <= 80


def _coalesce_author_fields(mapping: Optional[Dict[str, Any]]) -> Optional[str]:
    if not mapping or not isinstance(mapping, dict):
        return None
    rejected: list[str] = []
    for k in ("content_author", "author", "writer", "department", "author_raw"):
        v = mapping.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        try:
            from backend.shared.data_standardizer import DataStandardizer
            out = DataStandardizer.standardize_author(s)
            if out:
                s = out
        except Exception:
            pass
        if _is_valid_file_content_author(s):
            return s
        rejected.append(f"{k}:{_debug_insert_value(s, 120)}")
    if rejected:
        logger.debug("[ContentAuthor] invalid author candidates skipped | %s", rejected[:5])
    return None

def _is_songpa_board_url(value: Any) -> bool:
    return "songpa.go.kr" in str(value or "").lower() and "selectbbsnttview.do" in str(value or "").lower()


def _is_health_seoulmc_url(value: Any) -> bool:
    return "health.seoulmc.or.kr" in str(value or "").lower()


def _is_health_seoulmc_menu_title(value: Any) -> bool:
    compact = re.sub(r"\s+", "", str(value or "")).strip()
    return compact in {"嫄닿컯?占쎌슱?占쎌떇"}


def _extract_persisted_body_text(post_info: Dict[str, Any]) -> str:
    if not isinstance(post_info, dict):
        return ""
    for key in ("content_text", "parsed_content", "clean_content", "body_text", "content_body"):
        value = post_info.get(key)
        if value is None:
            continue
        text = str(value or "").replace("\x00", "").strip()
        if text:
            return text
    return ""


def _learn_list_body_columns(cols: Optional[Set[str]]) -> list[str]:
    if not cols:
        return []
    return [
        col
        for col in ("content_text", "parsed_content", "body_text", "content_body", "content_summary")
        if col in cols
    ]


def _is_weak_board_title_for_persist(value: Any) -> bool:
    txt = str(value or "").strip()
    if not txt or txt == "??占썬걠 ??占쎌벉":
        return True
    if re.match(r"^https?://", txt, flags=re.IGNORECASE):
        return True
    low = txt.lower()
    if any(token in low for token in ("error", "err_", "?占?占쏙옙", "??占쎌첒", "error_title", "error_title_signature")):
        return True
    if txt.count(" < ") >= 1 or txt.count(" > ") >= 1:
        return True
    compact_low = re.sub(r"\s+", "", txt).lower()
    if compact_low in {
        "?占쏀뙆?占쏀뙆援ъ껌songpa-guoffice",
        "?占쏀뙆援ъ껌songpa-guoffice",
        "songpa-guoffice",
    }:
        return True
    if "songpa-gu office" in low and "?占쏀뙆" in txt:
        return True
    compact = txt.replace(" ", "")
    if compact.lower() in {
        "songpa-guoffice",
        "menu",
        "contents",
        "department",
        "organization",
    }:
        return True
        return True
    try:
        # Common weak titles are short menu/organization labels. Do not reject
        # real Korean post titles such as "?占쎌젙洹쒖젣 ?占쎈Т紐⑸줉" just because they
        # are concise and all-Hangul.
        return bool(
            re.fullmatch(
                r"[\uac00-\ud7a3]{2,8}(??占?占?占??占쎌껌|援곗껌|援ъ껌|援먯쑁占?",
                compact,
            )
        )
    except re.error:
        return False


def _normalize_board_title_for_persist(value: Any) -> str:
    txt = re.sub(r"\s+", " ", str(value or "")).strip()
    if not txt:
        return ""
    status_prefixes = (
        "open",
        "closed",
        "scheduled",
        "recruiting",
    )
    changed = True
    while changed:
        changed = False
        for prefix in status_prefixes:
            if txt.startswith(prefix):
                txt = txt[len(prefix):].lstrip()
                if txt.startswith("("):
                    close_idx = txt.find(")")
                    if 0 < close_idx <= 80:
                        txt = txt[close_idx + 1 :].lstrip()
                changed = True
                break
    txt = re.sub(r"\s+\d{4}\.\d{1,2}\.\d{1,2}\.?\s*$", "", txt).strip()
    return txt


def _board_title_mojibake_score(value: Any) -> int:
    txt = str(value or "")
    if not txt:
        return 0
    score = txt.count("\ufffd") * 4
    score += txt.count("저장")
    cjk_count = len(re.findall(r"[\u3400-\u9fff\uf900-\ufaff]", txt))
    hangul_count = len(re.findall(r"[\uac00-\ud7a3]", txt))
    if cjk_count >= 3 and hangul_count == 0:
        score += cjk_count
    return score


def _board_title_quality_score(value: Any) -> int:
    txt = _normalize_board_title_for_persist(value)
    if not txt:
        return -1000
    score = 0
    if _is_weak_board_title_for_persist(txt):
        score -= 300
    score -= _board_title_mojibake_score(txt) * 30
    if re.match(r"^https?://", txt, flags=re.IGNORECASE):
        score -= 500
    if txt.count(" < ") >= 1 or txt.count(" > ") >= 1:
        score -= 120
    length = len(txt)
    if 4 <= length <= 120:
        score += 80
    elif length > 180:
        score -= 80
    if re.search(r"[\uac00-\ud7a3A-Za-z0-9]", txt):
        score += 40
    return score


def _pick_better_board_title_for_persist(existing: Any, incoming: Any) -> str:
    existing_title = _normalize_board_title_for_persist(existing)
    incoming_title = _normalize_board_title_for_persist(incoming)
    if not existing_title:
        return incoming_title
    if not incoming_title:
        return existing_title
    existing_score = _board_title_quality_score(existing_title)
    incoming_score = _board_title_quality_score(incoming_title)
    if incoming_score >= existing_score:
        return incoming_title
    return existing_title


def _is_sungdong_contract_learn_list_url(url: Any) -> bool:
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()
    except Exception:
        raw = str(url or "").lower()
        return "sd.go.kr" in raw and "newselectcontractwebview.do" in raw and "ctrtacctbookmngno=" in raw
    return bool(
        (host == "sd.go.kr" or host.endswith(".sd.go.kr"))
        and path.endswith("/newselectcontractwebview.do")
        and "ctrtacctbookmngno=" in query
    )


async def _fetch_board_title_for_learn_list_fallback(url: str, *, timeout_sec: float = 10.0) -> str:
    url = str(url or "").strip()
    if not url or not re.match(r"^https?://", url, flags=re.IGNORECASE):
        return ""

    def _fetch() -> str:
        ssl_context = None
        if url.lower().startswith("https://"):
            try:
                ssl_context = ssl.create_default_context()
                ssl_context.set_ciphers("DEFAULT@SECLEVEL=0")
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                try:
                    ssl_context.options |= ssl.OP_LEGACY_SERVER_CONNECT
                except AttributeError:
                    pass
            except Exception:
                ssl_context = None
        req = UrlRequest(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        open_kwargs = {"timeout": max(3.0, float(timeout_sec or 10.0))}
        if ssl_context is not None:
            open_kwargs["context"] = ssl_context
        with urlopen(req, **open_kwargs) as resp:
            raw = resp.read()
            content_type = str(resp.headers.get("Content-Type") or "")
        charset_match = re.search(r"charset\s*=\s*['\"]?([^;'\"]+)", content_type, flags=re.IGNORECASE)
        charset = (charset_match.group(1).strip() if charset_match else "") or "utf-8"
        try:
            return raw.decode(charset, errors="replace")
        except Exception:
            return raw.decode("utf-8", errors="replace")

    try:
        html = await asyncio.to_thread(_fetch)
    except Exception as exc:
        db_save_trace_log(
            "board.learn_list.title_fallback.fetch_failed url=%s err=%s",
            _debug_insert_value(url),
            _debug_insert_value(exc, 240),
            level=logging.WARNING,
        )
        return ""
    if not html:
        return ""

    title = ""
    try:
        from edu.extract_html import parse_html_content_for_crawling_mode

        parsed = parse_html_content_for_crawling_mode(html, url)
        if isinstance(parsed, dict):
            title = str(parsed.get("title") or parsed.get("subject") or parsed.get("web_title") or "").strip()
    except Exception as exc:
        db_save_trace_log(
            "board.learn_list.title_fallback.parse_html_failed url=%s err=%s",
            _debug_insert_value(url),
            _debug_insert_value(exc, 240),
            level=logging.WARNING,
        )

    if not title:
        try:
            from bs4 import BeautifulSoup  # type: ignore[import-not-found]

            soup = BeautifulSoup(html, "html.parser")
            try:
                from backend.shared.title_candidate_scoring import extract_title_with_scores

                scored = extract_title_with_scores(soup, url=url)
                scored_title = _normalize_board_title_for_persist(scored.get("title") or "")
                if scored_title and int(scored.get("title_score") or 0) >= 60:
                    title = scored_title
            except Exception:
                pass
            for selector in (
                "div.boardView > h4",
                ".boardView > h4",
                ".viewWrap .boardView > h4",
                ".inboxRead .headinfo > h3.BoR-h2",
                ".inboxRead .headinfo > h2.BoR-h2",
                ".headinfo > h3.BoR-h2",
                ".headinfo > h2.BoR-h2",
                "#contents .cont-top h2.tit",
                "#contents .cont-top .tit",
                "h3.BoR-h2",
                "h2.BoR-h2",
                "meta[property='og:title']",
                "title",
            ):
                if title:
                    break
                el = soup.select_one(selector)
                if not el:
                    continue
                if getattr(el, "name", "") == "meta":
                    title = str(el.get("content") or "").strip()
                else:
                    title = str(el.get_text(" ", strip=True) or "").strip()
                if title:
                    break
        except Exception:
            title = ""

    title = _normalize_board_title_for_persist(title)
    if (
        not title
        or re.match(r"^https?://", title, flags=re.IGNORECASE)
        or any(token in title.lower() for token in ("error", "err_", "error_title", "error_title_signature"))
    ):
        return ""
    return title


def _extract_author(file_info: Dict[str, Any]) -> Optional[str]:
    v = _coalesce_author_fields(file_info)
    if v:
        if _content_author_debug_enabled():
            _content_author_debug(
                "[ContentAuthorDebug][learn_list.extract_author] source=file_info result=%r author=%r content_author=%r writer=%r raw=%r department=%r",
                _content_author_debug_value(v),
                _content_author_debug_value((file_info or {}).get("author")),
                _content_author_debug_value((file_info or {}).get("content_author")),
                _content_author_debug_value((file_info or {}).get("writer")),
                _content_author_debug_value((file_info or {}).get("author_raw")),
                _content_author_debug_value((file_info or {}).get("department")),
            )
        return v
    orig = file_info.get("original_meta", {})
    if isinstance(orig, dict):
        ov = _coalesce_author_fields(orig)
        if _content_author_debug_enabled():
            _content_author_debug(
                "[ContentAuthorDebug][learn_list.extract_author] source=original_meta result=%r author=%r content_author=%r writer=%r raw=%r department=%r",
                _content_author_debug_value(ov),
                _content_author_debug_value(orig.get("author")),
                _content_author_debug_value(orig.get("content_author")),
                _content_author_debug_value(orig.get("writer")),
                _content_author_debug_value(orig.get("author_raw")),
                _content_author_debug_value(orig.get("department")),
            )
        return ov
    if _content_author_debug_enabled():
        _content_author_debug(
            "[ContentAuthorDebug][learn_list.extract_author] source=none result='' keys=%s",
            sorted(str(k) for k in (file_info or {}).keys()) if isinstance(file_info, dict) else [],
        )
    return None


def _extract_author_meta_value(file_info: Dict[str, Any], key: str) -> Optional[str]:
    value = file_info.get(key) if isinstance(file_info, dict) else None
    if value in (None, ""):
        orig = file_info.get("original_meta", {}) if isinstance(file_info, dict) else {}
        if isinstance(orig, dict):
            value = orig.get(key)
    text = str(value or "").strip()
    return text or None


def _learn_list_author_missing(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    try:
        from backend.board.board_meta_extractor import is_valid_content_author_value

        return not is_valid_content_author_value(text)
    except Exception:
        return False


def _extract_content_created_at(file_info: Dict[str, Any]) -> Optional[str]:
    return _format_reg_date(
        _extract_meta(file_info, "file_created_at")
        or _extract_meta(file_info, "content_created_at")
        or _extract_meta(file_info, "reg_date")
        or file_info.get("file_created_at")
        or file_info.get("content_created_at")
        or file_info.get("reg_date")
    )


def _coalesce_duplicate_parsed_fields(meta: Optional[Dict[str, Any]], cols: Optional[Set[str]]) -> Dict[str, str]:
    if not isinstance(meta, dict) or not cols:
        return {}

    out: Dict[str, str] = {}
    author_value = _extract_author(meta)
    if author_value and "content_author" in cols:
        out["content_author"] = author_value

    created_at = _extract_content_created_at(meta)
    if created_at and "content_created_at" in cols:
        out["content_created_at"] = created_at

    return out

def _format_reg_date(value: Any) -> Optional[str]:
    if not value: return None
    if isinstance(value, datetime): return value.strftime("%Y-%m-%d %H:%M:%S")
    normalized = str(value).replace(".", "-").replace("/", "-").replace("T", " ").strip()
    # YYYY-MM-DD HH:MM:SS ?占쎈뗄????占쎈즲
    m = re.match(r"^(\d{4}-\d{1,2}-\d{1,2}(?:\s+\d{1,2}:\d{2}:\d{2})?)", normalized)
    if m:
        val = m.group(1)
        if len(val) <= 10: val += " 00:00:00"
        return val
    return normalized


def _first_non_empty_str(*vals: Any) -> str:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def coalesce_learn_list_cates(info: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    """
    file_saved ??insert_into_learn_list 野껋럥占?占?占쏙옙 ?占쎄쑬履잌첎? ?酉??揶쏅뜄?占쏙쭪????袁⑺뒄????占쎈궔?占?占쏙옙 筌뤴뫁???
    - ?怨몄맄 dict??cate1/cate2(???占쎈챷???? ?占쎈똻??
    - original_meta(癰귣똾???袁⑷퍥 file_meta)??cate1/cate2夷똲tore_*夷똞ssigned_*
    _extract_meta占??怨뺛늺 ?怨몄맄????cate1????占쎌뱽 ??original_meta??癰귣똻? 筌륁궢占???占쎈챷?占썲첎? ??占쏙옙???
    """
    if not isinstance(info, dict):
        return ("", "")
    om = info.get("original_meta")
    om = om if isinstance(om, dict) else {}
    c1 = _first_non_empty_str(
        info.get("cate1"),
        om.get("cate1"),
        om.get("store_cate1"),
        om.get("assigned_cate1"),
        info.get("board_cate1_name"),
        om.get("board_cate1_name"),
        info.get("cate1_name"),
        om.get("cate1_name"),
    )
    c2 = _first_non_empty_str(
        info.get("cate2"),
        om.get("cate2"),
        om.get("store_cate2"),
        om.get("assigned_cate2"),
        info.get("board_cate2_name"),
        om.get("board_cate2_name"),
        info.get("cate2_name"),
        om.get("cate2_name"),
    )
    return (c1, c2)


def learn_list_row_cates_both_empty(
    db_c1: str, db_c2: str, has_cate2_column: bool
) -> bool:
    """
    content 餓λ쵎?????占쎄쑬占?UPDATE ??占쎌뒠 鈺곌퀗占? DB?????貫占??占쎄쑬履잌첎? ?占? ??占쎌뱽 ???占쏙옙 True.
    - cate2 ?占싼됱쓥????占쎌몵占?cate1夷똠ate2 ??????占쏙옙占???占쎈선????
    - cate2 ?占싼됱쓥????占쎌몵占?cate1占???占쏙옙占???占쎌몵占???
    """
    c1 = (db_c1 or "").strip()
    c2 = (db_c2 or "").strip()
    if has_cate2_column:
        return (not c1) and (not c2)
    return not c1


async def learn_list_merge_cate_on_duplicate_row(
    db_name: str,
    table_name: str,
    cols: Set[str],
    row: Any,
    meta: Dict[str, Any],
) -> bool:
    """
    LEARN_LIST content 餓λ쵎????癰귣쵑鍮:
    - ??? ??? cate1?cate2?? ??? ??? ??? ???(?????? cate2 ?????cate1????? ?? UPDATE.
    - ?????? ??? ???????? ????? ?????? ???.
    - ????????????? coalesce??cate1 ??? cate2?? ?????SET ???.
    """
    if not cols or not row:
        return False
    rid = _safe_row_get(row, "id")
    if rid is None:
        return False

    updated = False
    category_update_values: Dict[str, str] = {}
    parsed_update_values: Dict[str, str] = {}
    category_log_payload: Optional[Tuple[str, str, str, str]] = None
    parsed_log_fields: Tuple[str, ...] = ()

    if "cate1" in cols:
        db_c1 = str(_safe_row_get(row, "cate1") or "").strip()
        db_c2 = str(_safe_row_get(row, "cate2") or "").strip() if "cate2" in cols else ""
        has_c2 = "cate2" in cols
        _log_file_learn_list_cate_debug(
            stage="duplicate_merge_decision",
            db_name=db_name,
            table_name=table_name,
            file_info=meta,
            cols=cols,
            row_id=rid,
            extra={
                "db_cate1": db_c1,
                "db_cate2": db_c2,
                "both_empty": learn_list_row_cates_both_empty(db_c1, db_c2, has_c2),
                "cate2_missing": has_c2 and not db_c2,
            },
        )
        sub_cate_mode = str((meta or {}).get("_sub_cate_mode") or (meta or {}).get("sub_cate_mode") or "emp").strip()
        overwrite_sub_cate = is_sub_cate_overwrite(sub_cate_mode)
        n1, n2 = coalesce_learn_list_cates(meta)
        if n1 or n2:
            if "cate1" in cols and should_update_category_field(sub_cate_mode, db_c1, n1):
                category_update_values["cate1"] = n1
            if has_c2 and should_update_category_field(sub_cate_mode, db_c2, n2):
                category_update_values["cate2"] = n2
            if category_update_values:
                category_log_payload = (
                    db_c1,
                    db_c2,
                    category_update_values.get("cate1", db_c1),
                    category_update_values.get("cate2", db_c2),
                )
            else:
                logger.debug(
                    "[LearnList][file] duplicate category kept by sub_cate | id=%s db=(%r,%r) incoming=(%r,%r) sub_cate=%s overwrite=%s",
                    rid,
                    db_c1,
                    db_c2,
                    n1,
                    n2,
                    sub_cate_mode,
                    overwrite_sub_cate,
                )
        pass

    if "content_author" in cols:
        db_author = _safe_row_get(row, "content_author")
        new_author = _extract_author(meta)
        if new_author and _learn_list_author_missing(db_author):
            parsed_update_values["content_author"] = new_author

    for db_col, meta_key in (
        ("content_author_raw", "author_raw"),
        ("content_department", "department"),
        ("content_department_raw", "department_raw"),
    ):
        if db_col not in cols:
            continue
        db_value = str(_safe_row_get(row, db_col) or "").strip()
        new_value = _extract_author_meta_value(meta, meta_key)
        if new_value and not db_value:
            parsed_update_values[db_col] = new_value

    if parsed_update_values:
        parsed_log_fields = tuple(parsed_update_values.keys())

    combined_update_values: Dict[str, str] = {}
    combined_update_values.update(category_update_values)
    combined_update_values.update(parsed_update_values)
    if not combined_update_values:
        return updated

    try:
        set_parts = [f"`{column}` = %s" for column in combined_update_values]
        params = [combined_update_values[column] for column in combined_update_values]
        params.append(rid)
        await mysql_execute_query(
            f"UPDATE `{table_name}` SET {', '.join(set_parts)} WHERE id = %s",
            tuple(params),
            dbname=db_name,
        )
        updated = True
        if category_log_payload is not None:
            db_c1, db_c2, n1, n2 = category_log_payload
            logger.warning(
                "[LearnList][??????][file] ??? ??? | id=%s ???=(%r,%r) -> ???=(%r,%r)",
                rid,
                db_c1,
                db_c2,
                n1,
                n2,
            )
            _log_file_learn_list_cate_debug(
                stage="duplicate_merge_applied",
                db_name=db_name,
                table_name=table_name,
                file_info=meta,
                data=combined_update_values,
                cols=cols,
                row_id=rid,
                extra={"before_cate1": db_c1, "before_cate2": db_c2},
            )
        if parsed_log_fields:
            pass
    except Exception as ex:
        logger.warning(
            "[LearnList] ??? ??combined UPDATE ??? | id=%s err=%s",
            rid,
            ex,
        )

    return updated


async def _ensure_learn_list_content_unique_index(db_name: str, table_name: str) -> bool:
    """
    Return whether LEARN_LIST has a usable UNIQUE index on content.

    Runtime schema changes are intentionally disabled. If the index is missing,
    callers use the SELECT+INSERT fallback path.
    """
    if not db_name or not table_name:
        return False
    key = (db_name, table_name)
    if key in _learn_list_content_unique_ready:
        return _learn_list_content_unique_ready[key]
    raw_check = str(os.getenv("LEARN_LIST_CONTENT_UNIQUE_INDEX_CHECK", "0") or "0").strip().lower()
    if raw_check not in {"1", "true", "yes", "y", "on"}:
        _learn_list_content_unique_ready[key] = False
        return False
    try:
        chk_any = await mysql_execute_query(
            """
            SELECT 1 AS ok FROM information_schema.statistics
            WHERE table_schema = %s AND table_name = %s
              AND column_name = 'content' AND non_unique = 0
            LIMIT 1
            """,
            (db_name, table_name),
            fetch=True,
            dbname=db_name,
            op_name="schema_index_check:learn_list_content_unique_column",
        )
        if chk_any:
            _learn_list_content_unique_ready[key] = True
            logger.debug(
                "[LearnList] content ?占싼됱쓥 ?醫딅빍???紐껊쑔???袁⑹벥 ??占쏙옙? ?類ㅼ뵥 ??UPSERT 野껋럥占?????| db=%s table=%s",
                db_name,
                table_name,
            )
            return True
    except Exception as ex:
        logger.debug("[LearnList] content ?醫딅빍???占싼됱쓥 疫꿸퀣?) 鈺곌퀬????占쎈솭 | %s", ex)
    try:
        chk = await mysql_execute_query(
            """
            SELECT 1 AS ok FROM information_schema.statistics
            WHERE table_schema = %s AND table_name = %s AND index_name = %s AND non_unique = 0
            LIMIT 1
            """,
            (db_name, table_name, _CONTENT_UNIQUE_INDEX_NAME),
            fetch=True,
            dbname=db_name,
            op_name="schema_index_check:learn_list_content_unique_name",
        )
        if chk:
            _learn_list_content_unique_ready[key] = True
            return True
    except Exception as ex:
        logger.debug("[LearnList] content ?醫딅빍??鈺곕똻????? 鈺곌퀬????占쎈솭 | %s", ex)
    # Only id is indexed on current *_LEARN_LIST tables. Avoid unindexed
    # content duplicate prechecks here; file duplicate handling uses the
    # post-download scoped check and DB insert fallback instead.
    _learn_list_content_unique_ready[key] = False
    return False


# =================================================================
# [???占쏙옙] ???占쏙옙 ?類ｋ궖 ????(URL ?類ㅻ뻼 癰궰占??怨몄뒠)
# =================================================================
async def insert_into_learn_list(chat_bot_id: str, db_name: str, file_info: dict) -> Optional[int]:
    """???占쏙옙 ?類ｋ궖??DB????占쎌뿯??占쏙옙???占쎄쉐??ID??獄쏆꼹??(URL ?類ㅻ뻼 ??占쎌젟占?."""
    if not chat_bot_id or not db_name or not file_info.get('url'): return None
    _strip_hash_keys_from_learn_list_input(file_info)
    try:
        file_info["learn_list_duplicate"] = False
        file_info["learn_list_existing_status"] = None
    except Exception:
        pass

    # ?占?占쏙옙?袁る뱜 嚥≪뮄??(疫꿸퀣??疫꿸퀡???占?)
    try:
        _raw_url = str(file_info.get("url") or "")
        _canon_url = canonicalize_attachment_url_for_learn_list(_raw_url) or canonicalize_url_for_dedup(_raw_url)
    except Exception: pass

    try:
        started = time.perf_counter()
        logger.info(
            "[FilePersist][insert_begin] job_id=%s db=%s chat_bot_id=%s file_url=%s source_url=%s file=%s",
            file_info.get("job_id"),
            db_name,
            chat_bot_id,
            str(file_info.get("url") or "")[:220],
            str(file_info.get("source_url") or "")[:220],
            str(file_info.get("name") or file_info.get("subject") or "")[:160],
        )
        # debug checkpoints
        cp_times = {"t0": started}
        try:
            file_sub_cate_mode = await get_sub_cate_mode_from_config(chat_bot_id, dbname=db_name)
        except Exception:
            file_sub_cate_mode = "emp"
        try:
            file_info["_sub_cate_mode"] = file_sub_cate_mode
        except Exception:
            pass
        try:
            ref_c1, ref_c2 = coalesce_learn_list_cates(file_info)
        except Exception:
            ref_c1, ref_c2 = ("", "")
        try:
            if ref_c1 or ref_c2:
                try:
                    ref_c1_name, ref_c2_name = await _resolve_category_names_for_file_meta(
                        chat_bot_id=chat_bot_id,
                        db_name=db_name,
                        cate1=ref_c1,
                        cate2=ref_c2,
                    )
                    orig_meta = file_info.get("original_meta")
                    if not isinstance(orig_meta, dict):
                        orig_meta = {}
                        file_info["original_meta"] = orig_meta
                    if ref_c1_name:
                        orig_meta.setdefault("board_cate1_name", ref_c1_name)
                    if ref_c2_name:
                        orig_meta.setdefault("board_cate2_name", ref_c2_name)
                except Exception:
                    pass
            logger.info(
                "[FilePersist][category_mapping_begin] job_id=%s db=%s file_url=%s cate1=%s cate2=%s",
                file_info.get("job_id"),
                db_name,
                str(file_info.get("url") or "")[:220],
                ref_c1,
                ref_c2,
            )
            mapped_c1, mapped_c2 = await _ensure_file_learning_category_mapping(
                chat_bot_id=chat_bot_id,
                db_name=db_name,
                source_cate1=ref_c1,
                source_cate2=ref_c2,
                access_url=file_info.get("access_url"),
                request_cookies=file_info.get("_category_sync_request_cookies"),
                create_missing=True,
            )
            if mapped_c1 or mapped_c2:
                file_info["cate1"] = mapped_c1
                file_info["cate2"] = mapped_c2
            logger.info(
                "[FilePersist][category_mapping_done] job_id=%s db=%s file_url=%s cate1=%s cate2=%s",
                file_info.get("job_id"),
                db_name,
                str(file_info.get("url") or "")[:220],
                mapped_c1,
                mapped_c2,
            )
            if ref_c1 or ref_c2:
                orig_meta = file_info.get("original_meta")
                if not isinstance(orig_meta, dict):
                    orig_meta = {}
                    file_info["original_meta"] = orig_meta
                orig_meta.setdefault("ref_cate1", ref_c1)
                orig_meta.setdefault("ref_cate2", ref_c2)
            _log_file_learn_list_cate_debug(
                stage="after_category_mapping",
                db_name=db_name,
                file_info=file_info,
                extra={
                    "ref_cate1": ref_c1,
                    "ref_cate2": ref_c2,
                    "mapped_cate1": mapped_c1,
                    "mapped_cate2": mapped_c2,
                },
            )
        except Exception as exc:
            log_fn = logger.debug if (ref_c1 or ref_c2) else logger.debug
            log_fn(
                "[CategorySync][file-insert] category remap skipped | db=%s chat_bot_id=%s ref=(%s,%s) err=%s",
                db_name,
                chat_bot_id,
                ref_c1,
                ref_c2,
                exc,
            )
        # 1. ???占쏙옙???類ｋ궖 占???占쏙옙???類ｋ궖
        account_id = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        cp_times["t1"] = time.perf_counter()
        table_name = get_learn_list_table_name(account_id)
        try:
            file_info["_learn_list_table_name"] = table_name
        except Exception:
            pass
        logger.info(
            "[FilePersist][table_resolved] job_id=%s db=%s table=%s file_url=%s",
            file_info.get("job_id"),
            db_name,
            table_name,
            str(file_info.get("url") or "")[:220],
        )
        cp_times["t2"] = time.perf_counter()
        url = file_info.get('url')
        await _maybe_apply_file_insert_backpressure(url)
        canon_url = canonicalize_attachment_url_for_learn_list(url) or canonicalize_url_for_dedup(url)
        cp_times["t3"] = time.perf_counter()

        if not await _learn_list_table_exists(db_name, table_name):
            logger.warning(
                "[LearnList] insert skipped: LEARN_LIST table not found "
                "(chat_bot_id mismatch or unprovisioned table) | db=%s table=%s chat_bot_id=%s job_id=%s url=%s",
                db_name,
                table_name,
                chat_bot_id,
                file_info.get("job_id"),
                (url or "")[:180],
            )
            return None

        cp_before_cols = time.perf_counter()
        cols = await ensure_learn_list_standard_columns(db_name, table_name)
        cp_after_cols = time.perf_counter()
        _log_file_learn_list_cate_debug(
            stage="table_columns_loaded",
            db_name=db_name,
            table_name=table_name,
            file_info=file_info,
            cols=cols,
            extra={"col_count": len(cols or [])},
        )

        # 2. ???占쏙옙占?占???占썬걠 野껉퀣??
        subject = _normalize_file_subject_for_db(_resolve_file_learning_subject(file_info) or url.split('/')[-1])
        _fp = file_info.get('file_path') or file_info.get('local_path') or ''
        storage_filename = file_info.get('storage_filename') or (os.path.basename(_fp) if _fp else url.split("/")[-1])

        # 3. LEARN_LIST content URL ?占쎌꽦
        # 寃쎈줈 ?占쎌뒋 ?占쎌씤 臾몄꽌: backend/docs/FILE_STORAGE_FLOW.md
        # 臾쇰━ ?占?? /FileUpload/{domain}/{UUIDtail12}/{filename}
        # DB/viewer URL: /chat/uploaded_files/{UUIDtail12}/{filename}
        try:
            domain = (
                str(file_info.get("storage_domain") or "").strip()
                or _infer_storage_domain_from_local_path(_fp)
                or get_storage_domain_for_db_name(db_name)
                or "dev.han.kr"
            )
            access_base = normalize_access_url(file_info.get("access_url"), db_name)
            content_value = get_file_upload_content_url(access_base, domain, chat_bot_id, storage_filename)
        except Exception:
            content_value = get_file_upload_content_url(normalize_access_url(None, db_name), "dev.han.kr", chat_bot_id, storage_filename)

        # 4. ???占쏙옙 ??占??占쎄쑴占?
        try:
            file_size = int(file_info.get('size') or 0)
        except (TypeError, ValueError):
            file_size = 0
        if file_size == 0 and _fp and os.path.exists(_fp):
            file_size = os.path.getsize(_fp)

        # 5. ?怨쀬뵠???占싼딄쉐
        data = {
            "content": content_value,
            "subject": str(subject).strip(),
            # ???占쏙옙 ??쨌夷뚳㎗?? ??占쎈뮸 野껋럥占? MIME/url ???怨몄맄 筌롳옙???? ?占쎈떯???占쎌쓺 LEARN_LIST????占?file 占????占쏙옙
            "content_type": "file",
            "status": 'N',
            "size": file_size,
            "created_at": datetime.now(),
        }
        if _fp:
            data["content_address"] = _fp
        content_updated_at = (
            file_info.get("content_updated_at")
            or file_info.get("file_updated_at")
            or file_info.get("updated_at")
        )
        if content_updated_at:
            data["content_updated_at"] = content_updated_at
        data["chunk"] = 0
        data["segments"] = file_info.get("segments") or 0
        data["embedding_tokens"] = file_info.get("embedding_tokens") or 0

        # 6. ?占싼됱쓥 ?袁り숲占?占?筌롳옙?? ?占쎈떽? (cols??1b?占?占쏙옙 ??占? ?類ｋ궖)
        if cols:
            _cc1, _cc2 = coalesce_learn_list_cates(file_info)
            data["cate1"] = _cc1
            if "cate2" in cols:
                data["cate2"] = _cc2
            data["content_author"] = _extract_author(file_info)
            content_created_at = _extract_content_created_at(file_info)
            if content_created_at:
                data["content_created_at"] = content_created_at
            source_url = str(
                file_info.get("source_url")
                or _extract_meta(file_info, "source_url")
                or file_info.get("source_page")
                or _extract_meta(file_info, "source_page")
                or _extract_meta(file_info, "post_url")
                or ""
            ).strip()
            if source_url and "source_url" in cols:
                data["source_url"] = source_url
            data = _filter_learn_list_visible_write_data(data, cols)
            _ensure_learn_list_hash_columns_null(data, cols)
        else:
            data = _filter_learn_list_visible_write_data(data)

        if _content_author_debug_enabled():
            original_meta = file_info.get("original_meta") if isinstance(file_info.get("original_meta"), dict) else {}
            _content_author_debug(
                "[ContentAuthorDebug][learn_list.payload] job_id=%s db=%s table=%s url=%s subject=%r file_author=%r file_content_author=%r data_content_author=%r department=%r kind=%r raw=%r original_author=%r has_column=%s data_keys=%s",
                file_info.get("job_id"),
                db_name,
                table_name,
                (str(file_info.get("url") or "")[:220]),
                _content_author_debug_value(data.get("subject")),
                _content_author_debug_value(file_info.get("author")),
                _content_author_debug_value(file_info.get("content_author")),
                _content_author_debug_value(data.get("content_author")),
                _content_author_debug_value(file_info.get("department")),
                _content_author_debug_value(file_info.get("author_kind")),
                _content_author_debug_value(file_info.get("author_raw")),
                _content_author_debug_value(original_meta.get("content_author") or original_meta.get("author")),
                bool(cols and "content_author" in cols),
                sorted(str(k) for k in data.keys()),
            )

        _log_file_learn_list_cate_debug(
            stage="payload_built",
            db_name=db_name,
            table_name=table_name,
            file_info=file_info,
            data=data,
            cols=cols,
            extra={"payload_keys": sorted(str(k) for k in data.keys())},
        )

        def _log_learn_list_debug(stage: str, *, row_id: Any = None, duplicate: Any = None, extra: str = "") -> None:
            logger.debug(
                "[LearningDebug][learn_list.%s] job_id=%s db=%s table=%s row_id=%s duplicate=%s elapsed_ms=%s url=%s subject=%s extra=%s",
                stage,
                file_info.get("job_id"),
                db_name,
                table_name,
                row_id,
                duplicate,
                int((time.perf_counter() - started) * 1000),
                (canon_url or "")[:160],
                str(data.get("subject") or "")[:120],
                extra,
            )

        def _duplicate_merge_select_columns() -> list[str]:
            sels = ["`id`"]
            if "content" in cols:
                sels.append("`content`")
            if "cate1" in cols:
                sels.append("`cate1`")
            if "cate2" in cols:
                sels.append("`cate2`")
            if "status" in cols:
                sels.append("`status`")
            for parsed_col in (
                "content_author",
                "content_created_at",
            ):
                if parsed_col in cols:
                    sels.append(f"`{parsed_col}`")
            return sels

        async def _load_duplicate_merge_row(
            *,
            row_id: Any = None,
            content: Optional[str] = None,
        ) -> Tuple[Optional[Dict[str, Any]], Any]:
            if row_id is not None:
                where_sql = "`id` = %s"
                params = (row_id,)
            elif content:
                where_sql = "`content` = %s"
                params = (content,)
            else:
                return None, row_id
            mrows = await mysql_execute_query(
                f"SELECT {', '.join(_duplicate_merge_select_columns())} FROM `{table_name}` WHERE {where_sql} LIMIT 1",
                params,
                fetch=True,
                dbname=db_name,
            )
            if not mrows:
                return None, row_id
            row = mrows[0]
            return row, _safe_row_get(row, "id")

        async def _verify_file_learn_list_cate_row(stage: str, row_id: Any) -> None:
            verify_enabled = str(os.getenv("FILE_LEARN_LIST_VERIFY_AFTER_WRITE", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
            if not verify_enabled:
                return
            if row_id is None:
                _log_file_learn_list_cate_debug(
                    stage=f"{stage}_verify_skipped",
                    db_name=db_name,
                    table_name=table_name,
                    file_info=file_info,
                    data=data,
                    cols=cols,
                    extra={"reason": "row_id_missing"},
                )
                return
            try:
                select_cols = ["`id`"]
                for col in (
                    "content_type",
                    "cate1",
                    "cate2",
                    "content",
                    "subject",
                    "source_url",
                    "content_author",
                ):
                    if not cols or col in cols:
                        select_cols.append(f"`{col}`")
                rows = await mysql_execute_query(
                    f"SELECT {', '.join(select_cols)} FROM `{table_name}` WHERE id = %s LIMIT 1",
                    (row_id,),
                    fetch=True,
                    dbname=db_name,
                )
                row = rows[0] if rows else {}
                _log_file_learn_list_cate_debug(
                    stage=f"{stage}_verify",
                    db_name=db_name,
                    table_name=table_name,
                    file_info=file_info,
                    data=row if isinstance(row, dict) else {},
                    cols=cols,
                    row_id=row_id,
                    extra={"found": bool(rows)},
                )
            except Exception as exc:
                _log_file_learn_list_cate_debug(
                    stage=f"{stage}_verify_failed",
                    db_name=db_name,
                    table_name=table_name,
                    file_info=file_info,
                    data=data,
                    cols=cols,
                    row_id=row_id,
                    extra={"err": str(exc)[:300]},
                )

        learn_list_file_dup_debug_log(
            "INSERT ??占쎈즲 | table=%s content=%s job_id=%s subject=%s",
            table_name,
            (canon_url or "")[:220],
            file_info.get("job_id"),
            (str(data.get("subject") or "")[:80] or "-"),
        )
        _log_learn_list_debug("start")
        allow_existing_duplicate_update = str(
            os.getenv("FILE_CRAWL_ALLOW_EXISTING_DUPLICATE_ROW_UPDATE", "0") or "0"
        ).strip().lower() in ("1", "true", "yes", "on")

        async def _preselect_pending_duplicate_row() -> Optional[int]:
            # Speed-first file crawl: pre-insert LEARN_LIST duplicate SELECTs are disabled.
            # Downloaded files are checked by source_url + normalized filename + file_size,
            # then DB unique/upsert and 1062 handling remain as final defense.
            return None
        reused_pending_id = await _preselect_pending_duplicate_row()
        if reused_pending_id is not None:
            return reused_pending_id

        # 7a. content ?醫딅빍??+ UPSERT.
        content_dup_key_ok = False
        use_content_unique_upsert = bool(canon_url and cols and "content" in cols and data.get("content"))
        if use_content_unique_upsert:
            content_dup_key_ok = await _ensure_learn_list_content_unique_index(db_name, table_name)
            cp_after_unique = time.perf_counter()
            if content_dup_key_ok:
                # File crawl hot path: do not pre-read existing LEARN_LIST rows.
                # UNIQUE/UPSERT below is the duplicate guard; row payload loading holds
                # MariaDB connections and fetches fields not needed for normal file saving.
                working = dict(data)
                _log_file_learn_list_insert_value_gaps(
                    stage="upsert_before",
                    db_name=db_name,
                    table_name=table_name,
                    data=working,
                    cols=cols,
                    file_info=file_info,
                    source_url=url,
                )
                for _ups in range(6):
                    try:
                        col_keys = list(working.keys())
                        col_sql = ", ".join(f"`{c}`" for c in col_keys)
                        ph = ", ".join(["%s"] * len(col_keys))
                        upsert_sql = (
                            f"INSERT INTO `{table_name}` ({col_sql}) VALUES ({ph}) "
                            f"ON DUPLICATE KEY UPDATE `id` = LAST_INSERT_ID(`id`)"
                        )
                        t_before_upsert = time.perf_counter()
                        lid, rc = await mysql_upsert_then_last_insert_id(
                            upsert_sql,
                            tuple(working[k] for k in col_keys),
                            dbname=db_name,
                        )
                        t_after_upsert = time.perf_counter()
                        was_dup = rc >= 2
                        try:
                            file_info["learn_list_duplicate"] = bool(was_dup)
                        except Exception:
                            pass
                        if was_dup:
                            learn_list_file_dup_debug_log(
                                "UPSERT 餓λ쵎????| table=%s id=%s content=%s job_id=%s rc=%s",
                                table_name,
                                lid,
                                (canon_url or "")[:220],
                                file_info.get("job_id"),
                                rc,
                            )
                        else:
                            learn_list_file_dup_debug_log(
                                "UPSERT ?醫됲뇣 ??占쎌뿯 | table=%s id=%s content=%s job_id=%s",
                                table_name,
                                lid,
                                (canon_url or "")[:220],
                                file_info.get("job_id"),
                            )

                        # ?醫됲뇣 ??rowcount==1): ?怨쀬뵠?怨뺣뮉 INSERT ??甕곕뜆占???占쎈선占? SELECT夷똠ate merge ?類ｋ궗 ??占쎄탢.
                        if not was_dup and lid is not None:
                            try:
                                remember_learn_list_url_row(
                                    db_name=db_name,
                                    table_name=table_name,
                                    row={"id": lid, "content": data.get("content"), **data},
                                )
                            except Exception:
                                pass
                            insert_end = t_after_upsert
                            total_ms = int((insert_end - started) * 1000)
                            _log_learn_list_insert_slow(
                                total_ms,
                                url,
                                "[LearnList] Insert slow: %sms | path=new_row %s | table=%s | url=%s",
                                total_ms,
                                _learn_list_insert_slow_ms_segments(
                                    started,
                                    cp_after_cols,
                                    cp_after_unique,
                                    t_before_upsert,
                                    t_after_upsert,
                                ),
                                table_name,
                                (url or "")[:120],
                            )
                            _log_learn_list_debug(
                                "upsert_new",
                                row_id=lid,
                                duplicate=False,
                                extra=f"rowcount={rc}",
                            )
                            await _verify_file_learn_list_cate_row("upsert_new", lid)
                            log_file_category_trace(
                                "저장",
                                db=db_name,
                                table=table_name,
                                learn_list_id=lid,
                                cate1=data.get("cate1"),
                                cate2=data.get("cate2"),
                            )
                            try:
                                return int(lid)
                            except (TypeError, ValueError):
                                return lid

                        eff_id = lid
                        # File crawl hot path: duplicate UPSERT already returned the row id.
                        # Avoid a follow-up SELECT of id/content/cate/status/author fields.
                        if was_dup and not allow_existing_duplicate_update:
                            insert_end = time.perf_counter()
                            total_ms = int((insert_end - started) * 1000)
                            post_ms = int((insert_end - t_after_upsert) * 1000)
                            _log_learn_list_insert_slow(
                                total_ms,
                                url,
                                "[LearnList] Insert slow: %sms | path=dup_upsert_no_row_load post_upsert=%sms | %s | table=%s | url=%s",
                                total_ms,
                                post_ms,
                                _learn_list_insert_slow_ms_segments(
                                    started,
                                    cp_after_cols,
                                    cp_after_unique,
                                    t_before_upsert,
                                    t_after_upsert,
                                ),
                                table_name,
                                (url or "")[:120],
                            )
                            _log_learn_list_debug(
                                "upsert_duplicate",
                                row_id=eff_id,
                                duplicate=True,
                                extra=f"rowcount={rc} row_load=skipped post_ms={post_ms}",
                            )
                            log_file_category_trace(
                                "저장",
                                db=db_name,
                                table=table_name,
                                learn_list_id=eff_id,
                                cate1=data.get("cate1"),
                                cate2=data.get("cate2"),
                            )
                            try:
                                return int(eff_id) if eff_id is not None else None
                            except (TypeError, ValueError):
                                return eff_id
                    except Exception as exc:
                        col = _drop_unknown_column_and_retryable(exc)
                        if col and col != "__UNKNOWN__" and col in working:
                            logger.warning(f"[LearnList] UPSERT dropping unknown column: {col}")
                            working.pop(col, None)
                            continue
                        raise
                logger.error("[LearnList] UPSERT failed after column retries | table=%s", table_name)
                _log_learn_list_debug("upsert_failed", duplicate=None, extra="column_retries_exhausted")
                return None

        # 7b. ??占쏙옙? ?醫딅빍??沃섎챷??????醫롳옙????INSERT.
        async def _fallback_content_insert() -> Optional[int]:
            lookup_col = "content" if cols and "content" in cols else ""
            lookup_value = data.get("content") if lookup_col == "content" else ""
            learn_list_insert_debug_log(
                "file_fallback start db=%s table=%s lookup_col=%s lookup_value=%s content=%s job_id=%s",
                db_name,
                table_name,
                lookup_col,
                _debug_insert_value(lookup_value),
                _debug_insert_value(data.get("content")),
                file_info.get("job_id"),
            )
            if lookup_value and cols and lookup_col:
                try:
                    # File crawl hot path: do not load LEARN_LIST rows for preselect.
                    # Cache lookup can trigger a large LEARN_LIST load and holds a MariaDB connection.
                    rows_exist = []
                    learn_list_insert_debug_log(
                        "file_preselect result db=%s table=%s lookup_col=%s rows=%s lookup_value=%s",
                        db_name,
                        table_name,
                        lookup_col,
                        len(rows_exist or []),
                        _debug_insert_value(lookup_value),
                    )
                    if rows_exist:
                        try:
                            remember_learn_list_url_row(db_name=db_name, table_name=table_name, row=rows_exist[0])
                        except Exception:
                            pass
                        existing_id = _safe_row_get(rows_exist[0], "id")
                        if existing_id is not None:
                            try:
                                file_info["learn_list_duplicate"] = True
                                existing_status = str(_safe_row_get(rows_exist[0], "status") or "").strip().upper()
                                file_info["learn_list_existing_status"] = existing_status or None
                            except Exception:
                                pass
                            learn_list_file_dup_debug_log(
                                "?醫롳옙??HIT ??INSERT ??占쎌셽 | table=%s id=%s content=%s job_id=%s",
                                table_name,
                                existing_id,
                                (canon_url or "")[:200],
                                file_info.get("job_id"),
                            )
                            await learn_list_merge_cate_on_duplicate_row(
                                db_name, table_name, cols, rows_exist[0], file_info
                            )
                            if not allow_existing_duplicate_update:
                                _log_learn_list_debug(
                                    "preselect_duplicate",
                                    row_id=existing_id,
                                    duplicate=True,
                                    extra="fallback_preselect_hit",
                                )
                                await _verify_file_learn_list_cate_row("fallback_preselect_duplicate", existing_id)
                                try:
                                    return int(existing_id)
                                except (TypeError, ValueError):
                                    return existing_id
                except Exception as ex_lookup:
                    logger.debug("[LearnList] %s ?醫롳옙????占쎈솭(INSERT ?占쎄쑴?? | %s", lookup_col, ex_lookup)

            try:
                _log_file_learn_list_insert_value_gaps(
                    stage="fallback_before",
                    db_name=db_name,
                    table_name=table_name,
                    data=data,
                    cols=cols,
                    file_info=file_info,
                    source_url=url,
                )
                learn_list_insert_debug_log(
                    "file_insert call db=%s table=%s content=%s subject=%s",
                    db_name,
                    table_name,
                    _debug_insert_value(data.get("content")),
                    _debug_insert_value(data.get("subject")),
                )
                new_id = await _safe_maria_insert_data(table_name, data, db_name, warning_context={"source_url": source_url, "detail_url": source_url, "file_url": url, "job_id": file_info.get("job_id")})
                try:
                    remember_learn_list_url_row(
                        db_name=db_name,
                        table_name=table_name,
                        row={"id": new_id, "content": data.get("content"), **data},
                    )
                except Exception:
                    pass
            except Exception as exc:
                learn_list_insert_debug_log(
                    "file_insert exception db=%s table=%s err=%s content=%s",
                    db_name,
                    table_name,
                    _debug_insert_value(exc, 600),
                    _debug_insert_value(data.get("content")),
                    level=logging.WARNING,
                )
                if "duplicate" in str(exc).lower() or "1062" in str(exc).lower():
                    learn_list_file_dup_debug_log(
                        "INSERT 餓λ쵎????占쎌뇚(野껓옙?鍮夷똗NIQUE 揶쎛?? | table=%s content=%s job_id=%s err=%s",
                        table_name,
                        (canon_url or "")[:220],
                        file_info.get("job_id"),
                        str(exc)[:300],
                    )
                    sel_parts = ["`id`"]
                    if cols and "cate1" in cols:
                        sel_parts.append("`cate1`")
                    if cols and "cate2" in cols:
                        sel_parts.append("`cate2`")
                    if cols and "status" in cols:
                        sel_parts.append("`status`")
                    if cols and "content" in cols:
                        sel_parts.append("`content`")
                    if cols and "content_type" in cols:
                        sel_parts.append("`content_type`")
                    if cols and "type" in cols:
                        sel_parts.append("`type`")
                    if cols:
                        for parsed_col in (
                            "content_author",
                            "content_author_raw",
                            "content_department",
                            "content_department_raw",
                            "content_created_at",
                        ):
                            if parsed_col in cols:
                                sel_parts.append(f"`{parsed_col}`")
                    sel_sql = ", ".join(sel_parts)
                    rows = await mysql_execute_query(
                        f"SELECT {sel_sql} FROM `{table_name}` WHERE `{lookup_col or 'content'}` = %s LIMIT 1",
                        (lookup_value or data.get("content"),),
                        fetch=True,
                        dbname=db_name,
                    )
                    learn_list_insert_debug_log(
                        "file_duplicate_reselect result db=%s table=%s lookup_col=%s rows=%s lookup_value=%s",
                        db_name,
                        table_name,
                        lookup_col or "content",
                        len(rows or []),
                        _debug_insert_value(lookup_value or data.get("content")),
                    )
                    if rows:
                        file_info["learn_list_duplicate"] = True
                        try:
                            existing_status = str(_safe_row_get(rows[0], "status") or "").strip().upper()
                            file_info["learn_list_existing_status"] = existing_status or None
                        except Exception:
                            pass
                        learn_list_file_dup_debug_log(
                            "1062 ???????HIT | table=%s id=%s content=%s job_id=%s",
                            table_name,
                            _safe_row_get(rows[0], "id"),
                            (canon_url or "")[:220],
                            file_info.get("job_id"),
                        )
                        if cols:
                            await learn_list_merge_cate_on_duplicate_row(
                                db_name, table_name, cols, rows[0], file_info
                            )

                        _log_learn_list_debug(
                            "fallback_duplicate",
                            row_id=_safe_row_get(rows[0], "id"),
                            duplicate=True,
                            extra="insert_1062_reselect",
                        )
                        await _verify_file_learn_list_cate_row("fallback_duplicate", _safe_row_get(rows[0], "id"))
                        return _safe_row_get(rows[0], "id")
                    raise
                else:
                    raise
            insert_end = time.perf_counter()
            learn_list_file_dup_debug_log(
                "INSERT ?源껊궗 | table=%s new_id=%s content=%s job_id=%s",
                table_name,
                new_id,
                (canon_url or "")[:220],
                file_info.get("job_id"),
            )
            if _content_author_debug_enabled():
                _content_author_debug(
                    "[ContentAuthorDebug][learn_list.insert_success] job_id=%s db=%s table=%s row_id=%s url=%s data_content_author=%r file_author=%r file_content_author=%r",
                    file_info.get("job_id"),
                    db_name,
                    table_name,
                    new_id,
                    (str(file_info.get("url") or "")[:220]),
                    _content_author_debug_value(data.get("content_author")),
                    _content_author_debug_value(file_info.get("author")),
                    _content_author_debug_value(file_info.get("content_author")),
                )
            total_ms = int((insert_end - started) * 1000)
            _log_learn_list_insert_slow(
                total_ms,
                url,
                "[LearnList] Insert slow: %sms | path=fallback_insert | table=%s | url=%s",
                total_ms,
                table_name,
                (url or "")[:100],
            )
            _log_learn_list_debug(
                "fallback_insert",
                row_id=new_id,
                duplicate=False,
            )
            learn_list_insert_debug_log(
                "file_insert success db=%s table=%s new_id=%s content=%s",
                db_name,
                table_name,
                new_id,
                _debug_insert_value(data.get("content")),
            )
            await _verify_file_learn_list_cate_row("fallback_insert", new_id)
            log_file_category_trace(
                "저장",
                db=db_name,
                table=table_name,
                learn_list_id=new_id,
                cate1=data.get("cate1"),
                cate2=data.get("cate2"),
            )
            return new_id

        lock_key = str(data.get("content") or canon_url or url or "")
        if lock_key and not content_dup_key_ok:
            return await mysql_user_lock_run(
                db_name,
                _learn_list_content_user_lock_name(lock_key),
                30,
                _fallback_content_insert,
            )
        return await _fallback_content_insert()

    except Exception as e:
        learn_list_insert_debug_log(
            "file_insert outer_exception db=%s job_id=%s err=%s file_info_keys=%s",
            db_name,
            file_info.get("job_id") if isinstance(file_info, dict) else None,
            _debug_insert_value(e, 800),
            ",".join(sorted(str(k) for k in file_info.keys())) if isinstance(file_info, dict) else "-",
            level=logging.ERROR,
        )
        logger.error(
            "[LearningDebug][learn_list.error] job_id=%s db=%s elapsed_ms=%s err=%s",
            file_info.get("job_id") if isinstance(file_info, dict) else None,
            db_name,
            int((time.perf_counter() - started) * 1000) if "started" in locals() else None,
            e,
        )
        logger.error(f"[LearnList] insert error: {e}", exc_info=True)
        return None

# =================================================================
# [疫꿸퀬? 癰귣똻????占쎈땾] (疫꿸퀣??嚥≪뮇占??占?)
# =================================================================
async def ensure_learn_list_standard_columns(db_name: str, table_name: str) -> Set[str]:
    """LEARN_LIST ?袁⑹삺 ?占싼됱쓥 筌뤴뫖以됵쭕?獄쏆꼹???占쎈뼄."""
    if not db_name or not table_name:
        return set()
    return await _get_table_columns(db_name, table_name)

# crawling_log ???占쏙옙??update
async def update_mariadb_on_save(job_id, db_name, save_count, **kwargs) -> bool:
    t0 = time.perf_counter()
    result = await update_crawling_log_counters(job_id=job_id, saved=save_count, dbname=db_name, log_id=kwargs.get("craw_id"))
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    if _db_load_debug_enabled() or elapsed_ms >= _db_load_slow_ms():
        logger.debug("[DBLoad] crawling_log save counter updated | db=%s job_id=%s saved=%s elapsed_ms=%.1f", db_name, job_id, save_count, elapsed_ms)
    return result


def build_board_post_learn_list_input_preview(
    post_info: dict,
    *,
    cols: Optional[Set[str]] = None,
    created_at: Any = None,
) -> Dict[str, Any]:
    """
    Build the final LEARN_LIST insert payload for a board post without touching DB.

    This mirrors the non-duplicate insert payload assembled inside
    insert_board_post_into_learn_list(), so diagnostics such as inspect_detail.py can
    show the same field names/values that MariaDB would receive.
    """
    if not isinstance(post_info, dict):
        return {}

    info = dict(post_info)
    _strip_hash_keys_from_learn_list_input(info)

    stored_url = str(info.get("post_url") or info.get("url") or "").strip()
    if not stored_url:
        return {}

    title_candidate = _normalize_board_title_for_persist(info.get("title") or info.get("subject") or "")
    web_title_candidate = _normalize_board_title_for_persist(info.get("web_title") or "")
    title_guard_locked = bool(info.get("title_guard_locked"))
    if title_guard_locked:
        title = title_candidate
        web_title = web_title_candidate or title_candidate
    else:
        title = _pick_better_board_title_for_persist(web_title_candidate, title_candidate)
        web_title = _pick_better_board_title_for_persist(title, web_title_candidate)
    if _is_health_seoulmc_url(stored_url) and _is_health_seoulmc_menu_title(title):
        title = ""
    if _is_health_seoulmc_url(stored_url) and _is_health_seoulmc_menu_title(web_title):
        web_title = title
    if _is_sungdong_contract_learn_list_url(stored_url) and title_candidate and not _is_weak_board_title_for_persist(title_candidate):
        title = title_candidate
        web_title = title_candidate
    if _is_weak_board_title_for_persist(title):
        title = ""
    if _is_weak_board_title_for_persist(web_title):
        web_title = ""

    try:
        cate1_val = _normalize_cate_code(info.get("cate1"))
    except Exception:
        cate1_val = None
    try:
        cate2_val = _normalize_cate_code(info.get("cate2"))
    except Exception:
        cate2_val = None

    try:
        size_val = int(info.get("size") or 0)
    except Exception:
        size_val = 0

    created_value = _format_reg_date(info.get("reg_date") or info.get("content_created_at"))
    updated_value = _format_reg_date(info.get("content_updated_at"))

    data: Dict[str, Any] = {
        "content": stored_url,
        "subject": title or stored_url,
        "content_type": "url",
        "status": "N",
        "size": size_val,
        "created_at": created_at if created_at is not None else datetime.now(),
    }

    effective_cols = cols or set(_LEARN_LIST_DB_COLUMNS)
    if effective_cols:
        if "cate1" in effective_cols:
            data["cate1"] = cate1_val or ""
        if "cate2" in effective_cols:
            data["cate2"] = cate2_val or ""
        if "web_title" in effective_cols:
            data["web_title"] = web_title or title or ""
        if "content_created_at" in effective_cols and created_value:
            data["content_created_at"] = created_value
        if "content_updated_at" in effective_cols and updated_value:
            data["content_updated_at"] = updated_value

        author_for_db = _coalesce_author_fields(info)
        if author_for_db and "content_author" in effective_cols:
            data["content_author"] = author_for_db

        data = _filter_learn_list_visible_write_data(data, effective_cols)
        _ensure_learn_list_hash_columns_null(data, effective_cols)
    else:
        data = _filter_learn_list_visible_write_data(data)

    return data


async def insert_board_posts_into_learn_list_batch(
    chat_bot_id: str,
    db_name: str,
    post_infos: list[dict],
) -> list[Optional[int]]:
    """Insert several board posts into LEARN_LIST with fewer MariaDB round trips."""
    results: list[Optional[int]] = [None] * len(post_infos or [])
    if not chat_bot_id or not db_name or not post_infos:
        return results
    if len(post_infos) == 1:
        results[0] = await insert_board_post_into_learn_list(chat_bot_id, db_name, post_infos[0])
        return results
    safe_single_insert = str(os.getenv("BOARD_LEARN_LIST_BATCH_SAFE_SINGLE_INSERT", "0") or "0").strip().lower() in {
        "1",
        "true",
        "y",
        "yes",
        "on",
    }
    if safe_single_insert:
        for idx, info in enumerate(post_infos or []):
            try:
                results[idx] = await insert_board_post_into_learn_list(chat_bot_id, db_name, info)
            except Exception:
                results[idx] = None
        return results

    valid: list[tuple[int, dict, str]] = []
    for idx, info in enumerate(post_infos):
        if not isinstance(info, dict):
            continue
        _strip_hash_keys_from_learn_list_input(info)
        try:
            info["learn_list_duplicate"] = False
            info["learn_list_existing_status"] = None
        except Exception:
            pass
        url = str(info.get("post_url") or info.get("url") or "").strip()
        if url:
            valid.append((idx, info, url))
    if not valid:
        _learn_list_batch_debug_log(
            "no_valid_post_infos",
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            requested_count=len(post_infos or []),
            invalid_count=len(post_infos or []),
        )
        return results

    batch_total_t0 = time.perf_counter()
    first_url = valid[0][2] if valid else ""
    _learn_list_batch_debug_log(
        "start",
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        requested_count=len(post_infos or []),
        valid_count=len(valid),
        unique_url_count=len(set(url for _, _, url in valid)),
        first_url=first_url,
        sample_urls=_learn_list_batch_debug_sample((url for _, _, url in valid)),
        sample_titles=_learn_list_batch_debug_sample(
            ((info.get("title") or info.get("subject") or "") for _, info, _ in valid)
        ),
    )
    board_save_flow_trace(
        "learn_list_batch_insert",
        "start",
        db=db_name,
        counts={"requested": len(post_infos or []), "valid": len(valid)},
        url=first_url,
    )

    try:
        db_step_t0 = time.perf_counter()
        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        table_name = get_learn_list_table_name(account_identifier)
        cols = await ensure_learn_list_standard_columns(db_name, table_name)
        board_save_flow_trace(
            "learn_list_batch_resolve_table",
            "end",
            started_at=db_step_t0,
            db=db_name,
            table=table_name,
            col_count=len(cols or []),
            url=first_url,
        )
        if not cols and not await _learn_list_table_exists(db_name, table_name):
            return results

        unique_urls = list(dict.fromkeys(url for _, _, url in valid))
        if len(unique_urls) != len(valid):
            _learn_list_batch_debug_log(
                "duplicate_urls_inside_batch",
                db_name=db_name,
                table=table_name,
                valid_count=len(valid),
                unique_url_count=len(unique_urls),
                duplicate_inside_batch_count=max(0, len(valid) - len(unique_urls)),
                sample_urls=_learn_list_batch_debug_sample(unique_urls),
            )
        existing_by_url: dict[str, dict] = {}
        duplicate_select_columns = (
            "id",
            "status",
            "content_created_at",
            "content_updated_at",
            "subject",
            "web_title",
            "cate1",
            "cate2",
            "content_author",
            "content_author_raw",
            "content_department",
            "content_department_raw",
        )
        if unique_urls and "content" in (cols or _LEARN_LIST_DB_COLUMNS):
            db_step_t0 = time.perf_counter()
            board_save_flow_trace(
                "learn_list_batch_duplicate_lookup",
                "start",
                db=db_name,
                table=table_name,
                counts={"unique_urls": len(unique_urls)},
                url=first_url,
            )
            placeholders = ", ".join(["%s"] * len(unique_urls))
            content_type_filter_sql = "`content_type` = %s AND " if (cols and "content_type" in cols) else ""
            content_type_filter_params: tuple[Any, ...] = ("url",) if content_type_filter_sql else ()
            select_cols = ["`id`", "`content`"]
            for col in (
                "status",
                "content_created_at",
                "content_updated_at",
                "subject",
                "web_title",
                "cate1",
                "cate2",
                "content_author",
                "content_author_raw",
                "content_department",
                "content_department_raw",
            ):
                if not cols or col in cols:
                    select_cols.append(f"`{col}`")
            rows = await mysql_execute_query(
                f"SELECT {', '.join(dict.fromkeys(select_cols))} FROM `{table_name}` "
                f"WHERE {content_type_filter_sql}`content` IN ({placeholders})",
                (*content_type_filter_params, *unique_urls),
                fetch=True,
                dbname=db_name,
            )
            for row in rows or []:
                content = str(_safe_row_get(row, "content") or "").strip()
                if content and content not in existing_by_url:
                    existing_by_url[content] = row
                    try:
                        remember_learn_list_url_row(db_name=db_name, table_name=table_name, row=row)
                    except Exception:
                        pass
            board_save_flow_trace(
                "learn_list_batch_duplicate_lookup",
                "end",
                started_at=db_step_t0,
                db=db_name,
                table=table_name,
                counts={"unique_urls": len(unique_urls), "hits": len(existing_by_url)},
                url=first_url,
            )
            _learn_list_batch_debug_log(
                "direct_duplicate_lookup_done",
                db_name=db_name,
                table=table_name,
                unique_url_count=len(unique_urls),
                existing_hit_count=len(existing_by_url),
                existing_sample=_learn_list_batch_debug_sample(existing_by_url.keys()),
            )

            remaining_urls = [url for url in unique_urls if url not in existing_by_url]
            if remaining_urls and _board_learn_list_batch_normalized_lookup_enabled():
                normalized_lookup_t0 = time.perf_counter()
                normalized_hits = 0
                for url in remaining_urls:
                    try:
                        row = await _find_existing_board_row_by_normalized_url(
                            db_name=db_name,
                            table_name=table_name,
                            cols=cols,
                            candidate_url=url,
                            select_columns=duplicate_select_columns,
                            cache_only=False,
                        )
                    except Exception as lookup_exc:
                        logger.debug(
                            "[LearnList][BatchInsert] normalized duplicate lookup skipped | db=%s table=%s url=%s err=%s",
                            db_name,
                            table_name,
                            url[:180],
                            lookup_exc,
                        )
                        row = None
                    if row:
                        existing_by_url[url] = row
                        normalized_hits += 1
                board_save_flow_trace(
                    "learn_list_batch_normalized_duplicate_lookup",
                    "end",
                    started_at=normalized_lookup_t0,
                    db=db_name,
                    table=table_name,
                    counts={"remaining_urls": len(remaining_urls), "hits": normalized_hits},
                    url=first_url,
                )
                _learn_list_batch_debug_log(
                    "normalized_duplicate_lookup_done",
                    db_name=db_name,
                    table=table_name,
                    remaining_url_count=len(remaining_urls),
                    normalized_hit_count=normalized_hits,
                    total_existing_hit_count=len(existing_by_url),
                    remaining_sample=_learn_list_batch_debug_sample(remaining_urls),
                )
            elif remaining_urls:
                board_save_flow_trace(
                    "learn_list_batch_normalized_duplicate_lookup",
                    "skip",
                    db=db_name,
                    table=table_name,
                    counts={"remaining_urls": len(remaining_urls)},
                    reason="disabled",
                    env="BOARD_LEARN_LIST_BATCH_NORMALIZED_LOOKUP",
                    url=first_url,
                )
                _learn_list_batch_debug_log(
                    "normalized_duplicate_lookup_skipped",
                    db_name=db_name,
                    table=table_name,
                    remaining_url_count=len(remaining_urls),
                    reason="disabled",
                    env="BOARD_LEARN_LIST_BATCH_NORMALIZED_LOOKUP",
                    remaining_sample=_learn_list_batch_debug_sample(remaining_urls),
                )

        to_insert: list[tuple[int, dict, str, Dict[str, Any]]] = []
        now = datetime.now()
        for idx, info, url in valid:
            existing = existing_by_url.get(url)
            if existing:
                row_id = _safe_row_get(existing, "id")
                results[idx] = int(row_id) if row_id is not None else None
                try:
                    info["learn_list_duplicate"] = True
                    info["learn_list_existing_status"] = (
                        str(_safe_row_get(existing, "status") or "").strip().upper() or None
                    )
                    info["learn_list_id"] = results[idx]
                except Exception:
                    pass
                try:
                    if row_id is not None:
                        update_sets: list[str] = []
                        update_params: list[Any] = []
                        incoming_author = _coalesce_author_fields(info)
                        if (
                            incoming_author
                            and "content_author" in cols
                            and _learn_list_author_missing(_safe_row_get(existing, "content_author"))
                        ):
                            update_sets.append("`content_author` = %s")
                            update_params.append(incoming_author)
                        for db_col, meta_key in (
                            ("content_author_raw", "author_raw"),
                            ("content_department", "department"),
                            ("content_department_raw", "department_raw"),
                        ):
                            if db_col not in cols:
                                continue
                            incoming_meta = _extract_author_meta_value(info, meta_key)
                            if incoming_meta and not str(_safe_row_get(existing, db_col) or "").strip():
                                update_sets.append(f"`{db_col}` = %s")
                                update_params.append(incoming_meta)
                        if update_sets:
                            update_params.append(row_id)
                            await mysql_execute_query(
                                f"UPDATE `{table_name}` SET {', '.join(update_sets)} WHERE id = %s",
                                tuple(update_params),
                                dbname=db_name,
                            )
                except Exception as author_update_exc:
                    logger.debug(
                        "[LearnList][BatchInsert] duplicate author backfill skipped | db=%s table=%s id=%s url=%s err=%s",
                        db_name,
                        table_name,
                        row_id,
                        url[:180],
                        author_update_exc,
                    )
                continue
            data = build_board_post_learn_list_input_preview(info, cols=cols, created_at=now)
            if data:
                to_insert.append((idx, info, url, data))
        _learn_list_batch_debug_log(
            "classification_done",
            db_name=db_name,
            table=table_name,
            requested_count=len(post_infos or []),
            valid_count=len(valid),
            unique_url_count=len(unique_urls),
            existing_duplicate_count=len(existing_by_url),
            insert_candidate_count=len(to_insert),
            result_hit_count=sum(1 for value in results if value),
            insert_candidate_sample=_learn_list_batch_debug_sample((url for _, _, url, _ in to_insert)),
        )

        if to_insert:
            existing_cols = set(cols or set())
            all_columns = sorted(
                key
                for key in {key for _, _, _, data in to_insert for key in data.keys()}
                if not existing_cols or key in existing_cols
            )
            if not all_columns:
                return results
            db_step_t0 = time.perf_counter()
            board_save_flow_trace(
                "learn_list_batch_insert_sql",
                "start",
                db=db_name,
                table=table_name,
                counts={"insert_count": len(to_insert), "columns": len(all_columns)},
                url=first_url,
            )
            while True:
                row_placeholder = "(" + ", ".join(["%s"] * len(all_columns)) + ")"
                values_sql = ", ".join([row_placeholder] * len(to_insert))
                params: list[Any] = []
                for _, _, _, data in to_insert:
                    params.extend(data.get(col) for col in all_columns)
                try:
                    await maria_execute_query(
                        f"INSERT INTO `{table_name}` ({', '.join(f'`{col}`' for col in all_columns)}) VALUES {values_sql}",
                        tuple(params),
                        fetch=False,
                        dbname=db_name,
                    )
                    break
                except Exception as insert_exc:
                    missing_col = _extract_unknown_column_from_db_error(insert_exc)
                    if not missing_col or missing_col not in all_columns or len(all_columns) <= 1:
                        raise
                    logger.warning(
                        "[LearnList][BatchInsert] retry without missing column | db=%s table=%s missing_col=%s err=%s",
                        db_name,
                        table_name,
                        missing_col,
                        insert_exc,
                    )
                    try:
                        _invalidate_table_columns_cache(db_name, table_name)
                    except Exception:
                        pass
                    all_columns = [col for col in all_columns if col != missing_col]
                    if not all_columns:
                        return results
            _learn_list_batch_debug_log(
                "insert_sql_done",
                db_name=db_name,
                table=table_name,
                insert_candidate_count=len(to_insert),
                column_count=len(all_columns),
                sample_urls=_learn_list_batch_debug_sample((url for _, _, url, _ in to_insert)),
            )
            board_save_flow_trace(
                "learn_list_batch_insert_sql",
                "end",
                started_at=db_step_t0,
                db=db_name,
                table=table_name,
                counts={"insert_count": len(to_insert), "columns": len(all_columns)},
                url=first_url,
            )

            inserted_urls = [url for _, _, url, _ in to_insert]
            placeholders = ", ".join(["%s"] * len(inserted_urls))
            content_type_filter_sql = "`content_type` = %s AND " if (cols and "content_type" in cols) else ""
            content_type_filter_params: tuple[Any, ...] = ("url",) if content_type_filter_sql else ()
            db_step_t0 = time.perf_counter()
            rows = await mysql_execute_query(
                f"SELECT `id`, `content`, `status` FROM `{table_name}` "
                f"WHERE {content_type_filter_sql}`content` IN ({placeholders})",
                (*content_type_filter_params, *inserted_urls),
                fetch=True,
                dbname=db_name,
            )
            board_save_flow_trace(
                "learn_list_batch_select_inserted",
                "end",
                started_at=db_step_t0,
                db=db_name,
                table=table_name,
                counts={"inserted_urls": len(inserted_urls), "rows": len(rows or [])},
                url=first_url,
            )
            selected_contents = {
                str(_safe_row_get(row, "content") or "").strip()
                for row in rows or []
                if str(_safe_row_get(row, "content") or "").strip()
            }
            missing_after_insert = [url for url in inserted_urls if url not in selected_contents]
            _learn_list_batch_debug_log(
                "select_inserted_done",
                db_name=db_name,
                table=table_name,
                inserted_url_count=len(inserted_urls),
                selected_row_count=len(rows or []),
                missing_after_insert_count=len(missing_after_insert),
                selected_sample=_learn_list_batch_debug_sample(selected_contents),
                missing_sample=_learn_list_batch_debug_sample(missing_after_insert),
            )
            id_by_url: dict[str, Any] = {}
            for row in rows or []:
                content = str(_safe_row_get(row, "content") or "").strip()
                if content:
                    id_by_url[content] = _safe_row_get(row, "id")
                    try:
                        remember_learn_list_url_row(db_name=db_name, table_name=table_name, row=row)
                    except Exception:
                        pass
            for idx, info, url, data in to_insert:
                row_id = id_by_url.get(url)
                if row_id is None:
                    continue
                results[idx] = int(row_id)
                try:
                    info["learn_list_duplicate"] = False
                    info["learn_list_id"] = results[idx]
                except Exception:
                    pass
        board_save_flow_trace(
            "learn_list_batch_insert",
            "end",
            started_at=batch_total_t0,
            db=db_name,
            table=table_name,
            counts={
                "requested": len(post_infos or []),
                "valid": len(valid),
                "duplicates": sum(1 for _, info, _ in valid if bool(info.get("learn_list_duplicate"))),
                "inserted": len(to_insert) if "to_insert" in locals() else 0,
                "ok": sum(1 for value in results if value),
            },
            url=first_url,
        )
        _learn_list_batch_debug_log(
            "end",
            db_name=db_name,
            table=table_name,
            requested_count=len(post_infos or []),
            valid_count=len(valid),
            duplicate_count=sum(1 for _, info, _ in valid if bool(info.get("learn_list_duplicate"))),
            insert_candidate_count=len(to_insert) if "to_insert" in locals() else 0,
            result_hit_count=sum(1 for value in results if value),
            result_none_count=sum(1 for value in results if not value),
            result_sample=_learn_list_batch_debug_sample(results),
        )
        return results
    except Exception as exc:
        board_save_flow_trace(
            "learn_list_batch_insert",
            "fail",
            started_at=batch_total_t0 if "batch_total_t0" in locals() else None,
            level=logging.WARNING,
            db=db_name,
            url=first_url if "first_url" in locals() else "",
            counts={"requested": len(post_infos or [])},
            error=repr(exc),
        )
        logger.warning(
            "[LearnList][BatchInsert] fallback to single inserts | db=%s chat_bot_id=%s count=%s err=%s",
            db_name,
            chat_bot_id,
            len(post_infos or []),
            exc,
            exc_info=True,
        )
        for idx, info in enumerate(post_infos or []):
            try:
                results[idx] = await insert_board_post_into_learn_list(chat_bot_id, db_name, info)
            except Exception:
                results[idx] = None
        return results


async def insert_board_post_into_learn_list(chat_bot_id: str, db_name: str, post_info: dict) -> Optional[int]:
    """
    獄쏄퉮占?# backup_ori02) 疫꿸퀣? 癰귣벀??
    野껊슣?占썸묾?(?怨멸쉭??占쎌뵠筌왖) ?類ｋ궖??LEARN_LIST?????館占??
    - content/content_url ?占쎄쑴占?占?占쏙옙 野껊슣?占썸묾? URL(?占?占쏙옙???????館占?? (???占쏙옙 ??占쎌쨮??URL占?獄쏅떽?占쏙쭪? ??占쎌벉)
    """
    # 疫꿸퀡?? ??占쎌젾占??醫륁뒞??野꺜??占???占쎈뻻 ????占쎄탢 ??占쎈뻬
    if not chat_bot_id or not db_name or not isinstance(post_info, dict):
        db_save_trace_log(
            "board.learn_list.skip invalid_context chat_bot_id=%r db=%r post_info_type=%s",
            chat_bot_id,
            db_name,
            type(post_info).__name__,
            level=logging.WARNING,
        )
        return None
    _strip_hash_keys_from_learn_list_input(post_info)
    try:
        post_info["learn_list_duplicate"] = False
        post_info["learn_list_existing_status"] = None
    except Exception:
        pass

    # 疫꿸퀡?? ?占?占쏙옙 雅뚯눘?占썹몴?揶쎛?占? 餓λ쵎??筌ｋ똾寃뺟몴??袁る립 揶쏅벤???占?占쏙옙????占쎈뻬
    post_url = (post_info.get("post_url") or post_info.get("url") or "").strip()
    _songpa_title_trace(
        "db_insert_enter",
        url=post_url,
        post_url=post_info.get("post_url"),
        raw_url=post_info.get("url"),
        raw_title=post_info.get("title"),
        raw_subject=post_info.get("subject"),
        raw_web_title=post_info.get("web_title"),
    )
    lock_url = canonicalize_url_for_dedup(post_url) or post_url
    if lock_url and not bool(post_info.get("_learn_list_board_url_lock_held")):
        async def _locked_insert_board_post() -> Optional[int]:
            try:
                post_info["_learn_list_board_url_lock_held"] = True
                return await insert_board_post_into_learn_list(chat_bot_id, db_name, post_info)
            finally:
                try:
                    post_info.pop("_learn_list_board_url_lock_held", None)
                except Exception:
                    pass

        return await mysql_user_lock_run(
            db_name,
            _learn_list_content_user_lock_name(lock_url),
            30,
            _locked_insert_board_post,
        )
    if not post_url:
        db_save_trace_log(
            "board.learn_list.skip missing_post_url db=%s chat_bot_id=%s keys=%s",
            db_name,
            chat_bot_id,
            ",".join(sorted(str(k) for k in post_info.keys())) if isinstance(post_info, dict) else "-",
            level=logging.WARNING,
        )
        return None
    db_save_trace_log(
        "board.learn_list.start db=%s chat_bot_id=%s url=%s title=%s content_size=%s",
        db_name,
        chat_bot_id,
        _debug_insert_value(post_url),
        _debug_insert_value(post_info.get("title") or post_info.get("subject")),
        _debug_insert_value(post_info.get("size")),
    )
    save_total_t0 = time.perf_counter()
    board_save_flow_trace(
        "learn_list_insert",
        "start",
        db=db_name,
        url=post_url,
        title=_debug_insert_value(post_info.get("title") or post_info.get("subject"), 120),
        content_size=post_info.get("size"),
    )

    # DB?占?占쏙옙 ?占?占쏙옙 URL?????館占?占? 餓λ쵎???占?占쏙옙 ??占쎈퓠占??占?占쏙옙?遺얜쭆 URL???????占쎈뼄.
    stored_url = post_url
    dedup_url = canonicalize_url_for_dedup(stored_url) or stored_url
    title_step_t0 = time.perf_counter()
    title_candidate = _normalize_board_title_for_persist(post_info.get("title") or post_info.get("subject") or "")
    web_title_candidate = _normalize_board_title_for_persist(post_info.get("web_title") or "")
    title_guard_locked = bool(post_info.get("title_guard_locked"))
    if title_guard_locked:
        title = title_candidate
        web_title = web_title_candidate or title_candidate
    else:
        title = _pick_better_board_title_for_persist(web_title_candidate, title_candidate)
        web_title = _pick_better_board_title_for_persist(title, web_title_candidate)
    health_seoulmc_menu_title = _is_health_seoulmc_url(stored_url) and (
        _is_health_seoulmc_menu_title(title) or _is_health_seoulmc_menu_title(web_title)
    )
    if _is_sungdong_contract_learn_list_url(stored_url) and title_candidate and not _is_weak_board_title_for_persist(title_candidate):
        title = title_candidate
        web_title = title_candidate
    _songpa_title_trace(
        "db_title_decision_initial",
        url=stored_url,
        title_candidate=title_candidate,
        web_title_candidate=web_title_candidate,
        picked_title=title,
        picked_web_title=web_title,
        dedup_url=dedup_url,
    )
    if (
        health_seoulmc_menu_title
        or (
            (not title_guard_locked)
            and (_is_weak_board_title_for_persist(title) or _is_weak_board_title_for_persist(web_title))
        )
    ):
        fallback_title = await _fetch_board_title_for_learn_list_fallback(stored_url)
        if fallback_title:
            title = fallback_title
            web_title = fallback_title
            try:
                post_info["title"] = fallback_title
                post_info["subject"] = fallback_title
                post_info["web_title"] = fallback_title
            except Exception:
                pass
            db_save_trace_log(
                "board.learn_list.title_fallback.applied db=%s url=%s title=%s",
                db_name,
                _debug_insert_value(stored_url),
                _debug_insert_value(fallback_title),
            )
            _songpa_title_trace("db_title_fallback_applied", url=stored_url, fallback_title=fallback_title)
        else:
            db_save_trace_log(
                "board.learn_list.title_fallback.empty db=%s url=%s raw_title=%s raw_web_title=%s",
                db_name,
                _debug_insert_value(stored_url),
                _debug_insert_value(post_info.get("title") or post_info.get("subject")),
                _debug_insert_value(post_info.get("web_title")),
                level=logging.WARNING,
            )
            _songpa_title_trace(
                "db_title_fallback_empty",
                url=stored_url,
                raw_title=post_info.get("title") or post_info.get("subject"),
                raw_web_title=post_info.get("web_title"),
            )
    if _is_weak_board_title_for_persist(title):
        title = ""
    if _is_weak_board_title_for_persist(web_title):
        web_title = ""
    _songpa_title_trace(
        "db_title_decision_final",
        url=stored_url,
        final_title=title,
        final_web_title=web_title,
    )
    title_ms = int((time.perf_counter() - title_step_t0) * 1000)
    try:
        cate1_val = _normalize_cate_code(post_info.get("cate1"))
    except Exception:
        cate1_val = None

    try: 
        cate2_val = _normalize_cate_code(post_info.get("cate2"))
    except Exception:
        cate2_val = None

    try: 
        raw_cate2_val = post_info.get("cate2")
        normalized_cate2_val = _normalize_cate_code(raw_cate2_val)
        cate2_val = normalized_cate2_val if normalized_cate2_val is not None else None
    except Exception: cate2_val = None

    size_val = 0
    try:
        size_val = int(post_info.get("size") or 0)
    except Exception:
        size_val = 0
    created_value = _format_reg_date(post_info.get("reg_date") or post_info.get("content_created_at"))
    updated_value = _format_reg_date(post_info.get("content_updated_at"))

    try:
        # 疫꿸퀡?? ????占쏙옙?占? ???占쏙옙 ???占쏙옙????占쏙옙???類ㅼ젟??占쏙옙???? ?占싼됱쓥 筌뤴뫖占?嚥≪뮆占?
        db_step_t0 = time.perf_counter()
        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        table_name = get_learn_list_table_name(account_identifier)
        db_save_trace_log(
            "board.learn_list.resolve_table_name db=%s table=%s elapsed_ms=%s url=%s",
            db_name,
            table_name,
            int((time.perf_counter() - db_step_t0) * 1000),
            _debug_insert_value(stored_url),
        )
        board_save_flow_trace(
            "learn_list_resolve_table",
            "end",
            started_at=db_step_t0,
            db=db_name,
            table=table_name,
            url=stored_url,
        )
        db_step_t0 = time.perf_counter()
        cols = await ensure_learn_list_standard_columns(db_name, table_name)
        db_save_trace_log(
            "board.learn_list.table_resolved db=%s table=%s account_identifier=%s col_count=%s elapsed_ms=%s url=%s",
            db_name,
            table_name,
            account_identifier,
            len(cols or []),
            int((time.perf_counter() - db_step_t0) * 1000),
            _debug_insert_value(stored_url),
        )
        board_save_flow_trace(
            "learn_list_ensure_columns",
            "end",
            started_at=db_step_t0,
            db=db_name,
            table=table_name,
            col_count=len(cols or []),
            url=stored_url,
        )
        columns_ms = int((time.perf_counter() - db_step_t0) * 1000)
        if not cols:
            db_step_t0 = time.perf_counter()
            if not await _learn_list_table_exists(db_name, table_name):
                logger.warning(
                    "[LearnList] board insert skipped: LEARN_LIST table not found "
                    "(chat_bot_id mismatch or unprovisioned table) | db=%s table=%s chat_bot_id=%s url=%s",
                    db_name,
                    table_name,
                    chat_bot_id,
                    stored_url[:180],
                )
                return None
            db_save_trace_log(
                "board.learn_list.table_exists db=%s table=%s elapsed_ms=%s url=%s",
                db_name,
                table_name,
                int((time.perf_counter() - db_step_t0) * 1000),
                _debug_insert_value(stored_url),
            )

        # 疫꿸퀡?? DB ???觀占?? ?占?占쏙옙???占???占쎈┷, 餓λ쵎???占?占쏙옙?? ?臾믡걹 URL???占?占쏙옙?酉占????占쏙옙???占쎈뼄.
        db_step_t0 = time.perf_counter()
        board_save_flow_trace(
            "learn_list_duplicate_lookup",
            "start",
            db=db_name,
            table=table_name,
            url=stored_url,
            cache_only=False,
        )
        dup_row = await _find_existing_board_row_by_normalized_url(
            db_name=db_name,
            table_name=table_name,
            cols=cols,
            candidate_url=stored_url,
            select_columns=(
                "id",
                "status",
                "content_created_at",
                "content_updated_at",
                "subject",
                "web_title",
                "cate1",
                "cate2",
                "content_author",
                "content_author_raw",
                "content_department",
                "content_department_raw",
            ),
            cache_only=False,
            skip_like=_board_save_duplicate_skip_like_enabled(),
        )
        duplicate_ms = int((time.perf_counter() - db_step_t0) * 1000)
        db_save_trace_log(
            "board.learn_list.duplicate_cache_lookup db=%s table=%s elapsed_ms=%s hit=%s url=%s",
            db_name,
            table_name,
            int((time.perf_counter() - db_step_t0) * 1000),
            bool(dup_row),
            _debug_insert_value(stored_url),
        )
        board_save_flow_trace(
            "learn_list_duplicate_lookup",
            "end",
            started_at=db_step_t0,
            db=db_name,
            table=table_name,
            hit=bool(dup_row),
            row_id=_safe_row_get(dup_row, "id") if dup_row else None,
            existing_status=str(_safe_row_get(dup_row, "status") or "").strip().upper() if dup_row else None,
            url=stored_url,
        )
        _songpa_title_trace(
            "db_duplicate_lookup",
            url=stored_url,
            hit=bool(dup_row),
            existing_id=_safe_row_get(dup_row, "id") if dup_row else "",
            existing_status=str(_safe_row_get(dup_row, "status") or "").strip().upper() if dup_row else "",
            existing_subject=_safe_row_get(dup_row, "subject") if dup_row else "",
            existing_web_title=_safe_row_get(dup_row, "web_title") if dup_row else "",
            incoming_title=title,
            incoming_web_title=web_title,
        )
        allow_board_existing_duplicate_update = str(
            os.getenv("BOARD_CRAWL_ALLOW_EXISTING_DUPLICATE_ROW_UPDATE", "0") or "0"
        ).strip().lower() in ("1", "true", "yes", "on")

        if dup_row and not allow_board_existing_duplicate_update:
            _dup_id = int(_safe_row_get(dup_row, "id"))
            if _is_songpa_board_url(stored_url):
                try:
                    update_sets: list[str] = []
                    update_params: list[Any] = []
                    existing_subject = str(_safe_row_get(dup_row, "subject") or "").strip()
                    existing_web_title = str(_safe_row_get(dup_row, "web_title") or "").strip()
                    body_text = _extract_persisted_body_text(post_info)
                    if title and "subject" in cols and (
                        title_guard_locked
                        or
                        _is_weak_board_title_for_persist(existing_subject)
                        or _board_title_quality_score(title) >= _board_title_quality_score(existing_subject)
                    ) and existing_subject != title:
                        update_sets.append("`subject` = %s")
                        update_params.append(title)
                    if (web_title or title) and "web_title" in cols and (
                        title_guard_locked
                        or
                        _is_weak_board_title_for_persist(existing_web_title)
                        or _board_title_quality_score(web_title or title) >= _board_title_quality_score(existing_web_title)
                    ) and existing_web_title != (web_title or title):
                        update_sets.append("`web_title` = %s")
                        update_params.append(web_title or title)
                    if size_val > 0 and "size" in cols:
                        update_sets.append("`size` = %s")
                        update_params.append(size_val)
                    if body_text:
                        for body_col in _learn_list_body_columns(cols):
                            update_sets.append(f"`{body_col}` = %s")
                            update_params.append(body_text)
                    if update_sets:
                        update_params.append(_dup_id)
                        db_step_t0 = time.perf_counter()
                        await mysql_execute_query(
                            f"UPDATE `{table_name}` SET {', '.join(update_sets)} WHERE id = %s",
                            tuple(update_params),
                            dbname=db_name,
                        )
                        board_save_flow_trace(
                            "learn_list_songpa_duplicate_update",
                            "end",
                            started_at=db_step_t0,
                            db=db_name,
                            table=table_name,
                            row_id=_dup_id,
                            fields=[s.split("=", 1)[0].strip(" `") for s in update_sets],
                            url=stored_url,
                        )
                        _songpa_title_trace(
                            "db_duplicate_songpa_updated_existing",
                            url=stored_url,
                            existing_id=_dup_id,
                            old_subject=existing_subject,
                            old_web_title=existing_web_title,
                            new_subject=title,
                            new_web_title=web_title or title,
                            body_len=len(body_text or ""),
                            size=size_val,
                            fields=",".join([s.split("=", 1)[0].strip(" `") for s in update_sets]),
                        )
                    else:
                        _songpa_title_trace(
                            "db_duplicate_songpa_update_skipped",
                            url=stored_url,
                            existing_id=_dup_id,
                            existing_subject=existing_subject,
                            existing_web_title=existing_web_title,
                            incoming_title=title,
                            incoming_web_title=web_title,
                            body_len=len(body_text or ""),
                            reason="no_update_sets",
                        )
                except Exception as exc:
                    logger.warning(
                        "[SongpaTitleTrace] duplicate existing row update failed | table=%s id=%s url=%s err=%s",
                        table_name,
                        _dup_id,
                        (stored_url or "")[:220],
                        exc,
                    )
                    _songpa_title_trace(
                        "db_duplicate_songpa_update_failed",
                        url=stored_url,
                        existing_id=_dup_id,
                        error=str(exc),
                    )
            try:
                post_info["learn_list_duplicate"] = True
                post_info["learn_list_existing_status"] = str(_safe_row_get(dup_row, "status") or "").strip().upper() or None
                post_info["learn_list_id"] = _dup_id
            except Exception:
                pass
            logger.debug(
                "[LearnList][BoardDuplicate] existing row detected; normal crawl leaves existing row untouched | table=%s id=%s url=%s status=%s",
                table_name,
                _dup_id,
                (stored_url or "")[:180],
                post_info.get("learn_list_existing_status"),
            )
            _songpa_title_trace(
                "db_duplicate_return_existing",
                url=stored_url,
                existing_id=_dup_id,
                existing_subject=_safe_row_get(dup_row, "subject"),
                existing_web_title=_safe_row_get(dup_row, "web_title"),
                incoming_title=title,
                incoming_web_title=web_title,
            )
            return _dup_id

        if dup_row:
            existing_status = str(_safe_row_get(dup_row, "status") or "").strip().upper()
            if existing_status == "N":
                _dup_id = int(_safe_row_get(dup_row, "id"))
                db_step_t0 = time.perf_counter()
                await mysql_execute_query(
                    f"DELETE FROM `{table_name}` WHERE id = %s",
                    (_dup_id,),
                    dbname=db_name,
                )
                board_save_flow_trace(
                    "learn_list_pending_duplicate_delete",
                    "end",
                    started_at=db_step_t0,
                    db=db_name,
                    table=table_name,
                    row_id=_dup_id,
                    url=stored_url,
                )
                try:
                    post_info["learn_list_existing_status"] = "N"
                    post_info["learn_list_replaced_pending"] = True
                    post_info.pop("learn_list_id", None)
                except Exception:
                    pass
                logger.debug(
                    "[LearnList][BoardDuplicate] pending duplicate row removed before fresh insert | id=%s url=%s",
                    _dup_id,
                    (stored_url or "")[:180],
                )
                dup_row = None

        if dup_row:
            existing_date_value = _format_reg_date(
                _safe_row_get(dup_row, "content_updated_at") or _safe_row_get(dup_row, "content_created_at")
            )
            youthcenter_allow_overwrite = False
            try:
                from backend.board.youthcenter_board import is_youthcenter_policy_detail_url

                youthcenter_allow_overwrite = bool(
                    is_youthcenter_policy_detail_url(stored_url)
                    and updated_value
                    and updated_value != existing_date_value
                )
            except Exception:
                youthcenter_allow_overwrite = False

            post_info["learn_list_duplicate"] = not youthcenter_allow_overwrite
            _dup_id = int(_safe_row_get(dup_row, "id"))
            try:
                post_info["learn_list_existing_status"] = str(_safe_row_get(dup_row, "status") or "").strip().upper() or None
                post_info["learn_list_id"] = _dup_id
            except Exception:
                pass
            # 餓λ쵎??URL??占?????占썬걠占?諭?占쏙쭗?? 筌ㅼ뮇???占쎈뗄?占썲첎誘れ몵占?癰귣똻???占쎈뼄.
            is_hscity_photo_duplicate = "photo.hscity.go.kr" in str(stored_url or "").lower()
            try:
                update_sets: list[str] = []
                update_params: list[Any] = []

                if is_hscity_photo_duplicate:
                    db_c1 = str(_safe_row_get(dup_row, "cate1") or "").strip()
                    db_c2 = str(_safe_row_get(dup_row, "cate2") or "").strip()
                    if cate1_val and "cate1" in cols and db_c1 != cate1_val:
                        update_sets.append("`cate1` = %s")
                        update_params.append(cate1_val)
                    if cate2_val and "cate2" in cols and db_c2 != cate2_val:
                        update_sets.append("`cate2` = %s")
                        update_params.append(cate2_val)
                    if update_sets:
                        update_params.append(_dup_id)
                        await mysql_execute_query(
                            f"UPDATE `{table_name}` SET {', '.join(update_sets)} WHERE id = %s",
                            tuple(update_params),
                            dbname=db_name,
                        )
                        logger.debug(
                            "[Cate][HscityPhoto][duplicate-only] LEARN_LIST category updated in insert duplicate path | table=%s id=%s url=%s before=(%r,%r) incoming=(%r,%r)",
                            table_name,
                            _dup_id,
                            (stored_url or "")[:220],
                            db_c1,
                            db_c2,
                            cate1_val,
                            cate2_val,
                        )
                    else:
                        logger.debug(
                            "[Cate][HscityPhoto][duplicate-only] LEARN_LIST duplicate extra update skipped | table=%s id=%s url=%s existing=(%r,%r) incoming=(%r,%r)",
                            table_name,
                            _dup_id,
                            (stored_url or "")[:220],
                            db_c1,
                            db_c2,
                            cate1_val,
                            cate2_val,
                        )
                    update_sets = []
                    update_params = []

                if not is_hscity_photo_duplicate:
                    existing_subject = str(_safe_row_get(dup_row, "subject") or "").strip()
                    existing_web_title = str(_safe_row_get(dup_row, "web_title") or "").strip()
                    if title_guard_locked:
                        next_subject = title
                        next_web_title = web_title or title
                    else:
                        next_subject = _pick_better_board_title_for_persist(existing_subject, title)
                        next_web_title = _pick_better_board_title_for_persist(existing_web_title, web_title or next_subject)
                    logger.debug(
                        "[TitleDecisionTrace] stage=db_duplicate_title_decision url=%s locked=%s existing_subject=%r existing_web_title=%r incoming_title=%r incoming_web_title=%r next_subject=%r next_web_title=%r",
                        (stored_url or "")[:220],
                        title_guard_locked,
                        existing_subject[:220],
                        existing_web_title[:220],
                        title[:220],
                        (web_title or "")[:220],
                        next_subject[:220],
                        next_web_title[:220],
                    )
                    if (
                        title
                        and next_subject
                        and next_subject != existing_subject
                        and "subject" in cols
                    ):
                        update_sets.append("`subject` = %s")
                        update_params.append(next_subject)
                    elif title and existing_subject and next_subject == existing_subject and title != existing_subject:
                        logger.debug(
                            "[LearnList][TitleGuard] duplicate subject overwrite skipped | table=%s id=%s url=%s existing=%r incoming=%r existing_score=%s incoming_score=%s",
                            table_name,
                            _dup_id,
                            (stored_url or "")[:220],
                            existing_subject[:160],
                            title[:160],
                            _board_title_quality_score(existing_subject),
                            _board_title_quality_score(title),
                        )

                    if (
                        (web_title or next_subject)
                        and next_web_title
                        and next_web_title != existing_web_title
                        and "web_title" in cols
                    ):
                        update_sets.append("`web_title` = %s")
                        update_params.append(next_web_title)
                    elif web_title and existing_web_title and next_web_title == existing_web_title and web_title != existing_web_title:
                        logger.debug(
                            "[LearnList][TitleGuard] duplicate web_title overwrite skipped | table=%s id=%s url=%s existing=%r incoming=%r existing_score=%s incoming_score=%s",
                            table_name,
                            _dup_id,
                            (stored_url or "")[:220],
                            existing_web_title[:160],
                            web_title[:160],
                            _board_title_quality_score(existing_web_title),
                            _board_title_quality_score(web_title),
                        )

                if (not is_hscity_photo_duplicate) and cate1_val and "cate1" in cols:
                    update_sets.append("`cate1` = %s")
                    update_params.append(cate1_val)

                if (not is_hscity_photo_duplicate) and cate2_val and "cate2" in cols:
                    update_sets.append("`cate2` = %s")
                    update_params.append(cate2_val)

                incoming_author = _coalesce_author_fields(post_info)
                if (
                    (not is_hscity_photo_duplicate)
                    and incoming_author
                    and "content_author" in cols
                    and _learn_list_author_missing(_safe_row_get(dup_row, "content_author"))
                ):
                    update_sets.append("`content_author` = %s")
                    update_params.append(incoming_author)

                for db_col, meta_key in (
                    ("content_author_raw", "author_raw"),
                    ("content_department", "department"),
                    ("content_department_raw", "department_raw"),
                ):
                    if is_hscity_photo_duplicate or db_col not in cols:
                        continue
                    incoming_meta = _extract_author_meta_value(post_info, meta_key)
                    if incoming_meta and not str(_safe_row_get(dup_row, db_col) or "").strip():
                        update_sets.append(f"`{db_col}` = %s")
                        update_params.append(incoming_meta)

                if youthcenter_allow_overwrite:
                    pass

                if update_sets:
                    update_params.append(_dup_id)
                    db_step_t0 = time.perf_counter()
                    await mysql_execute_query(
                        f"UPDATE `{table_name}` SET {', '.join(update_sets)} WHERE id = %s",
                        tuple(update_params),
                        dbname=db_name,
                    )
                    board_save_flow_trace(
                        "learn_list_duplicate_update",
                        "end",
                        started_at=db_step_t0,
                        db=db_name,
                        table=table_name,
                        row_id=_dup_id,
                        fields=[s.split("=", 1)[0].strip(" `") for s in update_sets],
                        url=stored_url,
                    )
            except Exception:
                pass
            # ??占쎌뒠(???? ??占쎈뮉??筌롫뗀?占썲첎? ??占쏙옙占???占쎌몵占??臾믨쉐??占쎈０???占쎌뵠 ?占?占쏙옙 筌롫뗀?占썸에?癰귣떯占?
            if not is_hscity_photo_duplicate:
                try:
                    memo_fill = (post_info.get("memo1") or post_info.get("memo") or "").strip()
                    if memo_fill:
                        for _mcol in ("memo1", "memo"):
                            if _mcol not in cols:
                                continue
                            mrows = await mysql_execute_query(
                                f"SELECT `{_mcol}` AS _mv FROM `{table_name}` WHERE id = %s LIMIT 1",
                                (_dup_id,),
                                fetch=True,
                                dbname=db_name,
                            )
                            prev = ""
                            if mrows:
                                prev = _safe_row_get(mrows[0], "_mv")
                            prev = (str(prev) if prev is not None else "").strip()
                            if not prev:
                                await mysql_execute_query(
                                    f"UPDATE `{table_name}` SET `{_mcol}` = %s WHERE id = %s",
                                    (memo_fill, _dup_id),
                                    dbname=db_name,
                                )
                            break
                except Exception:
                    pass
                try:
                    await ensure_learn_list_type_not_blank(
                        db_name=db_name,
                        learn_list_table=table_name,
                        default_type=str(post_info.get("type") or "post").strip() or "post",
                        row_id=_dup_id,
                    )
                except Exception:
                    pass
            if youthcenter_allow_overwrite:
                try:
                    post_info["learn_list_overwrite"] = True
                except Exception:
                    pass
                logger.debug(
                    "[LearnList][Youthcenter] duplicate row reused for overwrite | id=%s url=%s existing_date=%s incoming_date=%s",
                    _dup_id,
                    (stored_url or "")[:180],
                    existing_date_value,
                    updated_value,
                )
            board_save_flow_trace(
                "learn_list_insert",
                "end",
                started_at=save_total_t0,
                db=db_name,
                table=table_name,
                url=stored_url,
                result="duplicate",
                row_id=_dup_id,
                existing_status=post_info.get("learn_list_existing_status"),
                overwrite=bool(post_info.get("learn_list_overwrite")),
            )
            return _dup_id

        # 疫꿸퀡?? DB????占쎌뿯??筌ㅼ뮇占??怨쀬뵠???類ㅿ옙??占썩봺 鈺곌퀡??
        payload_step_t0 = time.perf_counter()
        data: Dict[str, Any] = {
            "content": stored_url,
            "subject": title or stored_url,
            "content_type": "url",
            "status": "N",
            "size": size_val,
            "created_at": datetime.now(),
        }

        if cols:
            if "cate1" in cols:
                data["cate1"] = cate1_val or ""
            if "cate2" in cols:
                data["cate2"] = cate2_val or ""
            if "web_title" in cols:
                data["web_title"] = web_title or title or ""
            body_text = _extract_persisted_body_text(post_info)
            if body_text:
                for body_col in _learn_list_body_columns(cols):
                    data[body_col] = body_text
            # 疫꿸퀡?? ?怨멸쉭 ??占쎌뵠筌왖 ?源낆쨯??占쎈궢 URL ?占승???占싼됱쓥 ?怨쀬뵠???醫딅뼣
            if "content_created_at" in cols and created_value: data["content_created_at"] = created_value
            if "content_updated_at" in cols and updated_value: data["content_updated_at"] = updated_value

            # ?源낆쨯???臾믨쉐??疫꼲??占쎌뵠 ?占쎈뗄?占썲첎???LEARN_LIST.content_author (癰귢쑴占??占싼됱쓥占??紐낆넎)
            _auth_db = _coalesce_author_fields(post_info)
            if _auth_db and "content_author" in cols:
                data["content_author"] = _auth_db

            # 疫꿸퀡?? ???占쏙옙?占쎈뗄占???占쎌젫 鈺곕똻???占쎈뮉 ?占싼됱쓥占???占쎈┛?袁⑥쨯 ?袁り숲占?
            data = _filter_learn_list_visible_write_data(data, cols)
            _ensure_learn_list_hash_columns_null(data, cols)
        else:
            data = _filter_learn_list_visible_write_data(data)
        payload_ms = int((time.perf_counter() - payload_step_t0) * 1000)
        _songpa_title_trace(
            "db_insert_payload",
            url=stored_url,
            content=data.get("content"),
            subject=data.get("subject"),
            web_title=data.get("web_title"),
            body_len=len(_extract_persisted_body_text(post_info) or ""),
            content_type=data.get("content_type"),
            status=data.get("status"),
            keys=",".join(sorted(str(k) for k in data.keys())),
        )

        board_save_flow_trace(
            "learn_list_payload_build",
            "end",
            db=db_name,
            table=table_name,
            url=stored_url,
            keys=sorted(str(k) for k in data.keys()),
            content_size=size_val,
        )

        try:
            if "dongjak.go.kr/yeyak/progrm/master/online/" in str(stored_url or ""):
                logger.warning(
                    "[DongjakTypeDebug][board_insert_payload] table=%s has_type_col=%s payload_type=%s payload_content_type=%s url=%s",
                    table_name,
                    bool(cols and "type" in cols),
                    data.get("type"),
                    data.get("content_type"),
                    str(stored_url or "")[:220],
                )
        except Exception:
            pass

        if (
            dedup_url
            and "yongin.go.kr" in dedup_url.lower()
            and "/user/bbs/bd_selectbbs.do" in dedup_url.lower()
            and "/citizen/user/" not in dedup_url.lower()
        ):
            logger.debug(
                "[YonginTitleDebug][db_payload] db=%s table=%s url=%s subject=%r web_title=%r",
                db_name,
                table_name,
                stored_url[:220],
                data.get("subject"),
                data.get("web_title"),
            )
        try:
            learn_list_insert_debug_log(
                "board_insert call db=%s table=%s keys=%s content=%s subject=%s type=%s content_type=%s",
                db_name,
                table_name,
                ",".join(sorted(str(k) for k in data.keys())),
                _debug_insert_value(data.get("content")),
                _debug_insert_value(data.get("subject")),
                _debug_insert_value(data.get("type")),
                _debug_insert_value(data.get("content_type")),
            )
            db_step_t0 = time.perf_counter()
            board_save_flow_trace(
                "learn_list_insert_sql",
                "start",
                db=db_name,
                table=table_name,
                url=stored_url,
                keys=sorted(str(k) for k in data.keys()),
            )
            inserted_id = await _safe_maria_insert_data(table_name, data, db_name=db_name, warning_context={"post_url": post_url if "post_url" in locals() else "", "job_id": post_info.get("job_id") if isinstance(post_info, dict) else ""})
            _songpa_title_trace(
                "db_insert_result",
                url=stored_url,
                inserted_id=inserted_id,
                subject=data.get("subject"),
                web_title=data.get("web_title"),
                content=data.get("content"),
            )
            sql_ms = int((time.perf_counter() - db_step_t0) * 1000)
            
            board_save_flow_trace(
                "learn_list_insert_sql",
                "end",
                started_at=db_step_t0,
                db=db_name,
                table=table_name,
                row_id=inserted_id,
                url=stored_url,
            )
            try:
                remember_learn_list_url_row(
                    db_name=db_name,
                    table_name=table_name,
                    row={"id": inserted_id, "content": data.get("content"), **data},
                )
            except Exception:
                pass
            if (
                dedup_url
                and "yongin.go.kr" in dedup_url.lower()
                and "/user/bbs/bd_selectbbs.do" in dedup_url.lower()
                and "/citizen/user/" not in dedup_url.lower()
            ):
                logger.debug(
                    "[YonginTitleDebug][db_insert_result] db=%s table=%s id=%s url=%s subject=%r web_title=%r keys=%s",
                    db_name,
                    table_name,
                    inserted_id,
                    stored_url[:220],
                    data.get("subject"),
                    data.get("web_title"),
                    ",".join(sorted(str(k) for k in data.keys())),
                )
            db_save_trace_log(
                "board.learn_list.insert_result db=%s table=%s inserted_id=%s url=%s",
                db_name,
                table_name,
                inserted_id,
                _debug_insert_value(stored_url),
            )
        except Exception as insert_exc:
            board_save_flow_trace(
                "learn_list_insert_sql",
                "fail",
                started_at=db_step_t0 if "db_step_t0" in locals() else None,
                level=logging.WARNING,
                db=db_name,
                table=table_name,
                url=stored_url,
                error=repr(insert_exc),
            )
            learn_list_insert_debug_log(
                "board_insert exception db=%s table=%s err=%s content=%s",
                db_name,
                table_name,
                _debug_insert_value(insert_exc, 600),
                _debug_insert_value(data.get("content")),
                level=logging.WARNING,
            )
            if "duplicate" in str(insert_exc).lower() or "1062" in str(insert_exc).lower():
                raise
            else:
                raise
        # type 蹂댁젙 ?占쎌닔???占쎌옱 no-op?占쏙옙?占??占???占쏀뙣?占쎌뿉?占쎈뒗 ?占쎌텧?占쏙옙? ?占쎈뒗??
        post_info["learn_list_duplicate"] = False
        post_info["learn_list_id"] = inserted_id
        learn_list_insert_debug_log(
            "board_insert success db=%s table=%s new_id=%s content=%s",
            db_name,
            table_name,
            inserted_id,
            _debug_insert_value(data.get("content")),
        )
        board_save_flow_trace(
            "learn_list_insert",
            "end",
            started_at=save_total_t0,
            db=db_name,
            table=table_name,
            url=stored_url,
            result="inserted",
            row_id=inserted_id,
            title_ms=title_ms if "title_ms" in locals() else None,
            columns_ms=columns_ms if "columns_ms" in locals() else None,
            duplicate_ms=duplicate_ms if "duplicate_ms" in locals() else None,
            payload_ms=payload_ms if "payload_ms" in locals() else None,
            sql_ms=sql_ms if "sql_ms" in locals() else None,
            pre_sql_ms=(int((time.perf_counter() - save_total_t0) * 1000) - (sql_ms if "sql_ms" in locals() else 0)),
        )
        return int(inserted_id) if inserted_id else None
    except Exception as exc:
        board_save_flow_trace(
            "learn_list_insert",
            "fail",
            started_at=save_total_t0 if "save_total_t0" in locals() else None,
            level=logging.ERROR,
            db=db_name,
            url=post_url if "post_url" in locals() else "",
            error=repr(exc),
        )
        db_save_trace_log(
            "board.learn_list.error db=%s url=%s err=%s keys=%s",
            db_name,
            _debug_insert_value(post_url if "post_url" in locals() else ""),
            _debug_insert_value(exc, 800),
            ",".join(sorted(str(k) for k in post_info.keys())) if isinstance(post_info, dict) else "-",
            level=logging.ERROR,
            exc_info=True,
        )
        learn_list_insert_debug_log(
            "board_insert outer_exception db=%s url=%s err=%s post_info_keys=%s",
            db_name,
            _debug_insert_value(post_url if "post_url" in locals() else ""),
            _debug_insert_value(exc, 800),
            ",".join(sorted(str(k) for k in post_info.keys())) if isinstance(post_info, dict) else "-",
            level=logging.ERROR,
        )
        return None


async def update_learn_list_status_immediate(
    db_name: str,
    chat_bot_id: str,
    db_id: str,
    chunks: int,
    *,
    cate2: Any = _CATE_ARG_UNSET,
) -> bool:
    """獄쏄퉮占?疫꿸퀣?: ??占쎈뮸 燁삳똻???筌앹빓? ??占쎌젎??筌앸맩??status='Y' 獄쏆꼷?? cate2 沃섎챷?????cate ?占싼됱쓥?? ?占?."""
    allow = str(os.getenv("ALLOW_LEARN_LIST_STATUS_IMMEDIATE_Y") or "").strip().lower()
    if allow not in ("1", "true", "yes", "on"):
        return False
    return await update_learn_list_status_board(
        db_name, chat_bot_id, db_id, chunks, cate1=_CATE_ARG_UNSET, cate2=cate2
    )


async def is_board_post_duplicate(
    *,
    chat_bot_id: str,
    db_name: str,
    post_url: str,
) -> Optional[int]:
    """
    獄쏄퉮占?疫꿸퀣? 癰귣벀?? 野껊슣?占썸묾? URL??LEARN_LIST??鈺곕똻???占쎈뮉筌왖 ?類ㅼ뵥.
    - 揶쎛?館占?野껋럩??content 疫꿸퀣???占쎌쨮 ??占쏙옙?占썲칰?鈺곌퀬???占쏙옙?
      ??占쎄펾/??占쎄텕占?筌△뫁????content占???占쏙옙??占쎈뼄.
    """
    if not chat_bot_id or not db_name or not post_url:
        return None
    post_url = str(post_url).strip()
    if not post_url:
        return None
    lookup_t0 = time.perf_counter()
    crawl_trace(
        logger,
        phase="selection",
        action="board_post_duplicate_row",
        state="start",
        level=logging.DEBUG,
        db=db_name,
        url=post_url,
    )
    try:
        row = await get_board_post_duplicate_row(
            chat_bot_id=chat_bot_id,
            db_name=db_name,
            post_url=post_url,
        )
        if not row:
            return None
        rid = row.get("id")
        return int(rid) if rid is not None else None
    except Exception:
        return None


async def get_board_post_duplicate_row(
    *,
    chat_bot_id: str,
    db_name: str,
    post_url: str,
    job_id: str = "",
) -> Optional[Dict[str, Any]]:
    """野껊슣?占썸묾? URL 疫꿸퀣? 疫꿸퀣??LEARN_LIST row(id/status)??鈺곌퀬???占쎈뼄."""
    if not chat_bot_id or not db_name or not post_url:
        return None
    post_url = str(post_url).strip()
    if not post_url:
        return None
    lookup_t0 = time.perf_counter()
    crawl_trace(
        logger,
        phase="selection",
        action="board_post_duplicate_row",
        state="start",
        level=logging.DEBUG,
        db=db_name,
        url=post_url,
    )
    try:
        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        table_name = get_learn_list_table_name(account_identifier)
        cols = await ensure_learn_list_standard_columns(db_name, table_name)
        if not cols:
            cols = await _get_table_columns(db_name, table_name)

        select_columns = [
            "id",
            "status",
            "content_created_at",
            "content_updated_at",
            "subject",
            "content",
            "content_type",
            "memo1",
        ]
        for optional_col in (
            "web_title",
            "content_author",
        ):
            if optional_col in cols:
                select_columns.append(optional_col)

        row = await _find_existing_board_row_by_normalized_url(
            db_name=db_name,
            table_name=table_name,
            cols=cols,
            candidate_url=post_url,
            select_columns=tuple(select_columns),
            cache_only=False,
            job_id=job_id,
        )
        if not row:
            crawl_trace(
                logger,
                phase="selection",
                action="board_post_duplicate_row",
                state="end",
                level=logging.DEBUG,
                elapsed_ms=(time.perf_counter() - lookup_t0) * 1000.0,
                db=db_name,
                hit=False,
                table=table_name,
                url=post_url,
            )
            return None

        rid = _safe_row_get(row, "id")
        if rid is None:
            crawl_trace(
                logger,
                phase="selection",
                action="board_post_duplicate_row",
                state="end",
                level=logging.DEBUG,
                elapsed_ms=(time.perf_counter() - lookup_t0) * 1000.0,
                db=db_name,
                hit=False,
                reason="missing_id",
                table=table_name,
                url=post_url,
            )
            return None
        status = str(_safe_row_get(row, "status") or "").strip().upper()
        crawl_trace(
            logger,
            phase="selection",
            action="board_post_duplicate_row",
            state="end",
            level=logging.DEBUG,
            elapsed_ms=(time.perf_counter() - lookup_t0) * 1000.0,
            db=db_name,
            hit=True,
            table=table_name,
            row_id=rid,
            status=status or None,
            url=post_url,
        )
        return {
            "id": int(rid),
            "status": status or None,
            "content_created_at": _safe_row_get(row, "content_created_at"),
            "content_updated_at": _safe_row_get(row, "content_updated_at"),
            "subject": _safe_row_get(row, "subject"),
            "web_title": _safe_row_get(row, "web_title") if "web_title" in cols else None,
            "content": _safe_row_get(row, "content"),
            "content_type": _safe_row_get(row, "content_type"),
            "memo1": _safe_row_get(row, "memo1"),
            "content_author": _safe_row_get(row, "content_author") if "content_author" in cols else None,
        }
    except Exception as exc:
        crawl_trace(
            logger,
            phase="selection",
            action="board_post_duplicate_row",
            state="fail",
            level=logging.DEBUG,
            elapsed_ms=(time.perf_counter() - lookup_t0) * 1000.0,
            db=db_name,
            error=exc,
            url=post_url,
        )
        return None


async def delete_board_post_pending_duplicate_row(
    *,
    chat_bot_id: str,
    db_name: str,
    post_url: str,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "deleted": False,
        "row_id": None,
        "status": None,
    }
    if not chat_bot_id or not db_name or not post_url:
        return result
    try:
        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        table_name = get_learn_list_table_name(account_identifier)
        cols = await ensure_learn_list_standard_columns(db_name, table_name)
        if not cols:
            cols = await _get_table_columns(db_name, table_name)
        row = await _find_existing_board_row_by_normalized_url(
            db_name=db_name,
            table_name=table_name,
            cols=cols,
            candidate_url=post_url,
            select_columns=("id", "status"),
            cache_only=False,
        )
        if not row:
            return result
        row_id = _safe_row_get(row, "id")
        status = str(_safe_row_get(row, "status") or "").strip().upper() or None
        result["row_id"] = int(row_id) if row_id is not None else None
        result["status"] = status
        if result["row_id"] is None or status != "N":
            return result
        await mysql_execute_query(
            f"DELETE FROM `{table_name}` WHERE id = %s",
            (result["row_id"],),
            dbname=db_name,
        )
        result["deleted"] = True
        logger.debug(
            "[LearnList][BoardDuplicate] pending duplicate row deleted | id=%s url=%s",
            result["row_id"],
            str(post_url or "")[:180],
        )
        return result
    except Exception:
        return result


async def update_learn_list_cates_post_job(
    *,
    chat_bot_id: str,
    db_name: str,
    items: list[Dict[str, str]],
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "total": len(items or []),
        "updated": 0,
        "skipped_no_row": 0,
        "skipped_no_title": 0,
        "skipped_no_cols": 0,
        "skipped_existing_cate2": 0,
        "skipped_disabled": 0,
        "errors": 0,
    }
    try:
        enabled = str(os.getenv("BOARD_CONTENT_ENABLE_POST_JOB_CATE_UPDATE", "1") or "1").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
    except Exception:
        enabled = True
    if not enabled:
        stats["skipped_disabled"] = stats["total"]
        return stats
    if not (chat_bot_id and db_name):
        return stats
    if not items:
        return stats
    sub_cate_mode = await get_sub_cate_mode_from_config(chat_bot_id, dbname=db_name)

    def _normalize_text_for_match(value: Any) -> str:
        try:
            text = str(value or "").strip().lower()
        except Exception:
            text = ""
        if not text:
            return ""
        try:
            text = re.sub(r"[\s\W_]+", "", text, flags=re.UNICODE)
        except Exception:
            pass
        return text

    def _simplify_title_candidate(raw: Any) -> str:
        try:
            text = str(raw or "").strip()
        except Exception:
            text = ""
        if not text:
            return ""
        try:
            text = text.split("<", 1)[0].strip()
            text = re.sub(r"^\[[^\]]+\]\s*", "", text).strip()
            text = re.sub(r"\s*\([^)]*\)\s*", " ", text).strip()
            text = " ".join(text.split()).strip()
        except Exception:
            pass
        return text

    async def _load_category_candidates() -> list[dict[str, str]]:
        uuid_parts = str(chat_bot_id).split("-")
        last_element = uuid_parts[-1] if uuid_parts else None
        if not last_element:
            return []
        table = f"ASADAL_{last_element}_CATEGORY"
        try:
            rows = await _fetch_category_rows_cached(category_table=table, db_name=db_name)
        except Exception as exc:
            logger.warning("[LearnList][cate-post-job] category lookup failed | table=%s err=%s", table, exc)
            return []

        candidates: list[dict[str, str]] = []
        for row in rows or []:
            try:
                code = str(row.get("cate_code") or "").strip()
                tree = str(row.get("cate_treecode") or "").strip()
                name = str(row.get("cate_name") or "").strip()
            except Exception:
                continue
            if not code or not name:
                continue
            norm = _normalize_text_for_match(name)
            if not norm:
                continue
            candidates.append({"code": code, "tree": tree, "name": name, "norm": norm})
        candidates.sort(key=lambda item: len(item.get("norm", "")), reverse=True)
        logger.debug(
            "[LearnList][cate-post-job][category-load] table=%s rows=%s db=%s",
            table,
            len(candidates),
            db_name,
        )
        return candidates

    def _is_category_root_candidate(candidate: Optional[dict[str, str]]) -> bool:
        if not candidate:
            return False
        code = str(candidate.get("code") or "").strip()
        name = str(candidate.get("name") or "").strip()
        if code and code in {
            "AS0000000000",
            "AS0000000001",
            "homepage_learning",
            "file_learning",
        }:
            return True
        compact_name = name.lower().replace(" ", "")
        return (
            compact_name in {
                "homepage_learning",
                "homepagelearning",
                "?占쏀럹?占쏙옙??占쎌뒿",
                "?占쎌씪?占쎌뒿",
                "??占쎈읂?????占쎈뮸",
                "???占쏙옙??占쎈뮸",
            }
            or ("?占쏀럹?占쏙옙?" in name and "?占쎌뒿" in name)
            or ("?占쎌씪" in name and "?占쎌뒿" in name)
            or ("??占쎈읂???" in name and "??占쎈뮸" in name)
        )

    def _resolve_post_job_cate_pair(
        candidates: list[dict[str, str]],
        matched_code: str,
    ) -> tuple[str, str]:
        matched = next((row for row in candidates if row.get("code") == matched_code), None)
        if not matched:
            return "", matched_code
        matched_tree = str(matched.get("tree") or "").strip()
        if not matched_tree:
            return "", matched_code

        parents = [
            row
            for row in candidates
            if row is not matched
            and str(row.get("tree") or "").strip()
            and matched_tree.startswith(str(row.get("tree") or "").strip())
            and len(str(row.get("tree") or "").strip()) < len(matched_tree)
            and not _is_category_root_candidate(row)
        ]
        if parents:
            parents.sort(key=lambda row: len(str(row.get("tree") or "").strip()), reverse=True)
            parent_code = str(parents[0].get("code") or "").strip()
            if parent_code and parent_code != matched_code:
                return parent_code, matched_code
        children = [
            row
            for row in candidates
            if row is not matched
            and str(row.get("tree") or "").strip()
            and str(row.get("tree") or "").strip().startswith(matched_tree)
            and len(str(row.get("tree") or "").strip()) > len(matched_tree)
        ]
        if children:
            return matched_code, ""
        return "", matched_code

    def _descendant_category_candidates(
        candidates: list[dict[str, str]],
        parent_code: str,
    ) -> list[dict[str, str]]:
        parent = next((row for row in candidates if row.get("code") == parent_code), None)
        parent_tree = str((parent or {}).get("tree") or "").strip()
        if not parent_tree:
            return []
        return [
            row
            for row in candidates
            if row is not parent
            and str(row.get("tree") or "").strip()
            and str(row.get("tree") or "").strip().startswith(parent_tree)
            and len(str(row.get("tree") or "").strip()) > len(parent_tree)
        ]

    def _match_candidate_by_name(candidates: list[dict[str, str]], name: Any) -> Optional[dict[str, str]]:
        target_norm = _normalize_text_for_match(name)
        if not target_norm:
            return None
        for candidate in candidates:
            if str(candidate.get("norm") or "") == target_norm:
                return candidate
        return None

    def _resolve_hscity_photo_hint_pair(
        candidates: list[dict[str, str]],
        hint: Any,
    ) -> tuple[str, str]:
        text = str(hint or "").strip()
        if ">" not in text:
            logger.debug(
                "[LearnList][cate-post-job][hscity-resolve] skip no delimiter | hint=%r",
                text,
            )
            return "", ""
        path_parts = [part.strip() for part in text.split(">") if part and part.strip()]
        if len(path_parts) < 2:
            logger.debug(
                "[LearnList][cate-post-job][hscity-resolve] skip insufficient path | hint=%r parts=%s",
                text,
                path_parts,
            )
            return "", ""

        by_name: dict[str, list[dict[str, str]]] = {}
        for candidate in candidates:
            name = str(candidate.get("name") or "").strip()
            if name in path_parts:
                by_name.setdefault(name, []).append(candidate)

        matches: list[tuple[int, int, int, dict[str, str], dict[str, str]]] = []
        for left_idx, left_name in enumerate(path_parts):
            for right_idx in range(left_idx + 1, len(path_parts)):
                right_name = path_parts[right_idx]
                for left in by_name.get(left_name, []):
                    left_tree = str(left.get("tree") or "").strip()
                    if not left_tree:
                        continue
                    for right in by_name.get(right_name, []):
                        right_tree = str(right.get("tree") or "").strip()
                        if not right_tree.startswith(left_tree) or len(right_tree) <= len(left_tree):
                            continue
                        distance = right_idx - left_idx
                        adjacent_rank = 0 if distance == 1 else 1
                        matches.append((adjacent_rank, -left_idx, distance, left, right))

        if not matches:
            sample = [row.get("name") for row in candidates[:20]]
            logger.debug(
                "[LearnList][cate-post-job][hscity-resolve] no ordered pair | hint=%r parts=%s found_names=%s sample=%s",
                text,
                path_parts,
                list(by_name.keys()),
                sample,
            )
            return "", ""

        matches.sort(key=lambda item: (item[0], item[1], item[2]))
        _, _, _, left, right = matches[0]
        left_name = str(left.get("name") or "").strip()
        right_name = str(right.get("name") or "").strip()
        left_code = str(left.get("code") or "").strip()
        right_code = str(right.get("code") or "").strip()
        logger.debug(
            "[LearnList][cate-post-job][hscity-resolve] resolved | hint=%r parts=%s left=%r left_code=%s right=%r right_code=%s",
            text,
            path_parts,
            left_name,
            left_code,
            right_name,
            right_code,
        )
        return left_code, right_code

    def _match_category_code(
        candidates: list[dict[str, str]],
        *texts: Any,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        normalized_candidates = []
        for raw in texts:
            txt = _simplify_title_candidate(raw)
            norm = _normalize_text_for_match(txt)
            if not norm:
                continue
            normalized_candidates.append((txt, norm))
        if not normalized_candidates or not candidates:
            return None, None, None

        for original_text, norm_text in normalized_candidates:
            for candidate in candidates:
                cand_norm = candidate["norm"]
                if norm_text == cand_norm:
                    return candidate["code"], candidate["name"], original_text
                if cand_norm and cand_norm in norm_text:
                    return candidate["code"], candidate["name"], original_text
                if norm_text and norm_text in cand_norm and len(norm_text) >= 2:
                    return candidate["code"], candidate["name"], original_text

        try:
            from difflib import SequenceMatcher
        except Exception:
            SequenceMatcher = None  # type: ignore[assignment]

        best_ratio = 0.0
        best_candidate: Optional[dict[str, str]] = None
        best_text: Optional[str] = None
        if SequenceMatcher:
            for original_text, norm_text in normalized_candidates:
                for candidate in candidates:
                    try:
                        ratio = SequenceMatcher(None, norm_text, candidate["norm"]).ratio()
                    except Exception:
                        ratio = 0.0
                    if ratio > best_ratio:
                        best_ratio = ratio
                        best_candidate = candidate
                        best_text = original_text
        if best_candidate and best_text and best_ratio >= 0.60:
            return best_candidate["code"], best_candidate["name"], best_text
        return None, None, None

    try:
        account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
        table_name = get_learn_list_table_name(account_identifier)
    except Exception:
        return stats

    cols = await ensure_learn_list_standard_columns(db_name, table_name)
    if not cols:
        cols = await _get_table_columns(db_name, table_name)
    if not cols or ("cate1" not in cols and "cate2" not in cols):
        stats["skipped_no_cols"] = stats["total"]
        return stats

    category_candidates = await _load_category_candidates()

    for item in items:
        try:
            raw_url = str(item.get("raw_url") or "").strip()
        except Exception:
            raw_url = ""
        try:
            url = str(item.get("url") or "").strip()
        except Exception:
            url = ""
        lookup_url = raw_url or url
        if not lookup_url:
            stats["skipped_no_row"] += 1
            continue
        is_hscity_photo_item = "photo.hscity.go.kr" in str(lookup_url or "").lower()
        if is_hscity_photo_item:
            logger.debug(
                "[LearnList][cate-post-job][hscity-item] start | url=%s hint=%r title=%r web_title=%r",
                lookup_url[:220],
                item.get("category_hint"),
                item.get("title"),
                item.get("web_title"),
            )

        cached_learn_row = None
        try:
            cached_learn_row = await find_learn_list_row_in_url_cache(
                db_name=str(db_name),
                table_name=str(table_name),
                columns=("id", "content", "cate1", "cate2"),
                candidate_url=lookup_url,
                available_cols=cols,
            )
        except Exception as cache_exc:
            cached_learn_row = None
            logger.debug(
                "[LearnList][cate-post-job] memory lookup skipped | table=%s url=%s err=%s",
                table_name,
                lookup_url[:220],
                cache_exc,
            )

        if cached_learn_row:
            try:
                existing_id = int(_safe_row_get(cached_learn_row, "id") or 0) or None
            except Exception:
                existing_id = None
        else:
            try:
                existing_id = await is_board_post_duplicate(
                    chat_bot_id=str(chat_bot_id),
                    db_name=str(db_name),
                    post_url=lookup_url,
                )
            except Exception:
                existing_id = None
        if not existing_id:
            if is_hscity_photo_item:
                logger.debug(
                    "[LearnList][cate-post-job][hscity-item] skip no learn_list row | url=%s hint=%r",
                    lookup_url[:220],
                    item.get("category_hint"),
                )
            stats["skipped_no_row"] += 1
            continue

        current_cate1 = ""
        current_cate2 = ""
        if cached_learn_row:
            current_cate1 = str(_safe_row_get(cached_learn_row, "cate1") or "").strip()
            current_cate2 = str(_safe_row_get(cached_learn_row, "cate2") or "").strip()
        elif "cate1" in cols or "cate2" in cols:
            try:
                select_cols = [col for col in ("cate1", "cate2") if col in cols]
                rows = await mysql_execute_query(
                    f"SELECT {', '.join(select_cols)} FROM `{table_name}` WHERE id = %s LIMIT 1",
                    (existing_id,),
                    fetch=True,
                    dbname=db_name,
                )
                if rows:
                    current_cate1 = str(_safe_row_get(rows[0], "cate1") or "").strip()
                    current_cate2 = str(_safe_row_get(rows[0], "cate2") or "").strip()
            except Exception:
                current_cate1 = ""
                current_cate2 = ""

        if is_hscity_photo_item:
            matched_cate1, matched_cate2 = _resolve_hscity_photo_hint_pair(
                category_candidates,
                item.get("category_hint"),
            )
            matched_name = str(item.get("category_hint") or "").strip()
            matched_source = matched_name
            if not (matched_cate1 or matched_cate2):
                logger.debug(
                    "[LearnList][cate-post-job][hscity-item] skip no matched code | id=%s url=%s hint=%r current=(%s,%s)",
                    existing_id,
                    lookup_url[:220],
                    item.get("category_hint"),
                    current_cate1,
                    current_cate2,
                )
                stats["skipped_no_title"] += 1
                continue
        else:
            matched_code, matched_name, matched_source = _match_category_code(
                category_candidates,
                item.get("category_hint"),
                item.get("web_title"),
                item.get("title"),
            )
            if not matched_code:
                stats["skipped_no_title"] += 1
                continue
            matched_cate1, matched_cate2 = "", matched_code
        has_explicit_hint = bool(str(item.get("category_hint") or "").strip())
        overwrite_sub_cate = is_sub_cate_overwrite(sub_cate_mode)
        if not overwrite_sub_cate and current_cate1 and current_cate2:
            stats["skipped_existing_cate2"] += 1
            continue
        if (
            overwrite_sub_cate
            and current_cate1
            and current_cate2
            and (not has_explicit_hint or (current_cate1, current_cate2) == (matched_cate1, matched_cate2))
        ):
            stats["skipped_existing_cate2"] += 1
            continue

        set_parts: list[str] = []
        params: list[Any] = []
        if "cate1" in cols and should_update_category_field(sub_cate_mode, current_cate1, matched_cate1):
            set_parts.append("`cate1` = %s")
            params.append(matched_cate1)
        if "cate2" in cols and should_update_category_field(sub_cate_mode, current_cate2, matched_cate2):
            set_parts.append("`cate2` = %s")
            params.append(matched_cate2)
        if not set_parts:
            stats["skipped_no_cols"] += 1
            continue

        params.append(existing_id)
        sql = f"UPDATE `{table_name}` SET " + ", ".join(set_parts) + " WHERE id = %s"
        try:
            await mysql_execute_query(sql, tuple(params), fetch=False, dbname=db_name, op_name="learn_status_update")
            stats["updated"] += 1
            logger.debug(
                "[LearnList][cate-post-job] applied | table=%s id=%s cate1=%s cate2=%s cate_name=%s source=%s url=%s",
                table_name,
                existing_id,
                matched_cate1,
                matched_cate2,
                matched_name or "",
                (matched_source or "")[:120],
                lookup_url[:220],
            )
        except Exception as exc:
            stats["errors"] += 1
            logger.warning(
                "[LearnList][cate-post-job] update failed | table=%s id=%s cate1=%s cate2=%s err=%s",
                table_name,
                existing_id,
                matched_cate1,
                matched_cate2,
                exc,
            )

    return stats


async def update_learn_list_status_board(
    db_name: str,
    chat_bot_id: str,
    db_id: str,
    chunks: int,
    raw_filters_str: Optional[str] = None,  # ??占쎌맄 ?紐낆넎; url_filters ?袁⑸열 cate ?占?占쏙옙 獄쏆꼷????????? ??占쎌벉
    *,
    cate1: Any = _CATE_ARG_UNSET,
    cate2: Any = _CATE_ARG_UNSET,
    content_created_at: Optional[Any] = None,
    cate1_override: Optional[str] = None,
    cate2_override: Optional[str] = None,
    preserve_created_at: bool = False,
) -> bool:
    """
    ??占쎈뮸 ?袁⑥┷ ??LEARN_LIST.status='Y' ??占쎈쑓??占쎈뱜.

    - 野껊슣??????占쏙옙 ?占쎈벏???占쎌쨮 ?????占쎈뼄.
    - content_created_at: ??쨌占???占쏙옙?占?占쏙옙 ?占쎈뗄????源낆쨯???臾믨쉐???? ???占쏙옙?占쎈뗄占??占싼됱쓥????占쎌뱽 ???占쏙옙 獄쏆꼷??
    - cate1/cate2: ?紐꾩쁽????占쎌셽??占썬늺(_CATE_ARG_UNSET) ??占???占싼됱쓥?? UPDATE????占??? ??占쎌벉.
      筌뤿굞??????占쎈퓠????? 揶쏉옙??占퐋ne ??占? 域밸챶?占?獄쏆꼷?? url_filters JSON 筌ㅼ뮇占???袁⑸열 cate占?占?占쏙옙??占쏙옙? ??占쎌벉.
    """
    _ = raw_filters_str
    if not (db_name and chat_bot_id and db_id):
        return False
    learn_list_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    if not learn_list_table:
        return False
    try:
        chunks_i = max(0, int(chunks or 0))
    except Exception:
        chunks_i = 0
    cache_eligible = (
        cate1 is _CATE_ARG_UNSET
        and cate2 is _CATE_ARG_UNSET
        and cate1_override is None
        and cate2_override is None
        and not content_created_at
        and chunks_i <= 0
    )
    if cache_eligible and _learn_list_status_success_cache_hit(db_name, learn_list_table, db_id):
        return True
    try:
        cols = await _get_table_columns(db_name, learn_list_table)
        logger.debug(
            "[LearnList] status update target resolved | db=%s chat_bot_id=%s table=%s id=%s chunks=%s",
            db_name,
            chat_bot_id,
            learn_list_table,
            db_id,
            chunks,
        )

        set_parts: list[str] = []
        params: list[Any] = []

        if not cols:
            # ?占싼됱쓥 introspect ??占쎈솭 ?? 筌ㅼ뮇????占쎈쑓??占쎈뱜 ??占쎈즲 (疫꿸퀡???占싼됱쓥 揶쎛??
            set_parts = ["status = 'Y'"]
            params = []
        else:
            if "status" in cols:
                set_parts.append("status = 'Y'")
            if "chunk" in cols:
                set_parts.append("`chunk` = %s")
                params.append(chunks_i)

        # cate: override > ??占?cate1/cate2 ?紐꾩쁽). url_filters ?袁⑸열(JSON 筌ㅼ뮇占????占쎌쨮 ?占?占쏙옙 占?占쏙옙??占쏙옙? ??占쎌벉.
        include_cate1 = cate1_override is not None or cate1 is not _CATE_ARG_UNSET
        include_cate2 = cate2_override is not None or cate2 is not _CATE_ARG_UNSET

        if include_cate1:
            if cate1_override is not None:
                final_cate1 = _normalize_cate_code(cate1_override)
            else:
                final_cate1 = _normalize_cate_code(cate1)
            final_cate1 = final_cate1 or ""
        else:
            final_cate1 = None

        if include_cate2:
            if cate2_override is not None:
                final_cate2 = _normalize_cate_code(cate2_override)
            else:
                final_cate2 = _normalize_cate_code(cate2)
            final_cate2 = final_cate2 or ""
        else:
            final_cate2 = None

        sub_cate_mode = await get_sub_cate_mode_from_config(chat_bot_id, dbname=db_name)
        overwrite_sub_cate = is_sub_cate_overwrite(sub_cate_mode)

        if include_cate1 and final_cate1 and (not cols or "cate1" in cols):
            if overwrite_sub_cate:
                set_parts.append("cate1 = %s")
            else:
                set_parts.append("cate1 = CASE WHEN COALESCE(NULLIF(cate1, ''), '') = '' THEN %s ELSE cate1 END")
            params.append(final_cate1)
        if include_cate2 and final_cate2 and (not cols or "cate2" in cols):
            if overwrite_sub_cate:
                set_parts.append("cate2 = %s")
            else:
                set_parts.append("cate2 = CASE WHEN COALESCE(NULLIF(cate2, ''), '') = '' THEN %s ELSE cate2 END")
            params.append(final_cate2)

        # 3. ????占쏙옙?占?占쏙옙 ?占쎈뗄???野껊슣?占썼눧??源낆쨯???臾믨쉐????content_created_at
        formatted_post_date = _format_reg_date(content_created_at) if content_created_at else None
        if formatted_post_date and (not cols or "content_created_at" in cols):
            set_parts.append("content_created_at = %s")
            params.append(formatted_post_date)


        if cols and "status" not in cols:
            logger.warning(
                "[LearnList] status=Y update skipped: status column missing | table=%s id=%s",
                learn_list_table,
                db_id,
            )
            return False

        if not set_parts:
            return False

        sql = f"UPDATE `{learn_list_table}` SET " + ", ".join(set_parts) + " WHERE id = %s"
        params.append(str(db_id))

        logger.debug(
            "[LearnList][cate-update-debug] status update sql fields | table=%s id=%s include_cate1=%s include_cate2=%s final_cate=(%s,%s) set_parts=%s",
            learn_list_table,
            db_id,
            include_cate1,
            include_cate2,
            final_cate1 if include_cate1 else None,
            final_cate2 if include_cate2 else None,
            set_parts,
        )
        await mysql_execute_query(sql, tuple(params), fetch=False, dbname=db_name, op_name="learn_status_update")
        if not _learn_list_status_verify_enabled():
            _remember_learn_list_status_success(db_name, learn_list_table, db_id)
            logger.debug(
                "[LearnList] status=Y verify skipped | table=%s id=%s cache_eligible=%s",
                learn_list_table,
                db_id,
                cache_eligible,
            )
            return True
        verify_select_parts = ["`status` AS status"] if (not cols or "status" in cols) else ["'' AS status"]
        if cols and "content_type" in cols:
            verify_select_parts.append("`content_type` AS content_type")
        if cols and "cate1" in cols:
            verify_select_parts.append("`cate1` AS cate1")
        if cols and "cate2" in cols:
            verify_select_parts.append("`cate2` AS cate2")
        if cols and "content" in cols:
            verify_select_parts.append("`content` AS content")
        verify_rows = await mysql_execute_query(
            f"SELECT {', '.join(verify_select_parts)} FROM `{learn_list_table}` WHERE id = %s LIMIT 1",
            (str(db_id),),
            fetch=True,
            dbname=db_name,
        )
        if not verify_rows:
            logger.warning(
                "[LearnList] status=Y verify failed: row not found | table=%s id=%s chunks=%s",
                learn_list_table,
                db_id,
                chunks,
            )
            return False
        verify_row = verify_rows[0]
        verified_status = str(_safe_row_get(verify_row, "status") or "").strip().upper()
        logger.debug(
            "[LearnList][cate-update-debug] status update verify | table=%s id=%s status=%s content_type=%s cate=(%r,%r) include_cate=(%s,%s) requested=(%r,%r)",
            learn_list_table,
            db_id,
            verified_status or "-",
            _safe_row_get(verify_row, "content_type"),
            _safe_row_get(verify_row, "cate1"),
            _safe_row_get(verify_row, "cate2"),
            include_cate1,
            include_cate2,
            final_cate1 if include_cate1 else None,
            final_cate2 if include_cate2 else None,
        )
        if verified_status != "Y":
            logger.warning(
                "[LearnList] status=Y verify failed: status mismatch | table=%s id=%s status=%s",
                learn_list_table,
                db_id,
                verified_status or "-",
            )
            return False

        _remember_learn_list_status_success(db_name, learn_list_table, db_id)
        return True

        async def _cleanup_same_category_file_duplicates() -> None:
            # Speed-first file crawl keeps duplicate cleanup to the post-download
            # source_url + normalized filename + size check, plus DB unique/upsert defense.
            return
            if not cols or "subject" not in cols or "size" not in cols:
                return

            select_cols = ["`id`", "`subject`", "`size`", "`status`"]
            if "cate1" in cols:
                select_cols.append("`cate1`")
            if "cate2" in cols:
                select_cols.append("`cate2`")
            if "content" in cols:
                select_cols.append("`content`")
            if "type" in cols:
                select_cols.append("`type`")
            if "content_type" in cols:
                select_cols.append("`content_type`")

            current_rows = await mysql_execute_query(
                f"SELECT {', '.join(select_cols)} FROM `{learn_list_table}` WHERE id = %s LIMIT 1",
                (str(db_id),),
                fetch=True,
                dbname=db_name,
            )
            if not current_rows:
                return

            current = current_rows[0]
            current_subject = str(_safe_row_get(current, "subject") or "").strip()
            current_size = _safe_row_get(current, "size")
            current_type = str(_safe_row_get(current, "type") or _safe_row_get(current, "content_type") or "").strip().lower()
            current_cate1 = str(_safe_row_get(current, "cate1") or "").strip() if "cate1" in cols else ""
            current_cate2 = str(_safe_row_get(current, "cate2") or "").strip() if "cate2" in cols else ""
            current_content = str(_safe_row_get(current, "content") or "").strip() if "content" in cols else ""

            if not current_subject and not current_content:
                return
            if current_type and current_type != "file":
                return

            dup_ids: set[int] = set()
            if current_subject and current_size not in (None, ""):
                dup_where = ["`subject` = %s", "`size` = %s", "`id` <> %s"]
                dup_params: list[Any] = [current_subject, current_size, str(db_id)]
                if "type" in cols:
                    dup_where.append("`type` = %s")
                    dup_params.append("file")
                elif "content_type" in cols:
                    dup_where.append("`content_type` = %s")
                    dup_params.append("file")
                if "cate1" in cols:
                    if current_cate1:
                        dup_where.append("`cate1` = %s")
                        dup_params.append(current_cate1)
                    else:
                        dup_where.append("COALESCE(NULLIF(`cate1`, ''), '') = ''")
                if "cate2" in cols:
                    if current_cate2:
                        dup_where.append("`cate2` = %s")
                        dup_params.append(current_cate2)
                    else:
                        dup_where.append("COALESCE(NULLIF(`cate2`, ''), '') = ''")

                dup_rows = await mysql_execute_query(
                    f"SELECT `id` FROM `{learn_list_table}` WHERE {' AND '.join(dup_where)} ORDER BY `id` ASC LIMIT 50",
                    tuple(dup_params),
                    fetch=True,
                    dbname=db_name,
                )

                for row in dup_rows:
                    try:
                        rid = int(_safe_row_get(row, "id"))
                    except Exception:
                        rid = 0
                    if rid > 0:
                        dup_ids.add(rid)

            match_col = "content" if "content" in cols and current_content else ""
            match_value = current_content
            attach_keys = extract_attachment_key_candidates(match_value)
            if match_col and match_value:
                extra_where = ["`id` <> %s"]
                extra_params: list[Any] = [str(db_id)]
                if "type" in cols:
                    extra_where.append("`type` = %s")
                    extra_params.append("file")
                elif "content_type" in cols:
                    extra_where.append("`content_type` = %s")
                    extra_params.append("file")
                if "cate1" in cols:
                    if current_cate1:
                        extra_where.append("`cate1` = %s")
                        extra_params.append(current_cate1)
                    else:
                        extra_where.append("COALESCE(NULLIF(`cate1`, ''), '') = ''")
                if "cate2" in cols:
                    if current_cate2:
                        extra_where.append("`cate2` = %s")
                        extra_params.append(current_cate2)
                    else:
                        extra_where.append("COALESCE(NULLIF(`cate2`, ''), '') = ''")

                match_parts = [f"`{match_col}` = %s"]
                match_params: list[Any] = [match_value]
                for key in attach_keys[:6]:
                    match_parts.append(f"`{match_col}` LIKE %s ESCAPE '!'")
                    match_params.append(sql_like_contains_pattern(key))

                extra_sql = (
                    f"SELECT `id` FROM `{learn_list_table}` "
                    f"WHERE {' AND '.join(extra_where)} AND ({' OR '.join(match_parts)}) "
                    "ORDER BY `id` ASC LIMIT 100"
                )
                extra_rows = await mysql_execute_query(
                    extra_sql,
                    tuple(extra_params + match_params),
                    fetch=True,
                    dbname=db_name,
                )
                for row in extra_rows or []:
                    try:
                        rid = int(_safe_row_get(row, "id"))
                    except Exception:
                        rid = 0
                    if rid > 0:
                        dup_ids.add(rid)

            if not dup_ids:
                return

            dup_id_list = sorted(dup_ids)
            placeholders = ", ".join(["%s"] * len(dup_id_list))
            await mysql_execute_query(
                f"DELETE FROM `{learn_list_table}` WHERE `id` IN ({placeholders})",
                tuple(dup_id_list),
                fetch=False,
                dbname=db_name,
            )
            logger.debug(
                "[LearnList] same-category duplicate rows removed after status=Y | table=%s keep_id=%s removed_ids=%s subject=%s cate1=%s cate2=%s attach_keys=%s",
                learn_list_table,
                db_id,
                dup_id_list,
                current_subject[:120],
                current_cate1 or "-",
                current_cate2 or "-",
                len(attach_keys),
            )

        await _cleanup_same_category_file_duplicates()
        return True

    except Exception as e:
        logger.error("[LearnList] update_learn_list_status_board failed: %s", e, exc_info=True)
        return False


async def update_learn_list_pre_embedding_metrics(
    *,
    db_name: str,
    chat_bot_id: str,
    db_id: str,
    chunks: int,
    size_bytes: Optional[int] = None,
) -> bool:
    """
    獄쏄퀣???袁⑥퓢?????占??源낆쨯 ?袁⑸퓠 LEARN_LIST??size/chunk占??醫딆뺘?怨밸립??

    status??椰꾬옙?諭띄뵳?? ??占쎈뮉?? ??占쎌젫 ?袁⑥┷(status='Y') 筌ｌ꼶???獄쏄퀣???占쎌뮆占?finalize 野껋럥占?占?占쏙옙 ??占쎈뻬??占쎈뼄.
    """
    if not (db_name and chat_bot_id and db_id):
        return False
    learn_list_table = await resolve_learn_list_table_name_for_chatbot(chat_bot_id, db_name)
    if not learn_list_table:
        return False
    try:
        cols = await ensure_learn_list_standard_columns(db_name, learn_list_table)
        set_parts: list[str] = []
        params: list[Any] = []

        if size_bytes is not None and "size" in cols:
            try:
                size_i = max(0, int(size_bytes or 0))
            except Exception:
                size_i = 0
            set_parts.append("`size` = %s")
            params.append(size_i)

        try:
            chunks_i = max(0, int(chunks or 0))
        except Exception:
            chunks_i = 0
        if "chunk" in cols:
            set_parts.append("`chunk` = %s")
            params.append(chunks_i)

        if not set_parts:
            logger.debug(
                "[LearnList] pre-embedding metrics skipped; no compatible columns | db=%s table=%s id=%s chunks=%s size=%s",
                db_name,
                learn_list_table,
                db_id,
                chunks,
                size_bytes,
            )
            return True

        params.append(str(db_id))
        await mysql_execute_query(
            f"UPDATE `{learn_list_table}` SET {', '.join(set_parts)} WHERE id = %s",
            tuple(params),
            dbname=db_name,
        )
        logger.debug(
            "[LearnList] pre-embedding metrics updated | db=%s table=%s id=%s chunks=%s size=%s",
            db_name,
            learn_list_table,
            db_id,
            chunks_i,
            size_bytes,
        )
        return True
    except Exception as exc:
        logger.warning(
            "[LearnList] pre-embedding metrics update failed | db=%s chat_bot_id=%s id=%s chunks=%s size=%s err=%s",
            db_name,
            chat_bot_id,
            db_id,
            chunks,
            size_bytes,
            exc,
        )
        return False


async def sync_file_categories_from_homepage_learning(
    *,
    chat_bot_id: str,
    db_name: str,
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    if not str(chat_bot_id or "").strip():
        raise ValueError("chat_bot_id_required")
    if not str(db_name or "").strip():
        raise ValueError("db_name_required")

    preview = await preview_file_category_sync_plan(chat_bot_id=chat_bot_id, db_name=db_name)
    category_table = str(preview.get("category_table") or get_category_table_name(chat_bot_id))
    account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
    learn_list_table = get_learn_list_table_name(account_identifier)

    def _same_cate_code(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
        left_code = str((left or {}).get("cate_code") or "").strip()
        right_code = str((right or {}).get("cate_code") or "").strip()
        return bool(left_code and right_code and left_code == right_code)

    async def _ensure_child(
        parent_row: Dict[str, Any],
        cate_name: str,
        *,
        stats_key: str,
        force_create: bool = False,
    ) -> Dict[str, Any]:
        existing = await ensure_file_learning_category_by_name(
            chat_bot_id=chat_bot_id,
            db_name=db_name,
            cate_name=cate_name,
            parent_cate_code=str(parent_row.get("cate_code") or ""),
            access_url=access_url,
            request_cookies=request_cookies,
            create_missing=False,
        )
        if existing and not force_create:
            stats[stats_key]["reused"] += 1
            return dict(existing)

        logger.debug(
            "[CategorySync][file] existing file-learning category not found; creation disabled | parent=%s cate_name=%s",
            parent_row.get("cate_code"),
            cate_name,
        )
        return {}

    async def _count_and_update_by_cate2(old_cate2: str, new_cate1: str, new_cate2: str) -> int:
        affected = await mysql_execute_query(
            f"UPDATE `{learn_list_table}` SET cate1 = %s, cate2 = %s WHERE content_type = %s AND cate2 = %s",
            (str(new_cate1 or "").strip(), str(new_cate2 or "").strip(), "file", str(old_cate2 or "").strip()),
            fetch=False,
            dbname=db_name,
        )
        try:
            return int(affected or 0)
        except Exception:
            return 0

    async def _count_and_update_cate1_only(old_cate1: str, new_cate1: str, new_cate2: str = "") -> int:
        affected = await mysql_execute_query(
            f"""
            UPDATE `{learn_list_table}`
            SET cate1 = %s, cate2 = %s
            WHERE content_type = %s
              AND cate1 = %s
              AND COALESCE(NULLIF(cate2, ''), '') = ''
            """,
            (str(new_cate1 or "").strip(), str(new_cate2 or "").strip(), "file", str(old_cate1 or "").strip()),
            fetch=False,
            dbname=db_name,
        )
        try:
            return int(affected or 0)
        except Exception:
            return 0

    source_root = dict(preview.get("source_root") or {})
    target_root = dict(preview.get("target_root") or {})

    stats: Dict[str, Any] = {
        "cate1": {"created": 0, "reused": 0, "created_due_mismatch": 0},
        "cate2": {"created": 0, "reused": 0, "created_due_mismatch": 0},
        "updated_file_rows_by_cate1_only": 0,
        "updated_file_rows_by_cate2": 0,
    }
    mappings: list[Dict[str, Any]] = []

    for plan_item in list(preview.get("plan_items") or []):
        source_cate1 = dict(plan_item.get("source_cate1") or {})
        target_cate1 = dict(target_root)
        cate1_only_count = int(plan_item.get("matched_file_rows_by_cate1_only") or 0)
        cate2_plans = list(plan_item.get("cate2_plans") or [])
        cate1_only_count = 0

        mapping_item: Dict[str, Any] = {
            "source_cate1": dict(source_cate1),
            "target_cate1": dict(target_cate1),
            "target_cate2_for_cate1_only": None,
            "updated_file_rows_by_cate1_only": cate1_only_count,
            "cate2_mappings": [],
        }

        for cate2_plan in cate2_plans:
            source_cate2 = dict(cate2_plan.get("source_cate2") or {})
            matched_rows = int(cate2_plan.get("matched_file_rows") or 0)
            target_cate2 = dict(
                await _fetch_category_direct_child_by_name(
                    category_table=category_table,
                    parent_treecode=str(target_root.get("cate_treecode") or ""),
                    cate_name=str(source_cate2.get("cate_name") or ""),
                    db_name=db_name,
                )
                or {}
            )
            if target_cate2:
                stats["cate2"]["reused"] += 1
            else:
                target_cate2 = await _ensure_child(
                    dict(target_root),
                    str(source_cate2.get("cate_name") or ""),
                    stats_key="cate2",
                    force_create=False,
                )
            if not target_cate2:
                mapping_item["cate2_mappings"].append(
                    {
                        "source_cate2": dict(source_cate2),
                        "target_cate2": {},
                        "updated_file_rows": 0,
                        "skipped": "file_learning_category_missing",
                    }
                )
                continue
            updated_count = await _count_and_update_by_cate2(
                str(source_cate2.get("cate_code") or ""),
                str(target_root.get("cate_code") or ""),
                str(target_cate2.get("cate_code") or ""),
            )
            stats["updated_file_rows_by_cate2"] += updated_count
            mapping_item["cate2_mappings"].append(
                {
                    "source_cate2": dict(source_cate2),
                    "target_cate2": dict(target_cate2),
                    "updated_file_rows": updated_count,
                }
            )

        mappings.append(mapping_item)

    logger.debug(
        "[CategorySync][file] completed | db=%s chat_bot_id=%s cate1_created=%s cate2_created=%s rows_cate1_only=%s rows_cate2=%s",
        db_name,
        chat_bot_id,
        stats["cate1"]["created"],
        stats["cate2"]["created"],
        stats["updated_file_rows_by_cate1_only"],
        stats["updated_file_rows_by_cate2"],
    )
    return {
        "ok": True,
        "db_name": db_name,
        "chat_bot_id": chat_bot_id,
        "category_table": category_table,
        "learn_list_table": learn_list_table,
        "source_root": dict(source_root),
        "target_root": dict(target_root),
        "preview_summary": dict(preview.get("summary") or {}),
        "stats": stats,
        "mappings": mappings,
    }


async def update_file_categories_by_subject_names(
    *,
    chat_bot_id: str,
    db_name: str,
    subject_names: Iterable[str],
    board_cate2: str,
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
    blank_only: bool = True,
) -> Dict[str, Any]:
    names = _unique_preserve_order(str(name or "").strip() for name in subject_names if str(name or "").strip())
    logger.debug(
        "[CategorySync][file-update-only] start | db=%s chat_bot_id=%s board_cate2=%s subject_count=%s blank_only=%s subject_sample=%s",
        db_name,
        chat_bot_id,
        board_cate2,
        len(names),
        blank_only,
        names[:10],
    )
    if not names:
        return {"ok": True, "updated": 0, "reason": "no_subject_names"}
    if not str(board_cate2 or "").strip():
        return {"ok": True, "updated": 0, "reason": "no_board_cate2", "subject_count": len(names)}

    logger.debug(
        "[CategorySync][file-update-only] account lookup start | db=%s chat_bot_id=%s",
        db_name,
        chat_bot_id,
    )
    account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
    logger.debug(
        "[CategorySync][file-update-only] account lookup done | db=%s chat_bot_id=%s account=%s",
        db_name,
        chat_bot_id,
        account_identifier,
    )
    learn_list_table = get_learn_list_table_name(account_identifier)
    logger.debug(
        "[CategorySync][file-update-only] columns load start | db=%s table=%s",
        db_name,
        learn_list_table,
    )
    cols = await ensure_learn_list_standard_columns(db_name, learn_list_table)
    logger.debug(
        "[CategorySync][file-update-only] table | db=%s chat_bot_id=%s account=%s table=%s cols=%s",
        db_name,
        chat_bot_id,
        account_identifier,
        learn_list_table,
        sorted(cols or []),
    )
    if "subject" not in cols or "cate1" not in cols:
        return {
            "ok": True,
            "updated": 0,
            "reason": "missing_subject_or_cate1_column",
            "learn_list_table": learn_list_table,
        }

    board_cate2_code = str(board_cate2 or "").strip()
    mapping_cache_key = (str(db_name or "").strip(), str(chat_bot_id or "").strip(), board_cate2_code)
    if mapping_cache_key in _FILE_UPDATE_ONLY_MAPPING_CACHE:
        mapped_c1, mapped_c2 = _FILE_UPDATE_ONLY_MAPPING_CACHE[mapping_cache_key]
    else:
        logger.debug(
            "[CategorySync][file-update-only] mapping lookup start | db=%s chat_bot_id=%s board_cate2=%s",
            db_name,
            chat_bot_id,
            board_cate2_code,
        )
        mapped_c1, mapped_c2 = await _ensure_file_learning_category_mapping(
            chat_bot_id=chat_bot_id,
            db_name=db_name,
            source_cate1="",
            source_cate2=board_cate2_code,
            access_url=access_url,
            request_cookies=request_cookies,
            create_missing=False,
        )
        _FILE_UPDATE_ONLY_MAPPING_CACHE[mapping_cache_key] = (mapped_c1, mapped_c2)
        logger.debug(
            "[CategorySync][file-update-only] mapping lookup done | db=%s chat_bot_id=%s board_cate2=%s mapped=(%s,%s)",
            db_name,
            chat_bot_id,
            board_cate2_code,
            mapped_c1,
            mapped_c2,
        )
    if not mapped_c2:
        logger.warning(
            "[CategorySync][file-update-only] mapping empty: file-learning child category not found | db=%s chat_bot_id=%s board_cate2=%s mapped=(%s,%s)",
            db_name,
            chat_bot_id,
            board_cate2,
            mapped_c1,
            mapped_c2,
        )
        return {
            "ok": True,
            "updated": 0,
            "reason": "category_mapping_empty",
            "mapped_cate1": mapped_c1,
            "mapped_cate2": mapped_c2,
            "subject_count": len(names),
            "learn_list_table": learn_list_table,
        }

    file_conditions = []
    if "content_type" in cols:
        file_conditions.append("LOWER(COALESCE(`content_type`, '')) = 'file'")
    file_where = "(" + " OR ".join(file_conditions) + ")" if file_conditions else "1=1"

    where_parts = [f"`subject` IN ({{placeholders}})", file_where]
    if blank_only:
        where_parts.append("COALESCE(NULLIF(`cate1`, ''), '') = ''")
        if "cate2" in cols:
            where_parts.append("COALESCE(NULLIF(`cate2`, ''), '') = ''")

    set_parts = ["`cate1` = %s"]
    set_params: list[Any] = [mapped_c1]
    if "cate2" in cols:
        set_parts.append("`cate2` = %s")
        set_params.append(mapped_c2)

    updated = 0
    matched_subjects: list[str] = []
    chunk_size = 100
    for offset in range(0, len(names), chunk_size):
        chunk = names[offset : offset + chunk_size]
        placeholders = ", ".join(["%s"] * len(chunk))
        where_sql = " AND ".join(where_parts).format(placeholders=placeholders)
        if offset == 0:
            logger.debug(
                "[CategorySync][file-update-only] query | db=%s table=%s where=%s set_cate=(%s,%s) chunk_sample=%s",
                db_name,
                learn_list_table,
                where_sql,
                mapped_c1,
                mapped_c2,
                chunk[:10],
            )
        rows = await mysql_execute_query(
            f"SELECT `subject` FROM `{learn_list_table}` WHERE {where_sql} LIMIT {len(chunk)}",
            tuple(chunk),
            fetch=True,
            dbname=db_name,
        )
        for row in rows or []:
            subject = str((row or {}).get("subject") or "").strip()
            if subject and subject not in matched_subjects:
                matched_subjects.append(subject)
        affected = await mysql_execute_query(
            f"UPDATE `{learn_list_table}` SET {', '.join(set_parts)} WHERE {where_sql}",
            tuple(set_params + chunk),
            fetch=False,
            dbname=db_name,
        )
        try:
            updated += int(affected or 0)
        except Exception:
            updated += len(rows or [])
        logger.debug(
            "[CategorySync][file-update-only] chunk | db=%s table=%s offset=%s chunk=%s matched=%s affected=%s updated_total=%s",
            db_name,
            learn_list_table,
            offset,
            len(chunk),
            len(rows or []),
            affected,
            updated,
        )

    logger.debug(
        "[CategorySync][file-update-only] subject category update | db=%s chat_bot_id=%s cate2=%s mapped=(%s,%s) subjects=%s matched=%s updated=%s",
        db_name,
        chat_bot_id,
        board_cate2,
        mapped_c1,
        mapped_c2,
        len(names),
        len(matched_subjects),
        updated,
    )
    return {
        "ok": True,
        "updated": updated,
        "matched_subjects": matched_subjects[:20],
        "subject_count": len(names),
        "mapped_cate1": mapped_c1,
        "mapped_cate2": mapped_c2,
        "learn_list_table": learn_list_table,
    }


async def update_file_categories_by_source_page(
    *,
    chat_bot_id: str,
    db_name: str,
    source_page: str,
    board_cate2: str,
    access_url: Optional[str] = None,
    request_cookies: Optional[Dict[str, str]] = None,
    blank_only: bool = True,
) -> Dict[str, Any]:
    page_url = str(source_page or "").strip()
    if not page_url:
        return {"ok": True, "updated": 0, "reason": "no_source_page"}
    if not str(board_cate2 or "").strip():
        return {"ok": True, "updated": 0, "reason": "no_board_cate2", "source_page": page_url}

    account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
    learn_list_table = get_learn_list_table_name(account_identifier)
    cols = await ensure_learn_list_standard_columns(db_name, learn_list_table)
    source_cols = [col for col in ("source_page", "content") if col in cols]
    if not source_cols or "cate1" not in cols:
        return {
            "ok": True,
            "updated": 0,
            "reason": "missing_source_page_content_or_cate1_column",
            "learn_list_table": learn_list_table,
            "source_page": page_url,
        }

    board_cate2_code = str(board_cate2 or "").strip()
    mapping_cache_key = (str(db_name or "").strip(), str(chat_bot_id or "").strip(), board_cate2_code)
    if mapping_cache_key in _FILE_UPDATE_ONLY_MAPPING_CACHE:
        mapped_c1, mapped_c2 = _FILE_UPDATE_ONLY_MAPPING_CACHE[mapping_cache_key]
    else:
        mapped_c1, mapped_c2 = await _ensure_file_learning_category_mapping(
            chat_bot_id=chat_bot_id,
            db_name=db_name,
            source_cate1="",
            source_cate2=board_cate2_code,
            access_url=access_url,
            request_cookies=request_cookies,
            create_missing=False,
        )
        _FILE_UPDATE_ONLY_MAPPING_CACHE[mapping_cache_key] = (mapped_c1, mapped_c2)
    if not mapped_c2:
        logger.warning(
            "[CategorySync][file-update-only] source_page mapping empty | db=%s chat_bot_id=%s board_cate2=%s source_page=%s mapped=(%s,%s)",
            db_name,
            chat_bot_id,
            board_cate2,
            page_url[:220],
            mapped_c1,
            mapped_c2,
        )
        return {
            "ok": True,
            "updated": 0,
            "reason": "category_mapping_empty",
            "mapped_cate1": mapped_c1,
            "mapped_cate2": mapped_c2,
            "learn_list_table": learn_list_table,
            "source_page": page_url,
        }

    file_conditions = []
    if "content_type" in cols:
        file_conditions.append("LOWER(COALESCE(`content_type`, '')) = 'file'")
    file_where = "(" + " OR ".join(file_conditions) + ")" if file_conditions else "1=1"

    url_candidates = _unique_preserve_order(
        candidate
        for candidate in (
            page_url,
            page_url.replace("https://", "http://", 1) if page_url.startswith("https://") else "",
            page_url.replace("http://", "https://", 1) if page_url.startswith("http://") else "",
        )
        if candidate
    )
    placeholders = ", ".join(["%s"] * len(url_candidates))
    source_where = "(" + " OR ".join(f"`{col}` IN ({placeholders})" for col in source_cols) + ")"
    params_for_source = []
    for _col in source_cols:
        params_for_source.extend(url_candidates)

    where_parts = [source_where, file_where]
    if blank_only:
        where_parts.append("COALESCE(NULLIF(`cate1`, ''), '') = ''")
        if "cate2" in cols:
            where_parts.append("COALESCE(NULLIF(`cate2`, ''), '') = ''")
    where_sql = " AND ".join(where_parts)

    set_parts = ["`cate1` = %s"]
    set_params: list[Any] = [mapped_c1]
    if "cate2" in cols:
        set_parts.append("`cate2` = %s")
        set_params.append(mapped_c2)

    rows = await mysql_execute_query(
        f"SELECT `id`, `subject` FROM `{learn_list_table}` WHERE {where_sql} LIMIT 500",
        tuple(params_for_source),
        fetch=True,
        dbname=db_name,
    )
    affected = 0
    if rows:
        affected = await mysql_execute_query(
            f"UPDATE `{learn_list_table}` SET {', '.join(set_parts)} WHERE {where_sql}",
            tuple(set_params + params_for_source),
            fetch=False,
            dbname=db_name,
        )
    try:
        updated = int(affected or 0)
    except Exception:
        updated = len(rows or [])
    logger.debug(
        "[CategorySync][file-update-only] source_page update | db=%s table=%s source_cols=%s source_page=%s matched=%s affected=%s mapped=(%s,%s)",
        db_name,
        learn_list_table,
        source_cols,
        page_url[:220],
        len(rows or []),
        affected,
        mapped_c1,
        mapped_c2,
    )
    return {
        "ok": True,
        "updated": updated,
        "matched_rows": len(rows or []),
        "matched_subjects": [str((row or {}).get("subject") or "").strip() for row in list(rows or [])[:20]],
        "mapped_cate1": mapped_c1,
        "mapped_cate2": mapped_c2,
        "learn_list_table": learn_list_table,
        "source_page": page_url,
        "reason": "source_page_update",
    }


def _find_root_row_from_category_rows(rows: Iterable[Dict[str, Any]], root_key: str) -> Optional[Dict[str, Any]]:
    items = [dict(row or {}) for row in rows or []]
    preferred_code = _category_root_code_from_env(root_key)
    fallback_names = {str(name or "").strip() for name in _CATEGORY_ROOT_FALLBACK_NAMES.get(root_key, ())}
    if preferred_code:
        for row in items:
            code = str(row.get("cate_code") or "").strip()
            name = str(row.get("cate_name") or "").strip()
            if code == preferred_code and (not fallback_names or name in fallback_names):
                return row
    candidates = [
        row
        for row in items
        if str(row.get("cate_name") or "").strip() in fallback_names
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda row: (len(str(row.get("cate_treecode") or "")), str(row.get("cate_treecode") or "")))
    return candidates[0]


async def bulk_update_file_categories_by_source_pages(
    *,
    chat_bot_id: str,
    db_name: str,
    assignments: Iterable[Dict[str, str]],
    blank_only: bool = True,
) -> Dict[str, Any]:
    normalized: list[Dict[str, str]] = []
    seen_pairs: set[Tuple[str, str]] = set()
    for item in assignments or []:
        if not isinstance(item, dict):
            continue
        source_page = str(item.get("source_page") or item.get("url") or "").strip()
        board_cate1 = str(item.get("board_cate1") or item.get("cate1") or "").strip()
        board_cate2 = str(item.get("board_cate2") or item.get("cate2") or "").strip()
        if not (source_page and board_cate2):
            continue
        key = (source_page, board_cate1, board_cate2)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        normalized.append({"source_page": source_page, "board_cate1": board_cate1, "board_cate2": board_cate2})
    if not normalized:
        return {"ok": True, "updated": 0, "reason": "no_assignments"}

    category_table = get_category_table_name(chat_bot_id)
    category_rows = [
        row for row in await _fetch_category_rows_cached(category_table=category_table, db_name=db_name)
        if _category_row_is_used(row)
    ]
    category_rows.sort(key=lambda row: (len(str((row or {}).get("cate_treecode") or "")), str((row or {}).get("cate_treecode") or "")))
    by_code = {
        str((row or {}).get("cate_code") or "").strip(): dict(row or {})
        for row in category_rows
        if str((row or {}).get("cate_code") or "").strip()
    }
    row_by_tree: Dict[str, Dict[str, Any]] = {}
    for row in category_rows:
        tree = str((row or {}).get("cate_treecode") or "").strip()
        if tree:
            row_by_tree[tree] = dict(row or {})

    grouped_urls: Dict[Tuple[str, str], list[str]] = {}
    missing: list[Dict[str, str]] = []

    def _parent_code_from_loaded_rows(row: Optional[Dict[str, Any]]) -> str:
        tree = str((row or {}).get("cate_treecode") or "").strip()
        if not tree:
            return ""
        best_tree = ""
        best_code = ""
        for ancestor_tree, ancestor_row in row_by_tree.items():
            ancestor_text = str(ancestor_tree or "").strip()
            if not ancestor_text or ancestor_text == tree:
                continue
            if tree.startswith(ancestor_text) and len(ancestor_text) > len(best_tree):
                best_tree = ancestor_text
                best_code = str((ancestor_row or {}).get("cate_code") or "").strip()
        return best_code

    mapping_cache: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for item in normalized:
        board_cate1 = item.get("board_cate1", "")
        board_cate2 = item["board_cate2"]
        mapping_key = (board_cate1, board_cate2)
        if mapping_key in mapping_cache:
            mapped_c1, mapped_c2 = mapping_cache[mapping_key]
        else:
            mapped_c1, mapped_c2 = await _ensure_file_learning_category_mapping(
                chat_bot_id=chat_bot_id,
                db_name=db_name,
                source_cate1=board_cate1,
                source_cate2=board_cate2,
                create_missing=False,
            )
            mapping_cache[mapping_key] = (mapped_c1, mapped_c2)
        if len(missing) < 20 and not (mapped_c1 or mapped_c2):
            missing.append(
                {
                    "source_page": item["source_page"][:220],
                    "board_cate1": board_cate1,
                    "board_cate2": board_cate2,
                    "board_name": "",
                    "fallback_cate1": mapped_c1,
                    "fallback_cate2": mapped_c2,
                }
            )
        if not (mapped_c1 or mapped_c2):
            continue
        grouped_urls.setdefault((mapped_c1, mapped_c2), []).append(item["source_page"])

    account_identifier = await get_account_identifier_from_chatbot_setup(chat_bot_id, db_name)
    learn_list_table = get_learn_list_table_name(account_identifier)
    cols = await _get_table_columns(db_name, learn_list_table)
    source_cols = [col for col in ("source_page", "content") if col in cols]
    if not source_cols or "cate1" not in cols:
        return {
            "ok": True,
            "updated": 0,
            "reason": "missing_source_page_content_or_cate1_column",
            "assignment_count": len(normalized),
            "mapped_url_count": sum(len(urls) for urls in grouped_urls.values()),
            "missing": missing,
            "learn_list_table": learn_list_table,
        }

    file_conditions = []
    if "content_type" in cols:
        file_conditions.append("LOWER(COALESCE(`content_type`, '')) = 'file'")
    file_where = "(" + " OR ".join(file_conditions) + ")" if file_conditions else "1=1"

    set_cols = ["`cate1` = %s"]
    if "cate2" in cols:
        set_cols.append("`cate2` = %s")

    updated = 0
    update_groups = 0
    zero_update_diagnostics: list[Dict[str, Any]] = []
    for (mapped_c1, mapped_c2), urls in grouped_urls.items():
        unique_urls = _unique_preserve_order(urls)
        for offset in range(0, len(unique_urls), 100):
            chunk = unique_urls[offset : offset + 100]
            url_candidates: list[str] = []
            for url in chunk:
                url_candidates.extend(
                    candidate
                    for candidate in (
                        url,
                        url.replace("https://", "http://", 1) if url.startswith("https://") else "",
                        url.replace("http://", "https://", 1) if url.startswith("http://") else "",
                    )
                    if candidate
                )
            url_candidates = _unique_preserve_order(url_candidates)
            placeholders = ", ".join(["%s"] * len(url_candidates))
            source_where = "(" + " OR ".join(f"`{col}` IN ({placeholders})" for col in source_cols) + ")"
            source_params: list[Any] = []
            for _col in source_cols:
                source_params.extend(url_candidates)
            where_parts = [source_where, file_where]
            if blank_only:
                where_parts.append("COALESCE(NULLIF(`cate1`, ''), '') = ''")
                if "cate2" in cols:
                    where_parts.append("COALESCE(NULLIF(`cate2`, ''), '') = ''")
            set_params: list[Any] = [mapped_c1]
            if "cate2" in cols:
                set_params.append(mapped_c2)
            affected = await mysql_execute_query(
                f"UPDATE `{learn_list_table}` SET {', '.join(set_cols)} WHERE {' AND '.join(where_parts)}",
                tuple(set_params + source_params),
                fetch=False,
                dbname=db_name,
            )
            update_groups += 1
            try:
                updated += int(affected or 0)
            except Exception:
                pass
            try:
                if int(affected or 0) == 0 and len(zero_update_diagnostics) < 8:
                    source_only_rows = await mysql_execute_query(
                        f"SELECT COUNT(*) AS cnt FROM `{learn_list_table}` WHERE {source_where}",
                        tuple(source_params),
                        fetch=True,
                        dbname=db_name,
                    )
                    file_rows = await mysql_execute_query(
                        f"SELECT COUNT(*) AS cnt FROM `{learn_list_table}` WHERE {source_where} AND {file_where}",
                        tuple(source_params),
                        fetch=True,
                        dbname=db_name,
                    )
                    blank_rows = await mysql_execute_query(
                        f"SELECT COUNT(*) AS cnt FROM `{learn_list_table}` WHERE {' AND '.join(where_parts)}",
                        tuple(source_params),
                        fetch=True,
                        dbname=db_name,
                    )
                    sample_cols = ["`id`", "`subject`"]
                    for sample_col in ("content_type", "cate1", "cate2", "source_page", "content"):
                        if sample_col in cols:
                            sample_cols.append(f"`{sample_col}`")
                    sample_rows = await mysql_execute_query(
                        f"SELECT {', '.join(sample_cols)} FROM `{learn_list_table}` WHERE {source_where} LIMIT 5",
                        tuple(source_params),
                        fetch=True,
                        dbname=db_name,
                    )
                    zero_update_diagnostics.append(
                        {
                            "mapped": (mapped_c1, mapped_c2),
                            "chunk_size": len(chunk),
                            "source_cols": list(source_cols),
                            "source_only": int(((source_only_rows or [{}])[0] or {}).get("cnt") or 0),
                            "file_type": int(((file_rows or [{}])[0] or {}).get("cnt") or 0),
                            "blank_target": int(((blank_rows or [{}])[0] or {}).get("cnt") or 0),
                            "url_sample": chunk[:3],
                            "row_sample": [
                                {
                                    "id": (row or {}).get("id"),
                                    "subject": str((row or {}).get("subject") or "")[:120],
                                    "content_type": (row or {}).get("content_type"),
                                    "cate1": (row or {}).get("cate1"),
                                    "cate2": (row or {}).get("cate2"),
                                    "source_page": str((row or {}).get("source_page") or "")[:180],
                                    "content": str((row or {}).get("content") or "")[:180],
                                }
                                for row in list(sample_rows or [])
                                if isinstance(row, dict)
                            ],
                        }
                    )
            except Exception as diag_exc:
                logger.debug(
                    "[CategorySync][file-update-only] zero update diagnostic failed | db=%s table=%s err=%s",
                    db_name,
                    learn_list_table,
                    diag_exc,
                )

    logger.debug(
        "[CategorySync][file-update-only] bulk source_page update | db=%s chat_bot_id=%s assignments=%s category_rows=%s mapped_urls=%s groups=%s updated=%s missing=%s zero_update_diag=%s",
        db_name,
        chat_bot_id,
        len(normalized),
        len(category_rows),
        sum(len(urls) for urls in grouped_urls.values()),
        update_groups,
        updated,
        len(missing),
        zero_update_diagnostics,
    )
    return {
        "ok": True,
        "updated": updated,
        "reason": "bulk_source_page_update",
        "assignment_count": len(normalized),
        "category_count": len(category_rows),
        "mapped_url_count": sum(len(urls) for urls in grouped_urls.values()),
        "update_groups": update_groups,
        "missing": missing,
        "learn_list_table": learn_list_table,
        "mapped_category_count": len(grouped_urls),
        "zero_update_diagnostics": zero_update_diagnostics,
    }



