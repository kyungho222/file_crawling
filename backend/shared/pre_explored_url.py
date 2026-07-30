import logging
import os
import json
import re
import html
import random
import time
from datetime import datetime
from typing import Optional, List, AsyncIterable, Union, Dict, Any, Tuple
from backend.shared.url_scope import (
    extract_service_scope_path_prefix,
    extract_scope_host,
    extract_scope_identities,
    extract_scope_path_prefix,
    normalize_scope_path_prefix,
    url_matches_scope_identities,
)
from backend.shared.url_pattern_identity import (
    group_urls_by_structure_pattern,
    url_structure_pattern_has_variable,
    url_structure_pattern_key,
)
from backend.shared.exploration_query import (
    EXPLORATION_TABLE,
    ExplorationQuerySpec,
    build_exploration_conditions,
)
from backend.shared.detail_page_utils import url_query_suggests_board_article_detail
from backend.shared.crawl_trace import crawl_trace
from db.maria_operations import maria_execute_query, maria_select_data
from utils.attachment_url_normalize import sql_like_contains_pattern
from utils.url import ensure_url_scheme, canonicalize_url_for_dedup
from urllib.parse import quote, unquote, urlparse, parse_qsl, urlunparse

logger = logging.getLogger("backend.file.url_loader")

_TABLE_COLUMNS_CACHE: Dict[Tuple[str, str], set[str]] = {}
_CATEGORY_FULL_TABLE_CACHE: Dict[Tuple[str, str, Tuple[str, ...], str], Dict[str, Any]] = {}
_EXPLORATION_DATE_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "content_created_at",
    "reg_date",
    "content_updated_at",
    "created_at",
)


def _category_rule_debug_enabled() -> bool:
    return str(os.getenv("CATEGORY_RULE_DEBUG", "0")).strip().lower() in ("1", "true", "yes", "on")


def _db_load_debug_enabled() -> bool:
    return str(os.getenv("DB_LOAD_DEBUG", "0") or "0").strip().lower() in ("1", "true", "yes", "on")


def _db_load_slow_ms() -> float:
    try:
        return max(0.0, float(os.getenv("DB_LOAD_SLOW_QUERY_MS", "300") or "300"))
    except Exception:
        return 300.0


def _category_full_table_cache_ttl_sec() -> float:
    try:
        raw = float(os.getenv("CATEGORY_FULL_TABLE_CACHE_TTL_SEC", "300") or "300")
    except Exception:
        raw = 300.0
    return max(0.0, min(raw, 3600.0))


def _category_full_table_cache_key(
    *,
    db_name: str,
    table_name: str,
    select_cols: List[str],
    where_sql: str,
) -> Tuple[str, str, Tuple[str, ...], str]:
    return (
        str(db_name or "").strip(),
        str(table_name or "").strip(),
        tuple(str(col or "").strip() for col in select_cols or []),
        str(where_sql or "").strip(),
    )


def _category_full_table_cache_get(key: Tuple[str, str, Tuple[str, ...], str]) -> Optional[Dict[str, Any]]:
    ttl = _category_full_table_cache_ttl_sec()
    if ttl <= 0:
        return None
    cached = _CATEGORY_FULL_TABLE_CACHE.get(key)
    if not isinstance(cached, dict):
        return None
    expires_at = float(cached.get("expires_at") or 0.0)
    if expires_at <= time.time():
        _CATEGORY_FULL_TABLE_CACHE.pop(key, None)
        return None
    return cached


def _category_full_table_cache_put(
    key: Tuple[str, str, Tuple[str, ...], str],
    payload: Dict[str, Any],
) -> None:
    ttl = _category_full_table_cache_ttl_sec()
    if ttl <= 0:
        return
    item = dict(payload or {})
    item["expires_at"] = time.time() + ttl
    _CATEGORY_FULL_TABLE_CACHE[key] = item


def _exploration_rule_relaxed_fallback_min_rows() -> int:
    try:
        raw = int(str(os.getenv("EXPLORATION_RULE_RELAXED_FALLBACK_MIN_ROWS", "10") or "10").strip())
    except Exception:
        raw = 10
    return max(1, min(raw, 500))


def _coerce_bool_flag(raw: Any, default: bool = False) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return raw != 0
    try:
        text = str(raw).strip().lower()
    except Exception:
        return default
    if text in ("1", "true", "yes", "on", "y"):
        return True
    if text in ("0", "false", "no", "off", "n"):
        return False
    return default


async def count_exploration_post_urls(
    db_name: Optional[str] = None,
    chat_bot_id: Optional[str] = None,
    target_domains: Optional[List[str]] = None,
    contents_url: Optional[Union[str, List[str]]] = None,
    scope_path_prefix: Optional[str] = None,
) -> int:
    """Return the simple UI exploration count: all exploration rows whose type is post."""
    condition = ""
    legacy_condition = ""
    try:
        final_domains = extract_scope_identities(target_domains)
        if not final_domains and contents_url:
            domain = extract_scope_host(contents_url)
            if domain:
                final_domains = [domain]
        effective_prefix = normalize_scope_path_prefix(scope_path_prefix)
        if not effective_prefix and contents_url:
            effective_prefix = extract_service_scope_path_prefix(contents_url)
        query_conditions = build_exploration_conditions(
            ExplorationQuerySpec(
                chat_bot_id=chat_bot_id,
                target_domains=list(final_domains or []),
                path_prefix=effective_prefix,
                include_empty_type=False,
                dedupe_urls=True,
                require_active=True,
            )
        )
        condition = query_conditions.condition
        legacy_condition = query_conditions.legacy_condition
    except Exception as scope_exc:
        logger.debug(
            "[START_URLS_TRACE] exploration post count scope skipped | db=%s chat_bot_id=%s contents_url=%s prefix=%s err=%s",
            db_name,
            chat_bot_id,
            contents_url,
            scope_path_prefix,
            scope_exc,
        )
        query_conditions = build_exploration_conditions(
            ExplorationQuerySpec(
                chat_bot_id=chat_bot_id,
                include_empty_type=False,
                dedupe_urls=True,
                require_active=True,
            )
        )
        condition = query_conditions.condition
        legacy_condition = query_conditions.legacy_condition
    try:
        rows = await maria_select_data(
            EXPLORATION_TABLE,
            columns="COUNT(*) AS cnt",
            condition=condition,
            dbname=db_name,
        )
        if rows and isinstance(rows, list):
            row = rows[0] or {}
            return int(row.get("cnt") or row.get("COUNT(*)") or 0)
    except Exception as exc:
        if legacy_condition and _should_fallback_to_legacy_exploration_condition(exc):
            logger.warning(
                "[START_URLS_TRACE] exploration count filter columns unavailable -> fallback to legacy condition | db=%s chat_bot_id=%s err=%s",
                db_name,
                chat_bot_id,
                exc,
            )
            try:
                rows = await maria_select_data(
                    EXPLORATION_TABLE,
                    columns="COUNT(*) AS cnt",
                    condition=legacy_condition,
                    dbname=db_name,
                )
                if rows and isinstance(rows, list):
                    row = rows[0] or {}
                    return int(row.get("cnt") or row.get("COUNT(*)") or 0)
            except Exception as legacy_exc:
                logger.warning(
                    "[START_URLS_TRACE] legacy exploration post count failed | db=%s chat_bot_id=%s prefix=%s err=%s",
                    db_name,
                    chat_bot_id,
                    scope_path_prefix,
                    legacy_exc,
                )
                return 0
        logger.warning(
            "[START_URLS_TRACE] exploration post count failed | db=%s chat_bot_id=%s prefix=%s err=%s",
            db_name,
            chat_bot_id,
            scope_path_prefix,
            exc,
        )
    return 0


def _temporary_post_url_is_detail_candidate(url: str) -> bool:
    """빈 type URL을 CATEGORY 패턴으로 임시 post 승격할 때 목록 URL은 제외한다."""
    try:
        parsed = urlparse(ensure_url_scheme(str(url or "").strip()))
    except Exception:
        return False
    path = (parsed.path or "").lower()
    query = parsed.query or ""
    if not path:
        return False
    if url_query_suggests_board_article_detail(url):
        return True

    list_path_tokens = (
        "list.do",
        "list.asp",
        "list.php",
        "list.jsp",
        "/list/",
        "selectbbslist",
        "selectbbsnttlist",
        "boardlist",
        "bbslist",
    )
    if any(token in path for token in list_path_tokens):
        return False

    detail_path_tokens = (
        "view.do",
        "view.asp",
        "view.php",
        "view.jsp",
        "detail.do",
        "detail.asp",
        "detail.php",
        "detail.jsp",
        "read.do",
        "read.asp",
        "read.php",
        "read.jsp",
        "/view/",
        "/detail/",
        "/read/",
    )
    if any(token in path for token in detail_path_tokens):
        return bool(query)
    return False


async def mark_exploration_url_as_post_for_temporary_category_match(
    *,
    db_name: Optional[str],
    chat_bot_id: Optional[str],
    url: str,
    raw_url: Optional[str] = None,
    source: str = "board",
) -> bool:
    """CATEGORY url/query rules temporarily promoted an empty-type exploration URL to post."""
    dbn = str(db_name or "").strip()
    cid = str(chat_bot_id or "").strip()
    normalized_url = str(url or "").strip()
    original_url = str(raw_url or "").strip()
    if not (dbn and cid and normalized_url):
        return False

    params: List[Any]
    url_condition = "`url` = %s"
    params = ["post", cid, normalized_url]
    if original_url and original_url != normalized_url:
        url_condition = "(`url` = %s OR `url` = %s)"
        params = ["post", cid, normalized_url, original_url]
    query = (
        "UPDATE ASADAL_CRAWLING_EXPLORATION "
        "SET `type` = %s "
        "WHERE chat_bot_id = %s "
        f"AND {url_condition} "
        "AND COALESCE(TRIM(CAST(`type` AS CHAR)), '') = ''"
    )
    try:
        await maria_execute_query(query, tuple(params), dbname=dbn)
        logger.info(
            "[START_URLS_TEMP_POST] exploration type updated | source=%s db=%s chat_bot_id=%s url=%s",
            source,
            dbn,
            cid,
            normalized_url[:220],
        )
        return True
    except Exception as exc:
        logger.warning(
            "[START_URLS_TEMP_POST] exploration type update failed | source=%s db=%s chat_bot_id=%s url=%s err=%s",
            source,
            dbn,
            cid,
            normalized_url[:220],
            exc,
        )
        return False


def _resolve_stream_matched_rules_only_kwargs(kwargs: Dict[str, Any], *, default: bool = False) -> bool:
    mode = kwargs.get("category_start_urls_mode")
    if mode is None:
        mode = kwargs.get("category_pattern_mode")
    if mode is None:
        mode = kwargs.get("start_urls_category_mode")
    if mode is not None:
        mode_text = str(mode).strip().lower()
        if mode_text in {"all", "all_post", "all_posts", "post_all", "ignore", "ignore_category", "off", "disabled"}:
            return False
        if mode_text in {"category", "category_patterns", "pattern", "patterns", "matched", "matched_rules", "on", "enabled"}:
            return True

    for key in (
        "ignore_category_patterns",
        "ignore_category_url_patterns",
        "disable_category_patterns",
        "disable_category_url_patterns",
        "fetch_all_post_urls",
        "all_post_start_urls",
    ):
        if key in kwargs:
            return not _coerce_bool_flag(kwargs.get(key), default=False)

    for key in (
        "stream_matched_rules_only",
        "use_category_url_patterns",
        "use_category_patterns",
        "category_pattern_url_enabled",
        "category_pattern_filter_enabled",
        "category_url_pattern_filter_enabled",
        "category_start_urls_enabled",
    ):
        if key in kwargs:
            return _coerce_bool_flag(kwargs.get(key), default=default)

    return default


def _parse_target_date_range(target_date: Any) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(target_date, list) or len(target_date) < 2:
        return None, None

    start_raw = str(target_date[0] or "").strip()
    end_raw = str(target_date[1] or "").strip()
    if not start_raw or not end_raw:
        return None, None

    for fmt in ("%Y-%m-%d", "%y-%m-%d"):
        try:
            start_dt = datetime.strptime(start_raw, fmt)
            end_dt = datetime.strptime(end_raw, fmt)
            return start_dt.date().isoformat(), end_dt.date().isoformat()
        except Exception:
            continue
    return None, None


async def _get_table_columns_lower(db_name: Optional[str], table_name: str) -> set[str]:
    cache_key = (str(db_name or "").strip(), str(table_name or "").strip().lower())
    cached = _TABLE_COLUMNS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    if not cache_key[0] or not cache_key[1]:
        _TABLE_COLUMNS_CACHE[cache_key] = set()
        return set()

    try:
        rows = await maria_execute_query(
            """
            SELECT LOWER(column_name) AS column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND LOWER(table_name) = LOWER(%s)
            """,
            (cache_key[0], table_name),
            fetch=True,
            dbname=db_name,
        )
    except Exception:
        rows = []

    columns: set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        name = str(row.get("column_name") or "").strip().lower()
        if name:
            columns.add(name)

    _TABLE_COLUMNS_CACHE[cache_key] = columns
    return columns


async def _resolve_exploration_date_column(db_name: Optional[str], table_name: str) -> Optional[str]:
    columns = await _get_table_columns_lower(db_name, table_name)
    for candidate in _EXPLORATION_DATE_COLUMN_CANDIDATES:
        if candidate in columns:
            return candidate
    return None


def _build_exploration_date_range_condition(
    column_name: str,
    *,
    start_date_iso: Optional[str],
    end_date_iso: Optional[str],
) -> str:
    if not column_name or not start_date_iso or not end_date_iso:
        return ""

    quoted = f"`{column_name}`"
    normalized = (
        "REPLACE(REPLACE("
        f"SUBSTRING_INDEX(TRIM(CAST(COALESCE({quoted}, '') AS CHAR)), ' ', 1)"
        ", '.', '-'), '/', '-')"
    )
    date_expr = f"STR_TO_DATE({normalized}, '%Y-%m-%d')"
    return (
        f"{date_expr} IS NOT NULL "
        f"AND {date_expr} >= STR_TO_DATE('{start_date_iso}', '%Y-%m-%d') "
        f"AND {date_expr} <= STR_TO_DATE('{end_date_iso}', '%Y-%m-%d')"
    )


def _preview_debug_text(raw: Any, *, limit: int = 140) -> str:
    try:
        text = str(raw if raw is not None else "").strip()
    except Exception:
        text = ""
    if len(text) <= max(0, int(limit)):
        return text
    return text[: max(0, int(limit))] + "..."


def _normalize_rule_sequence(raw: Any) -> List[Any]:
    """
    url_pattern.include / exclude 등 DB·JSON에서 온 값이 리스트가 아니거나 타입이 뒤섞여 있어도
    순회 가능한 형태로 맞춘다. 변환 불가 시 빈 리스트.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, (str, bytes)):
        s = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        s = (s or "").strip()
        return [s] if s else []
    if isinstance(raw, dict):
        return [raw]
    try:
        if isinstance(raw, (tuple, set)):
            return list(raw)
    except Exception:
        pass
    try:
        return [raw]
    except Exception:
        return []


def _get_rule_entries(filters_obj: Optional[Dict[str, Any]]) -> List[Any]:
    if not isinstance(filters_obj, dict):
        return []
    rules = _normalize_rule_sequence(filters_obj.get("rules"))
    if rules:
        expanded: List[Any] = []
        for rule in rules:
            if not isinstance(rule, dict):
                expanded.append(rule)
                continue
            if rule.get("_strict_url_query"):
                expanded.append(rule)
                continue

            raw_url = None
            for key in ("url", "urls"):
                if key in rule:
                    raw_url = rule.get(key)
                    break
            raw_query = None
            for key in ("query", "queries", "querys", "query_list", "queryList", "params", "keys"):
                if key in rule:
                    raw_query = rule.get(key)
                    break

            if raw_url is None and raw_query is None:
                expanded.append(rule)
                continue

            base_rule = {
                key: value
                for key, value in rule.items()
                if key not in {"url", "urls", "query", "queries", "querys", "query_list", "queryList", "params", "keys"}
            }
            rule_entries = _normalize_category_rule_entries(raw_url, raw_query)
            if not rule_entries:
                expanded.append(rule)
                continue
            for entry in rule_entries:
                item = dict(base_rule)
                url_text = str(entry.get("url") or "").strip()
                query_keys = _unique_preserve_order_str(
                    [str(v or "").strip() for v in (entry.get("query_keys") or [])]
                )
                if url_text:
                    item["url"] = url_text
                if query_keys:
                    item["query"] = query_keys
                if item.get("url") or item.get("query"):
                    expanded.append(item)
        return expanded
    if any(key in filters_obj for key in ("url", "urls", "query", "queries", "querys", "query_list", "queryList", "params", "keys")):
        entries: List[Dict[str, Any]] = []
        for entry in _normalize_category_url_entries(filters_obj):
            item: Dict[str, Any] = {"url": str(entry.get("url") or "").strip()}
            query_keys = _unique_preserve_order_str(
                [str(v or "").strip() for v in (entry.get("query_keys") or [])]
            )
            if query_keys:
                item["query"] = query_keys
            if item.get("url") or item.get("query"):
                entries.append(item)
        return entries
    return []


def _summarize_rule_entries(filters_obj: Optional[Dict[str, Any]], *, limit: int = 5) -> List[Dict[str, Any]]:
    summary: List[Dict[str, Any]] = []
    for item in _get_rule_entries(filters_obj)[: max(0, int(limit))]:
        try:
            if isinstance(item, dict):
                summary.append(
                    {
                        "url": str(item.get("url") or "").strip(),
                        "query_keys": list(_extract_query_keys_from_rule_item(item)),
                        "cate1": str(item.get("cate1") or "").strip(),
                        "cate2": str(item.get("cate2") or "").strip(),
                    }
                )
            else:
                text = str(item or "").strip()
                if text:
                    summary.append({"url": text, "query_keys": [], "cate1": "", "cate2": ""})
        except Exception:
            continue
    return summary


def _rule_item_debug_parts(item: Any) -> Tuple[str, List[str]]:
    try:
        if isinstance(item, dict):
            url_text = str(item.get("url") or "").strip()
            query_keys = list(_extract_query_keys_from_rule_item(item))
            return url_text, query_keys
        url_text = str(item or "").strip()
        return url_text, []
    except Exception:
        return "", []


def _rule_item_shape(item: Any) -> str:
    url_text, query_keys = _rule_item_debug_parts(item)
    if url_text and query_keys:
        return "url+query"
    if url_text:
        return "url_only"
    if query_keys:
        return "query_only"
    return "empty"


def _get_query_exact_category_index(filters_obj: Dict[str, Any], rule_list: List[Any]) -> Dict[str, Tuple[str, str, int]]:
    cached = filters_obj.get("_query_exact_category_index")
    if isinstance(cached, dict):
        return cached
    global_c1 = _global_cate_rule_list(filters_obj, "cate1")
    global_c2 = _global_cate_rule_list(filters_obj, "cate2")
    index: Dict[str, Tuple[str, str, int]] = {}
    for idx, item in enumerate(rule_list or []):
        if not isinstance(item, dict):
            continue
        keyword = str(item.get("url") or "").strip()
        if keyword:
            continue
        query_keys = _extract_query_keys_from_rule_item(item)
        if not query_keys:
            continue
        c1 = _sanitize_category_value(item.get("cate1") or _global_cate_value_at(global_c1, idx))
        c2 = _sanitize_category_value(item.get("cate2") or _global_cate_value_at(global_c2, idx))
        if not (c1 or c2):
            continue
        for token in query_keys:
            text = str(token or "").strip().lower()
            if not text or "=" not in text:
                continue
            try:
                pairs = parse_qsl(text, keep_blank_values=True)
            except Exception:
                pairs = []
            for key, value in pairs:
                key_text = str(key or "").strip().lower()
                value_text = str(value or "").strip().lower()
                if not key_text or not value_text:
                    continue
                for alias in _query_key_aliases(key_text):
                    index.setdefault(f"{alias}={value_text}", (c1, c2, idx))
    try:
        filters_obj["_query_exact_category_index"] = index
    except Exception:
        pass
    return index


def _rule_shape_debug_summary(filters_obj: Optional[Dict[str, Any]], *, limit: int = 5) -> Dict[str, Any]:
    counts = {
        "url_only": 0,
        "query_only": 0,
        "url+query": 0,
        "empty": 0,
    }
    samples: List[Dict[str, Any]] = []
    for item in _get_rule_entries(filters_obj):
        shape = _rule_item_shape(item)
        counts[shape] = counts.get(shape, 0) + 1
        if len(samples) >= max(0, int(limit)):
            continue
        url_text, query_keys = _rule_item_debug_parts(item)
        samples.append(
            {
                "shape": shape,
                "url": url_text,
                "query_keys": query_keys,
            }
        )
    return {"counts": counts, "samples": samples}


def _global_cate_rule_list(filters_obj: Dict[str, Any], field: str) -> List[Any]:
    """
    url_pattern JSON 최상위 cate1 / cate2.
    - 권장: { "cate1": { "rules": ["코드1", "코드2"] } } 처럼 rules 배열을 rules URL 과 같은 길이로
    - UI가 자주 넣는 형태: "cate1": "단일코드" 문자열 → [단일코드] 로 두고 모든 rules 에 전파(_global_cate_value_at)
    """
    try:
        block = filters_obj.get(field)
        if isinstance(block, dict):
            inner = block.get("rules", [])
            return _normalize_rule_sequence(inner)
        if isinstance(block, list):
            return block
        if block is not None and str(block).strip():
            return [str(block).strip()]
    except Exception:
        pass
    return []


def _global_cate_value_at(global_list: List[Any], idx: int) -> str:
    """rules 인덱스 idx 에 대응하는 전역 분류값. 배열이 1개뿐이면 모든 idx 에 그 값을 쓴다."""
    if not global_list:
        return ""
    if idx < len(global_list):
        return _sanitize_category_value(global_list[idx])
    if len(global_list) == 1:
        return _sanitize_category_value(global_list[0])
    return ""


# scripts/compare_url_pattern.py 와 동일 — cate_match 적용 여부 판정용
_PATTERN_MENU_KEYS = {
    'bbsNo', 'bbsId', 'board_id', 'bo_table', 'key', 'mid', 'menuNo', 'categoryId',
    'ctgryCd', 'ctgry_cd', 'categoryCd', 'category_cd', 'mnNo', 'menuId',
    'cid',
    'bbsno', 'bbsid', 'boardid', 'board_id', 'bo_table', 'menuno', 'categoryid',
    'ctgrycd', 'categorycd', 'mnno', 'menuid',
    'bbsCode', 'bbscode', 'bbs_code', 'q_bbsCode', 'q_bbscode', 'q_bbs_code',
}
_MENU_QUERY_KEY_ALIASES = {
    "key": frozenset({"key", "menuno", "menuid", "mnno"}),
    "menuno": frozenset({"key", "menuno", "menuid", "mnno"}),
    "menuid": frozenset({"key", "menuno", "menuid", "mnno"}),
    "mnno": frozenset({"key", "menuno", "menuid", "mnno"}),
    "bbscode": frozenset({"bbscode", "bbscode", "bbs_code", "q_bbscode", "q_bbs_code"}),
    "bbs_code": frozenset({"bbscode", "bbs_code", "q_bbscode", "q_bbs_code"}),
    "q_bbscode": frozenset({"bbscode", "bbs_code", "q_bbscode", "q_bbs_code"}),
    "q_bbs_code": frozenset({"bbscode", "bbs_code", "q_bbscode", "q_bbs_code"}),
}
_PATTERN_ARTICLE_KEYS = {
    'nttNo', 'nttId', 'wr_id', 'idx', 'seq', 'no', 'articleNo', 'progrmNo', 'progrmSn', 'progrm_sn',
    'searchLctreKey', 'nmmIdx',
}
_BOARD_CODE_TOKEN_RE = re.compile(r"^(?:[A-Za-z]\d{6,}|[A-Za-z]+_\d+|BBSMSTR_[A-Za-z0-9_]+)$", re.IGNORECASE)
# scripts/check_url_categories.py 와 동일 — 동일 경로에서 스크립트명만 다른 경우(목록/상세 등)
_BOARD_SCRIPT_FILENAMES = frozenset({
    "list.do", "view.do", "selectBbsNttView.do", "selectBbsNttList.do",
    "read.do", "index.do", "allView.do", "ready.do",
    "eMinwonList.do", "eMinwonView.do", "eMiryangMinwonList.do", "eMiryangMinwonView.do",
})

# 광진구: /portal/bbs/B0000039/, /health/bbs/B0000039/ 처럼
# /bbs/{게시판코드}/ 형태일 때만 게시판 코드를 읽는다.
# querystring까지 삼켜서 `view.do?key=...` 같은 잘못된 토큰을 만들지 않도록
# 다음 슬래시가 있는 경우에만 매칭한다.
_BBS_PATH_SEGMENT_RE = re.compile(r"/bbs/([^/?#]+)/", re.IGNORECASE)


def _bbs_board_id_from_text(s: str) -> Optional[str]:
    try:
        m = _BBS_PATH_SEGMENT_RE.search(s or "")
        return m.group(1).lower() if m else None
    except Exception:
        return None


def _url_host_is_gwangjin(url: str) -> bool:
    try:
        netloc = (urlparse(url).netloc or "").lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        if netloc.endswith("gwangjin.go.kr") or netloc == "gwangjin.go.kr":
            return True
    except Exception:
        pass
    return "gwangjin.go.kr" in (url or "").lower()


def _gwangjin_unified_bbs_match(pattern_keyword: str, target_url: str) -> bool:
    """
    대상 URL이 gwangjin이고, 패턴·대상 양쪽에서 /bbs/{게시판코드}를 읽을 수 있으며
    코드가 같으면 True(portal/health 등 상위 경로 무시).
    """
    if not pattern_keyword or not target_url:
        return False
    if not _url_host_is_gwangjin(target_url):
        return False
    bid_t = _bbs_board_id_from_text(target_url)
    bid_p = _bbs_board_id_from_text(pattern_keyword)
    if not bid_t or not bid_p:
        return False
    return bid_t == bid_p


_GANGNAM_BOARD_LIST_PATH_RE = re.compile(
    r"(?P<prefix>/.+?/board/[^/]+)/list\.do\s*$",
    re.IGNORECASE,
)
_GANGNAM_BOARD_VIEW_PATH_RE = re.compile(
    r"(?P<prefix>/.+?/board/[^/]+)/\d+/view\.do\s*$",
    re.IGNORECASE,
)

_MAPO_NPORTAL_LIST_PATH_RE = re.compile(
    r"(?P<prefix>/site/main/nportal)/list/?\s*$",
    re.IGNORECASE,
)
_MAPO_NPORTAL_DETAIL_PATH_RE = re.compile(
    r"(?P<prefix>/site/main/nportal)/detail/?\s*$",
    re.IGNORECASE,
)
_GM_NFTC_BBS_SCRIPT_NAMES = frozenset({
    "bd_selectnftcbbslist.do",
    "bd_selectnftcbbsdetail.do",
})


def _url_host_is_gangnam_family(url: str) -> bool:
    """gangnam.go.kr 및 *.gangnam.go.kr."""
    if not url:
        return False
    try:
        raw = (url or "").strip()
        p = urlparse(raw if "://" in raw else f"https://{raw}")
        h = (p.netloc or "").lower().split("@")[-1].split(":")[0]
        if h.startswith("www."):
            h = h[4:]
        return h == "gangnam.go.kr" or h.endswith(".gangnam.go.kr")
    except Exception:
        return "gangnam.go.kr" in (url or "").lower()


def _gangnam_board_list_detail_cate_match(pattern_keyword: str, target_url: str) -> bool:
    """
    강남구: .../board/{코드}/list.do ↔ .../board/{코드}/{글번호}/view.do (및 상세↔상세 동일 프리픽스).
    include 가 목록 URL만 있을 때 상세 URL에 substring 매칭이 깨지는 문제 보완.
    """
    if not pattern_keyword or not target_url:
        return False
    if not _url_host_is_gangnam_family(pattern_keyword) or not _url_host_is_gangnam_family(target_url):
        return False
    try:
        pp = (urlparse(pattern_keyword).path or "").replace("\\", "/").strip()
        tp = (urlparse(target_url).path or "").replace("\\", "/").strip()
        ml = _GANGNAM_BOARD_LIST_PATH_RE.search(pp)
        mv = _GANGNAM_BOARD_VIEW_PATH_RE.search(tp)
        if ml and mv:
            return ml.group("prefix").rstrip("/") == mv.group("prefix").rstrip("/")
        mp = _GANGNAM_BOARD_VIEW_PATH_RE.search(pp)
        mt = _GANGNAM_BOARD_VIEW_PATH_RE.search(tp)
        if mp and mt:
            return mp.group("prefix").rstrip("/") == mt.group("prefix").rstrip("/")
    except Exception:
        return False
    return False


def _url_host_is_mapo(url: str) -> bool:
    if not url:
        return False
    try:
        raw = (url or "").strip()
        p = urlparse(raw if "://" in raw else f"https://{raw}")
        h = (p.netloc or "").lower().split("@")[-1].split(":")[0]
        if h.startswith("www."):
            h = h[4:]
        return h == "mapo.go.kr" or h.endswith(".mapo.go.kr")
    except Exception:
        return "mapo.go.kr" in (url or "").lower()


def _mapo_nportal_list_detail_cate_match(pattern_keyword: str, target_url: str) -> bool:
    """
    마포구청 신형 게시판:
    - /site/main/nPortal/list
    - /site/main/nPortal/detail?bcId=...
    관리자가 목록 URL을 include 에 넣어도 상세 URL에 같은 분류를 적용한다.
    """
    if not pattern_keyword or not target_url:
        return False
    if not _url_host_is_mapo(pattern_keyword) or not _url_host_is_mapo(target_url):
        return False
    try:
        pp = (urlparse(pattern_keyword).path or "").replace("\\", "/").strip()
        tp = (urlparse(target_url).path or "").replace("\\", "/").strip()
        pm = _MAPO_NPORTAL_LIST_PATH_RE.search(pp) or _MAPO_NPORTAL_DETAIL_PATH_RE.search(pp)
        tm = _MAPO_NPORTAL_LIST_PATH_RE.search(tp) or _MAPO_NPORTAL_DETAIL_PATH_RE.search(tp)
        if pm and tm:
            return pm.group("prefix").rstrip("/") == tm.group("prefix").rstrip("/")
    except Exception:
        return False
    return False


def _url_host_is_gm(url: str) -> bool:
    if not url:
        return False
    try:
        raw = (url or "").strip()
        p = urlparse(raw if "://" in raw else f"https://{raw}")
        h = (p.netloc or "").lower().split("@")[-1].split(":")[0]
        if h.startswith("www."):
            h = h[4:]
        return h == "gm.go.kr" or h.endswith(".gm.go.kr")
    except Exception:
        return "gm.go.kr" in (url or "").lower()


def _gm_nftc_list_detail_cate_match(pattern_keyword: str, target_url: str) -> bool:
    if not pattern_keyword or not target_url:
        return False
    if not _url_host_is_gm(pattern_keyword) or not _url_host_is_gm(target_url):
        return False
    try:
        pp = urlparse(pattern_keyword)
        tp = urlparse(target_url)
        p_name = os.path.basename(pp.path or "").lower()
        t_name = os.path.basename(tp.path or "").lower()
        if p_name not in _GM_NFTC_BBS_SCRIPT_NAMES or t_name not in _GM_NFTC_BBS_SCRIPT_NAMES:
            return False
        p_params = {str(k or "").strip().lower(): str(v or "").strip().lower() for k, v in parse_qsl(pp.query, keep_blank_values=True)}
        t_params = {str(k or "").strip().lower(): str(v or "").strip().lower() for k, v in parse_qsl(tp.query, keep_blank_values=True)}
        p_code = p_params.get("q_nftcbbscode")
        t_code = t_params.get("q_nftcbbscode")
        if p_code and t_code:
            return p_code == t_code
        return True
    except Exception:
        return False


def _board_script_names_equivalent(target_fn: str, pattern_fn: str) -> bool:
    if not target_fn or not pattern_fn:
        return False
    target_name = str(target_fn or "").strip().lower()
    pattern_name = str(pattern_fn or "").strip().lower()
    board_script_names = {str(name).lower() for name in _BOARD_SCRIPT_FILENAMES}
    if target_name == pattern_name:
        return True
    return target_name in board_script_names and pattern_name in board_script_names


def _structural_match_include_to_target(
    struct: Dict[str, Any],
    *,
    target_parsed,
    target_params: Dict[str, str],
    target_keys: set,
    target_base: str,
) -> bool:
    """
    include 샘플 URL과 수집 대상 URL의 구조 매칭(정확 매칭).
    """
    t_dir = (os.path.dirname(target_parsed.path) or "").rstrip("/")
    p_dir = (struct.get("dir_path") or "").rstrip("/")
    if p_dir != t_dir:
        return False

    p_base = struct.get("base_name") or ""
    if not _board_script_names_equivalent(target_base, p_base):
        return False

    params = struct.get("params") or {}
    for m_key in _PATTERN_MENU_KEYS:
        if m_key in params and target_params.get(m_key) != params[m_key]:
            return False

    pattern_has_article = any(k in params for k in _PATTERN_ARTICLE_KEYS)
    if pattern_has_article and not any(a_key in target_keys for a_key in _PATTERN_ARTICLE_KEYS):
        return False

    return True


def _structural_match_include_to_target_relaxed_menu(
    struct: Dict[str, Any],
    *,
    target_parsed,
    target_params: Dict[str, str],
    target_keys: set,
    target_base: str,
) -> bool:
    """
    include 샘플 URL과 수집 대상 URL의 구조 매칭(완화 매칭).
    - dir_path / script filename 동치는 동일하게 요구
    - MENU_KEYS는 target에 존재하는 키만 값 비교
    """
    t_dir = (os.path.dirname(target_parsed.path) or "").rstrip("/")
    p_dir = (struct.get("dir_path") or "").rstrip("/")
    if p_dir != t_dir:
        return False

    p_base = struct.get("base_name") or ""
    if not _board_script_names_equivalent(target_base, p_base):
        return False

    params = struct.get("params") or {}
    for m_key in _PATTERN_MENU_KEYS:
        if m_key in params and m_key in target_params:
            if target_params.get(m_key) != params[m_key]:
                return False

    pattern_has_article = any(k in params for k in _PATTERN_ARTICLE_KEYS)
    if pattern_has_article and not any(a_key in target_keys for a_key in _PATTERN_ARTICLE_KEYS):
        return False

    return True


def _strip_leading_www_after_scheme(s: str) -> str:
    """스킴 직후 www. 제거 — `https://www.a`↔`https://a` 키워드 매칭 통일"""
    u = (s or "").strip().lower()
    if "://www." in u:
        return u.replace("://www.", "://", 1)
    return u


def _keyword_match_to_target(pattern_keyword: str, target_url: str) -> bool:
    """단순 키워드 포함 여부로 매칭 판정(호스트 www 무시 + 광진 portal/health 등 동일 B코드 통합)"""
    if not pattern_keyword or not target_url:
        return False
    try:
        pk = str(pattern_keyword).strip().lower()
        tu = str(target_url).strip().lower()
        if pk in tu:
            return True
        pk_n = _strip_leading_www_after_scheme(pk)
        tu_n = _strip_leading_www_after_scheme(tu)
        if pk_n in tu_n:
            return True
        if _gwangjin_unified_bbs_match(pattern_keyword, target_url):
            return True
        if _gangnam_board_list_detail_cate_match(pattern_keyword, target_url):
            return True
        if _mapo_nportal_list_detail_cate_match(pattern_keyword, target_url):
            return True
        if _gm_nftc_list_detail_cate_match(pattern_keyword, target_url):
            return True
        return False
    except Exception:
        return False

def urls_pattern_match_for_cate(pattern_url: str, target_url: str) -> bool:
    """
    두 URL이 같은 패턴 계열인지(전체 URL 문자열 비교 아님 — 구조 매칭).
    ``_structural_match_include_to_target`` 과 동일한 기준으로 단일 호출용 래퍼.
    """
    try:
        if _gwangjin_unified_bbs_match(pattern_url, target_url):
            return True
        if _gangnam_board_list_detail_cate_match(pattern_url, target_url):
            return True
        if _mapo_nportal_list_detail_cate_match(pattern_url, target_url):
            return True
        if _gm_nftc_list_detail_cate_match(pattern_url, target_url):
            return True
        p1 = urlparse(pattern_url)
        p2 = urlparse(target_url)
        struct = {
            "dir_path": (os.path.dirname(p1.path) or "").rstrip("/"),
            "base_name": os.path.basename(p1.path),
            "params": get_url_params_dict(pattern_url),
        }
        return _structural_match_include_to_target(
            struct,
            target_parsed=p2,
            target_params=get_url_params_dict(target_url),
            target_keys=get_url_params_set(target_url),
            target_base=os.path.basename(p2.path),
        )
    except Exception:
        return False


def url_matches_exclude_patterns(target_url: str, exclude_patterns: Optional[List[str]]) -> bool:
    """
    exclude 패턴이 target_url 과 매칭되면 True를 반환한다.

    구조 매칭과 키워드 매칭을 함께 사용해서 include와 같은 계열의 패턴도
    탐색 단계에서 선제 제외할 수 있게 한다.
    """
    if not target_url or not exclude_patterns:
        return False
    for raw in exclude_patterns or []:
        try:
            pattern = str(raw or "").strip()
        except Exception:
            pattern = ""
        if not pattern:
            continue
        try:
            if urls_pattern_match_for_cate(pattern, target_url):
                return True
        except Exception:
            pass
        try:
            if _keyword_match_to_target(pattern, target_url):
                return True
        except Exception:
            pass
    return False


def get_url_params_dict(url: str) -> Dict[str, str]:
    """Return query parameters from a URL after normalizing escaped slashes."""
    try:
        if not url: return {}
        u = url.replace("\\/", "/").strip()
        parsed = urlparse(u)
        return dict(parse_qsl(parsed.query))
    except Exception:
        return {}


def _learn_list_column_from_row(row: Any, col_name: str) -> Any:
    """LEARN_LIST 행에서 단일 컬럼 값만 꺼낸다(비어 있지 않을 때). 대소문자 키 대응."""
    if not isinstance(row, dict):
        return None
    if col_name in row:
        v = row.get(col_name)
        if v is not None and (not isinstance(v, str) or v.strip()):
            return v
    lk = col_name.lower()
    lower = {str(k).lower(): v for k, v in row.items()}
    v = lower.get(lk)
    if v is not None and (not isinstance(v, str) or v.strip()):
        return v
    return None


def _raw_json_has_nonempty_rules(raw_val: Any) -> bool:
    """원본이 JSON이고 _pattern_json_has_nonempty_include 를 만족하는지."""
    if raw_val is None:
        return False
    try:
        if isinstance(raw_val, (bytes, bytearray)):
            raw_val = raw_val.decode("utf-8", errors="replace")
        data = json.loads(raw_val) if isinstance(raw_val, str) else raw_val
        return _pattern_json_has_nonempty_rules(data)
    except Exception:
        return False


def _learn_list_row_id(row: Any) -> int:
    """LEARN_LIST 행의 숫자 id (정렬·최신 행 선택용)."""
    if not isinstance(row, dict):
        return 0
    for k in ("id", "ID"):
        if k in row and row[k] is not None:
            try:
                return int(row[k])
            except (TypeError, ValueError):
                pass
    lower = {str(k).lower(): v for k, v in row.items()}
    v = lower.get("id")
    if v is not None:
        try:
            return int(v)
        except (TypeError, ValueError):
            pass
    return 0


def _normalize_contents_url_for_match(raw_url: Any) -> str:
    try:
        txt = ensure_url_scheme(str(raw_url or "").strip())
    except Exception:
        txt = str(raw_url or "").strip()
    if not txt:
        return ""
    try:
        p = urlparse(txt)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = p.path or ""
        # 온통청년 게시글 상세는 목록 컨텍스트 쿼리(curPageNum, srchParamEtc*)와 무관하게
        # 동일 글 경로(/bbs03View/{board}/{post})만 같으면 exact match로 취급한다.
        if host == "youthcenter.go.kr" and re.search(r"/bbs\d+view/\d+/\d+$", path, re.I):
            return urlunparse((p.scheme or "https", host, path, "", "", ""))
        return canonicalize_url_for_dedup(txt) or txt
    except Exception:
        return txt


def _valid_category_scope_url(raw_url: Any) -> Optional[str]:
    try:
        text = str(raw_url or "").strip()
    except Exception:
        return None
    if not text or text.lower() in {"http", "https", "http://", "https://"}:
        return None
    try:
        parsed = urlparse(ensure_url_scheme(text))
        if not parsed.netloc or "." not in parsed.netloc:
            return None
    except Exception:
        return None
    return text


def _should_skip_category_scope_filter(contents_url: Optional[str]) -> bool:
    try:
        raw = str(contents_url or "").strip()
    except Exception:
        raw = ""
    if not raw:
        return True
    try:
        parsed = urlparse(ensure_url_scheme(raw))
        path = str(parsed.path or "").strip().lower()
    except Exception:
        path = raw.lower()
    if not path or path == "/":
        return True
    return path.endswith(("/main.do", "/index.do", "/default.do"))


def _first_path_segment_from_pattern(pattern: Any) -> Optional[str]:
    try:
        raw = str(pattern or "").strip()
    except Exception:
        raw = ""
    if not raw:
        return None
    if "://" in raw or raw.startswith("//"):
        try:
            path = urlparse(ensure_url_scheme(raw)).path or ""
        except Exception:
            path = ""
    else:
        if "/" not in raw and "." not in raw:
            return None
        path = raw.split("?", 1)[0].strip()
    parts = [part for part in str(path or "").split("/") if part]
    if not parts:
        return None
    return str(parts[0] or "").strip().lower() or None


def _resolve_scope_path_prefix_from_patterns(
    contents_url: Optional[Union[str, List[str]]],
    pattern_keywords: Optional[List[str]],
) -> str:
    base_prefix = extract_service_scope_path_prefix(contents_url) or extract_scope_path_prefix(contents_url)
    if not base_prefix:
        return ""
    base_parts = [part for part in str(base_prefix or "").split("/") if part]
    if not base_parts:
        return ""
    base_first = str(base_parts[0] or "").strip().lower()
    keywords = [str(k or "").strip() for k in (pattern_keywords or []) if str(k or "").strip()]
    if not keywords:
        return base_prefix
    for keyword in keywords:
        seg = _first_path_segment_from_pattern(keyword)
        if not seg:
            continue
        if seg != base_first:
            return ""
    return base_prefix


def _resolve_preexplored_scope(
    *,
    target_domains: Optional[List[str]],
    contents_url: Optional[Union[str, List[str]]],
    use_rule_scope: bool,
    rule_patterns: Optional[List[str]] = None,
    explicit_path_prefix: Optional[str] = None,
) -> Tuple[List[str], str]:
    """
    게시판 start_urls 범위를 결정한다.

    - include 매칭을 사용할 때만 contents_url 기반 host/path_prefix를 적용한다.
    - include가 비었거나 matched_include_only가 꺼져 있으면 explicit target_domains만 존중한다.
    """
    final_domains = extract_scope_identities(target_domains)
    explicit_scope_path_prefix = normalize_scope_path_prefix(explicit_path_prefix)
    scope_path_prefix = explicit_scope_path_prefix
    if explicit_scope_path_prefix:
        if not final_domains and contents_url:
            domain = _extract_base_domain(contents_url)
            if domain:
                final_domains = [domain]
    elif use_rule_scope:
        if not final_domains and contents_url:
            domain = _extract_base_domain(contents_url)
            if domain:
                final_domains = [domain]
        scope_path_prefix = _resolve_scope_path_prefix_from_patterns(contents_url, rule_patterns)
    elif contents_url:
        domain = _extract_base_domain(contents_url)
        if domain and not final_domains:
            final_domains = [domain]
        scope_path_prefix = extract_service_scope_path_prefix(contents_url)
    return final_domains, scope_path_prefix


def _should_fallback_to_legacy_exploration_condition(exc: Exception) -> bool:
    try:
        msg = str(exc or "").strip().lower()
    except Exception:
        msg = ""
    if not msg:
        return False
    if "merge_status" not in msg and "is_active" not in msg:
        return False
    return any(
        token in msg
        for token in (
            "unknown column",
            "no such column",
            "doesn't exist",
            "does not exist",
            "invalid column",
            "1054",
        )
    )


def _learn_list_rows_newest_first(rows: Any) -> List[Any]:
    """id 기준 내림차순(최신 행 우선). SQL ORDER BY 보조용."""
    if not isinstance(rows, list) or not rows:
        return []
    try:
        return sorted(rows, key=_learn_list_row_id, reverse=True)
    except Exception:
        return list(rows)


def _pattern_json_has_nonempty_rules(data: Any) -> bool:
    """
    url_pattern JSON에 실제 쓸 만한 include 항목이 있는지.
    - mode가 명시적으로 exclude면 False (include 목록용 후보 아님).
    - mode 미지정이어도 include에 값이 있으면 True (DB에 mode 누락된 경우 대응).
    """
    if not isinstance(data, dict):
        return False
    mode = (data.get("mode") or "").strip().lower()
    if mode == "exclude":
        return False
    for item in _get_rule_entries(data):
        if isinstance(item, dict):
            u = item.get("url")
            if u is not None and str(u).strip():
                return True
            if _extract_query_keys_from_rule_item(item):
                return True
        elif isinstance(item, str) and item.strip():
            return True
        elif item is not None and str(item).strip():
            return True
    return False


def _raw_url_pattern_to_str(raw_val: Any) -> Optional[str]:
    if raw_val is None:
        return None
    if isinstance(raw_val, (bytes, bytearray)):
        try:
            return raw_val.decode("utf-8", errors="replace")
        except Exception:
            return None
    if isinstance(raw_val, str):
        return raw_val
    if isinstance(raw_val, (dict, list)):
        try:
            return json.dumps(raw_val, ensure_ascii=False)
        except Exception:
            return None
    return str(raw_val)


_CATEGORY_ROOT_DEFAULT_CODES: Dict[str, str] = {
    "homepage_learning": "AS1729062288",
}

_CATEGORY_ROOT_FALLBACK_NAMES: Dict[str, Tuple[str, ...]] = {
    "homepage_learning": ("홈페이지학습", "홈페이지 학습"),
}


def _sanitize_category_value(raw: Any) -> str:
    try:
        text = str(raw if raw is not None else "").strip()
    except Exception:
        text = ""
    if not text:
        return ""
    if text.lower() in {"undefined", "null", "none", "nan"}:
        return ""
    if "array" in text.lower():
        return ""
    return text


def _is_invalid_array_placeholder(raw: Any) -> bool:
    try:
        text = str(raw if raw is not None else "").strip()
    except Exception:
        text = ""
    if not text:
        return False
    normalized = text.lower()
    return normalized in {"array", "[array]", "(array)", "{array}"}


def _category_table_name_from_chat_bot_id(chat_bot_id: str) -> Optional[str]:
    try:
        raw = str(chat_bot_id or "").strip().split("-")[-1].strip()
    except Exception:
        raw = ""
    if not raw or not re.match(r"^[A-Za-z0-9]+$", raw):
        return None
    return f"ASADAL_{raw}_CATEGORY"


def _category_root_code_from_env(root_key: str) -> str:
    env_map = {
        "homepage_learning": "CATEGORY_SYNC_HOMEPAGE_ROOT_CODE",
    }
    env_name = env_map.get(str(root_key or "").strip(), "")
    if env_name:
        try:
            value = str(os.getenv(env_name, "") or "").strip()
            if value:
                return value
        except Exception:
            pass
    return _CATEGORY_ROOT_DEFAULT_CODES.get(str(root_key or "").strip(), "")


def _unique_preserve_order_str(values: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _split_query_text_fragments(text: str) -> List[str]:
    return _unique_preserve_order_str(
        [token.strip() for token in re.split(r"[,|&\s]+", str(text or "")) if token.strip()]
    )


def _extract_query_keys_from_value(raw: Any) -> List[str]:
    if raw is None:
        return []
    if _is_invalid_array_placeholder(raw):
        return []
    if isinstance(raw, dict):
        for key in ("query", "queries", "querys", "query_list", "queryList", "params", "keys"):
            if key in raw:
                return _extract_query_keys_from_value(raw.get(key))
        tokens: List[str] = []
        for key, value in raw.items():
            key_text = str(key or "").strip()
            if not key_text:
                continue
            if isinstance(value, (list, tuple, set)):
                for nested_value in value:
                    value_text = str(nested_value if nested_value is not None else "").strip()
                    tokens.append(f"{key_text}={value_text}" if value_text else key_text)
                continue
            if isinstance(value, dict):
                nested_tokens = _extract_query_keys_from_value(value)
                tokens.extend(nested_tokens or [key_text])
                continue
            value_text = str(value if value is not None else "").strip()
            tokens.append(f"{key_text}={value_text}" if value_text else key_text)
        return _unique_preserve_order_str(tokens)
    if isinstance(raw, (list, tuple, set)):
        out: List[str] = []
        for item in raw:
            out.extend(_extract_query_keys_from_value(item))
        return _unique_preserve_order_str(out)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        text = html.unescape(raw.strip())
        if not text:
            return []
        if text.startswith("?"):
            text = text[1:].strip()
            if not text:
                return []
        if "?" in text:
            try:
                parsed_query = urlparse(text).query
            except Exception:
                parsed_query = ""
            if not parsed_query and "?" in text:
                parsed_query = text.split("?", 1)[1].strip()
            if parsed_query and parsed_query != text:
                return _extract_query_keys_from_value(parsed_query)
        if text[:1] in ("{", "["):
            try:
                return _extract_query_keys_from_value(json.loads(text))
            except Exception:
                pass
        if "=" in text or "&" in text:
            tokens: List[str] = []
            try:
                for k, v in parse_qsl(text, keep_blank_values=True):
                    key = str(k or "").strip()
                    value = str(v or "").strip()
                    if not key:
                        continue
                    tokens.append(f"{key}={value}" if value else key)
            except Exception:
                pass
            tokens.extend(_split_query_text_fragments(text))
            return _unique_preserve_order_str(tokens)
        return _split_query_text_fragments(text)
    return []


def _extract_query_keys_from_rule_item(item: Any) -> List[str]:
    if not isinstance(item, dict):
        return []
    for key in ("query_keys", "query", "queries", "querys", "query_list", "queryList", "params", "keys"):
        if key in item:
            return _extract_query_keys_from_value(item.get(key))
    return []


def _query_key_name(token: str) -> str:
    text = str(token or "").strip()
    if not text:
        return ""
    return text.split("=", 1)[0].strip().lower()


def _query_key_aliases(key: str) -> frozenset[str]:
    key_text = str(key or "").strip().lower()
    if not key_text:
        return frozenset()
    return _MENU_QUERY_KEY_ALIASES.get(key_text, frozenset({key_text}))


def _query_token_alias_terms(token: str) -> List[str]:
    text = str(token or "").strip()
    if not text or "=" not in text:
        return []
    try:
        pairs = parse_qsl(text, keep_blank_values=True)
    except Exception:
        pairs = []
    terms: List[str] = []
    for key, value in pairs:
        key_text = str(key or "").strip()
        value_text = str(value or "").strip()
        if not key_text:
            continue
        for alias in _query_key_aliases(key_text):
            terms.append(f"{alias}={value_text}" if value_text else alias)
    return _unique_preserve_order_str(terms)


def _split_query_only_rule_groups(query_keys: List[str]) -> List[List[str]]:
    keys = _unique_preserve_order_str([str(v or "").strip() for v in (query_keys or [])])
    if len(keys) <= 1:
        return [keys] if keys else []
    return [[token] for token in keys]


def _target_url_token_context(target_url: str) -> Dict[str, Any]:
    try:
        parsed = urlparse(target_url or "")
        path = unquote(parsed.path or "").strip()
        parsed_query = (parsed.query or "").replace("&amp;", "&").replace("&#38;", "&").replace("&#x26;", "&")
        parsed_pairs = parse_qsl(parsed_query, keep_blank_values=True)
    except Exception:
        parsed = urlparse("")
        path = ""
        parsed_query = ""
        parsed_pairs = []
    path_lower = path.lower()
    path_tokens = {
        str(part or "").strip().lower()
        for part in re.split(r"[/?#&=]+", path_lower)
        if str(part or "").strip()
    }
    if path_lower:
        path_tokens.add(path_lower)
        path_tokens.add(path_lower.lstrip("/"))
    target_url_lower = unquote(str(target_url or "").strip()).lower()
    return {
        "parsed": parsed,
        "path": path,
        "path_tokens": path_tokens,
        "raw_query": parsed_query,
        "pairs": parsed_pairs,
        "url_lower": target_url_lower,
        "board_id": str(_bbs_board_id_from_text(target_url) or "").strip().lower(),
    }


def _target_url_matches_loose_query_token(
    *,
    token: str,
    context: Dict[str, Any],
) -> bool:
    token_text = html.unescape(str(token or "").strip().lower())
    if token_text.startswith("?"):
        token_text = token_text[1:].strip()
    if not token_text:
        return True
    path_tokens = set(context.get("path_tokens") or set())
    board_id = str(context.get("board_id") or "").strip().lower()
    url_lower = str(context.get("url_lower") or "").strip().lower()

    if token_text in path_tokens:
        return True
    if board_id and token_text == board_id:
        return True

    if "/" in token_text and len(token_text) >= 3:
        normalized_token = token_text.lstrip("/")
        return bool(normalized_token and normalized_token in url_lower)

    return False


def _target_url_matches_required_rule_tokens(target_url: str, required_tokens: List[str]) -> bool:
    if not required_tokens:
        return True
    context = _target_url_token_context(target_url)
    try:
        parsed_pairs = list(context.get("pairs") or [])
        target_keys = {
            str(k or "").strip().lower()
            for k, _ in parsed_pairs
        }
        target_pair_tokens = {
            f"{str(k or '').strip().lower()}={str(v or '').strip().lower()}"
            for k, v in parsed_pairs
            if str(k or "").strip()
        }
        target_values_by_key: Dict[str, set[str]] = {}
        for k, v in parsed_pairs:
            key_text = str(k or "").strip().lower()
            value_text = str(v or "").strip().lower()
            if not key_text:
                continue
            target_values_by_key.setdefault(key_text, set()).add(value_text)
        target_values = {
            str(v or "").strip().lower()
            for _, v in parsed_pairs
            if str(v or "").strip()
        }
    except Exception:
        target_keys = set()
        target_pair_tokens = set()
        target_values = set()
        target_values_by_key = {}
    target_board_id = str(context.get("board_id") or "").strip().lower()
    target_path_tokens = set(context.get("path_tokens") or set())
    if not target_keys and not target_pair_tokens and not target_values and not target_board_id and not target_path_tokens:
        return False
    for raw_token in required_tokens:
        token = str(raw_token or "").strip().lower()
        if not token:
            continue
        token = html.unescape(token)
        if token.startswith("?"):
            token = token[1:].strip()
            if not token:
                continue
        if "=" in token:
            try:
                parsed_rule_pairs = parse_qsl(token, keep_blank_values=True)
            except Exception:
                parsed_rule_pairs = []
            if parsed_rule_pairs:
                pair_ok = True
                for key, value in parsed_rule_pairs:
                    key_text = str(key or "").strip().lower()
                    value_text = str(value or "").strip().lower()
                    if not key_text:
                        continue
                    if value_text:
                        exact_match = f"{key_text}={value_text}" in target_pair_tokens
                        alias_match = any(
                            value_text in target_values_by_key.get(alias, set())
                            for alias in _query_key_aliases(key_text)
                        )
                        if not (exact_match or alias_match):
                            path_only_loose_match = bool(
                                not target_keys
                                and value_text
                                and (
                                    value_text in target_path_tokens
                                    or (target_board_id and value_text == target_board_id)
                                )
                            )
                            if not path_only_loose_match:
                                pair_ok = False
                                break
                    else:
                        if key_text not in target_keys:
                            pair_ok = False
                            break
                if pair_ok:
                    continue
        if token in target_keys or token in target_values:
            continue
        if _BOARD_CODE_TOKEN_RE.match(token) and target_board_id and token == target_board_id:
            continue
        if _target_url_matches_loose_query_token(token=token, context=context):
            continue
        return False
    return True


def _target_query_debug_context(target_url: str) -> Dict[str, Any]:
    context = _target_url_token_context(target_url)
    parsed_query = str(context.get("raw_query") or "")
    parsed_pairs = list(context.get("pairs") or [])
    keys = _unique_preserve_order_str([str(k or "").strip() for k, _ in parsed_pairs if str(k or "").strip()])
    pairs = _unique_preserve_order_str(
        [
            f"{str(k or '').strip()}={str(v or '').strip()}"
            for k, v in parsed_pairs
            if str(k or "").strip()
        ]
    )
    values = _unique_preserve_order_str([str(v or "").strip() for _, v in parsed_pairs if str(v or "").strip()])
    return {
        "path": str(context.get("path") or "").strip(),
        "raw_query": parsed_query[:240],
        "keys": keys[:20],
        "pairs": pairs[:20],
        "values": values[:20],
        "board_id": str(context.get("board_id") or "").strip(),
        "path_tokens": sorted(str(v) for v in (context.get("path_tokens") or set()))[:20],
    }


def _normalize_category_url_entries(raw_value: Any) -> List[Dict[str, Any]]:
    def _push(out: List[Dict[str, Any]], raw_url: Any, raw_query: Any = None) -> None:
        if _is_invalid_array_placeholder(raw_url):
            return
        query_keys = _extract_query_keys_from_value(raw_query)
        try:
            url_text = str(raw_url or "").strip()
        except Exception:
            url_text = ""
        if not url_text and not query_keys:
            return
        if url_text:
            out.append({"url": url_text, "query_keys": []})
        for group in _split_query_only_rule_groups(query_keys):
            out.append({"url": "", "query_keys": group})

    def _walk(raw: Any, out: List[Dict[str, Any]]) -> None:
        if raw is None:
            return
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return
            if _is_invalid_array_placeholder(text):
                return
            if text[:1] in ("{", "["):
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = None
                if parsed is not None:
                    _walk(parsed, out)
                    return
            _push(out, text, None)
            return
        if isinstance(raw, dict):
            urls_raw = None
            shared_query = None
            for key in ("url", "urls", "rules"):
                if key in raw:
                    urls_raw = raw.get(key)
                    break
            for key in ("query", "queries", "querys", "query_list", "queryList", "params", "keys"):
                if key in raw:
                    shared_query = raw.get(key)
                    break
            if urls_raw is None and "include" in raw:
                return
            if urls_raw is not None:
                normalized_rule_items = _normalize_rule_sequence(urls_raw)
                if not normalized_rule_items and shared_query is not None:
                    query_keys = _extract_query_keys_from_value(shared_query)
                    for group in _split_query_only_rule_groups(query_keys):
                        _push(out, "", group)
                    return
                for item in normalized_rule_items:
                    if isinstance(item, dict):
                        nested_url = item.get("url")
                        nested_query = None
                        for key in ("query", "queries", "querys", "query_list", "queryList", "params", "keys"):
                            if key in item:
                                nested_query = item.get(key)
                                break
                        _push(out, nested_url, nested_query)
                    else:
                        _push(out, item, None)
            if shared_query is not None:
                query_keys = _extract_query_keys_from_value(shared_query)
                for group in _split_query_only_rule_groups(query_keys):
                    _push(out, "", group)
                return
            for value in raw.values():
                _walk(value, out)
            return
        if isinstance(raw, (list, tuple, set)):
            for item in raw:
                _walk(item, out)
            return
        _push(out, raw, None)

    out: List[Dict[str, Any]] = []
    _walk(raw_value, out)
    deduped: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Tuple[str, ...]]] = set()
    for item in out:
        url_text = str(item.get("url") or "").strip()
        query_keys = _unique_preserve_order_str([str(v or "").strip() for v in (item.get("query_keys") or [])])
        if not url_text and not query_keys:
            continue
        key = (url_text, tuple(query_keys))
        if key in seen:
            continue
        seen.add(key)
        deduped.append({"url": url_text, "query_keys": query_keys})
    return deduped


def _normalize_category_rule_entries(raw_url_value: Any, raw_query_value: Any = None) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []

    for entry in _normalize_category_url_entries(raw_url_value):
        url_text = str(entry.get("url") or "").strip()
        query_keys = _unique_preserve_order_str([str(v or "").strip() for v in (entry.get("query_keys") or [])])
        if not url_text and not query_keys:
            continue
        merged.append({"url": url_text, "query_keys": query_keys})

    shared_query_keys = _extract_query_keys_from_value(raw_query_value)
    if shared_query_keys:
        for group in _split_query_only_rule_groups(shared_query_keys):
            merged.append({"url": "", "query_keys": group})

    deduped: List[Dict[str, Any]] = []
    seen: set[Tuple[str, Tuple[str, ...]]] = set()
    for item in merged:
        key = (
            str(item.get("url") or "").strip(),
            tuple(_unique_preserve_order_str([str(v or "").strip() for v in (item.get("query_keys") or [])])),
        )
        if (not key[0] and not key[1]) or key in seen:
            continue
        seen.add(key)
        deduped.append({"url": key[0], "query_keys": list(key[1])})

    return deduped


def _resolve_category_codes_for_tree(
    *,
    tree: str,
    row: Dict[str, Any],
    indexed_rows: Dict[str, Dict[str, Any]],
    root_tree: str,
) -> Tuple[str, str]:
    if not tree or not root_tree or not tree.startswith(root_tree):
        return "", ""
    current_code = str((row or {}).get("cate_code") or "").strip()
    root_row = (indexed_rows or {}).get(str(root_tree or "").strip(), {})
    root_name = str((root_row or {}).get("cate_name") or "").strip()
    include_root_parent = bool(root_name) and not _is_homepage_learning_root_row(root_row)
    parent_tree, parent_code, _parent_name = _find_category_parent_info(
        tree=tree,
        indexed_rows=indexed_rows,
        root_tree=root_tree,
        include_root=include_root_parent,
    )
    if parent_code and current_code and parent_code != current_code:
        return parent_code, current_code
    return current_code, ""


def _is_homepage_learning_root_row(row: Dict[str, Any]) -> bool:
    try:
        code = str((row or {}).get("cate_code") or "").strip()
    except Exception:
        code = ""
    if code and code == _CATEGORY_ROOT_DEFAULT_CODES.get("homepage_learning", ""):
        return True
    try:
        name = str((row or {}).get("cate_name") or "").strip()
    except Exception:
        name = ""
    if not name:
        return False
    if name in _CATEGORY_ROOT_FALLBACK_NAMES.get("homepage_learning", ()):
        return True
    name_low = name.lower().replace(" ", "")
    if "homepage_learning" in name_low or "homepagelearning" in name_low:
        return True
    return ("홈페이지" in name) and ("학습" in name)


def _resolve_category_codes_without_root(
    *,
    tree: str,
    row: Dict[str, Any],
    indexed_rows: Dict[str, Dict[str, Any]],
) -> Tuple[str, str]:
    if not tree:
        return "", ""
    current_code = str((row or {}).get("cate_code") or "").strip()
    parent_tree, parent_code, _parent_name = _find_category_parent_info(
        tree=tree,
        indexed_rows=indexed_rows,
    )
    if parent_code and current_code and parent_code != current_code:
        return parent_code, current_code
    return current_code, ""


def _find_board_category_info(
    indexed_rows: Dict[str, Dict[str, Any]],
    *,
    root_tree: Optional[str] = None,
) -> Tuple[str, str]:
    candidates: List[Tuple[int, str, str]] = []
    root_text = str(root_tree or "").strip()
    for tree, row in (indexed_rows or {}).items():
        tree_text = str(tree or "").strip()
        if root_text and (not tree_text or not tree_text.startswith(root_text)):
            continue
        name = str((row or {}).get("cate_name") or "").strip()
        if name != "게시판":
            continue
        code = str((row or {}).get("cate_code") or "").strip()
        if not code:
            continue
        candidates.append((len(tree_text), tree_text, code))
    if not candidates:
        return "", ""
    candidates.sort(key=lambda item: (item[0], item[1]))
    _length, tree_text, code = candidates[0]
    return code, tree_text


def _find_category_parent_info(
    *,
    tree: str,
    indexed_rows: Dict[str, Dict[str, Any]],
    root_tree: Optional[str] = None,
    include_root: bool = False,
) -> Tuple[str, str, str]:
    tree_text = str(tree or "").strip()
    if not tree_text:
        return "", "", ""

    best_tree = ""
    best_row: Dict[str, Any] = {}
    for ancestor_tree, ancestor_row in (indexed_rows or {}).items():
        ancestor_tree_text = str(ancestor_tree or "").strip()
        if not ancestor_tree_text or ancestor_tree_text == tree_text:
            continue
        if root_tree and not include_root and ancestor_tree_text == str(root_tree).strip():
            continue
        if tree_text.startswith(ancestor_tree_text) and len(ancestor_tree_text) < len(tree_text):
            if len(ancestor_tree_text) > len(best_tree):
                best_tree = ancestor_tree_text
                best_row = ancestor_row or {}

    if not best_tree:
        return "", "", ""

    return (
        str(best_tree or "").strip(),
        str((best_row or {}).get("cate_code") or "").strip(),
        str((best_row or {}).get("cate_name") or "").strip(),
    )


async def _load_category_url_pattern_object(
    chat_bot_id: str,
    db_name: str,
    *,
    contents_url: Optional[str] = None,
    require_nonempty_rules: bool = True,
    category_table_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    load_t0 = time.perf_counter()
    contents_url = _valid_category_scope_url(contents_url)
    table_name = str(category_table_name or "").strip() or _category_table_name_from_chat_bot_id(chat_bot_id)
    if not table_name or not str(db_name or "").strip():
        return None
    if not re.match(r"^[A-Za-z0-9_]+$", table_name):
        crawl_trace(
            logger,
            phase="category_rules",
            action="resolve_table",
            state="rejected",
            job_id=chat_bot_id,
            level=logging.WARNING,
            table=table_name,
        )
        return None
    table_source = "override" if category_table_name else "chat_bot_id"

    try:
        col_t0 = time.perf_counter()
        col_rows = await maria_execute_query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (db_name, table_name),
            fetch=True,
            dbname=db_name,
        )
        col_ms = (time.perf_counter() - col_t0) * 1000.0
    except Exception as exc:
        crawl_trace(
            logger,
            phase="category_rules",
            action="scan_columns",
            state="error",
            job_id=chat_bot_id,
            level=logging.ERROR,
            table=table_name,
            error=str(exc),
        )
        return None

    cols = {
        str((row or {}).get("column_name") or "").strip().lower()
        for row in (col_rows or [])
        if isinstance(row, dict)
    }
    required_cols = {"cate_code", "cate_treecode", "cate_name", "url"}
    if not required_cols.issubset(cols):
        crawl_trace(
            logger,
            phase="category_rules",
            action="scan_columns",
            state="missing_required",
            job_id=chat_bot_id,
            level=logging.WARNING,
            table=table_name,
            columns=sorted(cols),
            required=sorted(required_cols),
        )
        return None

    preferred_root_code = _category_root_code_from_env("homepage_learning")
    root_row: Optional[Dict[str, Any]] = None
    try:
        root_ms = 0.0
        if preferred_root_code:
            root_t0 = time.perf_counter()
            rows = await maria_execute_query(
                f"SELECT cate_code, cate_treecode, cate_name FROM `{table_name}` WHERE cate_code = %s LIMIT 1",
                (preferred_root_code,),
                fetch=True,
                dbname=db_name,
            )
            root_ms += (time.perf_counter() - root_t0) * 1000.0
            root_row = rows[0] if rows else None
        if not root_row:
            fallback_names = _CATEGORY_ROOT_FALLBACK_NAMES.get("homepage_learning", ())
            if fallback_names:
                placeholders = ", ".join(["%s"] * len(fallback_names))
                root_t0 = time.perf_counter()
                rows = await maria_execute_query(
                    f"SELECT cate_code, cate_treecode, cate_name FROM `{table_name}` WHERE cate_name IN ({placeholders}) ORDER BY LENGTH(cate_treecode) ASC, cate_treecode ASC LIMIT 1",
                    tuple(fallback_names),
                    fetch=True,
                    dbname=db_name,
                )
                root_ms += (time.perf_counter() - root_t0) * 1000.0
                root_row = rows[0] if rows else None
    except Exception as exc:
        crawl_trace(
            logger,
            phase="category_rules",
            action="resolve_root",
            state="error",
            job_id=chat_bot_id,
            level=logging.ERROR,
            table=table_name,
            error=str(exc),
        )
        return None

    root_tree = str((root_row or {}).get("cate_treecode") or "").strip()
    if not root_tree:
        crawl_trace(
            logger,
            phase="category_rules",
            action="resolve_root",
            state="not_found",
            job_id=chat_bot_id,
            level=logging.WARNING,
            table=table_name,
        )
        return None
    crawl_trace(
        logger,
        phase="category_rules",
        action="resolve_root",
        state="end",
        job_id=chat_bot_id,
        table=table_name,
        table_source=table_source,
        root=str((root_row or {}).get("cate_code") or "").strip(),
        root_tree=root_tree,
        url=(str(contents_url or "")[:180] if contents_url else ""),
    )

    select_cols = ["cate_code", "cate_treecode", "cate_name", "url"]
    if "query" in cols:
        select_cols.append("query")
    if "cate_use" in cols:
        select_cols.append("cate_use")
    crawl_trace(
        logger,
        phase="category_rules",
        action="scan_columns",
        state="end",
        job_id=chat_bot_id,
        counts={"selected_cols": len(select_cols), "all_cols": len(cols)},
        table=table_name,
        table_source=table_source,
        has_url_column=("url" in cols),
        has_query_column=("query" in cols),
        selected_cols=select_cols,
        all_cols=sorted(cols),
    )
    try:
        where_sql = "cate_treecode LIKE %s"
        if "cate_use" in cols:
            where_sql += " AND cate_use = 'y'"
        rows_t0 = time.perf_counter()
        rows = await maria_execute_query(
            f"SELECT {', '.join(select_cols)} FROM `{table_name}` WHERE {where_sql} ORDER BY cate_treecode ASC",
            (f"{root_tree}%",),
            fetch=True,
            dbname=db_name,
        )
        rows_ms = (time.perf_counter() - rows_t0) * 1000.0
    except Exception as exc:
        crawl_trace(
            logger,
            phase="category_rules",
            action="load_rows",
            state="error",
            job_id=chat_bot_id,
            level=logging.ERROR,
            table=table_name,
            error=str(exc),
        )
        return None

    indexed_rows: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        tree = str(row.get("cate_treecode") or "").strip()
        if tree:
            indexed_rows[tree] = row

    raw_rule_field_counts: Dict[str, int] = {
        "rows_with_raw_url": 0,
        "rows_with_raw_query": 0,
        "rows_with_parsed_query_keys": 0,
        "rows_with_raw_url_or_query": 0,
    }
    raw_rule_field_samples: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        raw_url = row.get("url")
        raw_query = row.get("query") if "query" in cols else None
        url_preview = _preview_debug_text(raw_url)
        query_preview = _preview_debug_text(raw_query)
        has_raw_url = bool(url_preview) and not _is_invalid_array_placeholder(raw_url)
        parsed_query_keys = _extract_query_keys_from_value(raw_query) if "query" in cols else []
        has_raw_query = bool(query_preview) and not _is_invalid_array_placeholder(raw_query)
        has_parsed_query = bool(parsed_query_keys)
        if has_raw_url:
            raw_rule_field_counts["rows_with_raw_url"] += 1
        if has_raw_query:
            raw_rule_field_counts["rows_with_raw_query"] += 1
        if has_parsed_query:
            raw_rule_field_counts["rows_with_parsed_query_keys"] += 1
        if has_raw_url or has_raw_query:
            raw_rule_field_counts["rows_with_raw_url_or_query"] += 1
        if len(raw_rule_field_samples) < 5 and (has_raw_url or has_raw_query):
            raw_rule_field_samples.append(
                {
                    "cate_code": str(row.get("cate_code") or "").strip(),
                    "tree": str(row.get("cate_treecode") or "").strip(),
                    "url_preview": url_preview,
                    "query_preview": query_preview,
                    "parsed_query_keys": list(parsed_query_keys),
                }
            )

    skip_scope_filter = _should_skip_category_scope_filter(contents_url)
    scope_identities = [] if skip_scope_filter else extract_scope_identities(contents_url)
    crawl_trace(
        logger,
        phase="category_rules",
        action="load_rows",
        state="end",
        job_id=chat_bot_id,
        elapsed_ms=rows_ms,
        counts={"rows": len(rows or []), "indexed_rows": len(indexed_rows)},
        table=table_name,
        table_source=table_source,
        skip_scope_filter=skip_scope_filter,
        scope_identities=scope_identities,
        url=(str(contents_url or "")[:180] if contents_url else ""),
    )
    after_rows_t0 = time.perf_counter()
    if _db_load_debug_enabled() or max(col_ms, root_ms, rows_ms) >= _db_load_slow_ms():
        crawl_trace(
            logger,
            phase="category_rules",
            action="load_queries",
            state="end",
            job_id=chat_bot_id,
            elapsed_ms=max(col_ms, root_ms, rows_ms),
            counts={"rows": len(rows or []), "cols": len(cols)},
            table=table_name,
            col_ms=col_ms,
            root_ms=root_ms,
            rows_ms=rows_ms,
            columns=sorted(cols),
            url=(str(contents_url or "")[:180] if contents_url else ""),
        )
    root_code = str((root_row or {}).get("cate_code") or "").strip()
    root_name = str((root_row or {}).get("cate_name") or "").strip()
    sample_hierarchy: List[Dict[str, Any]] = []
    for tree_key, row in list(indexed_rows.items())[:3]:
        parent_tree, parent_code, parent_name = _find_category_parent_info(
            tree=tree_key,
            indexed_rows=indexed_rows,
            root_tree=root_tree,
        )
        sample_hierarchy.append(
            {
                "tree": str(tree_key or "").strip(),
                "cate_code": str((row or {}).get("cate_code") or "").strip(),
                "cate_name": str((row or {}).get("cate_name") or "").strip(),
                "parent_tree": parent_tree,
                "parent_code": parent_code,
                "parent_name": parent_name,
                "root_code": root_code,
                "root_name": root_name,
            }
        )
    hierarchy_ms = (time.perf_counter() - after_rows_t0) * 1000.0
    crawl_trace(
        logger,
        phase="category_rules",
        action="build_hierarchy_sample",
        state="end",
        job_id=chat_bot_id,
        elapsed_ms=hierarchy_ms,
        counts={"rows": len(indexed_rows), "sample": len(sample_hierarchy)},
        root=root_code,
        root_name=root_name,
        root_tree=root_tree,
        sample=sample_hierarchy,
    )
    if _category_rule_debug_enabled():
        crawl_trace(
            logger,
            phase="category_rules",
            action="scan_raw_fields",
            state="end",
            job_id=chat_bot_id,
            counts=raw_rule_field_counts,
            has_query_column=("query" in cols),
            samples=raw_rule_field_samples,
        )
    def _collect_rule_items(
        current_rows: Dict[str, Dict[str, Any]],
        *,
        restrict_root_tree: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int, int, int, List[Dict[str, Any]], Dict[str, int], List[Dict[str, Any]]]:
        scoped_items: List[Dict[str, Any]] = []
        unscoped_items: List[Dict[str, Any]] = []
        with_rule_entries = 0
        without_rule_entries = 0
        without_cate_codes = 0
        samples: List[Dict[str, Any]] = []
        shape_counts: Dict[str, int] = {
            "url_only": 0,
            "query_only": 0,
            "url+query": 0,
            "empty": 0,
        }
        shape_samples: List[Dict[str, Any]] = []
        board_cate1_code, board_tree = _find_board_category_info(
            current_rows,
            root_tree=restrict_root_tree,
        )
        for tree, row in current_rows.items():
            if restrict_root_tree and (not tree or not tree.startswith(restrict_root_tree)):
                continue
            if board_tree:
                tree_text = str(tree or "").strip()
                if not tree_text.startswith(board_tree) or tree_text == board_tree:
                    continue
            url_entries = _normalize_category_rule_entries(
                row.get("url"),
                row.get("query") if "query" in cols else None,
            )
            if not url_entries:
                without_rule_entries += 1
                continue
            with_rule_entries += 1

            if restrict_root_tree:
                cate1_code, cate2_code = _resolve_category_codes_for_tree(
                    tree=tree,
                    row=row,
                    indexed_rows=current_rows,
                    root_tree=restrict_root_tree,
                )
            else:
                cate1_code, cate2_code = _resolve_category_codes_without_root(
                    tree=tree,
                    row=row,
                    indexed_rows=current_rows,
                )
            if not cate1_code and not cate2_code:
                without_cate_codes += 1
                continue

            current_cate_code = str((row or {}).get("cate_code") or "").strip()
            if board_cate1_code:
                if current_cate_code and current_cate_code != board_cate1_code:
                    cate1_code, cate2_code = board_cate1_code, current_cate_code
                else:
                    cate1_code, cate2_code = board_cate1_code, ""

            for entry in url_entries:
                sample_url = str(entry.get("url") or "").strip()
                query_keys = _unique_preserve_order_str([str(v or "").strip() for v in (entry.get("query_keys") or [])])
                if not sample_url and not query_keys:
                    continue
                if sample_url and query_keys:
                    shape = "url+query"
                elif sample_url:
                    shape = "url_only"
                elif query_keys:
                    shape = "query_only"
                else:
                    shape = "empty"
                shape_counts[shape] = shape_counts.get(shape, 0) + 1
                item: Dict[str, Any] = {
                    "url": sample_url,
                    "cate1": cate1_code,
                    "cate2": cate2_code,
                }
                if query_keys:
                    item["query"] = query_keys
                unscoped_items.append(item)
                if len(samples) < 5:
                    samples.append(item.copy())
                if len(shape_samples) < 5:
                    shape_samples.append(
                        {
                            "shape": shape,
                            "tree": tree,
                            "url": sample_url,
                            "query": list(query_keys),
                            "cate1": cate1_code,
                            "cate2": cate2_code,
                        }
                    )
                if len(shape_samples) <= 5:
                    parent_tree, parent_code, parent_name = _find_category_parent_info(
                        tree=tree,
                        indexed_rows=current_rows,
                        root_tree=restrict_root_tree,
                    )
                    crawl_trace(
                        logger,
                        phase="category_rules",
                        action="normalize_rule_sample",
                        state="matched",
                        job_id=chat_bot_id,
                        tree=str(tree or "").strip(),
                        cate_code=str((row or {}).get("cate_code") or "").strip(),
                        cate_name=str((row or {}).get("cate_name") or "").strip(),
                        parent_tree=parent_tree,
                        parent_code=parent_code,
                        parent_name=parent_name,
                        board_cate1=board_cate1_code,
                        url=sample_url,
                        query=list(query_keys),
                        resolved=(cate1_code, cate2_code),
                    )
                if sample_url and scope_identities and not url_matches_scope_identities(sample_url, scope_identities, path_prefix=""):
                    continue
                scoped_items.append(item)
        return scoped_items, unscoped_items, with_rule_entries, without_rule_entries, without_cate_codes, samples, shape_counts, shape_samples

    def _select_rule_items(
        scoped_items: List[Dict[str, Any]],
        unscoped_items: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        selected = scoped_items
        if not selected and unscoped_items and scope_identities:
            crawl_trace(
                logger,
                phase="category_rules",
                action="select_rules",
                state="fallback_unscoped",
                job_id=chat_bot_id,
                level=logging.WARNING,
                counts={"fallback_rules": len(unscoped_items)},
                url=(str(contents_url or "")[:180] if contents_url else ""),
                scope_identities=scope_identities,
            )
            selected = unscoped_items
        return selected

    async def _load_full_table_rule_items() -> Tuple[
        List[Dict[str, Any]],
        List[Dict[str, Any]],
        int,
        int,
        int,
        List[Dict[str, Any]],
        Dict[str, int],
        List[Dict[str, Any]],
    ]:
        full_t0 = time.perf_counter()
        full_where_sql = "cate_use = 'y'" if "cate_use" in cols else "1=1"
        cache_key = _category_full_table_cache_key(
            db_name=db_name,
            table_name=table_name,
            select_cols=select_cols,
            where_sql=full_where_sql,
        )
        cached_full = _category_full_table_cache_get(cache_key)
        if cached_full is not None:
            full_rows = list(cached_full.get("rows") or [])
            full_indexed_rows = dict(cached_full.get("indexed_rows") or {})
            full_raw_counts = dict(cached_full.get("raw_counts") or {})
            full_raw_samples = list(cached_full.get("raw_samples") or [])
            crawl_trace(
                logger,
                phase="category_rules",
                action="load_full_table",
                state="cache_hit",
                job_id=chat_bot_id,
                elapsed_ms=(time.perf_counter() - full_t0) * 1000.0,
                counts={"rows": len(full_rows), "indexed_rows": len(full_indexed_rows)},
                table=table_name,
            )
            if _category_rule_debug_enabled():
                crawl_trace(
                    logger,
                    phase="category_rules",
                    action="scan_full_table_raw_fields",
                    state="cache_hit",
                    job_id=chat_bot_id,
                    level=logging.DEBUG,
                    counts=full_raw_counts,
                    table=table_name,
                    has_query_column=("query" in cols),
                    samples=full_raw_samples,
                )
            return _collect_rule_items(
                full_indexed_rows,
                restrict_root_tree=None,
            )

        try:
            full_rows = await maria_execute_query(
                f"SELECT {', '.join(select_cols)} FROM `{table_name}` WHERE {full_where_sql} ORDER BY cate_treecode ASC",
                fetch=True,
                dbname=db_name,
            )
            full_ms = (time.perf_counter() - full_t0) * 1000.0
        except Exception as exc:
            crawl_trace(
                logger,
                phase="category_rules",
                action="load_full_table",
                state="error",
                job_id=chat_bot_id,
                level=logging.ERROR,
                table=table_name,
                error=str(exc),
            )
            full_rows = []
            full_ms = (time.perf_counter() - full_t0) * 1000.0
        full_indexed_rows: Dict[str, Dict[str, Any]] = {}
        for row in full_rows or []:
            if not isinstance(row, dict):
                continue
            tree = str(row.get("cate_treecode") or "").strip()
            if tree:
                full_indexed_rows[tree] = row
        full_raw_counts: Dict[str, int] = {
            "rows_with_raw_url": 0,
            "rows_with_raw_query": 0,
            "rows_with_parsed_query_keys": 0,
            "rows_with_raw_url_or_query": 0,
        }
        full_raw_samples: List[Dict[str, Any]] = []
        for row in full_rows or []:
            if not isinstance(row, dict):
                continue
            raw_url = row.get("url")
            raw_query = row.get("query") if "query" in cols else None
            url_preview = _preview_debug_text(raw_url)
            query_preview = _preview_debug_text(raw_query)
            has_raw_url = bool(url_preview) and not _is_invalid_array_placeholder(raw_url)
            has_raw_query = bool(query_preview) and not _is_invalid_array_placeholder(raw_query)
            parsed_query_keys = _extract_query_keys_from_value(raw_query) if "query" in cols else []
            has_parsed_query = bool(parsed_query_keys)
            if has_raw_url:
                full_raw_counts["rows_with_raw_url"] += 1
            if has_raw_query:
                full_raw_counts["rows_with_raw_query"] += 1
            if has_parsed_query:
                full_raw_counts["rows_with_parsed_query_keys"] += 1
            if has_raw_url or has_raw_query:
                full_raw_counts["rows_with_raw_url_or_query"] += 1
            if len(full_raw_samples) < 8 and (has_raw_url or has_raw_query):
                full_raw_samples.append(
                    {
                        "cate_code": str(row.get("cate_code") or "").strip(),
                        "tree": str(row.get("cate_treecode") or "").strip(),
                        "cate_name": _preview_debug_text(row.get("cate_name")),
                        "url_preview": url_preview,
                        "query_preview": query_preview,
                        "parsed_query_keys": list(parsed_query_keys),
                    }
                )
        crawl_trace(
            logger,
            phase="category_rules",
            action="load_full_table",
            state="end",
            job_id=chat_bot_id,
            elapsed_ms=full_ms,
            counts={"rows": len(full_rows or []), "indexed_rows": len(full_indexed_rows)},
            table=table_name,
        )
        crawl_trace(
            logger,
            phase="category_rules",
            action="scan_full_table_raw_fields",
            state="end",
            job_id=chat_bot_id,
            level=logging.WARNING,
            counts=full_raw_counts,
            table=table_name,
            has_query_column=("query" in cols),
            samples=full_raw_samples,
        )
        _category_full_table_cache_put(
            cache_key,
            {
                "rows": list(full_rows or []),
                "indexed_rows": dict(full_indexed_rows),
                "raw_counts": dict(full_raw_counts),
                "raw_samples": list(full_raw_samples),
            },
        )
        return _collect_rule_items(
            full_indexed_rows,
            restrict_root_tree=None,
        )

    normalize_t0 = time.perf_counter()
    scoped_rule_items, unscoped_rule_items, rows_with_rule_entries, rows_without_rule_entries, rows_without_cate_codes, rule_entry_samples, rule_shape_counts, rule_shape_samples = _collect_rule_items(
        indexed_rows,
        restrict_root_tree=root_tree,
    )
    normalize_ms = (time.perf_counter() - normalize_t0) * 1000.0

    used_full_table_fallback = False
    if not unscoped_rule_items:
        used_full_table_fallback = True
        crawl_trace(
            logger,
            phase="category_rules",
            action="fallback_full_table",
            state="no_root_rules",
            job_id=chat_bot_id,
            level=logging.DEBUG,
            table=table_name,
            root_tree=root_tree,
        )
        scoped_rule_items, unscoped_rule_items, rows_with_rule_entries, rows_without_rule_entries, rows_without_cate_codes, rule_entry_samples, rule_shape_counts, rule_shape_samples = await _load_full_table_rule_items()

    crawl_trace(
        logger,
        phase="category_rules",
        action="normalize_rules",
        state="end",
        job_id=chat_bot_id,
        elapsed_ms=normalize_ms,
        counts={
            "rows_with_rule_entries": rows_with_rule_entries,
            "rows_without_rule_entries": rows_without_rule_entries,
            "rows_without_cate_codes": rows_without_cate_codes,
            "unscoped_rules": len(unscoped_rule_items),
            "scoped_rules": len(scoped_rule_items),
        },
        used_full_fallback=used_full_table_fallback,
        sample_rules=rule_entry_samples,
    )
    if rows_with_rule_entries == 0:
        crawl_trace(
            logger,
            phase="category_rules",
            action="normalize_rules",
            state="empty",
            job_id=chat_bot_id,
            level=logging.WARNING,
            counts=raw_rule_field_counts,
            has_query_column=("query" in cols),
            samples=raw_rule_field_samples,
        )
    if _category_rule_debug_enabled():
        crawl_trace(
            logger,
            phase="category_rules",
            action="rule_shapes",
            state="end",
            job_id=chat_bot_id,
            counts=rule_shape_counts,
            samples=rule_shape_samples,
        )

    rule_items = _select_rule_items(scoped_rule_items, unscoped_rule_items)

    if (
        not used_full_table_fallback
        and contents_url
        and not skip_scope_filter
        and rule_items
        and resolve_cate_for_detail_url(str(contents_url), {"mode": "rule", "rules": rule_items}) is None
    ):
        crawl_trace(
            logger,
            phase="category_rules",
            action="fallback_full_table",
            state="contents_url_miss",
            job_id=chat_bot_id,
            level=logging.DEBUG,
            table=table_name,
            root_tree=root_tree,
            url=str(contents_url)[:180],
        )
        (
            full_scoped_rule_items,
            full_unscoped_rule_items,
            full_rows_with_rule_entries,
            full_rows_without_rule_entries,
            full_rows_without_cate_codes,
            full_rule_entry_samples,
            full_rule_shape_counts,
            full_rule_shape_samples,
        ) = await _load_full_table_rule_items()
        full_rule_items = _select_rule_items(full_scoped_rule_items, full_unscoped_rule_items)
        full_contents_matched = bool(
            full_rule_items
            and resolve_cate_for_detail_url(str(contents_url), {"mode": "rule", "rules": full_rule_items}) is not None
        )
        if full_rule_items:
            used_full_table_fallback = True
            scoped_rule_items = full_scoped_rule_items
            unscoped_rule_items = full_unscoped_rule_items
            rows_with_rule_entries = full_rows_with_rule_entries
            rows_without_rule_entries = full_rows_without_rule_entries
            rows_without_cate_codes = full_rows_without_cate_codes
            rule_entry_samples = full_rule_entry_samples
            rule_shape_counts = full_rule_shape_counts
            rule_shape_samples = full_rule_shape_samples
            rule_items = full_rule_items
            crawl_trace(
                logger,
                phase="category_rules",
                action="fallback_full_table",
                state="adopted",
                job_id=chat_bot_id,
                level=logging.DEBUG,
                counts={"rules": len(full_rule_items)},
                table=table_name,
                contents_url_matched=full_contents_matched,
            )

    if not rule_items and require_nonempty_rules:
        total_ms = (time.perf_counter() - load_t0) * 1000.0
        if _db_load_debug_enabled() or total_ms >= _db_load_slow_ms():
            crawl_trace(
                logger,
                phase="category_rules",
                action="load_summary",
                state="empty",
                job_id=chat_bot_id,
                elapsed_ms=total_ms,
                counts={"scoped_rules": len(scoped_rule_items), "unscoped_rules": len(unscoped_rule_items)},
                table=table_name,
                normalize_ms=normalize_ms,
                used_full_fallback=used_full_table_fallback,
                shape_counts=rule_shape_counts,
                url=(str(contents_url or "")[:180] if contents_url else ""),
            )
        return None
    total_ms = (time.perf_counter() - load_t0) * 1000.0
    if _db_load_debug_enabled() or total_ms >= _db_load_slow_ms():
        crawl_trace(
            logger,
            phase="category_rules",
            action="load_summary",
            state="end",
            job_id=chat_bot_id,
            elapsed_ms=total_ms,
            counts={
                "rules": len(rule_items),
                "scoped_rules": len(scoped_rule_items),
                "unscoped_rules": len(unscoped_rule_items),
            },
            table=table_name,
            normalize_ms=normalize_ms,
            used_full_fallback=used_full_table_fallback,
            shape_counts=rule_shape_counts,
            url=(str(contents_url or "")[:180] if contents_url else ""),
        )
    return {"mode": "rule", "rules": rule_items}


async def get_category_url_pattern_raw(
    chat_bot_id: str,
    method: str,
    db_name: str,
    contents_url: str = None,
    require_nonempty_rules: bool = True,
) -> Optional[str]:
    """
    CATEGORY 테이블의 url/query 기반 규칙만 조회한다.
    """
    try:
        category_obj = await _load_category_url_pattern_object(
            chat_bot_id,
            db_name,
            contents_url=contents_url,
            require_nonempty_rules=require_nonempty_rules,
        )
        if category_obj:
            rule_count = len(_get_rule_entries(category_obj))
            if _category_rule_debug_enabled():
                debug_summary = _rule_shape_debug_summary(category_obj)
                crawl_trace(
                    logger,
                    phase="category_rules",
                    action="raw_category_object",
                    state="end",
                    job_id=chat_bot_id,
                    counts={"rules": rule_count, **(debug_summary.get("counts") or {})},
                    samples=debug_summary.get("samples"),
                )
            crawl_trace(
                logger,
                phase="category_rules",
                action="load_url_query_rules",
                state="end",
                job_id=chat_bot_id,
                counts={"rules": rule_count},
                url=(str(contents_url or "")[:180] if contents_url else ""),
            )
            return _raw_url_pattern_to_str(category_obj)
    except Exception as category_exc:
        crawl_trace(
            logger,
            phase="category_rules",
            action="load_url_query_rules",
            state="error",
            job_id=chat_bot_id,
            level=logging.ERROR,
            error=str(category_exc),
        )
    return None


async def get_url_pattern_raw(
    chat_bot_id: str,
    method: str,
    db_name: str,
    contents_url: str = None,
    require_nonempty_rules: bool = True,
) -> Optional[str]:
    """
    Backward-compatible name. Cate resolution is CATEGORY-table only.
    This no longer reads LEARN_LIST.url_pattern for category codes.
    """
    return await get_category_url_pattern_raw(
        chat_bot_id,
        method,
        db_name,
        contents_url=contents_url,
        require_nonempty_rules=require_nonempty_rules,
    )

async def get_url_rule_filters(
    chat_bot_id: str,
    db_name: str,
    method: str = "period",
    contents_url: str = None,
) -> List[str]:
    """_CATEGORY 테이블의 url 컬럼(url)과 query 컬럼으로 만든 URL 패턴 목록만 추출한다."""
    try:
        category_obj = await _load_category_url_pattern_object(
            chat_bot_id,
            db_name,
            contents_url=contents_url,
        )
        if not category_obj:
            return []
        raw_list = _get_rule_entries(category_obj)
        patterns: List[str] = []
        query_only_count = 0
        query_only_samples: List[Dict[str, Any]] = []
        for item in raw_list:
            try:
                if isinstance(item, dict):
                    url = item.get("url")
                    url = str(url).strip() if url is not None else ""
                    if url:
                        patterns.append(url)
                    else:
                        query_keys = list(_extract_query_keys_from_rule_item(item))
                        if query_keys:
                            query_only_count += 1
                            if len(query_only_samples) < 5:
                                query_only_samples.append({"query_keys": query_keys, "cate1": str(item.get("cate1") or "").strip(), "cate2": str(item.get("cate2") or "").strip()})
                elif isinstance(item, str):
                    s = item.strip()
                    if s:
                        patterns.append(s)
                else:
                    s = str(item).strip() if item is not None else ""
                    if s:
                        patterns.append(s)
            except Exception as ex:
                crawl_trace(
                    logger,
                    phase="category_rules",
                    action="extract_url_patterns",
                    state="skip_malformed_rule",
                    job_id=chat_bot_id,
                    level=logging.DEBUG,
                    error=str(ex),
                )
                continue
        crawl_trace(
            logger,
            phase="category_rules",
            action="extract_url_patterns",
            state="end",
            job_id=chat_bot_id,
            counts={
                "total_rules": len(raw_list),
                "url_patterns": len(patterns),
                "query_only_rules": query_only_count,
            },
            samples=patterns[:5],
            query_only_samples=query_only_samples,
        )
        if _category_rule_debug_enabled():
            crawl_trace(
                logger,
                phase="category_rules",
                action="extract_url_patterns",
                state="debug_summary",
                job_id=chat_bot_id,
                counts={
                    "total_rules": len(raw_list),
                    "url_patterns": len(patterns),
                    "query_only_rules": query_only_count,
                },
                query_only_samples=query_only_samples,
            )
        return patterns
        
    except Exception as e:
        crawl_trace(
            logger,
            phase="category_rules",
            action="extract_url_patterns",
            state="error",
            job_id=chat_bot_id,
            level=logging.ERROR,
            error=str(e),
        )
        return []

def _extract_base_domain(url_input: Union[str, List[str]]) -> str:
    return extract_scope_host(url_input)


def _url_host_matches_scope_domains(url: str, domains: List[str], *, path_prefix: str = "") -> bool:
    return url_matches_scope_identities(url, domains, path_prefix=path_prefix)


def _reference_structure_pattern_key(contents_url: Optional[Union[str, List[str]]]) -> str:
    target = contents_url[0] if isinstance(contents_url, list) and contents_url else contents_url
    if not isinstance(target, str) or not target.strip():
        return ""
    if not url_structure_pattern_has_variable(target):
        return ""
    return url_structure_pattern_key(target)


def _url_matches_reference_structure_pattern(url: str, reference_pattern_key: str) -> bool:
    if not reference_pattern_key:
        return True
    return url_structure_pattern_key(url) == reference_pattern_key

def get_url_params_set(url: str):
    """URL에서 쿼리 파라미터의 '키(Key)' 목록만 추출하여 반환"""
    try:
        parsed = urlparse(url)
        query = parsed.query or ""
        pairs = parse_qsl(query, keep_blank_values=True)
        # 기능: 값(Value)은 무시하고 파라미터의 존재(Key)만 확인하여 구조 분석
        return set(k for k, v in pairs)
    except Exception:
        return set()


# def resolve_cate_for_detail_url(
#     target_url: str,
#     filters_obj: Optional[Dict[str, Any]],
# ) -> Optional[Tuple[str, str]]:
#     """
#     상세 URL이 url_pattern.include 중 하나와 구조적으로 일치할 때만 해당 항목의 cate1/cate2를 반환.
#     stream_asadal_urls_from_db 와 동일 규칙: 경로(dirname) + 스크립트명 + 게시판 쿼리 키;
#     패턴 URL에 본문 키(nttId 등)가 있을 때만 대상 URL에도 본문 키를 요구한다.
#     매칭 없으면 None — 전역 cate만으로는 채우지 않음(호출측에서 처리).
#     """
#     if not target_url or not filters_obj or filters_obj.get("mode") != "include":
#         return None
#     try:
#         url = ensure_url_scheme(str(target_url).strip())
#     except Exception:
#         url = (target_url or "").strip()
#     if not url:
#         return None

#     global_c1_list = filters_obj.get("cate1", {}).get("include", []) # 전역 cate1 배열
#     global_c2_list = filters_obj.get("cate2", {}).get("include", []) # 전역 cate2 배열

#     pattern_structures: List[Dict[str, Any]] = []

#     # 패턴 구조화 및 인덱스 정보 유지
#     for idx, item in enumerate(filters_obj.get("include", [])):
#         p = ""
#         c1_val, c2_val = None, None

#         if isinstance(item, dict):
#             # 1순위: 딕셔너리 내부의 cate 정보 사용
#             p = item.get("url", "")
#             c1_val = item.get("cate1")
#             c2_val = item.get("cate2")
#         elif isinstance(item, str):
#             # 2순위: 문자열일 경우 전역 배열에서 동일 인덱스의 값 참조
#             p = item
#             if idx < len(global_c1_list): c1_val = global_c1_list[idx]
#             if idx < len(global_c2_list): c2_val = global_c2_list[idx]

#         if not p: continue

#         p_parsed = urlparse(p)
#         pattern_structures.append({
#             "dir_path": (os.path.dirname(p_parsed.path) or "").rstrip("/"),
#             "base_name": os.path.basename(p_parsed.path),
#             "keys": get_url_params_set(p),
#             "params": get_url_params_dict(p),
#             "raw": p,
#             "cate1": str(c1_val or "").strip() if c1_val is not None else None, # 정제된 cate1
#             "cate2": str(c2_val or "").strip() if c2_val is not None else None, # 정제된 cate2
#         })

#     if not pattern_structures:
#         return None

#     try:
#         target_parsed = urlparse(url)
#         target_params = get_url_params_dict(url)
#         target_keys = get_url_params_set(url)
#         target_base = os.path.basename(target_parsed.path)
#     except Exception:
#         return None

#     for struct in pattern_structures:
#         if not _structural_match_include_to_target(
#             struct,
#             target_parsed=target_parsed,
#             target_params=target_params,
#             target_keys=target_keys,
#             target_base=target_base,
#         ):
#             continue

#         # dict 항목의 cate1/cate2 또는 문자열 항목 + 전역 cate1/cate2.include 동일 인덱스(위 루프에서 이미 struct에 반영)
#         c1 = struct.get("cate1")
#         c2 = struct.get("cate2")
#         c1 = "" if c1 is None else str(c1).strip()
#         c2 = "" if c2 is None else str(c2).strip()
#         return (c1, c2)

#     return None

def resolve_cate_for_detail_url(
    target_url: str,
    filters_obj: Optional[Dict[str, Any]],
) -> Optional[Tuple[str, str]]:
    """상세 URL이 include 키워드를 포함할 때 카테고리 반환(대소문자 무시). include 항목이 깨져 있어도 예외 없이 진행."""
    try:
        if not target_url or not filters_obj or not isinstance(filters_obj, dict):
            crawl_trace(
                logger,
                phase="category_rules",
                action="resolve_detail_url",
                state="invalid_input",
                level=logging.DEBUG,
                url=(target_url or "")[:180],
                has_filters=bool(filters_obj),
                filters_type=type(filters_obj).__name__ if filters_obj is not None else None,
            )
            return None
        # get_include_filters / _pattern_json_has_nonempty_include 과 동일: mode 미지정이면 include 패턴 사용.
        # UI(crawling_period01 등)는 mode 없이 url_pattern만 저장하는 경우가 많음.
        if (filters_obj.get("mode") or "").strip().lower() == "exclude":
            crawl_trace(
                logger,
                phase="category_rules",
                action="resolve_detail_url",
                state="skip_exclude_mode",
                level=logging.DEBUG,
                url=(target_url or "")[:180],
            )
            return None

        rule_list = _get_rule_entries(filters_obj)
        if _category_rule_debug_enabled():
            crawl_trace(
                logger,
                phase="category_rules",
                action="resolve_detail_url",
                state="start",
                counts={"rules": len(rule_list)},
                url=(target_url or "")[:180],
                target=_target_query_debug_context(target_url),
            )
        if not rule_list:
            crawl_trace(
                logger,
                phase="category_rules",
                action="resolve_detail_url",
                state="no_rules",
                level=logging.DEBUG,
                url=(target_url or "")[:180],
                filters_keys=sorted(list(filters_obj.keys()))[:20],
            )
            return None
        query_index = _get_query_exact_category_index(filters_obj, rule_list)
        if query_index:
            fast_t0 = time.perf_counter()
            try:
                parsed_pairs = parse_qsl(urlparse(target_url or "").query, keep_blank_values=True)
            except Exception:
                parsed_pairs = []
            for key, value in parsed_pairs:
                key_text = str(key or "").strip().lower()
                value_text = str(value or "").strip().lower()
                if not key_text or not value_text:
                    continue
                for alias in _query_key_aliases(key_text):
                    hit = query_index.get(f"{alias}={value_text}")
                    if hit:
                        c1s, c2s, idx = hit
                        if _category_rule_debug_enabled():
                            crawl_trace(
                                logger,
                                phase="category_rules",
                                action="resolve_detail_url",
                                state="query_index_match",
                                elapsed_ms=(time.perf_counter() - fast_t0) * 1000.0,
                                url=(target_url or "")[:180],
                                cate1=c1s,
                                cate2=c2s,
                                idx=idx,
                                token=f"{alias}={value_text}",
                            )
                        return (c1s, c2s)

        global_c1 = _global_cate_rule_list(filters_obj, "cate1")
        global_c2 = _global_cate_rule_list(filters_obj, "cate2")

        ordered_indices = list(range(len(rule_list)))

        fallback_pair: Optional[Tuple[str, str]] = None
        fallback_meta: Optional[Dict[str, Any]] = None
        _warned_empty_cate = False
        miss_samples: List[Dict[str, Any]] = []
        miss_counts: Dict[str, int] = {
            "url_rule_failed": 0,
            "query_rule_failed": 0,
            "url_and_query_rule_failed": 0,
            "empty_rule": 0,
        }

        for idx in ordered_indices:
            try:
                item = rule_list[idx]
                if isinstance(item, dict):
                    kw = item.get("url")
                    keyword = str(kw or "").strip()
                else:
                    keyword = str(item or "").strip()
                required_query_keys = _extract_query_keys_from_rule_item(item) if isinstance(item, dict) else []
                if not keyword and not required_query_keys:
                    miss_counts["empty_rule"] += 1
                    continue
                url_matched = bool(
                    keyword and (
                        urls_pattern_match_for_cate(keyword, target_url)
                        or _keyword_match_to_target(keyword, target_url)
                    )
                )
                query_matched = bool(
                    required_query_keys
                    and _target_url_matches_required_rule_tokens(target_url, required_query_keys)
                )
                has_url_rule = bool(keyword)
                has_query_rule = bool(required_query_keys)
                if has_url_rule and has_query_rule:
                    matched = url_matched and query_matched
                elif has_query_rule:
                    matched = query_matched
                else:
                    matched = url_matched
                if not matched:
                    if has_url_rule and has_query_rule:
                        miss_counts["url_and_query_rule_failed"] += 1
                    elif has_query_rule:
                        miss_counts["query_rule_failed"] += 1
                    else:
                        miss_counts["url_rule_failed"] += 1
                    if len(miss_samples) < 5:
                        miss_samples.append(
                            {
                                "idx": idx,
                                "url_rule": keyword[:180],
                                "query_rule": list(required_query_keys)[:12],
                                "url_matched": url_matched,
                                "query_matched": query_matched,
                                "has_url_rule": has_url_rule,
                                "has_query_rule": has_query_rule,
                                "cate1": str(item.get("cate1") or "").strip() if isinstance(item, dict) else "",
                                "cate2": str(item.get("cate2") or "").strip() if isinstance(item, dict) else "",
                            }
                        )
                    continue
                if _category_rule_debug_enabled():
                    crawl_trace(
                        logger,
                        phase="category_rules",
                        action="resolve_detail_url",
                        state="rule_match",
                        url=(target_url or "")[:180],
                        keyword=(keyword or "")[:120],
                        query_keys=required_query_keys,
                        url_matched=url_matched,
                        query_matched=query_matched,
                        idx=idx,
                    )

                if isinstance(item, dict):
                    c1, c2 = item.get("cate1"), item.get("cate2")
                else:
                    c1, c2 = None, None
                try:
                    c1s = _sanitize_category_value(c1)
                    c2s = _sanitize_category_value(c2)
                except Exception:
                    c1s, c2s = "", ""
                if not c1s:
                    c1s = _global_cate_value_at(global_c1, idx)
                if not c2s:
                    c2s = _global_cate_value_at(global_c2, idx)
                c1s = _sanitize_category_value(c1s)
                c2s = _sanitize_category_value(c2s)

                if c1s or c2s:
                    parent_debug = None
                    if isinstance(item, dict):
                        parent_debug = {
                            "cate1": str(item.get("cate1") or "").strip(),
                            "cate2": str(item.get("cate2") or "").strip(),
                            "url": str(item.get("url") or "").strip(),
                            "query": list(required_query_keys),
                        }
                    match_meta = {
                        "target": (target_url or "")[:180],
                        "cate1": c1s,
                        "cate2": c2s,
                        "matched_by": "url+query" if url_matched and query_matched else ("url" if url_matched else "query"),
                        "rule_meta": parent_debug,
                    }
                    if not c2s:
                        if fallback_pair is None:
                            fallback_pair = (c1s, c2s)
                            fallback_meta = match_meta
                        continue
                    if _category_rule_debug_enabled():
                        crawl_trace(
                            logger,
                            phase="category_rules",
                            action="resolve_detail_url",
                            state="resolved",
                            url=match_meta["target"],
                            cate1=c1s,
                            cate2=c2s,
                            matched_by=match_meta["matched_by"],
                            idx=idx,
                            query_keys=required_query_keys,
                            rule_meta=match_meta["rule_meta"],
                        )
                    return (c1s, c2s)
                if not _warned_empty_cate:
                    _warned_empty_cate = True
                    crawl_trace(
                        logger,
                        phase="category_rules",
                        action="resolve_detail_url",
                        state="matched_empty_category",
                        level=logging.WARNING,
                        url=(target_url or "")[:120],
                        keyword=(keyword or "")[:100],
                    )
                if fallback_pair is None:
                    fallback_pair = (c1s, c2s)
            except Exception as ex:
                crawl_trace(
                    logger,
                    phase="category_rules",
                    action="resolve_detail_url",
                    state="skip_rule_error",
                    level=logging.DEBUG,
                    idx=idx,
                    error=str(ex),
                )
                continue

        if fallback_pair is not None:
            if fallback_meta:
                if _category_rule_debug_enabled():
                    crawl_trace(
                        logger,
                        phase="category_rules",
                        action="resolve_detail_url",
                        state="fallback_resolved",
                        url=fallback_meta["target"],
                        cate1=fallback_meta["cate1"],
                        cate2=fallback_meta["cate2"],
                        matched_by=fallback_meta["matched_by"],
                        rule_meta=fallback_meta["rule_meta"],
                    )
            return fallback_pair
        crawl_trace(
            logger,
            phase="category_rules",
            action="resolve_detail_url",
            state="miss",
            level=logging.DEBUG,
            counts={"rules": len(rule_list), **miss_counts},
            url=(target_url or "")[:180],
            target=_target_query_debug_context(target_url),
            samples=miss_samples,
        )
        return None
    except Exception as ex:
        crawl_trace(
            logger,
            phase="category_rules",
            action="resolve_detail_url",
            state="error",
            level=logging.WARNING,
            url=(target_url or "")[:120],
            error=str(ex),
        )
        return None


def _sql_single_quoted_literal(value: str) -> str:
    """WHERE 절용 단순 이스케이프(신뢰된 chat_bot_id 등)."""
    return str(value).replace("\\", "\\\\").replace("'", "''")


def _rule_item_sql_prefilter_terms(item: Any) -> List[str]:
    terms: List[str] = []
    try:
        if isinstance(item, dict):
            url_text = str(item.get("url") or "").strip()
            query_tokens = list(_extract_query_keys_from_rule_item(item))
        else:
            url_text = str(item or "").strip()
            query_tokens = []
    except Exception:
        return []

    if url_text:
        try:
            parsed = urlparse(ensure_url_scheme(url_text))
        except Exception:
            parsed = urlparse(url_text)
        path_text = str(parsed.path or "").strip()
        if path_text and path_text != "/":
            terms.append(path_text)
        try:
            for key, value in parse_qsl(parsed.query or "", keep_blank_values=True):
                key_text = str(key or "").strip()
                value_text = str(value or "").strip()
                if not key_text:
                    continue
                # 입력 URL 기반 prefilter는 게시판 범위를 고정하는 안정 키만 사용한다.
                # 글 번호/페이지/정렬 파라미터까지 LIKE 조건에 넣으면
                # 같은 게시판 상세들이 불필요하게 탈락할 수 있다.
                term = f"{key_text}={value_text}" if value_text else key_text
                terms.append(term)
        except Exception:
            pass
        board_id = _bbs_board_id_from_text(url_text)
        if board_id:
            terms.append(f"/{board_id}/")

    for raw_token in query_tokens:
        token = str(raw_token or "").strip()
        if not token:
            continue
        terms.append(token)
        if _BOARD_CODE_TOKEN_RE.match(token):
            terms.append(f"/{token}/")

    return _unique_preserve_order_str([str(term or "").strip() for term in terms if str(term or "").strip()])


def _sql_prefilter_term_groups(terms: List[str]) -> List[List[str]]:
    groups: List[List[str]] = []
    for term in terms:
        text = str(term or "").strip()
        if not text:
            continue
        aliases = _query_token_alias_terms(text) if "=" in text else []
        if "/" in text and "://" not in text:
            encoded = quote(text, safe="=&")
            if encoded and encoded != text:
                aliases.append(encoded)
        group = _unique_preserve_order_str([text, *aliases])
        if group:
            groups.append(group)
    return groups


def _build_exploration_rule_sql_condition(
    filters_obj: Optional[Dict[str, Any]],
    *,
    column_name: str = "url",
    max_rules: int = 200,
    max_terms_per_rule: int = 8,
) -> Tuple[str, Dict[str, Any]]:
    t0 = time.perf_counter()
    if not isinstance(filters_obj, dict):
        return "", {"rule_items": 0, "sql_rules": 0, "sample_terms": []}

    clauses: List[str] = []
    sample_terms: List[Dict[str, Any]] = []
    rule_items = _get_rule_entries(filters_obj)
    rule_limit = max(1, int(max_rules or 200))
    truncated = len(rule_items) > rule_limit
    for idx, item in enumerate(rule_items[:rule_limit]):
        terms = _rule_item_sql_prefilter_terms(item)[: max(1, int(max_terms_per_rule or 8))]
        term_groups = _sql_prefilter_term_groups(terms)
        if not term_groups:
            continue
        group_parts: List[str] = []
        for group in term_groups:
            alias_parts: List[str] = []
            for term in group:
                pattern = sql_like_contains_pattern(term).replace("%", "%%")
                alias_parts.append(f"{column_name} LIKE '{_sql_single_quoted_literal(pattern)}' ESCAPE '!'")
            if not alias_parts:
                continue
            if len(alias_parts) == 1:
                group_parts.append(alias_parts[0])
            else:
                group_parts.append("(" + " OR ".join(alias_parts) + ")")
        if not group_parts:
            continue
        clauses.append("(" + " AND ".join(group_parts) + ")")
        if len(sample_terms) < 5:
            sample_terms.append({"idx": idx, "terms": list(terms), "groups": term_groups})

    if not clauses:
        meta = {"rule_items": len(rule_items), "sql_rules": 0, "sample_terms": sample_terms, "truncated": truncated}
        if _db_load_debug_enabled():
            crawl_trace(
                logger,
                phase="category_rules",
                action="build_rule_sql",
                state="empty",
                elapsed_ms=(time.perf_counter() - t0) * 1000.0,
                counts={"rule_items": len(rule_items), "sql_rules": 0},
                truncated=truncated,
                sample_terms=sample_terms,
            )
        return "", meta
    condition = "(" + " OR ".join(clauses) + ")"
    meta = {
        "rule_items": len(rule_items),
        "sql_rules": len(clauses),
        "sample_terms": sample_terms,
        "truncated": truncated,
        "sql_length": len(condition),
    }
    try:
        sql_length_warn = int(os.getenv("DB_LOAD_SQL_LENGTH_WARN", "20000") or "20000")
    except Exception:
        sql_length_warn = 20000
    if _db_load_debug_enabled() or len(condition) >= sql_length_warn:
        crawl_trace(
            logger,
            phase="category_rules",
            action="build_rule_sql",
            state="end",
            elapsed_ms=(time.perf_counter() - t0) * 1000.0,
            counts={"rule_items": len(rule_items), "sql_rules": len(clauses), "sql_length": len(condition)},
            truncated=truncated,
            max_rules=max_rules,
            max_terms_per_rule=max_terms_per_rule,
            sample_terms=sample_terms,
        )
    return condition, meta


def _stable_query_tokens_from_url(url: str) -> List[str]:
    try:
        parsed = urlparse(str(url or "").strip())
        pairs = parse_qsl(parsed.query or "", keep_blank_values=True)
    except Exception:
        return []
    tokens: List[str] = []
    for key, value in pairs:
        key_text = str(key or "").strip()
        value_text = str(value or "").strip()
        if not key_text:
            continue
        tokens.append(f"{key_text}={value_text}" if value_text else key_text)
    return _unique_preserve_order_str(tokens)


def _merge_runtime_rule_from_input_url(
    filters_obj: Optional[Dict[str, Any]],
    *,
    contents_url: Optional[str],
    cate1: Any = None,
    cate2: Any = None,
) -> Optional[Dict[str, Any]]:
    """
    사용자가 입력한 상세 URL과 요청 분류(cate1/cate2)를 임시 rule 로 주입한다.
    DB CATEGORY 규칙이 부족해도 같은 게시판 패턴의 exploration URL들에
    동일 분류를 전파할 수 있게 start_urls 단계에서만 사용한다.
    """
    try:
        normalized_url = ensure_url_scheme(str(contents_url or "").strip()) if contents_url else ""
    except Exception:
        normalized_url = str(contents_url or "").strip()
    cate1_text = _sanitize_category_value(cate1)
    cate2_text = _sanitize_category_value(cate2)
    if not normalized_url or not (cate1_text or cate2_text):
        return filters_obj

    runtime_rule = {
        "url": normalized_url,
        "cate1": cate1_text,
        "cate2": cate2_text,
    }
    stable_query_tokens = _stable_query_tokens_from_url(normalized_url)
    if stable_query_tokens:
        runtime_rule["query"] = stable_query_tokens

    if not isinstance(filters_obj, dict):
        return {"mode": "rule", "rules": [runtime_rule]}

    merged = dict(filters_obj)
    existing_rules = _get_rule_entries(filters_obj)
    merged_rules: List[Any] = [runtime_rule]
    runtime_key = (
        str(runtime_rule.get("url") or "").strip(),
        tuple(_extract_query_keys_from_rule_item(runtime_rule)),
        str(runtime_rule.get("cate1") or "").strip(),
        str(runtime_rule.get("cate2") or "").strip(),
    )
    for item in existing_rules:
        if isinstance(item, dict):
            item_key = (
                str(item.get("url") or "").strip(),
                tuple(_extract_query_keys_from_rule_item(item)),
                _sanitize_category_value(item.get("cate1")),
                _sanitize_category_value(item.get("cate2")),
            )
            if item_key == runtime_key:
                continue
        merged_rules.append(item)
    merged["rules"] = merged_rules
    return merged


# 요약: CATEGORY 테이블 url/query 규칙이 있으면 규칙 매칭 통과분만 yield.
#       CATEGORY 규칙이 없으면 ASADAL_CRAWLING_EXPLORATION 중 type='post' 행만 전부 yield(chat_bot_id·도메인 스코프).
# kwargs: stream_matched_include_only — CATEGORY 규칙이 있어도 False 이면 매칭 없이 post 전체 yield.
async def stream_asadal_urls_from_db(
    db_name: Optional[str] = None,
    target_domains: Optional[List[str]] = None,
    batch_size: int = 10,
    contents_url: Optional[Union[str, List[str]]] = None,
    chat_bot_id: Optional[str] = None,
    filtered_memory_storage: List[Dict[str, Any]] = None, # 기능: 패턴 통과 데이터를 담을 외부 메모리 리스트
    **kwargs
) -> AsyncIterable[List[Dict[str, Any]]]:
    
    current_contents_url = contents_url[0] if isinstance(contents_url, list) and contents_url else (contents_url if isinstance(contents_url, str) else None)
    request_cate1 = kwargs.get("cate1")
    request_cate2 = kwargs.get("cate2")
    exploration_date_filter_enabled = _coerce_bool_flag(
        kwargs.get("exploration_date_filter_enabled"),
        default=False,
    )
    target_date = kwargs.get("target_date")
    start_date_iso, end_date_iso = _parse_target_date_range(target_date)

    _method = kwargs.get("method", "period")
    stream_matched_rules_only = _resolve_stream_matched_rules_only_kwargs(kwargs, default=False)
    url_rule_patterns: List[str] = []
    if stream_matched_rules_only:
        url_rule_patterns = await get_url_rule_filters(
            chat_bot_id,
            db_name,
            method=_method,
            contents_url=current_contents_url,
        )
    else:
        logger.info(
            "[START_URLS_RULE_TRACE][board] category filtering disabled -> type=post only mode | chat_bot_id=%s contents_url=%s",
            chat_bot_id,
            (str(current_contents_url or "")[:180] if current_contents_url else ""),
        )

    filters_obj_for_cate: Optional[Dict[str, Any]] = None
    if chat_bot_id and db_name:
        try:
            filters_obj_for_cate = await _load_category_url_pattern_object(
                chat_bot_id,
                db_name,
                contents_url=current_contents_url,
            )
        except Exception:
            filters_obj_for_cate = None
    if stream_matched_rules_only:
        filters_obj_for_cate = _merge_runtime_rule_from_input_url(
            filters_obj_for_cate,
            contents_url=current_contents_url,
            cate1=request_cate1,
            cate2=request_cate2,
        )

    # 2. DB 원본 데이터 로드 (전체 대상)
    has_category_url_rules = bool(isinstance(filters_obj_for_cate, dict) and _get_rule_entries(filters_obj_for_cate))
    use_url_rule_scope = has_category_url_rules and stream_matched_rules_only
    final_domains, scope_path_prefix = _resolve_preexplored_scope(
        target_domains=target_domains,
        contents_url=contents_url,
        use_rule_scope=use_url_rule_scope,
        rule_patterns=url_rule_patterns,
        explicit_path_prefix=kwargs.get("scope_path_prefix"),
    )
    # start_urls는 같은 host의 탐색 DB post 전체를 일괄 투입해야 하므로
    # 요청 URL에서 파생된 path prefix는 적용하지 않는다. 다만 사용자가 명시한
    # start_urls/scope prefix는 반드시 유지한다.
    effective_path_prefix = normalize_scope_path_prefix(scope_path_prefix)
    logger.info(
        "[START_URLS_RULE_TRACE][board] init | chat_bot_id=%s method=%s contents_url=%s rule_count=%s rule_scope=%s stream_matched_rules_only=%s final_domains=%s path_prefix=%s requested_path_prefix=%s rule_sample=%s",
        chat_bot_id,
        _method,
        (str(current_contents_url or "")[:180] if current_contents_url else ""),
        len(_get_rule_entries(filters_obj_for_cate)) if isinstance(filters_obj_for_cate, dict) else 0,
        use_url_rule_scope,
        stream_matched_rules_only,
        final_domains,
        effective_path_prefix,
        normalize_scope_path_prefix(kwargs.get("scope_path_prefix")),
        _summarize_rule_entries(filters_obj_for_cate),
    )

    table_name = EXPLORATION_TABLE
    start_urls_order = str(kwargs.get("start_urls_order") or "").strip().lower()
    order_by = "id DESC" if start_urls_order in {"reverse", "desc", "backward", "backwards", "from_back", "back"} else "id ASC"
    exploration_date_condition = ""
    if not exploration_date_filter_enabled:
        logger.info(
            "[START_URLS_DATE_FILTER][board] off | chat_bot_id=%s target_date=%s",
            chat_bot_id,
            target_date,
        )
    elif not (start_date_iso and end_date_iso):
        logger.warning(
            "[START_URLS_DATE_FILTER][board] on_but_invalid_target_date | chat_bot_id=%s target_date=%s",
            chat_bot_id,
            target_date,
        )
    elif exploration_date_filter_enabled and start_date_iso and end_date_iso:
        exploration_date_column = await _resolve_exploration_date_column(db_name, table_name)
        exploration_date_condition = _build_exploration_date_range_condition(
            str(exploration_date_column or "").strip(),
            start_date_iso=start_date_iso,
            end_date_iso=end_date_iso,
        )
        if exploration_date_condition:
            logger.info(
                "[START_URLS_DATE_FILTER][board] applied | chat_bot_id=%s column=%s start=%s end=%s",
                chat_bot_id,
                exploration_date_column,
                start_date_iso,
                end_date_iso,
            )
        else:
            logger.warning(
                "[START_URLS_DATE_FILTER][board] skipped | chat_bot_id=%s start=%s end=%s column=%s",
                chat_bot_id,
                start_date_iso,
                end_date_iso,
                exploration_date_column,
            )
    query_conditions = build_exploration_conditions(
        ExplorationQuerySpec(
            chat_bot_id=chat_bot_id,
            target_domains=list(final_domains or []),
            path_prefix=effective_path_prefix,
            include_empty_type=stream_matched_rules_only,
            dedupe_urls=True,
            require_active=True,
            date_condition=exploration_date_condition,
        )
    )
    condition = query_conditions.condition
    legacy_condition = query_conditions.legacy_condition
    sql_rule_condition = ""
    sql_rule_meta: Dict[str, Any] = {"rule_items": 0, "sql_rules": 0, "sample_terms": []}
    if use_url_rule_scope and isinstance(filters_obj_for_cate, dict):
        sql_rule_condition, sql_rule_meta = _build_exploration_rule_sql_condition(filters_obj_for_cate, column_name="url")
        if sql_rule_condition:
            typed_or_matched_empty = f"(type IN ('post') OR (COALESCE(TRIM(CAST(`type` AS CHAR)), '') = '' AND ({sql_rule_condition})))"
            condition += f" AND {typed_or_matched_empty}"
            legacy_condition += f" AND {typed_or_matched_empty}"
        logger.info(
            "[START_URLS_RULE_TRACE][board] sql prefilter | chat_bot_id=%s sql_rules=%s rule_items=%s applied=%s sample_terms=%s",
            chat_bot_id,
            int(sql_rule_meta.get("sql_rules") or 0),
            int(sql_rule_meta.get("rule_items") or 0),
            bool(sql_rule_condition),
            sql_rule_meta.get("sample_terms") or [],
        )

    try:
        rows = await maria_select_data(table_name, columns="url, type", condition=condition, dbname=db_name, order_by=order_by)
        if not rows:
            logger.warning(
                "[DBStream] ASADAL_CRAWLING_EXPLORATION 조건에 맞는 행 없음 | dbname=%s chat_bot_id=%s domains=%s",
                db_name,
                chat_bot_id,
                final_domains,
            )
            return
    except Exception as e:
        if _should_fallback_to_legacy_exploration_condition(e):
            logger.warning(
                "[DBStream] exploration filter columns unavailable -> fallback to legacy condition | dbname=%s chat_bot_id=%s err=%s",
                db_name,
                chat_bot_id,
                e,
            )
            try:
                rows = await maria_select_data(
                    table_name,
                    columns="url, type",
                    condition=legacy_condition,
                    dbname=db_name,
                    order_by=order_by,
                )
                if not rows:
                    logger.warning(
                        "[DBStream] legacy fallback returned no rows | dbname=%s chat_bot_id=%s domains=%s",
                        db_name,
                        chat_bot_id,
                        final_domains,
                    )
                    return
            except Exception as legacy_exc:
                logger.error(f"[DBStream] Legacy fallback DB Query Error: {legacy_exc}")
                return
        else:
            logger.error(f"[DBStream] DB Query Error: {e}")
            return

    if use_url_rule_scope and sql_rule_condition and isinstance(rows, list):
        relaxed_threshold = _exploration_rule_relaxed_fallback_min_rows()
        if len(rows) < relaxed_threshold:
            try:
                relaxed_rows = await maria_select_data(
                    table_name,
                    columns="url, type",
                    condition=legacy_condition,
                    dbname=db_name,
                    order_by=order_by,
                )
            except Exception as relaxed_exc:
                logger.warning(
                    "[START_URLS_RULE_TRACE][board] relaxed fallback fetch failed | chat_bot_id=%s strict_count=%s threshold=%s err=%s",
                    chat_bot_id,
                    len(rows),
                    relaxed_threshold,
                    relaxed_exc,
                )
            else:
                merged_rows: List[Dict[str, Any]] = []
                seen_relaxed_urls: set[str] = set()
                for source_row in list(rows or []) + list(relaxed_rows or []):
                    if not isinstance(source_row, dict):
                        continue
                    raw_url = source_row.get("url")
                    try:
                        normalized_url = ensure_url_scheme(str(raw_url).strip()) if raw_url else ""
                    except Exception:
                        normalized_url = str(raw_url or "").strip()
                    dedupe_key = normalized_url or repr(source_row)
                    if dedupe_key in seen_relaxed_urls:
                        continue
                    seen_relaxed_urls.add(dedupe_key)
                    merged_rows.append(source_row)
                logger.info(
                    "[START_URLS_RULE_TRACE][board] relaxed fallback applied | chat_bot_id=%s strict_count=%s relaxed_count=%s merged_count=%s threshold=%s",
                    chat_bot_id,
                    len(rows),
                    len(relaxed_rows or []),
                    len(merged_rows),
                    relaxed_threshold,
                )
                rows = merged_rows

    if start_urls_order in {"shuffle", "random", "rand", "randomize", "mixed"} and isinstance(rows, list):
        random.shuffle(rows)

    # 3. 변수 초기화 및 필터링 로직 (메모리 저장용)
    all_valid_items = [] # 기능: 패턴 검사를 통과한 정예 데이터들을 일시 보관함
    seen = set() # 기능: 중복 URL 수집 방지용 세트
    if filtered_memory_storage is None: filtered_memory_storage = []
    reference_pattern_key = _reference_structure_pattern_key(contents_url)
    if reference_pattern_key:
        pattern_index = group_urls_by_structure_pattern(rows if isinstance(rows, list) else [])
        candidate_rows = list(pattern_index.get(reference_pattern_key) or [])
        logger.info(
            "[START_URLS_PATTERN] memory grouping applied | chat_bot_id=%s candidates=%s groups=%s matched=%s pattern=%s",
            chat_bot_id,
            len(rows) if isinstance(rows, list) else -1,
            len(pattern_index),
            len(candidate_rows),
            reference_pattern_key,
        )
        rows = candidate_rows
        logger.info(
            "[START_URLS_PATTERN] reference structure filter enabled | chat_bot_id=%s pattern=%s contents_url=%s",
            chat_bot_id,
            reference_pattern_key,
            (str(current_contents_url or "")[:180] if current_contents_url else ""),
        )

    async def _collect_with_keyword_match() -> None:
        """기능: CATEGORY url/query 규칙에 매칭되는 URL만 추출"""
        scanned_count = 0
        domain_skipped_count = 0
        unmatched_count = 0
        list_skipped_count = 0
        matched_count = 0
        domain_skipped_samples: List[str] = []
        unmatched_samples: List[str] = []
        list_skipped_samples: List[str] = []
        matched_samples: List[Dict[str, Any]] = []
        for r in rows:
            u = r.get("url")
            if not u: continue
            try:
                scanned_count += 1
                url = ensure_url_scheme(str(u).strip())
                if url in seen: continue
                row_type = str(r.get("type") or "").strip().lower() if isinstance(r, dict) else ""
                if final_domains and not _url_host_matches_scope_domains(url, final_domains, path_prefix=effective_path_prefix):
                    domain_skipped_count += 1
                    if len(domain_skipped_samples) < 5:
                        domain_skipped_samples.append(url[:200])
                    continue
                if not _url_matches_reference_structure_pattern(url, reference_pattern_key):
                    unmatched_count += 1
                    if len(unmatched_samples) < 5:
                        unmatched_samples.append(url[:200])
                    continue

                # CATEGORY url/query 매칭 + cate 결정은 resolve 함수 하나로 통일
                resolved_pair = resolve_cate_for_detail_url(url, filters_obj_for_cate)
                if resolved_pair is None and row_type != "post":
                    unmatched_count += 1
                    if len(unmatched_samples) < 5:
                        unmatched_samples.append(url[:200])
                    continue
                temporary_post_match = bool(row_type == "" and resolved_pair is not None)
                if temporary_post_match and not _temporary_post_url_is_detail_candidate(url):
                    list_skipped_count += 1
                    if len(list_skipped_samples) < 5:
                        list_skipped_samples.append(url[:200])
                    continue
                if temporary_post_match:
                    await mark_exploration_url_as_post_for_temporary_category_match(
                        db_name=db_name,
                        chat_bot_id=chat_bot_id,
                        url=url,
                        raw_url=str(u or "").strip(),
                        source="board",
                    )
                c1, c2 = resolved_pair if resolved_pair is not None else ("", "")
                seen.add(url)
                _cate_type = f"cate_match|{c1}|{c2}" if (c1 or c2) else "post"
                item = {
                    "url": url,
                    "type": "post",
                    "cate_match": _cate_type,
                }
                if temporary_post_match:
                    item["force_relearn"] = True
                    item["temporary_post_match"] = True
                    item["disable_playwright"] = True
                all_valid_items.append(item)
                matched_count += 1
                if len(matched_samples) < 5:
                    matched_samples.append({"url": url[:200], "cate1": c1, "cate2": c2})
            except: continue
        logger.info(
            "[START_URLS_RULE_TRACE][board] match summary | chat_bot_id=%s scanned=%s matched=%s unmatched=%s temp_list_skipped=%s domain_skipped=%s sample_matched=%s sample_unmatched=%s sample_temp_list_skipped=%s sample_domain_skipped=%s",
            chat_bot_id,
            scanned_count,
            matched_count,
            unmatched_count,
            list_skipped_count,
            domain_skipped_count,
            matched_samples,
            unmatched_samples,
            list_skipped_samples,
            domain_skipped_samples,
        )

    # 4. 필터링 실행 및 메모물 동기화
    if has_category_url_rules and stream_matched_rules_only:
        await _collect_with_keyword_match()
        # [수정] 기능: 추출된 정예 데이터들을 외부 저장소(filtered_memory_storage)에 한꺼번에 추가함
        filtered_memory_storage.extend(all_valid_items)

    # 5. 배치 전달: rule 매칭만 쓸 때만 정예 목록, 그 외(type=post DB 행 전체)
    if has_category_url_rules and stream_matched_rules_only:
        to_stream = all_valid_items
    else:
        to_stream = []
        for row in rows:
            try:
                raw_url = row.get("url") if isinstance(row, dict) else None
                url = ensure_url_scheme(str(raw_url).strip()) if raw_url else ""
                row_type = str(row.get("type") or "").strip().lower() if isinstance(row, dict) else ""
            except Exception:
                url = ""
                row_type = ""
            if not url:
                continue
            if final_domains and not _url_host_matches_scope_domains(url, final_domains, path_prefix=effective_path_prefix):
                continue
            if not _url_matches_reference_structure_pattern(url, reference_pattern_key):
                continue
            if row_type != "post":
                resolved_pair = resolve_cate_for_detail_url(url, filters_obj_for_cate) if has_category_url_rules else None
                if resolved_pair is None:
                    continue
                if row_type == "" and not _temporary_post_url_is_detail_candidate(url):
                    logger.info(
                        "[START_URLS_TEMP_POST] list url skipped | source=board db=%s chat_bot_id=%s url=%s",
                        db_name,
                        chat_bot_id,
                        url[:220],
                    )
                    continue
                row = dict(row)
                row["type"] = "post"
                if row_type == "":
                    await mark_exploration_url_as_post_for_temporary_category_match(
                        db_name=db_name,
                        chat_bot_id=chat_bot_id,
                        url=url,
                        raw_url=str(raw_url or "").strip(),
                        source="board",
                    )
                    row["force_relearn"] = True
                    row["temporary_post_match"] = True
                    row["disable_playwright"] = True
                    c1, c2 = resolved_pair if resolved_pair is not None else ("", "")
                    if c1 or c2:
                        row["cate_match"] = f"cate_match|{c1}|{c2}"
            to_stream.append(row)
    logger.info(
        "[START_URLS_RULE_TRACE][board] emit summary | chat_bot_id=%s order_by=%s has_rules=%s stream_matched_rules_only=%s db_row_count=%s emit_count=%s sample=%s",
        chat_bot_id,
        order_by,
        has_category_url_rules,
        stream_matched_rules_only,
        len(rows) if isinstance(rows, list) else -1,
        len(to_stream),
        (to_stream or [])[:5],
    )
    for i in range(0, len(to_stream), batch_size):
        yield to_stream[i : i + batch_size]
        
def save_crawled_urls_report(
    db_name: Optional[str] = None,
    crawled_urls: Optional[List[str]] = None,
    job_id: Optional[str] = None,
    target_domains: Optional[List[str]] = None,
    status: str = "completed",
    **kwargs
) -> Optional[str]:
    """
    크롤링 종료 후 실제 크롤링이 진행된 URL 목록을 별도 JSON 파일로 저장한다.
    targets_*.json(크롤링 대상)과 대응하여 crawled_*.json(실제 진행된 URL)로 체크용으로 사용.

    :param db_name: DB/계정명 (파일명에 사용)
    :param crawled_urls: 실제 크롤링 진행된 URL 리스트
    :param job_id: 작업 ID (메타정보)
    :param target_domains: 대상 도메인 (메타정보)
    :param status: 완료 상태 (completed/stopped/cancelled/error)
    :return: 저장된 파일 경로, 실패 시 None
    """
    if not crawled_urls:
        logger.info("[CrawledReport] No crawled URLs to save. Skipping report.")
        return None
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_db_name = str(db_name or "unknown").replace(":", "_")
        filename = f"crawled_{safe_db_name}_{timestamp}.json"
        file_path = os.path.join(current_dir, filename)

        report_data = {
            "db_name": db_name,
            "job_id": job_id,
            "target_domains": target_domains or [],
            "total_crawled": len(crawled_urls),
            "crawled_at": datetime.now().isoformat(),
            "status": status,
            "urls": [{"url": u} if isinstance(u, str) else u for u in crawled_urls],
        }

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)

        logger.info(
            "[CrawledReport] SUCCESS: Crawled URL list saved -> %s (count=%s)",
            file_path,
            len(crawled_urls),
        )
        return file_path
    except Exception as e:
        logger.error("[CrawledReport] Failed to save crawled URLs report: %s", e)
        return None


def resolve_workflow_class_for_colle(colle: str):
    """
    프론트 `colle` 값에 따라 게시판 본문 vs 첨부 다운로드 워크플로 클래스를 고른다.
    인스턴스 생성·속성 주입은 workflow_dispatch_assembly.assemble_workflow_after_url_resolve 에서 수행한다.
    """
    cm = str(colle or "").strip().lower()
    if cm == "file":
        from backend.file.file_download_workflow import FileDownloadWorkflow

        return FileDownloadWorkflow
    from backend.board.board_content_workflow import BoardContentWorkflow

    return BoardContentWorkflow


# 하위 호환: 예전 이름으로 import 하는 코드용 별칭(조회는 url_pattern 컬럼만).
get_url_filters_raw = get_url_pattern_raw
