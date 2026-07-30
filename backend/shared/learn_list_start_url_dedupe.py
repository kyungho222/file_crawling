import logging
import os
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlparse

from db.rdbms_router import rdbms_execute_query
from backend.shared.url_pattern_identity import canonical_url_key

logger = logging.getLogger("backend.shared.learn_list_start_url_dedupe")

_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_SAFE_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")
_LOADED_KEY_CACHE: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}


def learn_list_start_url_dedupe_enabled(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    raw = (
        data.get("learn_list_duplicate_exclude_enabled")
        if "learn_list_duplicate_exclude_enabled" in data
        else data.get("learnListDuplicateExcludeEnabled")
    )
    if raw is None:
        raw = data.get("learn_list_duplicate_exclude")
    if raw is None:
        raw = data.get("learnListDuplicateExclude")
    if raw is None:
        raw = os.getenv("LEARN_LIST_START_URL_DEDUPE_DEFAULT_ENABLED", "1")
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return raw != 0
    if isinstance(raw, str):
        return raw.strip().lower() in {"1", "true", "y", "yes", "on", "enabled"}
    return False


def _is_file_crawl_mode(data: Dict[str, Any]) -> bool:
    if not isinstance(data, dict):
        return False
    values = (
        data.get("colle"),
        data.get("ui_colle"),
        data.get("colle_mode"),
        data.get("content_type"),
    )
    return any(str(value or "").strip().lower() in {"file", "attach", "attachment"} for value in values)


def _learn_table_name(chat_bot_id: Optional[str]) -> str:
    text = str(chat_bot_id or "").strip()
    if not text:
        return "ASADAL_CRAWLING_LEARN_LIST"
    suffix = text.split("-")[-1].strip().lower()
    if not suffix:
        suffix = text.replace("-", "")[-12:].lower()
    if not suffix or not re.fullmatch(r"[A-Za-z0-9_]+", suffix):
        return "ASADAL_CRAWLING_LEARN_LIST"
    return f"ASADAL_{suffix}_LEARN_LIST"


async def _resolve_learn_table_name(db_name: str, chat_bot_id: Optional[str]) -> str:
    try:
        from db.mariadb_save_update import resolve_learn_list_table_name_for_chatbot

        resolved = await resolve_learn_list_table_name_for_chatbot(str(chat_bot_id or ""), str(db_name or ""))
        resolved = str(resolved or "").strip()
        if resolved and _SAFE_TABLE_RE.fullmatch(resolved):
            return resolved
    except Exception as exc:
        logger.warning(
            "[LearnListStartUrlDedupe] learn_list table resolve fallback | db=%s chat_bot_id=%s err=%s",
            db_name,
            chat_bot_id,
            exc,
        )
    return _learn_table_name(chat_bot_id)


def _item_url(item: Any) -> str:
    if isinstance(item, dict):
        for key in ("url", "content", "source_url", "href"):
            value = str(item.get(key) or "").strip()
            if value:
                return value
        return ""
    return str(item or "").strip()


def _canonical_url_key(raw_url: Any) -> str:
    return canonical_url_key(raw_url)


def _query_keys(raw_url: Any) -> List[str]:
    try:
        parsed = urlparse(str(raw_url or "").strip())
        return sorted(
            {
                str(key or "").strip().lower()
                for key, _value in parse_qsl(parsed.query or "", keep_blank_values=True)
                if str(key or "").strip()
            }
        )
    except Exception:
        return []


def _missing_required_detail_query_reason(raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    keys = set(_query_keys(text))
    if "ne.go.kr" in host and path.endswith("/platform/user/pblccomm/bd_pblcdiscussionview.do") and "discussionid" not in keys:
        return "missing_required_detail_query:discussionid"
    return ""


def _url_key_variants(raw_url: Any) -> Set[str]:
    key = _canonical_url_key(raw_url)
    if not key:
        return set()
    variants = {key}
    if key.startswith("https://"):
        variants.add(key.replace("https://", "http://", 1))
    elif key.startswith("http://"):
        variants.add(key.replace("http://", "https://", 1))
    return variants


def _cache_identity(
    db_name: str,
    chat_bot_id: Optional[str],
    table_name: str,
    job_id: Optional[str] = None,
) -> Tuple[str, str, str, str]:
    return (
        str(db_name or "").strip(),
        str(chat_bot_id or "").strip(),
        str(table_name or "").strip(),
        str(job_id or "").strip(),
    )


def _loaded_key_cache_ttl_sec() -> float:
    try:
        value = float(os.getenv("LEARN_LIST_START_URL_DEDUPE_CACHE_TTL_SEC", "300") or "300")
    except Exception:
        value = 300.0
    return max(0.0, min(value, 3600.0))


def _get_loaded_key_cache(
    *,
    db_name: str,
    chat_bot_id: Optional[str],
    table_name: str,
    job_id: Optional[str] = None,
    min_loaded: int = 1,
) -> Optional[Dict[str, Any]]:
    cached = _LOADED_KEY_CACHE.get(_cache_identity(db_name, chat_bot_id, table_name, job_id))
    if not isinstance(cached, dict):
        return None
    keys = cached.get("keys")
    if not isinstance(keys, set):
        return None
    if len(keys) < max(0, int(min_loaded or 0)):
        return None
    ttl = _loaded_key_cache_ttl_sec()
    if ttl > 0 and time.monotonic() - float(cached.get("loaded_at") or 0.0) > ttl:
        return None
    return cached


def remember_loaded_learn_list_url_keys(
    *,
    db_name: str,
    chat_bot_id: Optional[str],
    table_name: str,
    job_id: Optional[str] = None,
    keys: Set[str],
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    db = str(db_name or "").strip()
    table = str(table_name or "").strip()
    if not db or not table:
        return
    safe_keys = {str(key or "").strip() for key in (keys or set()) if str(key or "").strip()}
    job = str(job_id or "").strip()
    _LOADED_KEY_CACHE[_cache_identity(db, chat_bot_id, table, job)] = {
        "db_name": db,
        "chat_bot_id": str(chat_bot_id or "").strip(),
        "table": table,
        "job_id": job,
        "keys": safe_keys,
        "loaded": len(safe_keys),
        "limit": int((meta or {}).get("limit") or len(safe_keys)),
        "meta": dict(meta or {}),
        "loaded_at": time.monotonic(),
    }


def find_loaded_learn_list_url_key(
    *,
    db_name: str,
    chat_bot_id: Optional[str],
    candidate_url: Any,
    job_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Lookup only URL keys loaded earlier by start-url dedupe. This never touches DB."""
    db = str(db_name or "").strip()
    chat = str(chat_bot_id or "").strip()
    job = str(job_id or "").strip()
    if not db:
        return None
    variants = _url_key_variants(candidate_url)
    if not variants:
        return None
    for (cache_db, cache_chat, table, cache_job), cached in list(_LOADED_KEY_CACHE.items()):
        if cache_db != db:
            continue
        if job and cache_job != job:
            continue
        if chat and cache_chat and cache_chat != chat:
            continue
        keys = (cached or {}).get("keys")
        if not isinstance(keys, set):
            continue
        matched = variants & keys
        if matched:
            return {
                "table": table,
                "job_id": cache_job,
                "matched_key": next(iter(matched)),
                "loaded": int((cached or {}).get("loaded") or len(keys)),
                "loaded_at": float((cached or {}).get("loaded_at") or 0.0),
            }
    return None


def _extract_url_keys_from_content(value: Any) -> Set[str]:
    text = str(value or "").strip()
    if not text:
        return set()
    candidates = [text]
    candidates.extend(match.group(0).rstrip(".,);]") for match in _URL_RE.finditer(text))
    keys = {_canonical_url_key(candidate) for candidate in candidates}
    return {key for key in keys if key and key.startswith(("http://", "https://"))}


async def load_learn_list_url_keys(
    *,
    db_name: str,
    chat_bot_id: Optional[str],
    job_id: Optional[str] = None,
    limit: int = 200000,
) -> Tuple[Set[str], Dict[str, Any]]:
    table_name = await _resolve_learn_table_name(db_name, chat_bot_id)
    if not _SAFE_TABLE_RE.fullmatch(table_name):
        return set(), {"table": table_name, "loaded": 0, "error": "unsafe_table_name"}

    capped_limit = max(1, min(int(limit or 200000), 1000000))
    cached = _get_loaded_key_cache(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        table_name=table_name,
        job_id=job_id,
        min_loaded=1,
    )
    if cached and int(cached.get("limit") or 0) >= capped_limit:
        keys = set(cached.get("keys") or set())
        meta = dict(cached.get("meta") or {})
        meta.update(
            {
                "table": table_name,
                "loaded": len(keys),
                "cache_hit": True,
                "cache_source": "loaded_learn_list_url_keys",
                "limit": int(cached.get("limit") or capped_limit),
            }
        )
        logger.info(
            "[LearnListStartUrlDedupe] cache hit | db=%s chat_bot_id=%s job_id=%s table=%s loaded=%s limit=%s",
            db_name,
            chat_bot_id,
            job_id,
            table_name,
            len(keys),
            meta.get("limit"),
        )
        return keys, meta

    try:
        rows = await rdbms_execute_query(
            f"""
            SELECT `content`
            FROM `{table_name}`
            WHERE `content` IS NOT NULL
              AND TRIM(CAST(`content` AS CHAR)) <> ''
              AND LOWER(COALESCE(`content_type`, '')) = 'url'
            ORDER BY `id` DESC
            LIMIT %s
            """,
            (capped_limit,),
            fetch=True,
            dbname=db_name,
        )
    except Exception as exc:
        logger.warning(
            "[LearnListStartUrlDedupe] learn_list load skipped | db=%s table=%s err=%s",
            db_name,
            table_name,
            exc,
        )
        return set(), {"table": table_name, "loaded": 0, "error": str(exc)}

    keys: Set[str] = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        keys.update(_extract_url_keys_from_content(row.get("content")))

    meta = {"table": table_name, "loaded": len(keys), "rows": len(rows or []), "limit": capped_limit}
    remember_loaded_learn_list_url_keys(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        table_name=table_name,
        job_id=job_id,
        keys=keys,
        meta=meta,
    )
    return keys, meta


async def load_existing_learn_list_url_keys_for_start_urls(
    *,
    db_name: str,
    chat_bot_id: Optional[str],
    start_urls: Iterable[Any],
) -> Tuple[Set[str], Dict[str, Any]]:
    table_name = await _resolve_learn_table_name(db_name, chat_bot_id)
    if not _SAFE_TABLE_RE.fullmatch(table_name):
        return set(), {"table": table_name, "loaded": 0, "error": "unsafe_table_name"}

    candidates: List[str] = []
    seen: Set[str] = set()
    for item in start_urls or []:
        raw_url = _item_url(item)
        variants = _url_key_variants(raw_url)
        key = _canonical_url_key(raw_url)
        if not key or key in seen:
            continue
        seen.update(variants)
        candidates.append(key)
        for alt in sorted(variants - {key}):
            candidates.append(alt)

    if not candidates:
        return set(), {"table": table_name, "loaded": 0, "rows": 0, "candidate_count": 0}

    cached = _get_loaded_key_cache(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        table_name=table_name,
        min_loaded=1,
    )
    if cached:
        cached_keys = set(cached.get("keys") or set())
        existing = {key for key in seen if key in cached_keys}
        meta = {
            "table": table_name,
            "loaded": len(existing),
            "rows": int((cached.get("meta") or {}).get("rows") or 0),
            "candidate_count": len(candidates),
            "lookup": "targeted_cache",
            "cache_hit": True,
            "cache_source": "loaded_learn_list_url_keys",
        }
        remember_loaded_learn_list_url_keys(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            table_name=table_name,
            keys=cached_keys,
            meta=dict(cached.get("meta") or {}),
        )
        logger.info(
            "[LearnListStartUrlDedupe] targeted cache hit | db=%s chat_bot_id=%s table=%s candidates=%s matched=%s cached=%s",
            db_name,
            chat_bot_id,
            table_name,
            len(candidates),
            len(existing),
            len(cached_keys),
        )
        return existing, meta

    existing: Set[str] = set()
    rows_total = 0
    chunk_size = 200
    try:
        for offset in range(0, len(candidates), chunk_size):
            chunk = candidates[offset : offset + chunk_size]
            placeholders = ", ".join(["%s"] * len(chunk))
            rows = await rdbms_execute_query(
                f"""
                SELECT `content`
                FROM `{table_name}`
                WHERE `content_type` = %s
                  AND `content` IN ({placeholders})
                """,
                ("url", *chunk),
                fetch=True,
                dbname=db_name,
            )
            rows_total += len(rows or [])
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                existing.update(_extract_url_keys_from_content(row.get("content")))
    except Exception as exc:
        logger.warning(
            "[LearnListStartUrlDedupe] targeted learn_list lookup skipped | db=%s table=%s candidates=%s err=%s",
            db_name,
            table_name,
            len(candidates),
            exc,
        )
        return set(), {"table": table_name, "loaded": 0, "error": str(exc), "candidate_count": len(candidates)}

    meta = {
        "table": table_name,
        "loaded": len(existing),
        "rows": rows_total,
        "candidate_count": len(candidates),
        "lookup": "targeted",
    }
    remember_loaded_learn_list_url_keys(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        table_name=table_name,
        keys=existing,
        meta=meta,
    )
    return existing, meta


def filter_start_urls_against_learn_list(
    start_urls: Iterable[Any],
    learn_url_keys: Set[str],
) -> Tuple[List[Any], Dict[str, Any]]:
    filtered: List[Any] = []
    duplicate_samples: List[str] = []
    duplicate_detail_samples: List[Dict[str, Any]] = []
    seen_new: Set[str] = set()
    before = 0
    duplicates = 0
    duplicate_by_learn_list = 0
    duplicate_by_seen_new = 0
    missing_required_query_count = 0
    unique_key_count = 0

    for item in start_urls or []:
        before += 1
        raw_url = _item_url(item)
        variants = _url_key_variants(raw_url)
        missing_reason = _missing_required_detail_query_reason(raw_url)
        if missing_reason:
            missing_required_query_count += 1
        matched_learn = variants & learn_url_keys if variants else set()
        matched_seen = variants & seen_new if variants else set()
        if variants and (matched_learn or matched_seen):
            duplicates += 1
            if matched_learn:
                duplicate_by_learn_list += 1
            if matched_seen:
                duplicate_by_seen_new += 1
            if len(duplicate_samples) < 5:
                duplicate_samples.append(raw_url[:240])
            if len(duplicate_detail_samples) < 5:
                duplicate_detail_samples.append(
                    {
                        "url": raw_url[:240],
                        "key": (_canonical_url_key(raw_url) or "")[:240],
                        "query_keys": _query_keys(raw_url),
                        "reason": "learn_list" if matched_learn else "seen_new",
                        "missing_required_query": missing_reason,
                        "matched_key": (next(iter(matched_learn or matched_seen)) if (matched_learn or matched_seen) else "")[:240],
                    }
                )
            continue
        if variants:
            seen_new.update(variants)
            unique_key_count += 1
        filtered.append(item)

    if before and duplicates and len(filtered) <= max(1, int(before * 0.05)):
        logger.warning(
            "[FileUrlTrace][learn_list_dedupe.high_collapse] before=%s after=%s duplicates=%s by_learn_list=%s by_seen_new=%s unique_keys=%s missing_required_query=%s samples=%s",
            before,
            len(filtered),
            duplicates,
            duplicate_by_learn_list,
            duplicate_by_seen_new,
            unique_key_count,
            missing_required_query_count,
            duplicate_detail_samples,
        )
    return filtered, {
        "before": before,
        "after": len(filtered),
        "duplicates": duplicates,
        "duplicate_by_learn_list": duplicate_by_learn_list,
        "duplicate_by_seen_new": duplicate_by_seen_new,
        "unique_key_count": unique_key_count,
        "missing_required_query_count": missing_required_query_count,
        "duplicate_samples": duplicate_samples,
        "duplicate_detail_samples": duplicate_detail_samples,
    }


def filter_start_urls_against_loaded_learn_list_cache(
    *,
    db_name: str,
    chat_bot_id: Optional[str],
    job_id: Optional[str] = None,
    start_urls: Iterable[Any],
) -> Tuple[List[Any], Dict[str, Any]]:
    """Filter start URLs using only the in-process learn_list URL cache; never queries DB."""
    filtered: List[Any] = []
    duplicate_samples: List[str] = []
    duplicate_detail_samples: List[Dict[str, Any]] = []
    seen_new: Set[str] = set()
    before = 0
    duplicates = 0
    duplicate_by_cache = 0
    duplicate_by_seen_new = 0
    missing_required_query_count = 0
    unique_key_count = 0
    cache_loaded = 0
    cache_table = ""

    for item in start_urls or []:
        before += 1
        raw_url = _item_url(item)
        variants = _url_key_variants(raw_url)
        missing_reason = _missing_required_detail_query_reason(raw_url)
        if missing_reason:
            missing_required_query_count += 1
        cached_hit = find_loaded_learn_list_url_key(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            job_id=job_id,
            candidate_url=raw_url,
        )
        if cached_hit:
            cache_loaded = max(cache_loaded, int(cached_hit.get("loaded") or 0))
            cache_table = cache_table or str(cached_hit.get("table") or "")
        matched_seen = variants & seen_new if variants else set()
        if variants and (cached_hit or matched_seen):
            duplicates += 1
            if cached_hit:
                duplicate_by_cache += 1
            if matched_seen:
                duplicate_by_seen_new += 1
            if len(duplicate_samples) < 5:
                duplicate_samples.append(raw_url[:240])
            if len(duplicate_detail_samples) < 5:
                duplicate_detail_samples.append(
                    {
                        "url": raw_url[:240],
                        "key": (_canonical_url_key(raw_url) or "")[:240],
                        "query_keys": _query_keys(raw_url),
                        "reason": "cache" if cached_hit else "seen_new",
                        "missing_required_query": missing_reason,
                        "matched_key": str((cached_hit or {}).get("matched_key") or (next(iter(matched_seen)) if matched_seen else ""))[:240],
                    }
                )
            continue
        if variants:
            seen_new.update(variants)
            unique_key_count += 1
        filtered.append(item)

    if before and duplicates and len(filtered) <= max(1, int(before * 0.05)):
        logger.warning(
            "[FileUrlTrace][learn_list_dedupe.loaded_cache_high_collapse] before=%s after=%s duplicates=%s by_cache=%s by_seen_new=%s unique_keys=%s missing_required_query=%s samples=%s",
            before,
            len(filtered),
            duplicates,
            duplicate_by_cache,
            duplicate_by_seen_new,
            unique_key_count,
            missing_required_query_count,
            duplicate_detail_samples,
        )
    return filtered, {
        "enabled": True,
        "lookup": "loaded_cache_only",
        "cache_source": "loaded_learn_list_url_keys",
        "table": cache_table,
        "loaded": cache_loaded,
        "before": before,
        "after": len(filtered),
        "duplicates": duplicates,
        "duplicate_by_cache": duplicate_by_cache,
        "duplicate_by_seen_new": duplicate_by_seen_new,
        "unique_key_count": unique_key_count,
        "missing_required_query_count": missing_required_query_count,
        "duplicate_samples": duplicate_samples,
        "duplicate_detail_samples": duplicate_detail_samples,
    }


async def apply_learn_list_start_url_dedupe(
    *,
    data: Dict[str, Any],
    start_urls: List[Any],
    db_name: str,
    chat_bot_id: Optional[str],
) -> Tuple[List[Any], Dict[str, Any]]:
    if _is_file_crawl_mode(data):
        logger.info(
            "[FileUrlTrace][learn_list_dedupe.skip_file_crawl] db=%s chat_bot_id=%s count=%s reason=file_crawl_post_urls_must_not_match_board_url_duplicates",
            db_name,
            chat_bot_id,
            len(start_urls or []),
        )
        return start_urls, {
            "enabled": False,
            "skipped_reason": "file_crawl_post_urls_must_not_match_board_url_duplicates",
            "before": len(start_urls or []),
            "after": len(start_urls or []),
            "duplicates": 0,
        }
    if not learn_list_start_url_dedupe_enabled(data):
        return start_urls, {"enabled": False}

    raw_full_scan = data.get("learn_list_duplicate_exclude_full_scan")
    if raw_full_scan is None:
        raw_full_scan = os.getenv("LEARN_LIST_START_URL_DEDUPE_FULL_SCAN", "1")
    use_full_scan = True
    if isinstance(raw_full_scan, bool):
        use_full_scan = raw_full_scan
    elif isinstance(raw_full_scan, (int, float)):
        use_full_scan = raw_full_scan != 0
    elif isinstance(raw_full_scan, str):
        use_full_scan = raw_full_scan.strip().lower() not in {"0", "false", "n", "no", "off", "targeted"}

    if use_full_scan:
        try:
            limit = int(
                data.get("learn_list_duplicate_exclude_limit")
                or data.get("learnListDuplicateExcludeLimit")
                or os.getenv("LEARN_LIST_START_URL_DEDUPE_LIMIT", "200000")
                or "200000"
            )
        except Exception:
            limit = 200000
        learn_url_keys, load_meta = await load_learn_list_url_keys(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            job_id=(data or {}).get("job_id"),
            limit=limit,
        )
        load_meta["lookup"] = "full_scan"
        load_meta["limit"] = max(1, min(int(limit or 200000), 1000000))
    else:
        learn_url_keys, load_meta = await load_existing_learn_list_url_keys_for_start_urls(
            db_name=db_name,
            chat_bot_id=chat_bot_id,
            start_urls=start_urls,
        )
    filtered, filter_meta = filter_start_urls_against_learn_list(start_urls, learn_url_keys)
    meta = {
        "enabled": True,
        **load_meta,
        **filter_meta,
    }
    logger.info(
        "[LearnListStartUrlDedupe] applied | db=%s chat_bot_id=%s table=%s before=%s after=%s duplicates=%s by_learn_list=%s by_seen_new=%s unique_keys=%s missing_required_query=%s loaded=%s samples=%s detail_samples=%s",
        db_name,
        chat_bot_id,
        meta.get("table"),
        meta.get("before"),
        meta.get("after"),
        meta.get("duplicates"),
        meta.get("duplicate_by_learn_list"),
        meta.get("duplicate_by_seen_new"),
        meta.get("unique_key_count"),
        meta.get("missing_required_query_count"),
        meta.get("loaded"),
        meta.get("duplicate_samples"),
        meta.get("duplicate_detail_samples"),
    )
    return filtered, meta
