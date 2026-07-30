import asyncio
import logging
import os
import re
import time
from typing import Any, Dict, Iterable, Optional, Set, Tuple

from db.mysql_db_config import mysql_execute_query
from utils.url import canonicalize_url_for_dedup

logger = logging.getLogger("backend.shared.learn_list_url_row_cache")

_SAFE_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")
_CACHE: Dict[Tuple[str, str, Tuple[str, ...], int, str], Dict[str, Any]] = {}
_LOCKS: Dict[Tuple[str, str, Tuple[str, ...], int, str], asyncio.Lock] = {}


def learn_list_url_row_cache_enabled() -> bool:
    raw = os.getenv("LEARN_LIST_URL_ROW_CACHE_ENABLED", "1")
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def learn_list_url_row_cache_limit() -> int:
    try:
        value = int(os.getenv("LEARN_LIST_URL_ROW_CACHE_LIMIT", "1000000") or "1000000")
    except Exception:
        value = 1000000
    return max(1, min(value, 1000000))


def learn_list_url_row_cache_ttl_sec() -> float:
    try:
        value = float(os.getenv("LEARN_LIST_URL_ROW_CACHE_TTL_SEC", "300") or "300")
    except Exception:
        value = 300.0
    return max(0.0, min(value, 3600.0))


def normalize_learn_list_url_key(raw_url: Any) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        return (canonicalize_url_for_dedup(text) or text).strip()
    except Exception:
        return text


def learn_list_url_key_variants(raw_url: Any) -> Set[str]:
    key = normalize_learn_list_url_key(raw_url)
    if not key:
        return set()
    variants = {key}
    if key.startswith("https://"):
        variants.add(key.replace("https://", "http://", 1))
    elif key.startswith("http://"):
        variants.add(key.replace("http://", "https://", 1))
    return {v for v in variants if v}


def find_loaded_learn_list_row_in_url_cache(
    *,
    db_name: str,
    table_name: str,
    candidate_url: Any,
    job_id: Optional[str] = None,
    ignore_ttl: bool = False,
) -> Optional[Dict[str, Any]]:
    """Lookup only already-loaded in-memory rows. This function never touches DB."""
    db = str(db_name or "").strip()
    table = str(table_name or "").strip()
    job = str(job_id or "").strip()
    if not db or not table:
        return None
    now = time.monotonic()
    ttl = learn_list_url_row_cache_ttl_sec()
    variants = learn_list_url_key_variants(candidate_url)
    if not variants:
        return None
    for cache_key, cached in list(_CACHE.items()):
        if len(cache_key) < 2 or cache_key[0] != db or cache_key[1] != table:
            continue
        if job and (len(cache_key) < 5 or cache_key[4] != job):
            continue
        if not ignore_ttl and ttl > 0 and now - float((cached or {}).get("loaded_at") or 0.0) > ttl:
            continue
        by_key = (cached or {}).get("by_key")
        if not isinstance(by_key, dict):
            continue
        for variant in variants:
            row = by_key.get(variant)
            if isinstance(row, dict):
                return dict(row)
    return None


def _clean_columns(cols: Iterable[str], available_cols: Optional[Set[str]] = None) -> Tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for col in [*list(cols or ()), "id", "content"]:
        name = str(col or "").strip()
        if not name or name in seen:
            continue
        if available_cols is not None and name not in available_cols:
            continue
        seen.add(name)
        out.append(name)
    return tuple(out or ("id", "content"))


def _cache_key(
    db_name: str,
    table_name: str,
    columns: Tuple[str, ...],
    limit: int,
    job_id: Optional[str] = None,
) -> Tuple[str, str, Tuple[str, ...], int, str]:
    return (
        str(db_name or ""),
        str(table_name or ""),
        tuple(columns),
        int(limit or 0),
        str(job_id or "").strip(),
    )


_DATE_LIKE_COLUMNS = {
    "created_at",
    "updated_at",
    "content_created_at",
    "content_updated_at",
}


def _select_expr(col: str) -> str:
    if col in _DATE_LIKE_COLUMNS:
        return f"CAST(`{col}` AS CHAR) AS `{col}`"
    return f"`{col}`"


async def get_learn_list_url_row_cache(
    *,
    db_name: str,
    table_name: str,
    columns: Iterable[str],
    available_cols: Optional[Set[str]] = None,
    limit: Optional[int] = None,
    job_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not learn_list_url_row_cache_enabled():
        return None
    table = str(table_name or "").strip()
    db = str(db_name or "").strip()
    if not db or not table or not _SAFE_TABLE_RE.fullmatch(table):
        return None
    selected_cols = _clean_columns(columns, available_cols)
    if "content" not in selected_cols:
        selected_cols = (*selected_cols, "content")
    capped_limit = max(1, min(int(limit or learn_list_url_row_cache_limit()), 1000000))
    job = str(job_id or "").strip()
    key = _cache_key(db, table, selected_cols, capped_limit, job)
    now = time.monotonic()
    ttl = learn_list_url_row_cache_ttl_sec()

    cached = _CACHE.get(key)
    if cached and (ttl <= 0 or now - float(cached.get("loaded_at") or 0.0) <= ttl):
        return cached

    lock = _LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _LOCKS[key] = lock
    async with lock:
        cached = _CACHE.get(key)
        now = time.monotonic()
        if cached and (ttl <= 0 or now - float(cached.get("loaded_at") or 0.0) <= ttl):
            return cached

        select_sql = ", ".join(_select_expr(col) for col in selected_cols)
        t0 = time.perf_counter()
        rows = await mysql_execute_query(
            (
                f"SELECT {select_sql} FROM `{table}` "
                "WHERE `content` IS NOT NULL AND TRIM(CAST(`content` AS CHAR)) <> '' "
                "ORDER BY `id` DESC LIMIT %s"
            ),
            (capped_limit,),
            fetch=True,
            dbname=db,
        )
        by_key: Dict[str, Dict[str, Any]] = {}
        row_count = 0
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            row_count += 1
            for variant in learn_list_url_key_variants(row.get("content")):
                by_key.setdefault(variant, row)

        cache = {
            "db_name": db,
            "table_name": table,
            "job_id": job,
            "columns": selected_cols,
            "limit": capped_limit,
            "rows": rows or [],
            "row_count": row_count,
            "by_key": by_key,
            "loaded_at": time.monotonic(),
        }
        _CACHE[key] = cache
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        logger.info(
            "[LearnListUrlRowCache] loaded | db=%s table=%s job_id=%s rows=%s keys=%s limit=%s elapsed_ms=%.1f",
            db,
            table,
            job,
            row_count,
            len(by_key),
            capped_limit,
            elapsed_ms,
        )
        return cache


async def find_learn_list_row_in_url_cache(
    *,
    db_name: str,
    table_name: str,
    columns: Iterable[str],
    candidate_url: Any,
    available_cols: Optional[Set[str]] = None,
    limit: Optional[int] = None,
    job_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    db = str(db_name or "").strip()
    table = str(table_name or "").strip()
    job = str(job_id or "").strip()
    now = time.monotonic()
    ttl = learn_list_url_row_cache_ttl_sec()
    for cache_key, cached in list(_CACHE.items()):
        if len(cache_key) < 2 or cache_key[0] != db or cache_key[1] != table:
            continue
        if job and (len(cache_key) < 5 or cache_key[4] != job):
            continue
        if ttl > 0 and now - float((cached or {}).get("loaded_at") or 0.0) > ttl:
            continue
        by_key = (cached or {}).get("by_key")
        if not isinstance(by_key, dict):
            continue
        for variant in learn_list_url_key_variants(candidate_url):
            row = by_key.get(variant)
            if isinstance(row, dict):
                return dict(row)

    cache = await get_learn_list_url_row_cache(
        db_name=db_name,
        table_name=table_name,
        columns=columns,
        available_cols=available_cols,
        limit=limit,
        job_id=job,
    )
    if not cache:
        return None
    by_key = cache.get("by_key")
    if not isinstance(by_key, dict):
        return None
    for variant in learn_list_url_key_variants(candidate_url):
        row = by_key.get(variant)
        if isinstance(row, dict):
            return dict(row)
    return None


def remember_learn_list_url_row(
    *,
    db_name: str,
    table_name: str,
    row: Dict[str, Any],
    job_id: Optional[str] = None,
) -> None:
    if not isinstance(row, dict) or not row.get("content"):
        return
    db = str(db_name or "").strip()
    table = str(table_name or "").strip()
    job = str(job_id or "").strip()
    for key, cache in list(_CACHE.items()):
        if len(key) < 2 or key[0] != db or key[1] != table:
            continue
        if job and (len(key) < 5 or key[4] != job):
            continue
        by_key = cache.get("by_key")
        if not isinstance(by_key, dict):
            continue
        for variant in learn_list_url_key_variants(row.get("content")):
            by_key[variant] = dict(row)


def clear_learn_list_url_row_cache(*, db_name: str = "", table_name: str = "", job_id: str = "") -> None:
    db = str(db_name or "").strip()
    table = str(table_name or "").strip()
    job = str(job_id or "").strip()
    for key in list(_CACHE.keys()):
        if db and key[0] != db:
            continue
        if table and key[1] != table:
            continue
        if job and (len(key) < 5 or key[4] != job):
            continue
        _CACHE.pop(key, None)
        _LOCKS.pop(key, None)
