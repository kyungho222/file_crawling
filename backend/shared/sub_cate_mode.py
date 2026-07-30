from __future__ import annotations

import os
import time
from typing import Any, Dict, Tuple


SUB_CATE_OVERWRITE = "new"
SUB_CATE_FILL_EMPTY = "emp"
_SUB_CATE_MODE_CACHE: Dict[Tuple[str, str], Tuple[float, str]] = {}


def _sub_cate_mode_cache_ttl_sec() -> float:
    try:
        return max(0.0, float(os.getenv("SUB_CATE_MODE_CACHE_TTL_SEC", "300") or "300"))
    except Exception:
        return 300.0


def normalize_sub_cate_mode(value: Any) -> str:
    mode = str(value or "").strip().lower()
    return SUB_CATE_OVERWRITE if mode == SUB_CATE_OVERWRITE else SUB_CATE_FILL_EMPTY


def is_sub_cate_overwrite(mode: Any) -> bool:
    return normalize_sub_cate_mode(mode) == SUB_CATE_OVERWRITE


async def get_sub_cate_mode_from_config(
    chat_bot_id: str | None,
    dbname: str = "chatty",
) -> str:
    cache_key = (str(dbname or "").strip(), str(chat_bot_id or "").strip())
    ttl_sec = _sub_cate_mode_cache_ttl_sec()
    now = time.monotonic()
    if ttl_sec > 0:
        cached = _SUB_CATE_MODE_CACHE.get(cache_key)
        if cached and now - cached[0] <= ttl_sec:
            return cached[1]

    from db.crawl_db_manager import get_config_values_by_keys

    values = await get_config_values_by_keys(
        chat_bot_id,
        ["sub_cate"],
        dbname=dbname,
        match_chat_bot_id=True,
    )
    mode = normalize_sub_cate_mode(values.get("sub_cate"))
    if ttl_sec > 0:
        _SUB_CATE_MODE_CACHE[cache_key] = (now, mode)
    return mode


def should_update_category_field(mode: Any, existing_value: Any, incoming_value: Any) -> bool:
    existing = str(existing_value or "").strip()
    incoming = str(incoming_value or "").strip()
    if not incoming:
        return False
    if is_sub_cate_overwrite(mode):
        return existing != incoming
    return not existing


def merge_category_pair(
    mode: Any,
    existing_cate1: Any,
    existing_cate2: Any,
    incoming_cate1: Any,
    incoming_cate2: Any,
    *,
    has_cate2: bool = True,
) -> Tuple[str, str]:
    current_cate1 = str(existing_cate1 or "").strip()
    current_cate2 = str(existing_cate2 or "").strip()
    new_cate1 = str(incoming_cate1 or "").strip()
    new_cate2 = str(incoming_cate2 or "").strip()

    if is_sub_cate_overwrite(mode):
        return (
            new_cate1 or current_cate1,
            (new_cate2 or current_cate2) if has_cate2 else "",
        )

    return (
        current_cate1 or new_cate1,
        (current_cate2 or new_cate2) if has_cate2 else "",
    )
