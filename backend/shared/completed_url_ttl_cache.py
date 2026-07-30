from __future__ import annotations

import os
import time
from threading import Lock
from typing import Optional

from utils.url import canonicalize_url_for_dedup


_LOCK = Lock()
_CACHE: dict[str, dict[str, float]] = {
    "save": {},
    "study": {},
}


def completed_url_cache_ttl_sec() -> float:
    try:
        value = float(os.getenv("FILE_CRAWL_COMPLETED_URL_CACHE_TTL_SEC", "300") or "300")
    except Exception:
        value = 1800.0
    return max(60.0, min(value, 86400.0))


def _key(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    return canonicalize_url_for_dedup(text) or text


def remember_completed_url(url: str, *, stage: str = "study", ttl_sec: Optional[float] = None) -> None:
    key = _key(url)
    if not key:
        return
    stage_key = "save" if str(stage or "").strip().lower() == "save" else "study"
    ttl = completed_url_cache_ttl_sec() if ttl_sec is None else max(1.0, float(ttl_sec))
    expires_at = time.monotonic() + ttl
    with _LOCK:
        _CACHE.setdefault(stage_key, {})[key] = expires_at


def completed_url_cached(url: str, *, stage: str = "study", include_earlier: bool = True) -> bool:
    key = _key(url)
    if not key:
        return False
    stage_key = "save" if str(stage or "").strip().lower() == "save" else "study"
    now = time.monotonic()
    stages = [stage_key]
    if include_earlier and stage_key == "save":
        stages.append("study")
    with _LOCK:
        for sk in stages:
            bucket = _CACHE.setdefault(sk, {})
            expires_at = float(bucket.get(key, 0.0) or 0.0)
            if expires_at <= 0:
                continue
            if expires_at > now:
                return True
            bucket.pop(key, None)
    return False


def prune_completed_url_cache() -> None:
    now = time.monotonic()
    with _LOCK:
        for bucket in _CACHE.values():
            for key, expires_at in list(bucket.items()):
                if float(expires_at or 0.0) <= now:
                    bucket.pop(key, None)
