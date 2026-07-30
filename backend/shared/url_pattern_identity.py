from __future__ import annotations

import re
from typing import Any, Iterable, List, Tuple
from urllib.parse import parse_qsl, urlparse, urlunparse

from utils.crawl_url_normalizer import canonicalize_crawl_url
from utils.url import ensure_url_scheme

_NUMERIC_SEGMENT_RE = re.compile(r"^\d+$")


def canonical_url_key(raw_url: Any) -> str:
    """Key for exact URL identity after crawl URL normalization."""
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        if "://" in text or text.lower().startswith("www."):
            text = ensure_url_scheme(text)
    except Exception:
        pass
    try:
        return (canonicalize_crawl_url(text) or text).strip().rstrip("/")
    except Exception:
        return text.lower().rstrip("/")


def _structure_path(path: str) -> Tuple[str, bool]:
    parts: List[str] = []
    has_variable = False
    for segment in str(path or "/").split("/"):
        if not segment:
            continue
        if _NUMERIC_SEGMENT_RE.fullmatch(segment):
            parts.append("{num}")
            has_variable = True
        else:
            parts.append(segment)
    return "/" + "/".join(parts), has_variable


def url_structure_pattern_key(raw_url: Any) -> str:
    """
    Key for same-structure URL identity.

    Numeric path segments are variable, and query values are ignored. Query keys
    are preserved and sorted, so `?id=1&sort=a` and `?sort=b&id=2` share a key.
    """
    canonical = canonical_url_key(raw_url)
    if not canonical:
        return ""
    try:
        parsed = urlparse(canonical)
    except Exception:
        return canonical

    path, _has_variable = _structure_path(parsed.path or "/")
    query_keys = sorted(
        {
            str(key or "").strip().lower()
            for key, _value in parse_qsl(parsed.query or "", keep_blank_values=False)
            if str(key or "").strip()
        }
    )
    query = "&".join(query_keys)
    return urlunparse(
        (
            (parsed.scheme or "https").lower(),
            (parsed.netloc or "").lower(),
            path,
            "",
            query,
            "",
        )
    ).rstrip("/")


def url_structure_pattern_has_variable(raw_url: Any) -> bool:
    canonical = canonical_url_key(raw_url)
    if not canonical:
        return False
    try:
        parsed = urlparse(canonical)
    except Exception:
        return False
    _path, has_variable = _structure_path(parsed.path or "/")
    return has_variable


def group_urls_by_structure_pattern(urls: Iterable[Any]) -> dict[str, List[Any]]:
    grouped: dict[str, List[Any]] = {}
    for item in urls or []:
        raw_url = item.get("url") if isinstance(item, dict) else item
        key = url_structure_pattern_key(raw_url)
        if key:
            grouped.setdefault(key, []).append(item)
    return grouped


__all__ = [
    "canonical_url_key",
    "url_structure_pattern_key",
    "url_structure_pattern_has_variable",
    "group_urls_by_structure_pattern",
]
