from __future__ import annotations

import re
from typing import Any, Tuple

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

from utils.url import canonicalize_url_for_dedup


def normalize_file_detail_category(value: Any) -> str:
    """Keep the meaningful detailed category and drop its display-only suffix."""

    text = str(value or "").strip()
    if not text:
        return ""

    parts = re.split(r"\s*[:\uff1a]\s*", text, maxsplit=1)
    if len(parts) == 2 and parts[0].strip().endswith("(\uc0c1\uc138)"):
        return parts[0].strip()
    return text

def normalize_file_detail_cates(cate1: Any, cate2: Any) -> Tuple[str, str]:
    return normalize_file_detail_category(cate1), normalize_file_detail_category(cate2)


def filter_unexposed_file_detail_cates(html: Any, cate1: Any, cate2: Any) -> Tuple[str, str]:
    """Remove a synthetic detailed marker while retaining the category itself."""

    normalized_cate1, normalized_cate2 = normalize_file_detail_cates(cate1, cate2)
    detail_marker = "(\uc0c1\uc138)"
    if detail_marker not in normalized_cate1 and detail_marker not in normalized_cate2:
        return normalized_cate1, normalized_cate2

    body_text = ""
    if BeautifulSoup is not None and html:
        try:
            soup = BeautifulSoup(str(html), "html.parser")
            for node in soup(["head", "script", "style", "noscript", "template"]):
                node.decompose()
            body_text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()
        except Exception:
            body_text = ""

    def _remove_synthetic_marker(category: str) -> str:
        if detail_marker not in category or category in body_text:
            return category
        return re.sub(rf"\s*{re.escape(detail_marker)}\s*", " ", category).strip()

    return (
        _remove_synthetic_marker(normalized_cate1),
        _remove_synthetic_marker(normalized_cate2),
    )


def split_detail_cates(value: Any) -> Tuple[str, str]:
    text = str(value or "").strip()
    if not text.startswith("cate_match|"):
        return "", ""
    parts = text.split("|", 2)
    if len(parts) < 3:
        return "", ""
    return normalize_file_detail_cates(parts[1], parts[2])


def resolve_file_detail_cates(
    workflow: Any,
    detail_url: str,
    *,
    cate1: Any = "",
    cate2: Any = "",
    item_type: Any = "",
) -> Tuple[str, str]:
    """Resolve file-crawl categories without depending on board crawl results."""

    resolved_cate1, resolved_cate2 = normalize_file_detail_cates(cate1, cate2)
    if resolved_cate1 or resolved_cate2:
        return resolved_cate1, resolved_cate2

    resolved_cate1, resolved_cate2 = split_detail_cates(item_type)
    if resolved_cate1 or resolved_cate2:
        return resolved_cate1, resolved_cate2

    try:
        from backend.shared.pre_explored_url import resolve_cate_for_detail_url
    except Exception:
        resolve_cate_for_detail_url = None  # type: ignore[assignment]

    if resolve_cate_for_detail_url is not None:
        for attr in ("_file_crawl_url_pattern_obj_cache", "_url_pattern_obj_cache"):
            try:
                filters_obj = getattr(workflow, attr, None)
                if not filters_obj:
                    continue
                pair = resolve_cate_for_detail_url(detail_url, filters_obj)
            except Exception:
                pair = None
            if pair:
                resolved_cate1, resolved_cate2 = normalize_file_detail_cates(pair[0], pair[1])
                if resolved_cate1 or resolved_cate2:
                    return resolved_cate1, resolved_cate2

    try:
        url_key = canonicalize_url_for_dedup(detail_url) or detail_url
        matched_type = getattr(workflow, "_url_to_cate_map", {}).get(url_key)
        return split_detail_cates(matched_type)
    except Exception:
        return "", ""
