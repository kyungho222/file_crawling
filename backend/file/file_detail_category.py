from __future__ import annotations

from typing import Any, Tuple

from utils.url import canonicalize_url_for_dedup


def split_detail_cates(value: Any) -> Tuple[str, str]:
    text = str(value or "").strip()
    if not text.startswith("cate_match|"):
        return "", ""
    parts = text.split("|", 2)
    if len(parts) < 3:
        return "", ""
    return parts[1].strip(), parts[2].strip()


def resolve_file_detail_cates(
    workflow: Any,
    detail_url: str,
    *,
    cate1: Any = "",
    cate2: Any = "",
    item_type: Any = "",
) -> Tuple[str, str]:
    """Resolve file-crawl categories without depending on board crawl results."""

    resolved_cate1 = str(cate1 or "").strip()
    resolved_cate2 = str(cate2 or "").strip()
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
                resolved_cate1 = str(pair[0] or "").strip()
                resolved_cate2 = str(pair[1] or "").strip()
                if resolved_cate1 or resolved_cate2:
                    return resolved_cate1, resolved_cate2

    try:
        url_key = canonicalize_url_for_dedup(detail_url) or detail_url
        matched_type = getattr(workflow, "_url_to_cate_map", {}).get(url_key)
        return split_detail_cates(matched_type)
    except Exception:
        return "", ""
