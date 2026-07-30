from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from backend.shared.runtime_tab_view_core import RuntimeTabView
from utils.url import canonicalize_url_for_dedup, ensure_url_scheme

logger = logging.getLogger("runtime_tab_view")

# Singleton instance to preserve caches across calls (previous module-level behavior)
_RTV = RuntimeTabView()


async def resolve_start_urls_to_list_pages(start_urls: Sequence[str], *, allow_playwright: bool = True) -> List[str]:
    return await _RTV.resolve_start_urls_to_list_pages(start_urls, allow_playwright=allow_playwright)


async def extract_views_from_list_pages(
    list_urls: Sequence[str],
    *,
    mode: str = "board",
    allow_playwright: bool = True,
    enable_pagination: bool = True,
    max_pages_per_list: Optional[int] = None,
) -> List[str]:
    return await _RTV.extract_views_from_list_pages(
        list_urls,
        mode=mode,
        allow_playwright=allow_playwright,
        enable_pagination=enable_pagination,
        max_pages_per_list=max_pages_per_list,
    )


async def resolve_runtime_start_urls(
    start_urls: Sequence[Union[str, dict]],
    *,
    mode: str = "board",
    allow_playwright: bool = True,
    strict_view_only: bool = False,
) -> List[Union[str, dict]]:
    """
    dict 항목({"url","type", ...})은 파일 크롤 start_urls의 cate 메타 보존용.
    입·출력 길이가 같을 때만 메타를 다시 붙이고, 길이가 바뀌면 문자열만 반환한다.
    """
    if not start_urls:
        return []
    pairs: List[Tuple[Optional[dict], str]] = []
    for u in start_urls:
        if isinstance(u, dict):
            raw = (u.get("url") or "").strip()
            if not raw:
                continue
            pairs.append((dict(u), ensure_url_scheme(raw)))
        elif isinstance(u, str) and u.strip():
            pairs.append((None, ensure_url_scheme(u.strip())))
    if not pairs:
        return []
    normalized = [p[1] for p in pairs]

    def _norm_key(u: str) -> str:
        return (canonicalize_url_for_dedup(u) or (u or "").strip() or "").strip()

    meta_by_key: Dict[str, Dict[str, Any]] = {}
    for meta, u in pairs:
        k = _norm_key(u)
        if not k or meta is None:
            continue
        meta_by_key.setdefault(k, dict(meta))

    resolved = await _RTV.resolve_runtime_start_urls(
        normalized, mode=mode, allow_playwright=allow_playwright, strict_view_only=strict_view_only
    )
    if len(resolved) == len(pairs):
        out_eq: List[Union[str, dict]] = []
        for rurl, (meta, _) in zip(resolved, pairs):
            if meta is not None:
                merged: Dict[str, Any] = dict(meta)
                merged["url"] = rurl
                out_eq.append(merged)
            else:
                out_eq.append(rurl)
        return out_eq

    out: List[Union[str, dict]] = []
    for rurl in resolved:
        k = _norm_key(rurl)
        meta = meta_by_key.get(k) if k else None
        if meta is not None:
            merged2: Dict[str, Any] = dict(meta)
            merged2["url"] = rurl
            out.append(merged2)
        else:
            out.append(rurl)
    return out


__all__ = ["resolve_start_urls_to_list_pages", "extract_views_from_list_pages", "resolve_runtime_start_urls"]

