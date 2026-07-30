"""Seed URL resolution and expansion facade.

Seed URLs are short-lived crawl entry candidates. Older modules still expose
``start_urls_*`` names for compatibility, but new callers should import from
this module.
"""

from __future__ import annotations

from backend.shared.start_urls_generation import (
    StartUrlsResolution,
    _normalize_start_url_items,
    _start_url_identity_key,
    filter_start_urls_by_content_board_id,
    resolve_start_urls,
)
from backend.shared.start_urls_preexpand import (
    _build_page_url,
    _expand_list_to_views_router,
    _fetch_static_html,
    _guess_page_param,
    _is_list_page_url,
    _normalize_list_cache_key,
    expand_query_links_to_start_urls,
)

SeedUrlsResolution = StartUrlsResolution
normalize_seed_url_items = _normalize_start_url_items
seed_url_identity_key = _start_url_identity_key
filter_seed_urls_by_content_board_id = filter_start_urls_by_content_board_id
resolve_seed_urls = resolve_start_urls
expand_seed_urls_to_entry_urls = expand_query_links_to_start_urls
is_seed_list_page_url = _is_list_page_url
normalize_seed_list_cache_key = _normalize_list_cache_key

__all__ = [
    "SeedUrlsResolution",
    "StartUrlsResolution",
    "resolve_seed_urls",
    "resolve_start_urls",
    "normalize_seed_url_items",
    "_normalize_start_url_items",
    "seed_url_identity_key",
    "_start_url_identity_key",
    "filter_seed_urls_by_content_board_id",
    "filter_start_urls_by_content_board_id",
    "expand_seed_urls_to_entry_urls",
    "expand_query_links_to_start_urls",
    "is_seed_list_page_url",
    "_is_list_page_url",
    "normalize_seed_list_cache_key",
    "_normalize_list_cache_key",
    "_build_page_url",
    "_guess_page_param",
    "_fetch_static_html",
    "_expand_list_to_views_router",
]
