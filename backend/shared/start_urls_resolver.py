"""Compatibility exports for seed URL resolution.

The official facade lives in ``backend.shared.seed_urls``.
Keep this module so existing callers do not need to change imports.
"""

from backend.shared.seed_urls import (
    StartUrlsResolution,
    _normalize_start_url_items,
    _start_url_identity_key,
    filter_start_urls_by_content_board_id,
    resolve_start_urls,
)

__all__ = [
    "StartUrlsResolution",
    "filter_start_urls_by_content_board_id",
    "resolve_start_urls",
    "_normalize_start_url_items",
    "_start_url_identity_key",
]
