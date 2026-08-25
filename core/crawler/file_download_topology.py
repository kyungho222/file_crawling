"""Fixed download-worker topology for file crawling."""

from __future__ import annotations

from typing import Dict


def file_crawl_download_topology() -> Dict[str, int]:
    """Keep file downloads on one three-worker queue without a large lane."""
    return {
        "total_workers": 3,
        "normal_workers": 3,
        "large_workers": 0,
        "max_concurrent": 3,
    }
