"""Fixed download-worker topology for file crawling."""

from __future__ import annotations

from typing import Dict


def file_crawl_download_topology() -> Dict[str, int]:
    """Reserve HTTP workers for the normal queue and PW workers for fallback."""
    return {
        "total_workers": 5,
        "normal_workers": 3,
        "playwright_workers": 2,
        "large_workers": 0,
        "max_concurrent": 5,
    }
