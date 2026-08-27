"""Verify the download-only Playwright pool can serve all download workers."""
from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.crawler.download_browser_pool import _pool_size_from_env


def main() -> None:
    original = os.environ.pop("DOWNLOAD_BROWSER_POOL_SIZE", None)
    try:
        assert _pool_size_from_env() == 5
        os.environ["DOWNLOAD_BROWSER_POOL_SIZE"] = "9"
        assert _pool_size_from_env() == 5
    finally:
        if original is None:
            os.environ.pop("DOWNLOAD_BROWSER_POOL_SIZE", None)
        else:
            os.environ["DOWNLOAD_BROWSER_POOL_SIZE"] = original
    print("download browser pool verification passed")


if __name__ == "__main__":
    main()
