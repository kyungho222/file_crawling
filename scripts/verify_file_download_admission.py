"""Verify file-download host admission pacing without external services."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.crawler.file_download_topology import file_crawl_download_topology


def main() -> None:
    topology = file_crawl_download_topology()
    assert topology["total_workers"] == 5
    assert topology["normal_workers"] == 3
    assert topology["playwright_workers"] == 2
    assert topology["large_workers"] == 0
    assert topology["max_concurrent"] == 5
    print("file download topology verification passed")


if __name__ == "__main__":
    main()
