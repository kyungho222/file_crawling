"""Verify file-download host admission pacing without external services."""
from __future__ import annotations

import sys
import asyncio
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.crawler.file_download_topology import file_crawl_download_topology
async def main() -> None:
    topology = file_crawl_download_topology()
    assert topology["total_workers"] == 3
    assert topology["normal_workers"] == 3
    assert topology["large_workers"] == 0
    assert topology["max_concurrent"] == 3
    download_source = (PROJECT_ROOT / "core" / "crawler" / "workers" / "download.py").read_text(encoding="utf-8")
    assert "header_timeout = 20.0" in download_source
    assert "deferred_to_transport_timeout_queue" in download_source
    assert 'phase in {"http_connect", "http_response_headers_wait"}' in download_source
    assert "phase_elapsed_sec >= 20.0" in download_source
    assert "return concurrent_items > 1" in download_source
    print("file download topology and header requeue verification passed")


if __name__ == "__main__":
    asyncio.run(main())
