"""Verify response-header large-file deferral rules without network access."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.crawler.workers.download import (
    _download_item_hard_timeout_sec,
    _download_http_request_timeout,
    _should_defer_response_to_large_lane,
)


def main() -> None:
    header_size = 25 * 1024 * 1024
    meta = {"url": "https://example.test/file", "declared_file_size_bytes": 0}
    assert _should_defer_response_to_large_lane(
        meta,
        header_size,
        worker_lane="normal",
        large_queue_available=True,
    )
    assert not _should_defer_response_to_large_lane(
        meta,
        header_size,
        worker_lane="large",
        large_queue_available=True,
    )
    assert not _should_defer_response_to_large_lane(
        {**meta, "_large_lane_requeued": True},
        header_size,
        worker_lane="normal",
        large_queue_available=True,
    )

    declared = {"declared_file_size_bytes": header_size}
    assert _download_item_hard_timeout_sec(declared) > 90.0
    timeout = _download_http_request_timeout(declared, 30.0)
    assert getattr(timeout, "total", 0) > 30.0
    print("OK: response-header deferral and declared-size timeouts")


if __name__ == "__main__":
    main()