"""Verify response-header large-file deferral rules without network access."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.crawler.workers.download import (
    _download_http_request_timeout,
    _should_promote_streamed_file_to_large_lane,
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

    # Chunked or incorrect content-length responses start in the normal lane.
    # Once their received bytes cross the threshold, restart them in the
    # isolated large lane exactly once.
    assert _should_promote_streamed_file_to_large_lane(
        meta,
        header_size,
        worker_lane="normal",
        large_queue_available=True,
    )
    assert not _should_promote_streamed_file_to_large_lane(
        {**meta, "_large_lane_requeued": True},
        header_size,
        worker_lane="normal",
        large_queue_available=True,
    )
    assert not _should_promote_streamed_file_to_large_lane(
        meta,
        header_size,
        worker_lane="large",
        large_queue_available=True,
    )

    timeout = _download_http_request_timeout({}, 30.0)
    assert getattr(timeout, "total", None) is None
    assert getattr(timeout, "sock_read", 0) > 0
    print("OK: response-header deferral and declared-size timeouts")


if __name__ == "__main__":
    main()
