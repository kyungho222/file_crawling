"""Verify that a hard HTTP header deadline does not hold a download worker."""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.crawler.http_header_deadline import await_http_response_headers


async def main() -> None:
    started_at = time.monotonic()
    try:
        await await_http_response_headers(asyncio.sleep(0.2), timeout_sec=0.03)
    except asyncio.TimeoutError:
        elapsed_sec = time.monotonic() - started_at
        assert elapsed_sec < 0.12, elapsed_sec
    else:
        raise AssertionError("hard header deadline did not time out")
    print("download header deadline verification passed")


if __name__ == "__main__":
    asyncio.run(main())
