from __future__ import annotations

import asyncio
import os
from typing import Optional


PARTIAL_DOWNLOAD_MARKERS = (".part", ".crdownload", ".download")


def is_partial_download_path(path: str) -> bool:
    name = os.path.basename(str(path or "")).lower()
    if not name:
        return False
    return any(marker in name for marker in PARTIAL_DOWNLOAD_MARKERS)


def has_partial_download_sibling(path: str) -> bool:
    directory = os.path.dirname(str(path or ""))
    basename = os.path.basename(str(path or ""))
    if not directory or not basename:
        return False
    try:
        names = os.listdir(directory)
    except OSError:
        return False
    basename_lower = basename.lower()
    for name in names:
        if name == basename:
            continue
        low = name.lower()
        if not low.startswith(basename_lower):
            continue
        suffix = low[len(basename_lower):]
        if any(marker in suffix for marker in PARTIAL_DOWNLOAD_MARKERS):
            return True
    return False


async def wait_for_file_ready(
    path: str,
    *,
    timeout_sec: Optional[float] = None,
    interval_sec: float = 0.2,
    stable_checks: int = 2,
    allow_partial_name: bool = False,
    check_partial_siblings: bool = True,
) -> int:
    deadline = asyncio.get_running_loop().time() + (
        30.0 if timeout_sec is None else max(0.1, float(timeout_sec))
    )
    interval = max(0.05, min(float(interval_sec), 2.0))
    required_stable = max(1, int(stable_checks))
    last_size: Optional[int] = None
    stable_count = 0
    last_reason = "not_checked"

    while True:
        now = asyncio.get_running_loop().time()
        if now > deadline:
            raise TimeoutError(f"file not ready before timeout: {path} ({last_reason})")

        if not path:
            last_reason = "empty_path"
        elif not allow_partial_name and is_partial_download_path(path):
            last_reason = "partial_path"
        elif not os.path.isfile(path):
            last_reason = "missing"
        elif check_partial_siblings and has_partial_download_sibling(path):
            last_reason = "partial_sibling"
        else:
            try:
                size = os.path.getsize(path)
            except OSError:
                size = 0
            if size <= 0:
                last_reason = "empty"
                stable_count = 0
                last_size = size
            elif last_size == size:
                stable_count += 1
                if stable_count >= required_stable:
                    return size
                last_reason = "stabilizing"
            else:
                last_size = size
                stable_count = 1
                if stable_count >= required_stable:
                    return size
                last_reason = "size_changed"

        await asyncio.sleep(interval)
