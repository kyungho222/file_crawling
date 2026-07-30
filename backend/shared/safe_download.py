import asyncio
import os
from typing import Optional, Callable, Awaitable

from playwright.async_api import Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError, Download, Page


async def _expect_download_with_action(page: Page, action: Callable[[], Awaitable[None]], timeout_ms: int) -> Download:
    """
    page.expect_download wrapper that tolerates Playwright's "Download is starting" goto exception.
    The caller provides an `action` coroutine that triggers the download (click or goto).
    """
    async with page.expect_download(timeout=timeout_ms) as download_info:
        try:
            await action()
        except PlaywrightError as err:
            msg = str(err).lower()
            # Playwright may raise a "Download is starting" error when navigating to a direct-download URL.
            if "download is starting" in msg:
                # treat as success; the expected download will be available from download_info.value
                pass
            else:
                raise
    download = await download_info.value
    return download


async def click_or_goto_expect_download(page: Page, url: str, link_element: Optional[object] = None, expect_ms: int = 60000) -> Download:
    """
    Try to trigger a download by clicking `link_element` if provided, falling back to navigating
    directly to `url`. Returns the Playwright Download object.
    """
    # Prefer click when possible (simulates user action)
    if link_element:
        try:
            return await _expect_download_with_action(page, lambda: link_element.click(), expect_ms)
        except PlaywrightTimeoutError:
            # click did not fire download, fallback to direct navigation
            return await _expect_download_with_action(page, lambda: page.goto(url, wait_until="commit", timeout=expect_ms), expect_ms)
    # no link element -> navigate directly
    return await _expect_download_with_action(page, lambda: page.goto(url, wait_until="commit", timeout=expect_ms), expect_ms)


async def save_download_to_path(download: Download, target_path: str, wait_path_timeout_sec: float = 60.0) -> str:
    """
    Ensure the Download is finished and save it to `target_path`.
    Returns the target_path on success.
    """
    try:
        # wait for the underlying temporary path to be ready (some environments may not support .path())
        try:
            await asyncio.wait_for(download.path(), timeout=wait_path_timeout_sec)
        except Exception:
            pass
        await download.save_as(target_path)
    except PlaywrightError as err:
        raise
    return target_path


