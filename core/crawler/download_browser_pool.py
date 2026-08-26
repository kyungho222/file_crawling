"""Lazy Playwright browser pool used only by attachment-download fallbacks."""
from __future__ import annotations

import asyncio
import inspect
import logging
import os
from typing import Any, Awaitable, Callable, Optional

from core.crawler.browser_launch import BROWSER_LAUNCH_SEMAPHORE

logger = logging.getLogger(__name__)


def _pool_size_from_env() -> int:
    try:
        value = int(os.getenv("DOWNLOAD_BROWSER_POOL_SIZE", "2") or "2")
    except Exception:
        value = 2
    return max(1, min(value, 2))


class DownloadBrowserPool:
    """Own a small, lazy pool of browsers for download-only Playwright work.

    The scan/collection browser is deliberately not borrowed here.  Each lease is
    a browser instance; the download helper still creates and closes a fresh
    BrowserContext per file so cookies cannot leak between downloads.
    """

    def __init__(
        self,
        launch_browser: Callable[[], Awaitable[Any]],
        *,
        max_browsers: Optional[int] = None,
        label: str = "download",
    ) -> None:
        self._launch_browser = launch_browser
        self._max_browsers = max_browsers or _pool_size_from_env()
        self._label = label
        self._available: asyncio.Queue[Any] = asyncio.Queue(maxsize=self._max_browsers)
        self._created: dict[int, Any] = {}
        self._leased: set[int] = set()
        self._launching = 0
        self._closed_ids: set[int] = set()
        self._create_lock = asyncio.Lock()
        self._closed = False

    @property
    def max_browsers(self) -> int:
        return self._max_browsers

    async def acquire(self) -> Optional[Any]:
        if self._closed:
            return None
        try:
            browser = self._available.get_nowait()
        except asyncio.QueueEmpty:
            browser = None
        if browser is not None:
            self._leased.add(id(browser))
            return browser

        launch_new = False
        async with self._create_lock:
            if self._closed:
                return None
            if len(self._created) + self._launching < self._max_browsers:
                self._launching += 1
                launch_new = True

        if launch_new:
            await BROWSER_LAUNCH_SEMAPHORE.acquire()
            try:
                browser = await self._launch_browser()
            except Exception:
                BROWSER_LAUNCH_SEMAPHORE.release()
                async with self._create_lock:
                    self._launching = max(0, self._launching - 1)
                raise
            async with self._create_lock:
                self._launching = max(0, self._launching - 1)
            if self._closed:
                await self._close_browser(browser)
                return None
            self._created[id(browser)] = browser
            self._leased.add(id(browser))
            logger.info(
                "[DownloadBrowserPool][launched] label=%s browser_id=%s created=%s/%s",
                self._label,
                id(browser),
                len(self._created),
                self._max_browsers,
            )
            return browser

        # All slots are leased. Wait without launching another Chromium process.
        browser = await self._available.get()
        if self._closed:
            await self._close_browser(browser)
            return None
        self._leased.add(id(browser))
        return browser

    async def release(self, browser: Optional[Any]) -> None:
        if browser is None:
            return
        key = id(browser)
        self._leased.discard(key)
        connected = True
        try:
            value = browser.is_connected()
            connected = await value if inspect.isawaitable(value) else bool(value)
        except Exception:
            connected = False
        if self._closed or not connected or key not in self._created:
            await self._discard(browser)
            return
        try:
            self._available.put_nowait(browser)
        except asyncio.QueueFull:
            # Duplicate release or an already replaced browser: do not retain it.
            await self._discard(browser)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        browsers = list(self._created.values())
        self._created.clear()
        self._leased.clear()
        while True:
            try:
                self._available.get_nowait()
            except asyncio.QueueEmpty:
                break
        await asyncio.gather(*(self._close_browser(browser) for browser in browsers), return_exceptions=True)
        logger.info("[DownloadBrowserPool][closed] label=%s browsers=%s", self._label, len(browsers))

    async def _discard(self, browser: Any) -> None:
        self._created.pop(id(browser), None)
        await self._close_browser(browser)

    async def _close_browser(self, browser: Any) -> None:
        key = id(browser)
        if key in self._closed_ids:
            return
        self._closed_ids.add(key)
        try:
            await asyncio.wait_for(browser.close(), timeout=15.0)
        except asyncio.TimeoutError:
            logger.warning(
                "[DownloadBrowserPool][close_timeout] label=%s browser_id=%s timeout_sec=15",
                self._label,
                key,
            )
        except Exception:
            pass
        try:
            BROWSER_LAUNCH_SEMAPHORE.release()
        except ValueError:
            pass
