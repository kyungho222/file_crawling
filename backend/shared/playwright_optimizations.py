from __future__ import annotations

import logging
import os
from urllib.parse import urlparse
from typing import Any

logger = logging.getLogger("backend.shared.playwright_optimizations")


DEFAULT_BLOCKED_RESOURCE_TYPES = {"font", "image", "media"}
DEFAULT_BLOCKED_URL_PARTS = (
    "doubleclick.net",
    "googletagmanager.com",
    "google-analytics.com",
    "analytics",
    "/ads/",
    "adservice",
    "facebook.net",
    "hotjar",
)


def _env_bool(name: str, default: str = "1") -> bool:
    return str(os.getenv(name, default)).strip().lower() in ("1", "true", "yes", "on")


def _csv_set(name: str, default: str) -> set[str]:
    raw = os.getenv(name, default)
    return {item.strip().lower() for item in str(raw or "").split(",") if item.strip()}


def _csv_tuple(name: str, default_items: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name)
    if not raw:
        return default_items
    return tuple(item.strip().lower() for item in raw.split(",") if item.strip())


def resource_blocking_enabled() -> bool:
    return _env_bool("PLAYWRIGHT_RESOURCE_BLOCKING", "1")


def stealth_enabled_for_url(url: str) -> bool:
    domains = _csv_set("PLAYWRIGHT_STEALTH_DOMAINS", "")
    if not domains:
        return False
    host = (urlparse(url or "").netloc or "").lower()
    return any(host == domain or host.endswith("." + domain) for domain in domains)


async def apply_resource_blocking(context: Any) -> None:
    if not resource_blocking_enabled():
        return

    blocked_types = _csv_set(
        "PLAYWRIGHT_BLOCK_RESOURCE_TYPES",
        ",".join(sorted(DEFAULT_BLOCKED_RESOURCE_TYPES)),
    )
    blocked_url_parts = _csv_tuple("PLAYWRIGHT_BLOCK_URL_PARTS", DEFAULT_BLOCKED_URL_PARTS)

    async def _handler(route):
        try:
            req = route.request
            resource_type = str(getattr(req, "resource_type", "") or "").lower()
            req_url = str(getattr(req, "url", "") or "").lower()
            if resource_type in blocked_types or any(part in req_url for part in blocked_url_parts):
                await route.abort(error_code="blockedbyclient")
                return
        except Exception:
            pass
        try:
            await route.continue_()
        except Exception:
            try:
                await route.abort()
            except Exception:
                pass

    try:
        await context.route("**/*", _handler)
    except Exception as exc:
        logger.debug("[PlaywrightOpt] route setup failed: %s", exc)


async def apply_stealth_if_needed(page: Any, target_url: str) -> None:
    if not stealth_enabled_for_url(target_url):
        return

    try:
        from playwright_stealth import stealth_async  # type: ignore

        await stealth_async(page)
        return
    except Exception:
        pass

    script = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['ko-KR', 'ko', 'en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
window.chrome = window.chrome || { runtime: {} };
"""
    try:
        await page.add_init_script(script)
    except Exception as exc:
        logger.debug("[PlaywrightOpt] stealth init script failed: %s", exc)


async def configure_context_for_crawl(context: Any, target_url: str = "") -> None:
    await apply_resource_blocking(context)
