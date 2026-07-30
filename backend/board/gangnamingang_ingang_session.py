"""
강남인강(gangnamingang.han.kr) AsaProgram 채팅/파일 영역.

로그인( PHP 세션 등 )이 없으면 /chat/file.htm 등이 302 → / 로 떨어지므로
요청 쿠키·환경 변수로 세션을 주입한 뒤 HTML을 수집한다.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_DEFAULT_WARM = "https://gangnamingang.han.kr/"

# API 요청·워크플로에 실린 쿠키와 합쳐서 사용: GANGNAMINGANG_COOKIE=PHPSESSID=...;name=value
_ENV_COOKIE = "GANGNAMINGANG_COOKIE"


def is_gangnamingang_han_url(url: str) -> bool:
    if not url:
        return False
    u = (url or "").lower()
    if "gangnamingang.han.kr" not in u:
        return False
    if "://" in u:
        try:
            host = (urlparse(url).netloc or "").lower().split("@")[-1].split(":")[0]
            if host.startswith("www."):
                host = host[4:]
            return host == "gangnamingang.han.kr"
        except Exception:
            return True
    return True


def _parse_cookie_header(raw: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    s = (raw or "").strip()
    if not s:
        return out
    for part in s.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k, v = k.strip(), v.strip()
        if k:
            out[k] = v
    return out


def merge_gangnamingang_cookies(request_cookies: Optional[Dict[str, str]]) -> Dict[str, str]:
    """
    - 워크플로/ API의 request_cookies
    - 환경 변수 GANGNAMINGANG_COOKIE (Header-style `a=b; c=d`)
    request_cookies가 우선(덮어쓰기)한다.
    """
    merged: Dict[str, str] = {}
    env_raw = (os.getenv(_ENV_COOKIE) or "").strip()
    if env_raw:
        merged.update(_parse_cookie_header(env_raw))
    for k, v in (request_cookies or {}).items():
        if k and v is not None:
            merged[str(k)] = str(v)
    return merged


def _looks_like_login_or_gate(html: str) -> bool:
    """file.htm이 아닌 메인/로그인 랜딩에 흔한 자산."""
    h = html or ""
    if len(h) < 200:
        return True
    low = h.lower()
    if "loginskin" in low and "ids.gif" in low and "pws.gif" in low:
        return True
    if re.search(r"아이디\s*저장", h) and re.search(r"암호\s*저장", h):
        return True
    return False


def gangnamingang_fetch_seems_authorized(
    request_url: str, final_url: str, html: str
) -> Tuple[bool, str]:
    """
    리다이렉트로 / 만 받았는지, 로그인 스킨만 보이는지 판별.
    (쿠키 없으면 200이라도 '실패'로 취급해 Playwright·재시도로 넘긴다.)
    """
    orig = (request_url or "").lower()
    fin = (final_url or "").lower()
    if "/chat/" in orig and "/chat/" not in fin:
        p = urlparse(fin)
        if (p.path or "/").rstrip("/") in ("", "/"):
            return False, "redirect_away_from_chat"
    if "file.htm" in orig and "file.htm" not in fin:
        return False, "file_path_not_in_final_url"
    if _looks_like_login_or_gate(html):
        return False, "login_or_gate_landing"
    if len((html or "").strip()) < 80:
        return False, "html_too_short"
    return True, "ok"


async def fetch_gangnamingang_html_aiohttp(
    url: str,
    *,
    cookies: Optional[Dict[str, str]] = None,
    warm_url: str = _DEFAULT_WARM,
    timeout_sec: float = 30.0,
) -> Optional[Tuple[str, str]]:
    """
    aiohttp로 warming URL 방문 후 대상 URL GET.
    반환: (html, final_url) 또는 None.
    """
    if not url or not is_gangnamingang_han_url(url):
        return None
    try:
        import aiohttp
    except ImportError:
        logger.warning("[gangnamingang] aiohttp 없음")
        return None

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    jar = aiohttp.CookieJar(unsafe=True)
    timeout = aiohttp.ClientTimeout(total=max(5.0, float(timeout_sec)))
    ck = {k: v for k, v in (cookies or {}).items() if k}
    to = (url or "").strip()
    wu = (warm_url or _DEFAULT_WARM).strip() or _DEFAULT_WARM

    def _cookie_header(c: Dict[str, str]) -> str:
        return "; ".join(f"{k}={v}" for k, v in c.items())

    try:
        async with aiohttp.ClientSession(cookie_jar=jar, headers=headers, timeout=timeout) as session:
            h1 = dict(headers)
            if ck:
                h1["Cookie"] = _cookie_header(ck)
            async with session.get(wu, headers=h1, allow_redirects=True) as warm_resp:
                try:
                    await warm_resp.read()
                except Exception:
                    pass
            h2 = dict(headers)
            if ck:
                h2["Cookie"] = _cookie_header(ck)
            async with session.get(to, headers=h2, allow_redirects=True) as resp:
                if resp.status != 200:
                    logger.info(
                        "[gangnamingang] aiohttp status=%s | url=%s", resp.status, to[:200]
                    )
                    return None
                final_u = str(resp.url)
                try:
                    txt = await resp.text()
                except UnicodeDecodeError:
                    txt = (await resp.read()).decode("utf-8", errors="ignore")
                return (txt, final_u)
    except Exception as ex:
        logger.warning("[gangnamingang] aiohttp 실패 | url=%s err=%s", to[:200], ex)
    return None


def _playwright_cookies(merged: Dict[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, value in (merged or {}).items():
        if not name:
            continue
        rows.append(
            {
                "name": str(name),
                "value": str(value),
                "domain": "gangnamingang.han.kr",
                "path": "/",
            }
        )
    return rows


async def fetch_gangnamingang_html_playwright(
    url: str,
    *,
    cookies: Optional[Dict[str, str]] = None,
    warm_url: str = _DEFAULT_WARM,
    list_wait_ms: int = 500,
    detail_wait_ms: int = 1200,
) -> Optional[Tuple[str, str]]:
    if not url or not is_gangnamingang_han_url(url):
        return None
    try:
        from backend.board.playwright_renderer import _ensure_browser, _semaphore
    except Exception as ex:
        logger.warning("[gangnamingang] Playwright import 실패: %s", ex)
        return None

    from backend.shared.config import Config
    from backend.shared.playwright_optimizations import apply_stealth_if_needed, configure_context_for_crawl

    browser = await _ensure_browser()
    wu = (warm_url or _DEFAULT_WARM).strip()
    to = (url or "").strip()
    merged = {k: v for k, v in (cookies or {}).items() if k}
    async with _semaphore:
        context = await browser.new_context(ignore_https_errors=True, viewport={"width": 1920, "height": 1080})
        try:
            await configure_context_for_crawl(context, to)
            if merged:
                try:
                    await context.add_cookies(_playwright_cookies(merged))
                except Exception as ex:
                    logger.warning("[gangnamingang] add_cookies: %s", ex)
            page = await context.new_page()
            await apply_stealth_if_needed(page, to)
            wait_until = "domcontentloaded"
            await page.goto(wu, wait_until=wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
            if list_wait_ms > 0:
                await page.wait_for_timeout(list_wait_ms)
            await page.goto(to, wait_until=wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
            if detail_wait_ms > 0:
                await page.wait_for_timeout(detail_wait_ms)
            html = await page.content()
            final_u = page.url
            if html and len(html.strip()) > 50:
                return (html, final_u)
        except Exception as ex:
            logger.warning("[gangnamingang] Playwright 실패 | url=%s err=%s", to[:200], ex)
        finally:
            await context.close()
    return None
