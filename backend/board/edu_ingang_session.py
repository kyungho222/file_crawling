"""
강남인강 학습 플랫폼(edu.ingang.go.kr) 전용 세션 수집.

NGLMS iframe/detail 경로는 JSESSIONID가 없으면 `/error/message` 또는
ERROR 페이지로 떨어질 수 있으므로, 요청 쿠키/환경 변수 쿠키를 주입하고
warming URL 방문 뒤 대상 URL을 연다.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

logger = logging.getLogger(__name__)

_DEFAULT_WARM = "https://edu.ingang.go.kr/"
_ENV_COOKIE = "EDU_INGANG_COOKIE"
_ENV_WARM_URL = "EDU_INGANG_WARM_URL"
_EVENT_SCOPE_TO_SECTION_NO = {
    "high": "2020",
    "middle": "2021",
    "edulife": "2022",
}
_EVENT_PATH_RE = re.compile(
    r"^/NGLMS/(?P<section_no>\d+)/(?P<scope>high|middle|eduLife)/community/(?P<kind>eventList|eventView)$",
    re.IGNORECASE,
)


def is_edu_ingang_url(url: str) -> bool:
    if not url:
        return False
    low = (url or "").lower()
    if "edu.ingang.go.kr" not in low:
        return False
    if "://" not in low:
        return True
    try:
        host = (urlparse(url).netloc or "").lower().split("@")[-1].split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host == "edu.ingang.go.kr"
    except Exception:
        return True


def _parse_cookie_header(raw: str) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for part in str(raw or "").split(";"):
        chunk = part.strip()
        if not chunk or "=" not in chunk:
            continue
        k, v = chunk.split("=", 1)
        k = k.strip()
        if k:
            merged[k] = v.strip()
    return merged


def merge_edu_ingang_cookies(request_cookies: Optional[Dict[str, str]]) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    env_raw = (os.getenv(_ENV_COOKIE) or "").strip()
    if env_raw:
        merged.update(_parse_cookie_header(env_raw))
    for k, v in (request_cookies or {}).items():
        if k and v is not None:
            merged[str(k)] = str(v)
    return merged


def get_edu_ingang_warm_url() -> str:
    return (os.getenv(_ENV_WARM_URL) or _DEFAULT_WARM).strip() or _DEFAULT_WARM


def _canonical_event_scope(scope: str) -> str:
    low = str(scope or "").strip().lower()
    if low == "edulife":
        return "eduLife"
    return low


def normalize_edu_ingang_detail_url(url: str) -> str:
    """eventList?seq=...&type=view 형태를 실제 eventView 상세 URL로 보정한다."""
    if not url or not is_edu_ingang_url(url):
        return url
    try:
        parsed = urlparse(url)
        match = _EVENT_PATH_RE.match(parsed.path or "")
        if not match or str(match.group("kind") or "").lower() != "eventlist":
            return url
        params = dict(parse_qsl(parsed.query, keep_blank_values=True))
        seq = str(params.get("seq") or "").strip()
        view_type = str(params.get("type") or "").strip().lower()
        if not seq or (view_type and view_type != "view"):
            return url
        scope = _canonical_event_scope(match.group("scope"))
        section_no = _EVENT_SCOPE_TO_SECTION_NO.get(scope.lower(), str(match.group("section_no") or "").strip())
        new_path = f"/NGLMS/{section_no}/{scope}/community/eventView"
        new_query = urlencode([("seq", seq)])
        return urlunparse((parsed.scheme, parsed.netloc.lower(), new_path, "", new_query, ""))
    except Exception:
        return url


def derive_edu_ingang_warm_url(url: str) -> Optional[str]:
    """상세 진입 전에 먼저 방문할 동일 게시판 eventList URL을 추정한다."""
    if not url or not is_edu_ingang_url(url):
        return None
    try:
        parsed = urlparse(url)
        match = _EVENT_PATH_RE.match(parsed.path or "")
        if not match:
            return None
        scope = _canonical_event_scope(match.group("scope"))
        section_no = _EVENT_SCOPE_TO_SECTION_NO.get(scope.lower(), str(match.group("section_no") or "").strip())
        list_path = f"/NGLMS/{section_no}/{scope}/community/eventList"
        filtered = [
            (k, v)
            for k, v in parse_qsl(parsed.query, keep_blank_values=True)
            if str(k or "").lower() not in {"seq", "type"}
        ]
        new_query = urlencode(filtered, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc.lower(), list_path, "", new_query, ""))
    except Exception:
        return None


def edu_ingang_fetch_seems_authorized(request_url: str, final_url: str, html: str) -> Tuple[bool, str]:
    orig = (request_url or "").lower()
    fin = (final_url or "").lower()
    body = (html or "").strip()
    if not body or len(body) < 80:
        return False, "html_too_short"
    if "/error/message" in fin:
        return False, "redirect_to_error_message"
    if "title>error<" in body.lower():
        return False, "error_title"
    if "잘못된 요청입니다" in body:
        return False, "invalid_request_message"
    if "요청하신 페이지를 찾을 수 없습니다" in body:
        return False, "page_not_found_message"
    if "/nglms/" in orig and "/nglms/" not in fin:
        return False, "redirect_away_from_nglms"
    return True, "ok"


def _cookie_header(cookies: Dict[str, str]) -> str:
    return "; ".join(f"{k}={v}" for k, v in cookies.items() if k)


def _has_session_cookie(cookies: Dict[str, str]) -> bool:
    try:
        return bool((cookies or {}).get("JSESSIONID"))
    except Exception:
        return False


async def fetch_edu_ingang_html_aiohttp(
    url: str,
    *,
    cookies: Optional[Dict[str, str]] = None,
    warm_url: Optional[str] = None,
    timeout_sec: float = 30.0,
) -> Optional[Tuple[str, str]]:
    if not url or not is_edu_ingang_url(url):
        return None
    try:
        import aiohttp
    except ImportError:
        logger.warning("[edu.ingang] aiohttp 없음")
        return None

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    jar = aiohttp.CookieJar(unsafe=True)
    timeout = aiohttp.ClientTimeout(total=max(5.0, float(timeout_sec or 30.0)))
    merged = {k: v for k, v in (cookies or {}).items() if k}
    requested = (url or "").strip()
    target = normalize_edu_ingang_detail_url(requested)
    explicit_warm = str(warm_url or "").strip()
    warm = (
        derive_edu_ingang_warm_url(explicit_warm)
        or explicit_warm
        or derive_edu_ingang_warm_url(requested)
        or get_edu_ingang_warm_url()
    ).strip() or _DEFAULT_WARM

    try:
        async with aiohttp.ClientSession(cookie_jar=jar, headers=headers, timeout=timeout) as session:
            if not _has_session_cookie(merged):
                try:
                    probe_headers = dict(headers)
                    probe_headers["Referer"] = warm
                    async with session.get(target, headers=probe_headers, allow_redirects=False) as probe_resp:
                        logger.info(
                            "[edu.ingang] session probe | status=%s has_jsession=%s url=%s",
                            probe_resp.status,
                            bool(jar.filter_cookies(target).get("JSESSIONID")),
                            target[:200],
                        )
                except Exception as probe_ex:
                    logger.debug("[edu.ingang] session probe failed | url=%s err=%s", target[:200], probe_ex)

            warm_headers = dict(headers)
            warm_headers["Referer"] = warm
            if merged:
                warm_headers["Cookie"] = _cookie_header(merged)
            async with session.get(warm, headers=warm_headers, allow_redirects=True) as warm_resp:
                try:
                    await warm_resp.read()
                except Exception:
                    pass

            req_headers = dict(headers)
            req_headers["Referer"] = warm
            if merged:
                req_headers["Cookie"] = _cookie_header(merged)
            async with session.get(target, headers=req_headers, allow_redirects=True) as resp:
                if resp.status != 200:
                    logger.info("[edu.ingang] aiohttp status=%s | url=%s", resp.status, target[:200])
                    return None
                final_u = str(resp.url)
                try:
                    txt = await resp.text()
                except UnicodeDecodeError:
                    txt = (await resp.read()).decode("utf-8", errors="ignore")
                return txt, final_u
    except Exception as ex:
        logger.warning("[edu.ingang] aiohttp 실패 | url=%s err=%s", target[:200], ex)
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
                "domain": "edu.ingang.go.kr",
                "path": "/",
            }
        )
    return rows


async def fetch_edu_ingang_html_playwright(
    url: str,
    *,
    cookies: Optional[Dict[str, str]] = None,
    warm_url: Optional[str] = None,
    list_wait_ms: int = 500,
    detail_wait_ms: int = 1200,
) -> Optional[Tuple[str, str]]:
    if not url or not is_edu_ingang_url(url):
        return None
    try:
        from backend.board.playwright_renderer import _ensure_browser, _semaphore
    except Exception as ex:
        logger.warning("[edu.ingang] Playwright import 실패: %s", ex)
        return None

    from backend.shared.config import Config
    from backend.shared.playwright_optimizations import apply_stealth_if_needed, configure_context_for_crawl

    browser = await _ensure_browser()
    requested = (url or "").strip()
    target = normalize_edu_ingang_detail_url(requested)
    explicit_warm = str(warm_url or "").strip()
    warm = (
        derive_edu_ingang_warm_url(explicit_warm)
        or explicit_warm
        or derive_edu_ingang_warm_url(requested)
        or get_edu_ingang_warm_url()
    ).strip() or _DEFAULT_WARM
    merged = {k: v for k, v in (cookies or {}).items() if k}
    async with _semaphore:
        context = await browser.new_context(ignore_https_errors=True, viewport={"width": 1920, "height": 1080})
        try:
            await configure_context_for_crawl(context, target)
            if merged:
                try:
                    await context.add_cookies(_playwright_cookies(merged))
                except Exception as ex:
                    logger.warning("[edu.ingang] add_cookies 실패: %s", ex)
            page = await context.new_page()
            await apply_stealth_if_needed(page, target)
            wait_until = "domcontentloaded"
            if not _has_session_cookie(merged):
                try:
                    await page.goto(target, wait_until=wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
                    await page.wait_for_timeout(250)
                    logger.info("[edu.ingang] session probe via Playwright complete | final=%s", page.url[:200])
                except Exception as probe_ex:
                    logger.debug("[edu.ingang] Playwright session probe failed | url=%s err=%s", target[:200], probe_ex)
            await page.goto(warm, wait_until=wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
            if list_wait_ms > 0:
                await page.wait_for_timeout(list_wait_ms)
            await page.goto(target, wait_until=wait_until, referer=warm, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
            if detail_wait_ms > 0:
                await page.wait_for_timeout(detail_wait_ms)
            html = await page.content()
            return html, page.url
        except Exception as ex:
            logger.warning("[edu.ingang] Playwright 실패 | url=%s err=%s", target[:200], ex)
        finally:
            await context.close()
    return None
