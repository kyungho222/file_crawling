"""
fallback for detail page fetching and discovery.

이 모듈은 기존 크롤러 코드를 수정하지 않고도 '상세페이지 수집 실패' 시
대체 시도를 수행하도록 설계된 유틸입니다.

주요 기능:
- 다양한 HTTP 헤더/Referer로 재시도
- 렌더링(Playwright)이 가능하면 JS 렌더링 후 재검사
- 리스트(부모) HTML에서 후보 링크 탐색 및 재시도

의존성은 선택적입니다(BeautifulSoup/Playwright 없으면 graceful fallback).
"""

from __future__ import annotations

import logging
import time
from typing import List, Optional
from urllib.parse import urljoin

import requests

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

try:
    from playwright.sync_api import sync_playwright  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    sync_playwright = None  # type: ignore[assignment]

# 프로젝트의 기존 유틸을 재사용(있을 경우)
try:
    from .detail_page_utils import (
        is_post_detail_page_from_html,
        find_detail_page_url_in_parent,
    )
except Exception:
    # 상대경로 import 실패 시, 실행 환경에서 optional
    is_post_detail_page_from_html = None  # type: ignore[assignment]
    find_detail_page_url_in_parent = None  # type: ignore[assignment]


def robust_get(url: str, session: Optional[requests.Session] = None, headers: Optional[dict] = None, timeout: int = 15):
    """간단한 재시도/백오프가 포함된 GET. session을 주는 것이 권장됩니다."""
    sess = session or requests.Session()
    base_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    }
    if headers:
        base_headers.update(headers)

    backoff = 1.0
    for attempt in range(1, 4):
        try:
            resp = sess.get(url, headers=base_headers, timeout=timeout)
            return resp
        except requests.RequestException as e:
            logger.debug("robust_get: attempt=%s url=%s err=%s", attempt, url, e)
            time.sleep(backoff)
            backoff *= 2
    raise requests.RequestException(f"failed to GET {url} after retries")


def try_playwright_render(url: str, timeout: int = 20, user_agent: Optional[str] = None) -> Optional[str]:
    """Playwright를 사용해 페이지를 렌더링하고 HTML을 반환합니다. Playwright가 없으면 None."""
    if not sync_playwright:
        logger.debug("try_playwright_render: playwright not available")
        return None

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            if user_agent:
                page.set_user_agent(user_agent)
            page.goto(url, wait_until="networkidle", timeout=timeout * 1000)
            # 간단한 스크롤 유도(로딩된 컨텐츠 확보)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.5)
            content = page.content()
            browser.close()
            return content
    except Exception as e:
        logger.debug("try_playwright_render fail url=%s err=%s", url, e)
        return None


def attempt_detail_fetch(
    url: str,
    session: Optional[requests.Session] = None,
    referer: Optional[str] = None,
    allow_render: bool = True,
) -> Optional[str]:
    """
    상세페이지 수집을 위한 'fallback-aware' fetch 함수.

    플로우:
    1) 기본 GET 시도 -> HTML 검사(is_post_detail_page_from_html 사용 가능 시)
    2) Referer/헤더를 바꿔 재시도
    3) Playwright 렌더링으로 재시도(허용 시)
    반환값: 성공한 HTML 텍스트 또는 None
    """
    sess = session or requests.Session()

    # 1) 기본 시도
    try:
        resp = robust_get(url, session=sess)
        if resp and resp.status_code == 200:
            text = resp.text
            if is_post_detail_page_from_html:
                try:
                    if is_post_detail_page_from_html(text, url):
                        return text
                except Exception:
                    # 검사 실패해도 다음 스텝으로 진행
                    logger.debug("is_post_detail_page_from_html raised")
            else:
                # utils가 없으면 일단 반환(상위에서 검증)
                return text
    except Exception as e:
        logger.debug("attempt_detail_fetch basic GET failed: %s", e)

    # 2) 헤더/Referer 바꿔 재시도
    alt_headers = {"Referer": referer} if referer else {}
    try:
        resp = robust_get(url, session=sess, headers=alt_headers)
        if resp and resp.status_code == 200:
            text = resp.text
            if is_post_detail_page_from_html:
                try:
                    if is_post_detail_page_from_html(text, url):
                        return text
                except Exception:
                    logger.debug("is_post_detail_page_from_html raised on alt headers")
            else:
                return text
    except Exception:
        logger.debug("attempt_detail_fetch alt headers failed")

    # 3) Playwright 렌더링 시도
    if allow_render and sync_playwright:
        try:
            rendered = try_playwright_render(url, user_agent=None)
            if rendered and is_post_detail_page_from_html:
                try:
                    if is_post_detail_page_from_html(rendered, url):
                        return rendered
                except Exception:
                    logger.debug("is_post_detail_page_from_html raised on rendered html")
            elif rendered:
                return rendered
        except Exception as e:
            logger.debug("attempt_detail_fetch render failed: %s", e)
    else:
        logger.debug("attempt_detail_fetch: skipping render (allow_render=%s playwright=%s)", allow_render, bool(sync_playwright))

    return None


def fallback_discover_from_list_html(list_html: str, base_url: str) -> List[str]:
    """
    리스트(부모) HTML을 받아 후보 상세 URL 목록을 추출합니다.
    - BeautifulSoup가 없으면 간단한 정규식/문자열 탐색만 수행합니다.
    - find_detail_page_url_in_parent가 있으면 이를 우선 사용.
    """
    candidates: List[str] = []
    if not list_html:
        return candidates

    # 우선, 기존 유틸 함수가 있으면 재사용
    if find_detail_page_url_in_parent and BeautifulSoup:
        try:
            soup = BeautifulSoup(list_html, "html.parser")
            anchors = [a.get("href") for a in soup.find_all("a") if a.get("href")]
            for a in anchors:
                full = urljoin(base_url, a)
                if find_detail_page_url_in_parent([a], base_url):
                    candidates.append(full)
            return list(dict.fromkeys(candidates))
        except Exception:
            logger.debug("fallback_discover_from_list_html: existing helper failed, falling back")

    # fallback simple anchor 수집
    if BeautifulSoup:
        try:
            soup = BeautifulSoup(list_html, "html.parser")
            for a in soup.find_all("a"):
                href = a.get("href")
                if not href:
                    continue
                full = urljoin(base_url, href)
                candidates.append(full)
        except Exception as e:
            logger.debug("fallback_discover_from_list_html bs4 failed: %s", e)
    else:
        # 매우 간단한 정규식: href="..."
        import re

        for m in re.finditer(r'href\s*=\s*["\']([^"\']+)["\']', list_html, flags=re.I):
            href = m.group(1)
            candidates.append(urljoin(base_url, href))

    # 중복 제거 및 반환
    return list(dict.fromkeys(candidates))


__all__ = [
    "attempt_detail_fetch",
    "fallback_discover_from_list_html",
    "try_playwright_render",
]

