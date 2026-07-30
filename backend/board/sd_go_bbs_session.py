"""
성동구청(sd.go.kr) selectBbsNtt* 게시판: 목록 방문 후에야 상세가 열리는 세션 의존 사이트.

춘천시청 계약(chuncheon_contract)과 같이 동일 브라우저 컨텍스트에서
목록 URL → 상세 URL 순으로 이동해 쿠키·세션을 맞춘 뒤 HTML을 수집한다.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from backend.board.playwright_renderer import render_page_via_playwright_navigate_from

logger = logging.getLogger(__name__)


def is_sd_go_bbs_detail_url(url: str) -> bool:
    if not url or "sd.go.kr" not in url.lower():
        return False
    return "selectbbsnttview.do" in url.lower()


def is_sd_go_bbs_list_url(url: str) -> bool:
    if not url or "sd.go.kr" not in url.lower():
        return False
    return "selectbbsnttlist.do" in url.lower()


def sd_go_bbs_detail_to_list_url(url: str) -> Optional[str]:
    """
    상세(selectBbsNttView.do) URL에서 동일 게시판 목록(selectBbsNttList.do) URL을 만든다.
    게시글 식별 쿼리(nttNo 등)는 제거한다.
    """
    if not is_sd_go_bbs_detail_url(url):
        return None
    try:
        p = urlparse(url)
        path = p.path or ""
        new_path, n = re.subn(r"(?i)selectBbsNttView\.do", "selectBbsNttList.do", path)
        if not n or new_path == path:
            return None
        pairs = parse_qsl(p.query or "", keep_blank_values=True)
        drop_article_keys = {"nttno", "ntt_sn", "nttsn", "article_sn", "articleno"}
        filtered = [(k, v) for k, v in pairs if str(k).lower() not in drop_article_keys]
        q = urlencode(filtered, doseq=True)
        scheme = (p.scheme or "https").lower()
        return urlunparse((scheme, p.netloc, new_path, "", q, ""))
    except Exception:
        return None


async def fetch_sd_go_bbs_detail_html_playwright(detail_url: str) -> Optional[str]:
    """목록 warming 후 상세 HTML. 실패 시 None."""
    if not detail_url:
        return None
    list_url = sd_go_bbs_detail_to_list_url(detail_url)
    if not list_url:
        logger.warning("[sd_go_bbs] 목록 URL 유도 실패 | detail=%s", detail_url[:180])
        return None
    try:
        step_ms = int(os.getenv("BOARD_SD_BBS_STEP_WAIT_MS", "600") or "600")
    except Exception:
        step_ms = 600
    try:
        detail_ms = int(os.getenv("BOARD_SD_BBS_DETAIL_WAIT_MS", "1200") or "1200")
    except Exception:
        detail_ms = 1200
    step_ms = max(0, min(step_ms, 30_000))
    detail_ms = max(0, min(detail_ms, 60_000))

    logger.info(
        "[sd_go_bbs] 세션 warming | list=%s | detail=%s",
        list_url[:160],
        detail_url[:160],
    )
    try:
        html, final_u = await render_page_via_playwright_navigate_from(
            list_url,
            detail_url,
            wait_until="domcontentloaded",
            list_wait_ms=step_ms,
            detail_wait_ms=detail_ms,
        )
        if html and len(html.strip()) > 80:
            return html
        logger.warning(
            "[sd_go_bbs] HTML 짧음/비어 있음 | len=%s final=%s",
            len(html or ""),
            (final_u or "")[:120],
        )
    except Exception as ex:
        logger.warning("[sd_go_bbs] Playwright warming 실패: %s", ex)
    return None
