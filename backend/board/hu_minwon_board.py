"""
화성도시공사 온라인민원(hu.or.kr/minwon) 전용 파서.

- 제목: `.apv_tit_low01 .notice_c`
- 본문: 표 행 중 `민원 내용`, `처리 결과`
"""

from __future__ import annotations

import copy
import re
from typing import Any


def is_hu_minwon_url(url: str) -> bool:
    if not url:
        return False
    u = (url or "").lower()
    return "hu.or.kr" in u and "/minwon/" in u and "nr_view.do" in u


def extract_hu_minwon_title(soup: Any) -> str:
    if soup is None:
        return ""
    for sel in (
        ".apv_tit_low01 .notice_c",
        ".apv_tit_w .notice_c",
        "span.notice_c",
    ):
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        t = re.sub(r"\s+", " ", (el.get_text(" ", strip=True) or "")).strip()
        if t:
            return t
    return ""


def try_extract_hu_minwon_post(soup: Any, url: str):
    if soup is None or not is_hu_minwon_url(url):
        return None

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _extract_content_text,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    table = soup.select_one("table.apv_tit_low02")
    if not table:
        return None

    title = extract_hu_minwon_title(soup) or "제목 없음"

    content_labels = {"민원내용", "처리결과"}
    parts: list[str] = []
    wrap_doc = BeautifulSoup('<div class="hu-minwon-extract-wrap"></div>', "html.parser")
    wrap = wrap_doc.find("div")

    for tr in table.find_all("tr"):
        th = tr.find("th")
        if not th:
            continue
        label = re.sub(r"\s+", "", th.get_text(" ", strip=True) or "")
        if label not in content_labels:
            continue
        td = th.find_next_sibling("td")
        if not td:
            continue
        txt = _extract_content_text(copy.copy(td)).strip()
        txt = _format_numbered_list_lines(txt)
        txt = _trim_leading_skip_and_breadcrumb_text(txt)
        if not txt:
            continue
        parts.append(f"{label}\n{txt}")
        if wrap is not None:
            wrap.append(copy.copy(tr))

    if not parts:
        return None

    content_text = "\n\n".join(p for p in parts if p.strip()).strip()
    if not content_text:
        return None

    content_html = _sanitize_html_fragment(wrap if wrap is not None else table).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )
