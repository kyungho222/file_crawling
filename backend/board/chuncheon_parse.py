"""
춘천시청(chuncheon.go.kr) 상세 HTML 파싱 일원화.

- URL 패턴별로 계약 / 일자리(job-plus-center: 민간 employ·공공 public) / 관광(tour) / 복지(new-welfare) 분기
- 공공일자리 상세는 `h3.lavender-blue` 없이 `.job-support-listview` 만 있을 수 있으며, 본문은 `p.info-title` 다음 `div.se-contents` 로 온다.
- `board_content_extractor.extract_board_post` 등은 `parse_chuncheon_detail_soup` 만 호출하면 된다.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from typing import Iterator, Optional, Tuple

# ---------------------------------------------------------------------------
# 공통 결과
# ---------------------------------------------------------------------------


@dataclass
class ChuncheonParseResult:
    """춘천 전용 파서 공통 반환."""

    kind: str  # contract | job | tour | welfare
    title: str
    content_text: str
    content_html: str
    snippet: str


# ---------------------------------------------------------------------------
# URL (일자리)
# ---------------------------------------------------------------------------


def is_chuncheon_job_detail_url(u: str) -> bool:
    """경제포털 일자리 상세(채용 employ / 공공 public 등) URL 여부."""
    if not u:
        return False
    lu = (u or "").lower()
    if "chuncheon.go.kr" not in lu or "job-plus-center" not in lu:
        return False
    if "/detail/" in lu or "/detail?" in lu or lu.rstrip("/").endswith("/detail"):
        return True
    return False


# ---------------------------------------------------------------------------
# 일자리 상세
# ---------------------------------------------------------------------------


def _norm_label(s: str) -> str:
    return re.sub(r"\s+", "", (s or "").strip())


def _value_node_after_info_title(title_el) -> Optional[object]:
    """
    `p.info-title` 직후 값 노드: `p.info-text`(민간 채용) 또는 `div`(공공일자리 본문 `.se-contents` 등).
    """
    if title_el is None:
        return None
    n = title_el.next_sibling
    while n is not None and not getattr(n, "name", None):
        n = n.next_sibling
    if n is None:
        return None
    if n.name == "p":
        cls = n.get("class") or []
        if "info-text" in cls:
            return n
        return None
    if n.name == "div":
        return n
    return None


def _iter_title_value_pairs_in_li(li) -> Iterator[Tuple[object, object]]:
    """한 li 안의 모든 .info-title 과 짝 값(p.info-text 또는 본문 div)을 순서대로 반환."""
    if li is None:
        return
    for title_el in li.find_all("p", class_=lambda c: bool(c) and "info-title" in c):
        val = _value_node_after_info_title(title_el)
        if val is not None:
            yield (title_el, val)


def _iter_all_job_pairs(soup) -> Iterator[Tuple[str, str]]:
    """페이지 내 모든 job-support-listview 에서 (라벨, 값) 순회."""
    for wrap in soup.select(".job-support-listview"):
        for li in wrap.select(":scope > ul > li"):
            for title_el, value_el in _iter_title_value_pairs_in_li(li):
                label = _collapse_ws_local(title_el.get_text(" ", strip=True)).replace(":", "")
                value = _clean_preserve_newline_local(_plain_text_from_value_node(value_el))
                if label and value:
                    yield (label, value)


def _collapse_ws_local(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s).strip()


def _clean_preserve_newline_local(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"[ \t]+", " ", s)
    lines = [line.strip() for line in s.splitlines()]
    result = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", result).strip()


def _plain_text_from_value_node(value_el) -> str:
    """`p.info-text` 또는 rich `div` 본문을 평문으로."""
    if value_el is None:
        return ""
    if getattr(value_el, "name", None) == "div":
        from backend.board.board_content_extractor import _extract_content_text

        return _extract_content_text(value_el)
    return value_el.get_text("\n", strip=True)


def extract_chuncheon_job_title_from_soup(soup) -> Optional[str]:
    """
    채용 상세에서 라벨이 '채용제목' 또는 '제목' 인 값을 제목으로 반환.
    li.half 등으로 라벨이 첫 번째가 아닐 때도 동작한다.
    """
    if not soup:
        return None
    for label, value in _iter_all_job_pairs(soup):
        lc = _norm_label(label)
        if lc in ("채용제목", "제목"):
            t = _collapse_ws_local(value)
            if t and len(t) > 2 and t not in ("담당자정보", "상세정보", "관련자료"):
                return t
    return None


@dataclass
class ChuncheonJobParseResult:
    title: str
    content_text: str
    content_html: str
    snippet: str


def parse_job_detail_soup(soup, url: str) -> Optional[ChuncheonJobParseResult]:
    """
    일자리 상세 DOM에서 `.con-area.type-info` 전체(또는 모든 listview)를 추출한다.
    """
    if not soup or not url or not is_chuncheon_job_detail_url(url):
        return None

    from backend.board.board_content_extractor import (
        _collapse_ws,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _strip_chuncheon_portal_ui_noise,
        _trim_leading_skip_and_breadcrumb_text,
    )

    root = soup.select_one(".sub-content .con-area.type-info")
    if not root:
        root = soup.select_one(".con-area.type-info")

    listviews = soup.select(".job-support-listview")
    if not listviews:
        return None

    rows: list[str] = []

    def _append_listview(jsv) -> None:
        if not jsv:
            return
        for li in jsv.select(":scope > ul > li"):
            for title_el, value_el in _iter_title_value_pairs_in_li(li):
                label = _collapse_ws_local(title_el.get_text(" ", strip=True)).replace(":", "")
                value = _clean_preserve_newline_local(_plain_text_from_value_node(value_el))
                if label and value:
                    rows.append(f"{label}: {value}")

    if root:
        h3_secs = root.select("h3.lavender-blue")
        if h3_secs:
            for h3 in h3_secs:
                sec = _collapse_ws_local(h3.get_text(" ", strip=True))
                if sec:
                    rows.append(sec)
                jsv = h3.find_next_sibling("div", class_=lambda c: bool(c) and "job-support-listview" in c)
                if not jsv:
                    n = h3.find_next_sibling()
                    while n is not None:
                        if getattr(n, "name", None) == "div":
                            cls = n.get("class") or []
                            if "job-support-listview" in cls:
                                jsv = n
                                break
                        n = n.find_next_sibling()
                _append_listview(jsv)
        else:
            # 공공일자리(public/detail) 등: 섹션 h3 없이 `.con-area` 안에 listview 단독
            for jsv in root.select(".job-support-listview"):
                _append_listview(jsv)
    else:
        for wrap in listviews:
            _append_listview(wrap)

    if not rows:
        return None

    title = extract_chuncheon_job_title_from_soup(soup) or ""
    if not title:
        title_el = soup.select_one("meta[property='og:title']") or soup.select_one("title")
        if title_el:
            raw = (
                title_el.get("content")
                if hasattr(title_el, "get") and title_el.name == "meta"
                else title_el.get_text(" ", strip=True)
            )
            title = _collapse_ws(str(raw or ""))
            title = re.split(r"\s*(?:\||｜|-)\s*", title or "")[0].strip()
    if not title:
        title = "제목 없음"

    content_text = "\n".join(rows).strip()
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not content_text.strip():
        return None

    try:
        from bs4 import BeautifulSoup as BS

        frag = soup.new_tag("div", attrs={"class": "chuncheon-job-extract-wrap"})
        if root:
            node = BS(str(root), "html.parser").find(True)
            if node:
                _strip_chuncheon_portal_ui_noise(node)
                frag.append(node)
        else:
            for w in listviews:
                frag.append(copy.copy(w))
        content_html = _sanitize_html_fragment(frag).strip()
    except Exception:
        content_html = ""

    snippet = _collapse_ws(content_text)[:200]

    return ChuncheonJobParseResult(
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def parse_job_detail_html(html: str, url: str) -> Optional[ChuncheonJobParseResult]:
    try:
        from bs4 import BeautifulSoup as BS  # type: ignore[import-not-found]
    except Exception:
        return None
    try:
        soup = BS(html, "html.parser")
    except Exception:
        return None
    return parse_job_detail_soup(soup, url)


# ---------------------------------------------------------------------------
# 관광(tour) 상세
# ---------------------------------------------------------------------------


def parse_tour_detail_soup(soup, url: str) -> Optional[ChuncheonParseResult]:
    if not soup or not url:
        return None
    u = (url or "").lower()
    if "chuncheon.go.kr" not in u or "/tour/" not in u or "/detail/" not in u:
        return None

    from bs4 import BeautifulSoup as BS  # type: ignore[import-not-found]

    from backend.board.board_content_extractor import (
        _collapse_ws,
        _extract_content_text,
        _extract_title,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _strip_chuncheon_portal_ui_noise,
        _trim_leading_skip_and_breadcrumb_text,
    )

    sub_top = soup.select_one("#print-wrap .sub-top")
    info_box = (
        soup.select_one("#print-wrap .sec-scroll-02 .accordion-content")
        or soup.select_one("#print-wrap .sec-scroll-02 .view-wrap .accordion-content")
        or soup.select_one("#print-wrap .sec-scroll-02 .view-wrap")
    )
    if not sub_top and not info_box:
        return None

    title = ""
    summary = ""
    if sub_top:
        h = sub_top.select_one("h1, h2, h3")
        if h:
            title = _collapse_ws(h.get_text(" ", strip=True))
        p = sub_top.select_one("p")
        if p:
            summary = _collapse_ws(p.get_text(" ", strip=True))

    if not title:
        title = _extract_title(soup, soup)
    if not title:
        title = "제목 없음"

    text_parts: list[str] = []
    if title:
        text_parts.append(title)
    if summary and summary != title:
        text_parts.append(summary)

    if info_box:
        try:
            frag = BS(str(info_box), "html.parser")
            root = frag.find(True)
            if root:
                _strip_chuncheon_portal_ui_noise(root)
                info_text = _extract_content_text(root)
                if info_text.strip():
                    text_parts.append(info_text.strip())
        except Exception:
            pass

    content_text = "\n\n".join(x for x in text_parts if x and x.strip()).strip()
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not content_text:
        return None

    frag_out = soup.new_tag("div", attrs={"class": "chuncheon-tour-extract-wrap"})
    if title:
        t = soup.new_tag("h2")
        t.string = title
        frag_out.append(t)
    if summary and summary != title:
        s = soup.new_tag("p")
        s.string = summary
        frag_out.append(s)
    if info_box:
        try:
            ib = BS(str(info_box), "html.parser").find(True)
            if ib:
                _strip_chuncheon_portal_ui_noise(ib)
                frag_out.append(ib)
        except Exception:
            pass

    content_html = _sanitize_html_fragment(frag_out).strip()
    snippet = _collapse_ws(content_text)[:200]
    return ChuncheonParseResult(
        kind="tour",
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


# ---------------------------------------------------------------------------
# 복지(new-welfare) 상세
# ---------------------------------------------------------------------------


def parse_welfare_detail_soup(soup, url: str) -> Optional[ChuncheonParseResult]:
    if not soup or not url:
        return None
    u = (url or "").lower()
    if "chuncheon.go.kr" not in u or "/new-welfare/" not in u:
        return None

    from bs4 import BeautifulSoup as BS  # type: ignore[import-not-found]

    from backend.board.board_content_extractor import (
        _collapse_ws,
        _extract_content_text,
        _extract_title,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _strip_chuncheon_portal_ui_noise,
        _trim_leading_skip_and_breadcrumb_text,
    )

    wrap = (
        soup.select_one("#print-wrap .con-area .first-con")
        or soup.select_one(".sub-content .con-area .first-con")
        or soup.select_one(".con-area .first-con")
    )
    if not wrap:
        return None

    try:
        frag = BS(str(wrap), "html.parser")
        root = frag.find(True)
        if not root:
            return None
        try:
            _strip_chuncheon_portal_ui_noise(root)
        except Exception:
            pass
    except Exception:
        return None

    title_el = (
        root.select_one("h3")
        or root.select_one("h2")
        or soup.select_one("#print-wrap h3")
        or soup.select_one("#print-wrap h2")
    )
    title = _collapse_ws(title_el.get_text(" ", strip=True)) if title_el else ""
    if not title:
        title = _extract_title(soup, root)
    if not title:
        title = "제목 없음"

    content_text = _extract_content_text(root)
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not content_text.strip():
        return None

    content_html = _sanitize_html_fragment(root).strip()
    snippet = _collapse_ws(content_text)[:200]
    return ChuncheonParseResult(
        kind="welfare",
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


# ---------------------------------------------------------------------------
# URL 라우터 (계약은 chuncheon_contract 위임)
# ---------------------------------------------------------------------------


def parse_chuncheon_detail_soup(soup, url: str) -> Optional[ChuncheonParseResult]:
    """
    춘천시청 상세 URL에 맞는 전용 파서를 순서대로 시도한다.
    (계약 → 일자리 → 관광 → 복지)
    """
    if not soup or not url:
        return None
    u = (url or "").lower()
    if "chuncheon.go.kr" not in u:
        return None

    if "chuncheon.go.kr/contract/" in u and ("/detail/" in u or "ctrtacctbookmngno=" in u):
        from backend.board import chuncheon_contract

        r = chuncheon_contract.parse_contract_detail_soup(soup, url)
        if r:
            return ChuncheonParseResult(
                kind="contract",
                title=r.title,
                content_text=r.content_text,
                content_html=r.content_html,
                snippet=r.snippet,
            )

    rj = parse_job_detail_soup(soup, url)
    if rj:
        return ChuncheonParseResult(
            kind="job",
            title=rj.title,
            content_text=rj.content_text,
            content_html=rj.content_html,
            snippet=rj.snippet,
        )

    rt = parse_tour_detail_soup(soup, url)
    if rt:
        return rt

    rw = parse_welfare_detail_soup(soup, url)
    if rw:
        return rw

    return None


def parse_chuncheon_detail_html(html: str, url: str) -> Optional[ChuncheonParseResult]:
    try:
        from bs4 import BeautifulSoup as BS  # type: ignore[import-not-found]
    except Exception:
        return None
    try:
        soup = BS(html, "html.parser")
    except Exception:
        return None
    return parse_chuncheon_detail_soup(soup, url)
