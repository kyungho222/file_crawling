"""
용인시 상하수도사업소(water/wttnkManage) 상세 전용 파서.

- 제목: `#contents .h3_box > h3`
- 본문: `#contents .cont_box`
"""

from __future__ import annotations

from typing import Any


YONGIN_WATER_TITLE_SELECTORS = (
    "#contents .cont_box h4.tit_h4",
    "#contents .cont_box .tit_h4",
    "#contents .cont_box h4",
    ".cont_box h4.tit_h4",
    ".cont_box .tit_h4",
)

YONGIN_WATER_CAPTION_SELECTORS = (
    "#contents .cont_box caption",
    ".cont_box caption",
)

YONGIN_WATER_MENU_TITLE_SELECTORS = (
    "#contents .h3_box > h3",
    "#contents .h3_box h3",
    "#contents .h3-box > h3",
    "#contents .h3-box h3",
    ".h3_box > h3",
    ".h3_box h3",
)

YONGIN_WATER_CONTENT_SELECTORS = (
    "#contents .cont_box",
    "#contents .cont_box .tb_box",
)

YONGIN_WATER_CONTENT_EXCLUDE_SELECTORS = (
    ".tab_list",
    ".tab-list",
    "[class*='tab_list']",
    "[class*='tab-list']",
)

YONGIN_WATER_GENERIC_TITLES = {
    "저수조 청소 이력 상세",
    "저수조 수질검사 이력 상세",
    "옥내급수관 상태검사 이력 상세",
    "수도시설 관리교육 이력 상세",
}


def is_yongin_water_url(url: str) -> bool:
    if not url:
        return False
    u = (url or "").lower()
    return "yongin.go.kr" in u and "/water/wttnkmanage/" in u


def is_yongin_water_attachment_detail_url(url: str) -> bool:
    if not is_yongin_water_url(url):
        return False
    u = (url or "").lower()
    return any(
        path in u
        for path in (
            "/bd_selectwttnkcln.do",
            "/bd_selectwtwayedc.do",
            "/bd_selectwttnkinspct.do",
            "/bd_selectwsptnkinspct.do",
        )
    )


def _clean_text(raw: Any) -> str:
    try:
        return " ".join((raw.get_text(" ", strip=True) if hasattr(raw, "get_text") else str(raw or "")).split()).strip()
    except Exception:
        return ""


def _select_text(soup: Any, selectors: tuple[str, ...]) -> str:
    for sel in selectors:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        text = _clean_text(el) if el else ""
        if text:
            return text
    return ""


def _strip_caption_suffix(text: str) -> str:
    text = _clean_text(text)
    if text.endswith(" 안내"):
        return text[: -len(" 안내")].strip()
    if text.endswith("안내") and len(text) > len("안내"):
        return text[: -len("안내")].strip()
    return text


def _table_value_by_label(soup: Any, *labels: str) -> str:
    label_set = {str(label or "").strip() for label in labels if str(label or "").strip()}
    if not label_set:
        return ""
    try:
        headers = soup.select("#contents .cont_box th, .cont_box th")
    except Exception:
        headers = []
    for th in headers:
        label = _clean_text(th)
        if label not in label_set:
            continue
        try:
            td = th.find_next_sibling("td")
        except Exception:
            td = None
        value = _clean_text(td) if td else ""
        if value:
            return value
    return ""


def _build_yongin_water_detail_title(soup: Any, base_title: str) -> str:
    base_title = _strip_caption_suffix(base_title)
    if not base_title:
        base_title = _strip_caption_suffix(_select_text(soup, YONGIN_WATER_CAPTION_SELECTORS))

    company = _table_value_by_label(soup, "청소업체명", "검사업체명", "교육기관")
    tank = _table_value_by_label(soup, "저수조", "옥내급수관")
    year = _table_value_by_label(soup, "년도")
    half = _table_value_by_label(soup, "반기")
    start_date = _table_value_by_label(soup, "시작일", "검사일", "교육일")

    period = " ".join(part for part in (year, half) if part).strip()
    if not period:
        period = start_date

    detail_parts = []
    for part in (company, tank, period):
        if part and part not in detail_parts:
            detail_parts.append(part)

    if (
        base_title
        and detail_parts
        and (company or tank)
        and (base_title in YONGIN_WATER_GENERIC_TITLES or base_title.endswith("이력 상세"))
    ):
        return f"{base_title} - {' / '.join(detail_parts)}"
    if base_title:
        return base_title
    if detail_parts:
        return " / ".join(detail_parts)
    return ""


def extract_yongin_water_title(soup: Any) -> str:
    if soup is None:
        return ""
    title = _build_yongin_water_detail_title(soup, _select_text(soup, YONGIN_WATER_TITLE_SELECTORS))
    if title:
        return title
    title = _strip_caption_suffix(_select_text(soup, YONGIN_WATER_CAPTION_SELECTORS))
    if title:
        return title
    title = _select_text(soup, YONGIN_WATER_MENU_TITLE_SELECTORS)
    if title:
        return title
    return ""


def _remove_yongin_water_content_noise(soup: Any) -> None:
    for sel in YONGIN_WATER_CONTENT_EXCLUDE_SELECTORS:
        try:
            nodes = soup.select(sel)
        except Exception:
            nodes = []
        for node in nodes:
            try:
                node.decompose()
            except Exception:
                try:
                    node.extract()
                except Exception:
                    pass


def yongin_water_content_selector() -> str:
    return YONGIN_WATER_CONTENT_SELECTORS[0]


def try_extract_yongin_water_post(soup: Any, url: str):
    """
    BoardPostExtract 생성을 포함한 전용 추출.
    순환 의존을 피하기 위해 board_content_extractor 헬퍼는 함수 내부에서 가져온다.
    """
    if soup is None or not is_yongin_water_url(url):
        return None

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws,
        _extract_content_text,
        _extract_title,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _strip_noisy_tags,
        _trim_leading_skip_and_breadcrumb_text,
    )

    scope = soup.select_one("#contents") or soup.select_one("#container #contents")
    if not scope:
        return None

    title = extract_yongin_water_title(soup)

    body = (
        scope.select_one(".cont_box")
        or scope.select_one(".cont_box .tb_box")
        or scope
    )
    if not body:
        return None

    try:
        frag = BeautifulSoup(str(body), "html.parser")
    except Exception:
        return None
    root = frag.find(True)
    if not root:
        return None

    _remove_yongin_water_content_noise(frag)
    _strip_noisy_tags(frag)
    root = frag.find(True)
    if not root:
        return None

    content_text = _extract_content_text(root)
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not (content_text or "").strip():
        return None

    if not title:
        title = (_extract_title(soup, scope, selector_hint="#contents .h3_box h3") or "").strip()
    if not title:
        title = "제목 없음"

    content_html = _sanitize_html_fragment(root).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )
