"""
서울역사박물관(museum.seoul.go.kr) 상세 HTML 노이즈 제거.

- article#content 상단: 브레드크럼(#page_loc), 인쇄(#btn_print) 등
- 유물 기증 상세 등: 「사진 확대보기」 블록(캡션·썸네일 중복 텍스트)
- 게시판 상세(NR_boardView.do 등): 본문 루트는 `#contents` 또는 `article#content`. 제목은
  `div.article_info h3.tit_article`(발간도서) → `div.exhibit_info.info_area h3.tit`(전시·로비전) →
  `table#boardTbl caption.tit_article`(교육예약·표 상단 제목) → `h2#tit_page` → `caption.tit_article` 순으로 시도.
- NR_boardView 팝업(헤더·`#wrap` 없음): `body` 전체를 본문으로 쓰고 `.btn_area`·`form#dataForm`·`a.btn` 등만 제거.

게시판 DB명과 무관하게 URL 도메인이 museum.seoul.go.kr 이면 적용한다.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

_KOR_YMD = re.compile(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일")


def is_museum_seoul_url(url: str) -> bool:
    if not url:
        return False
    return "museum.seoul.go.kr" in (url or "").lower()


# CSS `a, b, c` 한 번에 select_one 하면 문서 순서로만 고르므로, 의도한 우선순위를 위해 순차 시도한다.
MUSEUM_SEOUL_TITLE_SELECTORS_ORDERED = (
    ".imgboardview-header .view-info .view-title",
    ".contents-wrap .imgboardview-header .view-title",
    ".collection-view .imgboardview-header .view-title",
    "h2.view-title",
    "#content article#content #bo_view .view_tit h5",
    "#content #bo_view .view_tit h5",
    "#bo_view .view_tit h5",
    "#bo_view .view_tit h4",
    ".bo_table .view_tit h4",
    ".view_tit.thumb .info h4",
    ".view_tit .info h4",
    ".bo_table .view_tit h5",
    ".view_tit h4",
    ".view_tit h5",
    "div.article_info h3.tit_article",
    "div.article_info ul.info li.wide",
    "ul.info li.wide",
    "div.exhibit_info.info_area h3.tit",
    "table#boardTbl caption.tit_article",
    "h2#tit_page",
    "caption.tit_article",
)


def _clean_museum_caption_title(text: str, *, selector: str = "") -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "").replace("\xa0", " ")).strip()
    if not cleaned:
        return ""
    if "caption.tit_article" not in (selector or ""):
        if "view_tit" in (selector or ""):
            without_category = re.sub(r"^\[[^\]]{1,20}\]\s*", "", cleaned).strip()
            return without_category if len(without_category) >= 6 else cleaned
        return cleaned
    without_category = re.sub(r"^\[[^\]]{1,20}\]\s*", "", cleaned).strip()
    return without_category if len(without_category) >= 6 else cleaned


def _clean_museum_info_wide_title(el: Any, *, selector: str = "") -> str:
    raw = re.sub(r"\s+", " ", (el.get_text(" ", strip=True) or "")).strip()
    if not raw or "li.wide" not in (selector or ""):
        return raw
    try:
        label_el = el.find(["strong", "b", "dt"])
        label_text = label_el.get_text(" ", strip=True) if label_el else ""
        label = re.sub(r"\s+", "", label_text)
    except Exception:
        label_el = None
        label_text = ""
        label = ""
    if label and any(key in label for key in ("기증자명", "기증자", "성명", "이름")):
        try:
            value = re.sub(r"^\s*" + re.escape(label_text) + r"\s*", "", raw).strip()
        except Exception:
            value = raw
        return value if len(value) >= 2 else raw
    return raw


def extract_museum_seoul_priority_title(soup: Any) -> str:
    """박물관 상세 제목: 발간 h3 → 전시 h3.tit → boardTbl caption → 페이지 h2 → 기타 caption 순."""
    if soup is None:
        return ""
    for sel in MUSEUM_SEOUL_TITLE_SELECTORS_ORDERED:
        try:
            el = soup.select_one(sel)
            if not el:
                continue
            t = _clean_museum_info_wide_title(el, selector=sel)
            t = _clean_museum_caption_title(t, selector=sel)
            if t:
                return t
        except Exception:
            pass
    return ""


def is_museum_seoul_giver_view_url(url: str) -> bool:
    """유물 기증 상세(NR_giverView 등)."""
    if not is_museum_seoul_url(url):
        return False
    u = (url or "").lower().replace("_", "")
    if "/relic/giver/" in (url or "").lower():
        return True
    if "nrgiverview.do" in u:
        return True
    return False


def is_museum_seoul_board_view_url(url: str) -> bool:
    """게시판 상세 NR_boardView.do."""
    if not is_museum_seoul_url(url):
        return False
    u = (url or "").lower().replace("_", "")
    return "nrboardview.do" in u


def is_museum_seoul_board_popup_html(soup: Any) -> bool:
    """레이아웃 래퍼 없이 본문만 오는 팝업/프레임용 HTML."""
    if soup is None:
        return False
    try:
        if soup.select_one("#wrap"):
            return False
        return bool(soup.select_one("body"))
    except Exception:
        return False


def strip_museum_seoul_popup_board_ui(soup: Any) -> None:
    """팝업형 게시 상세: 목록·인라인 버튼 링크 등만 제거(표·본문은 유지)."""
    if soup is None:
        return
    for sel in (".btn_area", "form#dataForm", 'form[name="dataForm"]', "#dataForm"):
        try:
            for el in soup.select(sel):
                try:
                    el.decompose()
                except Exception:
                    pass
        except Exception:
            pass
    try:
        for a in list(soup.select("a.btn")):
            try:
                a.decompose()
            except Exception:
                pass
    except Exception:
        pass


def _korean_ymd_to_datetime(m: re.Match[str]) -> Optional[datetime]:
    try:
        dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None
    try:
        from backend.shared.date_utils import _is_reasonable_date

        if not _is_reasonable_date(dt):
            return None
    except Exception:
        if dt.year < 1900 or dt.year > 2100:
            return None
    return dt


def extract_museum_seoul_giver_reg_date_from_soup(soup: Any) -> Optional[datetime]:
    """
    기증 상세에 별도 '등록일'이 없을 때, 「기증자 소개」 문단의 첫 YYYY년 M월 D일을
    기준 등록일(기증일)로 사용한다.
    """
    if soup is None:
        return None

    def _from_text(block: str) -> Optional[datetime]:
        if not block:
            return None
        m = _KOR_YMD.search(block)
        if not m:
            return None
        return _korean_ymd_to_datetime(m)

    try:
        for tag in soup.find_all(["th", "dt", "label"]):
            raw = (tag.get_text("", strip=True) or "").strip()
            clean = re.sub(r"\s+", "", raw)
            if clean != "기증자소개" and ("기증자" not in clean or "소개" not in clean):
                continue
            for sib in (tag.find_next_sibling(["td", "dd", "div"]),):
                if sib:
                    dt = _from_text(sib.get_text(" ", strip=True))
                    if dt:
                        return dt
            tr = tag.find_parent("tr")
            if tr:
                for td in tr.find_all("td"):
                    if tag in td.descendants:
                        continue
                    dt = _from_text(td.get_text(" ", strip=True))
                    if dt:
                        return dt
    except Exception:
        pass

    try:
        root = soup.select_one("article#content") or soup.select_one("#content")
        if root:
            for li in root.find_all("li"):
                tx = li.get_text(" ", strip=True)
                if "기증자" not in tx or "소개" not in tx:
                    continue
                dt = _from_text(tx)
                if dt:
                    return dt
    except Exception:
        pass

    try:
        root = soup.select_one("article#content") or soup.select_one("#content")
        if root:
            full = root.get_text(" ", strip=True)
            for m in _KOR_YMD.finditer(full):
                start = max(0, m.start() - 100)
                window = full[start : m.end()]
                if "기증" in window:
                    dt = _korean_ymd_to_datetime(m)
                    if dt:
                        return dt
    except Exception:
        pass

    return None


def strip_museum_seoul_board_noise(soup: Any) -> None:
    if soup is None:
        return

    for sel in ("#page_loc", "p#page_loc", "#btn_print"):
        try:
            for el in soup.select(sel):
                try:
                    el.decompose()
                except Exception:
                    pass
        except Exception:
            pass

    try:
        for el in soup.select(".page_head .sns_open, .page_head .sns_area, .page_head .util"):
            try:
                el.decompose()
            except Exception:
                pass
    except Exception:
        pass

    _strip_photo_zoom_blocks(soup)


def _strip_photo_zoom_blocks(soup: Any) -> None:
    """h3~h5 '사진 확대보기' 인접 이미지 영역(짧은 캡션·파일명 나열) 제거."""
    if soup is None:
        return
    try:
        heads = list(soup.find_all(["h3", "h4", "h5"]))
    except Exception:
        return

    for h in heads:
        try:
            t = h.get_text(" ", strip=True)
        except Exception:
            continue
        if not t:
            continue
        if t == "사진 확대보기":
            pass
        elif "사진" in t and "확대보기" in t and len(t) <= 24:
            pass
        else:
            continue

        try:
            sib = h.find_next_sibling()
            if sib is not None and sib.name in ("div", "ul", "section", "figure"):
                inner = sib.get_text(" ", strip=True)
                if inner and len(inner) < 500:
                    try:
                        sib.decompose()
                    except Exception:
                        pass
            try:
                h.decompose()
            except Exception:
                pass
        except Exception:
            pass

    try:
        for a in list(soup.find_all("a", href=True)):
            try:
                tx = a.get_text(" ", strip=True)
            except Exception:
                continue
            if tx == "사진 확대보기" or ("확대보기" in tx and len(tx) < 24):
                try:
                    a.decompose()
                except Exception:
                    pass
    except Exception:
        pass

    try:
        for tag in soup.find_all(["span", "p", "strong", "em"]):
            try:
                tx = tag.get_text(" ", strip=True)
            except Exception:
                continue
            if tx == "사진 확대보기" and not tag.find(["span", "p", "div"]):
                try:
                    tag.decompose()
                except Exception:
                    pass
    except Exception:
        pass
