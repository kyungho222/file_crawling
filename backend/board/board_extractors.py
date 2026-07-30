import logging
import re
import os
from typing import Optional

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

logger = logging.getLogger("backend.board.board_extractors")

def extract_url_from_js_string(js_text: str, base: str) -> Optional[str]:
    """JS 코드 내에서 게시글 상세 페이지 URL 패턴을 추출합니다."""
    try:
        if not js_text: return None
        s = str(js_text)
        
        # 1) 표준 .do 패턴 (재외동포청 등)
        m = re.search(r"(selectBbsNttView\.do[^\'\"\)\s>]*)", s, re.IGNORECASE)
        if m: return m.group(1)
        
        # 2) 파라미터 조합 (bbsNo, nttNo)
        b = re.search(r"bbsNo\s*[:=]\s*['\"]?(\d+)['\"]?", s, re.IGNORECASE)
        n = re.search(r"nttNo\s*[:=]\s*['\"]?(\d+)['\"]?", s, re.IGNORECASE)
        
        b_val = b.group(1) if b else re.search(r"bbsNo=(\d+)", s, re.IGNORECASE)
        if b_val and not isinstance(b_val, str): b_val = b_val.group(1)
        
        n_val = n.group(1) if n else re.search(r"nttNo=(\d+)", s, re.IGNORECASE)
        if n_val and not isinstance(n_val, str): n_val = n_val.group(1)
        
        if b_val and n_val:
            return f"selectBbsNttView.do?bbsNo={b_val}&nttNo={n_val}"
            
        # 3) 기타 이동 패턴
        m_href = re.search(r"location\.href\s*=\s*['\"]([^'\"]+)['\"]", s, re.IGNORECASE)
        if m_href: return m_href.group(1)
        
        m_open = re.search(r"open\(\s*['\"]([^'\"]+)['\"]", s, re.IGNORECASE)
        if m_open: return m_open.group(1)
        
    except Exception:
        return None
    return None

_JUNK_CONTAINER_SELECTORS = [
    ".pagination", ".paging", ".page-num",    # 페이지 번호 영역
    ".btn-group", ".view-nav", ".prev-next",  # 상세페이지 하단 버튼/이전글/다음글
    ".board-footer", ".board-navigation",      # 게시판 하단 부가 영역
    "header", "footer", "aside",              # 공통 레이아웃 영역
    ".navigation", "#sidebar", "#gnb"         # 메뉴 및 내비게이션
]

def remove_junk_elements(soup):
    """
    불필요한 링크가 포함된 구역을 삭제하여 탐색 범위를 본문으로 한정합니다.
    - 노원구청 등 상세페이지 내 목록 파라미터(pageIndex) 혼입 방지용
    """
    if not soup: 
        return soup
    
    # [보완] 제거된 개수를 로깅하여 디버깅에 활용할 수 있습니다.
    removed_count = 0
    for sel in _JUNK_CONTAINER_SELECTORS:
        targets = soup.select(sel)
        for junk in targets:
            junk.decompose()
            removed_count += 1
            
    if removed_count > 0:
        logger.debug(f"[Discovery] {removed_count}개의 불필요 영역 제거 완료 (Selector 기반)")
        
    return soup

def extract_title_from_html(soup) -> Optional[str]:
    """
    제목 추출 최종 병기.
    - strict-json 모드(oka)일 때는 하단 푸터나 메타 태그 로직을 '완전히 차단'합니다.
    """
    if not soup:
        return None

    try:
        from backend.board.jongno_board import (
            extract_jongno_apply_title,
            extract_jongno_board_title,
            extract_jongno_construction_status,
            extract_jongno_council_assembly_title,
            extract_jongno_minwon_form,
            is_jongno_board_article_url,
            is_jongno_apply_view_url,
            is_jongno_council_assembly_view_url,
        )

        canonical = ""
        try:
            canonical_el = soup.find("link", rel="canonical")
            canonical = canonical_el.get("href") if canonical_el else ""
        except Exception:
            canonical = ""
        page_url = str(getattr(soup, "_source_url", "") or canonical or "")
        html_head = str(soup)[:4000].lower()
        inferred_jongno_minwon_url = ""
        if not page_url:
            try:
                minwon_table = soup.select_one("table.view_type03")
                minwon_text = minwon_table.get_text(" ", strip=True) if minwon_table else ""
                if "민원사무명" in minwon_text:
                    inferred_jongno_minwon_url = (
                        "https://www.jongno.go.kr/portal/bbs/selectBoardArticle.do"
                        "?bbsId=BBSMSTR_000000000341"
                    )
            except Exception:
                inferred_jongno_minwon_url = ""
        if (
            is_jongno_board_article_url(page_url)
            or is_jongno_apply_view_url(page_url)
            or is_jongno_council_assembly_view_url(page_url)
            or bool(inferred_jongno_minwon_url)
            or ("jongno.go.kr" in html_head and "selectboardarticle.do" in html_head)
            or ("jongno.go.kr" in html_head and "selectapplyview.do" in html_head)
            or ("council.jongno.go.kr" in html_head and "chairman-detail" in html_head)
            or ("selectboardarticle.do" in html_head and "notice_title" in html_head)
        ):
            effective_url = page_url or inferred_jongno_minwon_url or "https://www.jongno.go.kr/portal/bbs/selectBoardArticle.do"
            jongno_special_post = (
                extract_jongno_minwon_form(soup, effective_url)
                or extract_jongno_construction_status(soup, effective_url)
                or {}
            )
            jongno_title = str(jongno_special_post.get("title") or "").strip() or (
                extract_jongno_council_assembly_title(soup, effective_url)
                or extract_jongno_apply_title(soup, effective_url)
                or extract_jongno_board_title(soup, effective_url)
                or ""
            ).strip()
            if jongno_title:
                return jongno_title
    except Exception:
        pass

    title_text: Optional[str] = None  # json_title_node 없을 때 89행 참조 방지

    # 1. 워크플로우에서 보낸 '엄격 모드' 신호 확인
    mode_tag = soup.select_one("meta[name='extraction-mode']")
    is_strict = mode_tag and mode_tag.get("content") == "strict-json"

    # 2. JSON 데이터 전용 ID로 제목 탐색 (가장 정확함)
    # select_one("#js-json-title")을 사용하여 0순위로 찾습니다.
    json_title_node = soup.select_one("#js-json-title") or soup.select_one(".js-json-title")
    
    if json_title_node:
        title_text = json_title_node.get_text(strip=True)
        
        # ✅ [핵심] oka(strict-json) 모드라면 여기서 즉시 반환!
        # 아래에 있는 일반 사이트용 선택자나 푸터 로직은 아예 실행되지 않습니다.
        if is_strict and title_text:
            return title_text

    # 3. [재외동포청이 아닐 때만 실행] 일반 사이트용 Fallback 로직
    if not is_strict:
        # 범용 선택자들 순차 탐색
        selectors = [
            ".inboxRead .headinfo > h3.BoR-h2",
            ".inboxRead .headinfo > h2.BoR-h2",
            ".headinfo > h3.BoR-h2",
            ".headinfo > h2.BoR-h2",
            "h3.BoR-h2",
            "h2.BoR-h2",
            "tr.p-table__subject .p-table__subject_text",
            ".p-table__subject .p-table__subject_text",
            ".p-table__subject_text",
            "h3.h0.title",
            ".h0.title",
            ".poll_view > h4",
            ".board_view_title",
            ".view_title",
            ".subject",
            ".title",
            ".tit",
            "h1",
        ]
        for sel in selectors:
            target = soup.select_one(sel)
            if target and target.get_text(strip=True):
                return re.sub(r'\s+', ' ', target.get_text(strip=True)).strip()

        # 최후의 수단: 메타 태그 (og:title)
        try:
            og = soup.select_one("meta[property='og:title']")
            if og and og.get("content"):
                return og["content"].strip()
        except:
            pass
            
    # oka 모드인데 위에서 return을 못 했다면(제목이 비었다면) 엉뚱한 푸터를 가져오느니 빈 값을 줍니다.
    return title_text if title_text else (soup.title.get_text(strip=True) if soup.title and not is_strict else None)
       
# --- 기타 유틸리티 함수들 ---

_NAV_CONTAINER_TAGS = ("nav", "header", "footer", "aside")
_NAV_CONTAINER_HINTS = ("menu", "gnb", "lnb", "snb", "sidebar", "nav", "header", "footer", "topmenu", "quick")

def is_nav_or_sidebar_anchor(a) -> bool:
    """링크가 본문이 아닌 네비게이션/메뉴 영역에 있는지 판별합니다."""
    try:
        for parent in a.parents:
            name = (getattr(parent, "name", "") or "").lower()
            if name in _NAV_CONTAINER_TAGS:
                return True
            
            pid = parent.get("id") or ""
            classes = parent.get("class") or []
            if isinstance(classes, str): classes = [classes]
            
            for token in [pid, *classes]:
                if any(h in str(token).lower() for h in _NAV_CONTAINER_HINTS):
                    return True
    except Exception:
        return False
    return False

def normalize_text_for_match(value: str) -> str:
    """텍스트 비교를 위해 공백 및 특수문자를 제거하고 소문자로 바꿉니다."""
    if not value: return ""
    return re.sub(r"[\s\W_]+", "", str(value).lower(), flags=re.UNICODE)

def match_cate_in_title(cate: str, title: str) -> bool:
    """카테고리명이 제목에 포함되어 있는지 유사도 검사를 수행합니다."""
    if not cate or not title: return False
    cate_norm = normalize_text_for_match(cate)
    title_norm = normalize_text_for_match(title)
    
    from difflib import SequenceMatcher
    threshold = float(os.getenv("CATE_NAME_FUZZY_THRESHOLD", "0.92"))
    return SequenceMatcher(None, cate_norm, title_norm).ratio() >= threshold
