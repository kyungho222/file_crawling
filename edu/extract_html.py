import os
import logging
import random
import re
from typing import Dict, List, Optional

from utils.hash_policy import sha256_hex_utf8
from bs4 import BeautifulSoup, Comment
from urllib.parse import urlparse, urljoin
from utils.url import get_safe_url, infer_content_type
import aiohttp
from utils.logging_util import LoggerSingleton
try:
    from edu.classes import CrawlStopSignal
except Exception:
    class CrawlStopSignal:
        def __init__(self):
            self.should_stop = False
            self.stop_reason = ""

        def set_stop(self, reason: str = "user_stop"):
            self.should_stop = True
            self.stop_reason = reason

        def is_stopped(self) -> bool:
            return self.should_stop

        def get_reason(self) -> str:
            return self.stop_reason

# 로거 설정
logger = LoggerSingleton.get_logger(logger_name="edu.extract_html", level=logging.INFO)

__all__ = [
    'extract_main_content',
    'extract_text_from_html',
    'extract_subject_for_crawling_mode',
    'extract_sub_subject_for_crawling_mode',
    'extract_page_snippet',
    'clean_snippet_text',
    'extract_favicon_url',
    'get_random_user_agent',
    'parse_html_content',
    'parse_html_content_for_crawling_mode',
    'build_structured_content',
    'extract_with_http',
    'extract_with_playwright',
]

# ============================================================================
# Private 설정 상수들
# ============================================================================

# 메인 콘텐츠 추출 관련
_MAIN_CONTENT_EXCLUDE_KEYWORDS = [
    'ad', 'advertisement', 'banner', 'menu', 'nav', 'sidebar',
    'footer', 'header', 'social', 'share', 'comment'
]

_MAIN_CONTENT_SELECTORS = [
    'main', '[role="main"]', '.main', '#main',
    'article', '.article', '.content', '#content',
    '.post', '.entry', '.board_view_content'
]

# 텍스트 추출 관련
_TAGS_TO_REMOVE = [
    "script", "style", "noscript", "head", "meta", "a", "a href",
    "iframe", "footer", "nav", "button", "select", "label",
    "legend", "fieldset", "option", "aside"
]

_POPUP_CLASSES = [
    "popup", "modal", "overlay", "terms_ft_pop", "popup_content",
    "PrtcPolicy", "infoPolicy", "sitemap", "layer_popup"
]

_REMOVE_CLASSES = [
    "menu", "footer", "header", "top_menu_layer", "navigation",
    "nav-menu", "spam-box"
]

_BLOCK_TAGS = {'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'li', 'blockquote', 'pre', 'td', 'th', 'dt', 'dd'}

# a 태그 허용 클래스(정확히 일치하는 경우만 유지)
_ALLOWED_A_TAG_CLASSES = [
    # 예: "keep-link", "board_view_link primary"
    "bold"
]

# 스니펫 정제 관련
_SNIPPET_UNWANTED_PATTERNS = [
    "html", "HTML", ">", "<", "javascript:", "mailto:", "tel:",
    "css", "CSS", "script", "SCRIPT", "style", "STYLE"
]

# 제목 추출 관련
_SUBJECT_CLASS_KEYWORDS = [
    'subject', 'sub', 'heading', '제목', 'headline', 'tit', 'pg-tit',
    'pg-title', 'pg-hd-tit', 'sub-tit', 'sub-title', 'sub-cont1',
    'title', 'sp-title', 'big_title', 'htit', 'p-table__subject_text',
    'sub_title', 'subject_title', 'main-content-tit', 'head', 'board-view',
    'content-title'
]

_SUBJECT_ID_KEYWORDS = [
    'title', 'subject', 'heading', 'tit', 'sub', 'head', 'sub-cont1'
]

_COMPOUND_TITLE_CONTAINERS = ['sub-search', 'search-area', 'title-container']

_COMPOUND_TITLE_EXCLUDE_CLASSES = [
    'small_title', 'eng_title', 'sub_desc', 'skip', 'satisfaction', 'blind'
]

_TITLE_SEARCH_ROOT_SELECTORS = [
    "#view_div",
    ".bod_view",
    ".board_view",
    ".view_top",
    ".view_cont",
    ".view-cont",
    ".p-table",
    "#contents",
    "#content",
    "#conts",
    "article",
    "main",
    "[role='main']",
]

_TITLE_FAST_PATH_SELECTORS = [
    ".inboxRead .headinfo > h3.BoR-h2",
    ".inboxRead .headinfo > h2.BoR-h2",
    ".headinfo > h3.BoR-h2",
    ".headinfo > h2.BoR-h2",
    ".board-view-title h3",
    ".board-view-head .board-view-title h3",
    ".poll_view > h4.no_bgimg.mT0",
    ".poll_view h4.no_bgimg",
    ".poll_view > h4",
    ".view_top .tit",
    ".board_view .tit",
    ".bod_view h4",
    ".board_view h4",
    ".view_cont h4",
    ".view-cont h4",
    ".board_view_title",
    ".bod_view_title",
    ".view_title",
    ".view-title",
    "caption.tit_article",
    "table caption.tit_article",
    ".p-table__subject_text",
    ".bbs-header strong.tit",
    ".bbs-header .tit",
    ".title .lf strong",
    ".title strong",
    "strong.tit",
]

_NAV_EXCLUDE_CONFIG = {
    'single_keywords': [
        'left-nav', 'navigation', 'nav', 'menu', 'sidebar', 'left-menu',
        'side-nav', 'gnb', 'lnb', 'top-nav', 'main-nav', 'left-tit', 'aside',
        'submenu', 'area', 'skip', 'jump_menu', 'blind',
        # 헤더/푸터 관련 일반 키워드
        'header', 'footer', 'site-header', 'site-footer',
        'global-header', 'global-footer',
        # 헤더 관련 클래스 추가
        'header-box', 'quick-links', 'header-tp', 'header-tp-right', 
        'header-tp-left', 'tp-right-util', 'quick-links-btn', 'quick-links-list'
    ],
    'exact_matches': [
        'tit mt20', 'sub-visual sb5', 'sub-visual sb1', 'box_title h0'
    ],
    'id_keywords': [
        'jump_menu', 'skip', 'gnb', 'lnb', 'nav', 'menu', 'header', 'footer',
        'nuri-header-top', 'layout-wrap', 'skipnavigation'
    ]
}

# 서브 제목 추출 관련 (테스트용 기본값)
_DEFAULT_SUB_SUBJECT_CLASSES = [
    'chattyFloor2 width-auto', 'chattyFloor6 chattyFloor3',
    'chattyFloor3', 'toctext'
]

# User-Agent 목록
_USER_AGENTS = [
    # Windows Chrome (최신 버전들)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    # Windows Firefox (최신 버전들)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",
    # Windows Edge (최신 버전들)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    # macOS Safari (최신 버전들)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    # macOS Chrome
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # 한국 정부 사이트 호환성을 위한 IE 스타일
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (compatible; MSIE 11.0; Windows NT 10.0; WOW64; Trident/7.0)",
    # 모바일 시뮬레이션
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    # 구버전 브라우저 (레거시 시스템 호환)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
]

# ============================================================================
# Private 헬퍼 함수들
# ============================================================================

def _is_nav_excluded(element, config: Dict) -> bool:
    """네비게이션 관련 요소인지 확인하는 함수 (class 및 id 속성 모두 확인)
    
    Args:
        element: BeautifulSoup 요소
        config: 네비게이션 제외 설정 딕셔너리
        
    Returns:
        네비게이션 요소인 경우 True, 아니면 False
    """
    # 태그 이름 기반 1차 제외
    if element and element.name in ['nav', 'header', 'footer', 'aside']:
        return True

    # class 속성 확인
    element_classes = element.get('class', [])
    if isinstance(element_classes, str):
        element_classes = [element_classes]
    
    element_classes_lower = [cls.lower() for cls in element_classes]
    element_classes_set = set(element_classes_lower)
    
    # 1. 단일 클래스 키워드 매치 (하나만 있어도 제외)
    if element_classes_set & set(config['single_keywords']):
        return True

    # 1.5 클래스 문자열 부분 매치 (ex: total-menu-container)
    for cls_name in element_classes_lower:
        for keyword in config['single_keywords']:
            if keyword in cls_name:
                return True
    
    # 2. 정확한 클래스 문자열 매치 (순서 무관하게 정확히 일치)
    element_class_str = ' '.join(sorted(element_classes_lower))
    for exact_match in config['exact_matches']:
        exact_match_sorted = ' '.join(sorted(exact_match.lower().split()))
        if element_class_str == exact_match_sorted:
            return True
    
    # 3. id 속성 키워드 매치 (id에 키워드가 포함되면 제외)
    element_id = element.get('id', '')
    if element_id:
        element_id_lower = element_id.lower()
        for id_keyword in config['id_keywords']:
            if id_keyword.lower() in element_id_lower:
                return True
    
    return False


def _extract_compound_title(container, exclude_classes: List[str]) -> str:
    """복합 제목 추출 함수 (sub-search 등 특수 컨테이너용)
    
    Args:
        container: BeautifulSoup 컨테이너 요소
        exclude_classes: 제외할 클래스 리스트
        
    Returns:
        추출된 복합 제목 문자열, 실패 시 None
    """
    try:
        # 컨테이너에서 모든 텍스트 수집
        title_parts = []
        
        # 직접 텍스트 노드 추출 (태그 밖의 텍스트)
        for content in container.stripped_strings:
            # 제외할 요소들의 텍스트인지 확인
            parent_element = None
            for element in container.find_all(string=content):
                if element.parent:
                    parent_element = element.parent
                    break
            
            if parent_element:
                parent_classes = parent_element.get('class', [])
                if isinstance(parent_classes, str):
                    parent_classes = [parent_classes]
                
                # 제외할 클래스에 속하는지 확인
                if not any(exclude_class in parent_classes for exclude_class in exclude_classes):
                    # 불필요한 기호 제거
                    cleaned_content = content.strip()
                    if cleaned_content and cleaned_content not in ['-', '|', ':', '>', '<', '(', ')', '[', ']']:
                        title_parts.append(cleaned_content)
        
        if title_parts:
            # 중복 제거 및 정리
            unique_parts = []
            for part in title_parts:
                if part not in unique_parts and len(part.strip()) > 1:
                    unique_parts.append(part.strip())
            
            # 조합해서 제목 생성
            compound_title = ' '.join(unique_parts)
            
            # 최종 정리 (연속된 공백 제거 등)
            compound_title = re.sub(r'\s+', ' ', compound_title).strip()
            
            # 길이 제한 (너무 길면 제외)
            if 5 <= len(compound_title) <= 100:
                logger.info(f"[복합제목추출] 컨테이너에서 추출: '{compound_title}'")
                return compound_title
                
    except Exception as e:
        logger.warning(f"[복합제목추출 오류] 컨테이너 처리 중 오류: {e}")
        
    return None


def _iter_title_search_roots(soup: BeautifulSoup):
    """대표 콘텐츠 루트를 먼저 훑어 제목 탐색 범위를 줄인다."""
    seen = set()
    for selector in _TITLE_SEARCH_ROOT_SELECTORS:
        try:
            root = soup.select_one(selector)
        except Exception:
            root = None
        if root is None:
            continue
        root_id = id(root)
        if root_id in seen:
            continue
        seen.add(root_id)
        yield root
    yield soup


def _is_title_candidate_excluded(element, exclude_fn) -> bool:
    if not element:
        return True
    if exclude_fn(element):
        return True
    if any(parent.name in ['header', 'footer'] for parent in element.parents):
        return True
    if any(exclude_fn(parent) for parent in element.parents):
        return True
    return False


def _extract_title_fast_path(soup: BeautifulSoup, exclude_fn) -> str:
    """자주 맞는 공통 제목 셀렉터를 먼저 확인해 전체 DOM 순회를 줄인다."""
    selector_query = ", ".join(_TITLE_FAST_PATH_SELECTORS)
    for root in _iter_title_search_roots(soup):
        try:
            candidates = root.select(selector_query)
        except Exception:
            continue
        for element in candidates:
            if _is_title_candidate_excluded(element, exclude_fn):
                continue
            text = _get_title_text_allowing_single_link(element)
            if text and len(text) > 5:
                return text
    return ""


def _get_title_text_allowing_single_link(tag) -> str:
    """제목 후보 태그의 텍스트는 비어있지 않으면 반환한다."""
    if not tag:
        return ""

    def _collapse_title_ws(text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    def _strip_status_title_noise(text: str) -> str:
        cleaned = _collapse_title_ws(text)
        if not cleaned:
            return ""

        cleaned = re.sub(
            r"^(?:(?:접수중|접수마감|접수예정|신청중|신청마감|모집중|모집마감|공고중|진행중|진행예정|상시)\s*(?:\([^)]*\))?\s*)+",
            "",
            cleaned,
        ).strip()
        cleaned = re.sub(r"\s+\d{4}\.\d{1,2}\.\d{1,2}\.?\s*$", "", cleaned).strip()
        cleaned = _strip_title_meta_tail(cleaned)
        return cleaned

    def _strip_title_meta_tail(text: str) -> str:
        labels = (
            "작성자",
            "등록자",
            "등록인",
            "담당자",
            "글쓴이",
            "성명",
            "담당부서",
            "부서",
            "부서명",
            "작성부서",
            "작성부서명",
            "등록일",
            "작성일",
            "게시일",
            "수정일",
            "조회수",
            "조회",
        )
        split_idx = -1
        matched_label = ""
        for label in labels:
            token = f" {label}"
            idx = text.find(token)
            if idx == -1:
                continue
            after_idx = idx + len(token)
            next_char = text[after_idx:after_idx + 1]
            if next_char and not next_char.isspace() and next_char not in ":：":
                continue
            if split_idx == -1 or idx < split_idx:
                split_idx = idx
                matched_label = label

        if split_idx < 0:
            return text

        tail = text[split_idx:].strip()
        label_hits = sum(1 for label in labels if f" {label}" in tail)
        has_date = bool(
            re.search(
                r"\d{4}[-./]\d{1,2}[-./]\d{1,2}|\d{4}년\s*\d{1,2}월\s*\d{1,2}일",
                tail,
            )
        )
        if label_hits >= 2 or (matched_label in {"등록일", "작성일", "게시일", "수정일", "조회수", "조회"} and has_date) or (label_hits >= 1 and has_date):
            return text[:split_idx].strip(" -|:/")
        return text

    def _is_noise_title_subtree(node) -> bool:
        for parent in [node] + list(getattr(node, "parents", [])):
            if not getattr(parent, "get", None):
                continue
            classes = parent.get('class', [])
            if isinstance(classes, str):
                classes = [classes]
            class_tokens = [str(cls).lower() for cls in classes]
            element_id = str(parent.get('id', '') or '').lower()
            haystack = class_tokens + ([element_id] if element_id else [])
            if any(
                keyword in token
                for token in haystack
                for keyword in ("tag", "badge", "label", "date", "status", "state")
            ):
                return True
        return False

    if tag.name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
        full_text = _collapse_title_ws(tag.get_text(" ", strip=True))
        return _strip_status_title_noise(full_text)

    for nested in tag.select("strong, h1, h2, h3, h4, h5, h6"):
        if nested is tag or _is_noise_title_subtree(nested):
            continue
        nested_text = _strip_status_title_noise(nested.get_text(" ", strip=True))
        if nested_text:
            return nested_text

    full_text = _collapse_title_ws(tag.get_text(" ", strip=True))
    if not full_text:
        return ""

    return _strip_status_title_noise(full_text)


def _extract_clean_title_text(tag) -> str:
    """제목 후보 태그에서 상태 배지 등을 제외한 텍스트를 반환한다."""
    if not tag:
        return ""

    cleaned = _get_title_text_allowing_single_link(tag)
    if cleaned:
        return cleaned

    return re.sub(r"\s+", " ", tag.get_text(" ", strip=True)).strip()


def _clean_caption_article_title(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(text or "").replace("\xa0", " ")).strip()
    if not cleaned:
        return ""
    without_category = re.sub(r"^\[[^\]]{1,20}\]\s*", "", cleaned).strip()
    return without_category if len(without_category) >= 6 else cleaned


def _extract_caption_article_title(soup: BeautifulSoup) -> str:
    for selector in (
        "#content .page_cont caption.tit_article",
        "#content caption.tit_article",
        "table caption.tit_article",
        "caption.tit_article",
    ):
        try:
            element = soup.select_one(selector)
            if not element:
                continue
            title = _clean_caption_article_title(_extract_clean_title_text(element))
            if title and len(title) > 5:
                return title
        except Exception:
            continue
    return ""


def _extract_museum_seoul_priority_title_for_html(soup: BeautifulSoup, url: str) -> str:
    try:
        from backend.board.museum_seoul_board import extract_museum_seoul_priority_title, is_museum_seoul_url

        if is_museum_seoul_url(url or ""):
            return (extract_museum_seoul_priority_title(soup) or "").strip()
    except Exception:
        pass
    return ""


def _extract_title_from_heading_with_class(
    soup: BeautifulSoup,
    subject_class_keywords: List[str],
    subject_id_keywords: List[str],
    exclude_fn
) -> str:
    """1단계-1: 제목 클래스가 있는 h 태그에서 추출
    
    Args:
        soup: BeautifulSoup 객체
        subject_class_keywords: 제목 클래스 키워드 리스트
        subject_id_keywords: 제목 ID 키워드 리스트
        exclude_fn: 네비게이션 제외 판단 함수
        
    Returns:
        추출된 제목 문자열, 실패 시 빈 문자열
    """
    subject_class_keywords_set = {keyword.lower() for keyword in subject_class_keywords}
    subject_id_keywords_lower = [keyword.lower() for keyword in subject_id_keywords]
    heading_names = [f"h{i}" for i in range(1, 9)]

    for root in _iter_title_search_roots(soup):
        for h_tag in root.find_all(heading_names):
            if _is_title_candidate_excluded(h_tag, exclude_fn):
                continue

            h_class = {cls.lower() for cls in h_tag.get('class', [])}
            if h_class and h_class & subject_class_keywords_set:
                extracted_title = _get_title_text_allowing_single_link(h_tag)
                if extracted_title:
                    return extracted_title

            h_id = str(h_tag.get('id', '') or '').lower()
            if h_id and any(keyword in h_id for keyword in subject_id_keywords_lower):
                extracted_title = _get_title_text_allowing_single_link(h_tag)
                if extracted_title:
                    return extracted_title

        for p_tag in root.find_all("p"):
            if _is_title_candidate_excluded(p_tag, exclude_fn):
                continue

            p_class = {cls.lower() for cls in p_tag.get('class', [])}
            if p_class and p_class & subject_class_keywords_set:
                extracted_title = _get_title_text_allowing_single_link(p_tag)
                if extracted_title:
                    return extracted_title
    
    return ""


def _extract_title_from_heading_general(soup: BeautifulSoup, exclude_fn) -> str:
    """1단계-2: 일반 h 태그에서 추출
    
    Args:
        soup: BeautifulSoup 객체
        exclude_fn: 네비게이션 제외 판단 함수
        
    Returns:
        추출된 제목 문자열, 실패 시 빈 문자열
    """
    heading_names = [f"h{i}" for i in range(1, 9)]

    for root in _iter_title_search_roots(soup):
        for h_tag in root.find_all(heading_names):
            if _is_title_candidate_excluded(h_tag, exclude_fn):
                continue

            if h_tag.get_text(strip=True):
                extracted_title = _get_title_text_allowing_single_link(h_tag)
                if extracted_title:
                    return extracted_title

    return ""


def _extract_title_from_other_tags(
    soup: BeautifulSoup,
    subject_class_keywords: List[str],
    exclude_fn
) -> str:
    """1단계-3: h 태그 외 다른 태그에서 추출
    
    Args:
        soup: BeautifulSoup 객체
        subject_class_keywords: 제목 클래스 키워드 리스트
        exclude_fn: 네비게이션 제외 판단 함수
        
    Returns:
        추출된 제목 문자열, 실패 시 빈 문자열
    """
    subject_class_keywords_set = {keyword.lower() for keyword in subject_class_keywords}

    for root in _iter_title_search_roots(soup):
        for tag_name in ['div', 'span', 'p', 'strong', 'b', 'em', 'section']:
            for tag in root.find_all(tag_name):
                tag_class = {cls.lower() for cls in tag.get('class', [])}
                if not tag_class or not (tag_class & subject_class_keywords_set):
                    continue
                if _is_title_candidate_excluded(tag, exclude_fn):
                    continue

                raw_text = tag.get_text(strip=True)
                extracted_title = _get_title_text_allowing_single_link(tag)
                if extracted_title and len(raw_text) <= 200:  # 너무 긴 텍스트는 제외
                    return extracted_title
    
    return ""


def _extract_title_fallback(soup: BeautifulSoup) -> str:
    """2-3단계: article, meta, title 태그에서 추출 (대안 방법)
    
    Args:
        soup: BeautifulSoup 객체
        
    Returns:
        추출된 제목 문자열, 실패 시 빈 문자열
    """
    # Article title
    article = soup.find("article")
    if article:
        article_h1 = article.find("h1")
        if article_h1 and article_h1.get_text(strip=True):
            # a 태그가 포함된 h 태그는 제외
            if not article_h1.find("a"):
                extracted_title = _extract_clean_title_text(article_h1)
                return extracted_title
        
        # article 내 다른 제목 태그들 시도
        for i in range(2, 7):  # h2~h6
            article_h = article.find(f"h{i}")
            if article_h and article_h.get_text(strip=True):
                # a 태그가 포함된 h 태그는 제외
                if not article_h.find("a"):
                    extracted_title = _extract_clean_title_text(article_h)
                    return extracted_title
    
    # Open Graph / Twitter 제목
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        og_content = og_title.get("content").strip()
        if og_content:
            return og_content

    twitter_title = soup.find("meta", attrs={"name": "twitter:title"})
    if twitter_title and twitter_title.get("content"):
        twitter_content = twitter_title.get("content").strip()
        if twitter_content:
            return twitter_content

    # Meta description을 제목으로 사용 (짧은 경우만)
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        desc_content = meta_desc.get("content").strip()
        if len(desc_content) <= 100:  # 짧은 설명만 제목으로 사용
            return desc_content
    
    # 3단계: title 태그에서 추출
    if soup.title and soup.title.string:
        extracted_title = soup.title.string.strip()
        return extracted_title
    
    return ""


def _extract_text_blocks_recursive(element, block_texts: List[str], block_tags: set) -> List[str]:
    """DOM 트리를 순회하면서 블록 요소의 텍스트만 추출
    
    Args:
        element: BeautifulSoup 요소
        block_texts: 추출된 텍스트를 저장할 리스트
        block_tags: 블록 태그 집합
        
    Returns:
        추출된 블록 텍스트 리스트
    """
    if element.name in block_tags:
        # 블록 요소인 경우 직접 텍스트 추출
        text = element.get_text(separator=' ', strip=True)
        if text and len(text) > 5:  # 의미있는 텍스트만 (최소 5자)
            block_texts.append(text)
    elif element.name == 'div':
        # div는 자식 요소들을 재귀적으로 처리
        for child in element.children:
            if hasattr(child, 'name'):  # Tag 객체인 경우
                _extract_text_blocks_recursive(child, block_texts, block_tags)
    
    return block_texts


def _remove_duplicate_blocks(blocks: List[str]) -> List[str]:
    """중복 블록 제거 로직 (부분 포함 관계도 고려)
    
    Args:
        blocks: 블록 텍스트 리스트
        
    Returns:
        중복이 제거된 블록 텍스트 리스트
    """
    unique_blocks = []
    seen_blocks = set()
    
    for block in blocks:
        cleaned_block = block.strip()
        # 중복 체크 (부분 포함 관계도 고려)
        is_duplicate = False
        to_remove = set()  # 제거할 항목들을 별도로 수집
        
        for seen in seen_blocks:
            if cleaned_block in seen or seen in cleaned_block:
                if len(cleaned_block) <= len(seen):
                    is_duplicate = True
                    break
                else:
                    # 더 긴 텍스트로 교체할 항목을 수집
                    to_remove.add(seen)
        
        # 수집된 항목들을 제거
        for seen in to_remove:
            seen_blocks.discard(seen)
            # unique_blocks에서도 해당 항목 제거
            unique_blocks = [b for b in unique_blocks if b.strip() != seen]
        
        if not is_duplicate and len(cleaned_block) > 5:
            unique_blocks.append(cleaned_block)
            seen_blocks.add(cleaned_block)
    
    return unique_blocks


# ============================================================================
# Public API 함수들
# ============================================================================

def extract_main_content(html_content: str) -> str:
    """
    HTML에서 메인 콘텐츠 영역만 추출하여 의미있는 변경 감지
    
    Args:
        html_content: HTML 콘텐츠 문자열
        
    Returns:
        추출된 메인 콘텐츠 텍스트
    """
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        
        # 스크립트, 스타일, 광고 등 제거
        for tag in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            tag.decompose()
        
        # 광고나 네비게이션 관련 클래스 제거
        for tag in soup.find_all(class_=lambda x: x and any(
            keyword in str(x).lower() for keyword in _MAIN_CONTENT_EXCLUDE_KEYWORDS
        )):
            tag.decompose()
        
        # 메인 콘텐츠 영역 우선 순위로 추출
        main_selectors = _MAIN_CONTENT_SELECTORS
        
        main_content = ""
        for selector in main_selectors:
            main_element = soup.select_one(selector)
            if main_element:
                main_content = main_element.get_text(separator=' ', strip=True)
                break
        
        # 메인 영역을 찾지 못한 경우 body 전체 사용
        if not main_content:
            body = soup.find('body')
            if body:
                main_content = body.get_text(separator=' ', strip=True)
            else:
                main_content = soup.get_text(separator=' ', strip=True)
        
        # 연속된 공백 정리
        main_content = re.sub(r'\s+', ' ', main_content).strip()
        
        return main_content
        
    except Exception as e:
        logger.warning(f"[메인 콘텐츠 추출 오류] 전체 텍스트 사용: {e}")
        # 오류 시 전체 텍스트 사용
        return extract_text_from_html(html_content)


def extract_text_from_html(html_content):
    """HTML에서 텍스트 추출 (완화된 기준)"""
    soup = BeautifulSoup(html_content, "html.parser")

    # a 태그 텍스트를 보존할 상위 클래스 (화이트리스트)
    preserve_link_text_parent_classes = ["bold"]
    
    # 제외할 부모 태그 (헤더/푸터 등)
    excluded_parent_tags = {"header", "footer"}
    
    # 제외할 클래스는 태그와 무관하게 모두 제거 (먼저 처리)
    excluded_classes = [
        "popup", "modal", "overlay", "terms_ft_pop", "popup_content",
        "PrtcPolicy", "infoPolicy", "sitemap", "layer_popup",
        "menu", "footer", "header", "top_menu_layer", "navigation", "nav-menu", "login_info"
    ]
    for cls in excluded_classes:
        for element in soup.find_all(class_=lambda x: x and (cls in str(x))):
            element.decompose()
    
    # header/footer 태그 제거 (a 태그 처리 전에)
    for tag_name in excluded_parent_tags:
        for element in soup.find_all(tag_name):
            element.decompose()
    
    # a 태그 처리
    for a_tag in soup.find_all("a"):
        should_preserve_text = False
        has_child_tag = a_tag.find(True) is not None
        
        # header/footer 내부에 있는지 확인
        has_excluded_parent_tag = any(
            ancestor.name in excluded_parent_tags 
            for ancestor in a_tag.parents
        )
        
        if has_excluded_parent_tag:
            # header/footer 내부면 무조건 제거
            a_tag.decompose()
            continue
        
        # 화이트리스트 클래스 확인 (a 태그 자체 클래스)
        if preserve_link_text_parent_classes:
            current_classes = a_tag.get("class", [])
            if isinstance(current_classes, str):
                current_classes = [current_classes]
            
            if current_classes and any(
                cls in preserve_link_text_parent_classes 
                for cls in [c.lower() for c in current_classes]
            ):
                should_preserve_text = True
            else:
                # 부모 요소의 클래스 확인
                for ancestor in a_tag.parents:
                    ancestor_classes = ancestor.get("class", [])
                    if isinstance(ancestor_classes, str):
                        ancestor_classes = [ancestor_classes]
                    
                    if ancestor_classes and any(
                        cls in preserve_link_text_parent_classes 
                        for cls in [c.lower() for c in ancestor_classes]
                    ):
                        should_preserve_text = True
                        break
        
        # 보존 조건 확인
        if should_preserve_text and not has_child_tag:
            # 링크 태그만 제거하고 텍스트는 보존
            a_tag.unwrap()
        else:
            # 링크와 텍스트 모두 제거
            a_tag.decompose()

    # 최소한의 태그만 제거 (완화된 기준)
    # "a"는 위에서 이미 처리했으므로 제거 목록에서 제외
    for tag in [
        "script",
        "style",
        "noscript",
        "head",
        "meta",
        # "a",  # 위에서 이미 처리됨
        "iframe",
        "nav",
        "button",
        "select",
        "label",
        "legend",
        "fieldset",
        "option",
        "aside",
        "strong"
    ]:
        for element in soup.find_all(tag):
            element.decompose()

    # 숨겨진 요소들 제거 (display:none, visibility:hidden)
    for element in soup.find_all(style=lambda value: value and (
        'display:none' in value.replace(' ', '') or 
        'display: none' in value or
        'visibility:hidden' in value.replace(' ', '') or
        'visibility: hidden' in value
    )):
        element.decompose()

    # 주석 제거
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    # ✅ 개선: DOM 트리 순서대로 블록 요소 텍스트 추출 (중복 제거)
    def extract_text_blocks(element, block_texts):
        """DOM 트리를 순회하면서 블록 요소의 텍스트만 추출"""
        block_tags = {'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'li', 'blockquote', 'pre', 'td', 'th', 'dt', 'dd'}
        
        if element.name in block_tags:
            # 블록 요소인 경우 직접 텍스트 추출
            text = element.get_text(separator=' ', strip=True)
            if text and len(text) > 1:  # 의미있는 텍스트만 (최소 2글자, 단일 기호 제외)
                block_texts.append(text)
        elif element.name in ('html', 'body', 'div', 'ul', 'ol', 'dl', 
                               'table', 'tbody', 'thead', 'tfoot', 'tr',
                               'section', 'article', 'main', 'figure', 'details'):
            # 컨테이너 요소는 자식 요소들을 재귀적으로 처리
            for child in element.children:
                if hasattr(child, 'name'):  # Tag 객체인 경우
                    extract_text_blocks(child, block_texts)
        
        return block_texts
    
    # 블록 요소들을 DOM 순서대로 추출
    block_texts = []
    for child in soup.children:
        if hasattr(child, 'name'):  # Tag 객체인 경우
            extract_text_blocks(child, block_texts)
    
    # 블록 요소가 없는 경우 전체 텍스트 사용 (폴백)
    if not block_texts:
        all_text = soup.get_text(separator=' ', strip=True)
        # 문장 단위로 분할 (점, 물음표, 느낌표 기준)
        sentences = re.split(r'[.!?。！？]+', all_text)
        block_texts = [s.strip() for s in sentences if s.strip() and len(s.strip()) > 1]
    
    # 중복 제거 및 정리
    unique_blocks = []
    seen_blocks = set()

    for block in block_texts:
        cleaned_block = block.strip()
        # 중복 체크 (부분 포함 관계도 고려)
        is_duplicate = False
        to_remove = set()  # 제거할 항목들을 별도로 수집

        for seen in seen_blocks:
            if cleaned_block in seen or seen in cleaned_block:
                if len(cleaned_block) <= len(seen):
                    is_duplicate = True
                    break
                else:
                    # 더 긴 텍스트로 교체할 항목을 수집
                    to_remove.add(seen)

        # 수집된 항목들을 제거
        for seen in to_remove:
            seen_blocks.discard(seen)
            # unique_blocks에서도 해당 항목 제거
            unique_blocks = [b for b in unique_blocks if b.strip() != seen]

        if not is_duplicate and len(cleaned_block) > 5:
            unique_blocks.append(cleaned_block)
            seen_blocks.add(cleaned_block)

    
    result = '\n'.join(unique_blocks)
    
    # 최종 정제: 연속된 줄바꿈 정리  
    result = re.sub(r'\n{3,}', '\n\n', result)
    
    return result


def _build_flexible_space_regex(text: str) -> str:
    tokens = [tok for tok in re.split(r"\s+", str(text or "").strip()) if tok]
    if not tokens:
        return ""
    return r"\s*".join(re.escape(tok) for tok in tokens)


def _normalize_text_line(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _extract_preferred_body_text(soup: BeautifulSoup, html_content: str, url: str = "") -> str:
    try:
        from backend.board.jongno_board import extract_jongno_minwon_form

        jongno_post = extract_jongno_minwon_form(soup, url or "")
        if jongno_post and jongno_post.get("content_text"):
            return str(jongno_post.get("content_text") or "")
    except Exception:
        pass

    try:
        from backend.board.pyeongtaek_board import is_pyeongtaek_city_url, strip_pyeongtaek_noise

        if is_pyeongtaek_city_url(url or ""):
            strip_pyeongtaek_noise(soup, url=url or "")
    except Exception:
        pass

    is_asimc_page = False
    try:
        from backend.board.asimc_board import (
            asimc_content_selector_hint,
            extract_asimc_image_lines,
            is_asimc_url,
            strip_asimc_noise,
        )

        if is_asimc_url(url or ""):
            is_asimc_page = True
            strip_asimc_noise(soup, url=url or "")
            hint = asimc_content_selector_hint(url or "")
            hinted_selectors = [selector.strip() for selector in hint.split(",") if selector.strip()]
        else:
            hinted_selectors = []
    except Exception:
        extract_asimc_image_lines = None  # type: ignore[assignment]
        hinted_selectors = []

    content_selectors = hinted_selectors + [
        ".board-view-wrap .board-view-cont .board-view-contents",
        ".board-view-cont .board-view-contents",
        ".board-view-contents",
        ".view_cont",
        ".view-cont",
        ".view_content",
        ".board_view.table_scroll",
        "table.view_type03",
        ".board_view",
        ".board_view_content",
        ".board_view_area",
        ".p-table__content",
        ".write_contents",
        ".kboard-content",
        "#txt",
        "#mainCont",
    ]

    for selector in content_selectors:
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        if not node:
            continue
        text = extract_text_from_html(str(node))
        if text and len(_normalize_text_line(text)) >= 20:
            return text
        if is_asimc_page and selector in hinted_selectors:
            try:
                if node.select_one("img"):
                    if extract_asimc_image_lines:
                        return "\n".join(extract_asimc_image_lines(node, url=url or "")).strip()
                    return ""
            except Exception:
                pass

    return extract_text_from_html(html_content)


def _strip_leading_breadcrumb_and_title(text: str, title: str) -> str:
    raw_text = str(text or "").strip()
    if not raw_text:
        return ""
    title_norm = _normalize_text_line(title)
    markers = (
        "게시글 상세 보기",
        "게시물 상세 보기",
        "상세 보기",
        "상세보기",
    )

    lines = [_normalize_text_line(line) for line in re.split(r"[\r\n]+", raw_text)]
    lines = [line for line in lines if line]
    if not lines:
        return ""

    cleaned_lines = []
    started = False

    for line in lines:
        current = line

        if not started:
            if ">" in current[:320] and any(marker in current for marker in markers):
                if title_norm and title_norm in current:
                    current = current.split(title_norm, 1)[1].lstrip(" >|-:")
                    if current:
                        cleaned_lines.append(current)
                        started = True
                continue

            if title_norm and current == title_norm:
                continue

            if title_norm and current.startswith(title_norm):
                current = current[len(title_norm):].lstrip(" >|-:")
                if not current:
                    continue

            started = True

        cleaned_lines.append(current)

    if not cleaned_lines:
        normalized = _normalize_text_line(raw_text)
        if title_norm and normalized.startswith(title_norm):
            normalized = normalized[len(title_norm):].lstrip(" >|-:")
        return normalized.strip()

    return "\n".join(cleaned_lines).strip()


def _strip_common_content_footer_noise(text: str) -> str:
    raw_text = str(text or "").strip()
    if not raw_text:
        return ""

    footer_patterns = (
        r"밀양시홈페이지이\(가\)\s*창작한.+?공공누리.+",
        r".*저작물은\s*공공누리.+",
        r"^담당자\s*:\s*.+전화\s*:\s*.+$",
        r"^수정일\s*:\s*\d{4}[./-]\d{1,2}[./-]\d{1,2}\.?\s*$",
        r"^최종수정일\s*:\s*\d{4}[./-]\d{1,2}[./-]\d{1,2}\.?\s*$",
    )

    cleaned_lines: list[str] = []
    for index, line in enumerate(raw_text.splitlines()):
        current = _normalize_text_line(line)
        if not current:
            if cleaned_lines:
                cleaned_lines.append("")
            continue
        if index == 0 and current == "스킵네비게이션":
            continue
        if any(re.search(pattern, current, flags=re.IGNORECASE) for pattern in footer_patterns):
            break
        cleaned_lines.append(current)

    while cleaned_lines and cleaned_lines[-1] == "":
        cleaned_lines.pop()
    return "\n".join(cleaned_lines).strip()


def _strip_jongno_council_header_noise(text: str, url: str) -> str:
    raw_text = str(text or "").strip()
    url_low = str(url or "").lower()
    if not raw_text or "council.jongno.go.kr" not in url_low:
        return raw_text

    lines = [_normalize_text_line(line) for line in raw_text.splitlines()]
    while lines and not lines[0]:
        lines.pop(0)

    noise_prefixes = (
        "최상단 메뉴",
        "종로구청 통합 로그인 페이지로 연결.",
        "종로구청 통합 회원가입 페이지로 연결.",
    )

    changed = True
    while lines and changed:
        changed = False
        current = lines[0]
        if any(current.startswith(prefix) for prefix in noise_prefixes):
            lines.pop(0)
            changed = True

    while lines and not lines[0]:
        lines.pop(0)

    return "\n".join(lines).strip()


def clean_snippet_text(text: str, max_length: int = 200) -> str:
    """스니펫 텍스트를 정제합니다."""
    if not text:
        return ""

    # 불필요한 문구들 제거
    unwanted_patterns = _SNIPPET_UNWANTED_PATTERNS

    # 줄바꿈과 탭을 공백으로 변환
    cleaned_text = text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')

    # 연속된 공백을 하나로 통합
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text)

    # 불필요한 문구 제거 (단어 경계 고려)
    for pattern in unwanted_patterns:
        cleaned_text = re.sub(rf'\b{re.escape(pattern)}\b', '', cleaned_text, flags=re.IGNORECASE)

    # 특수문자로 시작하는 부분 제거
    cleaned_text = re.sub(r'^[^\w가-힣]*', '', cleaned_text)

    # 다시 연속된 공백 정리
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    # 의미있는 텍스트가 시작되는 지점 찾기
    words = cleaned_text.split()
    meaningful_words = []

    for word in words:
        # 3글자 이상이고 한글이나 영문이 포함된 단어부터 시작
        if len(word) >= 2 and re.search(r'[가-힣a-zA-Z]', word):
            meaningful_words = words[words.index(word):]
            break

    if meaningful_words:
        result_text = ' '.join(meaningful_words)
    else:
        result_text = cleaned_text

    # 최대 길이로 자르기
    if len(result_text) > max_length:
        result_text = result_text[:max_length].rsplit(' ', 1)[0] + '...'

    return result_text.strip()


def extract_page_snippet(soup: BeautifulSoup, main_content_text: str, length: int = 200) -> str:
    """HTML soup과 본문 텍스트에서 스니펫을 추출합니다.
    먼저 'div.snippet'을 찾고, 없으면 본문 텍스트의 앞부분을 사용합니다.
    """
    snippet = ""
    try:
        snippet_div = soup.find("div", class_="snippet")
        if snippet_div:
            snippet = snippet_div.get_text(separator=" ", strip=True)
            if snippet:
                logger.debug(f"Found snippet in div.snippet: {snippet[:length//2]}...")
                # 스니펫 정제
                snippet = clean_snippet_text(snippet, length)
                return snippet
    except Exception as e:
        logger.warning(f"Error finding div.snippet: {e}")

    if not snippet and main_content_text:  # snippet_div가 없거나 내용이 없는 경우
        # 스니펫 정제 후 길이 제한
        snippet = clean_snippet_text(main_content_text, length)
        logger.debug(f"Generated snippet from main content: {snippet[:length//2]}...")
    return snippet


def extract_favicon_url(soup: BeautifulSoup, base_url: str) -> str:
    """HTML soup에서 파비콘 URL을 추출합니다."""
    favicon_url = ""

    try:
        # 1. link 태그에서 icon 관련 rel 속성 찾기
        favicon_selectors = [
            'link[rel="icon"]',
            'link[rel="shortcut icon"]',
            'link[rel="apple-touch-icon"]',
            'link[rel="apple-touch-icon-precomposed"]',
            'link[rel*="icon"]'
        ]

        for selector in favicon_selectors:
            favicon_link = soup.select_one(selector)
            if favicon_link and favicon_link.get('href'):
                favicon_url = favicon_link['href']
                break

        # 2. 파비콘을 찾지 못한 경우 기본 경로 시도
        if not favicon_url:
            parsed_url = urlparse(base_url)
            default_favicon = f"{parsed_url.scheme}://{parsed_url.netloc}/favicon.ico"
            favicon_url = default_favicon
            logger.debug(f"Using default favicon path: {favicon_url}")
        else:
            # 상대 경로인 경우 절대 경로로 변환
            if not favicon_url.startswith(('http://', 'https://')):
                favicon_url = urljoin(base_url, favicon_url)
            logger.debug(f"Found favicon in HTML: {favicon_url}")

    except Exception as e:
        logger.warning(f"Error extracting favicon URL: {e}")
        # 오류 발생 시 기본 파비콘 경로 사용
        parsed_url = urlparse(base_url)
        favicon_url = f"{parsed_url.scheme}://{parsed_url.netloc}/favicon.ico"

    return favicon_url


def get_random_user_agent():
    """정부 사이트 및 일반 사이트 호환성을 위한 다양한 User-Agent 목록"""
    return random.choice(_USER_AGENTS)


def extract_subject_for_crawling_mode(soup: BeautifulSoup, url: str, block_tag: Optional[str] = None) -> str:
    """크롤링 모드 전용 3단계 게시물 제목(subject) 추출 함수
    1단계: DB에서 조회한 block 태그로 추출 (block_tag가 있는 경우)
    2단계: h1~h8 태그에서 추출
    3단계: 대안 방법으로 추출 (og:title, article title, meta description 등)
    4단계: title 태그에서 추출
    
    Args:
        soup: BeautifulSoup 객체
        url: 크롤링 대상 URL
        block_tag: DB에서 조회한 block 태그 (CSS 선택자 또는 태그명)
    """
    logger.debug(f"[크롤링 게시물제목추출 시작] URL: {url}, block_tag: {block_tag}")
    museum_title = _extract_museum_seoul_priority_title_for_html(soup, url)
    if museum_title:
        logger.info(f"[서울역사박물관 제목 우선] title={museum_title[:72]!r}")
        return museum_title
    caption_title = _extract_caption_article_title(soup)
    if caption_title:
        logger.info(f"[caption 제목 우선] title={caption_title[:72]!r}")
        return caption_title
    
    try:
        # 강남구 계열(보건소·본청·의료관광): BoardContentWorkflow / extract_board_post 와 동일 제목.
        # url_edu·dynamic_link_crawler 는 DB block_tag 를 최우선하는데, 오설정 시 '화면크기' 등 UI만 잡히므로
        # gangnam_board 를 block_tag 보다 먼저 적용한다.
        try:
            from backend.board.gangnam_board import extract_gangnam_board_title, is_gangnam_family_url

            if is_gangnam_family_url(url or ""):
                gn = extract_gangnam_board_title(soup, url=url or "")
                if gn and gn.strip() and gn.strip() != "제목 없음":
                    logger.info(f"[강남구 계열 제목 우선] title={gn[:72]!r}")
                    return gn.strip()
        except Exception:
            pass

        # 평택시청: blind 헤더 대신 실제 게시글 카드 내부 제목을 우선 사용.
        try:
            from backend.board.pyeongtaek_board import extract_pyeongtaek_title, is_pyeongtaek_city_url

            if is_pyeongtaek_city_url(url or ""):
                pt = extract_pyeongtaek_title(soup, url=url or "")
                if pt and pt.strip() and pt.strip() != "제목 없음":
                    logger.info(f"[평택시청 제목 우선] title={pt[:72]!r}")
                    return pt.strip()
        except Exception:
            pass

        try:
            from backend.board.songpa_board import extract_songpa_title, is_songpa_main_office_url

            if is_songpa_main_office_url(url or ""):
                sp = extract_songpa_title(soup, url=url or "")
                if sp and sp.strip() and sp.strip() != "제목 없음":
                    logger.info(f"[송파구청 제목 우선] title={sp[:72]!r}")
                    return sp.strip()
        except Exception:
            pass

        try:
            from backend.board.seongbuk_board import extract_seongbuk_title, is_seongbuk_bbs_view_url

            if is_seongbuk_bbs_view_url(url or ""):
                sb = extract_seongbuk_title(soup, url=url or "")
                if sb and sb.strip() and sb.strip() != "제목 없음":
                    logger.info(f"[성북구청 제목 우선] title={sb[:72]!r}")
                    return sb.strip()
        except Exception:
            pass

        try:
            from backend.board.asimc_board import extract_asimc_title, is_asimc_url

            if is_asimc_url(url or ""):
                asimc_title = extract_asimc_title(soup, url=url or "")
                if asimc_title and asimc_title.strip() and asimc_title.strip() != "제목 없음":
                    logger.info(f"[ASIMC 제목 우선] title={asimc_title[:72]!r}")
                    return asimc_title.strip()
        except Exception:
            pass

        try:
            from backend.board.jongno_board import (
                extract_jongno_apply_title,
                extract_jongno_board_title,
                extract_jongno_construction_status,
                extract_jongno_council_post,
            )

            jongno_title = (
                extract_jongno_apply_title(soup, url or "")
                or extract_jongno_board_title(soup, url or "")
            )
            if jongno_title and jongno_title != "제목 없음":
                logger.info(f"[종로 제목 우선] title={jongno_title[:72]!r}")
                return jongno_title
            jongno_council_post = extract_jongno_council_post(soup, url or "")
            if jongno_council_post and str(jongno_council_post.get("title") or "").strip():
                jongno_council_title = str(jongno_council_post.get("title") or "").strip()
                logger.info(f"[종로구의회 제목 우선] title={jongno_council_title[:72]!r}")
                return jongno_council_title
            jongno_construction_post = extract_jongno_construction_status(soup, url or "")
            if jongno_construction_post and str(jongno_construction_post.get("title") or "").strip():
                jongno_construction_title = str(jongno_construction_post.get("title") or "").strip()
                logger.info(f"[종로 공사장 제목 우선] title={jongno_construction_title[:72]!r}")
                return jongno_construction_title
        except Exception:
            pass

        try:
            url_low = (url or "").lower()
            if "miryang.go.kr" in url_low and "selectnoticedetail.do" in url_low:
                for selector in (
                    "#board-wrap .inboxRead .headinfo > h3.BoR-h2",
                    "#board-wrap .inboxRead .headinfo > h2.BoR-h2",
                    ".BoardRead.board-list-wrap .inboxRead .headinfo > h3.BoR-h2",
                    ".BoardRead.board-list-wrap .inboxRead .headinfo > h2.BoR-h2",
                    ".inboxRead .headinfo > h3.BoR-h2",
                    ".inboxRead .headinfo > h2.BoR-h2",
                    ".headinfo > h3.BoR-h2",
                    ".headinfo > h2.BoR-h2",
                    "h3.BoR-h2",
                    "h2.BoR-h2",
                ):
                    element = soup.select_one(selector)
                    if not element:
                        continue
                    miryang_title = _extract_clean_title_text(element)
                    if miryang_title and miryang_title != "제목 없음":
                        logger.info(f"[밀양 제목 우선] title={miryang_title[:72]!r}")
                        return miryang_title
        except Exception:
            pass

        try:
            url_low = (url or "").lower()
            if "miryang.go.kr" in url_low and (
                "selectnoticedetail.do" in url_low
                or "selectboarddetail.do" in url_low
                or "selectminutesdetail.do" in url_low
                or "eminwonview.do" in url_low
                or "emiryangminwonview.do" in url_low
                or "egovtourdetail.do" in url_low
            ):
                for selector in (
                    "#board-wrap .inboxRead .headinfo > h3.BoR-h2",
                    "#board-wrap .inboxRead .headinfo > h2.BoR-h2",
                    ".BoardRead.board-list-wrap .inboxRead .headinfo > h3.BoR-h2",
                    ".BoardRead.board-list-wrap .inboxRead .headinfo > h2.BoR-h2",
                    ".inboxRead .headinfo > h3.BoR-h2",
                    ".inboxRead .headinfo > h2.BoR-h2",
                    ".headinfo > h3.BoR-h2",
                    ".headinfo > h2.BoR-h2",
                    "h3.BoR-h2",
                    "h2.BoR-h2",
                    "#contents .cont-top h2.tit",
                    "#contents .cont-top .tit",
                ):
                    element = soup.select_one(selector)
                    if not element:
                        continue
                    miryang_title = _extract_clean_title_text(element)
                    if miryang_title and miryang_title != "제목 없음":
                        logger.info(f"[밀양 제목 우선] title={miryang_title[:72]!r}")
                        return miryang_title
        except Exception:
            pass

        try:
            url_low = (url or "").lower()
            if "anseong.go.kr" in url_low and "eduinsttview.do" in url_low:
                for selector in (
                    "div.edu-detail-header h2.edu-title",
                    ".edu-detail-header h2.edu-title",
                    "h2.edu-title",
                ):
                    element = soup.select_one(selector)
                    if not element:
                        continue
                    anseong_title = _extract_clean_title_text(element)
                    if anseong_title and anseong_title != "제목 없음":
                        logger.info(f"[안성 제목 우선] title={anseong_title[:72]!r}")
                        return anseong_title
        except Exception:
            pass

        # 성동구청 selectBbsNtt* 계열: 공용 nav 제외 규칙에 box_title h0 가 걸릴 수 있어
        # 게시물 카드 내부 제목을 URL 패턴 기준으로 먼저 확정한다.
        try:
            url_low = (url or "").lower()
            if "guro.go.kr" in url_low and "/yeyak/edclctreview.do" in url_low:
                for selector in (
                    "tbody tr:first-child td:first-child",
                    "table tbody tr:first-child td:first-child",
                    "tbody tr:first-child td",
                    "table tbody tr:first-child td",
                ):
                    element = soup.select_one(selector)
                    if not element:
                        continue
                    guro_title = _extract_clean_title_text(element)
                    if not guro_title or guro_title == "제목 없음":
                        continue
                    guro_title = re.sub(r"\s*신청자\s*:\s*\d+\s*/\s*\d+\s*명.*$", "", guro_title, flags=re.IGNORECASE)
                    guro_title = re.sub(r"\s*대기자\s*:\s*\d+\s*/\s*\d+\s*명.*$", "", guro_title, flags=re.IGNORECASE)
                    guro_title = guro_title.strip(" |:-")
                    if guro_title and guro_title not in {"< 참고사항 >", "참고사항"}:
                        logger.info(f"[구로 제목 우선] title={guro_title[:72]!r}")
                        return guro_title
        except Exception:
            pass

        try:
            url_low = (url or "").lower()
            if "gangdong.go.kr" in url_low and "/web/newportal/bbs/" in url_low:
                try:
                    for th in soup.select("div.table01.table02.table_view table th, div.table_view table th, table th"):
                        label = re.sub(r"\s+", "", th.get_text(" ", strip=True) or "")
                        if label not in ("사업명", "제목", "행사제목"):
                            continue
                        td = th.find_next_sibling("td")
                        if not td:
                            td = th.find_parent("tr").find("td") if th.find_parent("tr") else None
                        if not td:
                            continue
                        gangdong_title = _extract_clean_title_text(td)
                        if gangdong_title and gangdong_title != "제목 없음":
                            logger.info(f"[강동구 게시판 표 제목 우선] title={gangdong_title[:72]!r}")
                            return gangdong_title
                except Exception:
                    pass

            if "gangdong.go.kr" in url_low and "/web/newreserve/reserve/view" in url_low:
                try:
                    for th in soup.select("#con table th, table th"):
                        label = re.sub(r"\s+", "", th.get_text(" ", strip=True) or "")
                        if label not in ("행사제목", "제목"):
                            continue
                        td = th.find_next_sibling("td")
                        if not td:
                            continue
                        gangdong_title = _extract_clean_title_text(td)
                        if gangdong_title and gangdong_title != "제목 없음":
                            logger.info(f"[강동구 예약 제목 우선] title={gangdong_title[:72]!r}")
                            return gangdong_title
                except Exception:
                    pass

        except Exception:
            pass

        try:
            url_low = (url or "").lower()
            if "sd.go.kr" in url_low and "newselectcontractwebview.do" in url_low:
                try:
                    from backend.board.sungdong_board import extract_sungdong_contract_title

                    sd_contract_title = extract_sungdong_contract_title(soup, url=url or "")
                except Exception:
                    sd_contract_title = ""
                if sd_contract_title and sd_contract_title != "제목 없음":
                    logger.info(f"[성동구청 계약 제목 우선] title={sd_contract_title[:72]!r}")
                    return sd_contract_title
            if "sd.go.kr" in url_low and (
                "selectbbsnttlist.do" in url_low or "selectbbsnttview.do" in url_low
            ):
                for sel in (
                    "#contents #board .box.type2 h3.box_title.h0",
                    "#contents h3.box_title.h0",
                    "article h3.box_title.h0",
                    "h3.box_title.h0",
                ):
                    el = soup.select_one(sel)
                    if not el:
                        continue
                    sd_title = _extract_clean_title_text(el)
                    if sd_title and sd_title != "제목 없음":
                        logger.info(f"[성동구청 제목 우선] title={sd_title[:72]!r}")
                        return sd_title
        except Exception:
            pass

        # 설정 상수 사용
        subject_class_keywords = _SUBJECT_CLASS_KEYWORDS
        subject_id_keywords = _SUBJECT_ID_KEYWORDS
        compound_title_containers = _COMPOUND_TITLE_CONTAINERS
        nav_exclude_config = _NAV_EXCLUDE_CONFIG
        
        # 네비게이션 제외 판단 함수 (클로저)
        url_lower = (url or "").lower()
        is_seocho = "seocho.go.kr" in url_lower

        def is_nav_excluded(element):
            if is_seocho and element:
                element_id = (element.get('id', '') or '').lower()
                element_classes = element.get('class', [])
                if isinstance(element_classes, str):
                    element_classes = [element_classes]
                element_classes_lower = [cls.lower() for cls in element_classes]
                if element_id == "snav" or "snav" in element_classes_lower:
                    return False
                if element_id == "content-area" or "content-area" in element_classes_lower:
                    return False
            return _is_nav_excluded(element, nav_exclude_config)

        if "gokams.or.kr" in url_lower:
            for selector in (
                ".bbs-view .bbs-header strong.tit",
                ".bbs-view .bbs-header .tit",
                ".bbs-header strong.tit",
                ".bbs-header .tit",
                ".title .lf strong",
                ".title strong",
            ):
                try:
                    element = soup.select_one(selector)
                except Exception:
                    element = None
                if not element:
                    continue
                text = _get_title_text_allowing_single_link(element)
                if text and len(text) > 5:
                    return text
        
        # ✅ 0단계: DB에서 조회한 block 태그로 제목 추출 (최우선)
        if block_tag and block_tag.strip():
            block_tag = block_tag.strip()
            logger.info(f"[DB block 태그 추출 시도] block_tag: {block_tag}")
            try:
                # CSS 선택자 형식인 경우 (예: "h1.title", "#content h2", ".article-title")
                if any(char in block_tag for char in [' ', '.', '#', '>', '+', '~']):
                    elements = soup.select(block_tag)
                    if elements:
                        for element in elements:
                            text = _extract_clean_title_text(element)
                            if text and len(text) > 5:  # 최소 5자 이상
                                logger.info(f"[DB block 태그 추출 성공] block_tag={block_tag}, 제목: {text[:50]}...")
                                return text
                # 단순 태그명인 경우 (예: "h1", "h2", "title")
                else:
                    element = soup.find(block_tag)
                    if element:
                        text = _extract_clean_title_text(element)
                        if text and len(text) > 5:  # 최소 5자 이상
                            logger.info(f"[DB block 태그 추출 성공] block_tag={block_tag}, 제목: {text[:50]}...")
                            return text
                    # 태그명이 아니고 클래스나 ID일 수도 있음
                    # 클래스 시도 (예: ".title")
                    if block_tag.startswith('.'):
                        class_name = block_tag[1:]
                        element = soup.find(class_=class_name)
                        if element:
                            text = _extract_clean_title_text(element)
                            if text and len(text) > 5:
                                logger.info(f"[DB block 태그 추출 성공] class={class_name}, 제목: {text[:50]}...")
                                return text
                    # ID 시도 (예: "#title")
                    elif block_tag.startswith('#'):
                        id_name = block_tag[1:]
                        element = soup.find(id=id_name)
                        if element:
                            text = _extract_clean_title_text(element)
                            if text and len(text) > 5:
                                logger.info(f"[DB block 태그 추출 성공] id={id_name}, 제목: {text[:50]}...")
                                return text
                    # 클래스명으로 직접 시도 (점 없이)
                    element = soup.find(class_=block_tag)
                    if element:
                        text = _extract_clean_title_text(element)
                        if text and len(text) > 5:
                            logger.info(f"[DB block 태그 추출 성공] class={block_tag}, 제목: {text[:50]}...")
                            return text
                
                logger.info(f"[DB block 태그 추출 실패] block_tag={block_tag}로 제목을 찾지 못함, 기존 로직으로 폴백")
            except Exception as e:
                logger.warning(f"[DB block 태그 추출 오류] block_tag={block_tag}, 오류: {e}, 기존 로직으로 폴백")
        
        # 0단계-특이케이스: 서초구청(content-area)에서 제목 우선 추출
        if is_seocho:
            seocho_container = soup.find(id="content-area") or soup.find(class_="content-area")
            if seocho_container:
                for i in range(1, 7):
                    h_tag = seocho_container.find(f"h{i}")
                    if h_tag:
                        raw_text = _extract_clean_title_text(h_tag)
                        if raw_text:
                            logger.info(f"[서초구청 제목추출] content-area h{i}: {raw_text[:50]}...")
                            return raw_text

        # 0단계: 복합 제목 추출 (sub-search 등 특수 컨테이너에서 제목 조합)
        for container_class in compound_title_containers:
            containers = soup.find_all(class_=container_class)
            for container in containers:
                if container:
                    if is_nav_excluded(container):
                        continue
                    # header, footer, 네비게이션 요소 제외
                    if any(parent.name in ['header', 'footer'] for parent in container.parents):
                        continue
                    if any(is_nav_excluded(parent) for parent in container.parents):
                        continue
                        
                    compound_title = _extract_compound_title(container, _COMPOUND_TITLE_EXCLUDE_CLASSES)
                    if compound_title:
                        return compound_title
        
        # 1단계: h1~h8 태그에서 제목 추출

        # 1단계-1: 제목 관련 class를 가진 h 태그 우선 검색
        for selector in (
            ".poll_view > h4.no_bgimg.mT0",
            ".poll_view h4.no_bgimg",
            ".poll_view > h4",
            "strong.tit",
            ".tit",
            ".view_top .tit",
            ".board_view .tit",
            ".bod_view h4",
            ".board_view h4",
            ".view_cont h4",
            ".view-cont h4",
            ".board_view_title",
            ".bod_view_title",
            ".view_title",
        ):
            try:
                for element in soup.select(selector):
                    if not element:
                        continue
                    if is_nav_excluded(element):
                        continue
                    if any(parent.name in ['header', 'footer'] for parent in element.parents):
                        continue
                    if any(is_nav_excluded(parent) for parent in element.parents):
                        continue
                    text = _get_title_text_allowing_single_link(element)
                    if text and len(text) > 5:
                        return text
            except Exception:
                continue

        result = _extract_title_fast_path(soup, is_nav_excluded)
        if result:
            return result

        result = _extract_title_from_heading_with_class(
            soup, subject_class_keywords, subject_id_keywords, is_nav_excluded
        )
        if result:
            return result
        
        # 1단계-2: 일반 h 태그 검색
        result = _extract_title_from_heading_general(soup, is_nav_excluded)
        if result:
            return result
        
        # 1단계-3: h 태그 외 다른 태그 검색
        result = _extract_title_from_other_tags(soup, subject_class_keywords, is_nav_excluded)
        if result:
            return result
        
        # 2-3단계: 대안 방법 (article, meta, title 태그)
        result = _extract_title_fallback(soup)
        if result:
            return result
        
        # 모든 단계 실패
        logger.warning(f"[제목추출 실패] 제목을 추출하지 못했습니다: {url}")
        return ""
        
    except Exception as e:
        logger.error(f"[제목추출 오류] URL: {url}, 오류: {e}")
        return ""


def extract_sub_subject_for_crawling_mode(
    soup: BeautifulSoup, 
    url: str
) -> List[str]:
    """크롤링 모드 전용 서브 제목(sub_subject) 추출 함수.
    
    플로우:
    1. target_classes에 선언된 클래스를 가진 요소를 찾음
    2. 해당 요소의 텍스트를 직접 추출 (제외 로직 없이)
    
    Args:
        soup: BeautifulSoup 객체.
        url: 크롤링 대상 URL.
    
    Returns:
        추출된 서브 제목 문자열 리스트. 찾지 못한 경우 빈 문자열('').
    
    Raises:
        Exception: HTML 파싱 중 오류 발생 시.
    """
    logger.debug(f"[크롤링 서브 제목추출 시작] URL: {url}")
    try:
        # 기본 타겟 클래스 (필요시 파라미터로 받을 수 있음)
        target_classes = _DEFAULT_SUB_SUBJECT_CLASSES
        
        logger.debug(f"[설정] target_classes: {target_classes}")
        
        sub_subjects = []
        target_class_sets = {
            target_class: frozenset(target_class.lower().split())
            for target_class in target_classes
        }
        target_lookup = {}
        for target_class, target_class_set in target_class_sets.items():
            target_lookup.setdefault(target_class_set, []).append(target_class)
        matched_elements = {target_class: [] for target_class in target_classes}

        # 모든 태그를 한 번만 순회한 뒤 target class별로 분배한다.
        for element in soup.find_all(True):
            element_classes = element.get('class', [])
            if not element_classes:
                continue
            element_class_set = frozenset(c.lower() for c in element_classes)
            for target_class in target_lookup.get(element_class_set, ()):
                matched_elements[target_class].append(element)

        # target_classes에 선언된 클래스를 가진 요소 처리
        for target_class in target_classes:
            # 클래스를 set으로 변환 (띄어쓰기로 분리)
            target_class_set = target_class_sets[target_class]
            logger.debug(f"[검색] 찾을 class: {target_class_set}")
            
            found_count = 0
            for element in matched_elements[target_class]:
                found_count += 1
                element_classes = element.get('class', [])
                logger.debug(f"  [{found_count}] 일치: <{element.name}> class={element_classes}")

                # 요소의 텍스트를 직접 추출 (30글자까지만)
                full_text = element.get_text(strip=True)
                text = full_text[:30]  # 서브제목은 30글자까지만 저장
                if text and text not in sub_subjects:
                    sub_subjects.append(text)
                    logger.debug(f"       추출: {full_text[:80]} (저장: {text})")
                elif text in sub_subjects:
                    logger.debug(f"       - 중복 제외: {full_text[:60]} (저장: {text})")

            logger.debug(f"[검색 완료] '{target_class}' 일치 요소: {found_count}개")
        
        # 결과 반환
        if sub_subjects:
            logger.debug(f"[서브 제목추출 성공] URL: {url}, 총 {len(sub_subjects)}개 추출")
            return sub_subjects
        else:
            logger.debug(f"[서브 제목추출 실패] URL: {url}, 조건에 맞는 서브 제목 없음")
            return ''
        
    except Exception as e:
        logger.error(f"[서브 제목추출 오류] URL: {url}, 오류: {e}")
        return []


def parse_html_content(html_content: str, url: str) -> Dict[str, str]:
    """HTML 콘텐츠를 파싱하여 구조화된 데이터 반환"""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        try:
            from backend.board.jongno_board import (
                extract_jongno_apply_post,
                extract_jongno_construction_status,
                extract_jongno_council_post,
                extract_jongno_minwon_form,
            )

            jongno_post = (
                extract_jongno_council_post(soup, url or "")
                or extract_jongno_apply_post(soup, url or "")
                or extract_jongno_construction_status(soup, url or "")
                or extract_jongno_minwon_form(soup, url or "")
            )
        except Exception:
            jongno_post = None
        if jongno_post:
            content = str(jongno_post.get("content_text") or "")
            return {
                "title": str(jongno_post.get("title") or ""),
                "content": content,
                "snippet": str(jongno_post.get("snippet") or clean_snippet_text(content)),
                "favicon_url": extract_favicon_url(soup, url),
                "url": url,
            }

        # 제목 추출
        try:
            from backend.board.asimc_board import is_asimc_url, try_extract_asimc_post

            asimc_post = try_extract_asimc_post(soup, url or "") if is_asimc_url(url or "") else None
        except Exception:
            asimc_post = None
        if asimc_post:
            content = str(getattr(asimc_post, "content_text", "") or "")
            return {
                "title": str(getattr(asimc_post, "title", "") or ""),
                "content": content,
                "snippet": str(getattr(asimc_post, "snippet", "") or clean_snippet_text(content)),
                "favicon_url": extract_favicon_url(soup, url),
                "url": url,
            }

        title = _extract_museum_seoul_priority_title_for_html(soup, url) or _extract_caption_article_title(soup)
        if soup.title and soup.title.string:
            title = title or soup.title.string.strip()
        if not title:
            h1_tag = soup.find("h1")
            if h1_tag:
                title = _extract_clean_title_text(h1_tag)

        # 본문 내용 추출
        try:
            from backend.board.asimc_board import is_asimc_url
        except Exception:
            is_asimc_url = None  # type: ignore[assignment]
        if is_asimc_url and is_asimc_url(url or ""):
            main_text = _extract_preferred_body_text(soup, html_content, url=url)
        else:
            main_text = extract_text_from_html(html_content)

        # 스니펫 추출
        page_snippet = extract_page_snippet(soup, main_text)

        # 파비콘 추출
        favicon_url = extract_favicon_url(soup, url)

        return {
            "title": title,
            "content": main_text,
            "snippet": page_snippet,
            "favicon_url": favicon_url,
            "url": url
        }

    except Exception as e:
        logger.error(f"[HTML 파싱 실패] URL: {url}: {e}")
        return None


def _extract_web_title_for_structured_content(soup: BeautifulSoup, url: str) -> str:
    """구조화 결과의 web_title 추출. 사이트별 분기파일 우선순위를 먼저 적용한다."""
    try:
        from backend.board.songpa_board import extract_songpa_web_title, is_songpa_main_office_url

        if is_songpa_main_office_url(url or ""):
            songpa_title = (extract_songpa_web_title(soup, url=url or "") or "").strip()
            if songpa_title:
                return songpa_title
    except Exception:
        pass

    try:
        return soup.title.string.strip() if soup.title and soup.title.string else ""
    except Exception:
        return ""


def parse_html_content_for_crawling_mode(html_content: str, url: str, block_tag: Optional[str] = None) -> Dict[str, str]:
    """크롤링 모드 전용 HTML 콘텐츠 파싱 함수 - 고급 제목 추출 포함
    
    Args:
        html_content: HTML 콘텐츠
        url: 크롤링 대상 URL
        block_tag: DB에서 조회한 block 태그 (CSS 선택자 또는 태그명)
    """
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        try:
            from backend.board.jongno_board import (
                extract_jongno_apply_post,
                extract_jongno_construction_status,
                extract_jongno_council_post,
                extract_jongno_minwon_form,
            )

            jongno_post = (
                extract_jongno_council_post(soup, url or "")
                or extract_jongno_apply_post(soup, url or "")
                or extract_jongno_construction_status(soup, url or "")
                or extract_jongno_minwon_form(soup, url or "")
            )
        except Exception:
            jongno_post = None
        if jongno_post:
            web_title = soup.title.string.strip() if soup.title and soup.title.string else ""
            content = str(jongno_post.get("content_text") or "")
            return {
                "subject": str(jongno_post.get("title") or ""),
                "title": str(jongno_post.get("title") or ""),
                "web_title": web_title,
                "content": content,
                "snippet": str(jongno_post.get("snippet") or clean_snippet_text(content)),
                "favicon_url": extract_favicon_url(soup, url),
                "url": url,
            }

        # 크롤링 모드 전용 3단계 게시물 제목(subject) 추출
        try:
            from backend.board.asimc_board import is_asimc_url, try_extract_asimc_post

            asimc_post = try_extract_asimc_post(soup, url or "") if is_asimc_url(url or "") else None
        except Exception:
            asimc_post = None
        if asimc_post:
            web_title = soup.title.string.strip() if soup.title and soup.title.string else ""
            content = str(getattr(asimc_post, "content_text", "") or "")
            title = str(getattr(asimc_post, "title", "") or "")
            return {
                "subject": title,
                "title": title,
                "web_title": web_title,
                "content": content,
                "snippet": str(getattr(asimc_post, "snippet", "") or clean_snippet_text(content)),
                "favicon_url": extract_favicon_url(soup, url),
                "url": url,
            }

        try:
            from backend.board.seongbuk_board import try_extract_seongbuk_post

            seongbuk_post = try_extract_seongbuk_post(soup, url or "")
        except Exception:
            seongbuk_post = None
        if seongbuk_post:
            web_title = soup.title.string.strip() if soup.title and soup.title.string else ""
            content = str(getattr(seongbuk_post, "content_text", "") or "")
            title = str(getattr(seongbuk_post, "title", "") or "")
            return {
                "subject": title,
                "title": title,
                "web_title": web_title,
                "content": content,
                "snippet": str(getattr(seongbuk_post, "snippet", "") or clean_snippet_text(content)),
                "favicon_url": extract_favicon_url(soup, url),
                "url": url,
            }

        try:
            from backend.board.yongin_board import is_yongin_general_bbs_url, try_extract_yongin_general_post

            yongin_post = try_extract_yongin_general_post(soup, url or "") if is_yongin_general_bbs_url(url or "") else None
        except Exception:
            yongin_post = None
        if yongin_post:
            web_title = soup.title.string.strip() if soup.title and soup.title.string else ""
            content = str(getattr(yongin_post, "content_text", "") or "")
            title = str(getattr(yongin_post, "title", "") or "")
            return {
                "subject": title,
                "title": title,
                "web_title": web_title,
                "content": content,
                "snippet": str(getattr(yongin_post, "snippet", "") or clean_snippet_text(content)),
                "favicon_url": extract_favicon_url(soup, url),
                "url": url,
            }

        main_subject = extract_subject_for_crawling_mode(soup, url, block_tag=block_tag)
        subject = main_subject
        if len(main_subject) < 11:
            sub_subject = extract_sub_subject_for_crawling_mode(soup, url)
            logger.debug(f"추출 된 서브 제목: {sub_subject}")
        else:
            sub_subject = ''

        # sub_subject 처리: 리스트인 경우 문자열로 변환, 'null'이면 빈 문자열로
        if isinstance(sub_subject, list):
            sub_subject = ", ".join(sub_subject)
        elif sub_subject == 'null':
            sub_subject = ''

        # main_subject와 sub_subject 결합
        if not sub_subject:
            subject = main_subject
        else:
            subject = " - ".join([main_subject, sub_subject])

        # 사이트별 분기파일 우선순위를 반영한 웹페이지 제목 추출
        web_title = _extract_web_title_for_structured_content(soup, url)
        try:
            from backend.shared.title_candidate_scoring import extract_title_with_scores

            scored_title = extract_title_with_scores(soup, url=url or "")
            parser_title = re.sub(r"\s+", " ", str(scored_title.get("title") or "")).strip()
            if parser_title and int(scored_title.get("title_score") or 0) >= 60:
                web_title = parser_title
        except Exception:
            pass

        # 본문 내용 추출 (기존 로직과 동일)
        main_text = _extract_preferred_body_text(soup, html_content, url=url)
        main_text = _strip_leading_breadcrumb_and_title(main_text, subject or main_subject)
        main_text = _strip_common_content_footer_noise(main_text)
        main_text = _strip_jongno_council_header_noise(main_text, url)

        # 스니펫 추출 (기존 로직과 동일)
        page_snippet = extract_page_snippet(soup, main_text)

        # 파비콘 추출 (기존 로직과 동일)
        favicon_url = extract_favicon_url(soup, url)

        return {
            "subject": subject,
            "title": subject,  # 크롤링 모드에서 통일된 제목 필드로도 제공
            "web_title": web_title,  # 실제 페이지 title 태그
            "content": main_text,
            "snippet": page_snippet,
            "favicon_url": favicon_url,
            "url": url
        }

    except Exception as e:
        logger.error(f"[크롤링모드 HTML 파싱 실패] URL: {url}: {e}")
        return None


def build_structured_content(soup: BeautifulSoup, current_url: str, html_content: str, block_tag: Optional[str] = None) -> Dict[str, str]:
    """페이지에서 제목/본문/스니펫/파비콘을 구조화하여 반환
    
    Args:
        soup: BeautifulSoup 객체
        current_url: 크롤링 대상 URL
        html_content: HTML 콘텐츠
        block_tag: DB에서 조회한 block 태그 (CSS 선택자 또는 태그명)
    """
    try:
        try:
            from backend.board.jongno_board import (
                extract_jongno_apply_post,
                extract_jongno_construction_status,
                extract_jongno_council_post,
                extract_jongno_minwon_form,
            )

            jongno_post = (
                extract_jongno_council_post(soup, current_url or "")
                or extract_jongno_apply_post(soup, current_url or "")
                or extract_jongno_construction_status(soup, current_url or "")
                or extract_jongno_minwon_form(soup, current_url or "")
            )
        except Exception:
            jongno_post = None
        if jongno_post:
            web_title = soup.title.string.strip() if soup.title and soup.title.string else ""
            content = str(jongno_post.get("content_text") or "")
            return {
                "title": str(jongno_post.get("title") or ""),
                "web_title": web_title,
                "content": content,
                "snippet": str(jongno_post.get("snippet") or clean_snippet_text(content)),
                "favicon_url": extract_favicon_url(soup, current_url),
            }
        # 크롤링 모드 전용 3단계 게시물 제목 추출
        try:
            from backend.board.seongbuk_board import try_extract_seongbuk_post

            seongbuk_post = try_extract_seongbuk_post(soup, current_url or "")
        except Exception:
            seongbuk_post = None
        if seongbuk_post:
            web_title = soup.title.string.strip() if soup.title and soup.title.string else ""
            content = str(getattr(seongbuk_post, "content_text", "") or "")
            return {
                "title": str(getattr(seongbuk_post, "title", "") or ""),
                "web_title": web_title,
                "content": content,
                "snippet": str(getattr(seongbuk_post, "snippet", "") or clean_snippet_text(content)),
                "favicon_url": extract_favicon_url(soup, current_url),
            }

        main_subject = extract_subject_for_crawling_mode(soup, current_url, block_tag=block_tag)

        if len(main_subject) < 11:
            sub_subject = extract_sub_subject_for_crawling_mode(soup, current_url)
        else:
            sub_subject = ''

        # sub_subject 처리: 리스트인 경우 문자열로 변환
        if isinstance(sub_subject, list) and len(sub_subject) > 0:
            sub_subject = ", ".join(sub_subject)
        elif not sub_subject or sub_subject == 'null':
            sub_subject = ''
        
        # main_subject와 sub_subject 결합
        if not sub_subject:
            title = main_subject
        else:
            title = " - ".join([main_subject, sub_subject])
        
        # 사이트별 분기파일 우선순위를 반영한 웹페이지 제목 추출
        web_title = _extract_web_title_for_structured_content(soup, current_url)
        try:
            from backend.shared.title_candidate_scoring import extract_title_with_scores

            scored_title = extract_title_with_scores(soup, url=current_url or "")
            parser_title = re.sub(r"\s+", " ", str(scored_title.get("title") or "")).strip()
            if parser_title and int(scored_title.get("title_score") or 0) >= 60:
                web_title = parser_title
        except Exception:
            pass

        # 본문: extract_text_from_html() 함수 사용 (링크 제거 포함)
        content = _extract_preferred_body_text(soup, html_content, url=current_url)
        content = _strip_leading_breadcrumb_and_title(content, title)
        content = _strip_common_content_footer_noise(content)
        content = _strip_jongno_council_header_noise(content, current_url)

        # 스니펫/파비콘
        snippet = extract_page_snippet(soup, content)
        favicon_url = extract_favicon_url(soup, current_url)

        return {
            "title": title or "",
            "web_title": web_title or "",
            "content": content or "",
            "snippet": snippet or "",
            "favicon_url": favicon_url or "",
        }
        
    except Exception as e:
        logger.error(f"[콘텐츠 구조화 오류] {current_url}: {e}")
        return {"title": "", "content": "", "snippet": "", "favicon_url": ""}


async def extract_with_http(url: str, collect_metadata: bool = True, crawl_mode: str = None, block_tag: Optional[str] = None) -> Dict[str, str]:
    # 1줄 설명: 안전한 URL 인코딩 및 공공기관의 비표준 파일 타입 대응 로직을 포함하여 콘텐츠를 추출함
    
    # 1. URL 정규화 및 안전한 인코딩 적용 (한글/공백 대응)
    safe_url = get_safe_url(url) # 1줄 설명: 주소 내 한글이나 특수문자를 인코딩하여 서버 인식 오류를 방지함
    
    url_lower = safe_url.lower()
    if any(url_lower.endswith(ext) for ext in ['.xml', '.json', '.rss', '.atom']):
        logger.info(f"[HTTP 추출 제외] XML/JSON 확장자 스킵: {url}")
        return None
    
    # 2. Referer를 포함한 브라우저 모방 헤더 설정
    headers = {
        "User-Agent": get_random_user_agent(),
        "Referer": url, # 1줄 설명: 접근 차단을 피하기 위해 현재 페이지 주소를 레퍼러로 주입함
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
    }

    timeout_settings = aiohttp.ClientTimeout(total=30, connect=8, sock_read=20)

    async with aiohttp.ClientSession(timeout=timeout_settings, headers=headers) as session:
        async with session.get(safe_url) as response: # 1줄 설명: 인코딩 처리된 안전한 URL로 요청을 보냄
            if response.status == 200:
                # 3. 비표준 Content-Type(file 등)을 URL 기반으로 교정
                raw_ctype = response.headers.get('Content-Type', '').lower()
                content_type = infer_content_type(safe_url, raw_ctype) # 1줄 설명: 서버의 잘못된 헤더 응답을 실제 파일 확장자로 추론함
                
                # 4. 파일 다운로드 응답인 경우 (HTML이 아닌 경우) 처리
                if not any(t in content_type for t in ['html', 'xml', 'json']):
                    logger.info(f"[파일 응답 감지] 타입: {content_type}, URL: {url}")
                    # 1줄 설명: 텍스트 추출이 불가능한 파일 형식인 경우 관련 정보를 반환함
                    return {"url": url, "content_type": content_type, "is_file": True}

                if any(excluded_type in content_type for excluded_type in ['application/json', 'application/xml', 'text/xml', 'application/rss+xml', 'application/atom+xml']):
                    logger.info(f"[크롤링 제외] XML/JSON 페이지 스킵: {url}, Content-Type: {content_type}")
                    return None
                
                try:
                    html_content = await response.text()
                except UnicodeDecodeError:
                    html_content = await response.text(errors='ignore')

                # 5. 크롤링 모드에 따른 HTML 파싱 함수 선택
                if crawl_mode == "crawling":
                    result = parse_html_content_for_crawling_mode(html_content, url, block_tag=block_tag)
                    logger.info(f"[크롤링모드 HTTP 파싱] URL: {url}, block_tag: {block_tag}")
                else:
                    result = parse_html_content(html_content, url)
                
                # 6. 에러 페이지 검증 (기존 로직 유지)
                if result:
                    content = result.get('content', '')
                    title = result.get('title', '') or result.get('subject', '') or result.get('web_title', '')
                    
                    error_keywords = ['페이지를 찾을 수 없습니다', '페이지 오류', '접속 권한 없음', '삭제된 게시물', '404 not found'] # 중략
                    
                    content_lower = content.lower()
                    title_lower = title.lower()
                    
                    if any(keyword in content_lower or keyword in title_lower for keyword in error_keywords):
                        logger.warning(f"[HTTP 크롤링 제외] 에러 페이지 감지: {url}")
                        return None
                    
                    # 7. 메타데이터 및 해시값 수집
                    if collect_metadata:
                        main_content = re.sub(r'\s+', ' ', result.get('content', '')).strip()
                        metadata = {
                            'content_length': response.headers.get('Content-Length'),
                            'content_hash': sha256_hex_utf8(main_content)
                        }
                        result['_metadata'] = metadata
                        logger.info(f"[HTTP 메타데이터 수집] URL: {url}")
                
                return result
            else:
                logger.warning(f"[HTTP 요청 실패] 상태코드: {response.status}, URL: {url}")
                return None

async def extract_with_playwright(
    url: str,
    collect_metadata: bool = True,
    crawl_mode: str = None,
    stop_signal: Optional[CrawlStopSignal] = None,
    block_tag: Optional[str] = None
) -> Dict[str, str]:
    """Playwright 방식으로 콘텐츠 추출 (메타데이터 수집 포함)
    
    Note: fetch_page_with_timeout은 url_edu.py에 있으므로 import 필요
    """
    # url_edu.py에서 fetch_page_with_timeout을 import하여 사용
    from edu.url_edu import fetch_page_with_timeout
    
    try:
        html_content = await fetch_page_with_timeout(url, 0, timeout=60, stop_signal=stop_signal)
        if html_content:
            # 크롤링 모드에 따른 HTML 파싱 함수 선택 (리다이렉트된 URL 반영)
            final_url = url  # 기본값
            if crawl_mode == "crawling":
                result = parse_html_content_for_crawling_mode(html_content, url, block_tag=block_tag)
                logger.info(f"[크롤링모드 Playwright 파싱] URL: {url}, block_tag: {block_tag}")
            else:
                result = parse_html_content(html_content, url)
                
            # 결과에 최종 URL 정보 추가 (필요시 활용 가능)
            if result and isinstance(result, dict):
                result['_final_url'] = url
            
            # ✅ 에러 페이지 검증 추가
            if result:
                content = result.get('content', '')
                title = result.get('title', '') or result.get('subject', '') or result.get('web_title', '')
                
                # 에러 페이지 키워드 체크 (대소문자 구분 없이)
                error_keywords = [
                    '페이지를 찾을 수 없습니다',
                    '페이지를 찾을수 없습니다',
                    '페이지가 존재하지 않습니다',
                    '존재하지 않는 페이지',
                    '페이지 오류',
                    '페이지오류',
                    '서비스 준비중',
                    '점검 중',
                    '시스템 점검',
                    '임시로 이용할 수 없습니다',
                    '서버 오류',
                    '요청하신 페이지를 표시할 수 없습니다',
                    '권한이 없습니다',
                    '로그인이 필요합니다',
                    '접속 권한 없음',
                    '접근 제한',
                    '차단되었습니다',
                    '해당 글은 비공개',
                    '삭제된 게시물',
                    '비정상적인 접근',
                    '자동화 접근',
                    '로봇 차단',
                    '캡차',
                    '인증이 필요합니다',
                    'page not found',
                    'service unavailable',
                    'maintenance',
                    'temporarily unavailable',
                    'forbidden',
                    'access restricted',
                    'permission denied',
                    'login required',
                    'sign in',
                    'session expired',
                    'captcha',
                    'robot',
                    'bot detected',
                    'page cannot be displayed',
                    'error occurred',
                    '404 error',
                    '404 not found',
                    '접근할 수 없습니다',
                    '잘못된 경로',
                    '잘못된 페이지',
                    'not found',
                    'access denied',
                    '접근 거부',
                    '삭제된 페이지',
                    '삭제되었습니다'
                ]
                
                content_lower = content.lower()
                title_lower = title.lower()
                title_stripped = title.strip()
                
                # 1. 에러 키워드가 제목이나 내용에 포함되어 있는지 확인
                is_error_page = any(keyword in content_lower or keyword in title_lower for keyword in error_keywords)
                
                # 2. 제목이 비어있거나 너무 짧은 경우 (10자 이하)
                is_title_empty_or_short = len(title_stripped) == 0 or len(title_stripped) <= 10
                
                # 3. 제목이 숫자로만 구성된 경우 (200, 404, 500, 44985 등)
                is_numeric_only = title_stripped.isdigit()
                
                # 4. HTTP 상태 코드 패턴 체크 (200, 400, 403, 404, 500, 502, 503 등)
                http_status_codes = ['200', '400', '401', '403', '404', '500', '502', '503', '504']
                is_http_status = title_stripped in http_status_codes
                
                # 에러 판정
                if is_error_page:
                    logger.warning(f"[Playwright 크롤링 제외] 에러 페이지 감지: {url}")
                    logger.warning(f"   제목: {title[:50]}...")
                    logger.warning(f"   내용: {content[:100]}...")
                    return None
                
                # 제목이 짧고 숫자로만 구성된 경우
                if is_title_empty_or_short and is_numeric_only:
                    logger.warning(f"[Playwright 크롤링 제외] 제목이 숫자만 포함 ({title_stripped}): {url}")
                    logger.warning(f"   제목: {title[:50]}...")
                    logger.warning(f"   내용: {content[:100]}...")
                    return None
                
                if is_http_status:
                    logger.warning(f"[Playwright 크롤링 제외] HTTP 상태 코드 제목 감지 ({title_stripped}): {url}")
                    logger.warning(f"   제목: {title[:50]}...")
                    logger.warning(f"   내용: {content[:100]}...")
                    return None
                
                # 제목이 비어있으면 제목 추출 실패로 간주하여 제외
                if len(title_stripped) == 0:
                    logger.warning(f"[Playwright 크롤링 제외] 제목 추출 실패 (빈 제목): {url}")
                    logger.warning(f"   내용: {content[:100]}...")
                    return None
            
            # 메타데이터 수집 (신규 URL 처리용) - Playwright는 HTTP 헤더 정보 없이 해시만 계산
            if collect_metadata and result:
                # ✅ result['content']는 이미 텍스트로 추출된 것
                # 크롤링 결과 및 DB 저장과 동일한 정규화 로직 적용
                main_content = re.sub(r'\s+', ' ', result.get('content', '')).strip()
                metadata = {
                    'etag': None,  # Playwright에서는 HTTP 헤더 정보 수집 불가
                    'last_modified': None,
                    'content_length': None,
                    'content_hash': sha256_hex_utf8(main_content)
                }
                result['_metadata'] = metadata
                logger.info(f"[Playwright 메타데이터 수집] URL: {url}")
                logger.info(f"   - Content-Hash: {metadata.get('content_hash')}")
                logger.info(f"   - HTTP 헤더 정보: Playwright에서는 수집 불가")
            
            return result
        else:
            return None
    except Exception as e:
        logger.error(f"[Playwright 추출 실패] URL: {url}: {e}")
        return None
