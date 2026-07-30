"""
게시글 상세페이지 탐지 및 처리 유틸리티

원본: crawling/temp 프로젝트의 backend/detail_page_utils.py
현재 프로젝트(core crawler)에서 Scan 단계가
- "상세페이지"를 더 정확하게 판별하고
- 등록일/작성자 추출을 필요한 경우에만 수행
할 수 있도록 이식한 모듈입니다.
"""

from __future__ import annotations

import logging
import re
from typing import List, Optional
from urllib.parse import parse_qs, parse_qsl, urlparse

logger = logging.getLogger(__name__)

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]


# 게시판 고유 파라미터 (URL 쿼리 파라미터)
BOARD_PARAM_PATTERNS = [
    "nttid",
    "nttId",
    "ntt_id",
    "nttno",
    "nttNo",
    "bbsid",
    "bbsId",
    "bbs_id",
    "articleno",
    "articleNo",
    "article_no",
    "boardno",
    "boardNo",
    "board_no",
    "postno",
    "postNo",
    "post_no",
    "num",
    "id",
    "seq",
    "no",
    # 추가 보강: 다양한 프로젝트에서 사용되는 파라미터명
    "idx",
    "articleid",
    "article_id",
    "bbsno",
    "bbs_no",
    "postid",
    "post_id",
    "boardid",
    "board_id",
    "docid",
    "doc_id",
    "progrmsn",
    "progrmSn",
    "progrm_sn",
    "empmnsn",
    "empmnSn",
    "empmn_sn",
    # 노원구청 온라인접수·예약 상세 등
    "resveSn",
    "resvesn",
    # 충남청년포털 맞춤형 청년정책 등 (/customSupp/.../view?bizId=...)
    "bizid",
    "bizId",
    # 그누보드·XE 등
    "wr_id",
    "document_srl",
]

# 게시판 목록에서 숫자 값이 '글 PK'가 아니라 페이징·정렬·기간·탭 등에 쓰이기 쉬운 파라미터명(소문자)
BOARD_LIST_NUMERIC_QUERY_KEYS: frozenset[str] = frozenset(
    {
        "page",
        "pageindex",
        "page_index",
        "pageno",
        "page_no",
        "curpage",
        "cur_page",
        "pg",
        "cp",
        "boardpage",
        "startpage",
        "start",
        "offset",
        "limit",
        "rows",
        "row",
        "perpage",
        "pagesize",
        "listcount",
        "recordcountperpage",
        "block",
        "blockpage",
        "sst",
        "sod",
        "sop",
        "sort",
        "order",
        "ord",
        "year",
        "yr",
        "yyyy",
        "month",
        "mon",
        "mm",
        "day",
        "dd",
        "week",
        "wk",
        "category",
        "cat",
        "cate",
        "cat_id",
        "cate_id",
        "ctype",
        "mno",
        "menu",
        "menuno",
        "menu_cd",
        "menu_no",
        "deptid",
        "tab",
        "tabindex",
        "item",
        "p",
    }
)

# _is_detail_view_url 등: 페이징 파라미터만 있을 때 상세 부정에 사용
BOARD_PAGINATION_QUERY_KEYS: frozenset[str] = frozenset(
    {"page", "pageno", "pageindex", "curpage", "cur_page", "pg", "cp"}
)

_BOARD_PARAM_KEYS_LOWER: frozenset[str] = frozenset(p.lower() for p in BOARD_PARAM_PATTERNS)


def url_query_suggests_board_article_detail(url: str) -> bool:
    """
    쿼리만으로 '특정 글·항목 1건'을 가리킬 가능성이 있는지 판단합니다.
    - BOARD_PARAM_PATTERNS에 해당하는 키가 있고 값이 비어 있지 않으면 True
    - 그 외, 목록용 숫자 키(BOARD_LIST_NUMERIC_QUERY_KEYS)가 아닌 파라미터에 양의 정수 문자열이 있으면 True
      (그누보드 wr_id·알 수 없는 CMS의 uid= 등 하드코딩 없이 포괄)
    """
    if not url:
        return False
    try:
        items = parse_qsl(urlparse(url).query or "", keep_blank_values=False)
    except Exception:
        return False
    for raw_k, raw_v in items:
        k = (raw_k or "").strip().lower()
        v = (raw_v or "").strip()
        if k in _BOARD_PARAM_KEYS_LOWER and v:
            return True
    for raw_k, raw_v in items:
        k = (raw_k or "").strip().lower()
        v = (raw_v or "").strip()
        if not k or not v:
            continue
        if not re.fullmatch(r"[0-9]+", v):
            continue
        try:
            n = int(v)
        except ValueError:
            continue
        if n <= 0:
            continue
        if k in BOARD_LIST_NUMERIC_QUERY_KEYS:
            continue
        return True
    return False


# 날짜 패턴 (한 자리 숫자 및 다양한 구분자 지원)
DATE_PATTERN = re.compile(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}")

# 상세 페이지 식별용 키워드 패턴 (경로 또는 파일명에 포함)
DETAIL_PAGE_PATTERNS = [
    "brddetail.do",
    "brdview.do",
    "postdetail.do",
    "selectbbsnttview.do",
    "selectbbsnttlist.do",
    "detail.do",
    "view.asp",
    "view.php",
    "view.jsp",
    "detail.asp",
    "detail.php",
    "detail.jsp",
    "read.asp",
    "read.php",
    "read.jsp",
    "board_view",
    "board_read",
    "board_detail",
    "selectempmntestinfo.do",
    "/view/",
    # Spring 등 경로 /view?query (슬래시 없이 쿼리로 이어지는 상세)
    "/view?",
    "/detail/",
    "/read/",
    "/post/detail",
    # 보완: 약간 더 일반적인 패턴들 추가(오탐 주의로 단순 포함 검사만)
    "view.do",
    "read.do",
    "detail.do",
]


LIST_PAGE_PATH_PATTERNS = (
    "list.do",
    "alllist.do",
    "eventlist.do",
    "list.asp",
    "list.jsp",
    "list.php",
    "list.html",
    "deptgdc.do",
    "selectbbsnttlist.do",
)


DETAIL_PATH_HINT_PATTERNS = (
    "openbrlviewer",
    "allview.do",
    "brdview.do",
    "brddetail.do",
    "postdetail.do",
    "selectbbsnttview.do",
    "view.do",
    "detail.do",
    "read.do",
)


def is_likely_board_list_url(url: str) -> bool:
    """목록/메뉴 URL인지 URL 형태만으로 보수적으로 판별합니다."""
    if not url:
        return False
    try:
        parsed = urlparse(url)
        path = (parsed.path or "").lower()
        url_lower = url.lower()
    except Exception:
        path = ""
        url_lower = str(url or "").lower()

    if any(hint in path for hint in DETAIL_PATH_HINT_PATTERNS) and url_query_suggests_board_article_detail(url):
        return False
    if "openbrlviewer" in url_lower:
        return False
    if any(pattern in path for pattern in LIST_PAGE_PATH_PATTERNS):
        return not url_query_suggests_board_article_detail(url)
    if re.search(r"/(?:board|notice|bbs)(?:/[^/?#]+)*/?$", path):
        return not url_query_suggests_board_article_detail(url)
    return False


def _is_yongin_water_detail_url(url: str) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(url)
        path = (parsed.path or "").lower()
        if "yongin.go.kr" not in (parsed.netloc or "").lower():
            return False
        if "/water/wttnkmanage/" not in path:
            return False

        query = parse_qs(parsed.query or "")
        query_lower = {str(k or "").lower(): v for k, v in query.items()}
        detail_param_keys = (
            "q_clnsn",
            "q_edcsn",
            "q_inspctsn",
            "q_wsptnkinspctsn",
        )
        if not any((query_lower.get(key) or [""])[0] for key in detail_param_keys):
            return False

        detail_paths = (
            "bd_selectwttnkcln.do",
            "bd_selectwtwayedc.do",
            "bd_selectwttnkinspct.do",
            "bd_selectwsptnkinspct.do",
        )
        return any(path.endswith(detail_path) for detail_path in detail_paths)
    except Exception:
        return False


def is_detail_page_url(url: str) -> bool:
    """
    URL이 게시글 상세페이지인지 확인합니다.
    단순 '?' 포함 여부만으로는 오탐지가 많으므로, 구체적인 패턴과 파라미터를 검사합니다.
    """
    if not url:
        return False

    url_lower = url.lower()

    # 목록 URL은 상세 패턴보다 먼저 제외한다.
    if is_likely_board_list_url(url):
        return False

    try:
        from backend.board.kisa_identity_parse import is_identity_kisa_bbs_article_url

        if is_identity_kisa_bbs_article_url(url):
            return True
    except Exception:
        pass

    if "openbrlviewer" in url_lower:
        return True

    # 1. 상세 페이지 키워드 패턴 확인 (view.do, detail.php 등)
    if any(pattern in url_lower for pattern in DETAIL_PAGE_PATTERNS):
        return True

    if _is_yongin_water_detail_url(url):
        return True

    # 2. 게시판 고유 파라미터 확인 (nttId, num 등)
    # URL 파싱 부하를 줄이기 위해 먼저 문자열 검색으로 빠른 필터링 후 정밀 파싱
    if any(param.lower() in url_lower for param in (p.lower() for p in BOARD_PARAM_PATTERNS)):
        try:
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            for key in query_params.keys():
                if key.lower() in (p.lower() for p in BOARD_PARAM_PATTERNS):
                    return True
        except Exception:
            pass

    # EPIK(원어민보조교사선발): /web/epik/epk/updates/{숫자} (쿼리 없는 REST형 상세)
    if "epik.go.kr" in url_lower and re.search(r"/epk/updates/\d+", url_lower):
        return True

    return False


def is_post_detail_page_from_html(html: str, url: str) -> bool:
    """
    HTML과 URL을 분석하여 게시글 상세페이지인지 확인합니다.

    판별 로직: 아래 조건 중 2개 이상 충족 시 게시글로 인정
    1) URL에 '?' 포함
    2) 게시판 고유 파라미터 존재
    3) 제목 <a> 태그 존재
    4) 등록일 컬럼/키워드 존재
    5) 날짜 패턴 매칭 (HTML 텍스트에서)
    6) 작성자 영역 존재
    """
    if not html or not url:
        return False
    # 우선: URL이 명백한 '목록(list)' 페이지 패턴이면 상세 판정보다 목록을 우선시함.
    # - 예: 춘천시청의 clean URL(확장자/파라미터 없이 notice/legislative 등) 등
    try:
        lu = url.lower()
        parsed = urlparse(url)
        path = (parsed.path or "").lower()

        # 춘천시청 전용: 상세 식별자(num, nttno 등)가 전혀 없고 notice/legislative 등 키워드가 있으면 리스트로 간주
        if "chuncheon.go.kr" in lu:
            if (
                not any(k in lu for k in ["/detail/", "ctrtacctbookmngno="])
                and not url_query_suggests_board_article_detail(url)
                and any(k in lu for k in ["notice", "-info", "legislative", "contract", "daega"])
            ):
                logger.debug("[is_post_detail_page_from_html] URL looks like chuncheon list -> treat as list")
                return False

        # 일반 목록 패턴: list.* 확장자(예: list.do) 등
        if re.search(r"[a-z0-9]list\.(do|asp|jsp|php|html)", lu):
            logger.debug("[is_post_detail_page_from_html] URL matches list.* pattern -> treat as list")
            return False

        # 경로 기반: /board/, /notice/, /bbs/ 등 목록 키워드가 있고 상세 쿼리 신호가 없으면 리스트로 간주
        try:
            from backend.board.kisa_identity_parse import is_identity_kisa_bbs_article_url as _kisa_article
        except Exception:
            _kisa_article = lambda _u: False  # type: ignore[misc, assignment]
        if (
            any(x in path for x in ["/board/", "/notice/", "/bbs/"])
            and "detail" not in path
            and not url_query_suggests_board_article_detail(url)
            and not _kisa_article(url)
        ):
            logger.debug("[is_post_detail_page_from_html] URL path looks like list -> treat as list")
            return False
    except Exception:
        # 실패 시 기본 판정 로직으로 진행
        pass
    # 추가 검사: HTML 구조가 '목록'인 경우(여러 게시물 항목/페이징 등)에는 상세 판정보다 목록 우선
    try:
        if BeautifulSoup:
            soup = BeautifulSoup(html, "html.parser")
            # 페이징/목록 네비게이션 존재 시 리스트로 간주
            if soup.select_one(".pagination, .paging, .pg, .page-navi, .paging-area"):
                logger.debug("[is_post_detail_page_from_html] HTML contains pagination -> treat as list")
                return False

            # 후보 리스트 아이템 셀렉터들
            list_selectors = [
                "ul.board_list li",
                ".board_list li",
                ".list_box li",
                ".notice-list li",
                ".news-list li",
                "table tbody tr",
                ".board_list tr",
                ".list tr",
            ]
            candidates = []
            for sel in list_selectors:
                try:
                    found = soup.select(sel)
                except Exception:
                    found = []
                if found:
                    candidates.extend(found)

            # 대체 검사: anchor 주변에 날짜 텍스트가 있는 경우(목록형)
            if not candidates:
                anchors = soup.find_all("a")
                list_like = 0
                for a in anchors:
                    parent = a.find_parent(["li", "tr", "div", "td"]) or a.parent
                    text = parent.get_text(" ", strip=True) if parent else a.get_text(" ", strip=True)
                    if DATE_PATTERN.search(text) or any(k in text for k in ("등록일", "작성일", "게시일", "날짜")):
                        list_like += 1
                        if list_like >= 3:
                            logger.debug("[is_post_detail_page_from_html] multiple anchors with nearby dates -> treat as list")
                            return False
            else:
                cnt = 0
                for it in candidates:
                    t = it.get_text(" ", strip=True) or ""
                    if DATE_PATTERN.search(t) or it.find("a"):
                        cnt += 1
                        if cnt >= 3:
                            logger.debug("[is_post_detail_page_from_html] multiple list-like items detected -> treat as list")
                            return False
    except Exception:
        pass

    score = 0
    matched_conditions: list[str] = []

    # 1) URL에 '?' 포함
    if "?" in url:
        score += 1
        matched_conditions.append("url_has_query")

    # 2) 게시판 고유 파라미터 존재
    try:
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        has_board_param = any(k.lower() in (p.lower() for p in BOARD_PARAM_PATTERNS) for k in query_params.keys())
        if has_board_param:
            score += 1
            matched_conditions.append("has_board_param")
    except Exception:
        pass

    # HTML 파싱이 불가능하면 URL 기반 점수만으로 판단
    if not BeautifulSoup:
        return score >= 1

    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
    except Exception as e:
        logger.debug("[is_post_detail_page_from_html] HTML 파싱 실패: %s", e)
        return score >= 1

    # 3) 제목 관련 검사: <a> 링크가 있는 경우 외에도, 제목 태그(h1/h2/h3)의 존재를 허용
    title_link_selectors = [
        "h1 a",
        "h2 a",
        "h3 a",
        "h4 a",
        ".title a",
        ".subject a",
        ".post-title a",
        ".article-title a",
        ".board-title a",
        "#title a",
        "#subject a",
        "a[rel='bookmark']",
    ]
    has_title_link = any(soup.select_one(sel) for sel in title_link_selectors)
    if has_title_link:
        score += 1
        matched_conditions.append("has_title_link")
    else:
        # 제목이 <a> 없이 h1/h2/h3로만 있는 경우(길이가 적절하면)도 인정
        try:
            for tag_name in ("h1", "h2", "h3"):
                tag = soup.find(tag_name)
                if tag:
                    txt = (tag.get_text(" ", strip=True) or "")
                    if len(txt) >= 5:
                        score += 1
                        matched_conditions.append(f"has_{tag_name}_title")
                        break
        except Exception:
            pass

    # 4) 등록일 컬럼/키워드 존재
    date_selectors = [
        "td.date",
        "td.reg-date",
        "td.write-date",
        "span.date",
        "span.reg-date",
        "span.write-date",
        ".date",
        ".reg-date",
        ".write-date",
        ".info",
        ".writer",
        ".post-meta",
        ".post-date",
        ".created",
        '[class*="date"]',
        '[id*="date"]',
        '[class*="info"]',
    ]
    has_date_column = False
    for selector in date_selectors:
        target = soup.select_one(selector)
        if not target:
            continue
        t_text = target.get_text(" ", strip=True)
        if DATE_PATTERN.search(t_text) or any(k in t_text for k in ("등록일", "작성일", "날짜", "게시일")):
            has_date_column = True
            break
    if has_date_column:
        score += 1
        matched_conditions.append("has_date_column")

    # 5) 날짜 패턴 매칭
    body_text = soup.get_text(" ", strip=True)
    if DATE_PATTERN.search(body_text):
        score += 1
        matched_conditions.append("date_pattern_matched")

    # 추가: meta 태그 기반 힌트(og:type=article 등)는 신뢰도 보정에 사용
    try:
        og_type = None
        og_tag = soup.select_one("meta[property='og:type']") or soup.select_one("meta[name='og:type']")
        if og_tag:
            og_type = (og_tag.get("content") or "").strip().lower()
        twitter_card = None
        tw_tag = soup.select_one("meta[name='twitter:card']")
        if tw_tag:
            twitter_card = (tw_tag.get("content") or "").strip().lower()
        if og_type == "article" or twitter_card in ("summary", "summary_large_image"):
            score += 1
            matched_conditions.append("meta_article_type")
    except Exception:
        pass

    # 6) 작성자 영역 존재
    author_selectors = [".writer", ".author", ".post-author", '[class*="writer"]', '[class*="author"]', '[class*="author-"]']
    if any(soup.select_one(sel) for sel in author_selectors):
        score += 1
        matched_conditions.append("has_author_area")

    is_post = score >= 2
    if is_post:
        logger.debug(
            "[is_post_detail_page_from_html] OK score=%s url=%s cond=%s",
            score,
            url[:120],
            ",".join(matched_conditions),
        )
    return is_post


def find_detail_page_url_in_parent(links: List[str], base_url: str) -> str:
    """링크 목록에서 게시글 상세페이지 URL을 찾습니다."""
    from urllib.parse import urljoin

    for link in links:
        if not link:
            continue
        full_url = urljoin(base_url, link)
        if is_detail_page_url(full_url):
            return full_url
    return ""


def extract_board_url_from_post_url(post_url: str) -> Optional[str]:
    """
    게시글 상세페이지 URL에서 게시판 목록 페이지 URL을 추출합니다.
    (temp 프로젝트 로직 이식. 현재 프로젝트에서는 필요 시에만 사용)
    """
    if not post_url:
        return None

    try:
        parsed = urlparse(post_url)
        path = parsed.path.lower()
        query = parsed.query

        board_path = path
        replacements = [
            ("view.do", "list.do"),
            ("detail.do", "list.do"),
            ("read.do", "list.do"),
            ("view.asp", "list.asp"),
            ("detail.asp", "list.asp"),
            ("read.asp", "list.asp"),
            ("view.php", "list.php"),
            ("detail.php", "list.php"),
            ("read.php", "list.php"),
            ("view.jsp", "list.jsp"),
            ("detail.jsp", "list.jsp"),
            ("read.jsp", "list.jsp"),
            ("brdview.do", "brdlist.do"),
            ("brddetail.do", "brdlist.do"),
        ]
        for old, new in replacements:
            if old in board_path:
                board_path = board_path.replace(old, new)
                break

        if query:
            query_params = parse_qs(query)
            board_params: dict[str, str] = {}
            board_param_keys = ["menuno", "menu_no", "menu_cd", "bbsid", "bbs_id", "boardno", "board_no"]

            for key, values in query_params.items():
                key_lower = key.lower()
                # 게시글 고유 파라미터 제외
                if key_lower in ("nttid", "ntt_id", "num", "id", "seq", "no", "articleno", "article_no"):
                    continue
                if any(bp in key_lower for bp in board_param_keys) or len(query_params) == 1:
                    board_params[key] = values[0] if values else ""

            if board_params:
                from urllib.parse import urlencode

                board_query = urlencode(board_params)
                board_url = f"{parsed.scheme}://{parsed.netloc}{board_path}?{board_query}"
            else:
                board_url = f"{parsed.scheme}://{parsed.netloc}{board_path}"
        else:
            board_url = f"{parsed.scheme}://{parsed.netloc}{board_path}"

        if board_url != post_url:
            return board_url
        return None
    except Exception as e:
        logger.debug("[extract_board_url_from_post_url] fail: %s url=%s", e, post_url[:120])
        return None
