"""
KISA 본인확인 지원포털(identity.kisa.or.kr) 게시 상세 전용 제목 추출.

레이아웃 A — 가이드/지침 등(예: /web/main/bbs/guide/…):
    `#content table.tbl_view th.titBox` (동일 문구가 `<caption>`에도 있음)
    본문: `#content table.tbl_view td.bo_con` (내부 div에 안내 문구·첨부 안내 인접 행과 분리)

레이아웃 B — 교육영상(전체/이용자/기관 등, 예: edu_all, edu_user):
    제목은 «제목» 행 `td`, 본문은 `th`가 없는 행 중 텍스트가 가장 긴 `td`(첨부 행은 짧음)

공통 폴백: 동일 영역 `table.tbl_view caption`

샘플:
- https://identity.kisa.or.kr/web/main/bbs/guide/43?cp=1
- https://identity.kisa.or.kr/web/main/bbs/guide/113?cp=1
- https://identity.kisa.or.kr/web/main/bbs/edu_user/46?cp=1
- https://identity.kisa.or.kr/web/main/bbs/edu_all/45?cp=1
"""

from __future__ import annotations

import re
from html import unescape
from typing import Any
from urllib.parse import urlparse

# /web/main/bbs/guide/43, .../edu_identification/56 등 글 번호가 경로에 있는 상세 URL
_KISA_BBS_ARTICLE_PATH_RE = re.compile(r"/web/main/bbs/[^/]+/\d+$", re.IGNORECASE)

# 교육형 표에서 첨부·안내 한 줄과 본문 행을 구분 (본문은 수백~수천 자)
_MIN_EDU_STYLE_BODY_CHARS = 120


def _norm(text: str) -> str:
    t = unescape((text or "").strip())
    t = re.sub(r"[\u00A0\t\r\n]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    # 연도·부가설명 괄호(예: (2024.3)) 보존 — 일반 board _norm과 달리 ()는 제거하지 않음
    t = t.strip("·|｜-–—:：[]「」『』<>")
    return t


def is_identity_kisa_url(url: str) -> bool:
    if not url:
        return False
    return "identity.kisa.or.kr" in (url or "").lower()


def is_identity_kisa_bbs_article_url(url: str) -> bool:
    """
    게시 상세: 경로 `/web/main/bbs/{게시유형}/{숫자}`.
    쿼리에 `cp`만 있어도 상세(목록용 페이지 키와 별개).
    """
    if not is_identity_kisa_url(url):
        return False
    try:
        path = (urlparse(url).path or "").strip().rstrip("/")
        return bool(_KISA_BBS_ARTICLE_PATH_RE.search(path))
    except Exception:
        return False


def select_identity_kisa_content_root(soup: Any) -> Any:
    """
    본문 루트 `td` 선택.

    1) 가이드/지침: `td.bo_con`
    2) 교육영상(edu_all·edu_user 등): `th`가 없는 행의 `td` 중 텍스트 길이가 최대이면서
       `_MIN_EDU_STYLE_BODY_CHARS` 이상인 셀(첨부 링크 행 제외)
    """
    if soup is None:
        return None
    scope = soup.select_one("#content") or soup
    try:
        bo = scope.select_one("table.tbl_view td.bo_con")
        if bo:
            return bo

        tbl = scope.select_one("table.tbl_view")
        if not tbl:
            return None

        best_td = None
        best_len = 0
        for tr in tbl.find_all("tr"):
            if tr.find("th"):
                continue
            td = tr.find("td")
            if not td:
                continue
            raw = td.get_text(" ", strip=True)
            n = len(raw)
            if n < _MIN_EDU_STYLE_BODY_CHARS:
                continue
            if n > best_len:
                best_len = n
                best_td = td
        return best_td
    except Exception:
        return None


def extract_identity_kisa_board_title(soup: Any, *, url: str = "") -> str:
    """상세 HTML에서 게시 제목. 실패 시 빈 문자열. (url은 호출부 통일용, DOM만으로 판별.)"""
    if soup is None:
        return ""
    scope = soup.select_one("#content") or soup

    try:
        tit = scope.select_one("table.tbl_view th.titBox")
        if tit:
            s = _norm(tit.get_text(" ", strip=True))
            if len(s) >= 2:
                return s
    except Exception:
        pass

    label_flat = "제목"
    try:
        for tbl in scope.select("table.tbl_view"):
            for tr in tbl.find_all("tr"):
                th = tr.find("th")
                if not th:
                    continue
                th_flat = re.sub(r"\s+", "", th.get_text(strip=True))
                if th_flat != label_flat and "제목" not in th.get_text():
                    continue
                td = th.find_next_sibling("td")
                if not td:
                    td = tr.find("td")
                if not td:
                    continue
                s = _norm(td.get_text(" ", strip=True))
                if len(s) >= 2:
                    return s
    except Exception:
        pass

    try:
        cap = scope.select_one("table.tbl_view caption")
        if cap:
            s = _norm(cap.get_text(" ", strip=True))
            if len(s) >= 2:
                return s
    except Exception:
        pass

    return ""
