"""
춘천시청 계약정보공개(chuncheon.go.kr/contract/) 전용 수집·파싱.

기존 navigate_from / 병렬 하이브리드 대신, 단일 브라우저 컨텍스트에서
「계약 포털 → 해당 목록 → 상세」 순으로 고정 파이프라인을 밟은 뒤,
실패 시에만 목록에서 관리번호 링크를 클릭해 상세를 연다.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import os
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

logger = logging.getLogger(__name__)

CONTRACT_PORTAL_URL = "https://www.chuncheon.go.kr/contract/"
DETAIL_CONTENT_SELECTORS = ".ctrtAcctBook, .serch_result_wrap, .detail_view"


# ---------------------------------------------------------------------------
# URL 판별·변환 (워크플로 discover / 분기와 동일 의미)
# ---------------------------------------------------------------------------


def is_chuncheon_contract_url(u: str) -> bool:
    if not u:
        return False
    return "chuncheon.go.kr/contract/" in (u or "").lower()


def is_chuncheon_contract_detail_url(u: str) -> bool:
    if not is_chuncheon_contract_url(u):
        return False
    lu = (u or "").lower()
    return "/detail/" in lu or "ctrtacctbookmngno=" in lu


def is_chuncheon_contract_list_url(u: str) -> bool:
    if not is_chuncheon_contract_url(u):
        return False
    lu = (u or "").lower()
    if any(k in lu for k in ("/detail/", "ctrtacctbookmngno=")):
        return False
    return True


def chuncheon_contract_detail_to_list_url(url: str) -> Optional[str]:
    if not url or not is_chuncheon_contract_url(url):
        return None
    lu = (url or "").lower()
    if "/detail/" not in lu and "ctrtacctbookmngno=" not in lu:
        return None
    try:
        parsed = urlparse(url)
        path = (parsed.path or "").rstrip("/")
        if "/detail" in path.lower():
            path = path.split("/detail")[0] or path
        path = path.rstrip("/") + "/"
        return urlunparse((parsed.scheme or "https", parsed.netloc, path, "", "", ""))
    except Exception:
        return None


def extract_ctrt_acct_book_mng_no(url: str) -> str:
    m = re.search(r"ctrtAcctBookMngNo=([^&]+)", url or "", re.I)
    return (m.group(1) if m else "") or ""


def normalize_chuncheon_detail_url(url: str) -> str:
    """
    춘천 계약 상세 URL의 /detail/? 형태를 /detail? 로 정규화.
    목록의 실제 링크 형식과 맞추면 직접 진입 성공률이 올라간다.
    """
    if not url:
        return ""
    try:
        p = urlparse(url)
        path = (p.path or "")
        if path.endswith("/detail/"):
            path = path[:-1]
        return urlunparse((p.scheme or "https", p.netloc, path, p.params, p.query, p.fragment))
    except Exception:
        return url


def _with_page_index(list_url: str, page_index: int) -> str:
    if page_index <= 1:
        return list_url
    try:
        p = urlparse(list_url)
        q = parse_qs(p.query or "", keep_blank_values=True)
        # 춘천 계약 목록은 pg 기반 페이징을 사용한다.
        # 기존 경로 호환을 위해 pageIndex도 함께 유지한다.
        q["pg"] = [str(page_index)]
        q["pageIndex"] = [str(page_index)]
        return urlunparse((p.scheme or "https", p.netloc, p.path, p.params, urlencode(q, doseq=True), p.fragment))
    except Exception:
        sep = "&" if "?" in list_url else "?"
        return f"{list_url}{sep}pg={page_index}&pageIndex={page_index}"


def detail_html_looks_loaded(html: Optional[str]) -> bool:
    if not html or len(html) < 200:
        return False
    h = html.lower()
    # 목록 페이지(.serch_sub_result_ul 등)를 상세로 오인하지 않도록 방지
    if "serch_sub_result_ul" in h and "content01_2title_box" not in h and "content01_ul" not in h:
        return False
    for needle in (
        "ctrtacctbook",
        "ctrt_acct_book",
        "contractacctbook",
        "content01_2title_box",
        "content01_title_v",
        "content01_ul",
    ):
        if needle in h:
            return True
    return False


def _norm_one_line(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).strip()


def _chuncheon_meta_title_is_menu_noise(t: str) -> bool:
    """og:title·<title>이 실제 계약명이 아니라 메뉴/페이지 껍데기인 경우."""
    s = _norm_one_line(t)
    if not s:
        return True
    if "상세보기" in s:
        return True
    if re.match(r"^수의\s*계약현황", s):
        return True
    # 목록 유형 메뉴 제목(물품/용역 등) — 상세 계약명이 아님
    if re.match(r"^(물품|용역|공사|매각|공사용역)\s*계약현황", s, re.I):
        return True
    if re.search(r"계약현황\s*$", s) and len(s) <= 32:
        return True
    if "계약정보공개" in s and len(s) < 40:
        return True
    # "수의 계약현황 | 춘천시 계약정보공개" 등 파이프로 이어진 사이트 메뉴/브랜딩
    if "|" in s or "｜" in s:
        if re.search(r"수의\s*계약현황|계약정보\s*공개|춘천시", s):
            return True
    if "춘천시" in s and "계약" in s and len(s) <= 48:
        return True
    return False


def _chuncheon_is_acceptable_extracted_title(t: str) -> bool:
    """DOM/중간 후보가 사이트 메뉴·브랜딩 문구면 제외."""
    if not t or len(t.strip()) < 2:
        return False
    return not _chuncheon_meta_title_is_menu_noise(t)


def _looks_like_chuncheon_contract_menu_noise(text: str) -> bool:
    """
    계약 상세 본문이 아니라 상단/좌측 메뉴 텍스트를 긁은 경우를 차단.
    """
    t = _norm_one_line(text)
    if not t:
        return True
    low = t.lower()
    menu_tokens = (
        "발주계획",
        "입찰정보",
        "입찰공고",
        "개찰공고",
        "계약정보",
        "계약현황",
        "하도급계약",
        "변경계약",
        "감독/검사",
        "대가지급",
        "알림마당",
        "계약법규",
        "계약서식",
        "수의계약",
        "제한업체",
        "원가공개",
        "contract information open",
    )
    hits = sum(1 for tok in menu_tokens if tok in t or tok in low)
    return hits >= 6 and len(t) < 2500


def _chuncheon_serch_result_data_row_li_from_anchor(a):
    """계약명 링크 a → 목록 데이터 행 li.serch_result_list (serch_sub_result_list 열은 제외)."""
    cur = getattr(a, "parent", None)
    depth = 0
    while cur is not None and depth < 28:
        if getattr(cur, "name", None) == "li":
            cls = cur.get("class") or []
            cl = " ".join(str(c).lower() for c in cls)
            # 열 셀 클래스명에 serch_result_list가 포함되므로 반드시 먼저 제외
            if "serch_sub_result" in cl.replace("_", ""):
                pass
            elif "serch_result_list" in cl:
                if "serch_result_title" in cl:
                    return None
                return cur
        cur = getattr(cur, "parent", None)
        depth += 1
    return None


def extract_contract_name_from_list_row_for_url(soup, detail_url: str) -> str:
    """
    상세 URL에 맞는 a[href*=ctrtAcctBookMngNo]가 있는 행에서 계약명 열(5번째) 값.
    (상세 DOM 대신 목록만 온 경우, URL별로 구분되는 유일한 후보)
    """
    mng = extract_ctrt_acct_book_mng_no(detail_url or "")
    if not mng or not soup:
        return ""
    bad = frozenset({"", "정보없음", "계약명", "제목", "제목 없음"})
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "")
        mm = re.search(r"ctrtAcctBookMngNo\s*=\s*([^&]+)", href, re.I)
        if not mm or (mm.group(1) or "").strip() != mng.strip():
            continue
        row_li = _chuncheon_serch_result_data_row_li_from_anchor(a)
        if not row_li:
            continue
        sub_ul = row_li.select_one("ul.serch_sub_result_ul")
        if not sub_ul:
            continue
        cells = [
            _norm_one_line(li.get_text(" ", strip=True))
            for li in sub_ul.find_all("li", recursive=False)
        ]
        if len(cells) < 5:
            continue
        t = _norm_one_line(cells[4])
        if t and t not in bad and len(t) > 1:
            return t[:500]
    return ""


def _is_chuncheon_contract_name_label(lab: str) -> bool:
    """th/라벨이 '계약명'인지(공백·NBSP 변형 허용)."""
    x = (lab or "").replace("\u00a0", " ")
    x = re.sub(r"\s+", "", _norm_one_line(x))
    if not x:
        return False
    if x == "계약명":
        return True
    return x.startswith("계약명") and len(x) <= 12


def extract_contract_name_title(soup, detail_url: str = "") -> str:
    """
    계약 상세 화면의 '계약명' 표시 텍스트.
    DOM 변형(클래스 접미사, 하이픈/언더스코어 혼용)에 대비해 단계적으로 탐색한다.
    detail_url 이 있으면 목록 HTML일 때 동일 관리번호 행의 계약명 열을 마지막에 시도한다.
    """
    if not soup:
        return ""

    bad = frozenset({"", "정보없음", "계약명", "제목", "제목 없음"})

    # 상세 URL인데 실제 HTML이 목록이면, 동일 관리번호 행의 계약명을 최우선(다른 경로의 DOM·메타보다 먼저)
    if detail_url and is_chuncheon_contract_detail_url(detail_url):
        t_row = extract_contract_name_from_list_row_for_url(soup, detail_url)
        if t_row:
            return t_row

    # 0) 상세 본문 첫 li: GNB·다른 영역에 동일 클래스가 있어도 계약 요약 블록 우선 (이미지 DOM 기준)
    try:
        li0 = soup.select_one(".serch_result_wrap .content01_ul > li.content01_list")
        if li0:
            p0 = li0.select_one(".content01_2title_box .content01_title_v p")
            if not p0:
                v0 = li0.select_one(".content01_2title_box .content01_title_v")
                if v0:
                    t = _norm_one_line(v0.get_text(" ", strip=True))
                    if t and len(t) > 1 and t not in bad and _chuncheon_is_acceptable_extracted_title(t):
                        return t
            else:
                t = _norm_one_line(p0.get_text(" ", strip=True))
                if t and len(t) > 1 and t not in bad and _chuncheon_is_acceptable_extracted_title(t):
                    return t
    except Exception:
        pass

    # 1) 라벨이 정확히 '계약명'인 박스만 (첫 번째 .content01_title_v p가 가격 등으로 잘못 잡히는 경우 방지)
    #    값이 <p>가 아니라 div 직하 텍스트만 있는 변형도 수용
    try:
        for box in soup.select(".content01_2title_box"):
            tit = box.select_one(".content01_title")
            val_el = box.select_one(".content01_title_v")
            if not tit or not val_el:
                continue
            lab = _norm_one_line(tit.get_text(" ", strip=True))
            if not _is_chuncheon_contract_name_label(lab):
                continue
            p = val_el.find("p")
            raw = p.get_text(" ", strip=True) if p else val_el.get_text(" ", strip=True)
            t = _norm_one_line(raw)
            if t and len(t) > 1 and t not in bad and _chuncheon_is_acceptable_extracted_title(t):
                return t
    except Exception:
        pass

    strict_selectors = (
        ".serch_result_wrap .content01_title_v p",
        ".serch_result_wrap .content01_title_v > p",
        ".content01_2title_box .content01_title_v p",
        ".content01_2title_box .content01_title_v > p",
        ".content01_title_v p",
        ".content01_title_v > p",
    )
    for sel in strict_selectors:
        try:
            el = soup.select_one(sel)
            if el:
                t = _norm_one_line(el.get_text(" ", strip=True))
                if t and len(t) > 1 and t not in bad and _chuncheon_is_acceptable_extracted_title(t):
                    return t
        except Exception:
            pass

    def _class_blob(tag) -> str:
        cls = tag.get("class") or []
        return " ".join(str(c).lower() for c in cls).replace("-", "_")

    wrap = soup.select_one(".serch_result_wrap")
    if not wrap:
        for tag in soup.find_all(True):
            blob = _class_blob(tag)
            if "serch_result_wrap" in blob or "serch_result" in blob:
                wrap = tag
                break
    search_roots = [wrap] if wrap else [soup]

    for root in search_roots:
        if not root:
            continue
        for div in root.find_all("div"):
            blob = _class_blob(div)
            if "content01_title_v" not in blob and "content01titlev" not in blob.replace("_", ""):
                continue
            for p in div.find_all("p", recursive=False):
                t = _norm_one_line(p.get_text(" ", strip=True))
                if t and len(t) > 1 and t not in bad and _chuncheon_is_acceptable_extracted_title(t):
                    return t
            p = div.find("p")
            if p:
                t = _norm_one_line(p.get_text(" ", strip=True))
                if t and len(t) > 1 and t not in bad and _chuncheon_is_acceptable_extracted_title(t):
                    return t
            t = _norm_one_line(div.get_text(" ", strip=True))
            if t and len(t) > 1 and t not in bad and _chuncheon_is_acceptable_extracted_title(t):
                return t

    # 표 행: th가 계약명인 경우
    try:
        for tr in soup.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            h = _norm_one_line(cells[0].get_text(" ", strip=True))
            if not _is_chuncheon_contract_name_label(h):
                continue
            t = _norm_one_line(cells[1].get_text(" ", strip=True))
            if t and len(t) > 1 and t not in bad and _chuncheon_is_acceptable_extracted_title(t):
                return t
    except Exception:
        pass

    return ""


# ---------------------------------------------------------------------------
# 표시용 제목: 물품·수의 등 계약 구분 + 계약명
# ---------------------------------------------------------------------------

# /contract/{segment}/… 목록 URL의 첫 경로 세그먼트 → 저장 제목 앞에 붙일 구분(짧은 라벨)
_CONTRACT_PATH_KIND_LABEL: dict[str, str] = {
    "data": "물품계약",
    "order": "발주계획",
    "manager": "감독·검사",
    "report": "계약법규",
    "daega": "대가지급",
    "main": "",
}


def _chuncheon_contract_board_path_segment(url: str) -> str:
    try:
        p = urlparse(url or "")
        parts = [x for x in (p.path or "").split("/") if x]
        if len(parts) >= 2 and parts[0].lower() == "contract":
            return parts[1].lower()
    except Exception:
        pass
    return ""


def _contract_kind_label_from_soup_titles(soup) -> str:
    """<title>·og:title 에서 '○○ 계약현황' 등으로 구분 추출."""
    if not soup:
        return ""
    for el in (soup.select_one("title"), soup.select_one("meta[property='og:title']")):
        if el is None:
            continue
        raw = el.get("content") if getattr(el, "name", None) == "meta" else el.get_text(" ", strip=True)
        raw = _norm_one_line(raw or "")
        if not raw:
            continue
        m = re.match(r"^(.+?)\s+계약현황\b", raw)
        if m:
            head = (m.group(1) or "").strip()
            if head:
                return head if head.endswith("계약") else f"{head}계약"
        if re.match(r"^발주\s*계획", raw) or raw.startswith("발주계획"):
            return "발주계획"
        if re.match(r"^감독\s*/\s*검사", raw):
            return "감독·검사"
        if raw.startswith("대가지급"):
            return "대가지급"
        if raw.startswith("계약법규"):
            return "계약법규"
    return ""


def extract_contract_kind_label(url: str, soup) -> str:
    """
    계약정보 게시판 구분(물품·수의·발주 등).
    상세 HTML의 title/og가 있으면 우선(수의/물품이 동일 /contract/data/ 인 경우 대비),
    없으면 URL 경로 세그먼트로 추론.
    """
    k = _contract_kind_label_from_soup_titles(soup)
    if k:
        return k
    seg = _chuncheon_contract_board_path_segment(url or "")
    return _CONTRACT_PATH_KIND_LABEL.get(seg) or ""


def extract_contract_display_title(soup, url: str = "") -> str:
    """
    저장·표시용 제목: `구분 | 순수 계약명`.
    구분은 extract_contract_kind_label, 계약명은 extract_contract_name_title(+메타 폴백).
    """
    if not soup:
        return "제목 없음"

    from backend.board.board_content_extractor import _collapse_ws

    base = extract_contract_name_title(soup, url)
    if not base:
        meta_og = soup.select_one("meta[property='og:title']")
        if meta_og and meta_og.get("content"):
            cand = _collapse_ws(str(meta_og.get("content")))
            cand0 = re.split(r"\s*(?:\||｜|-)\s*", cand)[0].strip() if cand else ""
            if cand0 and not _chuncheon_meta_title_is_menu_noise(cand0):
                base = cand0
        if not base:
            t_el = soup.select_one("title")
            if t_el:
                cand = _collapse_ws(t_el.get_text(" ", strip=True))
                cand0 = re.split(r"\s*(?:\||｜|-)\s*", cand)[0].strip() if cand else ""
                if cand0 and not _chuncheon_meta_title_is_menu_noise(cand0):
                    base = cand0
    if not base:
        base = "제목 없음"

    kind = extract_contract_kind_label(url or "", soup)
    if not kind:
        return base
    if base.startswith(kind):
        return base
    return f"{kind} | {base}"


# ---------------------------------------------------------------------------
# Playwright: 포털 → 목록 → 상세 (실패 시 목록에서 링크 클릭)
# ---------------------------------------------------------------------------


async def fetch_contract_detail_html(detail_url: str) -> Optional[str]:
    """
    계약 상세 HTML만 반환. 정적 GET과 달리 세션·경로를 맞추기 위해
    항상 CONTRACT_PORTAL_URL 을 먼저 연 뒤 같은 컨텍스트에서 목록·상세를 연다.
    """
    if not detail_url or not is_chuncheon_contract_detail_url(detail_url):
        return None
    detail_url = normalize_chuncheon_detail_url(detail_url)

    from backend.board.playwright_renderer import _ensure_browser, _semaphore
    from backend.board.playwright_renderer import render_page_via_playwright_hybrid_click
    from backend.shared.config import Config

    list_url = chuncheon_contract_detail_to_list_url(detail_url) or detail_url
    if list_url == detail_url:
        try:
            parsed = urlparse(detail_url)
            path = (parsed.path or "").rstrip("/")
            if "/detail" in path.lower():
                path = path.split("/detail")[0].rstrip("/") + "/"
                list_url = urlunparse((parsed.scheme or "https", parsed.netloc, path, "", "", ""))
        except Exception:
            pass

    mng_no = extract_ctrt_acct_book_mng_no(detail_url)
    logger.info(
        "[chuncheon_contract] 입력 URL | detail=%s | list=%s | target_mng_no=%s",
        detail_url,
        list_url,
        mng_no,
    )

    try:
        step_ms = int(os.getenv("BOARD_CHUNCHEON_STEP_WAIT_MS", "900") or "900")
    except Exception:
        step_ms = 900
    step_ms = max(200, min(step_ms, 5000))

    try:
        detail_ms = int(os.getenv("BOARD_CHUNCHEON_DETAIL_WAIT_MS", "2200") or "2200")
    except Exception:
        detail_ms = 2200
    detail_ms = max(400, min(detail_ms, 12000))

    try:
        max_pages_same_context = int(os.getenv("BOARD_CHUNCHEON_MAX_PAGES_SAME_CONTEXT", "80") or "80")
    except Exception:
        max_pages_same_context = 80
    max_pages_same_context = max(5, min(max_pages_same_context, 300))

    try:
        max_pages_hybrid = int(os.getenv("BOARD_CHUNCHEON_MAX_PAGES_HYBRID", "80") or "80")
    except Exception:
        max_pages_hybrid = 80
    max_pages_hybrid = max(10, min(max_pages_hybrid, 300))

    try:
        max_pages_hybrid_seq = int(os.getenv("BOARD_CHUNCHEON_MAX_PAGES_HYBRID_SEQ", "140") or "140")
    except Exception:
        max_pages_hybrid_seq = 140
    max_pages_hybrid_seq = max(20, min(max_pages_hybrid_seq, 500))

    timeout_ms = int(Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
    wait = "domcontentloaded"

    browser = await _ensure_browser()

    async with _semaphore:
        for attempt in range(2):
            context = await browser.new_context(
                ignore_https_errors=True,
                viewport={"width": 1920, "height": 1080},
            )
            page = await context.new_page()
            try:
                await page.goto(CONTRACT_PORTAL_URL, wait_until=wait, timeout=timeout_ms)
                await page.wait_for_timeout(step_ms)

                await page.goto(list_url, wait_until=wait, timeout=timeout_ms)
                await page.wait_for_timeout(step_ms)

                await page.goto(
                    detail_url,
                    wait_until=wait,
                    timeout=timeout_ms,
                    referer=list_url,
                )
                await page.wait_for_timeout(detail_ms)
                try:
                    await page.wait_for_selector(
                        DETAIL_CONTENT_SELECTORS,
                        state="visible",
                        timeout=min(10000, detail_ms + 4000),
                    )
                except Exception:
                    pass

                html = await page.content()
                if detail_html_looks_loaded(html):
                    return html

                if mng_no:
                    try:
                        # 동일 페이지/동일 컨텍스트에서 pageIndex를 넘겨가며 target href 탐색
                        found_href = ""
                        for page_idx in range(1, max_pages_same_context + 1):
                            candidate_list_url = _with_page_index(list_url, page_idx)
                            moved_by_click = False
                            if page_idx > 1:
                                # pageIndex 쿼리만으로 동일 목록이 반복되는 경우가 있어
                                # 페이지 번호 클릭을 우선 시도한다.
                                try:
                                    moved_by_click = await page.evaluate(
                                        """(idx) => {
                                            const target = String(idx);
                                            const nodes = Array.from(document.querySelectorAll("a, button"));
                                            const pageLike = nodes.filter((el) => {
                                                const txt = String((el.textContent || "").trim());
                                                if (!txt) return false;
                                                if (txt !== target) return false;
                                                const cl = String(el.className || "").toLowerCase();
                                                const p = el.closest(".pagination, .paging, .page_nav");
                                                return !!p || cl.includes("page") || cl.includes("paging");
                                            });
                                            if (!pageLike.length) return false;
                                            const el = pageLike[0];
                                            el.scrollIntoView({ block: "center" });
                                            el.click();
                                            return true;
                                        }""",
                                        page_idx,
                                    )
                                    if moved_by_click:
                                        # 일부 페이지는 soft navigation으로 URL만 갱신되므로 짧게만 대기
                                        try:
                                            await page.wait_for_load_state("domcontentloaded", timeout=min(timeout_ms, 1200))
                                        except Exception:
                                            pass
                                except Exception:
                                    moved_by_click = False
                            if not moved_by_click:
                                await page.goto(
                                    candidate_list_url,
                                    wait_until=wait,
                                    timeout=timeout_ms,
                                    referer=CONTRACT_PORTAL_URL,
                                )
                            await page.wait_for_timeout(max(200, step_ms // 2))
                            try:
                                await page.wait_for_selector(
                                    "tbody tr, .serch_sub_result_ul li, a[href*='ctrt'], a[href*='Ctrt'], a[onclick], button[onclick]",
                                    timeout=3500,
                                )
                            except Exception:
                                pass
                            # pageIndex 쿼리만 바뀌고 실제 목록 페이지가 유지되는 현상 점검
                            page_probe = await page.evaluate(
                                """() => {
                                    const currentUrl = String(window.location.href || "");
                                    const active = document.querySelector(
                                      ".pagination .on, .paging .on, .page_nav .on, .pagination strong, .paging strong"
                                    );
                                    const txt = String((active?.textContent || "").trim());
                                    const num = parseInt((txt.match(/\\d+/) || [])[0] || "", 10);
                                    return {
                                        currentUrl,
                                        activePageText: txt,
                                        activePageNum: Number.isFinite(num) ? num : null,
                                    };
                                }"""
                            )
                            if isinstance(page_probe, dict):
                                active_no = page_probe.get("activePageNum")
                                if isinstance(active_no, int) and active_no != page_idx:
                                    logger.debug(
                                        "[chuncheon_contract] pageIndex 불일치 감지 | requested=%s active=%s active_text=%s url=%s",
                                        page_idx,
                                        active_no,
                                        str(page_probe.get("activePageText") or "")[:40],
                                        str(page_probe.get("currentUrl") or "")[:180],
                                    )
                                logger.info(
                                    "[chuncheon_contract] 페이지 이동 | requested=%s active=%s active_text=%s url=%s",
                                    page_idx,
                                    page_probe.get("activePageNum"),
                                    str(page_probe.get("activePageText") or ""),
                                    str(page_probe.get("currentUrl") or ""),
                                )

                            scan_result = await page.evaluate(
                                """(mngNo) => {
                                    const anchors = Array.from(document.querySelectorAll("a[href]"));
                                    const clickables = Array.from(document.querySelectorAll("a[href], a[onclick], button[onclick]"));
                                    const target = String(mngNo || "").trim().toLowerCase();
                                    let hasKey = 0;
                                    let hasId = 0;
                                    const compareLogs = [];
                                    for (const a of anchors) {
                                        const hrefRaw = String(a.getAttribute("href") || "");
                                        if (!hrefRaw) continue;
                                        const match = hrefRaw.match(/ctrtacctbookmngno\\s*=\\s*([^&]+)/i);
                                        if (!match || !match[1]) continue;
                                        hasKey += 1;
                                        const foundId = String(match[1]).trim().toLowerCase();
                                        compareLogs.push({
                                            source: "href",
                                            foundId,
                                            matched: foundId === target,
                                            sample: hrefRaw.slice(0, 220),
                                        });
                                        if (foundId === target) {
                                            hasId += 1;
                                            return {
                                                href: hrefRaw,
                                                clickIndex: -1,
                                                totalAnchors: anchors.length,
                                                totalClickables: clickables.length,
                                                keyAnchors: hasKey,
                                                idMatches: hasId,
                                                matchedBy: "href",
                                                compareLogs,
                                            };
                                        }
                                    }
                                    // href가 아닌 onclick 내부에 ID가 들어있는 케이스 대응
                                    for (let i = 0; i < clickables.length; i += 1) {
                                        const el = clickables[i];
                                        const onclickRaw = String(
                                            el.getAttribute("onclick") || (typeof el.onclick === "function" ? (el.onclick.toString() || "") : "")
                                        );
                                        if (!onclickRaw) continue;
                                        const m2 = onclickRaw.match(/ctrtacctbookmngno\\s*[=:]\\s*['"]?([^&'")\\s]+)/i);
                                        if (!m2 || !m2[1]) continue;
                                        hasKey += 1;
                                        const foundId2 = String(m2[1]).trim().toLowerCase();
                                        compareLogs.push({
                                            source: "onclick",
                                            foundId: foundId2,
                                            matched: foundId2 === target,
                                            sample: onclickRaw.slice(0, 220),
                                        });
                                        if (foundId2 === target) {
                                            hasId += 1;
                                            return {
                                                href: "",
                                                clickIndex: i,
                                                totalAnchors: anchors.length,
                                                totalClickables: clickables.length,
                                                keyAnchors: hasKey,
                                                idMatches: hasId,
                                                matchedBy: "onclick",
                                                compareLogs,
                                            };
                                        }
                                    }
                                    return {
                                        href: "",
                                        clickIndex: -1,
                                        totalAnchors: anchors.length,
                                        totalClickables: clickables.length,
                                        keyAnchors: hasKey,
                                        idMatches: hasId,
                                        matchedBy: "",
                                        compareLogs,
                                    };
                                }""",
                                mng_no,
                            )
                            try:
                                target_norm = (mng_no or "").strip().lower()
                                for row in ((scan_result or {}).get("compareLogs") or []):
                                    logger.info(
                                        "[chuncheon_contract] href 비교 | page=%s target=%s found=%s matched=%s source=%s sample=%s",
                                        page_idx,
                                        target_norm,
                                        str((row or {}).get("foundId") or ""),
                                        bool((row or {}).get("matched")),
                                        str((row or {}).get("source") or ""),
                                        str((row or {}).get("sample") or ""),
                                    )
                            except Exception:
                                pass
                            href = str((scan_result or {}).get("href") or "").strip()
                            click_raw = (scan_result or {}).get("clickIndex", -1)
                            try:
                                click_index = int(click_raw)
                            except Exception:
                                click_index = -1
                            if not href and click_index < 0:
                                try:
                                    logger.debug(
                                        "[chuncheon_contract] pageIndex=%s 미발견 | total_anchors=%s total_clickables=%s key_hits=%s id_matches=%s target=%s",
                                        page_idx,
                                        (scan_result or {}).get("totalAnchors", "n/a"),
                                        (scan_result or {}).get("totalClickables", "n/a"),
                                        (scan_result or {}).get("keyAnchors", "n/a"),
                                        (scan_result or {}).get("idMatches", "n/a"),
                                        mng_no,
                                    )
                                except Exception:
                                    pass
                            if href:
                                logger.info(
                                    "[chuncheon_contract] href 매칭 성공 | page=%s href=%s",
                                    page_idx,
                                    href,
                                )
                                found_href = href
                                break
                            if click_index >= 0:
                                try:
                                    logger.debug(
                                        "[chuncheon_contract] pageIndex=%s onclick 매칭 발견(clickIndex=%s)",
                                        page_idx,
                                        click_index,
                                    )
                                    async with page.expect_navigation(wait_until=wait, timeout=timeout_ms):
                                        clicked = await page.evaluate(
                                            """(idx) => {
                                                const nodes = Array.from(document.querySelectorAll("a[href], a[onclick], button[onclick]"));
                                                const n = Number(idx);
                                                if (!Number.isFinite(n) || n < 0 || n >= nodes.length) return false;
                                                const el = nodes[n];
                                                el.scrollIntoView({block: "center"});
                                                el.click();
                                                return true;
                                            }""",
                                            click_index,
                                        )
                                        if not clicked:
                                            raise RuntimeError("onclick target element not found")
                                    html = await page.content()
                                    if detail_html_looks_loaded(html):
                                        return html
                                except Exception as ex:
                                    logger.debug("[chuncheon_contract] onclick click fallback failed(page=%s): %s", page_idx, ex)
                        if found_href:
                            target_url = urljoin(list_url, found_href)
                            target_url = normalize_chuncheon_detail_url(target_url)
                            await page.goto(
                                target_url,
                                wait_until=wait,
                                timeout=timeout_ms,
                                referer=list_url,
                            )
                        else:
                            # 마지막 수단으로 기존 클릭 시도
                            loc_hi = page.locator(f"a[href*='ctrtAcctBookMngNo={mng_no}']")
                            loc_lo = page.locator(f"a[href*='ctrtacctbookmngno={mng_no}']")
                            n_hi = await loc_hi.count()
                            link = loc_hi.first if n_hi else loc_lo.first
                            await link.wait_for(state="visible", timeout=8000)
                            async with page.expect_navigation(wait_until=wait, timeout=timeout_ms):
                                await link.click(timeout=8000)
                    except Exception as ex:
                        logger.debug("[chuncheon_contract] same-context list->detail fallback failed: %s", ex)
                    await page.wait_for_timeout(detail_ms)
                    try:
                        await page.wait_for_selector(
                            DETAIL_CONTENT_SELECTORS,
                            state="visible",
                            timeout=8000,
                        )
                    except Exception:
                        pass
                    html = await page.content()
                    if detail_html_looks_loaded(html):
                        return html

            except Exception as ex:
                logger.debug("[chuncheon_contract] fetch attempt %s: %s", attempt + 1, ex)
            finally:
                try:
                    await context.close()
                except Exception:
                    pass
            await asyncio.sleep(0.35)

    # 최종 폴백: pageIndex 병렬 탐색으로 target 관리번호 링크를 찾아 상세 진입
    if mng_no:
        try:
            html2, _final_url = await render_page_via_playwright_hybrid_click(
                list_url=list_url,
                target_id=mng_no,
                wait_until=wait,
                list_wait_ms=max(250, step_ms),
                detail_wait_ms=max(600, detail_ms),
                wait_for_selector=DETAIL_CONTENT_SELECTORS,
                max_pages_to_search=max_pages_hybrid,
                parallel_pages=12,
                sequential_fallback=False,
            )
            if detail_html_looks_loaded(html2):
                return html2
        except Exception as ex:
            logger.debug("[chuncheon_contract] hybrid click fallback failed: %s", ex)
        # 2차(옵션) 폴백: 필요 시에만 순차 페이지 탐색 활성화 (기본 OFF)
        enable_seq = str(os.getenv("BOARD_CHUNCHEON_ENABLE_SEQ_FALLBACK", "0")).strip().lower() in ("1", "true", "yes", "on")
        if enable_seq:
            try:
                html3, _final_url2 = await render_page_via_playwright_hybrid_click(
                    list_url=list_url,
                    target_id=mng_no,
                    wait_until=wait,
                    list_wait_ms=max(250, step_ms),
                    detail_wait_ms=max(600, detail_ms),
                    wait_for_selector=DETAIL_CONTENT_SELECTORS,
                    max_pages_to_search=max_pages_hybrid_seq,
                    parallel_pages=12,
                    sequential_fallback=True,
                )
                if detail_html_looks_loaded(html3):
                    return html3
            except Exception as ex:
                logger.debug("[chuncheon_contract] hybrid sequential fallback failed: %s", ex)

    logger.warning(
        "[chuncheon_contract] HTML 수집 실패 | detail=%s | list=%s",
        (detail_url or "")[:100],
        (list_url or "")[:100],
    )
    return None


# ---------------------------------------------------------------------------
# 파싱 (BeautifulSoup 루트 → 계약 본문만)
# ---------------------------------------------------------------------------


@dataclass
class ChuncheonContractParseResult:
    title: str
    content_text: str
    content_html: str
    snippet: str


def parse_contract_detail_soup(soup, url: str) -> Optional[ChuncheonContractParseResult]:
    """계약 상세 DOM에서 표·본문 영역만 추출."""
    if not soup or not url:
        return None
    u = (url or "").lower()
    if "chuncheon.go.kr/contract/" not in u:
        return None
    if "/detail/" not in u and "ctrtacctbookmngno=" not in u:
        return None

    from backend.board.board_content_extractor import (
        _clean_preserve_newline,
        _collapse_ws,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    root = soup.select_one(".ctrtAcctBook")
    if not root:
        root = soup.select_one(".serch_result_wrap")
    if not root:
        root = soup.select_one(".detail_view")
    if not root:
        for tag in soup.find_all(True):
            cls = tag.get("class") or []
            cj = "".join(str(c).lower() for c in cls)
            if "ctrtacctbook" in cj.replace("_", ""):
                root = tag
                break
            if "serch_result_wrap" in cj or "serch_sub_result" in cj:
                root = tag
                break
    if not root:
        return None

    probe = _collapse_ws(root.get_text(" ", strip=True))
    if not probe or (len(probe) <= 24 and "정보없음" in probe):
        return None

    title = extract_contract_display_title(soup, url)

    parts: list[str] = []
    for tbl in root.find_all("table"):
        lines: list[str] = []
        for tr in tbl.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            texts = [
                _collapse_ws(c.get_text(" ", strip=True))
                for c in cells
                if (c.get_text(strip=True) or "").strip()
            ]
            if not texts:
                continue
            lines.append("  :  ".join(texts) if len(texts) > 1 else texts[0])
        if lines:
            parts.append("\n".join(lines))
    if parts:
        content_text = "\n\n".join(parts)
    else:
        content_text = _clean_preserve_newline(root.get_text("\n", strip=True))
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not (content_text or "").strip() or len(content_text.strip()) < 4:
        return None
    if _looks_like_chuncheon_contract_menu_noise(content_text):
        return None

    frag = soup.new_tag("div", attrs={"class": "chuncheon-contract-extract-wrap"})
    frag.append(copy.copy(root))
    content_html = _sanitize_html_fragment(frag).strip()
    snippet = _collapse_ws(content_text)[:200]

    return ChuncheonContractParseResult(
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def parse_contract_detail_html(html: str, url: str) -> Optional[ChuncheonContractParseResult]:
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return None
    return parse_contract_detail_soup(soup, url)
