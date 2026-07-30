"""
강남구 계열 사이트(*.gangnam.go.kr) 전용 제목 추출·URL 판별.

포함 예: 본청(www.gangnam.go.kr), 강남구보건소(health.gangnam.go.kr),
의료관광(medicaltour.gangnam.go.kr), 스마트복지관(bokji.gangnam.go.kr) 등.

- 의료관광 공지 상세: `.faq_detail_wrap h5.faq_title` (`medicaltour.gangnam.go.kr/.../notice_*/view.do`)
- 의료관광 병원 상세: `colid='MEDICAL_NM'` 병원명
- 스마트복지관 게시 상세: `.board-content-detail-info h4` (`bokji.gangnam.go.kr/board/.../view.do`)
- 보건소 통합예약 상세: `p.calendar_info_tit` (`.../reservation/detailView.do?ptrUnqNo=...`)
- 보건소 등 공지 상세: `.com-post-hd-01 p.title` (게시 헤더 «제목» 주석 인접)
- 본청 주민센터 게시 상세: `.bbs-view_head p.title` (`/center/board/…/view.do`)
- 본청 열린구청장실 등 게시 상세: `article.bbs-view header.top h3.title` (`/leader/board/…/view.do`)
- 일반 게시판: `.board-view-title`, `.post-title`, `h3.tit` 등
- 본청 콘텐츠형 상세(`/contents/…`): `section.sub-header h2.sub-title`(텍스트·img) 또는
  `og:title`/`<title>` 브레드크럼 마지막 구간(정적 HTML에 서브헤더가 없을 때)
- 본청 온라인신청 상세: `h3.con-title`, 본문 `#contents-wrap` (`/apply/.../view.do`)
- 폴백: 컨텐츠 영역 헤더, `og:title`(사이트·메뉴명 노이즈 제외)

워크플로·extract_board_post는 `is_gangnam_family_url`로 분기한 뒤
`extract_gangnam_board_title`만 호출하는 것을 권장한다.
"""

from __future__ import annotations

import re
from datetime import datetime
from html import unescape
from typing import Any, Optional
from urllib.parse import unquote, urlparse


def is_gangnam_health_url(url: str) -> bool:
    """강남구보건소(health.gangnam.go.kr) 상세 등."""
    if not url:
        return False
    try:
        raw = (url or "").strip()
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host == "health.gangnam.go.kr"
    except Exception:
        return "health.gangnam.go.kr" in (url or "").lower()


def strip_gangnam_health_satisfaction_blocks(soup: Any) -> None:
    """
    health.gangnam.go.kr 게시 상세 하단의 페이지 만족도 조사·투표·결과 UI 제거.
    (본문 #content에 com-post-content와 형제로 붙는 div.research, #researchRusult)
    """
    if soup is None:
        return
    for sel in (".research", "#researchRusult"):
        try:
            for tag in soup.select(sel):
                try:
                    tag.decompose()
                except Exception:
                    pass
        except Exception:
            pass


def is_gangnam_family_url(url: str) -> bool:
    """
    강남구 공식 도메인 패밀리 여부.

    `gangnam.go.kr` 및 모든 `*.gangnam.go.kr` 호스트(보건소·의료관광·본청 등).
    스킴 없는 문자열은 부분 문자열로도 판별한다.
    """
    if not url:
        return False
    raw = (url or "").strip()
    low = raw.lower()
    if "gangnam.go.kr" not in low:
        return False
    try:
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        if host:
            return host == "gangnam.go.kr" or host.endswith(".gangnam.go.kr")
    except Exception:
        return False
    # 호스트 없음(상대 경로·스킴 생략 등): 문자열에 도메인이 포함된 경우만 허용
    return True


def _is_gangnam_health_reservation_detail_url(url: Optional[str]) -> bool:
    """보건소 통합예약 프로그램 상세(detailView.do). 절대·상대 URL 모두."""
    if not url:
        return False
    raw = url.strip()
    low = raw.lower()
    pathish = low.split("?")[0].split("#")[0]
    if "detailview.do" not in pathish or "reservation" not in pathish:
        return False
    if is_gangnam_health_url(raw):
        return True
    if raw.startswith("/") and "/web/service/reservation/" in pathish:
        return True
    return False


def _is_gangnam_medicaltour_notice_view_url(url: Optional[str]) -> bool:
    """강남의료관광 공지·갤러리 등 상세(view.do). 경로에 notice 포함."""
    if not url:
        return False
    low = (url or "").lower()
    if "medicaltour.gangnam.go.kr" not in low:
        return False
    if "view.do" not in low:
        return False
    path = (urlparse(low).path or "").lower()
    return "/notice" in path or "notice_" in path


def _is_gangnam_bokji_board_view_url(url: Optional[str]) -> bool:
    """강남구 스마트복지관(bokji) 게시판 상세 view.do."""
    if not url:
        return False
    low = (url or "").lower()
    if "bokji.gangnam.go.kr" not in low:
        return False
    return "/board/" in low and "view.do" in low


def _gangnam_main_portal_host(netloc: str) -> bool:
    """본청 www / 루트 호스트만(서브도메인 제외)."""
    h = (netloc or "").lower().split("@")[-1].split(":")[0]
    if h.startswith("www."):
        h = h[4:]
    return h == "gangnam.go.kr"


def _is_gangnam_main_contents_path_url(url: Optional[str]) -> bool:
    """본청 정적 콘텐츠 상세 등 경로에 /contents/ 포함 (view.do 등)."""
    if not url:
        return False
    try:
        raw = (url or "").strip()
        p = urlparse(raw if "://" in raw else f"https://{raw}")
        if not _gangnam_main_portal_host(p.netloc or ""):
            return False
        return "/contents/" in (p.path or "").lower()
    except Exception:
        low = (url or "").lower()
        return "gangnam.go.kr" in low and "/contents/" in low


def _is_gangnam_main_center_board_view_url(url: Optional[str]) -> bool:
    """본청 주민센터(동) 게시판 상세: /center/board/…/view.do."""
    if not url:
        return False
    try:
        raw = (url or "").strip()
        p = urlparse(raw if "://" in raw else f"https://{raw}")
        if not _gangnam_main_portal_host(p.netloc or ""):
            return False
        path_l = (p.path or "").lower()
        return "/center/board/" in path_l and "view.do" in path_l
    except Exception:
        low = (url or "").lower()
        return "gangnam.go.kr" in low and "/center/board/" in low and "view.do" in low


def _is_gangnam_main_leader_board_view_url(url: Optional[str]) -> bool:
    """본청 구청장실·리더 게시판 상세: /leader/board/…/view.do."""
    if not url:
        return False
    try:
        raw = (url or "").strip()
        p = urlparse(raw if "://" in raw else f"https://{raw}")
        if not _gangnam_main_portal_host(p.netloc or ""):
            return False
        path_l = (p.path or "").lower()
        return "/leader/board/" in path_l and "view.do" in path_l
    except Exception:
        low = (url or "").lower()
        return "gangnam.go.kr" in low and "/leader/board/" in low and "view.do" in low


def is_gangnam_main_board_view_url(url: Optional[str]) -> bool:
    """
    강남구청 본청 일반 게시판 상세: /board/{boardId}/{postId}/view.do.

    예: https://www.gangnam.go.kr/board/B_000001/1076483/view.do?mid=...
    """
    if not url:
        return False
    try:
        raw = (url or "").strip()
        p = urlparse(raw if "://" in raw else f"https://{raw}")
        if not _gangnam_main_portal_host(p.netloc or ""):
            return False
        path = (p.path or "").lower()
        return path.startswith("/board/") and "view.do" in path
    except Exception:
        low = (url or "").lower()
        return "gangnam.go.kr" in low and "/board/" in low and "view.do" in low


def is_gangnam_main_apply_view_url(url: Optional[str]) -> bool:
    """
    강남구청 본청 온라인 신청·안내 상세 (/apply/.../view.do).
    예: https://www.gangnam.go.kr/apply/estate_tax/newest/view.do?mid=...
    """
    if not url:
        return False
    try:
        raw = (url or "").strip()
        p = urlparse(raw if "://" in raw else f"https://{raw}")
        if not _gangnam_main_portal_host(p.netloc or ""):
            return False
        path = (p.path or "").lower()
        return "/apply/" in path and "view.do" in path
    except Exception:
        low = (url or "").lower()
        return "www.gangnam.go.kr" in low and "/apply/" in low and "view.do" in low


def is_gangnam_main_office_board_view_url(url: Optional[str]) -> bool:
    """
    본청 하위 기관(/office/...) 게시판 상세: .../board/.../view.do (강남문화재단 gfac 단원·공연 안내 등).

    예: https://www.gangnam.go.kr/office/gfac/board/gfac_artmember02/32/view.do?mid=...
    """
    if not url:
        return False
    try:
        raw = (url or "").strip()
        p = urlparse(raw if "://" in raw else f"https://{raw}")
        if not _gangnam_main_portal_host(p.netloc or ""):
            return False
        path = (p.path or "").lower()
        return "/office/" in path and "/board/" in path and "view.do" in path
    except Exception:
        low = (url or "").lower()
        return "gangnam.go.kr" in low and "/office/" in low and "/board/" in low and "view.do" in low


def extract_gangnam_office_board_reg_date(soup: Any) -> Optional[datetime]:
    """
    /office/.../board/.../view.do 상세의 게시일(등록일).

    마크업은 `.bbs-view .post-info` 우측에 `<span>YYYY-MM-DD</span>`와 조회수가 붙어 있고
    '등록일' 라벨(th/dt)이 없어 공통 라벨 기반 추출이 실패하는 경우가 많다.
    """
    if soup is None:
        return None
    from backend.shared.date_utils import parse_date

    try:
        pinfo = soup.select_one(".bbs-view .post-info")
        if not pinfo:
            return None
        col = pinfo.select_one(".text-right") or pinfo
        for sp in col.find_all("span"):
            raw = (sp.get_text(strip=True) or "").strip()
            if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", raw):
                parsed = parse_date(raw)
                if parsed:
                    return parsed
        for sp in pinfo.find_all("span"):
            raw = (sp.get_text(strip=True) or "").strip()
            if re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", raw):
                parsed = parse_date(raw)
                if parsed:
                    return parsed
    except Exception:
        return None
    return None


def strip_gangnam_main_apply_noise(soup: Any) -> None:
    """본청 apply 상세 하단 만족도(con-poll)·카카오 채널 안내 등 본문 외 블록 제거."""
    if soup is None:
        return
    for sel in (".con-poll", ".kakaochannel_area"):
        try:
            for tag in soup.select(sel):
                try:
                    tag.decompose()
                except Exception:
                    pass
        except Exception:
            pass


def gangnam_main_apply_content_selector_hint(url: Optional[str]) -> Optional[str]:
    """extract_board_post 본문 루트 힌트. 해당 URL일 때만 #contents-wrap."""
    if url and is_gangnam_main_apply_view_url(url):
        return "#contents-wrap"
    return None


def gangnam_main_board_content_selector_hint(url: Optional[str]) -> Optional[str]:
    """extract_board_post 본문 루트 힌트. 본청 일반 게시판은 .bbs-view 내부만 사용."""
    if url and is_gangnam_main_board_view_url(url):
        return ".bbs-view"
    return None


def is_gangnam_error_page(soup: Any) -> bool:
    """강남구청 오류 안내 페이지를 실제 게시글로 저장하지 않도록 판별."""
    if soup is None:
        return False
    try:
        error_wrap = soup.select_one("#error-wrap")
        title = error_wrap.select_one("p.title") if error_wrap else soup.select_one("#error-wrap p.title")
        title_text = re.sub(r"\s+", " ", (title.get_text(" ", strip=True) if title else "")).strip()
        body_text = re.sub(r"\s+", " ", (soup.get_text(" ", strip=True) or "")).strip()
        return (
            "정상적인 접근이 아닙니다" in title_text
            or (
                "정상적인 접근이 아닙니다" in body_text
                and "입력하신 페이지 주소가 정확한지" in body_text
            )
        )
    except Exception:
        return False


def _gangnam_title_is_satisfaction_noise(text: str) -> bool:
    """본문/폴백에서 자주 잡히는 페이지 만족도 조사 문구."""
    t = (text or "").strip()
    if not t:
        return True
    return "만족하십니까" in t or "만족도 조사" in t


def _gangnam_extract_breadcrumb_tail_from_meta(soup: Any, norm_fn) -> str:
    """
    og:title / <title> 이 '강남구청 > … > 화면명' 형태일 때 마지막 세그먼트.
    requests 정적 HTML에는 section.sub-header 가 없고 메타만 있는 경우가 많다.
    """
    if soup is None:
        return ""
    raw_sources = []
    og = soup.select_one("meta[property='og:title']")
    if og and og.get("content"):
        raw_sources.append(og["content"])
    tit = soup.find("title")
    if tit:
        t = (tit.string or tit.get_text() or "").strip()
        if t:
            raw_sources.append(t)
    seen = set()
    for raw in raw_sources:
        val = norm_fn(unescape(raw))
        if not val or val in seen or ">" not in val:
            continue
        seen.add(val)
        tail = norm_fn(val.split(">")[-1])
        if len(tail) < 2:
            continue
        if _gangnam_title_is_satisfaction_noise(tail):
            continue
        if tail in ("강남구청", "메인", "Main", "main"):
            continue
        return tail
    return ""


def _gangnam_extract_sub_header_sub_title(soup: Any, norm_fn) -> str:
    """
    본청 레이아웃: section.sub-header > h2.sub-title.
    일부 메뉴는 제목이 텍스트가 아니라 이미지(alt/title/src 파일명)로만 제공된다.
    """
    if soup is None:
        return ""
    h2 = soup.select_one("section.sub-header h2.sub-title")
    if not h2:
        return ""
    for img in h2.select("img"):
        alt = (img.get("alt") or "").strip()
        if alt and len(alt) >= 2 and alt.lower() not in ("image", "이미지", "사진", "photo"):
            t = norm_fn(alt)
            if t and not _gangnam_title_is_satisfaction_noise(t):
                return t
        tit = (img.get("title") or "").strip()
        if tit and len(tit) >= 2:
            t = norm_fn(tit)
            if t and not _gangnam_title_is_satisfaction_noise(t):
                return t
        src = (img.get("src") or "").strip()
        if src:
            path = src.split("?")[0].split("/")[-1]
            stem = path.rsplit(".", 1)[0] if path else ""
            stem = unquote(stem).replace("_", " ").strip()
            if stem and len(stem) >= 2:
                t = norm_fn(stem)
                if t and not _gangnam_title_is_satisfaction_noise(t):
                    return t
    raw_txt = h2.get_text(strip=True)
    if raw_txt:
        t = norm_fn(raw_txt)
        if t and not _gangnam_title_is_satisfaction_noise(t):
            return t
    return ""


def gangnam_main_apply_footer_department(soup: Any) -> Optional[str]:
    """본청 apply 상세 하단 담당부서(ul.bottom_info 등; <footer> 밖에 있는 케이스 포함)."""
    if soup is None:
        return None
    try:
        for li in soup.select("ul.bottom_info li"):
            tit = li.select_one("span.tit.team")
            dep = li.select_one("span.dep-name")
            if not tit or not dep:
                continue
            lab = re.sub(r"\s+", " ", (tit.get_text(" ", strip=True) or "")).strip()
            if "담당" not in lab or "부서" not in lab:
                continue
            t = re.sub(r"\s+", " ", (dep.get_text(" ", strip=True) or "")).strip()
            if t and len(t) <= 80:
                return t
        el = soup.select_one(
            "footer .dep-name, #main-footer-re24 .dep-name, .main-footer-re24 .dep-name, .main-footer-mo-re24 .dep-name"
        )
        if el:
            t = re.sub(r"\s+", " ", (el.get_text(" ", strip=True) or "")).strip()
            if t and len(t) <= 80:
                return t
    except Exception:
        return None
    return None


def extract_gangnam_board_title(soup: Any, url: Optional[str] = None) -> str:
    """
    강남 계열 상세·게시 HTML에서 제목 추출.

    Args:
        soup: BeautifulSoup 문서 루트
        url: 요청 URL(선택). 향후 경로별 분기용.

    Returns:
        정규화된 제목. 없으면 "제목 없음".
    """
    if soup is None:
        return "제목 없음"

    def _norm(text: str) -> str:
        t = unescape((text or "").strip())
        t = re.sub(r"\s+", " ", t).strip()
        return t

    # 1) 의료관광 공지/커뮤니티 상세: FAQ형 게시 제목 (영·다국어 공지)
    if url and _is_gangnam_medicaltour_notice_view_url(url):
        faq_tit = soup.select_one(".faq_detail_wrap h5.faq_title, ul.detail_content h5.faq_title, h5.faq_title")
        if faq_tit and faq_tit.get_text(strip=True):
            return _norm(faq_tit.get_text(strip=True))

    # 2) 스마트복지관(bokji): 상세 본문 헤더 h4 (섹션 h3 '복지소식'·주변 '이미지' alt와 구분)
    if url and _is_gangnam_bokji_board_view_url(url):
        bokji_tit = soup.select_one(
            ".boardDetail-wrap .board-content-detail-info h4, "
            ".board-content.boardDetail-wrap .board-content-detail-info h4, "
            ".board-content-detail-info h4"
        )
        if bokji_tit and bokji_tit.get_text(strip=True):
            t = _norm(bokji_tit.get_text(strip=True))
            if t and t not in ("이미지", "image", "Image"):
                return t

    # 2b) 본청 /apply/.../view.do : 프로그램 헤드라인(p.apply-title) → 서브헤더 h3.con-title(메뉴명)
    if url and is_gangnam_main_apply_view_url(url):
        # 강남문화재단(gfac) 등 행사/공연 상세 제목 (.event-view .event-title)
        # 기능: gfac 상세 페이지의 제목 요소를 1순위로 추출 (1줄 주석)
        gfac_tit = soup.select_one(".event-view .event-title, .event-title")
        if gfac_tit and gfac_tit.get_text(strip=True):
            return _norm(gfac_tit.get_text(strip=True))

        prog_tit = soup.select_one("#contents-wrap p.apply-title, .apply-info-cont p.apply-title, p.apply-title")
        if prog_tit and prog_tit.get_text(strip=True):
            return _norm(prog_tit.get_text(strip=True))
        apply_tit = soup.select_one("h3.con-title")
        if apply_tit and apply_tit.get_text(strip=True):
            return _norm(apply_tit.get_text(strip=True))

    # 3) 의료관광 병원 상세: 병원명
    gn_medical = soup.select_one(".depth2_content_title_wrap02 h5.editableItem[colid='MEDICAL_NM']")
    if gn_medical and gn_medical.get_text(strip=True):
        return _norm(gn_medical.get_text(strip=True))

    # 4) 강남구보건소 통합예약 상세: .calendar_info_h 내 프로그램명
    if url and _is_gangnam_health_reservation_detail_url(url):
        cal_tit = soup.select_one("p.calendar_info_tit, .calendar_info .calendar_info_tit")
        if cal_tit and cal_tit.get_text(strip=True):
            return _norm(cal_tit.get_text(strip=True))

    # 5) 강남구보건소 등: 게시 상세 헤더 `<!-- 제목 -->` 다음 `p.title`
    #    (참고: https://health.gangnam.go.kr/web/community/notice/.../view.do )
    health_title = soup.select_one(".boardDetailViewBox .com-post-hd-01 p.title")
    if health_title and health_title.get_text(strip=True):
        return _norm(health_title.get_text(strip=True))
    health_title = soup.select_one(".com-post-hd-01 p.title")
    if health_title and health_title.get_text(strip=True):
        return _norm(health_title.get_text(strip=True))

    # 5a) 본청 일반 게시판 상세 (/board/.../view.do): .bbs-view 안의 실제 게시글 제목
    if url and is_gangnam_main_board_view_url(url):
        main_board_tit = soup.select_one(".bbs-view .post-title h4, .bbs-view .bbs-view_head p.title")
        if main_board_tit and main_board_tit.get_text(strip=True):
            t = _norm(main_board_tit.get_text(strip=True))
            if t and not _gangnam_title_is_satisfaction_noise(t):
                return t

    # 5b) 본청 주민센터 게시 상세 (.bbs-view > .bbs-view_head > p.title)
    if url and _is_gangnam_main_center_board_view_url(url):
        cv_tit = soup.select_one(".bbs-view .bbs-view_head p.title, .bbs-view_head p.title")
        if cv_tit and cv_tit.get_text(strip=True):
            t = _norm(cv_tit.get_text(strip=True))
            if t and not _gangnam_title_is_satisfaction_noise(t):
                return t

    # 5c) 본청 /leader/board/… (열린구청장실 생생포토 등): article.bbs-view > header.top > h3.title
    if url and _is_gangnam_main_leader_board_view_url(url):
        ld_tit = soup.select_one("article.bbs-view header.top h3.title, .bbs-view header.top h3.title")
        if ld_tit and ld_tit.get_text(strip=True):
            t = _norm(ld_tit.get_text(strip=True))
            if t and not _gangnam_title_is_satisfaction_noise(t):
                return t

    # 6) 일반 게시판(본청 등)
    gn_board = soup.select_one(".board-view-title, .post-title, .view-title, h3.tit")
    if gn_board and gn_board.get_text(strip=True):
        return _norm(gn_board.get_text(strip=True))

    # 6b) 본청(www): 서브헤더 h2.sub-title — /contents/… 등에서 .content_wrap h2보다 앞서야 하며,
    #     제목이 이미지인 경우 alt/title/src 기반으로 복구한다.
    if url:
        try:
            raw_u = (url or "").strip()
            pu = urlparse(raw_u if "://" in raw_u else f"https://{raw_u}")
            if _gangnam_main_portal_host(pu.netloc or ""):
                sub_t = _gangnam_extract_sub_header_sub_title(soup, _norm)
                if sub_t:
                    return sub_t
        except Exception:
            pass

    # 6c) 본청 /contents/… : 정적 HTML은 h2.sub-title 없이 메타 브레드크럼만 있는 경우가 많음
    if url and _is_gangnam_main_contents_path_url(url):
        bc = _gangnam_extract_breadcrumb_tail_from_meta(soup, _norm)
        if bc:
            return bc

    # 7) 구조적 폴백 (.content_wrap h2는 만족도 블록 제목을 잡는 경우가 있어 노이즈 제외)
    gn_fallback = soup.select_one(".container .editableItem, .content_wrap h2, .sub_title_area h3")
    if gn_fallback and gn_fallback.get_text(strip=True):
        cand = _norm(gn_fallback.get_text(strip=True))
        if cand and not _gangnam_title_is_satisfaction_noise(cand):
            return cand

    # 8) og:title
    og = soup.select_one("meta[property='og:title']")
    if og and og.get("content"):
        val = _norm(og["content"])
        if val.lower() in ("이미지", "image"):
            val = ""
        if val and _gangnam_title_is_satisfaction_noise(val):
            val = ""
        # 본청: "강남구청 > … > 메뉴명" → 마지막 세그먼트를 제목으로
        if url and is_gangnam_main_apply_view_url(url) and ">" in val:
            tail = _norm(val.split(">")[-1])
            if tail and len(tail) >= 2:
                return tail
        noise_titles = (
            "강남구청",
            "Gangnam Medical Tour",
            "Gangnam Medi Tour for Better Life",
            "강남구 의료관광",
            "메인 | 강남구청",
            "강남구보건소",
            "GANGNAM PUBLIC HEALTH",
            "공지사항",
            "통합예약",
            "Notice",
            "Community Notice",
            "Gallery",
            "Inquiry",
        )
        if val and not any(noise in val for noise in noise_titles):
            return val
        # 복지관: "글제목 | 복지플랫폼 > …" 형태
        if val and "|" in val and "bokji" in (url or "").lower():
            head = _norm(val.split("|", 1)[0])
            if head and len(head) >= 3:
                return head

    return "제목 없음"
