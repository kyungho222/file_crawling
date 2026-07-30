"""
Anseong city board parsing helpers.

Anseong pages use a few different detail templates under the same host:
- library program reservation detail: table row label "신청명"
- BBS detail under library/festival/tourPortal/etc.: `.bod_view > h4`
- youth policy detail: `.bod_view > h4`
- recruitment notice detail: `.bod_view h4 > strong`
- theme tourist detail: `.tourAreaView/.tourAreaViewer .desc h4 > strong`
- lifelong learning lecture detail: `.learning_content .bod_title .bod_subject`
- existing education institution detail: `div.edu-detail-header h2.edu-title`
"""

from __future__ import annotations

import re
from datetime import datetime
from html import unescape
from typing import Any, Optional
from urllib.parse import urlparse


ANSEONG_TITLE_LABELS = (
    "신청명",
    "프로그램명",
    "강좌명",
    "강의명",
    "교육명",
    "행사명",
    "예약명",
    "제목",
    "글제목",
    "제목명",
)

ANSEONG_TITLE_SELECTORS = (
    "div.edu-detail-header h2.edu-title",
    ".learning_content .bod_title .bod_subject",
    ".learning_wrap .bod_title .bod_subject",
    ".bod_title .bod_subject",
    ".tourAreaViewer .desc h4 > strong",
    ".tourAreaViewer .desc h4 strong",
    ".tourAreaViewer h4 > strong",
    ".tourAreaView .desc h4 > strong",
    ".tourAreaView .desc h4 strong",
    ".tourAreaView h4 > strong",
    ".bod_view h4 > strong",
    ".bod_view h4 strong",
    ".bod_view > h4",
    "div.bod_view > h4",
    ".bod_view h4",
)

ANSEONG_NOISE_TITLES = {
    "공지사항 게시판 내용 보기",
    "게시판 내용 보기",
    "온라인강좌 신청확인",
    "상세보기",
    "목록",
    "본문",
}

ANSEONG_POSTED_DATE_LABELS = (
    "등록일",
    "작성일",
    "게시일",
    "공고일",
    "날짜",
    "일자",
    "등록일자",
    "작성일자",
    "게시일자",
    "공고일자",
    "작성일시",
    "등록일시",
)

ANSEONG_MODIFIED_DATE_LABELS = (
    "수정일",
    "최종수정일",
    "수정일자",
)


def _collapse_ws(text: Any) -> str:
    value = unescape(str(text or "")).replace("\xa0", " ")
    value = re.sub(r"[\t\r\n]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _norm_label(text: Any) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip()


def _clean_title(text: Any) -> str:
    value = _collapse_ws(text)
    value = value.strip("·|｜-–—:：[]「」『』<> ")
    return _collapse_ws(value)


def _parse_date_text(text: Any) -> Optional[datetime]:
    value = _collapse_ws(text)
    if not value:
        return None
    patterns = (
        r"(\d{4})\s*[-./]\s*(\d{1,2})\s*[-./]\s*(\d{1,2})",
        r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일",
        r"\b(\d{4})(\d{2})(\d{2})\b",
    )
    for pattern in patterns:
        match = re.search(pattern, value)
        if not match:
            continue
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except Exception:
            continue
    return None


def _is_noise_title(text: Any) -> bool:
    value = _clean_title(text)
    if not value:
        return True
    compact = _norm_label(value)
    if compact in {_norm_label(v) for v in ANSEONG_NOISE_TITLES}:
        return True
    if len(value) <= 1:
        return True
    if re.fullmatch(r"[\d\s\-_/.:]+", value):
        return True
    return False


def is_anseong_url(url: str) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        return host == "anseong.go.kr" or host.endswith(".anseong.go.kr")
    except Exception:
        return "anseong.go.kr" in (url or "").lower()


def is_anseong_library_program_view_url(url: str) -> bool:
    if not is_anseong_url(url):
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        path = (parsed.path or "").lower()
        return path.endswith("/library/activity/program/view.do")
    except Exception:
        return "/library/activity/program/view.do" in (url or "").lower()


def is_anseong_bbs_view_url(url: str) -> bool:
    if not is_anseong_url(url):
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        path = (parsed.path or "").lower()
        return path.endswith("/bbs/view.do") or "/bbs/view.do" in path
    except Exception:
        return "/bbs/view.do" in (url or "").lower()


def is_anseong_library_bbs_view_url(url: str) -> bool:
    return is_anseong_bbs_view_url(url)


def is_anseong_recruitment_notice_url(url: str) -> bool:
    if not is_anseong_url(url):
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        path = (parsed.path or "").lower()
        return path.endswith("/portal/recruitment/notice/view.do")
    except Exception:
        return "/portal/recruitment/notice/view.do" in (url or "").lower()


def is_anseong_portal_frame_view_url(url: str) -> bool:
    if not is_anseong_url(url):
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        path = (parsed.path or "").lower()
        return path.endswith("/portal/frame/view.do")
    except Exception:
        return "/portal/frame/view.do" in (url or "").lower()


def _label_value(root: Any, *labels: str) -> str:
    if root is None:
        return ""
    wanted = {_norm_label(label) for label in labels if label}
    try:
        rows = root.select("tr")
    except Exception:
        rows = []
    for tr in rows:
        try:
            cells = tr.find_all(["th", "td", "dt", "dd"], recursive=False)
        except Exception:
            cells = []
        if len(cells) < 2:
            try:
                cells = tr.find_all(["th", "td", "dt", "dd"])
            except Exception:
                cells = []
        if len(cells) < 2:
            continue
        for idx, cell in enumerate(cells[:-1]):
            label = _norm_label(cell.get_text(" ", strip=True))
            if label not in wanted:
                continue
            for nxt in cells[idx + 1 :]:
                title = _clean_title(nxt.get_text(" ", strip=True))
                if title and not _is_noise_title(title):
                    return title

    try:
        tags = root.find_all(["th", "dt", "label", "span"], limit=180)
    except Exception:
        tags = []
    for tag in tags:
        label = _norm_label(tag.get_text(" ", strip=True))
        if label not in wanted:
            continue
        try:
            sibling = tag.find_next_sibling(["td", "dd", "div", "span"])
        except Exception:
            sibling = None
        if sibling is None:
            continue
        title = _clean_title(sibling.get_text(" ", strip=True))
        if title and not _is_noise_title(title):
            return title
    return ""


def _date_label_value(root: Any, labels: tuple[str, ...]) -> Optional[datetime]:
    if root is None:
        return None
    wanted = {_norm_label(label) for label in labels if label}

    try:
        scope_nodes = root.select(".view_info, .bod_view .view_info, .learning_content .view_info")
    except Exception:
        scope_nodes = []
    scopes = list(scope_nodes or []) + [root]

    for scope in scopes:
        try:
            rows = scope.select("tr")
        except Exception:
            rows = []
        for tr in rows:
            try:
                cells = tr.find_all(["th", "td", "dt", "dd", "span", "li"], recursive=False)
            except Exception:
                cells = []
            if len(cells) < 2:
                try:
                    cells = tr.find_all(["th", "td", "dt", "dd", "span", "li"])
                except Exception:
                    cells = []
            if len(cells) < 2:
                continue
            for idx, cell in enumerate(cells[:-1]):
                label = _norm_label(cell.get_text(" ", strip=True))
                if label not in wanted:
                    continue
                for nxt in cells[idx + 1 :]:
                    dt = _parse_date_text(nxt.get_text(" ", strip=True))
                    if dt:
                        return dt

        try:
            tags = scope.find_all(["dt", "th", "strong", "span", "em", "li"], limit=220)
        except Exception:
            tags = []
        for tag in tags:
            label_text = tag.get_text(" ", strip=True)
            label = _norm_label(label_text)
            if label in wanted:
                try:
                    sibling = tag.find_next_sibling(["dd", "td", "span", "em", "div", "li"])
                except Exception:
                    sibling = None
                if sibling is not None:
                    dt = _parse_date_text(sibling.get_text(" ", strip=True))
                    if dt:
                        return dt
            if any(w in label for w in wanted):
                dt = _parse_date_text(label_text)
                if dt:
                    return dt
    return None


def _date_from_view_info(root: Any) -> Optional[datetime]:
    if root is None:
        return None
    try:
        nodes = root.select(
            ".view_info, .viewInfo, .view_info2, .viewInfo2, "
            ".bod_view .view_info, .learning_content .view_info, "
            ".board_view_info, .bbs_view_info, .post_info, .post-info, "
            ".boardInfo, .board_info, .info, .infor, .view_top, .view-head"
        )
    except Exception:
        nodes = []
    for node in nodes or []:
        try:
            text = _collapse_ws(node.get_text(" ", strip=True))
        except Exception:
            text = ""
        if not text:
            continue
        compact = _norm_label(text)
        posted_markers = tuple(_norm_label(v) for v in ANSEONG_POSTED_DATE_LABELS)
        if any(marker in compact for marker in posted_markers):
            dt = _parse_date_text(text)
            if dt:
                return dt
    return None


def _date_from_frame_inputs(root: Any) -> Optional[datetime]:
    if root is None:
        return None
    date_key_re = re.compile(
        r"(?:reg|rgs|write|writ|post|date|dt|create|cret|insert|inst|bbs).*",
        re.IGNORECASE,
    )
    try:
        inputs = root.find_all(["input", "meta"], limit=260)
    except Exception:
        inputs = []
    for tag in inputs:
        try:
            attrs = " ".join(
                str(tag.get(k) or "")
                for k in ("id", "name", "property", "itemprop", "class")
            )
            value = str(tag.get("value") or tag.get("content") or "")
        except Exception:
            attrs = ""
            value = ""
        if not value or not date_key_re.search(attrs):
            continue
        dt = _parse_date_text(value)
        if dt:
            return dt

    try:
        scripts = root.find_all("script", limit=80)
    except Exception:
        scripts = []
    script_patterns = (
        r"(?:reg(?:ist)?Date|regDt|writeDate|writeDt|postDate|postDt|createDate|createDt|bbsDt)\s*[:=]\s*['\"]([^'\"]+)['\"]",
        r"['\"](?:reg(?:ist)?Date|regDt|writeDate|writeDt|postDate|postDt|createDate|createDt|bbsDt)['\"]\s*:\s*['\"]([^'\"]+)['\"]",
    )
    for script in scripts:
        try:
            text = script.string or script.get_text(" ", strip=False) or ""
        except Exception:
            text = ""
        if not text:
            continue
        for pattern in script_patterns:
            for raw in re.findall(pattern, text, flags=re.IGNORECASE):
                dt = _parse_date_text(raw)
                if dt:
                    return dt
    return None


def _date_from_meta_only_containers(root: Any) -> Optional[datetime]:
    if root is None:
        return None
    try:
        nodes = root.select(
            ".view_info, .viewInfo, .board_view_info, .bbs_view_info, "
            ".post_info, .post-info, .boardInfo, .board_info, .infor, "
            ".view_top, .view-head, .viewHeader, .view_header"
        )
    except Exception:
        nodes = []
    for node in nodes or []:
        try:
            text = _collapse_ws(node.get_text(" ", strip=True))
        except Exception:
            text = ""
        if not text:
            continue
        # Avoid large content blocks; this fallback is only for compact metadata rows.
        if len(text) > 260:
            continue
        lowered = text.lower()
        if any(skip in lowered for skip in ("copyright", "all rights", "주소", "footer")):
            continue
        dt = _parse_date_text(text)
        if dt:
            return dt
    return None


def _approx_recruitment_notice_date(root: Any) -> Optional[datetime]:
    if root is None:
        return None
    try:
        text = _collapse_ws(root.get_text(" ", strip=True))
    except Exception:
        text = ""
    if not text:
        return None

    # Prefer a full date close to an announcement label if present.
    for marker in ("공고일", "공고일자", "공고기간", "공고 기간"):
        compact_marker = _norm_label(marker)
        compact_text = _norm_label(text)
        pos = compact_text.find(compact_marker)
        if pos < 0:
            continue
        # Use original text chunk generously; compact offsets are not byte offsets,
        # so find the visible marker separately.
        visible_pos = text.find(marker.replace(" ", ""))
        if visible_pos < 0:
            visible_pos = text.find(marker)
        chunk = text[max(0, visible_pos) : visible_pos + 220] if visible_pos >= 0 else text
        dt = _parse_date_text(chunk)
        if dt:
            return dt

    # Many Anseong recruitment details expose only "제2024-1234호".
    # Use the notice-number year as a coarse registration-date estimate.
    for pattern in (
        r"제\s*(20\d{2})\s*[-–]\s*\d+\s*호",
        r"(?:채용)?공고번호\s*[:：]?\s*[^0-9]{0,20}(20\d{2})\s*[-–]\s*\d+",
    ):
        match = re.search(pattern, text)
        if match:
            try:
                return datetime(int(match.group(1)), 1, 1)
            except Exception:
                pass

    # Last resort for recruitment pages: if only application/recruitment periods
    # exist, use the first visible full-date year as year-level approximation.
    year_matches = []
    for pattern in (
        r"(\d{4})\s*[-./]\s*\d{1,2}\s*[-./]\s*\d{1,2}",
        r"(\d{4})년\s*\d{1,2}월\s*\d{1,2}일",
    ):
        for match in re.finditer(pattern, text):
            try:
                year_matches.append(int(match.group(1)))
            except Exception:
                pass
    if year_matches:
        return datetime(min(year_matches), 1, 1)
    return None


def extract_anseong_title(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_anseong_url(url)):
        return ""

    if is_anseong_library_program_view_url(url):
        title = _label_value(soup, *ANSEONG_TITLE_LABELS)
        if title:
            return title

    for selector in ANSEONG_TITLE_SELECTORS:
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        title = _clean_title(node.get_text(" ", strip=True)) if node is not None else ""
        if title and not _is_noise_title(title):
            return title

    title = _label_value(soup, *ANSEONG_TITLE_LABELS)
    if title:
        return title

    return ""


def extract_anseong_reg_date(soup: Any, *, url: str = "") -> Optional[datetime]:
    if soup is None or (url and not is_anseong_url(url)):
        return None

    frame_view = is_anseong_portal_frame_view_url(url)

    if frame_view:
        input_date = _date_from_frame_inputs(soup)
        if input_date:
            return input_date

    posted = _date_label_value(soup, ANSEONG_POSTED_DATE_LABELS)
    if posted:
        return posted

    view_info_date = _date_from_view_info(soup)
    if view_info_date:
        return view_info_date

    if frame_view:
        meta_only_date = _date_from_meta_only_containers(soup)
        if meta_only_date:
            return meta_only_date

    modified = _date_label_value(soup, ANSEONG_MODIFIED_DATE_LABELS)
    if modified:
        return modified

    if is_anseong_recruitment_notice_url(url):
        approx = _approx_recruitment_notice_date(soup)
        if approx:
            return approx

    return None
