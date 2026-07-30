"""
평택시청(pyeongtaek.go.kr) 전용 제목 추출 로직.

현재 지원 케이스:
- 일반 게시판 상세: `/board/post/view.do`
- 새올 고시공고 상세: `/saeol/gosi/view.do`
- 주민참여예산 투표 상세: `/vote/result.do`

공통적으로 `h4.blind` 같은 안내용 헤더는 제외하고, 실제 본문 카드 내부의 제목만 선택한다.
"""

from __future__ import annotations

import re
from typing import Any, Iterable, Optional
from urllib.parse import urlparse


def _collapse_ws(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _host_and_path(url: str) -> tuple[str, str]:
    if not url:
        return "", ""
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        path = (parsed.path or "").lower()
        return host, path
    except Exception:
        low = (url or "").lower()
        return ("pyeongtaek.go.kr" if "pyeongtaek.go.kr" in low else "", low)


def is_pyeongtaek_city_url(url: str) -> bool:
    host, _path = _host_and_path(url)
    return host == "pyeongtaek.go.kr"


def is_pyeongtaek_board_post_view_url(url: str) -> bool:
    host, path = _host_and_path(url)
    return host == "pyeongtaek.go.kr" and path.endswith("/board/post/view.do")


def is_pyeongtaek_gosi_view_url(url: str) -> bool:
    host, path = _host_and_path(url)
    return host == "pyeongtaek.go.kr" and path.endswith("/saeol/gosi/view.do")


def is_pyeongtaek_vote_result_url(url: str) -> bool:
    host, path = _host_and_path(url)
    return host == "pyeongtaek.go.kr" and path.endswith("/vote/result.do")


def is_pyeongtaek_recruit_view_url(url: str) -> bool:
    host, path = _host_and_path(url)
    return host == "pyeongtaek.go.kr" and path.endswith("/recruitanm/view.do")


def pyeongtaek_content_selector_hint(url: str) -> str:
    if is_pyeongtaek_vote_result_url(url):
        return "#conts .poll_view"
    if is_pyeongtaek_recruit_view_url(url):
        return "#conts table.tableSt_view, #conts table.tbl_openview"
    if is_pyeongtaek_board_post_view_url(url) or is_pyeongtaek_gosi_view_url(url):
        return "#detailForm .view_cont, #view_div .view_cont, #conts .view_cont"
    if is_pyeongtaek_city_url(url):
        return "#conts .view_cont, #conts .poll_view"
    return ""


def _has_hidden_marker(tag: Any) -> bool:
    if tag is None:
        return True
    try:
        classes = [str(v or "").strip().lower() for v in (tag.get("class") or [])]
    except Exception:
        classes = []
    if any(cls in {"blind", "skip", "hidden"} for cls in classes):
        return True
    try:
        style = str(tag.get("style") or "").lower()
    except Exception:
        style = ""
    return "display:none" in style or "visibility:hidden" in style


def _is_noise_title(text: str) -> bool:
    txt = _collapse_ws(text)
    if not txt:
        return True
    compact = txt.replace(" ", "")
    if compact in {
        "고시공고상세보기",
        "게시글상세보기",
        "상세보기",
        "고시공고",
        "공지사항",
        "목록",
    }:
        return True
    if txt.count(">") >= 2:
        return True
    return False


def _candidate_text(tag: Any) -> str:
    if tag is None or _has_hidden_marker(tag):
        return ""
    try:
        text = tag.get_text(" ", strip=True)
    except Exception:
        text = ""
    text = _collapse_ws(text)
    if _is_noise_title(text):
        return ""
    return text


def _is_pyeongtaek_noise_text(text: str) -> bool:
    txt = _collapse_ws(text)
    if not txt:
        return False
    if any(token in txt for token in ("페이지 만족도", "만족하십니까", "매우만족", "매우불만")):
        return True
    if any(token in txt for token in ('공공누리', '출처표시', "본 공공저작물은")):
        return True
    return False


def strip_pyeongtaek_noise(soup: Any, *, url: str = "") -> None:
    if soup is None:
        return
    if url and not is_pyeongtaek_city_url(url):
        return

    selectors = (
        ".open_license",
        ".pageInfo",
        ".research",
        "#researchForm",
        ".opinion_wrap",
    )
    for selector in selectors:
        try:
            for tag in soup.select(selector):
                try:
                    tag.decompose()
                except Exception:
                    pass
        except Exception:
            pass

    try:
        candidates = soup.find_all(["div", "section", "form", "fieldset", "p", "ul", "ol", "dl", "table"])
    except Exception:
        candidates = []
    for tag in candidates:
        try:
            if tag is None or getattr(tag, "name", "") in {"html", "body"}:
                continue
            txt = _collapse_ws(tag.get_text(" ", strip=True))
            if not txt or len(txt) > 260:
                continue
            if not _is_pyeongtaek_noise_text(txt):
                continue
            tag.decompose()
        except Exception:
            continue


def _iter_selector_candidates(soup: Any, selectors: Iterable[str]) -> Iterable[str]:
    if soup is None:
        return
    for selector in selectors:
        try:
            nodes = soup.select(selector)
        except Exception:
            continue
        for node in nodes:
            text = _candidate_text(node)
            if text:
                yield text


def _find_label_value(root: Any, *labels: str) -> str:
    if root is None or not labels:
        return ""
    wanted = {re.sub(r"\s+", "", str(label or "")) for label in labels if label}
    for row in root.select("tr, dl"):
        try:
            cells = row.find_all(["th", "td", "dt", "dd"], recursive=False)
        except Exception:
            cells = []
        if len(cells) < 2:
            continue
        for idx, cell in enumerate(cells[:-1]):
            label = re.sub(r"\s+", "", cell.get_text(" ", strip=True))
            if label not in wanted:
                continue
            for nxt in cells[idx + 1 :]:
                text = _candidate_text(nxt)
                if text:
                    return text
    return ""


def _extract_full_date_text(text: Any) -> str:
    txt = _collapse_ws(text)
    if not txt:
        return ""
    m = re.search(r"\b(20\d{2})[-./](\d{1,2})[-./](\d{1,2})\b", txt)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = re.search(r"(20\d{2})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", txt)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = re.search(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)", txt)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return ""


def _extract_yymmdd_date_text(text: Any) -> str:
    txt = _collapse_ws(text)
    if not txt:
        return ""
    for m in re.finditer(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)", txt):
        yy, mm, dd = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{2000 + yy:04d}-{mm:02d}-{dd:02d}"
    return ""


def extract_pyeongtaek_recruit_reg_date_text(soup: Any, *, url: str = "") -> str:
    """평택 시험·채용 상세의 게시 기준일을 명시 라벨, 접수기간, 첨부명 순으로 추론한다."""
    if soup is None or not is_pyeongtaek_recruit_view_url(url):
        return ""

    explicit_labels = ("등록일", "작성일", "게시일", "등록일자", "작성일자", "게시일자")
    for scope_sel in ("#conts", "#content", "body"):
        try:
            scope = soup.select_one(scope_sel)
        except Exception:
            scope = None
        text = _find_label_value(scope, *explicit_labels)
        date_text = _extract_full_date_text(text)
        if date_text:
            return date_text

    period_labels = ("모집시작일", "접수시작일", "접수기간", "모집기간", "기간")
    for scope_sel in ("#conts", "#content", "body"):
        try:
            scope = soup.select_one(scope_sel)
        except Exception:
            scope = None
        text = _find_label_value(scope, *period_labels)
        date_text = _extract_full_date_text(text)
        if date_text:
            return date_text

    try:
        root = soup.select_one("#conts table.tableSt_view, #conts table.tbl_openview") or soup.select_one("#conts")
    except Exception:
        root = None
    if root is not None:
        try:
            for tag in root.select("a[title], a[onclick], span"):
                blob = " ".join(
                    str(v or "")
                    for v in (
                        tag.get("title") if hasattr(tag, "get") else "",
                        tag.get("onclick") if hasattr(tag, "get") else "",
                        tag.get_text(" ", strip=True) if hasattr(tag, "get_text") else "",
                    )
                )
                date_text = _extract_full_date_text(blob) or _extract_yymmdd_date_text(blob)
                if date_text:
                    return date_text
        except Exception:
            pass
    return ""


def extract_pyeongtaek_title(soup: Any, *, url: str = "") -> str:
    if soup is None or not is_pyeongtaek_city_url(url):
        return ""

    selectors: list[str] = []
    if is_pyeongtaek_vote_result_url(url):
        selectors.extend(
            [
                "#conts .poll_view > h4.no_bgimg.mT0",
                "#conts .poll_view h4.no_bgimg",
                "#conts .poll_view > h4",
                ".poll_view > h4.no_bgimg.mT0",
                ".poll_view h4.no_bgimg",
                ".poll_view > h4",
            ]
        )

    if is_pyeongtaek_board_post_view_url(url) or is_pyeongtaek_gosi_view_url(url):
        selectors.extend(
            [
                "#detailForm .bod_view > h4",
                "#detailForm .bod_view h4",
                "#view_div .bod_view > h4",
                "#view_div .bod_view h4",
                "#conts .bod_view > h4",
                "#conts .bod_view h4",
                ".bod_view > h4",
                ".bod_view h4",
            ]
        )

    for scope_sel in ("#detailForm", "#view_div", "#conts", "#content"):
        try:
            scope = soup.select_one(scope_sel)
        except Exception:
            scope = None
        text = _find_label_value(scope, "제목", "고시공고명", "설문명")
        if text:
            return text

    selectors.extend(
        [
            "#content h3",
            "#content h2",
            "#content h1",
            "#conts h3",
            "#conts h2",
            "#conts h1",
        ]
    )

    for text in _iter_selector_candidates(soup, selectors):
        return text

    return ""
