"""
게시글 상세 HTML에서 제목/본문 텍스트를 best-effort로 추출합니다.

목표:
- 사이트별 DOM 구조 편차가 커서, '안전한 기본값' 기반으로 추출합니다.
- 네비/헤더/푸터/스크립트/광고 영역은 제거하고 main/article/content 후보를 우선합니다.

NOTE:
- 춘천시청(chuncheon.go.kr) 상세: `backend.board.chuncheon_parse.parse_chuncheon_detail_soup` 가 URL 유형별로 분기·추출한다.
- 작성자/부서/등록일 추출은 기존 유틸을 재사용합니다:
  - backend.board.board_meta_extractor.extract_author_info_from_html
  - backend.shared.date_utils.extract_post_date
- db_name oka + JSON 응답 시: workflow가 넣은 가상 HTML(#js-json-title, extraction-mode)이면
  board_extractors.extract_title_from_html을 사용해 제목을 추출합니다.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from html import unescape as _html_unescape
from typing import Optional, Dict, Any
from urllib.parse import urljoin

try:
    from backend.board.board_extractors import extract_title_from_html
except Exception:  # pragma: no cover
    extract_title_from_html = None  # type: ignore[assignment]

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]


_WS_RE = re.compile(r"\s+")


@dataclass
class BoardPostExtract:
    url: str
    title: str
    content_text: str
    content_html: str
    snippet: str


def _collapse_ws(s: str) -> str:
    if not s:
        return ""
    return _WS_RE.sub(" ", s).strip()


def _looks_like_skip_navigation_only_text(text: str) -> bool:
    """접근성 스킵 링크만 남은 텍스트는 게시글 본문으로 보지 않는다."""
    if not text:
        return False
    compact = re.sub(r"\s+", "", str(text or ""))
    if not compact:
        return False
    skip_tokens = (
        "스킵네비게이션",
        "본문바로가기",
        "본문내용바로가기",
        "주메뉴바로가기",
        "메뉴바로가기",
        "콘텐츠바로가기",
        "내용바로가기",
    )
    if compact in skip_tokens:
        return True
    if "스킵네비게이션" not in compact or len(compact) > 80:
        return False
    rest = compact
    for token in skip_tokens:
        rest = rest.replace(token, "")
    return not rest


def _normalize_site_config_css_selector(sel: str) -> str:
    """site_configs의 title_tag/content_tag: 'title' → '.title', 'h4.viewInfo__tit'·'div.a.b'는 그대로."""
    s = (sel or "").strip()
    if not s:
        return s
    if s.startswith((".", "#", "[")):
        return s
    if "," in s:
        return s
    if re.match(r"^[a-zA-Z][\w-]*[.#\[:]", s):
        return s
    return "." + s


def _format_numbered_list_lines(text: str) -> str:
    """
    공용 후처리:
    - '1. ... 2. ... 3. ...' 처럼 번호 목록이 한 줄로 뭉개진 경우, 항목 경계에 줄바꿈을 복원한다.
    - 날짜(예: 2025. 12. 8.) 같은 패턴을 망치지 않도록 2~20까지만 대상으로 하고,
      직전 문자가 숫자인 경우(연속 숫자)에는 줄바꿈을 넣지 않는다.
    """
    t = (text or "").strip()
    if not t:
        return ""
    if "1." not in t:
        return t
    try:
        # 1. 이 최소 1번, 2~20. 이 2번 이상이면 목록으로 간주
        import re as _re
        # N. 뒤에 공백이 없고 바로 한글·영문이 오는 목차도 인식 (12.민방위 → 2.민방위)
        _after_num = r"(?:\s|[가-힣A-Za-z]|$)"
        if len(_re.findall(r"(?<!\d)\b1\." + _after_num, t)) < 1:
            return t
        if len(_re.findall(r"(?<!\d)\b(?:[2-9]|1\d|20)\." + _after_num, t)) < 2:
            return t

        _split_m = _re.compile(
            r"(?<!\d)\s+(?=(?:[2-9]|1\d|20)\.(?:\s|[가-힣A-Za-z]|$))"
        )
        # 연도. 월. 일 날짜 구간의 공백은 개행하지 않음 (2024. 8. 22 → 8 앞에서 끊기지 않게)
        _date_year_tail = _re.compile(r"(?:19|20)\d{2}\.\s*$")
        _date_short_year_tail = _re.compile(r"[‘'’]?\d{2}\.\s*$")
        _date_ym_tail = _re.compile(r"(?:19|20)\d{2}\.\s+\d{1,2}\.\s*$")

        def _sp_repl(m: "re.Match") -> str:
            pre = t[: m.start()]
            if _date_year_tail.search(pre) or _date_short_year_tail.search(pre) or _date_ym_tail.search(pre):
                return m.group(0)
            return "\n"

        t2 = _split_m.sub(_sp_repl, t)
        # 줄 단위로 공백 정리
        lines = [_collapse_ws(x) for x in t2.splitlines()]
        lines = [x for x in lines if x]
        return "\n".join(lines)
    except Exception:
        return t


def _trim_leading_skip_and_breadcrumb_text(text: str) -> str:
    """추출 평문 앞단의 스킵 링크·중복 바로가기·짧은 breadcrumb 잔여를 제거한다."""
    if not text:
        return ""
    if _looks_like_skip_navigation_only_text(text):
        return ""
    t = text.lstrip()
    glue = ("스킵네비게이션", "본문바로가기", "본문내용 바로가기", "주메뉴 바로가기", "주메뉴바로가기")
    for _ in range(30):
        hit = False
        for g in glue:
            if t.startswith(g):
                t = t[len(g) :].lstrip()
                hit = True
                break
        if not hit:
            break
    t = re.sub(r"^(?:스킵네비게이션|본문바로가기|본문내용\s*바로가기|주메뉴\s*바로가기)\s*", "", t, flags=re.IGNORECASE).lstrip()
    lines = t.splitlines()
    out_i = 0
    while out_i < len(lines):
        line = lines[out_i].strip()
        if not line:
            out_i += 1
            continue
        lu = line.upper()
        if lu.startswith("HOME ") or lu.startswith("HOME>") or (line.startswith("HOME ") and ">" in line):
            out_i += 1
            continue
        if len(line) < 280 and line.count(">") >= 2 and "HOME" in lu and ">" in line:
            out_i += 1
            continue
        if line in {"스킵네비게이션", "본문바로가기", "본문 바로가기", "본문내용 바로가기", "주메뉴바로가기", "주메뉴 바로가기"}:
            out_i += 1
            continue
        break
    cleaned = "\n".join(lines[out_i:]).strip() if lines else t.strip()
    return "" if _looks_like_skip_navigation_only_text(cleaned) else cleaned


def _build_flexible_space_regex(text: str) -> str:
    tokens = [tok for tok in re.split(r"\s+", str(text or "").strip()) if tok]
    if not tokens:
        return ""
    return r"\s*".join(re.escape(tok) for tok in tokens)


def _strip_leading_breadcrumb_and_title(text: str, title: str) -> str:
    """본문 앞에 붙는 breadcrumb/상세보기/중복 제목을 제거한다."""
    t = str(text or "").lstrip()
    if not t:
        return ""

    breadcrumb_patterns = (
        r"^\s*(?:[^\n>]{0,80}\s*>\s*){2,}[^\n]{0,220}?(?:게시글|게시물)?\s*상세\s*보기\s*",
        r"^\s*(?:[^\n>]{0,80}\s*>\s*){2,}[^\n]{0,220}?(?:상세|보기)\s*",
    )
    for pat in breadcrumb_patterns:
        new_t = re.sub(pat, "", t, count=1, flags=re.IGNORECASE)
        if new_t != t:
            t = new_t.lstrip()
            break

    title_pat = _build_flexible_space_regex(title)
    if title_pat:
        t = re.sub(
            rf"^(?:{title_pat})(?:\s*(?:안내|상세|보기|본문))*\s*",
            "",
            t,
            count=1,
            flags=re.IGNORECASE,
        ).lstrip()

    return t.strip()


def _dobong_td_for_th_label(tbl, label_needle: str):
    """표 안에서 th 라벨이 needle과 일치하는 행의 td 텍스트."""
    if tbl is None or not label_needle:
        return ""
    needle_flat = re.sub(r"\s+", "", label_needle)
    for tr in tbl.find_all("tr"):
        th = tr.find("th")
        if not th:
            continue
        th_flat = re.sub(r"\s+", "", th.get_text(strip=True))
        if th_flat != needle_flat and label_needle not in th.get_text():
            continue
        td = th.find_next_sibling("td")
        if not td:
            td = tr.find("td")
        if not td:
            continue
        txt = td.get_text(" ", strip=True)
        if txt and len(txt) >= 2:
            return _collapse_ws(txt)
    return ""


def _dobong_find_table_by_class(root, class_name: str):
    if root is None or not class_name:
        return None
    want = class_name.lower()
    for tbl in root.find_all("table"):
        tcls = tbl.get("class") or []
        if any(str(c).lower() == want for c in tcls):
            return tbl
    return None


def _dobong_title_from_meta_tables(root) -> str:
    """도봉구청(dobong.go.kr) 메타 표: boardView+설문명(poll), boardWrite+제목(위원회 Committee_PView 등)."""
    if root is None:
        return ""
    for cls, label in (("boardView", "설문명"), ("boardWrite", "제목")):
        tbl = _dobong_find_table_by_class(root, cls)
        if not tbl:
            continue
        got = _dobong_td_for_th_label(tbl, label)
        if got:
            return got
    return ""


_MIRYANG_TITLE_LABELS = (
    "제목",
    "항목",
    "공표항목",
    "민원명",
    "행사명",
    "계약명",
    "사업명",
    "공사명",
    "용역명",
    "물품명",
    "교육명",
    "강좌명",
    "모집명",
)


def _miryang_title_from_label_table(scope) -> str:
    """밀양시청 상세 표에서 제목 역할을 하는 라벨-값 쌍을 찾는다."""
    if scope is None:
        return ""
    flat_labels = tuple(re.sub(r"\s+", "", label) for label in _MIRYANG_TITLE_LABELS)
    for tr in scope.select("table tr"):
        for th in tr.find_all("th"):
            label_flat = re.sub(r"\s+", "", th.get_text(" ", strip=True))
            if not label_flat or not any(label in label_flat for label in flat_labels):
                continue
            td = th.find_next_sibling("td")
            if not td:
                continue
            txt = _collapse_ws(td.get_text(" ", strip=True))
            if txt and len(txt) >= 2:
                return txt
    return ""


_GENERIC_TITLE_LABELS = (
    "제목",
    "공고명",
    "고시공고명",
    "공시공고명",
    "공고제목",
    "민원명",
    "제목",
    "행사명",
    "축제명",
    "공연명",
    "전시명",
    "강연명",
    "교육명",
    "사업명",
    "프로그램명",
    "프로그램",
)


def _generic_title_from_label_table(scope) -> str:
    if scope is None:
        return ""
    flat_labels = {re.sub(r"\s+", "", label) for label in _GENERIC_TITLE_LABELS}
    try:
        rows = scope.select("table tr")
    except Exception:
        rows = []
    for tr in rows:
        try:
            headers = tr.find_all("th")
        except Exception:
            headers = []
        for th in headers:
            label_flat = re.sub(r"\s+", "", th.get_text(" ", strip=True) or "")
            if label_flat not in flat_labels:
                continue
            td = th.find_next_sibling("td")
            if not td:
                try:
                    td = tr.find("td")
                except Exception:
                    td = None
            if not td:
                continue
            txt = _collapse_ws(td.get_text(" ", strip=True))
            if txt and len(txt) >= 2:
                return txt
    return ""


def _miryang_expand_public_info_links(scope, base_url: str) -> None:
    """사전정보공표 표의 '링크바로가기' 버튼은 실제 href가 정보값이므로 텍스트로 보존한다."""
    if scope is None:
        return
    try:
        links = scope.select("table td a[href]")
    except Exception:
        links = []
    for a in links:
        try:
            href = str(a.get("href") or "").strip()
            if not href:
                continue
            text = _collapse_ws(a.get_text(" ", strip=True))
            if text and "링크바로가기" not in text and "바로가기" not in text:
                continue
            a.replace_with(urljoin(base_url or "", href))
        except Exception:
            pass


def _strip_hwp_editor_artifacts(scope) -> None:
    """한컴 웹에디터가 남기는 숨은 JSON 보관 div는 본문 HTML/텍스트에서 제외한다."""
    if scope is None:
        return
    try:
        targets = scope.select("#hwpEditorBoardContent, .hwp_editor_board_content")
    except Exception:
        targets = []
    for tag in targets:
        try:
            tag.decompose()
        except Exception:
            pass


def _miryang_expand_embedded_media(scope, base_url: str) -> None:
    """밀양 게시글의 iframe 영상은 공통 정제에서 제거되기 전에 URL 텍스트로 보존한다."""
    if scope is None:
        return
    try:
        iframes = list(scope.find_all("iframe"))
    except Exception:
        iframes = []
    for iframe in iframes:
        try:
            src = str(iframe.get("src") or "").strip()
            if not src:
                continue
            full_src = urljoin(base_url or "", src)
            label = "동영상"
            title = _collapse_ws(iframe.get("title") or "")
            if title:
                replacement = f"\n{label}: {title} ({full_src})\n"
            else:
                replacement = f"\n{label}: {full_src}\n"
            iframe.replace_with(replacement)
        except Exception:
            pass


def _miryang_attachment_lines(scope, base_url: str) -> list[str]:
    """밀양 게시글 첨부영역에서 실제 다운로드 링크만 본문 보강용 텍스트로 추출한다."""
    if scope is None:
        return []
    try:
        links = list(scope.select(".outboxRead .file a[href], .file a[href]"))
    except Exception:
        links = []

    out: list[str] = []
    seen: set[str] = set()
    for a in links:
        try:
            href = str(a.get("href") or "").strip()
            if not href or "filewebdown.do" not in href.lower():
                continue
            full_href = urljoin(base_url or "", href)
            name_el = a.select_one(".F-nme")
            name = _collapse_ws(
                (name_el.get_text(" ", strip=True) if name_el else "")
                or a.get("title")
                or a.get_text(" ", strip=True)
            )
            if not name:
                name = full_href
            key = f"{name}|{full_href}"
            if key in seen:
                continue
            seen.add(key)
            out.append(f"첨부파일: {name} ({full_href})")
        except Exception:
            pass
    return out


def _normalize_miryang_detail_url(raw_url: str, base_url: str) -> str:
    """밀양 상세에 간혹 섞이는 http://https:// 형태를 정규화한다."""
    u = str(raw_url or "").strip()
    if not u:
        return ""
    if u.lower().startswith("http://https://"):
        u = "https://" + u[len("http://https://") :]
    elif u.lower().startswith("http://http://"):
        u = "http://" + u[len("http://http://") :]
    return urljoin(base_url or "", u)


def _miryang_labeled_items(scope) -> list[tuple[str, str]]:
    """밀양 관광 상세의 아이콘 라벨/값 목록을 구조적으로 추출한다."""
    if scope is None:
        return []
    items: list[tuple[str, str]] = []
    try:
        rows = scope.select(".TurInfo-Box li")
    except Exception:
        rows = []
    for row in rows:
        try:
            label_el = row.select_one(".tu-Titsp")
            value_el = row.select_one(".tu-Txtsp")
            label = _collapse_ws(label_el.get_text(" ", strip=True)) if label_el else ""
            value = _collapse_ws(value_el.get_text(" ", strip=True)) if value_el else ""
            if label and value:
                items.append((label, value))
        except Exception:
            pass
    try:
        text_boxes = scope.select(".TurInfo-Box .Txt-Box")
    except Exception:
        text_boxes = []
    seen = {(label, value) for label, value in items}
    for box in text_boxes:
        try:
            label_el = box.select_one(".tu-Titsp")
            value_el = box.select_one(".tu-Txtsp")
            label = _collapse_ws(label_el.get_text(" ", strip=True)) if label_el else ""
            value = _collapse_ws(value_el.get_text(" ", strip=True)) if value_el else ""
            label = re.sub(r"\s*[:：]\s*$", "", label).strip()
            if label and value and (label, value) not in seen:
                seen.add((label, value))
                items.append((label, value))
        except Exception:
            pass
    return items


def _miryang_image_lines(scope, base_url: str, *, limit: int = 12) -> list[str]:
    """밀양 관광 상세의 본문 이미지 URL을 텍스트로 보존한다."""
    if scope is None:
        return []
    try:
        imgs = list(scope.select(".Pimg-Box #image-gallery img[src], .Pimg-Box img[src]"))
    except Exception:
        imgs = []
    out: list[str] = []
    seen: set[str] = set()
    for img in imgs:
        try:
            src = _normalize_miryang_detail_url(img.get("src") or "", base_url)
            if not src or src in seen:
                continue
            seen.add(src)
            alt = _collapse_ws(img.get("alt") or "")
            if alt and alt != "이미지 없음":
                out.append(f"이미지: {alt} ({src})")
            else:
                out.append(f"이미지: {src}")
            if len(out) >= limit:
                break
        except Exception:
            pass
    return out


def _try_extract_miryang_tour_lodging_post(soup, url: str) -> Optional[BoardPostExtract]:
    """밀양 문화관광 숙박 상세: 업체 기본정보와 소개/오시는길만 추출한다."""
    if not soup or not url or BeautifulSoup is None:
        return None
    u_low = (url or "").lower()
    if "miryang.go.kr" not in u_low or "egovlodgingdetail.do" not in u_low:
        return None

    root = soup.select_one(".Subcontent-Box") or soup.select_one("#contents")
    info = root.select_one(".TurInfo-Box") if root else None
    if not root or not info:
        return None

    items = _miryang_labeled_items(root)
    item_map = {label: value for label, value in items if label and value}

    title = (item_map.get("숙박점명") or "").strip()
    if title:
        title = re.sub(r"^\[[^\]]+\]\s*", "", title).strip() or title
    if not title:
        meta_title = soup.find("meta", attrs={"property": "og:title"})
        title = _collapse_ws(meta_title.get("content") if meta_title else "")
        title = re.sub(r"^\s*숙박\s*[-:]\s*", "", title).strip()
    if not title:
        title = "제목 없음"

    lines: list[str] = []
    for label, value in items:
        lines.append(f"{label}: {value}")

    homepage = ""
    try:
        home_a = info.select_one("a.homIc[href], a.tu-btn3[href], a[href*='foresttrip']")
        if home_a:
            homepage = _normalize_miryang_detail_url(home_a.get("href") or "", url)
    except Exception:
        homepage = ""
    if homepage:
        lines.append(f"홈페이지: {homepage}")

    lines.extend(_miryang_image_lines(root, url))

    try:
        sections = list(root.select(".Induc-Box"))
    except Exception:
        sections = []
    for section in sections:
        try:
            heading = _collapse_ws((section.select_one(".Ind-sp") or section.select_one("h2")).get_text(" ", strip=True))
        except Exception:
            heading = ""
        try:
            body = section.select_one(".Ind-Txtbx") or section
            body_text = _extract_miryang_content_text(body, base_url=url)
            body_text = _trim_leading_skip_and_breadcrumb_text(body_text)
        except Exception:
            body_text = ""
        if heading and body_text:
            lines.append(f"{heading}: {body_text}")
        elif body_text:
            lines.append(body_text)

    content_text = "\n".join(line for line in lines if _collapse_ws(line)).strip()
    if not content_text:
        return None

    html_doc = BeautifulSoup("<div class=\"miryang-tour-lodging-extract-root\"></div>", "html.parser")
    wrap = html_doc.find("div")
    if wrap is not None:
        for line in lines:
            p = html_doc.new_tag("p")
            p.string = line
            wrap.append(p)
    content_html = _sanitize_html_fragment(wrap if wrap is not None else root).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_miryang_tour_detail_post(soup, url: str) -> Optional[BoardPostExtract]:
    """밀양 문화관광 일반 관광지 상세: 지도 마커/주변 목록 대신 상세 영역만 추출한다."""
    if not soup or not url or BeautifulSoup is None:
        return None
    u_low = (url or "").lower()
    if "miryang.go.kr" not in u_low or "egovtourdetail.do" not in u_low:
        return None

    root = soup.select_one("#contents") or soup
    info = root.select_one(".TurInfo-Box") if root else None
    if not root or not info:
        return None

    items = _miryang_labeled_items(root)
    item_map = {label: value for label, value in items if label and value}

    title = ""
    for sel in (".cont-top h2.tit", ".cont-top .tit", "#contents > h2", "h2.tit"):
        el = root.select_one(sel)
        if not el:
            continue
        title = _collapse_ws(el.get_text(" ", strip=True))
        if title:
            break
    if not title:
        title = (item_map.get("지명") or "").strip()
    if not title:
        meta_title = soup.find("meta", attrs={"property": "og:title"})
        title = _collapse_ws(meta_title.get("content") if meta_title else "")
    if not title:
        title = _collapse_ws(soup.title.get_text(" ", strip=True) if soup.title else "")
    if not title:
        title = "제목 없음"

    lines: list[str] = []
    for label, value in items:
        lines.append(f"{label}: {value}")

    lines.extend(_miryang_image_lines(root, url))

    try:
        sections = list(root.select(".Induc-Box"))
    except Exception:
        sections = []
    for section in sections:
        try:
            heading_node = section.select_one(".Ind-sp") or section.select_one("h2") or section.select_one("h3")
            heading = _collapse_ws(heading_node.get_text(" ", strip=True)) if heading_node else ""
        except Exception:
            heading = ""
        try:
            body = section.select_one(".Ind-Txtbx") or section
            body_text = _extract_miryang_content_text(body, base_url=url)
            body_text = _trim_leading_skip_and_breadcrumb_text(body_text)
            body_text = _strip_leading_breadcrumb_and_title(body_text, heading)
        except Exception:
            body_text = ""
        if heading and body_text:
            lines.append(f"{heading}: {body_text}")
        elif body_text:
            lines.append(body_text)

    content_text = "\n".join(line for line in lines if _collapse_ws(line)).strip()
    if not content_text:
        return None

    html_doc = BeautifulSoup("<div class=\"miryang-tour-detail-extract-root\"></div>", "html.parser")
    wrap = html_doc.find("div")
    if wrap is not None:
        for line in lines:
            p = html_doc.new_tag("p")
            p.string = line
            wrap.append(p)
    content_html = _sanitize_html_fragment(wrap if wrap is not None else info).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _extract_miryang_content_text(root, *, base_url: str = "") -> str:
    """밀양 에디터 본문은 span 조각이 많으므로 인라인 텍스트를 붙이고 블록 경계만 줄바꿈한다."""
    if root is None or BeautifulSoup is None:
        return ""
    try:
        frag = BeautifulSoup(str(root), "html.parser")
        node = frag.find(True)
        if not node:
            return ""
        _strip_hwp_editor_artifacts(frag)
        _miryang_expand_embedded_media(frag, base_url)

        for tr in node.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            for i, cell in enumerate(cells):
                if i < len(cells) - 1:
                    cell.insert_after(" | ")
            tr.insert_after("\n")

        for br in node.find_all("br"):
            br.replace_with("\n")

        block_tags = ["p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "section", "article"]
        for tag in node.find_all(block_tags):
            tag.insert_after("\n")

        text = node.get_text(separator="").replace("\u00a0", " ")
        text = re.sub(r"(\s*\|\s*)+", " | ", text)
        text = _clean_preserve_newline(text)
        text = re.sub(r"(?:\s*\|\s*){2,}", " | ", text)
        text = re.sub(r"\n\s*\n+", "\n", text).strip()
        return text
    except Exception:
        try:
            return _extract_content_text(root)
        except Exception:
            return ""


def _try_extract_dobong_main_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    도봉구청(dobong.go.kr): #mainCont 내 .bbsWrite(위원회)·.bbsView(설문 등) 우선.
    GNB·스킵·좌측메뉴는 제외한다.
    """
    if not soup or not url or BeautifulSoup is None:
        return None
    if "dobong.go.kr" not in url.lower():
        return None
    main = soup.select_one("#mainCont") or soup.select_one("#scontents")
    if not main:
        return None
    focus = main.select_one(".bbsWrite") or main.select_one(".bbsView") or main
    try:
        frag = BeautifulSoup(str(focus), "html.parser")
    except Exception:
        return None
    root = frag.find(True)
    if not root:
        return None
    _strip_noisy_tags(frag)
    root = frag.find(True)
    if not root:
        return None
    title = _dobong_title_from_meta_tables(root)
    if not title:
        title = _extract_title(frag, root)
    content_text = _extract_content_text(root)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not (content_text or "").strip() or len(content_text.strip()) < 8:
        return None
    if not (title or "").strip():
        title = "제목 없음"
    _w = BeautifulSoup("<div class=\"dobong-extract-root\"></div>", "html.parser")
    wrap = _w.find("div")
    if wrap is not None:
        wrap.append(copy.copy(root))
    content_html = _sanitize_html_fragment(wrap if wrap is not None else root).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_hscity_board_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    화성특례시청(hscity.go.kr): div.board_write 표의 «제목»·«내용» 행.
    범용 _select_main_node는 GNB·左메뉴·푸터가 섞인 긴 본문을 만들 수 있어,
    «내용» td(내부 div.txt 우선)만 잘라 동일 도메인 공통 템플릿에 맞춘다.
    """
    if not soup or not url or BeautifulSoup is None:
        return None
    if "hscity.go.kr" not in url.lower():
        return None
    scope = soup.select_one("div.board_write")
    if not scope:
        return None
    tbl = scope.find("table")
    if not tbl:
        return None
    try:
        from backend.board.hscity_board import extract_hscity_board_title

        title = (extract_hscity_board_title(soup, url=url) or "").strip()
    except Exception:
        title = ""
    if not title:
        title = (_dobong_td_for_th_label(tbl, "제목") or "").strip()

    content_td = None
    needle_flat = re.sub(r"\s+", "", "내용")
    for tr in tbl.find_all("tr"):
        th = tr.find("th", attrs={"scope": "row"}) or tr.find("th")
        if not th:
            continue
        th_flat = re.sub(r"\s+", "", th.get_text(strip=True))
        if th_flat != needle_flat and "내용" not in th.get_text():
            continue
        content_td = th.find_next_sibling("td")
        if not content_td:
            content_td = tr.find("td")
        break
    if not content_td:
        return None

    inner = content_td.select_one("div.txt") or content_td
    try:
        frag = BeautifulSoup(str(inner), "html.parser")
    except Exception:
        return None
    root = frag.find(True)
    if not root:
        return None
    _strip_noisy_tags(frag)
    root = frag.find(True)
    if not root:
        return None
    content_text = _extract_content_text(root)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not (content_text or "").strip() or len(content_text.strip()) < 8:
        return None
    if not title:
        title = (_extract_title(frag, root) or "").strip()
    if not title:
        title = "제목 없음"
    _w = BeautifulSoup("<div class=\"hscity-extract-root\"></div>", "html.parser")
    wrap = _w.find("div")
    if wrap is not None:
        wrap.append(copy.copy(root))
    content_html = _sanitize_html_fragment(wrap if wrap is not None else root).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_hscity_photo_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    포토 화성 상세(photo.hscity.go.kr): 이미지 옆 .title-info 메타 영역만 추출한다.
    같은 페이지 아래 관련사진 목록이 크기 때문에 범용 본문 선택으로 내려가면 오염된다.
    """
    if not soup or not url or BeautifulSoup is None:
        return None
    if "photo.hscity.go.kr" not in url.lower():
        return None

    info = soup.select_one(".photo-title .title-info") or soup.select_one(".title-info")
    if not info:
        return None

    try:
        frag = BeautifulSoup(str(info), "html.parser")
    except Exception:
        return None

    for tag in frag.select("script, style, noscript, iframe, form, button"):
        try:
            tag.decompose()
        except Exception:
            pass
    for li in list(frag.find_all("li")):
        try:
            if not li.get_text(" ", strip=True):
                li.decompose()
        except Exception:
            pass

    root = frag.select_one(".title-info") or frag.find(True)
    if not root:
        return None

    content_text = _extract_content_text(root)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not (content_text or "").strip() or len(content_text.strip()) < 8:
        return None

    title = ""
    try:
        for li in root.find_all("li"):
            text = _collapse_ws(li.get_text(" ", strip=True))
            compact = re.sub(r"\s+", "", text)
            if compact.startswith("제목:") or compact.startswith("제목："):
                title = re.sub(r"^\s*제목\s*[:：]\s*", "", text).strip()
                break
    except Exception:
        title = ""
    if not title:
        heading = soup.select_one(".header-detail h2")
        title = _collapse_ws(heading.get_text(" ", strip=True)) if heading else ""
    if not title:
        title = (_extract_title(soup, soup) or "").strip()
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


def _try_extract_copyright_board_post(soup, url: str) -> Optional[BoardPostExtract]:
    if not soup or not url or BeautifulSoup is None:
        return None
    try:
        from backend.board.copyright_board import (
            extract_copyright_title,
            is_copyright_board_url,
            select_copyright_content_node,
        )
    except Exception:
        return None
    if not is_copyright_board_url(url):
        return None
    content_node = select_copyright_content_node(soup)
    if content_node is None:
        return None
    try:
        frag = BeautifulSoup(str(content_node), "html.parser")
    except Exception:
        return None
    for noisy in frag.select("script, style, iframe, form, button, .btn-board, .prev-next, .paging, .board-pager"):
        noisy.decompose()
    _strip_noisy_tags(frag)
    root = frag.find(True)
    if not root:
        return None
    content_text = _extract_content_text(root)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not (content_text or "").strip() or len(content_text.strip()) < 8:
        return None
    title = (extract_copyright_title(soup, url=url) or "").strip() or (_extract_title(soup, soup) or "").strip()
    if not title:
        title = "제목 없음"
    _w = BeautifulSoup("<div class=\"copyright-extract-root\"></div>", "html.parser")
    wrap = _w.find("div")
    if wrap is not None:
        wrap.append(copy.copy(root))
    content_html = _sanitize_html_fragment(wrap if wrap is not None else root).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_guro_proposal_post(soup, url: str) -> Optional[BoardPostExtract]:
    """구로구청 주민제안 상세: 사업 필드와 내용 셀을 함께 본문으로 사용한다."""
    if not soup or not url or BeautifulSoup is None:
        return None
    u_low = (url or "").lower()
    if "guro.go.kr" not in u_low or not re.search(r"(?:[?&])bbsNo=769(?:&|$)", url, re.IGNORECASE):
        return None

    table = soup.select_one("table.p-table")
    if not table:
        return None

    title = (_extract_title(soup, table) or "").strip()
    allowed_labels = {
        "사업명",
        "총사업비",
        "사업위치",
        "사업기간",
        "사업개요",
        "사업내용",
        "사업효과",
    }
    parts: list[str] = []
    structured_count = 0

    for tr in table.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not td:
            continue
        label = _collapse_ws(th.get_text(" ", strip=True)) if th else ""
        value = _extract_content_text(td)
        value = _collapse_ws(value)
        if not value:
            continue

        td_classes = td.get("class") or []
        is_content_cell = "p-table__content" in td_classes
        if label in allowed_labels:
            structured_count += 1
            parts.append(f"{label}: {value}")
        elif is_content_cell:
            parts.append(f"내용: {value}")

    if structured_count == 0 or not parts:
        return None

    content_text = "\n".join(parts).strip()
    if not content_text:
        return None

    html_doc = BeautifulSoup("<div class=\"guro-proposal-extract-root\"></div>", "html.parser")
    wrap = html_doc.find("div")
    if wrap is not None:
        for line in parts:
            p = html_doc.new_tag("p")
            p.string = line
            wrap.append(p)
    content_html = _sanitize_html_fragment(wrap if wrap is not None else table).strip()
    snippet = _collapse_ws(content_text)[:200]

    return BoardPostExtract(
        url=url,
        title=title or "제목 없음",
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_gokams_board_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    공연예술유통 P:art:ner(gokams.or.kr) 공지 상세 전용 추출.
    - `.bbs-view` 범위만 대상으로 삼아 팝업/이전글/다음글 텍스트가 본문에 섞이지 않게 한다.
    - 본문이 이미지로만 구성된 경우 텍스트는 빈 문자열로 유지한다.
    """
    if not soup or not url or BeautifulSoup is None:
        return None
    if "gokams.or.kr" not in url.lower():
        return None

    view = soup.select_one(".bbs-view")
    header = view.select_one(".bbs-header") if view else None
    body = view.select_one(".bbs-body .editor-text") if view else None
    if body is None and view is not None:
        body = view.select_one(".bbs-body")
    if not view or not header or not body:
        return None

    title = ""
    for sel in (".bbs-header .tit", ".bbs-header strong.tit", ".bbs-header strong", ".tit"):
        el = view.select_one(sel)
        if not el:
            continue
        title = _collapse_ws(el.get_text(" ", strip=True))
        if title:
            break
    if not title:
        title = _extract_title(soup, view)

    try:
        content_text = _extract_content_text(copy.copy(body)).strip()
    except Exception:
        content_text = ""
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = _strip_leading_breadcrumb_and_title(content_text, title)

    has_inline_images = bool(body.find("img"))
    if not title and not content_text and not has_inline_images:
        return None

    content_html = _sanitize_html_fragment(body).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=(title or "제목 없음").strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_ne_board_post(soup, url: str) -> Optional[BoardPostExtract]:
    """National Education Commission(ne.go.kr) board detail pages."""
    if not soup or not url or BeautifulSoup is None:
        return None
    u_low = (url or "").lower()
    if "ne.go.kr" not in u_low or "bd_selectbbs.do" not in u_low:
        return None

    view = soup.select_one(".conts-board")
    if not view:
        return None

    title = ""
    title_el = view.select_one(".conts-board-title .tit")
    if title_el:
        title = _collapse_ws(title_el.get_text(" ", strip=True))
    if not title:
        title = (_extract_title(soup, view, selector_hint=".conts-board-title .tit") or "").strip()

    body = (
        view.select_one(".conts-board-contents .txt")
        or view.select_one(".conts-board-contents")
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

    for tag in list(frag.select(".hwp_editor_board_content")):
        try:
            if not _collapse_ws(tag.get_text(" ", strip=True)):
                tag.decompose()
        except Exception:
            pass
    _strip_noisy_tags(frag)
    root = frag.find(True)
    if not root:
        return None

    content_text = _extract_content_text(root)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = _strip_leading_breadcrumb_and_title(content_text, title)
    if not (content_text or "").strip() or len(content_text.strip()) < 8:
        return None

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


def _try_extract_miryang_board_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    밀양시청 계열 상세:
    - 일반 게시판: board-view-wrap 내부 .board-view-contents
    - 계약현황 등 표 중심 상세: BoardRead/Board-Box 내부 메타 테이블
    """
    if not soup or not url or BeautifulSoup is None:
        return None
    if "miryang.go.kr" not in (url or "").lower():
        return None

    view = (
        soup.select_one(".board-wrap.board-view-wrap")
        or soup.select_one(".board-view-wrap")
        or soup.select_one(".BoardRead.board-list-wrap")
        or soup.select_one(".board-list-wrap .inboxRead")
        or soup.select_one(".inboxRead")
        or soup.select_one(".Board-Box")
        or soup.select_one(".board-box")
    )
    if not view:
        return None

    view_classes = {str(c).lower() for c in (view.get("class") or [])}
    is_board_box_view = "board-box" in view_classes
    body = (
        view.select_one(".board-view-cont .board-view-contents")
        or view.select_one(".board-view-contents")
        or view.select_one(".Board-Box")
        or view.select_one(".board-box")
        or view.select_one(".cont")
        or (view if is_board_box_view else None)
    )
    if not body:
        return None

    title = ""
    for sel in (
        ".inboxRead .headinfo > h2.BoR-h2",
        ".inboxRead .headinfo > h3.BoR-h2",
        ".headinfo > h2.BoR-h2",
        ".headinfo > h3.BoR-h2",
        ".board-view-head h3",
        ".board-view-head h2",
        ".board-view-head .title",
        ".board-view-head .board-view-title h3",
        "h1#b_title",
        "h3.BoR-h2",
        "h2.BoR-h2",
    ):
        if title:
            break
        el = view.select_one(sel)
        if not el:
            continue
        t = _collapse_ws(el.get_text(" ", strip=True))
        if t:
            title = t
            break
    if not title:
        title = _miryang_title_from_label_table(view)

    try:
        frag = BeautifulSoup(str(body), "html.parser")
    except Exception:
        return None
    root = frag.find(True)
    if not root:
        return None

    _miryang_expand_public_info_links(frag, url)
    _miryang_expand_embedded_media(frag, url)
    _strip_hwp_editor_artifacts(frag)
    _strip_noisy_tags(frag)
    root = frag.find(True)
    if not root:
        return None

    content_text = _extract_miryang_content_text(root, base_url=url)
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = _strip_leading_breadcrumb_and_title(content_text, title)
    attachment_lines = _miryang_attachment_lines(view, url)
    if attachment_lines:
        existing = content_text or ""
        new_lines = [line for line in attachment_lines if line not in existing]
        if new_lines:
            content_text = (content_text.rstrip() + "\n" + "\n".join(new_lines)).strip()
    if not (content_text or "").strip():
        return None

    if not title:
        title = (_extract_title(soup, body) or "").strip()
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


def _strip_noisy_tags(soup) -> None:
    if soup is None: return

    def _is_attachment_preview_tag(tag) -> bool:
        try:
            if not tag or getattr(tag, "name", "") not in {"a", "button"}:
                return False
            href = str(tag.get("href") or "").strip().lower()
            classes = [str(c).lower() for c in (tag.get("class") or [])]
            if "previewbbs.do" in href:
                return True
            if any("preview" in c for c in classes):
                return True
            if tag.find_parent(class_="p-attach") or tag.find_parent(class_="p-attach__item"):
                txt = _collapse_ws(tag.get_text(" ", strip=True))
                if "미리보기" in txt or "음성듣기" in txt:
                    return True
        except Exception:
            return False
        return False

    # 0. [추가] 숨김 태그 및 CSS 숨김 요소 물리적 제거
    for tag in soup.find_all(True):
        # Tag.attrs가 None인 경우(파서/문서 특이 케이스) tag.get()이 실패하므로 건너뜀
        if tag is None or not hasattr(tag, "attrs") or tag.attrs is None:
            continue
        raw_style = tag.get("style")
        style = raw_style.replace(" ", "").lower() if raw_style else ""

        if tag.has_attr('hidden') or "display:none" in style or "visibility:hidden" in style:
            try: tag.decompose()
            except: pass

    # 주의: class/id 부분문자열 매칭이라 "info"는 .info-textcon(밀양 평생학습 등 본문 래퍼)까지 제거함.
    # "print"는 제외: 강남구청 등 본문 래퍼에 print-100(인쇄용 폭)이 붙어 [class*='print']로
    # #gnsubContent 전체가 사라지는 사례가 있음. 인쇄 UI는 .print_area 등 명시 셀렉터로 제거.
    unwanted_patterns = [
        "banner", "side", "wing", "kakao", "sns", "share",
        "satisfy", "survey", "util", "btn", "footer",
        "prev", "next", "file", "popup", "modal", "overlay",
    ]

    for pat in unwanted_patterns:
        # 속성 선택자 [class*='...']를 사용하여 해당 단어가 포함된 모든 태그 타겟팅
        for tag in soup.select(f"[class*='{pat}'], [id*='{pat}']"):
            try:
                if _is_attachment_preview_tag(tag):
                    continue
                tag.decompose()
            except: pass

    # 1. 기술적 태그 및 명확한 푸터 UI 영역 제거 (추가)
    unwanted_selectors = [
        "script", "style", "noscript", "iframe", "svg", "caption", # 기술 노이즈 통합
        "header", "nav", "footer", "#header", "#footer", "#nav", "#gnb",
        ".header", ".footer", ".nav", ".menu", ".gnb", ".top_util", 
        ".all_menu", ".search_area", ".weather_area", ".skip_nav", 
        "#u_skip", "#skipNavi", "#skipnavigation", "#skipNavigation", "#skip",
        ".u_skip", ".skip_navi", ".skipNavi", ".skipnavigation", ".skipNavigation",
        ".breadcrumb", ".location", "#location", ".loc_icon01",
        "#lnb", ".lnb", "#left_menu", ".left_menu",
        ".s_con_left", "#s_con_left",  # 도봉구청 등 좌측 메뉴 컬럼
        "#sub-tit-box",  # 밀양 평생학습 등: 섹션 제목·breadcrumb 묶음(본문 .board 앞)
        ".sns_share", ".print_area", ".print_btn", "#print", ".print",
        ".btn_area", ".satisfaction",
        ".popup-wrap", ".popup-inner", ".popup-top", ".popup-cont",
        ".over-popup", ".layer-popup", ".layer_pop", ".modal", ".overlay",
        # [추가] 일반 게시판 전용 불순물 영역
        ".content_satisfaction", ".view_util", ".view_sns", # 만족도/SNS 공유
        ".prev_next_area", ".bbs_btn_wrap", ".view_bottom_util", # 이전다음글/버튼군
        ".view_info", # 작성일/조회수 등이 뭉쳐있는 상단/하단 메타 영역
    ]
    
    def _skip_decompose(tag) -> bool:
        # 그누보드 게시글 보기: <article id="bo_v"><header><h2 id="bo_v_title">…</h2></header>
        # 범용 header 제거 시 실제 글 제목까지 사라지므로 보존
        if tag and tag.name == "header":
            art = tag.find_parent("article")
            if art and (art.get("id") or "").strip() == "bo_v":
                return True
        # 서울역사박물관 등: 표 caption.tit_article 에 게시 제목이 들어가 있음(NR_boardView). 범용 caption 제거에서 제외
        if tag and tag.name == "caption":
            cls = tag.get("class") or []
            if any(str(c).lower() == "tit_article" for c in cls):
                return True
        return False

    for sel in _LAYOUT_CHROME_SELECTORS:
        try:
            targets = soup.select(sel) if sel.startswith((".", "#", "[")) else soup.find_all(sel)
        except Exception:
            targets = []
        for tag in list(targets or []):
            try:
                if _skip_decompose(tag):
                    continue
                tag.decompose()
            except Exception:
                pass

    for sel in unwanted_selectors:
        # 클래스/아이디면 select, 태그명이면 find_all로 찾아 제거
        targets = soup.select(sel) if sel.startswith((".", "#")) else soup.find_all(sel)
        for tag in targets:
            try:
                if _skip_decompose(tag):
                    continue
                tag.decompose()
            except: pass

    # 1b. 앵커 스킵 링크(#본문 등): 짧은 텍스트 + 바로가기 문구만 제거
    try:
        for a in list(soup.find_all("a", href=True)):
            try:
                h = (a.get("href") or "").strip()
                if not h.startswith("#"):
                    continue
                txt = a.get_text(strip=True)
                if len(txt) > 40:
                    continue
                if "바로가기" in txt or "바로 가기" in txt:
                    a.decompose()
                    continue
                if txt in ("본문", "주메뉴", "주 메뉴", "내용"):
                    a.decompose()
            except Exception:
                pass
    except Exception:
        pass

    # 2. 기존 텍스트 기반 노이즈 제거 로직 (유지 및 보강)
    noise_texts = [
        "미리보기", "음성듣기", "목록", "이전글", "다음글", "조회수", "작성일", "등록일", "첨부파일",
        "콘텐츠 만족도 조사", "인쇄", "공유하기", "페이스북", "X", "블로그", "닫기",
        "스킵네비게이션", "본문바로가기", "본문내용 바로가기", "주메뉴 바로가기", "전체메뉴", "검색영역",
    ]

    # [수정] 본문 보호 로직 실효성 강화
    for tag in soup.find_all(["a", "button", "span", "div", "li", "th", "td"], limit=2000):
        try:
            txt = tag.get_text(strip=True)
            if not txt: continue
            
            is_meta_noise = any(nt in txt for nt in noise_texts) and len(txt) < 30
            is_access_guide = "상세보기" in txt and "정보를 제공합니다" in txt
            
            if is_meta_noise or is_access_guide:
                p = tag.parent
                protected = [
                    "p-table__subject_text", 
                    "p-table__content", 
                    "p-attach",
                    "p-attach__item",
                    "p-attach__preview",
                    "edu_detail_info",  # 상세정보 영역 예시
                    "txt",               # 지자체 표준 본문 ID/클래스
                    "view_cont"          # 상세페이지 컨텐츠 영역
                ]
                p_classes = p.get("class", []) if p else []
                
                # 보호 클래스가 부모에 있다면 삭제 스킵
                if any(c in p_classes for c in protected): continue 

                if tag.name in ["a", "button"]:
                    if _is_attachment_preview_tag(tag):
                        continue
                    if len(_collapse_ws(p.get_text(" ", strip=True))) < 150:
                        tag.decompose()
                        continue
                # 확실한 노이즈가 아니면 함부로 지우지 않음
        except: pass

def _sanitize_html_fragment(node) -> str:
    """
    구조를 최대한 보존한 HTML 조각을 만든다. (CSS는 저장하지 않음)
    - 목표: table/tr/td/th, rowspan/colspan, span/p/br 등 "DOM 구조"가 원본과 최대한 일치.
    - 보안/노이즈: script/style/iframe 등은 제거, 이벤트 핸들러(on*)와 style 속성은 제거.
    - class/id는 유지(나중에 외부 CSS를 적용하거나 구조 매핑에 사용 가능).
    """
    if node is None:
        return ""
    if not BeautifulSoup:
        return ""
    try:
        # 깊은 복사(원본 soup 변형 방지)
        html = str(node)
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
    except Exception:
        return ""

    # 제거할 태그(보안/노이즈)
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "svg"]):
        try:
            tag.decompose()
        except Exception:
            pass

    # 속성 정리(구조 보존 중심)
    # - class/id 유지
    # - table 구조 속성(rowspan/colspan 등) 유지
    # - style/on* 제거 (CSS는 저장하지 않음, XSS 방지)
    global_keep = {"id", "class", "role"}
    per_tag_keep = {
        "a": {"href", "title", "name"},
        "img": {"src", "alt", "title", "width", "height"},
        "td": {"rowspan", "colspan"},
        "th": {"rowspan", "colspan", "scope"},
        "col": {"span"},
    }

    for tag in list(soup.find_all(True)):
        name = (getattr(tag, "name", "") or "").lower()
        try:
            attrs = dict(getattr(tag, "attrs", {}) or {})
        except Exception:
            attrs = {}

        # 이벤트 핸들러/인라인 스타일 제거
        for k in list(attrs.keys()):
            lk = (k or "").lower()
            if lk.startswith("on") or lk == "style":
                try:
                    del tag.attrs[k]
                except Exception:
                    pass

        # 허용 속성만 남김 (data-*, aria-*는 유지)
        try:
            keep = set(global_keep) | set(per_tag_keep.get(name, set()))
            for k in list(getattr(tag, "attrs", {}).keys()):  # type: ignore[union-attr]
                lk = (k or "").lower()
                if lk in keep or lk.startswith("data-") or lk.startswith("aria-"):
                    continue
                try:
                    del tag.attrs[k]
                except Exception:
                    pass
        except Exception:
            pass

        # a/img URL 속성의 javascript: 차단
        if name in {"a", "img"}:
            try:
                key = "href" if name == "a" else "src"
                v = (tag.get(key) or "").strip()
                if v.lower().startswith("javascript:"):
                    del tag.attrs[key]
            except Exception:
                pass

    # 최상위 wrapper를 반환
    try:
        out = str(soup)
        # BeautifulSoup가 자동으로 <html><body>를 붙이는 경우가 있어 제거
        out = out.replace("<html><body>", "").replace("</body></html>", "")
        return out.strip()
    except Exception:
        return ""


def _looks_like_accessibility_skip_container(el) -> bool:
    """접근성 스킵/숨김 컨테이너를 본문 후보에서 제외한다."""
    if el is None:
        return False
    try:
        el_id = (el.get("id") or "").strip().lower()
    except Exception:
        el_id = ""
    try:
        cls_raw = el.get("class") or []
        classes = [str(c).strip().lower() for c in cls_raw if str(c).strip()]
    except Exception:
        classes = []

    class_joined = " ".join(classes)
    skip_class_tokens = ("hidden-tx", "skip", "u_skip", "skip_nav", "skip_navi", "skipnavi", "skipnavigation")
    if any(tok in class_joined for tok in skip_class_tokens):
        return True
    if el_id in {"skip", "u_skip", "skipnavi", "skipnavigation"}:
        return True

    try:
        txt = _collapse_ws(el.get_text(" ", strip=True))
    except Exception:
        txt = ""
    if not txt:
        return False

    if len(txt) <= 16 and ("본문" in txt and ("바로가기" in txt or "시작" in txt)):
        return True
    if len(txt) <= 40 and "스킵네비게이션" in txt:
        return True
    return False


_LAYOUT_CHROME_CLASS_ID_TOKENS = (
    "gnb", "lnb", "snb", "tnb", "nav", "navi", "navbar", "navigation",
    "menu", "menubar", "allmenu", "all-menu", "all_menu", "sitemap",
    "breadcrumb", "location", "path", "quick", "shortcut",
    "header", "footer", "sidebar", "side-bar", "side_bar", "sidemenu", "side-menu", "side_menu",
    "leftmenu", "left-menu", "left_menu", "rightmenu", "right-menu", "right_menu",
    "aside", "wing", "floating", "popup", "modal", "overlay",
    "share", "sns", "util", "toolbar", "print", "satisfaction", "survey", "poll",
    "search", "family-site", "familysite", "related-site", "related_site",
)


_LAYOUT_CHROME_SELECTORS = (
    "header", "footer", "nav", "aside",
    "[role='navigation']", "[role='banner']", "[role='contentinfo']", "[role='search']",
    "#header", "#footer", "#nav", "#gnb", "#lnb", "#snb", "#tnb",
    ".header", ".footer", ".nav", ".gnb", ".lnb", ".snb", ".tnb",
    ".menu", ".menu_wrap", ".menu-wrap", ".menu_area", ".menu-area",
    ".all_menu", ".all-menu", ".allmenu", ".site_map", ".sitemap",
    ".breadcrumb", ".breadcrumbs", ".location", "#location", ".path",
    ".side", ".side_menu", ".side-menu", ".sidemenu", ".sidebar", ".side-bar", ".side_bar",
    ".left_menu", ".left-menu", ".leftmenu", "#left_menu", "#left-menu", "#leftmenu",
    ".right_menu", ".right-menu", ".rightmenu", "#right_menu", "#right-menu", "#rightmenu",
    ".quick", ".quick_menu", ".quick-menu", ".quickmenu", ".wing", ".floating",
    ".top_util", ".top-util", ".util", ".util_menu", ".util-menu", ".toolbar",
    ".sns_share", ".sns-share", ".share", ".share_area", ".share-area",
    ".print_area", ".print-area", ".print_btn", ".print-button",
    ".satisfaction", ".content_satisfaction", ".satisfy", ".survey", ".poll",
    ".search_area", ".search-area", ".family_site", ".family-site", ".familysite",
    ".skip_nav", ".skip-navi", ".skip_navi", ".skipNavi", ".skipnavigation", ".skipNavigation",
    "#skip", "#u_skip", "#skipNavi", "#skipnavigation", "#skipNavigation",
)


_LAYOUT_CHROME_TEXT_MARKERS = (
    "본문바로가기", "본문 바로가기", "본문내용 바로가기", "주메뉴 바로가기",
    "전체메뉴", "메뉴열기", "메뉴 열기", "메뉴닫기", "메뉴 닫기",
    "누리집", "사이트맵", "패밀리사이트", "만족도", "만족도 조사",
    "공유하기", "인쇄하기", "이전글", "다음글", "목록",
)


def _class_id_text(el) -> str:
    if el is None:
        return ""
    parts: list[str] = []
    try:
        parts.append(str(el.get("id") or ""))
    except Exception:
        pass
    try:
        parts.extend(str(c or "") for c in (el.get("class") or []))
    except Exception:
        pass
    try:
        parts.append(str(el.get("role") or ""))
    except Exception:
        pass
    return " ".join(parts).strip().lower()


def _looks_like_layout_chrome_container(el) -> bool:
    """Page chrome: top/bottom/left/right menus, quick links, sharing, search, satisfaction."""
    if el is None:
        return False
    try:
        name = (getattr(el, "name", "") or "").lower()
    except Exception:
        name = ""
    if name in {"header", "footer", "nav", "aside"}:
        return True
    ident = _class_id_text(el)
    ident_parts = {part for part in re.split(r"[^a-z0-9]+", ident) if part}
    if ident:
        for tok in _LAYOUT_CHROME_CLASS_ID_TOKENS:
            tok_parts = {part for part in re.split(r"[^a-z0-9]+", tok) if part}
            if tok in ident_parts or (tok_parts and tok_parts.issubset(ident_parts)):
                return True
    try:
        txt = _collapse_ws(el.get_text(" ", strip=True))
    except Exception:
        txt = ""
    if txt and len(txt) <= 120 and any(marker in txt for marker in _LAYOUT_CHROME_TEXT_MARKERS):
        return True
    return False


def _inside_layout_chrome(el) -> bool:
    try:
        current = el
        while current is not None:
            if _looks_like_layout_chrome_container(current):
                return True
            current = getattr(current, "parent", None)
    except Exception:
        return False
    return False


def is_gachi_pbanc_frontview_url(url: str) -> bool:
    """충북가치자람 지원사업 신청 탭: /portal/pbanc/.../frontView.do"""
    u = (url or "").lower()
    if "gachi.chungbuk.go.kr" not in u:
        return False
    return "/portal/pbanc/" in u and "frontview.do" in u


def gachi_pbanc_frontview_content_root_selector() -> str:
    """
    - 탭형(pbanc08 등): `#pbancContentArea` (상단 h2.sub_title 제외)
    - 단일 공고형(pbanc09 등): `#pbancContentArea` 없음 → `#go_content .inner` 직계 `.support_area.pregnancy`
    """
    return "#pbancContentArea, #go_content .inner > .support_area.pregnancy"


def _select_main_node(soup, *, selector_hint: Optional[str] = None):
    if selector_hint:
        try:
            hinted = [
                el for el in soup.select(selector_hint)
                if not _looks_like_accessibility_skip_container(el) and not _looks_like_layout_chrome_container(el)
            ]
        except Exception:
            hinted = []
        if len(hinted) == 1:
            return hinted[0]
        if len(hinted) > 1:
            combined = soup.new_tag("div", attrs={"class": "hinted-combined-content"})
            for node in hinted:
                combined.append(copy.copy(node))
            return combined

    # 우선순위: 게시글 본문에 자주 쓰이는 컨테이너
    selectors = [
        # 0-a. 춘천시 경제포털(채용/공공일자리) 상세 본문 래퍼
        ".job-support-listview",
        ".co-area.type-info",
        ".board-view-wrap .board-view-cont .board-view-contents",  # 밀양 청년 게시판 등: 버튼/첨부영역 제외한 실제 본문
        ".board-view-cont .board-view-contents",
        ".board-view-contents",

        # 0. 지자체 표준 상세페이지 최상위 컨테이너 (상세정보 탭 포함 영역)
        "#txt",                   # 구로구청 등 다수 지자체에서 본문 전체를 감싸는 ID
        "#mainCont",              # 도봉구청·일부 지자체 본문 래퍼(헤더/GNB 제외)
        ".view_cont",             # 상세정보를 포함한 컨텐츠 영역
        ".tab_content",           # 탭으로 분리된 상세정보 영역
        
        # 1. 지자체/공공기관 표준 및 표 형식 게시판 (강화)
        ".p-table__content",      # 구로구 등 지자체 표준 본문 셀
        ".p-table",               # 공공기관 표 형식 본문 전체
        ".view_content",          # 일반적인 게시판 본문 영역 01
        ".view_area",             # 일반적인 게시판 본문 영역 02
        ".view_cont",             # 일반적인 게시판 본문 영역 03
        "#boardContent",          # Common board body id
        ".board-content",         # Common board body class
        ".board_view_area",       # 게시판 상세 내용 영역
        
        # 2. 국내 주요 CMS (GnuBoard, XE, KBoard 등)
        "#bo_v_atc",              # 그누보드(GnuBoard) 본문 ID
        ".write_contents",        # XE/라이믹스 본문 클래스
        ".kboard-content",        # KBoard 본문 클래스
        
        # 3. 기존 범용 후보군 (유지 및 정렬)
        ".dbData",
        ".dbdata",
        "#go_content",
        "#contentDiv",
        "#content",
        "#contents",
        ".view",
        ".view_wrap",
        "article",
        "main",
        ".content",
        ".contents",
        ".board_view",
        ".board-view",
        
        # 4. 뉴스/블로그 및 현대적 웹 구조
        ".post-body",             # 워드프레스/블로그 등
        ".article-body",          # 뉴스 사이트 등
        ".entry-content",
        ".post-content",
        ".article-content",
    ]

    if selector_hint:
        selectors = [selector_hint] + selectors

    all_found = []

    for sel in selectors:
        try:
            # 기능별 1줄 주석: 설정된 셀렉터와 매칭되는 모든 요소를 리스트에 수집
            found_elements = soup.select(sel)
            for el in found_elements:
                if _looks_like_accessibility_skip_container(el):
                    continue
                if _inside_layout_chrome(el):
                    continue
                if el not in all_found:
                    all_found.append(el)
        except: pass

    if not all_found:
        return soup.body or soup

    # [수정 최소화] 중복 제거: 다른 노드에 포함된 자식 노드는 제외
    matched_nodes = []
    for i, node in enumerate(all_found):
        if not any(node in other.parents for j, other in enumerate(all_found) if i != j):
            matched_nodes.append(node)

    # 결과 반환 로직
    if len(matched_nodes) == 1:
        return matched_nodes[0]
    
    # 기능별 1줄 주석: 여러 독립된 영역이 발견된 경우 하나의 컨테이너로 병합함
    combined_container = soup.new_tag("div", attrs={"class": "extracted-combined-content"})
    for node in matched_nodes:
        # 각 영역 사이에 구분선이나 여백을 주어 가독성을 높일 수 있음
        combined_container.append(copy.copy(node))
    
    return combined_container


def _extract_title(soup, root, *, selector_hint: Optional[str] = None) -> str:
    """후보군 중 가장 적절한 제목을 선택 (우선순위: 전용 클래스 > 태그 > 메타)"""
    candidates: list[tuple[int, str]] = [] # 후보 리스트 초기화 (UnboundLocalError 방지)

    def _strip_status_title_noise(text: str) -> str:
        cleaned = _collapse_ws(text or "")
        if not cleaned:
            return ""
        cleaned = re.sub(
            r"^(?:(?:접수중|접수마감|접수예정|신청중|신청마감|모집중|모집마감|공고중|진행중|진행예정|상시)\s*(?:\([^)]*\))?\s*)+",
            "",
            cleaned,
        ).strip()
        cleaned = re.sub(r"\s+\d{4}\.\d{1,2}\.\d{1,2}\.?\s*$", "", cleaned).strip()
        return cleaned

    def _add(prio: int, txt: Optional[str]) -> None:
        """후보 추가 헬퍼 함수"""
        v = _strip_status_title_noise(txt or "")
        if v: candidates.append((int(prio), v))

    def _strip_caption_category_prefix(txt: str) -> str:
        v = _collapse_ws(str(txt or "").replace("\xa0", " "))
        if not v:
            return ""
        stripped = re.sub(r"^\[[^\]]{1,20}\]\s*", "", v).strip()
        return stripped if len(stripped) >= 6 else v

    def _is_noise_title(t: str) -> bool:
        """메뉴명이나 UI 텍스트인지 판별"""
        lt = (t or "").lower().strip()
        if not t or lt == "insert title here": return True
        if any(x in t for x in ["즐겨찾기", "포털사이트", "<", ">"]): return True
        if any(x in lt for x in ["home", "트위터", "페이스북", "카카오", "프린트", "공유"]): return True
        if len(t) <= 3 or re.fullmatch(r"[\d\W]+", lt): return True
        return False # 모든 검사 통과 시 정상 제목으로 간주

    def _strip_site_suffix(t: str) -> str:
        """제목 끝의 ' - 사이트명' 등 제거"""
        seps = [" - ", " | ", " :: ", " — ", " – ", " · ", " / "]
        for sep in seps:
            if sep in t:
                parts = [p.strip() for p in t.split(sep) if p.strip()]
                if len(parts) >= 2:
                    if sep == " - " and re.search(r"\d", parts[1]):
                        return t
                    return parts[0] # 일반적으로 좌측이 실제 제목
        return t

    # 제목 후보 셀렉터 순서 (태그보다 구체적인 클래스명을 상단 배치하여 구로구청 사례 해결)
    title_selectors = (
        "#bo_v_title .bo_v_tit",  # 그누보드 게시글 보기(송파구육아종합지원센터 등)
        ".poll_view > h4.no_bgimg.mT0",
        ".poll_view h4.no_bgimg",
        ".poll_view > h4",
        ".teacher_cnt strong.area_title",
        ".teacher_cnt .area_title",
        "strong.area_title",
        ".area_title",
        "form.form-inline .program-reserv-wrap .subject h4",
        ".program-reserv-wrap .subject h4",
        ".subject h4",
        "#contentDiv .nw-txbx > h3",
        "#contentDiv .nw-txbx h3",
        ".nw-content-data .nw-txbx > h3",
        ".nw-content-data .nw-txbx h3",
        "#contentDiv .nw-content-data .view.mb20 > p.content-txt",
        "#contentDiv .nw-content-data .view > p.content-txt",
        ".nw-content-data .view.mb20 > p.content-txt",
        ".nw-content-data .view > p.content-txt",
        ".imgboardview-header .view-info .view-title",
        ".contents-wrap .imgboardview-header .view-title",
        ".collection-view .imgboardview-header .view-title",
        "h2.view-title",
        "#content article#content #bo_view .view_tit h5",
        "#content article#content #bo_view .view_tit h4",
        "#content #bo_view .view_tit h5",
        "#content #bo_view .view_tit h4",
        "#bo_view .view_tit h5",
        "#bo_view .view_tit h4",
        ".bo_table .view_tit h4",
        ".view_tit.thumb .info h4",
        ".view_tit .info h4",
        ".bo_table .view_tit h5",
        ".view_tit h4",
        ".view_tit h5",
        "#content .page_cont caption.tit_article",
        "#content caption.tit_article",
        "table caption.tit_article",
        "caption.tit_article",
        "table.board_view_table th[colspan] > b",
        "table.board_view_table tr:first-child th > b",
        "table.board_view_table th > b",
        "div.boardView > h4",
        ".boardView > h4",
        ".inboxRead .headinfo > h3.BoR-h2", "h3.BoR-h2", "h2.BoR-h2",
        "h3.h0.title", ".h0.title", ".poll_view > h4",
        ".p-table__subject_text", ".view_title", ".view-tit", ".subject", ".title", ".tit",
        "h1", "h2", "h3", "h4"
    )

    # 셀렉터 힌트 우선 적용
    if selector_hint:
        try:
            el = soup.select_one(selector_hint)
            if el: _add(0, el.get_text(" ", strip=True))
        except: pass

    for scope in (root, soup):
        try:
            label_title = _generic_title_from_label_table(scope)
            if label_title:
                _add(0, label_title)
                break
        except Exception:
            pass

    # 클래스 및 태그 기반 후보 수집
    for i, sel in enumerate(title_selectors):
        try:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(" ", strip=True)
                if "caption.tit_article" in sel:
                    text = _strip_caption_category_prefix(text)
                _add(i + 1, text)
        except: pass

    # 최종 후보 정제 및 선택
    cleaned = [(pr, c) for pr, c in candidates if c]
    non_noise = [(pr, c) for pr, c in cleaned if not _is_noise_title(c)]

    if non_noise:
        non_noise.sort(key=lambda x: (x[0], -len(x[1]))) # 우선순위 낮은 순, 긴 텍스트 순
        return _strip_site_suffix(non_noise[0][1])
    
    if cleaned:
        cleaned.sort(key=lambda x: (-len(x[1]), x[0])) #Fallback: 가장 긴 것 선택
        return _strip_site_suffix(cleaned[0][1])

    return ""

def _clean_preserve_newline(s: str) -> str:
    """줄바꿈은 보존하고, 가로 공백(스페이스, 탭)만 정리합니다."""
    if not s: return ""
    # 1. 가로 공백(스페이스, 탭)이 2개 이상이면 하나로 축소
    s = re.sub(r"[ \t]+", " ", s)
    # 2. 각 라인의 앞뒤 공백 정리
    lines = [line.strip() for line in s.splitlines()]
    # 3. 3개 이상의 연속된 줄바꿈은 2개로 축소 (가독성)
    result = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", result).strip()

def _extract_content_text(root) -> str:
    """기능별 1줄 주석: 표의 tr 단위 개행과 인라인 공백 처리를 통해 단어 뭉침과 쪼개짐을 동시에 해결""" # 1줄 주석
    if root is None: return ""
    try:
        # 1. 표 구조 보존: 셀 사이 | 삽입 및 행(tr) 뒤 개행
        for tr in root.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            for i, cell in enumerate(cells):
                if i < len(cells) - 1: cell.insert_after(' | ')
            tr.insert_after('\n')

        # 2. 블록 태그 개행 삽입
        BLOCK_TAGS = ['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'br', 'section', 'article']
        for tag in root.find_all(BLOCK_TAGS):
            tag.insert_after('\n')

        # 3. [핵심] separator=' '를 사용하여 span 등으로 쪼개진 단어 결합
        text = root.get_text(separator=' ')
        
        # 4. 가로 공백 및 문단 정제
        text = re.sub(r"(\s*\|\s*)+", " | ", text)
        text = _clean_body_text(text)
        return text
    except Exception: return _collapse_ws(root.get_text(separator=' '))

# 목차 점선 뒤 쪽수+다음 항목: ".......... 12.민" → ".......... 1" + 개행 + "2.민" (점 뒤 공백 없어도 됨)
_TOC_LEADER_GLUE = re.compile(
    r"((?:\.|·|⋯|․){2,}[\s\u00a0]*)(\d+?)((?:[2-9]|1[0-9]|20)\.(?=\s|[가-힣A-Za-z]|$))",
    re.UNICODE,
)


def _split_toc_page_from_next_section(s: str) -> str:
    """
    점선·점 문자 뒤에 오는 숫자 열이 '쪽수 + 바로 이어진 N.(목차)'로 붙은 경우만 줄바꿈.
    - \\d+? 로 쪽수를 최소로 잡아 12. → 1 / 2., 673. → 67 / 3. 처럼 복원.
    - 일반 본문의 '12. 제목'은 앞에 점선이 없으므로 여기서 건드리지 않음.
    """

    def _repl(m: re.Match) -> str:
        return m.group(1) + m.group(2) + "\n" + m.group(3)

    return _TOC_LEADER_GLUE.sub(_repl, s)


def _clean_body_text(s: str) -> str:
    """기능별 1줄 주석: 가. 나. 다. 및 □ 등의 기호 문단을 찾아 줄바꿈을 지능적으로 복원""" # 1줄 주석
    if not s: return ""
    for symbol in ["※", "☞", "⇒", "□", "○"]:
        s = s.replace(symbol, f"\n{symbol}")
    
    # 한글 목록(가. 나.) 및 기호(*, -) 앞 줄바꿈 삽입
    s = re.sub(r"\s+(?=[가-하]\s*[\.\)])", r"\n", s)
    s = re.sub(r"\s+(?=[□○●※\-\*])", r"\n", s)
    
    # 괄호 뒤 붙은 숫자 목록 분리
    s = re.sub(r"([\)가-힣])(?=(?:[1-9]|1[0-9]|20)\.\s*[가-힣A-Za-z])", r"\1\n", s)
    
    s = re.sub(r"[ \t]+", " ", s)
    s = _format_numbered_list_lines(s)
    
    lines = [line.strip() for line in s.splitlines() if line.strip()]
    return "\n".join(lines).strip()

def _noise_penalty_score(t: str) -> int:
    """
    '본문이 아닌 UI/네비/메타 텍스트'일 가능성을 패널티로 환산한다.
    - 사이트 맞춤형이 아니라, 포털/게시판에서 흔히 등장하는 토큰을 공용으로 사용한다.
    """
    if not t:
        return 10_000
    tokens = (
        # navigation / chrome
        "본문내용 바로가기", "주메뉴 바로가기", "메뉴 닫기", "메뉴 열기",
        "주간 인기 검색어", "월간 인기 검색어", "네비게이션바", "전체메뉴열기", "전체메뉴닫기",
        "로그인", "회원가입", "Language", "English", "中文", "日本語",
        "HOME >", "사이트", "패밀리", "누리집", "바로가기", "본문바로가기",
        "전체메뉴", "검색영역", "소통참여", "통합구민참여",
        # share / utility
        "트위터", "페이스북", "카카오", "공유", "프린트", "인쇄",
        "전자점자", "뷰어", "다운로드", "바로보기", "바로듣기",
        # board meta
        "부서", "작성자", "전화번호", "등록일", "수정일", "조회수",
        "첨부파일", "목록", "이전글", "다음글",
        # misc
        "Insert title here", "게시물 저장 중입니다",
    )
    hits = 0
    for tok in tokens:
        if tok and tok in t:
            hits += 1
    # 길이가 너무 길면서 토큰도 많으면(헤더/푸터 포함) 추가 패널티
    extra = 0
    if len(t) > 1200 and hits >= 4:
        extra = 600
    return (hits * 120) + extra

def _pick_best_content_text(soup, root, *, title: str):
    """
    공용 본문 선택 로직:
    - 여러 후보 컨테이너의 텍스트를 뽑아 '길이 - 노이즈패널티'로 점수화해서 최적을 선택한다.
    - '제목만 들어있는 컨테이너'(t==title)는 강하게 패널티 처리하여 본문 오인을 방지한다.
    """
    candidates = []
    seen_ids = set()

    def _add_candidate(el, *, weight: int = 0, tag_penalty: int = 0) -> None:
        if el is None:
            return
        if _looks_like_accessibility_skip_container(el):
            return
        if _inside_layout_chrome(el):
            return
        try:
            key = id(el)
        except Exception:
            key = None
        if key is not None and key in seen_ids:
            return
        if key is not None:
            seen_ids.add(key)
        try:
            txt = _collapse_ws(el.get_text(" ", strip=True))
        except Exception:
            txt = ""
        if not txt:
            return
        if _looks_like_skip_navigation_only_text(txt):
            return
        # 짧은 메타/네비 텍스트는 후보에서 제외 (예: '목록', '이전글', '다음글' 등)
        try:
            if len(txt) < 40 and any(tok in txt for tok in ("목록", "이전글", "다음글", "첨부파일", "조회수", "등록일", "작성자")):
                return
        except Exception:
            pass
        # 너무 짧은 텍스트는 보통 제목/메타 조각이므로 제외(단, 짧지만 의미있는 케이스는 title 비교로 살림)
        if len(txt) < 12 and (not title or txt != title):
            return
        raw_len = len(txt)
        penalty = _noise_penalty_score(txt) + int(tag_penalty or 0)
        if ">" in txt[:240] and any(marker in txt for marker in ("게시글 상세 보기", "게시물 상세 보기", "상세 보기", "상세보기")):
            penalty += 900
        # 제목-only 컨테이너 강한 패널티(본문 오인 방지)
        if title and txt == title and raw_len < 80:
            penalty += 1000
        # 과도하게 긴 텍스트(페이지 전체/헤더+푸터 포함)는 일반적으로 노이즈가 섞이므로 길이 기반 패널티
        # (도메인 맞춤이 아닌 공용 규칙)
        length_penalty = 0
        if raw_len > 1200:
            length_penalty = min(2000, (raw_len - 1200) // 2)

        # 점수: 길이는 1000까지만 유리하게 반영(너무 길면 위 length_penalty로 불리)
        score = (min(raw_len, 1000) + int(weight or 0)) - penalty - length_penalty
        candidates.append((score, txt, el))

    # 1) root(이미 main 후보) 우선 고려
    _add_candidate(root, weight=60)

    # 2) 흔한 본문 컨테이너 후보들
    selector_list = (
        # 춘천시 경제포털(채용/공공일자리) 상세
        (".job-support-listview", 680, 0),
        (".co-area.type-info", 520, 0),
        (".board-view-wrap .board-view-cont .board-view-contents", 760, 0),
        (".board-view-cont .board-view-contents", 720, 0),
        (".board-view-contents", 680, 0),
        (".view_cont", 700, 0),
        (".view-cont", 700, 0),
        (".view_content", 680, 0),
        ("#boardContent", 760, 0),
        (".board-content", 720, 0),
        ("#txt", 660, 0),
        ("#mainCont", 660, 0),
        (".tab_content", 640, 0),

        # 흔한 '본문' 컨테이너(가중치 높게)
        (".dbData", 520, 0),
        (".dbdata", 520, 0),
        ("#content", 220, 0),
        ("#contents", 220, 0),
        (".view", 200, 0),
        (".view_wrap", 180, 0),
        (".p-table", 220, 0),  # 춘천시 등 계약정보 표
        ("article", 160, 0),
        ("main", 40, 120),  # main은 헤더/메타가 섞이는 경우가 많아 약한 패널티
        (".content", 120, 0),
        (".contents", 120, 0),
        (".board_view", 160, 0),
        (".board-view", 160, 0),
        (".post", 140, 0),
        (".post-content", 140, 0),
        (".entry-content", 140, 0),
        (".article", 120, 0),
        (".article-content", 120, 0),
        (".board_view_content", 160, 0),
        (".board_view_area", 160, 0),
        (".article-body", 160, 0),
        (".viewArticle", 160, 0),
    )
    for sel, w, tp in selector_list:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        _add_candidate(el, weight=w, tag_penalty=tp)

    # 3) 최종 선택
    if not candidates:
        return "", None
    candidates.sort(reverse=True, key=lambda x: x[0])
    best_txt = candidates[0][1]
    best_el = candidates[0][2]
    return best_txt, best_el


def _nowon_subtree_to_plain(node) -> str:
    """노원 인쇄영역 부분 트리 → 자연스러운 평문(인라인은 공백으로 이어 붙임, 표는 |/줄 유지)."""
    if node is None:
        return ""
    try:
        frag = BeautifulSoup(str(node), "html.parser")
        root = frag.find(True)
        if not root:
            return ""
        t = _extract_content_text(root).strip()
        return t.replace("\u00a0", " ")
    except Exception:
        try:
            return _clean_preserve_newline(node.get_text("\n", strip=True)).strip().replace("\u00a0", " ")
        except Exception:
            return ""


def _try_extract_nowon_printarea_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    노원구청(nowon.kr) 상세 공통: #printArea .article-view
    게시·사전정보공개(h1), 온라인접수(BD_selectOnlineRcept, h4) 모두 동일 뼈대.
    메타 표(table.table-article) + 본문(.article-body)만 사용해 GNB/푸터를 배제한다.
    """
    if not soup or not url:
        return None
    u = url.lower()
    if "nowon.kr" not in u:
        return None
    view = soup.select_one("#printArea .article-view")
    if not view:
        return None
    title_el = view.select_one(
        "h1.article-subject, h1.article_subject, h4.article-subject, h4.article_subject"
    )
    title = _collapse_ws(title_el.get_text(" ", strip=True)) if title_el else ""
    if not title:
        return None

    meta_tbl = view.select_one("table.table-article") or view.select_one("table.table.table-article")
    body_el = view.select_one(".article-body")

    parts: list[str] = []
    if meta_tbl:
        parts.append(_nowon_subtree_to_plain(meta_tbl))
    if body_el:
        parts.append(_nowon_subtree_to_plain(body_el))
    content_text = "\n\n".join(p for p in parts if p and p.strip())
    content_text = _format_numbered_list_lines(content_text)
    if not content_text.strip():
        return None

    frag = soup.new_tag("div", attrs={"class": "nowon-extract-wrap"})
    if meta_tbl:
        frag.append(copy.copy(meta_tbl))
    if body_el:
        frag.append(copy.copy(body_el))
    content_html = _sanitize_html_fragment(frag).strip()
    snippet = _collapse_ws(content_text)[:200]

    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


_CHUNCHEON_PORTAL_UI_ANCHOR_TEXTS = frozenset(
    {
        "복사",
        "복사하기",
        "링크복사",
        "링크 복사",
        "링크 복사하기",
        "링크더보기",
        "링크 더보기",
        "더보기",
        "프린트",
        "프린트하기",
        "인쇄",
        "인쇄하기",
        "공유",
        "공유하기",
        "닫기",
        "트위터",
        "페이스북",
        "페이스북 공유가기",
        "카카오스토리",
        "카카오톡",
        "블로그",
        "의견등록",
        "현재 페이지에서 제공하는 정보에 대하여 만족하셨습니까?",
    }
)


def _strip_chuncheon_portal_ui_noise(root) -> None:
    """
    춘천시청 계열 서브사이트(복지·본청 상세 등) 본문 블록에 붙는
    공유/복사/인쇄/만족도 조사 UI를 제거한다. (원본 soup 오염 방지: 복제 노드에만 호출)
    """
    if root is None:
        return
    selectors = (
        ".url-copy",
        ".btn-wrap",
        ".btn_area",
        ".share_area",
        ".sns_area",
        ".sns_wrap",
        ".share_wrap",
        "ul.link",
        "li.link-copy",
        ".view_util",
        ".view_sns",
        ".print_area",
        ".sns_share",
        ".content_satisfaction",
        ".contents-bottom",
        ".common-area",
        ".sub-top",
        "[class*='sns-link']",
        "[class*='share_link']",
    )
    for sel in selectors:
        try:
            for el in list(root.select(sel)):
                try:
                    el.decompose()
                except Exception:
                    pass
        except Exception:
            pass
    for tag in list(root.find_all(["button", "input"])):
        try:
            if tag.name == "input":
                t = (tag.get("type") or "").lower()
                if t not in ("button", "submit", "reset", "image"):
                    continue
            tag.decompose()
        except Exception:
            pass
    for tag in list(root.select("[role='button']")):
        try:
            tag.decompose()
        except Exception:
            pass
    # 요청사항: 춘천시청 상세 본문에서는 링크성 UI 노이즈가 많아 a 태그를 전부 제외한다.
    for a in list(root.find_all("a")):
        try:
            a.decompose()
        except Exception:
            pass


def _try_extract_chuncheon_post(soup, url: str) -> Optional[BoardPostExtract]:
    """춘천시청 상세: `chuncheon_parse.parse_chuncheon_detail_soup` 일원화."""
    from backend.board import chuncheon_parse

    r = chuncheon_parse.parse_chuncheon_detail_soup(soup, url)
    if not r:
        return None
    return BoardPostExtract(
        url=url,
        title=r.title,
        content_text=r.content_text,
        content_html=r.content_html,
        snippet=r.snippet,
    )


def _try_extract_chungnam_youth_bbs_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    충남청년포털 공지·게시판 상세(/web/main/bbs/...).
    제목: #container .content_in_wrap .sInner > h5, 본문: .ntc_board_view
    """
    if not soup or not url:
        return None
    u = (url or "").lower()
    if "youth.chungnam.go.kr" not in u or "/bbs/" not in u:
        return None

    title_el = soup.select_one("#container .content_in_wrap .sInner > h5") or soup.select_one(
        ".content_in_wrap .sInner > h5"
    )
    title = _collapse_ws(title_el.get_text(" ", strip=True)) if title_el else ""

    view = soup.select_one(".ntc_board_view")
    if not view:
        return None

    if not title:
        meta = soup.select_one("meta[property='og:title']") or soup.select_one("title")
        if meta:
            raw = meta.get("content") if meta.name == "meta" else meta.get_text(" ", strip=True)
            title = _collapse_ws(raw or "")
            title = re.split(r"\s*(?:\||｜|-)\s*", title or "")[0].strip()
    if not title:
        title = "제목 없음"

    content_text = _extract_content_text(copy.copy(view))
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not content_text.strip():
        return None

    frag = soup.new_tag("div", attrs={"class": "chungnam-youth-bbs-extract-wrap"})
    frag.append(copy.copy(view))
    content_html = _sanitize_html_fragment(frag).strip()
    snippet = _collapse_ws(content_text)[:200]

    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_chungnam_youth_space_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    충남청년포털 청년 공간정보 상세(/youthSpaceInfo/.../view).
    제목: .thum_desc .sub_tit h4 (상단 h3는 메뉴/섹션명), 본문: .cif_spc_box
    """
    if not soup or not url:
        return None
    u = (url or "").lower()
    if "youth.chungnam.go.kr" not in u or "youthspaceinfo" not in u or "/view" not in u:
        return None

    box = soup.select_one("#container .content_in_wrap .sInner .cif_spc_box")
    if not box:
        return None

    title_el = (
        soup.select_one("#container .content_in_wrap .sInner .cif_spc_box .thum_desc .sub_tit h4")
        or soup.select_one(".cif_spc_box .thum_desc .sub_tit h4")
        or soup.select_one(".thum_desc .sub_tit h4")
    )
    title = _collapse_ws(title_el.get_text(" ", strip=True)) if title_el else ""

    if not title:
        meta = soup.select_one("meta[property='og:title']") or soup.select_one("title")
        if meta:
            raw = meta.get("content") if meta.name == "meta" else meta.get_text(" ", strip=True)
            title = _collapse_ws(raw or "")
            title = re.split(r"\s*(?:\||｜|-)\s*", title or "")[0].strip()
    if not title:
        title = "제목 없음"

    content_text = _extract_content_text(copy.copy(box))
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not content_text.strip():
        return None

    frag = soup.new_tag("div", attrs={"class": "chungnam-youth-space-extract-wrap"})
    frag.append(copy.copy(box))
    content_html = _sanitize_html_fragment(frag).strip()
    snippet = _collapse_ws(content_text)[:200]

    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_chungnam_youth_custom_supp_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    충남청년포털(youth.chungnam.go.kr) 맞춤형 청년정책 상세(/customSupp/.../view).

    인기 검색어·전체메뉴·네비게이션은 #container 바깥/형제 레이어에 있고,
    실제 상세 본문은 `.content_in_wrap .sInner` 안의 `.sub_tit`(한 줄 요약)과
    `form#customSuppForm`(사업개요·신청자격 등)으로 한정된다.
    """
    if not soup or not url:
        return None
    u = (url or "").lower()
    if "youth.chungnam.go.kr" not in u or "customsupp" not in u:
        return None

    form_el = soup.select_one("form#customSuppForm")
    if not form_el:
        return None

    title_el = soup.select_one("#container .content_in_wrap .sInner > h3")
    title = _collapse_ws(title_el.get_text(" ", strip=True)) if title_el else ""
    if not title:
        for li in form_el.select("li"):
            lab = li.find(["span", "dt", "th", "strong"])
            if not lab:
                continue
            if re.sub(r"\s+", "", lab.get_text("", strip=True) or "") != "정책명":
                continue
            val = lab.find_next_sibling(["dd", "td", "span", "div"])
            if val:
                title = _collapse_ws(val.get_text(" ", strip=True))
                break

    inner = soup.select_one("#container .content_in_wrap .sInner")
    sub_el = inner.select_one(".sub_tit") if inner else None

    text_parts: list[str] = []
    frag = soup.new_tag("div", attrs={"class": "chungnam-youth-extract-wrap"})
    if sub_el and sub_el.get_text(strip=True):
        text_parts.append(_extract_content_text(copy.copy(sub_el)))
        frag.append(copy.copy(sub_el))
    text_parts.append(_extract_content_text(copy.copy(form_el)))
    frag.append(copy.copy(form_el))

    content_text = "\n\n".join(p.strip() for p in text_parts if p and p.strip())
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not content_text.strip():
        return None

    if not title:
        title = "제목 없음"

    content_html = _sanitize_html_fragment(frag).strip()
    snippet = _collapse_ws(content_text)[:200]

    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_umppa_kidscafe_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    탄생육아(umppa.seoul.go.kr) 상세 전용.
    - 키즈카페/게시판 상세에서 공통 제목 셀렉터를 우선 시도한다.
    - 본문은 .viewWrap 계열과 게시판 .board-content 계열을 모두 지원한다.
    """
    if not soup or not url:
        return None
    lu = (url or "").lower()
    if "umppa.seoul.go.kr" not in lu:
        return None
    if "/kidscafe/" not in lu and "bordcontdetail.do" not in lu:
        return None

    title = ""
    for sel in (
        ".board-detail .board-top .title",
        ".board-top .title-wrap h3.title",
        ".board-top .title",
        ".kidscafe_title h2.sub_title01",
        ".content-info .title-wrap h2.title",
    ):
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            title = _collapse_ws(el.get_text(" ", strip=True))
            if title:
                break

    body_el = (
        soup.select_one(".viewWrap")
        or soup.select_one(".viewwrap")
        or soup.select_one(".board-detail .viewWrap")
        or soup.select_one(".board-detail .board-content")
        or soup.select_one(".board-content")
        or soup.select_one(".board-detail")
    )
    if not body_el:
        return None

    text_parts: list[str] = []
    if title:
        text_parts.append(title)
    body_text = _extract_content_text(copy.copy(body_el)).strip()
    if body_text:
        text_parts.append(body_text)

    content_text = "\n\n".join(p for p in text_parts if p and p.strip())
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not content_text.strip():
        return None

    frag = soup.new_tag("div", attrs={"class": "umppa-kidscafe-extract-wrap"})
    if title:
        title_el = soup.new_tag("h2")
        title_el.string = title
        frag.append(title_el)
    frag.append(copy.copy(body_el))

    content_html = _sanitize_html_fragment(frag).strip()
    snippet = _collapse_ws(content_text)[:200]
    if not title:
        title = "제목 없음"

    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_mme_or_kr_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    중견기업정보마당(mme.or.kr) 게시판 상세.
    화면의 .view_cont는 비어 있고 JS가 채우며, 정적 HTML에는 input#email_cn_html[value]에
    이스케이프된 본문 HTML이 들어 있다.
    """
    if not soup or not url:
        return None
    if "mme.or.kr" not in (url or "").lower():
        return None

    inp = soup.select_one("input#email_cn_html")
    raw = (inp.get("value") or "").strip() if inp else ""
    if not raw:
        return None

    try:
        decoded = _html_unescape(raw)
    except Exception:
        return None

    inner = BeautifulSoup(f'<div class="mme-board-cn-wrap">{decoded}</div>', "html.parser")
    root = inner.select_one(".mme-board-cn-wrap")
    if not root:
        return None

    content_text = _extract_content_text(copy.copy(root))
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = (content_text or "").strip()
    if not content_text:
        return None

    title = ""
    h3 = soup.select_one(".list_view h3")
    if h3 and h3.get_text(strip=True):
        title = _collapse_ws(h3.get_text(" ", strip=True))
    if not title:
        vt = soup.select_one(".view_tit")
        if vt:
            t = vt.get_text(" ", strip=True)
            if t:
                title = _collapse_ws(t.split("|")[0].strip())
    if not title:
        title = _collapse_ws(_extract_title(soup, soup, selector_hint=".list_view h3"))

    if not title:
        title = "제목 없음"

    frag = soup.new_tag("div", attrs={"class": "mme-or-kr-extract-wrap"})
    frag.append(copy.copy(root))

    content_html = _sanitize_html_fragment(frag).strip()
    snippet = _collapse_ws(content_text)[:200]

    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_identity_kisa_post(soup, url: str) -> Optional[BoardPostExtract]:
    """
    KISA 본인확인 지원포털(identity.kisa.or.kr) 상세.
    - 가이드/지침: `td.bo_con`
    - 교육영상(edu_all·edu_user 등): `th` 없는 행의 본문 `td`(첨부 행보다 긴 텍스트)
    참고: guide/113, edu_all/45
    """
    if not soup or not url:
        return None
    if "identity.kisa.or.kr" not in (url or "").lower():
        return None

    try:
        from backend.board.kisa_identity_parse import (
            extract_identity_kisa_board_title,
            select_identity_kisa_content_root,
        )
    except Exception:
        return None

    bo = select_identity_kisa_content_root(soup)
    if not bo:
        return None

    title = (extract_identity_kisa_board_title(soup, url=url) or "").strip()
    if not title:
        title = "제목 없음"

    content_text = _extract_content_text(copy.copy(bo))
    content_text = (content_text or "").strip()
    if not content_text:
        return None

    if title and title != "제목 없음":
        pure_title = re.escape(_collapse_ws(title))
        if len(content_text) > len(title) + 20:
            content_text = re.sub(
                rf"^{pure_title}(\s*(안내|상세|보기|본문))*",
                "",
                content_text,
                flags=re.IGNORECASE | re.DOTALL,
            ).lstrip()

    frag = soup.new_tag("div", attrs={"class": "kisa-identity-extract-wrap"})
    frag.append(copy.copy(bo))
    content_html = _sanitize_html_fragment(frag).strip()
    snippet = _collapse_ws(content_text)[:200]

    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_edu_ingang_notice_post(soup, url: str) -> Optional[BoardPostExtract]:
    """강남인강 공지/커뮤니티 상세는 `#contents`만 본문으로 사용한다."""
    if not soup or not url:
        return None
    if "edu.ingang.go.kr" not in (url or "").lower():
        return None

    body = soup.select_one("#contents")
    if not body:
        return None

    view = body.find_parent(class_="board_view") or soup.select_one(".board_view")
    title = ""
    if view is not None:
        for sel in ("h1", "h2", "h3", ".title", ".subject"):
            el = view.select_one(sel)
            if not el:
                continue
            txt = _collapse_ws(el.get_text(" ", strip=True))
            if txt:
                title = txt
                break
    if not title:
        title = (_extract_title(soup, view or body) or "").strip()
    if not title:
        title = "제목 없음"

    try:
        frag = BeautifulSoup(str(body), "html.parser")
    except Exception:
        return None
    root = frag.find(True)
    if not root:
        return None

    _strip_noisy_tags(frag)
    root = frag.find(True)
    if not root:
        return None

    content_text = _extract_content_text(root)
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not (content_text or "").strip():
        return None

    wrap = soup.new_tag("div", attrs={"class": "edu-ingang-notice-extract-wrap"})
    wrap.append(copy.copy(root))
    content_html = _sanitize_html_fragment(wrap).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def _try_extract_k_cohesion_post(soup, url: str) -> Optional[BoardPostExtract]:
    """k-cohesion.go.kr Manpa board detail pages."""
    if not soup or not url:
        return None
    u_low = (url or "").lower()
    if "k-cohesion.go.kr" not in u_low or "/pcnc/contents/" not in u_low:
        return None
    if "schm=view" not in u_low and "uid=view_" not in u_low:
        return None

    view = soup.select_one("#sub_content .board_detail_wrap") or soup.select_one(".board_detail_wrap")
    if not view:
        return None

    title_el = view.select_one(".detail_tit")
    title = ""
    if title_el:
        title_node = copy.copy(title_el)
        for category in title_node.select(".category"):
            category.decompose()
        title = _collapse_ws(title_node.get_text(" ", strip=True))
    if not title:
        title = (_extract_title(soup, view) or "").strip()
    if not title:
        title = "제목 없음"

    meta_lines: list[str] = []
    info = view.select_one(".detail_info")
    if info:
        for span in info.select(".info"):
            text = _collapse_ws(span.get_text(" ", strip=True))
            if text and text not in meta_lines:
                meta_lines.append(text)

    body = view.select_one(".detail_con .pre_wrap") or view.select_one(".detail_con")
    body_text = _extract_content_text(copy.copy(body)).strip() if body else ""

    image_lines: list[str] = []
    if body:
        seen_images: set[str] = set()
        for img in body.select("img[src]"):
            src = urljoin(url, str(img.get("src") or "").strip())
            if not src or src in seen_images:
                continue
            seen_images.add(src)
            alt = _collapse_ws(img.get("alt") or img.get("title") or "")
            image_lines.append(f"이미지: {alt} ({src})" if alt else f"이미지: {src}")

    attachment_lines: list[str] = []
    seen_files: set[str] = set()
    for a in view.select(".attached_wrap a[href], #fileDiv a[href], [data-uploaded-box] a[href]"):
        href = urljoin(url, str(a.get("href") or "").strip())
        if not href or href in seen_files:
            continue
        seen_files.add(href)
        name = _collapse_ws(a.get_text(" ", strip=True) or a.get("title") or "")
        if name:
            attachment_lines.append(f"첨부파일: {name} ({href})")
        else:
            attachment_lines.append(f"첨부파일: {href}")

    html_text = str(view)
    for file_id in re.findall(r"""fileId\s*=\s*['"]?([A-Za-z0-9_-]{8,})""", html_text):
        api_url = urljoin(url, f"/afile/fileList.do?fileId={file_id}")
        if api_url not in seen_files:
            seen_files.add(api_url)
            attachment_lines.append(f"첨부파일 목록 API: {api_url}")

    parts: list[str] = [title]
    parts.extend(meta_lines)
    if body_text:
        parts.append(body_text)
    parts.extend(image_lines)
    parts.extend(attachment_lines)
    content_text = "\n".join(part for part in parts if _collapse_ws(part)).strip()
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    if not content_text:
        return None

    frag = soup.new_tag("div", attrs={"class": "k-cohesion-extract-wrap"})
    heading = soup.new_tag("h2")
    heading.string = title
    frag.append(heading)
    if info:
        frag.append(copy.copy(info))
    if body:
        frag.append(copy.copy(body))
    for line in attachment_lines:
        p = soup.new_tag("p")
        p.string = line
        frag.append(p)

    content_html = _sanitize_html_fragment(frag).strip()
    snippet = _collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def extract_board_post(html: str, *, url: str, selector_profile: Optional[Dict[str, Any]] = None) -> Optional[BoardPostExtract]:
    # 요약: 게시글의 제목과 본문을 추출하고 줄바꿈을 보존하여 반환하는 메인 로직
    if not html or not url: return None # 입력값이 없으면 종료

    try:
        soup = BeautifulSoup(html, "html.parser") # HTML 파싱 객체 생성
    except Exception: return None

    u_low = (url or "").lower()

    try:
        from backend.board.gangnam_board import is_gangnam_error_page, is_gangnam_family_url

        if is_gangnam_family_url(url or "") and is_gangnam_error_page(soup):
            return None
    except Exception:
        pass

    k_cohesion_post = _try_extract_k_cohesion_post(soup, url)
    if k_cohesion_post:
        return k_cohesion_post

    try:
        from backend.board.youthcenter_board import (
            is_youthcenter_bbs_view_url,
            is_youthcenter_event_detail_url,
            is_youthcenter_openapi_doc_url,
            is_youthcenter_policy_detail_url,
            try_extract_youthcenter_post,
        )

        youthcenter_openapi_post = try_extract_youthcenter_post(soup, url, html=html)
    except Exception:
        youthcenter_openapi_post = None
        is_youthcenter_bbs_view_url = None  # type: ignore[assignment]
        is_youthcenter_event_detail_url = None  # type: ignore[assignment]
        is_youthcenter_openapi_doc_url = None  # type: ignore[assignment]
        is_youthcenter_policy_detail_url = None  # type: ignore[assignment]
    if youthcenter_openapi_post:
        return BoardPostExtract(
            url=url,
            title=youthcenter_openapi_post.title,
            content_text=youthcenter_openapi_post.content_text,
            content_html=youthcenter_openapi_post.content_html,
            snippet=youthcenter_openapi_post.snippet,
        )
    if any(
        checker and checker(url)
        for checker in (
            is_youthcenter_bbs_view_url,
            is_youthcenter_policy_detail_url,
            is_youthcenter_event_detail_url,
            is_youthcenter_openapi_doc_url,
        )
    ):
        return None

    nowon_post = _try_extract_nowon_printarea_post(soup, url)
    if nowon_post:
        return nowon_post

    try:
        from backend.board.jongno_board import (
            extract_jongno_apply_post,
            extract_jongno_construction_status,
            extract_jongno_council_assembly_post,
            extract_jongno_council_post,
            extract_jongno_general_board_post,
            extract_jongno_minwon_form,
        )

        jongno_post = (
            extract_jongno_council_assembly_post(soup, url or "")
            or
            extract_jongno_council_post(soup, url or "")
            or extract_jongno_apply_post(soup, url or "")
            or extract_jongno_construction_status(soup, url or "")
            or extract_jongno_minwon_form(soup, url or "")
            or extract_jongno_general_board_post(soup, url or "")
        )
    except Exception:
        jongno_post = None
    if jongno_post:
        return BoardPostExtract(
            url=url,
            title=str(jongno_post.get("title") or ""),
            content_text=str(jongno_post.get("content_text") or ""),
            content_html=str(jongno_post.get("content_html") or ""),
            snippet=str(jongno_post.get("snippet") or ""),
        )

    dobong_post = _try_extract_dobong_main_post(soup, url)
    if dobong_post:
        return dobong_post

    hscity_photo_post = _try_extract_hscity_photo_post(soup, url)
    if hscity_photo_post:
        return hscity_photo_post

    hscity_post = _try_extract_hscity_board_post(soup, url)
    if hscity_post:
        return hscity_post

    copyright_post = _try_extract_copyright_board_post(soup, url)
    if copyright_post:
        return copyright_post

    try:
        from backend.board.dongjak_board import try_extract_dongjak_post
    except Exception:
        try_extract_dongjak_post = None  # type: ignore[assignment]

    dongjak_post = try_extract_dongjak_post(soup, url) if try_extract_dongjak_post else None
    if dongjak_post:
        return dongjak_post

    try:
        from backend.board.guro_board import try_extract_guro_post
    except Exception:
        try_extract_guro_post = None  # type: ignore[assignment]

    guro_post = try_extract_guro_post(soup, url) if try_extract_guro_post else None
    if guro_post:
        return guro_post

    try:
        from backend.board.gm_board import (
            try_extract_gm_contract_post,
            try_extract_gm_epeople_iframe_post,
            try_extract_gm_festival_post,
            try_extract_gm_general_post,
            try_extract_gm_group_info_post,
            try_extract_gm_lobas_tcm_post,
            try_extract_gm_nftc_post,
            try_extract_gm_static_info_post,
        )
    except Exception:
        try_extract_gm_contract_post = None  # type: ignore[assignment]
        try_extract_gm_epeople_iframe_post = None  # type: ignore[assignment]
        try_extract_gm_festival_post = None  # type: ignore[assignment]
        try_extract_gm_general_post = None  # type: ignore[assignment]
        try_extract_gm_group_info_post = None  # type: ignore[assignment]
        try_extract_gm_lobas_tcm_post = None  # type: ignore[assignment]
        try_extract_gm_nftc_post = None  # type: ignore[assignment]
        try_extract_gm_static_info_post = None  # type: ignore[assignment]

    gm_epeople_iframe_post = try_extract_gm_epeople_iframe_post(soup, url) if try_extract_gm_epeople_iframe_post else None
    if gm_epeople_iframe_post:
        return gm_epeople_iframe_post

    gm_lobas_tcm_post = try_extract_gm_lobas_tcm_post(soup, url) if try_extract_gm_lobas_tcm_post else None
    if gm_lobas_tcm_post:
        return gm_lobas_tcm_post

    gm_group_info_post = try_extract_gm_group_info_post(soup, url) if try_extract_gm_group_info_post else None
    if gm_group_info_post:
        return gm_group_info_post

    gm_festival_post = try_extract_gm_festival_post(soup, url) if try_extract_gm_festival_post else None
    if gm_festival_post:
        return gm_festival_post

    gm_static_info_post = try_extract_gm_static_info_post(soup, url) if try_extract_gm_static_info_post else None
    if gm_static_info_post:
        return gm_static_info_post

    gm_contract_post = try_extract_gm_contract_post(soup, url) if try_extract_gm_contract_post else None
    if gm_contract_post:
        return gm_contract_post

    gm_post = try_extract_gm_general_post(soup, url) if try_extract_gm_general_post else None
    if gm_post:
        return gm_post

    gm_nftc_post = try_extract_gm_nftc_post(soup, url) if try_extract_gm_nftc_post else None
    if gm_nftc_post:
        return gm_nftc_post

    try:
        from backend.board.seongbuk_board import try_extract_seongbuk_post
    except Exception:
        try_extract_seongbuk_post = None  # type: ignore[assignment]

    seongbuk_post = try_extract_seongbuk_post(soup, url) if try_extract_seongbuk_post else None
    if seongbuk_post:
        return seongbuk_post

    gokams_post = _try_extract_gokams_board_post(soup, url)
    if gokams_post:
        return gokams_post

    ne_post = _try_extract_ne_board_post(soup, url)
    if ne_post:
        return ne_post

    try:
        from backend.board.hu_minwon_board import try_extract_hu_minwon_post
    except Exception:
        try_extract_hu_minwon_post = None  # type: ignore[assignment]

    hu_minwon_post = try_extract_hu_minwon_post(soup, url) if try_extract_hu_minwon_post else None
    if hu_minwon_post:
        return hu_minwon_post

    try:
        from backend.board.yongin_board import (
            try_extract_yongin_citizen_post,
            try_extract_yongin_general_post,
            try_extract_yongin_qestnar_post,
            try_extract_yongin_resve_post,
        )
    except Exception:
        try_extract_yongin_citizen_post = None  # type: ignore[assignment]
        try_extract_yongin_general_post = None  # type: ignore[assignment]
        try_extract_yongin_qestnar_post = None  # type: ignore[assignment]
        try_extract_yongin_resve_post = None  # type: ignore[assignment]

    yongin_resve_post = try_extract_yongin_resve_post(soup, url) if try_extract_yongin_resve_post else None
    if yongin_resve_post:
        return yongin_resve_post

    yongin_citizen_post = try_extract_yongin_citizen_post(soup, url) if try_extract_yongin_citizen_post else None
    if yongin_citizen_post:
        return yongin_citizen_post

    yongin_qestnar_post = try_extract_yongin_qestnar_post(soup, url) if try_extract_yongin_qestnar_post else None
    if yongin_qestnar_post:
        return yongin_qestnar_post

    yongin_general_post = try_extract_yongin_general_post(soup, url) if try_extract_yongin_general_post else None
    if yongin_general_post:
        return yongin_general_post

    try:
        from backend.board.yongin_water_board import try_extract_yongin_water_post
    except Exception:
        try_extract_yongin_water_post = None  # type: ignore[assignment]

    yongin_water_post = try_extract_yongin_water_post(soup, url) if try_extract_yongin_water_post else None
    if yongin_water_post:
        return yongin_water_post

    try:
        from backend.board.songpa_board import try_extract_songpa_post
    except Exception:
        try_extract_songpa_post = None  # type: ignore[assignment]

    songpa_post = try_extract_songpa_post(soup, url) if try_extract_songpa_post else None
    if songpa_post:
        return songpa_post

    try:
        from backend.board.asimc_board import try_extract_asimc_post
    except Exception:
        try_extract_asimc_post = None  # type: ignore[assignment]

    asimc_post = try_extract_asimc_post(soup, url) if try_extract_asimc_post else None
    if asimc_post:
        return asimc_post

    try:
        from backend.board.edu_ingang_board import try_extract_edu_ingang_lecture_post
    except Exception:
        try_extract_edu_ingang_lecture_post = None  # type: ignore[assignment]

    edu_ingang_lecture_post = (
        try_extract_edu_ingang_lecture_post(soup, url) if try_extract_edu_ingang_lecture_post else None
    )
    if edu_ingang_lecture_post:
        return edu_ingang_lecture_post

    miryang_tour_detail_post = _try_extract_miryang_tour_detail_post(soup, url)
    if miryang_tour_detail_post:
        return miryang_tour_detail_post

    miryang_tour_lodging_post = _try_extract_miryang_tour_lodging_post(soup, url)
    if miryang_tour_lodging_post:
        return miryang_tour_lodging_post

    miryang_post = _try_extract_miryang_board_post(soup, url)
    if miryang_post:
        return miryang_post

    if "chuncheon.go.kr" in u_low:
        chuncheon_post = _try_extract_chuncheon_post(soup, url)
        if chuncheon_post:
            return chuncheon_post
        # 계약 상세 URL인데 전용 파서가 실패한 경우: 일반 휴리스틱 오인 방지
        if "chuncheon.go.kr/contract/" in u_low and ("/detail/" in u_low or "ctrtacctbookmngno=" in u_low):
            return None

    chungnam_bbs_post = _try_extract_chungnam_youth_bbs_post(soup, url)
    if chungnam_bbs_post:
        return chungnam_bbs_post

    chungnam_space_post = _try_extract_chungnam_youth_space_post(soup, url)
    if chungnam_space_post:
        return chungnam_space_post

    chungnam_youth_post = _try_extract_chungnam_youth_custom_supp_post(soup, url)
    if chungnam_youth_post:
        return chungnam_youth_post

    umppa_kidscafe_post = _try_extract_umppa_kidscafe_post(soup, url)
    if umppa_kidscafe_post:
        return umppa_kidscafe_post

    mme_post = _try_extract_mme_or_kr_post(soup, url)
    if mme_post:
        return mme_post

    kisa_post = _try_extract_identity_kisa_post(soup, url)
    if kisa_post:
        return kisa_post

    edu_ingang_notice_post = _try_extract_edu_ingang_notice_post(soup, url)
    if edu_ingang_notice_post:
        return edu_ingang_notice_post

    _strip_noisy_tags(soup) # 1. 불필요한 태그 및 UI 요소 제거

    # 서울역사박물관(museum.seoul.go.kr): 브레드크럼·인쇄/공유·사진 확대보기 블록 제거 후 article#content 기준 추출
    try:
        from backend.board.museum_seoul_board import is_museum_seoul_url, strip_museum_seoul_board_noise

        if is_museum_seoul_url(url or ""):
            strip_museum_seoul_board_noise(soup)
    except Exception:
        pass

    # 강남구보건소: #content 범위 추출 시 하단 만족도 조사·투표·결과 블록이 본문에 섞이므로 선제 제거
    try:
        from backend.board.gangnam_board import is_gangnam_health_url, strip_gangnam_health_satisfaction_blocks

        if is_gangnam_health_url(url or ""):
            strip_gangnam_health_satisfaction_blocks(soup)
    except Exception:
        pass

    try:
        from backend.board.songpa_board import is_songpa_main_office_url, strip_songpa_main_office_noise

        if is_songpa_main_office_url(url or ""):
            strip_songpa_main_office_noise(soup, url=url or "")
    except Exception:
        pass

    try:
        from backend.board.pyeongtaek_board import is_pyeongtaek_city_url, strip_pyeongtaek_noise

        if is_pyeongtaek_city_url(url or ""):
            strip_pyeongtaek_noise(soup, url=url or "")
    except Exception:
        pass
    try:
        from backend.board.asimc_board import is_asimc_url, strip_asimc_noise

        if is_asimc_url(url or ""):
            strip_asimc_noise(soup, url=url or "")
    except Exception:
        pass

    content_hint = selector_profile.get("content_selector") if selector_profile else None
    # 강남구청 본청 /apply/.../view.do : 만족도(con-poll)·카카오 안내 제거, 본문은 #contents-wrap 우선
    try:
        from backend.board.gangnam_board import (
            gangnam_main_apply_content_selector_hint,
            gangnam_main_board_content_selector_hint,
            is_gangnam_main_apply_view_url,
            is_gangnam_main_board_view_url,
            strip_gangnam_main_apply_noise,
        )

        if is_gangnam_main_apply_view_url(url or ""):
            strip_gangnam_main_apply_noise(soup)
            if not content_hint:
                content_hint = gangnam_main_apply_content_selector_hint(url or "")
        elif is_gangnam_main_board_view_url(url or "") and not content_hint:
            content_hint = gangnam_main_board_content_selector_hint(url or "")
    except Exception:
        pass

    try:
        from backend.board.songpa_board import is_songpa_main_office_url, songpa_content_selector_hint

        if is_songpa_main_office_url(url or "") and not content_hint:
            content_hint = songpa_content_selector_hint(url or "") or content_hint
    except Exception:
        pass
    try:
        from backend.board.pyeongtaek_board import is_pyeongtaek_city_url, pyeongtaek_content_selector_hint

        if is_pyeongtaek_city_url(url or "") and not content_hint:
            content_hint = pyeongtaek_content_selector_hint(url or "") or content_hint
    except Exception:
        pass
    try:
        from backend.board.asimc_board import asimc_content_selector_hint, is_asimc_url

        if is_asimc_url(url or "") and not content_hint:
            content_hint = asimc_content_selector_hint(url or "") or content_hint
    except Exception:
        pass
    # edu.ingang.go.kr 공지/커뮤니티 상세는 #content/.content 래퍼에
    # 메뉴·메타가 함께 들어 있으므로 실제 본문 블록 #contents를 우선 사용한다.
    if "edu.ingang.go.kr" in u_low:
        try:
            if soup.select_one("#contents") is not None:
                content_hint = "#contents"
        except Exception:
            pass
    title_hint = selector_profile.get("title_selector") if selector_profile else None

    # site_configs 도메인(spscc.or.kr, gachi.chungbuk.go.kr, ghss.or.kr, museum.seoul.go.kr 등): 제목/본문 힌트 반영
    if (
        "spscc.or.kr" in u_low
        or "guro.go.kr" in u_low
        or "gachi.chungbuk.go.kr" in u_low
        or "ghss.or.kr" in u_low
        or "museum.seoul.go.kr" in u_low
    ):
        try:
            from backend.board.site_config_manager import config_manager

            tags = config_manager.get_tags(url or "")
            if not title_hint:
                ts = (tags.get("title_tag") or "").strip()
                title_hint = _normalize_site_config_css_selector(ts) if ts else None
            if not content_hint:
                cs = (tags.get("content_tag") or "").strip()
                content_hint = _normalize_site_config_css_selector(cs) if cs else None
        except Exception:
            pass

    # 충북가치자람 pbanc frontView 전용: 본문 루트만 지정(제목 추출 로직은 그대로, 상단 사업명 h2는 영역 밖)
    if is_gachi_pbanc_frontview_url(url or ""):
        content_hint = gachi_pbanc_frontview_content_root_selector()

    # 서울역사박물관 NR_boardView 팝업(#wrap 없음): body 전체를 본문으로(버튼·목록 폼은 선제 제거)
    museum_board_popup_full_body = False
    try:
        from backend.board.museum_seoul_board import (
            is_museum_seoul_board_popup_html,
            is_museum_seoul_board_view_url,
            is_museum_seoul_url,
            strip_museum_seoul_popup_board_ui,
        )

        if (
            is_museum_seoul_url(url or "")
            and is_museum_seoul_board_view_url(url or "")
            and is_museum_seoul_board_popup_html(soup)
        ):
            strip_museum_seoul_popup_board_ui(soup)
            content_hint = "body"
            museum_board_popup_full_body = True
    except Exception:
        pass

    title = ""
    try:
        from backend.board.pyeongtaek_board import extract_pyeongtaek_title, is_pyeongtaek_city_url

        if is_pyeongtaek_city_url(url or ""):
            pt = (extract_pyeongtaek_title(soup, url=url or "") or "").strip()
            if pt:
                title = pt
    except Exception:
        pass

    if not title:
        try:
            from backend.board.asimc_board import extract_asimc_title, is_asimc_url

            if is_asimc_url(url or ""):
                at = (extract_asimc_title(soup, url=url or "") or "").strip()
                if at:
                    title = at
        except Exception:
            pass

    # 강남구 계열(*.gangnam.go.kr): 보건소·본청·의료관광 등 제목은 gangnam_board에서만 통일
    if not title:
        try:
            from backend.board.gangnam_board import extract_gangnam_board_title, is_gangnam_family_url

            if is_gangnam_family_url(url or ""):
                gt = extract_gangnam_board_title(soup, url=url or "")
                if gt and gt != "제목 없음":
                    title = gt
        except Exception:
            title = ""

    if not title:
        try:
            from backend.board.anseong_board import extract_anseong_title, is_anseong_url

            if is_anseong_url(url or ""):
                at = (extract_anseong_title(soup, url=url or "") or "").strip()
                if at:
                    title = at
        except Exception:
            pass

    if not title:
        try:
            from backend.board.songpa_board import extract_songpa_title, is_songpa_main_office_url

            if is_songpa_main_office_url(url or ""):
                st = (extract_songpa_title(soup, url=url or "") or "").strip()
                if st:
                    title = st
        except Exception:
            pass

    # 서울역사박물관: h3.tit_article(article_info) → h2#tit_page → caption 순(콤마 select_one은 문서순이라 전용 순차 추출)
    if not title:
        try:
            from backend.board.museum_seoul_board import extract_museum_seoul_priority_title, is_museum_seoul_url

            if is_museum_seoul_url(url or ""):
                mt = (extract_museum_seoul_priority_title(soup) or "").strip()
                if mt:
                    title = mt
        except Exception:
            pass

    # 2. 제목 우선 추출 (본문 판별 시 제목 제외 패널티 활용 위함)
    if not title:
        eff_title_hint = title_hint
        try:
            from backend.board.museum_seoul_board import is_museum_seoul_url

            if is_museum_seoul_url(url or ""):
                eff_title_hint = None
        except Exception:
            pass
        title = _extract_title(soup, soup, selector_hint=eff_title_hint)
    
    # 3. 본문 후보지 탐색 및 점수 기반 최적 노드 결정 (Monitor Finish 연결)
    primary_root = _select_main_node(soup, selector_hint=content_hint)
    if museum_board_popup_full_body:
        content_text = _extract_content_text(primary_root)
        best_root = primary_root
    elif content_hint and primary_root is not None:
        hinted_text = _extract_content_text(primary_root)
        if _collapse_ws(hinted_text):
            content_text = hinted_text
            best_root = primary_root
        else:
            content_text, best_root = _pick_best_content_text(soup, primary_root, title=title)
    else:
        content_text, best_root = _pick_best_content_text(soup, primary_root, title=title)
    
    # 4. 기능별 1줄 주석: 결정된 최적 노드가 없을 경우 탐색된 기본 root를 사용하도록 보완
    final_root = best_root if best_root else primary_root

    extract_root = final_root
    chuncheon_ui_stripped = False
    if "chuncheon.go.kr" in u_low and final_root is not None:
        try:
            frag = BeautifulSoup(str(final_root), "html.parser")
            node = frag.find(True)
            if node:
                _strip_chuncheon_portal_ui_noise(node)
                extract_root = node
                chuncheon_ui_stripped = True
        except Exception:
            extract_root = final_root
    
    # 5. 최종 텍스트 및 HTML 확정
    if not content_text or chuncheon_ui_stripped:
        content_text = _extract_content_text(extract_root)
    content_html = _sanitize_html_fragment(extract_root)

    # [주의] 여기서 content_text = _collapse_ws(content_text)를 절대 호출하지 마세요!
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = _strip_leading_breadcrumb_and_title(content_text, title)

    if title:
        pure_title = _build_flexible_space_regex(_collapse_ws(title))
        # 제목 + (안내|상세|보기) 등이 붙어있으면 제거
        if pure_title and len(content_text) > len(title) + 20:
            content_text = re.sub(
                rf"^(?:{pure_title})(?:\s*(안내|상세|보기|본문))*\s*",
                "",
                content_text,
                flags=re.IGNORECASE | re.DOTALL,
            ).lstrip()
        
    if not title or not content_text.strip() or _looks_like_skip_navigation_only_text(content_text): return None # 필수 데이터 부재 시 실패 처리
    
    snippet = _collapse_ws(content_text)[:200] # 6. 스니펫은 한 줄 요약용이므로 공백 압축
    
    return BoardPostExtract(
        url=url, 
        title=title.strip(), # 제목 앞뒤 공백 정리
        content_text=content_text, # 줄바꿈이 살아있는 최종 본문
        content_html=(content_html or "").strip(), # 정제된 HTML
        snippet=snippet # 한 줄 요약본
    )

# [추가] 통계 표 전용 추출 로직
def extract_miryang_stat_table(html: str) -> list[dict]:
    # BeautifulSoup 객체 생성
    soup = BeautifulSoup(html, "html.parser")
    # 통계 데이터가 포함된 테이블 탐색
    table = soup.select_one("table.p-table, table.stat_table") or soup.find("table")
    
    if not table:
        return []

    results = []
    # 표의 헤더(연도 등) 추출
    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    
    # 데이터 행(tr) 순회
    for row in table.find("tbody").find_all("tr"):
        # 각 셀(td)의 데이터 추출
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        # 헤더와 데이터를 1:1로 매핑하여 딕셔너리 생성
        if len(headers) == len(cells):
            row_data = dict(zip(headers, cells))
            results.append(row_data)
            
    return results
