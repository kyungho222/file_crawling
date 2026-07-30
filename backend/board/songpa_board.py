"""
송파구청(songpa.go.kr/www) 전용 파서.

- 공통:
  - 제목 추출 셀렉터/라벨 기반 폴백
  - 본문 루트 힌트 제공
- 전용:
  - 직원검색(`/www/selectEmpList.do`) 결과 표를 검색 폼과 분리해 추출
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Optional
from urllib.parse import unquote, urlparse


logger = logging.getLogger("backend.board.songpa_board")
SONGPA_TITLE_TRACE_PREFIX = "[SongpaTitleTrace]"


def _songpa_title_trace(stage: str, *, url: str = "", **fields: Any) -> None:
    if "songpa.go.kr" not in str(url or "").lower():
        return
    try:
        compact = {
            str(k): (str(v or "")[:240] if v is not None else "")
            for k, v in (fields or {}).items()
        }
        logger.warning("%s stage=%s url=%s fields=%s", SONGPA_TITLE_TRACE_PREFIX, stage, str(url or "")[:240], compact)
        try:
            os.makedirs("logs", exist_ok=True)
            with open(os.path.join("logs", "songpa_title_trace.log"), "a", encoding="utf-8") as fp:
                fp.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {SONGPA_TITLE_TRACE_PREFIX} module=songpa_board stage={stage} url={str(url or '')[:240]} fields={compact}\n")
        except Exception:
            pass
    except Exception:
        pass


SONGPA_COMMON_TITLE_SELECTORS = (
    "#contents tr.p-table__subject .p-table__subject_text",
    "#contents .p-table__subject .p-table__subject_text",
    "#contents .p-table__subject_text",
    "tr.p-table__subject .p-table__subject_text",
    ".p-table__subject_text",
    "#board .p-table__subject .p-table__subject_text",
    "#board .p-table__subject_text",
    "#contents h3",
    "#contents .title",
    "#contents .tit",
    "#contents h2",
    "#contents h1",
    ".title",
    ".tit",
    "h3",
    "h2",
    "h1",
)

SONGPA_COMMON_CONTENT_SELECTOR = "#contents"

SONGPA_EMPLOYEE_TABLE_HEADERS = ("부서명", "팀명", "직위", "전화번호", "담당업무")


def _collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _norm_label(text: str) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip()


def is_songpa_main_office_url(url: str) -> bool:
    if not url:
        return False
    low = (url or "").lower()
    if "songpa.go.kr" not in low:
        return False
    if "songpakids.com" in low:
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        path = (parsed.path or "").lower()
        if not host.endswith("songpa.go.kr"):
            return False
        if host.startswith("learn.") or path.startswith("/learn/") or path == "/learn":
            return False
        return True
    except Exception:
        return "songpa.go.kr" in low and "songpakids.com" not in low and "/learn" not in low


def is_songpa_employee_list_url(url: str) -> bool:
    if not is_songpa_main_office_url(url):
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        return (parsed.path or "").lower().endswith("/www/selectemplist.do")
    except Exception:
        return "/www/selectemplist.do" in (url or "").lower()


def is_songpa_board_view_url(url: str) -> bool:
    if not is_songpa_main_office_url(url):
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        return (parsed.path or "").lower().endswith("/www/selectbbsnttview.do")
    except Exception:
        return "/www/selectbbsnttview.do" in (url or "").lower()


def songpa_content_selector_hint(url: str) -> str:
    if not is_songpa_main_office_url(url):
        return ""
    if is_songpa_employee_list_url(url):
        return "#contents table"
    if is_songpa_board_view_url(url):
        return "#board .p-table__content, #contents #board .p-table__content"
    return SONGPA_COMMON_CONTENT_SELECTOR


def _find_scope(soup: Any):
    if soup is None:
        return None
    return soup.select_one("#contents") or soup.select_one("#content") or soup


def _find_label_value(root: Any, *labels: str) -> str:
    if root is None or not labels:
        return ""
    wanted = {_norm_label(label) for label in labels if label}
    for tr in root.select("tr"):
        cells = tr.find_all(["th", "td", "dt", "dd"])
        if len(cells) < 2:
            continue
        for idx, cell in enumerate(cells[:-1]):
            label = _norm_label(cell.get_text(" ", strip=True))
            if label not in wanted:
                continue
            for nxt in cells[idx + 1 :]:
                value = _collapse_ws(nxt.get_text(" ", strip=True))
                if value:
                    return value
    return ""


def _extract_songpa_subject_text(el: Any) -> str:
    if el is None:
        return ""
    try:
        direct_parts = []
        for node in getattr(el, "contents", []) or []:
            if getattr(node, "name", None) is None:
                txt = _collapse_ws(str(node))
                if txt:
                    direct_parts.append(txt)
        direct_text = _collapse_ws(" ".join(direct_parts))
        if direct_text:
            return direct_text
    except Exception:
        pass
    try:
        for noisy in el.select(".p-icon, [class*='icon'], img, svg"):
            try:
                noisy.decompose()
            except Exception:
                pass
    except Exception:
        pass
    return _collapse_ws(el.get_text(" ", strip=True))


def _restore_songpa_numbered_breaks(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    value = re.sub(r"\r\n?", "\n", value)
    value = re.sub(r"[ \t]*\n[ \t]*", "\n", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    value = re.sub(r"(?<![\d.])(?=\d{1,2}\.\s+[^\d.~])", "\n", value)
    value = re.sub(r"(?m)^\?\s+", "- ", value)
    value = re.sub(r"\s+([.,:;])", r"\1", value)
    value = re.sub(r"으\s+로서", "으로서", value)
    return "\n".join(_collapse_ws(line) for line in value.splitlines() if _collapse_ws(line)).strip()


def _extract_songpa_detail_value_text(el: Any) -> str:
    if el is None:
        return ""
    lines: list[str] = []
    try:
        for child in getattr(el, "children", []) or []:
            name = (getattr(child, "name", "") or "").lower()
            if name in {"script", "style"}:
                continue
            if name == "table":
                for tr in child.find_all("tr"):
                    cells = []
                    for cell in tr.find_all(["th", "td"], recursive=False):
                        txt = _collapse_ws(cell.get_text(" ", strip=True))
                        if txt:
                            cells.append(txt)
                    if cells:
                        lines.append(" | ".join(cells))
                continue
            if name in {"p", "div", "li", "dt", "dd"}:
                txt = _collapse_ws(child.get_text(" ", strip=True))
                if txt:
                    lines.append(txt)
                continue
            if name in {"ul", "ol"}:
                for li in child.find_all("li", recursive=False):
                    txt = _collapse_ws(li.get_text(" ", strip=True))
                    if txt:
                        lines.append(txt)
                continue
            if name == "br":
                continue
            if name:
                txt = _collapse_ws(child.get_text(" ", strip=True))
            else:
                txt = _collapse_ws(str(child))
            if txt:
                lines.append(txt)
    except Exception:
        lines = []
    if lines:
        text = "\n".join(line for line in lines if line)
    else:
        try:
            text = el.get_text(" ", strip=True)
        except Exception:
            text = ""
    return _restore_songpa_numbered_breaks(text)


def _songpa_main_table_rows(table: Any) -> list[Any]:
    if table is None:
        return []
    try:
        tbody = table.find("tbody", recursive=False)
        root = tbody or table
        return list(root.find_all("tr", recursive=False))
    except Exception:
        try:
            return list(table.find_all("tr", recursive=False))
        except Exception:
            return []


def _extract_songpa_alert_message(soup: Any) -> str:
    if soup is None:
        return ""
    scripts = []
    try:
        scripts = [str(script.string or script.get_text(" ", strip=False) or "") for script in soup.find_all("script")]
    except Exception:
        scripts = []

    for script in scripts:
        if "decodeURIComponent" not in script and "alert(" not in script:
            continue
        match = re.search(r"decodeURIComponent\(\s*(['\"])(.*?)\1\s*\)", script, flags=re.DOTALL)
        if match:
            msg = _collapse_ws(unquote(match.group(2)))
            if msg:
                return msg

    try:
        title = _collapse_ws(soup.title.get_text(" ", strip=True)) if soup.title else ""
    except Exception:
        title = ""
    if title == "안내메시지":
        body_text = _collapse_ws(soup.get_text(" ", strip=True))
        if body_text and body_text != title:
            return body_text
    return ""


def _is_songpa_noise_title(text: str) -> bool:
    txt = _collapse_ws(text)
    if not txt:
        return True
    compact_low = re.sub(r"\s+", "", txt).lower()
    if compact_low in {
        "송파송파구청songpa-guoffice",
        "송파구청songpa-guoffice",
        "songpa-guoffice",
    }:
        return True
    return txt in {
        "직원검색",
        "구청안내",
        "우리송파",
        "공유하기",
        "주메뉴",
        "본문",
        "송파구청",
    }


def extract_songpa_title(soup: Any, *, url: str = "") -> str:
    _songpa_title_trace("songpa_extract_enter", url=url, has_soup=bool(soup))
    if soup is None or (url and not is_songpa_main_office_url(url)):
        _songpa_title_trace("songpa_extract_skip", url=url, reason="empty_soup_or_not_main_office")
        return ""

    scope = _find_scope(soup)
    if scope is None:
        _songpa_title_trace("songpa_extract_skip", url=url, reason="scope_not_found")
        return ""

    if is_songpa_employee_list_url(url):
        for sel in ("#contents h3", "#contents h2", "#contents h1", "h3", "h2"):
            try:
                el = scope.select_one(sel) if sel.startswith("#contents") else scope.select_one(sel)
            except Exception:
                el = None
            if not el:
                continue
            txt = _collapse_ws(el.get_text(" ", strip=True))
            if txt and txt not in {"직원검색", "구청안내"}:
                return txt

    title_from_label = _find_label_value(scope, "제목", "프로그램명", "강좌명", "민원명")
    if title_from_label:
        _songpa_title_trace("songpa_extract_return_label", url=url, selected=title_from_label)
        return title_from_label

    for sel in SONGPA_COMMON_TITLE_SELECTORS:
        try:
            el = soup.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        if "p-table__subject_text" in sel:
            txt = _extract_songpa_subject_text(el)
        else:
            txt = _collapse_ws(el.get_text(" ", strip=True))
        if not txt:
            continue
        _songpa_title_trace("songpa_extract_selector_candidate", url=url, selector=sel, text=txt, noise=_is_songpa_noise_title(txt))
        if _is_songpa_noise_title(txt):
            continue
        _songpa_title_trace("songpa_extract_return_selector", url=url, selector=sel, selected=txt)
        return txt
    _songpa_title_trace("songpa_extract_empty", url=url)
    return ""


def extract_songpa_web_title(soup: Any, *, url: str = "") -> str:
    """송파구청 본청 URL의 저장용 web_title은 게시글 제목 우선으로 맞춘다."""
    if soup is None or (url and not is_songpa_main_office_url(url)):
        return ""

    title = extract_songpa_title(soup, url=url)
    if title and title != "제목 없음" and not _is_songpa_noise_title(title):
        return title

    if is_songpa_board_view_url(url):
        alert_message = _extract_songpa_alert_message(soup)
        if alert_message:
            return "안내메시지"
    return ""


def _extract_songpa_board_detail_table_text(soup: Any, *, url: str = "", title: str = "") -> str:
    if soup is None or not is_songpa_board_view_url(url):
        return ""
    root = soup.select_one("#board") or soup.select_one("#contents") or soup
    table = None
    try:
        table = root.select_one("table:has(.p-table__subject_text)")
    except Exception:
        table = None
    if table is None:
        try:
            for candidate in root.select("table"):
                if candidate.select_one(".p-table__subject_text"):
                    table = candidate
                    break
        except Exception:
            table = None
    if table is None:
        return ""

    lines = []
    title_text = _collapse_ws(title)
    if title_text and not _is_songpa_noise_title(title_text):
        lines.append(title_text)

    seen = {_collapse_ws(line) for line in lines if line}
    rows = _songpa_main_table_rows(table)
    for tr in rows:
        try:
            subject_el = tr.select_one(".p-table__subject_text")
        except Exception:
            subject_el = None
        if subject_el is not None:
            subject = _extract_songpa_subject_text(subject_el)
            if subject and not _is_songpa_noise_title(subject) and subject not in seen:
                lines.insert(0, subject)
                seen.add(subject)
            continue

        th = tr.find("th")
        tds = tr.find_all("td")
        if not tds:
            continue
        content_td = None
        try:
            content_td = tr.select_one("td.p-table__content, td[title='내용']")
        except Exception:
            content_td = None
        td = content_td or tds[-1]
        label = _collapse_ws(th.get_text(" ", strip=True)) if th else ""
        if not label:
            try:
                label = _collapse_ws(td.get("title") or "")
            except Exception:
                label = ""
        value = _extract_songpa_detail_value_text(td)
        if not value:
            continue
        if value == "http://":
            continue
        if label:
            if "\n" in value:
                line = f"{label}:\n{value}"
            else:
                line = f"{label}: {value}"
        else:
            line = value
        if _collapse_ws(line) in seen:
            continue
        seen.add(_collapse_ws(line))
        lines.append(line)

    return "\n".join(line for line in lines if _collapse_ws(line)).strip()


def _extract_songpa_board_detail_table_html(soup: Any, *, url: str = "") -> str:
    if soup is None or not is_songpa_board_view_url(url):
        return ""
    root = soup.select_one("#board") or soup.select_one("#contents") or soup
    try:
        table = root.select_one("table:has(.p-table__subject_text)")
    except Exception:
        table = None
    if table is None:
        try:
            for candidate in root.select("table"):
                if candidate.select_one(".p-table__subject_text"):
                    table = candidate
                    break
        except Exception:
            table = None
    return str(table or "").strip()


def strip_songpa_main_office_noise(soup: Any, *, url: str = "") -> None:
    if soup is None:
        return
    if url and not is_songpa_main_office_url(url):
        return

    selectors = [
        "#contents > .tab_menu.type1.depth5",
        "#contents #tab_menu_target",
        "#contents .tab_list.clearfix",
        "#contents .tab_panel",
        "#board .p-attach",
        "#board .p-post-move",
        "#board .text_center.margin_t_30.list",
    ]
    for sel in selectors:
        try:
            for tag in soup.select(sel):
                try:
                    tag.decompose()
                except Exception:
                    pass
        except Exception:
            pass


def _is_employee_table(table: Any) -> bool:
    if table is None:
        return False
    try:
        caption = _collapse_ws(table.find("caption").get_text(" ", strip=True)) if table.find("caption") else ""
    except Exception:
        caption = ""
    if "전화번호 안내" in caption and "담당업무" in caption:
        return True

    headers = [_collapse_ws(th.get_text(" ", strip=True)) for th in table.select("th")]
    return all(any(want == head for head in headers) for want in ("부서명", "전화번호", "담당업무"))


def _find_employee_table(scope: Any):
    if scope is None:
        return None
    for table in scope.select("table"):
        if _is_employee_table(table):
            return table
    return None


def _find_employee_intro(scope: Any, table: Any) -> str:
    if scope is None or table is None:
        return ""
    try:
        for p in scope.find_all("p"):
            if table in p.parents:
                continue
            txt = _collapse_ws(p.get_text(" ", strip=True))
            if not txt:
                continue
            if "검색" in txt and "담당" in txt:
                return txt
    except Exception:
        return ""
    return ""


def _employee_headers(table: Any) -> list[str]:
    headers: list[str] = []
    if table is None:
        return headers
    try:
        for th in table.select("thead th"):
            txt = _collapse_ws(th.get_text(" ", strip=True))
            if txt:
                headers.append(txt)
    except Exception:
        headers = []
    if headers:
        return headers
    try:
        first_row = table.select_one("tr")
        if not first_row:
            return headers
        for th in first_row.find_all("th"):
            txt = _collapse_ws(th.get_text(" ", strip=True))
            if txt:
                headers.append(txt)
    except Exception:
        return []
    return headers


def extract_songpa_employee_content_text(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_songpa_employee_list_url(url)):
        return ""
    scope = _find_scope(soup)
    table = _find_employee_table(scope)
    if table is None:
        return ""

    headers = _employee_headers(table)
    intro = _find_employee_intro(scope, table)
    lines: list[str] = []
    if intro:
        lines.append(intro)

    for tr in table.select("tbody tr"):
        raw_cells = [_collapse_ws(td.get_text(" ", strip=True)) for td in tr.find_all(["th", "td"])]
        if not any(raw_cells):
            continue
        if headers and len(raw_cells) >= len(headers):
            pairs = []
            for head, cell in zip(headers, raw_cells):
                if cell:
                    pairs.append(f"{head}: {cell}")
            if pairs:
                lines.append(" | ".join(pairs))
                continue
        lines.append(" | ".join(cell for cell in raw_cells if cell))

    if not lines:
        return _collapse_ws(table.get_text(" ", strip=True))
    return "\n".join(line for line in lines if line).strip()


def extract_songpa_employee_content_html(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_songpa_employee_list_url(url)):
        return ""
    scope = _find_scope(soup)
    table = _find_employee_table(scope)
    if table is None:
        return ""

    intro = _find_employee_intro(scope, table)
    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return str(table)

    wrapper = BeautifulSoup("", "html.parser")
    root = wrapper.new_tag("div", attrs={"class": "songpa-employee-content"})
    if intro:
        p = wrapper.new_tag("p")
        p.string = intro
        root.append(p)
    try:
        table_copy = BeautifulSoup(str(table), "html.parser")
        table_root = table_copy.find("table") or table_copy
        root.append(table_root)
    except Exception:
        root.append(str(table))
    return str(root)


def try_extract_songpa_board_view_post(soup: Any, url: str):
    if soup is None or not is_songpa_board_view_url(url):
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws as _shared_collapse_ws,
        _extract_content_text,
        _format_numbered_list_lines,
        _sanitize_html_fragment,
        _trim_leading_skip_and_breadcrumb_text,
    )

    try:
        from bs4 import BeautifulSoup  # type: ignore[import-not-found]
    except Exception:
        return None

    strip_songpa_main_office_noise(soup, url=url)

    alert_message = _extract_songpa_alert_message(soup)
    if alert_message:
        html_doc = BeautifulSoup("<div class=\"songpa-alert-message\"></div>", "html.parser")
        root = html_doc.find("div")
        if root is not None:
            p = html_doc.new_tag("p")
            p.string = alert_message
            root.append(p)
            content_html = _sanitize_html_fragment(root).strip()
        else:
            content_html = ""
        snippet = _shared_collapse_ws(alert_message)[:200]
        return BoardPostExtract(
            url=url,
            title="안내메시지",
            content_text=alert_message,
            content_html=content_html,
            snippet=snippet,
        )

    title = extract_songpa_title(soup, url=url) or "제목 없음"
    table_content_text = _extract_songpa_board_detail_table_text(soup, url=url, title=title)
    if table_content_text and ("\n" in table_content_text or len(table_content_text) > len(title) + 10):
        table_html = _extract_songpa_board_detail_table_html(soup, url=url)
        content_html = ""
        if table_html:
            try:
                table_frag = BeautifulSoup(table_html, "html.parser")
                table_root = table_frag.find("table") or table_frag.find(True)
                if table_root is not None:
                    content_html = _sanitize_html_fragment(table_root).strip()
            except Exception:
                content_html = ""
        snippet = _shared_collapse_ws(table_content_text)[:200]
        _songpa_title_trace(
            "songpa_board_table_detail_return",
            url=url,
            title=title,
            content_len=len(table_content_text),
            snippet=snippet,
        )
        return BoardPostExtract(
            url=url,
            title=title.strip(),
            content_text=table_content_text,
            content_html=content_html,
            snippet=snippet,
        )

    body = soup.select_one("#board .p-table__content") or soup.select_one(".p-table__content")
    if body is None:
        return None

    try:
        frag = BeautifulSoup(str(body), "html.parser")
    except Exception:
        return None
    root = frag.find(True)
    if root is None:
        return None

    content_text = _extract_content_text(root)
    content_text = _format_numbered_list_lines(content_text)
    content_text = _trim_leading_skip_and_breadcrumb_text(content_text)
    content_text = (content_text or "").strip()
    if not content_text:
        return None

    if title and len(content_text) > len(title) + 20:
        pure_title = re.escape(_shared_collapse_ws(title))
        content_text = re.sub(
            rf"^{pure_title}(\s*(안내|상세|보기|본문))*",
            "",
            content_text,
            flags=re.IGNORECASE | re.DOTALL,
        ).lstrip()

    content_html = _sanitize_html_fragment(root).strip()
    snippet = _shared_collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=content_html,
        snippet=snippet,
    )


def try_extract_songpa_post(soup: Any, url: str):
    if soup is None or not is_songpa_main_office_url(url):
        return None
    if is_songpa_board_view_url(url):
        general_post = try_extract_songpa_board_view_post(soup, url)
        if general_post:
            return general_post
    if not is_songpa_employee_list_url(url):
        return None

    from backend.board.board_content_extractor import (
        BoardPostExtract,
        _collapse_ws as _shared_collapse_ws,
        _sanitize_html_fragment,
    )

    title = extract_songpa_title(soup, url=url) or "제목 없음"
    content_text = extract_songpa_employee_content_text(soup, url=url)
    if not content_text:
        return None

    raw_html = extract_songpa_employee_content_html(soup, url=url)
    content_html = ""
    if raw_html:
        try:
            from bs4 import BeautifulSoup  # type: ignore[import-not-found]

            frag = BeautifulSoup(raw_html, "html.parser")
            root = frag.find(True)
            if root:
                content_html = _sanitize_html_fragment(root).strip()
        except Exception:
            content_html = raw_html

    snippet = _shared_collapse_ws(content_text)[:200]
    return BoardPostExtract(
        url=url,
        title=title.strip(),
        content_text=content_text,
        content_html=(content_html or raw_html or "").strip(),
        snippet=snippet,
    )
