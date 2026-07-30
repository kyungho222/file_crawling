"""
성북구청(sb.go.kr) 전용 추출 헬퍼.

- 대상: `/www/selectEminwonView.do` 상세
- 특징: 실제 제목이 상단 `<title>`이 아니라 메타 표의 `제목` 행 `td`에 들어간다.
- 대상: `/yeyak/unityProgrmWebView.do` 예약 상세
- 특징: 범용 article 추출 시 상단 예약 카테고리 탭과 달력이 본문에 섞인다.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse


def _collapse_ws(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _norm_label(text: str) -> str:
    return re.sub(r"[\sㆍ·]+", "", str(text or "")).strip()


def is_seongbuk_eminwon_view_url(url: str) -> bool:
    if not url:
        return False
    low = str(url or "").lower()
    if "sb.go.kr" not in low:
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        path = (parsed.path or "").lower()
        return host.endswith("sb.go.kr") and path.endswith("/www/selecteminwonview.do")
    except Exception:
        return "sb.go.kr" in low and "selecteminwonview.do" in low


def is_seongbuk_yeyak_program_view_url(url: str) -> bool:
    if not url:
        return False
    low = str(url or "").lower()
    if "sb.go.kr" not in low:
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        path = (parsed.path or "").lower()
        return host.endswith("sb.go.kr") and path.endswith("/yeyak/unityprogrmwebview.do")
    except Exception:
        return "sb.go.kr" in low and "unityprogrmwebview.do" in low


def is_seongbuk_bbs_view_url(url: str) -> bool:
    if not url:
        return False
    low = str(url or "").lower()
    if "sb.go.kr" not in low:
        return False
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        path = (parsed.path or "").lower()
        return host.endswith("sb.go.kr") and path.endswith("/www/selectbbsnttview.do")
    except Exception:
        return "sb.go.kr" in low and "selectbbsnttview.do" in low


def _find_scope(soup: Any):
    if soup is None:
        return None
    return (
        soup.select_one("#contents .epForm.bbs.gosi.view")
        or soup.select_one("#contents")
        or soup
    )


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


def _find_bbs_scope(soup: Any):
    if soup is None:
        return None
    return (
        soup.select_one("#contents table.p-table.block")
        or soup.select_one("#contents table.p-table")
        or soup.select_one("table.p-table.block")
        or soup.select_one("table.p-table")
    )


def _clean_bbs_value(cell: Any) -> str:
    if cell is None:
        return ""
    try:
        for el in cell.select("script, style, noscript, .p-attach__preview"):
            el.decompose()
    except Exception:
        pass
    return _collapse_ws(cell.get_text(" ", strip=True))


def _seongbuk_bbs_label_lines(scope: Any) -> list[str]:
    lines: list[str] = []
    if scope is None:
        return lines
    for tr in scope.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        label = _collapse_ws(th.get_text(" ", strip=True)).strip(" :")
        value = _clean_bbs_value(td)
        if not label or not value:
            continue
        if label == "첨부파일":
            files: list[str] = []
            for a in td.select("a.p-attach__link, a[href*='downloadBbsFile.do'], a[href*='downloadbbsfile.do']"):
                file_text = _collapse_ws(a.get_text(" ", strip=True))
                file_text = re.sub(r"^(?:hwp|hwpx|pdf|docx?|xlsx?|pptx?|zip)\s+문서\s+", "", file_text, flags=re.I).strip()
                if file_text and file_text not in files:
                    files.append(file_text)
            value = ", ".join(files) if files else value
        lines.append(f"{label}: {value}")
    return lines


def extract_seongbuk_bbs_title(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_seongbuk_bbs_view_url(url)):
        return ""
    scope = _find_bbs_scope(soup)
    if scope is None:
        return ""
    for sel in (".p-table__subject_text", "td[colspan] .subject", "td[colspan] strong", "td[colspan]"):
        try:
            el = scope.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        txt = _collapse_ws(el.get_text(" ", strip=True))
        if txt:
            return txt
    return _find_label_value(scope, "민원명", "제목", "서식명")


def extract_seongbuk_bbs_reg_date(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_seongbuk_bbs_view_url(url)):
        return ""
    scope = _find_bbs_scope(soup)
    if scope is None:
        return ""
    for sel in (".p-author__info time", "time"):
        try:
            el = scope.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        txt = _collapse_ws(el.get_text(" ", strip=True))
        if re.search(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", txt):
            return txt
    return _find_label_value(scope, "작성일", "등록일")


def extract_seongbuk_title(soup: Any, *, url: str = "") -> str:
    if soup is None or (
        url
        and not (
            is_seongbuk_eminwon_view_url(url)
            or is_seongbuk_yeyak_program_view_url(url)
            or is_seongbuk_bbs_view_url(url)
        )
    ):
        return ""

    if url and is_seongbuk_bbs_view_url(url):
        title = extract_seongbuk_bbs_title(soup, url=url)
        if title:
            return title

    if url and is_seongbuk_yeyak_program_view_url(url):
        title = extract_seongbuk_yeyak_title(soup, url=url)
        if title:
            return title

    scope = _find_scope(soup)
    if scope is None:
        return ""

    title = _find_label_value(scope, "제목", "공고명", "민원명")
    if title:
        return title

    for sel in (
        "#contents .epForm.bbs.gosi.view h1",
        "#contents .epForm.bbs.gosi.view h2",
        "#contents .epForm.bbs.gosi.view h3",
    ):
        try:
            el = scope.select_one(sel) if sel.startswith("#contents") else scope.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        txt = _collapse_ws(el.get_text(" ", strip=True))
        if txt and txt not in {"공고", "고시공고 상세"}:
            return txt
    return ""


def _clean_yeyak_label(text: str) -> str:
    return _collapse_ws(str(text or "").replace("ㆍ", "").replace("·", "")).strip(" :")


def extract_seongbuk_yeyak_title(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_seongbuk_yeyak_program_view_url(url)):
        return ""
    root = (
        soup.select_one("#contents .program.edu_view")
        or soup.select_one("#contents .program")
        or soup.select_one("#contents")
        or soup
    )
    for sel in ("h3", "h2", "h1"):
        try:
            el = root.select_one(sel)
        except Exception:
            el = None
        if not el:
            continue
        txt = _collapse_ws(el.get_text(" ", strip=True))
        if txt:
            return txt
    return ""


def _yeyak_table_lines(scope: Any) -> list[str]:
    lines: list[str] = []
    if scope is None:
        return lines
    table = scope.select_one(".apply_right table.table") or scope.select_one("table.table")
    if table is None:
        return lines
    for tr in table.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        label = _clean_yeyak_label(th.get_text(" ", strip=True))
        value = _collapse_ws(td.get_text(" ", strip=True))
        if label and value:
            lines.append(f"{label}: {value}")
    return lines


def _yeyak_tab_lines(scope: Any) -> list[str]:
    lines: list[str] = []
    if scope is None:
        return lines
    tab_titles = [
        _collapse_ws(el.get_text(" ", strip=True))
        for el in scope.select(".apply_tab .tab_list .tab_item .tab_button span")
    ]
    contents = scope.select(".apply_tab .tab_content")
    for idx, content in enumerate(contents):
        title = tab_titles[idx] if idx < len(tab_titles) else ""
        parts: list[str] = []
        for sel in (".content_detail", ".map_info", ".map_info_item"):
            for el in content.select(sel):
                txt = _collapse_ws(el.get_text(" ", strip=True))
                if txt and txt not in parts:
                    parts.append(txt)
        if not parts:
            continue
        body = " / ".join(parts)
        lines.append(f"{title}: {body}" if title else body)
    return lines


def _make_lines_html(soup: Any, lines: list[str], css_class: str) -> str:
    try:
        doc = soup.__class__(f'<div class="{css_class}"></div>', "html.parser")
        wrap = doc.find("div")
        for line in lines:
            p = doc.new_tag("p")
            p.string = line
            wrap.append(p)
        return str(wrap or "")
    except Exception:
        return "\n".join(lines)


def try_extract_seongbuk_post(soup: Any, url: str):
    if soup is None or not (is_seongbuk_yeyak_program_view_url(url) or is_seongbuk_bbs_view_url(url)):
        return None

    try:
        from backend.board.board_content_extractor import BoardPostExtract
    except Exception:
        return None

    if is_seongbuk_bbs_view_url(url):
        scope = _find_bbs_scope(soup)
        if scope is None:
            return None
        title = extract_seongbuk_bbs_title(soup, url=url) or "제목 없음"
        lines: list[str] = []
        reg_date = extract_seongbuk_bbs_reg_date(soup, url=url)
        if reg_date:
            lines.append(f"작성일: {reg_date}")
        lines.extend(_seongbuk_bbs_label_lines(scope))
        content_text = "\n".join(line for line in lines if _collapse_ws(line)).strip()
        if not content_text:
            return None
        return BoardPostExtract(
            url=url,
            title=title,
            content_text=content_text,
            content_html=_make_lines_html(soup, lines, "seongbuk-bbs-extract-root"),
            snippet=_collapse_ws(content_text)[:200],
        )

    scope = (
        soup.select_one("#contents .program.edu_view")
        or soup.select_one("#contents .program")
        or soup.select_one("#contents")
    )
    if scope is None:
        return None

    title = extract_seongbuk_yeyak_title(soup, url=url) or "제목 없음"
    lines = _yeyak_table_lines(scope)
    status_el = scope.select_one(".apply_right .ready")
    status = _collapse_ws(status_el.get_text(" ", strip=True)) if status_el else ""
    if status:
        lines.append(f"접수상태: {status}")
    lines.extend(_yeyak_tab_lines(scope))

    content_text = "\n".join(line for line in lines if _collapse_ws(line)).strip()
    if not content_text:
        return None
    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=_make_lines_html(soup, lines, "seongbuk-yeyak-extract-root"),
        snippet=_collapse_ws(content_text)[:200],
    )


def extract_seongbuk_author_info(soup: Any, *, url: str = "") -> dict[str, str]:
    if soup is None or (
        url
        and not (
            is_seongbuk_eminwon_view_url(url)
            or is_seongbuk_bbs_view_url(url)
            or is_seongbuk_yeyak_program_view_url(url)
        )
    ):
        return {}

    if url and is_seongbuk_yeyak_program_view_url(url):
        scope = (
            soup.select_one("#contents .program.edu_view")
            or soup.select_one("#contents .program")
            or soup.select_one("#contents")
            or soup
        )
        org = _find_label_value(scope, "운영기관", "주관기관", "기관")
        if org:
            return {
                "author": org,
                "author_raw": org,
                "department": org,
                "department_raw": org,
                "author_kind": "org",
            }
        return {
            "author": "성북구청",
            "author_raw": "성북구청",
            "department": "성북구청",
            "department_raw": "성북구청",
            "author_kind": "org",
        }

    if url and is_seongbuk_bbs_view_url(url):
        scope = _find_bbs_scope(soup)
        if scope is None:
            return {}
        department = _find_label_value(scope, "주관부서", "담당부서", "부서")
        author = department
        out: dict[str, str] = {}
        if author:
            out["author"] = author
            out["author_raw"] = author
        if department:
            out["department"] = department
            out["department_raw"] = department
        return out

    scope = _find_scope(soup)
    if scope is None:
        return {}

    author = _find_label_value(scope, "작성자", "등록자", "등록인", "담당자", "성명")
    department = _find_label_value(scope, "담당부서", "작성부서", "부서", "부서명", "담당과", "담당팀")
    author_raw = author
    department_raw = department

    if not author and department:
        author = department
        author_raw = department_raw

    out: dict[str, str] = {}
    if author:
        out["author"] = author
        out["author_raw"] = author_raw
    if department:
        out["department"] = department
        out["department_raw"] = department_raw
    return out
