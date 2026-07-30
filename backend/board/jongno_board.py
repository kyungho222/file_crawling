from __future__ import annotations

import re
from html import unescape
from typing import Any, Dict, Optional
from urllib.parse import urljoin


_WS_RE = re.compile(r"\s+")
_PROCESS_CHILD_LABELS = {
    "접수처",
    "경유처",
    "처분청",
    "대조공부",
    "비치대장",
    "처리기간",
    "최종결재",
    "수수료",
    "면허세",
    "현장조사사항",
    "처리요건",
    "후속민원",
    "처리흐름",
}


def _collapse_ws(text: Any) -> str:
    return _WS_RE.sub(" ", str(text or "")).strip()


def _flat_label(text: Any) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip()


def _clean_jongno_title(text: Any) -> str:
    title = _collapse_ws(text)
    title = re.sub(r"^\s*제목\s*[:：]?\s*", "", title).strip()
    return title


def is_jongno_minwon_form_url(url: str) -> bool:
    u = (url or "").lower()
    return (
        "jongno.go.kr" in u
        and "/portal/bbs/selectboardarticle.do" in u
        and "bbsmstr_000000000341" in u
    )


def is_jongno_construction_status_url(url: str) -> bool:
    u = (url or "").lower()
    return (
        "jongno.go.kr" in u
        and "/portal/bbs/selectboardarticle.do" in u
        and "bbsmstr_000000001528" in u
    )


def is_jongno_board_article_url(url: str) -> bool:
    u = (url or "").lower()
    return "jongno.go.kr" in u and (
        "/portal/bbs/selectboardarticle.do" in u
        or "/health/bbs/selectboardarticle.do" in u
        or "/mayor/bbs/selectboardarticle.do" in u
    )


def is_jongno_apply_view_url(url: str) -> bool:
    u = (url or "").lower()
    return "jongno.go.kr" in u and "/apply/selectapplyview.do" in u


def is_jongno_council_board_view_url(url: str) -> bool:
    u = (url or "").lower()
    return "council.jongno.go.kr" in u and "/council/bbs/" in u and "/view.do" in u


def is_jongno_council_assembly_view_url(url: str) -> bool:
    u = (url or "").lower()
    return (
        "council.jongno.go.kr" in u
        and "/council/councilasemby/view/" in u
        and ".do" in u
    )


def _cell_text_with_links(cell, base_url: str) -> str:
    if cell is None:
        return ""
    try:
        cloned = cell.__copy__()
    except Exception:
        cloned = cell
    try:
        for a in list(cloned.find_all("a")):
            href = _collapse_ws(a.get("href") or "")
            label = _collapse_ws(a.get_text(" ", strip=True))
            if href:
                full = urljoin(base_url or "", href)
                replacement = f"{label} ({full})" if label else full
                a.replace_with(replacement)
    except Exception:
        pass
    try:
        text = cloned.get_text("\n", strip=True)
    except Exception:
        text = cell.get_text("\n", strip=True)
    text = unescape(text).replace("\xa0", " ")
    lines = [_collapse_ws(line) for line in text.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def _extract_rows(table, base_url: str) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    if table is None:
        return rows

    row_group = ""
    for tr in table.find_all("tr"):
        pending_label = ""
        for cell in tr.find_all(["th", "td"], recursive=False):
            name = (cell.name or "").lower()
            if name == "th":
                label = _collapse_ws(cell.get_text(" ", strip=True))
                if not label:
                    continue
                if cell.get("rowspan") and not pending_label:
                    row_group = label
                    continue
                pending_label = label
                continue

            if name != "td":
                continue
            value = _cell_text_with_links(cell, base_url)
            label_parts = []
            pending_flat = _flat_label(pending_label)
            if (
                row_group
                and pending_label
                and _flat_label(row_group) != pending_flat
                and pending_flat in _PROCESS_CHILD_LABELS
            ):
                label_parts.append(row_group)
            if pending_label:
                label_parts.append(pending_label)
            label = _collapse_ws(" ".join(label_parts)) or "내용"
            if value:
                rows.append((label, value))
            pending_label = ""
    return rows


def extract_jongno_minwon_form(soup, url: str) -> Optional[Dict[str, str]]:
    if not soup or not is_jongno_minwon_form_url(url or ""):
        return None

    table = soup.select_one("table.view_type03")
    if table is None:
        view = soup.select_one(".board_view.table_scroll") or soup.select_one(".board_view")
        table = view.select_one("table") if view else None
    if table is None:
        return None

    rows = _extract_rows(table, url)
    if not rows:
        return None

    title = ""
    for label, value in rows:
        flat = _flat_label(label)
        if "민원사무명" in flat:
            title = _collapse_ws(value.splitlines()[0] if value else "")
            break
    if not title:
        for label, value in rows:
            if _flat_label(label) in {"제목", "민원명", "사무명"} and value:
                title = _collapse_ws(value.splitlines()[0])
                break
    if not title:
        title = "민원편람"

    lines: list[str] = []
    for label, value in rows:
        label_norm = _collapse_ws(label)
        value_norm = value.strip()
        if not label_norm or not value_norm:
            continue
        lines.append(f"{label_norm}: {value_norm}")

    content_text = "\n".join(lines).strip()
    if not content_text:
        return None

    return {
        "title": title,
        "content_text": content_text,
        "content_html": str(table),
        "snippet": _collapse_ws(content_text)[:200],
    }


def extract_jongno_author_info(soup, url: str) -> Dict[str, str]:
    if not soup or not is_jongno_board_article_url(url or ""):
        return {}

    if is_jongno_minwon_form_url(url or ""):
        table = soup.select_one("table.view_type03")
        if table is None:
            view = soup.select_one(".board_view.table_scroll") or soup.select_one(".board_view")
            table = view.select_one("table") if view else None
        return _extract_jongno_author_info_from_rows(
            _extract_rows(table, url) if table else [],
            prefer_department=True,
        )

    table = _find_jongno_general_board_table(soup)
    if table is None:
        view = soup.select_one(".board_view")
        table = view.select_one("table") if view else None
    rows = (_extract_jongno_em_label_rows(table) + _extract_rows(table, url)) if table else []
    return _extract_jongno_author_info_from_rows(rows)


def _extract_jongno_author_info_from_rows(
    rows: list[tuple[str, str]],
    *,
    prefer_department: bool = False,
) -> Dict[str, str]:
    department = ""
    person = ""
    for label, value in rows:
        flat = _flat_label(label)
        first_value = _collapse_ws(value.splitlines()[0] if value else "")
        if not first_value:
            continue
        if not department and flat in {"처리부서", "담당부서", "부서", "주관부서", "접수처"}:
            department = first_value
        if not person and flat in {"담당자", "글쓴이", "작성자", "등록자"}:
            person = first_value

    if person and not prefer_department:
        out = {
            "author": person,
            "author_raw": person,
            "author_kind": "person",
        }
        if department:
            out["department"] = department
            out["department_raw"] = department
        return out
    if department:
        return {
            "author": department,
            "author_raw": department,
            "department": department,
            "department_raw": department,
            "author_kind": "org",
        }
    return {}


def _extract_jongno_em_label_rows(table) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    if table is None:
        return rows
    try:
        items = list(table.select("li"))
        items.extend(th for th in table.select("th") if not th.select("li"))
    except Exception:
        items = []
    for item in items:
        try:
            label_node = item.find("em")
        except Exception:
            label_node = None
        if label_node is None:
            continue
        label = _collapse_ws(label_node.get_text(" ", strip=True))
        if not label:
            continue
        try:
            clone = item.__copy__()
            for em in list(clone.find_all("em")):
                em.decompose()
            value = _collapse_ws(clone.get_text(" ", strip=True))
        except Exception:
            value = ""
        if value:
            rows.append((label, value))
    return rows


def extract_jongno_construction_status(soup, url: str) -> Optional[Dict[str, str]]:
    if not soup or not is_jongno_construction_status_url(url or ""):
        return None

    table = soup.select_one("table.view_type03")
    if table is None:
        return None

    rows = _extract_rows(table, url)
    if not rows:
        return None

    filtered_rows: list[tuple[str, str]] = []
    for label, value in rows:
        label_norm = _collapse_ws(label)
        value_norm = _collapse_ws(value)
        if not label_norm or not value_norm:
            continue
        if label_norm == "대지위치(지도)":
            continue
        filtered_rows.append((label_norm, value_norm))

    if not filtered_rows:
        return None

    title = ""
    preferred_title_labels = (
        "대지위치(주소)",
        "허가번호",
    )
    for preferred_label in preferred_title_labels:
        for label, value in filtered_rows:
            if label == preferred_label and value:
                title = value
                break
        if title:
            break
    if not title:
        title = "공사장현황 안내"

    content_text = "\n".join(f"{label}: {value}" for label, value in filtered_rows).strip()
    if not content_text:
        return None

    return {
        "title": title,
        "content_text": content_text,
        "content_html": str(table),
        "snippet": _collapse_ws(content_text)[:200],
    }


def extract_jongno_board_title(soup, url: str) -> str:
    if not soup or not is_jongno_board_article_url(url or ""):
        return ""

    title = _clean_jongno_title(_extract_jongno_div_table_value(_find_jongno_apply_table(soup), "제목"))
    if title:
        return title

    for table in soup.select(".board_view table"):
        title = _clean_jongno_title(_extract_jongno_labeled_table_value(table, "제목"))
        if title:
            return title

    for selector in (
        ".board_view table.view_type01 th.notice_title h4",
        ".board_view table.view_type01 th.first.notice_title h4",
        "table.view_type01 th.notice_title h4",
        "table.view_type01 h4",
        ".board_view h4",
    ):
        try:
            element = soup.select_one(selector)
        except Exception:
            element = None
        if not element:
            continue
        title = _clean_jongno_title(element.get_text(" ", strip=True))
        if title:
            return title

    for selector in (
        ".board_view table.view_type01 th.notice_title",
        ".board_view table.view_type01 th.first.notice_title",
        "table.view_type01 th.notice_title",
        "table.view_type01 th.first.notice_title",
    ):
        try:
            element = soup.select_one(selector)
        except Exception:
            element = None
        if not element:
            continue
        title = _clean_jongno_title(_extract_jongno_notice_title_text(element))
        if title:
            return title
    return ""


def _extract_jongno_notice_title_text(element) -> str:
    if element is None:
        return ""
    try:
        cloned = element.__copy__()
    except Exception:
        cloned = element
    try:
        for blind in list(cloned.select(".blind")):
            blind.decompose()
    except Exception:
        pass
    return _collapse_ws(cloned.get_text(" ", strip=True))


def _extract_jongno_labeled_table_value(table, label: str) -> str:
    if table is None:
        return ""
    wanted = _flat_label(label)
    try:
        rows = table.find_all("tr")
    except Exception:
        rows = []
    for tr in rows:
        try:
            cells = tr.find_all(["th", "td"], recursive=False)
        except Exception:
            cells = []
        for idx, cell in enumerate(cells):
            if (cell.name or "").lower() != "th":
                continue
            if _flat_label(cell.get_text(" ", strip=True)) != wanted:
                continue
            for value_cell in cells[idx + 1 :]:
                if (value_cell.name or "").lower() != "td":
                    continue
                value = _collapse_ws(value_cell.get_text(" ", strip=True))
                if value:
                    return value
    return ""


def _extract_jongno_div_table_value(table, label: str) -> str:
    if table is None:
        return ""
    wanted = _flat_label(label)
    try:
        rows = table.select(":scope > .tr")
    except Exception:
        try:
            rows = table.find_all(class_="tr", recursive=False)
        except Exception:
            rows = []
    if not rows:
        try:
            rows = table.select(".tr")
        except Exception:
            rows = []
    for row in rows:
        try:
            label_cell = row.select_one(":scope > .th, :scope > th")
            value_cell = row.select_one(":scope > .td, :scope > td")
        except Exception:
            label_cell = row.find(class_="th", recursive=False)
            value_cell = row.find(class_="td", recursive=False)
        if label_cell is None:
            label_cell = row.find(["th"], recursive=False) or row.find(class_="th")
        if value_cell is None:
            value_cell = row.find(["td"], recursive=False) or row.find(class_="td")
        if label_cell is None or value_cell is None:
            continue
        if _flat_label(label_cell.get_text(" ", strip=True)) != wanted:
            continue
        strong = value_cell.find("strong")
        source = strong or value_cell
        value = _collapse_ws(source.get_text(" ", strip=True))
        if value:
            return value
    return ""


def _find_jongno_apply_table(soup):
    if soup is None:
        return None
    try:
        tables = soup.select("div#divTable.table")
    except Exception:
        tables = []
    for table in tables:
        try:
            classes = table.get("class") or []
            if isinstance(classes, str):
                classes = classes.split()
            if "page" in classes:
                continue
        except Exception:
            pass
        if _extract_jongno_div_table_value(table, "제목"):
            return table
    return tables[0] if tables else None


def extract_jongno_apply_title(soup, url: str) -> str:
    if not soup or not is_jongno_apply_view_url(url or ""):
        return ""
    table = _find_jongno_apply_table(soup)
    title = _extract_jongno_div_table_value(table, "제목")
    if title:
        return title
    for legacy_table in _find_jongno_apply_legacy_tables(soup):
        title = _extract_jongno_labeled_table_value(legacy_table, "제목")
        if title:
            return title
    return ""


def _find_jongno_apply_legacy_tables(soup) -> list[Any]:
    if soup is None:
        return []
    try:
        tables = list(soup.select("#subContent .board_view table, .content_area .board_view table"))
    except Exception:
        tables = []
    out: list[Any] = []
    for table in tables:
        try:
            text = _flat_label(table.get_text(" ", strip=True))
        except Exception:
            text = ""
        if not text:
            continue
        if any(label in text for label in ("제목", "신고내용", "답변", "진행상태")):
            out.append(table)
    return out


def _extract_jongno_apply_legacy_rows(soup, url: str) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for table in _find_jongno_apply_legacy_tables(soup):
        rows.extend(_extract_rows(table, url))
    return rows


def extract_jongno_apply_post(soup, url: str) -> Optional[Dict[str, str]]:
    if not soup or not is_jongno_apply_view_url(url or ""):
        return None
    table = _find_jongno_apply_table(soup)
    title = extract_jongno_apply_title(soup, url)
    content = _extract_jongno_div_table_value(table, "내용") if table is not None else ""
    content_html = str(table) if table is not None else ""
    if not content:
        legacy_rows = _extract_jongno_apply_legacy_rows(soup, url)
        keep_labels = {
            "신고내용",
            "내용",
            "답변",
            "진행상태",
            "처리기한",
            "처리일자",
            "담당부서",
        }
        content_lines: list[str] = []
        for label, value in legacy_rows:
            flat = _flat_label(label)
            value = value.strip()
            if not value or flat not in keep_labels:
                continue
            label_norm = _collapse_ws(label)
            if flat in {"신고내용", "내용", "답변"}:
                content_lines.append(value)
            else:
                content_lines.append(f"{label_norm}: {value}")
        content = "\n".join(line for line in content_lines if _collapse_ws(line)).strip()
        legacy_tables = _find_jongno_apply_legacy_tables(soup)
        content_html = "\n".join(str(t) for t in legacy_tables)
    if not title and not content:
        return None
    return {
        "title": title or "제목 없음",
        "content_text": content,
        "content_html": content_html,
        "snippet": _collapse_ws(content)[:200],
    }


def extract_jongno_council_assembly_title(soup, url: str) -> str:
    if not soup or not is_jongno_council_assembly_view_url(url or ""):
        return ""
    for selector in (
        ".chairman-detail dt strong",
        ".chairman-detail .chairman-txt dt strong",
        ".chairman-detail dt",
    ):
        try:
            element = soup.select_one(selector)
        except Exception:
            element = None
        if not element:
            continue
        strong = element.find("strong") if getattr(element, "name", "") != "strong" else element
        source = strong or element
        title = _collapse_ws(source.get_text(" ", strip=True))
        if title:
            return title
    return ""


def extract_jongno_council_assembly_post(soup, url: str) -> Optional[Dict[str, str]]:
    if not soup or not is_jongno_council_assembly_view_url(url or ""):
        return None
    detail = soup.select_one(".chairman-detail")
    if detail is None:
        return None
    title = extract_jongno_council_assembly_title(soup, url)
    if not title:
        return None

    lines: list[str] = []
    try:
        cloned = detail.__copy__()
    except Exception:
        cloned = detail
    try:
        for img in list(cloned.find_all("img")):
            alt = _collapse_ws(img.get("alt") or "")
            if alt:
                img.replace_with(alt)
            else:
                img.decompose()
    except Exception:
        pass
    try:
        text = cloned.get_text("\n", strip=True)
    except Exception:
        text = detail.get_text("\n", strip=True)
    for line in text.splitlines():
        value = _collapse_ws(line)
        if not value:
            continue
        if value not in lines:
            lines.append(value)
    content_text = "\n".join(lines).strip()
    return {
        "title": title,
        "content_text": content_text or title,
        "content_html": str(detail),
        "snippet": _collapse_ws(content_text or title)[:200],
    }


def _find_jongno_general_board_table(soup):
    if soup is None:
        return None
    try:
        views = soup.select(".board_view:not(.list), div.board_view")
    except Exception:
        views = []
    for view in views:
        try:
            table = view.select_one("table.view_type01")
        except Exception:
            table = None
        if not table:
            continue
        try:
            if table.select_one("th.notice_title") and (
                table.select_one("tr.notice_txt td.output")
                or table.select_one("tr.notice_txt")
            ):
                return table
        except Exception:
            continue
    try:
        tables = soup.select("table.view_type01")
    except Exception:
        tables = []
    for table in tables:
        try:
            if "board_view list" in " ".join(table.parent.get("class") or []):
                continue
            if table.select_one("th.notice_title") and (
                table.select_one("tr.notice_txt td.output")
                or table.select_one("tr.notice_txt")
            ):
                return table
        except Exception:
            continue
    try:
        tables = soup.select(".board_view table")
    except Exception:
        tables = []
    for table in tables:
        try:
            if "board_view list" in " ".join(table.parent.get("class") or []):
                continue
            if _extract_jongno_labeled_table_value(table, "제목") and _extract_jongno_labeled_table_value(table, "내용"):
                return table
        except Exception:
            continue
    return None


def _jongno_notice_body_text(cell, base_url: str) -> str:
    if cell is None:
        return ""
    try:
        cloned = cell.__copy__()
    except Exception:
        cloned = cell
    try:
        for blind in list(cloned.select(".blind")):
            blind.decompose()
    except Exception:
        pass
    try:
        for a in list(cloned.find_all("a")):
            href = _collapse_ws(a.get("href") or "")
            label = _collapse_ws(a.get_text(" ", strip=True))
            if href:
                full = urljoin(base_url or "", href)
                a.replace_with(f"{label} ({full})" if label else full)
    except Exception:
        pass
    try:
        for br in list(cloned.find_all("br")):
            br.replace_with("\n")
    except Exception:
        pass
    try:
        text = cloned.get_text("\n", strip=True)
    except Exception:
        text = cell.get_text("\n", strip=True)
    text = unescape(text).replace("\xa0", " ")
    lines = [_collapse_ws(line) for line in text.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def extract_jongno_general_board_post(soup, url: str) -> Optional[Dict[str, str]]:
    if not soup or not is_jongno_board_article_url(url or ""):
        return None
    if is_jongno_minwon_form_url(url or "") or is_jongno_construction_status_url(url or ""):
        return None

    table = _find_jongno_general_board_table(soup)
    if table is None:
        return None

    title = extract_jongno_board_title(soup, url)
    if not title:
        title_cell = table.select_one("th.notice_title")
        if title_cell is not None:
            title = _extract_jongno_notice_title_text(title_cell)
    if not title:
        return None

    body_cell = table.select_one("tr.notice_txt td.output")
    if body_cell is None:
        row = table.select_one("tr.notice_txt")
        body_cell = row.find(["td", "th"]) if row else None
    if body_cell is None:
        try:
            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"], recursive=False)
                for idx, cell in enumerate(cells):
                    if (cell.name or "").lower() == "th" and _flat_label(cell.get_text(" ", strip=True)) == "내용":
                        body_cell = next(
                            (
                                value_cell
                                for value_cell in cells[idx + 1 :]
                                if (value_cell.name or "").lower() == "td"
                            ),
                            None,
                        )
                        break
                if body_cell is not None:
                    break
        except Exception:
            body_cell = None
    content_text = _jongno_notice_body_text(body_cell, url)
    if not content_text:
        return None

    return {
        "title": title,
        "content_text": content_text,
        "content_html": str(body_cell or table),
        "snippet": _collapse_ws(content_text)[:200],
    }


def extract_jongno_council_post(soup, url: str) -> Optional[Dict[str, str]]:
    if not soup or not is_jongno_council_board_view_url(url or ""):
        return None

    table = soup.select_one("table.table-type1")
    if table is None:
        return None

    title = ""
    for selector in (
        "thead th[colspan]",
        "thead th",
    ):
        try:
            element = table.select_one(selector)
        except Exception:
            element = None
        if not element:
            continue
        title = _collapse_ws(element.get_text(" ", strip=True))
        if title:
            break
    if not title:
        return None

    lines: list[str] = []
    body_html = ""

    for tr in table.select("tbody > tr"):
        direct_cells = tr.find_all(["th", "td"], recursive=False)
        if not direct_cells:
            continue
        if len(direct_cells) == 1:
            only_cell = direct_cells[0]
            txt_area = only_cell.select_one(".txt-area")
            if txt_area is not None:
                body_html = str(txt_area)
                body_text = _cell_text_with_links(txt_area, url)
                if body_text:
                    lines.append(body_text)
                continue
            single_text = _cell_text_with_links(only_cell, url)
            if single_text:
                lines.append(single_text)
            continue

        pending_label = ""
        for cell in direct_cells:
            name = (cell.name or "").lower()
            if name == "th":
                pending_label = _collapse_ws(cell.get_text(" ", strip=True))
                continue
            if name != "td":
                continue
            value = _cell_text_with_links(cell, url)
            if not value:
                pending_label = ""
                continue
            if pending_label == "첨부파일":
                pending_label = ""
                continue
            if "이미지변환중입니다." in value:
                pending_label = ""
                continue
            if pending_label and pending_label not in {"내용", "본문"}:
                lines.append(f"{pending_label}: {value}")
            else:
                lines.append(value)
            pending_label = ""

    content_text = "\n".join(line for line in lines if _collapse_ws(line)).strip()
    if not content_text:
        return None
    if not body_html:
        body_html = str(table)

    return {
        "title": title,
        "content_text": content_text,
        "content_html": body_html,
        "snippet": _collapse_ws(content_text)[:200],
    }
