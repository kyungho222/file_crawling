"""
Guro-gu office (guro.go.kr) board parsing helpers.

This module keeps Guro-specific title and body rules out of the generic
board extractor. Guro pages mostly use the p-table layout, but some boards
store meaningful business fields outside the plain content cell.
"""

from __future__ import annotations

import re
from typing import Any, Optional
from urllib.parse import parse_qs, urljoin, urlparse


GURO_TITLE_SELECTORS = (
    "tr.p-table__subject .p-table__subject_text",
    ".p-table__subject .p-table__subject_text",
    ".p-table__subject_text",
    "h3.h0.title",
    ".poll_view > h4",
    ".title",
    "h3",
    "h2",
    "h1",
)

GURO_CONTENT_SELECTOR = ".p-table__content"
GURO_PROPOSAL_BBS_NO = "769"
GURO_ATTACHMENT_CONTEXT_BBS_NOS = {"687", "838", "846", "847", "855", "1145"}
GURO_PROPOSAL_LABELS = {
    "사업명",
    "총사업비",
    "사업위치",
    "사업기간",
    "사업개요",
    "사업내용",
    "사업효과",
}
GURO_CONTENT_TABLE_BBS_NOS = {"1187"}
GURO_CONTENT_TABLE_LABELS = {
    "년도",
    "제목",
    "소개글",
    "HTML주소",
}


def _collapse_ws(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "").replace("\xa0", " ")).strip()


def _strip_leading_title(content_text: str, title: str) -> str:
    text = (content_text or "").strip()
    title_norm = _collapse_ws(title)
    if not text or not title_norm:
        return text

    lines = text.splitlines()
    while lines and not _collapse_ws(lines[0]):
        lines.pop(0)
    if not lines:
        return ""

    first = _collapse_ws(lines[0])
    if first == title_norm:
        return "\n".join(lines[1:]).lstrip()

    flat = _collapse_ws(text)
    if flat.startswith(title_norm + " "):
        return flat[len(title_norm) :].strip()
    return text


def _norm_label(text: Any) -> str:
    return re.sub(r"\s+", "", str(text or "")).strip()


def _url_parts(url: str) -> tuple[str, str, dict[str, list[str]]]:
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        return host, (parsed.path or "").lower(), parse_qs(parsed.query)
    except Exception:
        low = (url or "").lower()
        return ("guro.go.kr" if "guro.go.kr" in low else "", low, {})


def is_guro_url(url: str) -> bool:
    host, _path, _qs = _url_parts(url or "")
    return host == "guro.go.kr" or host.endswith(".guro.go.kr")


def is_guro_board_view_url(url: str) -> bool:
    host, path, _qs = _url_parts(url or "")
    return (host == "guro.go.kr" or host.endswith(".guro.go.kr")) and path.endswith(
        "/selectbbsnttview.do"
    )


def is_guro_lecture_view_url(url: str) -> bool:
    host, path, _qs = _url_parts(url or "")
    return (host == "guro.go.kr" or host.endswith(".guro.go.kr")) and path.endswith(
        "/yeyak/edclctreview.do"
    )


def is_guro_propse_view_url(url: str) -> bool:
    host, path, _qs = _url_parts(url or "")
    return (host == "guro.go.kr" or host.endswith(".guro.go.kr")) and path.endswith(
        "/guro1st/propseview.do"
    )


def guro_bbs_no(url: str) -> str:
    _host, _path, qs = _url_parts(url or "")
    value = qs.get("bbsNo") or qs.get("bbsno") or []
    return str(value[0]) if value else ""


def is_guro_proposal_url(url: str) -> bool:
    return is_guro_board_view_url(url) and guro_bbs_no(url) == GURO_PROPOSAL_BBS_NO


def guro_content_selector_hint(url: str) -> str:
    if not is_guro_url(url):
        return ""
    return GURO_CONTENT_SELECTOR


def _clean_title_text(value: Any) -> str:
    text = _collapse_ws(value)
    text = re.sub(r"\s*신청자\s*:\s*\d+\s*/\s*\d+\s*명.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*대기자\s*:\s*\d+\s*/\s*\d+\s*명.*$", "", text, flags=re.IGNORECASE)
    return text.strip(" |:-")


def _label_value(root: Any, *labels: str) -> str:
    if root is None:
        return ""
    wanted = {_norm_label(label) for label in labels if label}
    for tr in root.select("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        for idx, cell in enumerate(cells[:-1]):
            label = _norm_label(cell.get_text(" ", strip=True))
            if label not in wanted:
                continue
            for nxt in cells[idx + 1 :]:
                value = _clean_title_text(nxt.get_text(" ", strip=True))
                if value:
                    return value
    return ""


def _clean_guro_lecture_org(value: Any) -> str:
    text = _collapse_ws(value)
    if not text:
        return ""
    text = re.sub(r"\b\d{2,3}-\d{3,4}-\d{4}\b.*$", "", text).strip()
    text = re.sub(r"\b\d{5}\s+.*$", "", text).strip()
    text = re.sub(r"\s+\d+\s*(?:층|호|실)\b.*$", "", text).strip()
    return text.strip(" |:-")


def _guro_lecture_org_from_place(value: Any) -> str:
    text = _collapse_ws(value)
    if not text:
        return ""
    for pattern in (
        r"([가-힣0-9]+(?:동|읍|면)\s*주민센터)",
        r"([가-힣0-9]+(?:동|읍|면)\s*자치회관)",
        r"([가-힣0-9]+(?:동|읍|면))",
    ):
        match = re.search(pattern, text)
        if match:
            return re.sub(r"\s+", "", match.group(1)).strip()
    return _clean_guro_lecture_org(text)


def extract_guro_author_info(soup: Any, *, url: str = "") -> dict[str, str]:
    if soup is None:
        return {}

    if is_guro_propse_view_url(url):
        author = _guro_propse_author(soup)
        if not author:
            return {}
        return {
            "author": author,
            "author_raw": author,
            "author_kind": "person",
        }

    if is_guro_lecture_view_url(url):
        owner = _clean_guro_lecture_org(_label_value(soup, "주최", "주관", "운영기관", "교육기관", "기관"))
        if not owner:
            owner = _guro_lecture_org_from_place(_label_value(soup, "강의장소", "교육장소", "장소"))
        if not owner:
            return {}

        return {
            "author": owner,
            "author_raw": owner,
            "department": owner,
            "department_raw": owner,
            "author_kind": "org",
        }

    return {}


def _guro_propse_author(soup: Any) -> str:
    for selector in (
        ".suggestion_view_title .name_date .name",
        ".suggestion .view .name_date .name",
        ".name_date .name",
    ):
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        if node is None:
            continue
        try:
            text = _collapse_ws(node.get_text(" ", strip=True))
        except Exception:
            text = ""
        text = re.sub(r"^(?:작성자|등록자|제안자)\s*", "", text).strip(" |:-")
        if text:
            return text
    return ""


def extract_guro_title(soup: Any, *, url: str = "") -> str:
    if soup is None or (url and not is_guro_url(url)):
        return ""

    url_low = (url or "").lower()
    if "/guro1st/" in url_low:
        for selector in (
            ".execution_view_title .info_cts h3.title",
            ".execution_view_title h3.title",
            ".bbs__view .info_cts h3.title",
        ):
            try:
                node = soup.select_one(selector)
            except Exception:
                node = None
            title = _clean_title_text(node.get_text(" ", strip=True)) if node else ""
            if title:
                return title

    if "/yeyak/edclctreview.do" in url_low:
        for selector in (
            "tbody tr:first-child td:first-child",
            "table tbody tr:first-child td:first-child",
            "tbody tr:first-child td",
            "table tbody tr:first-child td",
        ):
            try:
                node = soup.select_one(selector)
            except Exception:
                node = None
            title = _clean_title_text(node.get_text(" ", strip=True)) if node else ""
            if title and title not in {"< 참고사항 >", "참고사항"}:
                return title

    title_from_label = _label_value(soup, "제목", "사업명", "강좌명", "교육명")
    if title_from_label:
        return title_from_label

    try:
        first_row = soup.select_one("table.p-table tr")
        cells = first_row.find_all(["th", "td"], recursive=False) if first_row else []
    except Exception:
        cells = []
    if len(cells) == 1 and getattr(cells[0], "name", "") == "td":
        title = _clean_title_text(cells[0].get_text(" ", strip=True))
        if title and "상세보기" not in title and title not in {"구로구청", "본문"}:
            return title

    for selector in GURO_TITLE_SELECTORS:
        try:
            node = soup.select_one(selector)
        except Exception:
            node = None
        title = _clean_title_text(node.get_text(" ", strip=True)) if node else ""
        if title and title not in {"구로구청", "본문", "참고사항", "< 참고사항 >"}:
            return title
    return ""


def _content_text(node: Any) -> str:
    if node is None:
        return ""
    try:
        for tag in node.find_all(attrs={"hidden": True}):
            tag.decompose()
        for tag in node.find_all(style=True):
            style = str(tag.get("style") or "").replace(" ", "").lower()
            if "display:none" in style or "visibility:hidden" in style:
                tag.decompose()
        for tag in node.find_all(["script", "style", "noscript", "iframe", "svg"]):
            tag.decompose()

        block_names = {"p", "li"}
        block_lines: list[str] = []
        candidates = []
        if getattr(node, "name", None) in block_names:
            candidates.append(node)
        candidates.extend(node.find_all(list(block_names)))
        for block in candidates:
            if block is not node and block.find_parent(list(block_names)) is not None:
                continue
            line = _collapse_ws(block.get_text(" ", strip=True))
            if line:
                block_lines.append(line)
        if not block_lines:
            tr_candidates = [node] if getattr(node, "name", None) == "tr" else []
            tr_candidates.extend(node.find_all("tr"))
            for block in tr_candidates:
                if block is not node and block.find_parent("tr") is not None:
                    continue
                line = _collapse_ws(block.get_text(" ", strip=True))
                if line:
                    block_lines.append(line)
        if block_lines:
            return "\n".join(block_lines).strip()

        for tr in node.find_all("tr"):
            tr.insert_after("\n")
        for tag in node.find_all(["p", "div", "li", "br", "section", "article"]):
            tag.insert_after("\n")
        lines = [_collapse_ws(line) for line in node.get_text("\n", strip=True).splitlines()]
        return "\n".join(line for line in lines if line).strip()
    except Exception:
        try:
            return _collapse_ws(node.get_text(" ", strip=True))
        except Exception:
            return ""


def _clean_guro_body_text(text: Any) -> str:
    value = str(text or "").replace("\xa0", " ").strip()
    if not value:
        return ""
    email = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    value = re.sub(r"\(\s*\n\s*(" + email + r")\s*\n\s*\)", r"(\1)", value)
    value = re.sub(r"\(\s*\n\s*(" + email + r")\s*\)", r"(\1)", value)
    value = re.sub(r"\n\s*(\d+\s*부\.)", r" \1", value)
    return value.strip()


def _image_lines(node: Any, base_url: str) -> list[str]:
    if node is None:
        return []
    lines: list[str] = []
    seen: set[str] = set()
    try:
        images = node.find_all("img")
    except Exception:
        images = []
    for img in images:
        try:
            src = (img.get("src") or "").strip()
        except Exception:
            src = ""
        if not src:
            continue
        if src.lower().startswith("data:"):
            continue
        full_src = urljoin(base_url, src)
        if full_src.lower().startswith("data:") or len(full_src) > 2000:
            continue
        if full_src in seen:
            continue
        seen.add(full_src)
        alt = _collapse_ws(img.get("alt") or img.get("title") or "")
        if alt:
            lines.append(f"본문 이미지: {alt} ({full_src})")
        else:
            lines.append(f"본문 이미지: {full_src}")
    return lines


def _absolute_guro_url(base_url: str, href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    if href.lower().startswith(("http://", "https://")):
        return href
    try:
        parsed = urlparse(base_url if "://" in base_url else f"https://{base_url}")
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if href.startswith("/"):
            return urljoin(origin, href)
        if href.lower().startswith(("downloadbbsfile.do", "previewbbs.do")):
            return urljoin(origin + "/", href)
    except Exception:
        pass
    return urljoin(base_url, href)


def _attachment_lines(root: Any, base_url: str) -> list[str]:
    if root is None:
        return []
    lines: list[str] = []
    seen: set[str] = set()
    try:
        links = root.select(".p-attach a[href], a[href*='downloadBbsFile.do']")
    except Exception:
        links = []
    for link in links:
        try:
            href = (link.get("href") or "").strip()
        except Exception:
            href = ""
        if not href:
            continue
        href_low = href.lower()
        text = _collapse_ws(link.get_text(" ", strip=True))
        if "previewbbs.do" in href_low or "미리보기" in text or "음성듣기" in text:
            continue
        if "downloadbbsfile.do" not in href_low:
            continue

        full_href = _absolute_guro_url(base_url, href)
        if full_href in seen:
            continue
        seen.add(full_href)

        name = text
        name = re.sub(r"\b파일다운로드\b", "", name).strip()
        name = re.sub(r"\b미리보기/음성듣기\b", "", name).strip()
        name = re.sub(r"^(?:pdf|hwp|hwpx|docx?|xlsx?|pptx?)\s+파일\s+", "", name, flags=re.IGNORECASE).strip()
        if name:
            lines.append(f"첨부파일: {name} ({full_href})")
        else:
            lines.append(f"첨부파일: {full_href}")
    return lines


def _proposal_parts(table: Any, url: str = "") -> list[str]:
    parts: list[str] = []
    structured_count = 0
    for tr in table.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not td:
            continue
        label = _collapse_ws(th.get_text(" ", strip=True)) if th else ""
        value = _collapse_ws(_content_text(td))
        image_lines = _image_lines(td, url)
        if not value and not image_lines:
            continue
        value_parts = [value] if value else []
        value_parts.extend(image_lines)
        value = "\n".join(value_parts)

        classes = td.get("class") or []
        if label in GURO_PROPOSAL_LABELS:
            structured_count += 1
            parts.append(f"{label}: {value}")
        elif "p-table__content" in classes:
            parts.append(f"내용: {value}")

    return parts if structured_count else []


def _content_table_parts(table: Any) -> list[str]:
    parts: list[str] = []
    for tr in table.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        label = _collapse_ws(cells[0].get_text(" ", strip=True))
        value = _collapse_ws(_content_text(cells[1]))
        if not label or label not in GURO_CONTENT_TABLE_LABELS or not value:
            continue
        parts.append(f"{label}: {value}")
    return parts


def _clean_guro1st_line(text: Any) -> str:
    line = _collapse_ws(text)
    line = re.sub(r"\s+([),.:])", r"\1", line)
    line = re.sub(r"([(])\s+", r"\1", line)
    line = re.sub(r"([제])\s+(\d+)\s+조", r"\1\2조", line)
    line = re.sub(r"^-\s*", "- ", line)
    return line.strip()


def _guro1st_content_text(root: Any) -> str:
    if root is None:
        return ""
    parts: list[str] = []
    try:
        items = root.select(".view_list .list_item")
    except Exception:
        items = []
    if not items:
        return _content_text(root)

    seen: set[str] = set()
    for item in items:
        heading = item.select_one("h4.title")
        if heading:
            sub = heading.select_one(".sub_title")
            sub_text = _clean_guro1st_line(sub.get_text(" ", strip=True)) if sub else ""
            if sub:
                sub.decompose()
            head_text = _clean_guro1st_line(heading.get_text(" ", strip=True))
            for line in (head_text, sub_text):
                if line and line not in seen:
                    parts.append(line)
                    seen.add(line)

        for p in item.find_all("p"):
            try:
                if p.find("p"):
                    continue
                line = _clean_guro1st_line(p.get_text(" ", strip=True))
            except Exception:
                line = ""
            if not line or line in seen:
                continue
            parts.append(line)
            seen.add(line)

    return "\n".join(parts).strip()


def _make_content_html(soup: Any, parts: list[str], css_class: str) -> str:
    try:
        doc = soup.__class__(f'<div class="{css_class}"></div>', "html.parser")
        wrap = doc.find("div")
        for line in parts:
            p = doc.new_tag("p")
            p.string = line
            wrap.append(p)
        return str(wrap or "")
    except Exception:
        return "\n".join(parts)


def try_extract_guro_post(soup: Any, url: str):
    if soup is None or not is_guro_url(url):
        return None

    try:
        from backend.board.board_content_extractor import BoardPostExtract
    except Exception:
        return None

    title = extract_guro_title(soup, url=url) or "제목 없음"

    if is_guro_proposal_url(url):
        table = soup.select_one("table.p-table")
        if table:
            parts = _proposal_parts(table, url)
            if parts:
                content_text = "\n".join(parts).strip()
                return BoardPostExtract(
                    url=url,
                    title=title,
                    content_text=content_text,
                    content_html=_make_content_html(soup, parts, "guro-proposal-extract-root"),
                    snippet=_collapse_ws(content_text)[:200],
                )

    if guro_bbs_no(url) in GURO_CONTENT_TABLE_BBS_NOS:
        table = soup.select_one("table.p-table")
        if table:
            parts = _content_table_parts(table)
            if parts:
                content_text = "\n".join(parts).strip()
                return BoardPostExtract(
                    url=url,
                    title=title,
                    content_text=content_text,
                    content_html=_make_content_html(soup, parts, "guro-content-table-extract-root"),
                    snippet=_collapse_ws(content_text)[:200],
                )

    if "/guro1st/" in (url or "").lower():
        body = soup.select_one(".execution_view_cts")
        if body:
            content_text = _guro1st_content_text(body)
            content_text = _strip_leading_title(content_text, title)
            if content_text:
                return BoardPostExtract(
                    url=url,
                    title=title,
                    content_text=content_text,
                    content_html=str(body).strip(),
                    snippet=_collapse_ws(content_text)[:200],
                )

    body = soup.select_one(GURO_CONTENT_SELECTOR)
    if not body:
        return None
    content_text = _clean_guro_body_text(_content_text(body))
    image_lines = _image_lines(body, url)
    if image_lines:
        text_parts = [content_text] if content_text else []
        text_parts.extend(image_lines)
        content_text = "\n".join(text_parts).strip()
    if guro_bbs_no(url) in GURO_ATTACHMENT_CONTEXT_BBS_NOS:
        table = soup.select_one("table.p-table") or soup
        attachment_lines = _attachment_lines(table, url)
        if attachment_lines:
            text_parts = [content_text] if content_text else []
            text_parts.extend(attachment_lines)
            content_text = "\n".join(text_parts).strip()
    content_text = _strip_leading_title(content_text, title)
    if not content_text:
        return None
    return BoardPostExtract(
        url=url,
        title=title,
        content_text=content_text,
        content_html=str(body).strip(),
        snippet=_collapse_ws(content_text)[:200],
    )
