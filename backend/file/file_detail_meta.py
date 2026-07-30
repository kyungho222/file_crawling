from __future__ import annotations

import re
from typing import Any, Dict, Optional

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

from backend.shared.date_utils import parse_date


_DATE_LABELS = ("등록일", "작성일", "작성일자", "게시일", "게시일자", "등록일시")
_AUTHOR_LABELS = ("작성자", "글쓴이", "등록자", "담당자", "담당부서", "부서")


def _clean(value: Any, limit: int = 240) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit]


def _label_value_from_text(text: str, labels: tuple[str, ...]) -> str:
    body = _clean(text, 2000)
    for label in labels:
        pattern = rf"{re.escape(label)}\s*[:：]?\s*([^\n\r|]+)"
        match = re.search(pattern, body)
        if match:
            value = _clean(match.group(1), 160)
            value = re.split(r"\s+(?:등록일|작성일|조회|첨부|제목)\b", value)[0].strip()
            if value and value != label:
                return value
    return ""


def _date_from_text(text: str) -> str:
    labeled = _label_value_from_text(text, _DATE_LABELS)
    candidates = [labeled] if labeled else []
    candidates.extend(
        match.group(0)
        for match in re.finditer(
            r"(?:19|20)\d{2}[-./년]\s*\d{1,2}[-./월]\s*\d{1,2}(?:일)?(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?",
            text or "",
        )
    )
    for candidate in candidates:
        try:
            parsed = parse_date(candidate)
        except Exception:
            parsed = None
        if parsed is not None:
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
    return ""


def _meta_from_definition_like_nodes(soup: Any, labels: tuple[str, ...]) -> str:
    if soup is None:
        return ""
    for node in soup.find_all(["dt", "th", "strong", "span", "li", "p"]):
        label_text = _clean(node.get_text(" ", strip=True), 120)
        if not any(label in label_text for label in labels):
            continue
        sibling = node.find_next_sibling()
        if sibling is not None:
            value = _clean(sibling.get_text(" ", strip=True), 160)
            if value and value != label_text:
                return value
        parent = node.parent
        if parent is not None:
            parent_text = _clean(parent.get_text(" ", strip=True), 240)
            value = _label_value_from_text(parent_text, labels)
            if value:
                return value
    return ""


def extract_file_detail_meta_from_html(html: str, *, url: str = "") -> Dict[str, str]:
    """File-crawl-only detail metadata extraction.

    This intentionally does not import or call board crawling modules. It is used
    when file crawling fetches a post/detail page only to find attachments.
    """

    text = str(html or "")
    soup: Optional[Any] = None
    if BeautifulSoup is not None and text:
        try:
            soup = BeautifulSoup(text, "html.parser")  # type: ignore[operator]
        except Exception:
            soup = None

    visible_text = soup.get_text(" ", strip=True) if soup is not None else text
    reg_date = _meta_from_definition_like_nodes(soup, _DATE_LABELS)
    if reg_date:
        reg_date = _date_from_text(reg_date)
    if not reg_date:
        reg_date = _date_from_text(visible_text)

    author = _meta_from_definition_like_nodes(soup, _AUTHOR_LABELS)
    if not author:
        author = _label_value_from_text(visible_text, _AUTHOR_LABELS)

    department = ""
    if author and any(word in author for word in ("과", "팀", "부", "센터", "담당관")):
        department = author

    return {
        "reg_date": _clean(reg_date, 32),
        "author": _clean(author, 120),
        "department": _clean(department, 120),
    }
