"""Korea Copyright Commission(copyright.or.kr) board-specific parsing helpers."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional


def is_copyright_board_url(url: Optional[str]) -> bool:
    return "copyright.or.kr" in str(url or "").lower()


def _collapse_ws(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _is_dictionary_detail_url(url: Optional[str]) -> bool:
    low = str(url or "").lower()
    return "copyright.or.kr" in low and "/information-materials/dictionary/view.do" in low


def _is_dictionary_list_url(url: Optional[str]) -> bool:
    low = str(url or "").lower()
    return "copyright.or.kr" in low and "/information-materials/dictionary/list.do" in low


def _is_dictionary_title_noise(text: str) -> bool:
    compact = _collapse_ws(text)
    if not compact or compact == "-":
        return True
    lowered = compact.lower()
    noise_tokens = (
        "용어사전",
        "저작권기술 용어",
        "영어/불어",
        "참고",
    )
    return any(token.lower() in lowered for token in noise_tokens)


def _format_dictionary_title(term: Any) -> str:
    text = _collapse_ws(term)
    if _is_dictionary_title_noise(text):
        return ""
    return f"용어사전 - {text}"


def _extract_dictionary_term_title(soup: Any) -> str:
    if soup is None:
        return ""
    try:
        selectors = (
            "div.view_contents p[align='center'] strong span",
            "div.view_contents strong span",
            "div.view_contents strong",
        )
        for selector in selectors:
            for node in soup.select(selector):
                title = _format_dictionary_title(node.get_text(" ", strip=True))
                if title:
                    return title
    except Exception:
        pass

    table = soup.select_one("table.table-row") if hasattr(soup, "select_one") else None
    return _format_dictionary_title(_value_for_label(table or soup, "용어"))


def _value_for_label(scope: Any, label: str) -> str:
    if scope is None:
        return ""
    wanted = _collapse_ws(label)
    try:
        for tr in scope.select("tr"):
            cells = tr.find_all(["th", "td"], recursive=False)
            for idx, cell in enumerate(cells):
                if cell.name != "th":
                    continue
                cell_label = _collapse_ws(cell.get_text(" ", strip=True))
                if cell_label != wanted:
                    continue
                for nxt in cells[idx + 1 :]:
                    if nxt.name == "td":
                        return _collapse_ws(nxt.get_text(" ", strip=True))
    except Exception:
        return ""
    return ""


def extract_copyright_title(soup: Any, url: Optional[str] = None) -> str:
    if soup is None:
        return ""
    if _is_dictionary_list_url(url):
        return "용어사전"
    if _is_dictionary_detail_url(url):
        title = _extract_dictionary_term_title(soup)
        if title:
            return title
    try:
        hidden = soup.select_one("input#contentSubject")
        value = _collapse_ws(hidden.get("value") if hidden else "")
        if value:
            return value
    except Exception:
        pass
    table = soup.select_one("table.table-row") if hasattr(soup, "select_one") else None
    return _value_for_label(table or soup, "제목")


def split_copyright_department_author(raw: Any) -> Dict[str, str]:
    text = _collapse_ws(raw)
    if not text:
        return {}
    text = re.sub(r"\([^)]*\)", "", text).strip()
    if not text:
        return {}

    parts = text.split()
    department = text
    author = ""
    if len(parts) >= 2:
        last = parts[-1]
        if re.fullmatch(r"[가-힣]{2,4}", last):
            author = last
            department = " ".join(parts[:-1]).strip() or text

    out: Dict[str, str] = {
        "department": department,
        "department_raw": raw if isinstance(raw, str) else text,
    }
    if author:
        out.update(
            {
                "author": author,
                "author_raw": author,
                "author_kind": "person",
            }
        )
    else:
        out.update(
            {
                "author": department,
                "author_raw": department,
                "author_kind": "org",
            }
        )
    return out


def extract_copyright_author_info(html: str, url: Optional[str] = None) -> Dict[str, str]:
    if not html or not is_copyright_board_url(url):
        return {}
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return {}
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return {}
    table = soup.select_one("table.table-row") or soup
    return split_copyright_department_author(_value_for_label(table, "담당부서"))


def apply_copyright_author_info(
    author_info: Optional[Dict[str, Any]],
    *,
    html: str,
    url: Optional[str] = None,
) -> Dict[str, Any]:
    info: Dict[str, Any] = dict(author_info or {})
    extracted = extract_copyright_author_info(html, url=url)
    if extracted:
        info.update(extracted)
        info["_copyright_author_applied"] = "1"
    return info


def select_copyright_content_node(soup: Any) -> Any:
    if soup is None:
        return None
    try:
        node = soup.select_one("table.table-row td.td-cont")
        if node is not None:
            return node
        table = soup.select_one("table.table-row")
        if table is not None:
            rows = table.select("tr")
            if rows:
                for tr in reversed(rows):
                    if not tr.find("th"):
                        td = tr.find("td")
                        if td is not None:
                            return td
    except Exception:
        return None
    return None
