import re
from typing import List

try:
    from bs4 import BeautifulSoup, NavigableString  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]
    NavigableString = None  # type: ignore[assignment]


_BREADCRUMB_SELECTORS = [
    "nav[aria-label*='breadcrumb']",
    "nav[aria-label*='Breadcrumb']",
    "ol.breadcrumb",
    "ul.breadcrumb",
    ".breadcrumb",
    "#breadcrumb",
    ".breadcrumbs",
    ".bread-crumb",
    ".breadCrumb",
    ".location",
    ".locationarea",
    ".page_location",
    ".page-location",
    ".pagepath",
    ".page-path",
    ".pagePath",
    ".path",
    # Gwangjin detail pages: <div class="hgroup"><p>HOME > ...</p></div>
    ".hgroup > p",
]


def _split_breadcrumb_tokens(text: str) -> List[str]:
    if not text:
        return []
    text = str(text)
    text = (
        text.replace("&gt;", ">")
        .replace("&raquo;", ">")
        .replace("&rsaquo;", ">")
        .replace("\u203a", ">")
        .replace("\u00bb", ">")
    )
    text = re.sub(r"\s+", " ", text).strip()
    parts = re.split(r"\s*(?:>|/|\\|\|)\s*", text)
    return [p.strip() for p in parts if p.strip()]


def _clean_breadcrumb_tokens(tokens: List[str]) -> List[str]:
    cleaned: List[str] = []
    seen_adjacent = ""
    for token in tokens:
        value = re.sub(r"\s+", " ", str(token or "")).strip()
        if not value or value in {">", "/", "|"}:
            continue
        low = value.lower().replace(" ", "")
        if low in {"home", "main", "start", "homepage"}:
            continue
        if value == seen_adjacent:
            continue
        cleaned.append(value)
        seen_adjacent = value
    return cleaned


def _own_label_tokens(node) -> List[str]:
    if node is None:
        return []
    labels: List[str] = []
    for child in getattr(node, "children", []) or []:
        try:
            child_name = str(getattr(child, "name", "") or "").lower()
        except Exception:
            child_name = ""
        if child_name in {"a", "span", "em", "strong", "button"}:
            text = child.get_text(" ", strip=True)
            if text:
                labels.append(text)
    return labels


def _own_label_text(node) -> str:
    if node is None:
        return ""
    for child in getattr(node, "children", []) or []:
        try:
            child_name = str(getattr(child, "name", "") or "").lower()
        except Exception:
            child_name = ""
        if child_name in {"a", "span", "em", "strong", "button"}:
            text = child.get_text(" ", strip=True)
            if text:
                return text
    parts: List[str] = []
    for child in getattr(node, "children", []) or []:
        try:
            child_name = str(getattr(child, "name", "") or "").lower()
        except Exception:
            child_name = ""
        if child_name in {"ul", "ol", "div", "nav"}:
            continue
        if NavigableString is not None and isinstance(child, NavigableString):
            parts.append(str(child))
        elif child_name in {"a", "span", "em", "strong", "button"}:
            parts.append(child.get_text(" ", strip=True))
    return re.sub(r"\s+", " ", " ".join(p for p in parts if p)).strip()


def _top_level_li_nodes(node) -> List[object]:
    try:
        items = node.find_all("li")
    except Exception:
        return []
    out: List[object] = []
    for item in items:
        parent = getattr(item, "parent", None)
        nested = False
        while parent is not None and parent is not node:
            if str(getattr(parent, "name", "") or "").lower() == "li":
                nested = True
                break
            parent = getattr(parent, "parent", None)
        if not nested:
            out.append(item)
    if out:
        return out
    # Some sites wrap breadcrumb li nodes in an extra ul/ol under the matched div.
    for item in items:
        parent = getattr(item, "parent", None)
        if str(getattr(parent, "name", "") or "").lower() in {"ul", "ol"}:
            grand = getattr(parent, "parent", None)
            if grand is node:
                out.append(item)
    return out




def _tokens_from_title(soup) -> List[str]:
    try:
        title_node = soup.find("title")
        title_text = title_node.get_text(" ", strip=True) if title_node else ""
    except Exception:
        title_text = ""
    if not title_text:
        return []
    tokens = _clean_breadcrumb_tokens(_split_breadcrumb_tokens(title_text))
    if tokens and tokens[0].endswith("청") and len(tokens) >= 2:
        tokens = tokens[1:]
    return tokens

def extract_file_breadcrumb_tokens_from_html(html: str, *, include_title_fallback: bool = True) -> List[str]:
    if not html or not BeautifulSoup:
        return []
    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
        candidates = []
        for selector in _BREADCRUMB_SELECTORS:
            try:
                candidates.extend(soup.select(selector))
            except Exception:
                pass
        for node in candidates:
            tokens: List[str] = []
            li_nodes = _top_level_li_nodes(node)
            if li_nodes:
                for li in li_nodes:
                    direct_tokens = _own_label_tokens(li)
                    if len(li_nodes) == 1 and len(direct_tokens) > 1:
                        # Some templates put every depth into one li. Only this exceptional
                        # shape preserves '/' inside a single menu name such as ??/??.
                        tokens.extend(direct_tokens)
                    else:
                        tokens.extend(_split_breadcrumb_tokens(_own_label_text(li)))
            else:
                direct_labels: List[str] = []
                for child in getattr(node, "children", []) or []:
                    try:
                        child_name = str(getattr(child, "name", "") or "").lower()
                    except Exception:
                        child_name = ""
                    if child_name in {"a", "span", "em", "strong", "button"}:
                        label = child.get_text(" ", strip=True)
                        if label:
                            direct_labels.append(label)
                if direct_labels:
                    for label in direct_labels:
                        tokens.extend(_split_breadcrumb_tokens(label))
                else:
                    tokens.extend(_split_breadcrumb_tokens(node.get_text(" ", strip=True)))
            cleaned = _clean_breadcrumb_tokens(tokens)
            if len(cleaned) >= 2:
                return cleaned
        if include_title_fallback:
            title_tokens = _tokens_from_title(soup)
            if len(title_tokens) >= 2:
                return title_tokens
        return []
    except Exception:
        return []


def extract_file_web_title_from_html(html: str) -> str:
    tokens = extract_file_breadcrumb_tokens_from_html(html)
    if not tokens:
        return ""
    last = tokens[-1]
    if len(last) < 2 and len(tokens) >= 2:
        last = tokens[-2]
    return re.sub(r"\s+", " ", last).strip()


def extract_file_category_breadcrumb_from_html(html: str) -> str:
    tokens = extract_file_breadcrumb_tokens_from_html(html)
    if len(tokens) >= 2:
        candidate_index = -2
        candidate = re.sub(r"\s+", " ", tokens[candidate_index]).strip()
        if candidate.endswith(" \uc18c\uac1c") and len(tokens) >= 3:
            candidate = re.sub(r"\s+", " ", tokens[-3]).strip()
        return candidate
    return ""
