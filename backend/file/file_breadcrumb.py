import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

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

_BREADCRUMB_PROFILE_DIR = Path(__file__).with_name("breadcrumb_profiles")
_PROFILE_CACHE: Dict[str, Tuple[int, Dict[str, Any]]] = {}


def _profile_domain_candidates(detail_url: str) -> List[str]:
    try:
        host = (urlparse(str(detail_url or "")).hostname or "").strip().lower()
    except Exception:
        host = ""
    if not host:
        return []
    candidates = [host]
    if host.startswith("www."):
        candidates.append(host[4:])
    else:
        candidates.append(f"www.{host}")
    return candidates


def _load_breadcrumb_profile(detail_url: str) -> Dict[str, Any]:
    """Load an optional domain profile without making it a crawl dependency."""
    for domain in _profile_domain_candidates(detail_url):
        path = _BREADCRUMB_PROFILE_DIR / domain / f"{domain}.json"
        try:
            stat = path.stat()
        except OSError:
            continue
        cached = _PROFILE_CACHE.get(str(path))
        if cached and cached[0] == stat.st_mtime_ns:
            return cached[1]
        try:
            with path.open("r", encoding="utf-8") as profile_file:
                data = json.load(profile_file)
            profile = data if isinstance(data, dict) else {}
        except (OSError, ValueError, TypeError):
            profile = {}
        _PROFILE_CACHE[str(path)] = (stat.st_mtime_ns, profile)
        return profile
    return {}


def _profile_selectors(profile: Dict[str, Any]) -> List[str]:
    raw = profile.get("selectors") if isinstance(profile, dict) else None
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list):
        return []
    return [str(selector).strip() for selector in raw if str(selector or "").strip()]


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

def extract_file_breadcrumb_tokens_from_html(
    html: str,
    *,
    detail_url: str = "",
    include_title_fallback: bool = True,
) -> List[str]:
    if not html or not BeautifulSoup:
        return []
    try:
        soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
        candidates = []
        profile = _load_breadcrumb_profile(detail_url)
        profile_selectors = _profile_selectors(profile)
        selectors = profile_selectors + [
            selector for selector in _BREADCRUMB_SELECTORS if selector not in profile_selectors
        ]
        for selector in selectors:
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
        profile_title_fallback = profile.get("title_fallback") if profile else None
        use_title_fallback = include_title_fallback if profile_title_fallback is None else bool(profile_title_fallback)
        if use_title_fallback:
            title_tokens = _tokens_from_title(soup)
            if len(title_tokens) >= 2:
                return title_tokens
        return []
    except Exception:
        return []


def extract_file_web_title_from_html(html: str, *, detail_url: str = "") -> str:
    tokens = extract_file_breadcrumb_tokens_from_html(html, detail_url=detail_url)
    if not tokens:
        return ""
    last = tokens[-1]
    if len(last) < 2 and len(tokens) >= 2:
        last = tokens[-2]
    return re.sub(r"\s+", " ", last).strip()


def extract_file_category_breadcrumb_from_html(html: str, *, detail_url: str = "") -> str:
    tokens = extract_file_breadcrumb_tokens_from_html(html, detail_url=detail_url)
    if len(tokens) >= 2:
        profile = _load_breadcrumb_profile(detail_url)
        try:
            candidate_index = int(profile.get("category_index", -2))
        except (TypeError, ValueError):
            candidate_index = -2
        if not -len(tokens) <= candidate_index < len(tokens):
            candidate_index = -2
        candidate = re.sub(r"\s+", " ", tokens[candidate_index]).strip()
        if candidate.endswith(" \uc18c\uac1c") and len(tokens) >= 3:
            candidate = re.sub(r"\s+", " ", tokens[-3]).strip()
        return candidate
    return ""
