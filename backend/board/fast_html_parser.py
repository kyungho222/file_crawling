from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional
from urllib.parse import urljoin

try:
    from selectolax.lexbor import LexborHTMLParser  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional dependency during staged deploy
    LexborHTMLParser = None  # type: ignore[assignment]


_DATE_PATTERNS = (
    re.compile(r"(?<!\d)(20\d{2})[.\-/년\s]+(0?[1-9]|1[0-2])[.\-/월\s]+(0?[1-9]|[12]\d|3[01])(?:일)?(?!\d)"),
    re.compile(r"(?<!\d)(19\d{2})[.\-/년\s]+(0?[1-9]|1[0-2])[.\-/월\s]+(0?[1-9]|[12]\d|3[01])(?:일)?(?!\d)"),
)

_CONTENT_SELECTORS = (
    "#contents .board_view",
    "#contents .bbs_view",
    "#contents .view_area",
    "#contents .view_cont",
    "#contents .view_content",
    "#contents .board_view_content",
    "#contents .bbs_view_content",
    "#contents .tbl_view",
    "#content .board_view",
    "#content .bbs_view",
    "#content .view_area",
    "#content .view_cont",
    "#content .view_content",
    ".board_view",
    ".bbs_view",
    ".board-view",
    ".bbs-view",
    ".view_area",
    ".view_cont",
    ".view-content",
    ".view_content",
    ".board_view_content",
    ".bbs_view_content",
    ".tbl_view",
    "article",
    "#contents",
    "#content",
    "body",
)


@dataclass
class FastHtmlResult:
    ok: bool
    parser: str = "selectolax"
    title: str = ""
    text: str = ""
    date_text: str = ""
    date_dt: Optional[datetime] = None
    links: List[str] = field(default_factory=list)
    content_selector: str = ""
    html_len: int = 0
    text_len: int = 0
    reason: str = ""


def _node_text(node: Any, *, separator: str = " ", strip: bool = True) -> str:
    if node is None:
        return ""
    try:
        return str(node.text(separator=separator, strip=strip) or "")
    except TypeError:
        try:
            text = str(node.text(strip=strip) or "")
        except TypeError:
            text = str(node.text() or "")
    except Exception:
        return ""
    if separator != " ":
        return text.strip() if strip else text
    return re.sub(r"\s+", " ", text).strip() if strip else re.sub(r"\s+", " ", text)


def _attr(node: Any, name: str) -> str:
    try:
        return str((getattr(node, "attributes", {}) or {}).get(name) or "").strip()
    except Exception:
        return ""


def _extract_title(tree: Any) -> str:
    for selector, attr_name in (
        ('meta[property="og:title"]', "content"),
        ('meta[name="title"]', "content"),
        ('meta[name="twitter:title"]', "content"),
    ):
        try:
            node = tree.css_first(selector)
        except Exception:
            node = None
        value = _attr(node, attr_name)
        if value:
            return value
    try:
        node = tree.css_first("title")
    except Exception:
        node = None
    return _node_text(node)


def _extract_date(text: str) -> tuple[str, Optional[datetime]]:
    sample = str(text or "")[:20000]
    for pattern in _DATE_PATTERNS:
        match = pattern.search(sample)
        if not match:
            continue
        try:
            year, month, day = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
            return f"{year:04d}-{month:02d}-{day:02d}", datetime(year, month, day)
        except Exception:
            continue
    return "", None


def _select_content_node(tree: Any) -> tuple[Any, str]:
    for selector in _CONTENT_SELECTORS:
        try:
            node = tree.css_first(selector)
        except Exception:
            node = None
        if node is None:
            continue
        text = _node_text(node)
        if len(text) >= 30 or selector == "body":
            return node, selector
    return getattr(tree, "root", None), "root"


def _extract_links(tree: Any, base_url: str, *, limit: int = 80) -> List[str]:
    links: List[str] = []
    seen = set()
    try:
        nodes = tree.css("a[href]")
    except Exception:
        nodes = []
    for node in nodes:
        href = _attr(node, "href")
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        absolute = urljoin(base_url or "", href)
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
        if len(links) >= limit:
            break
    return links


def parse_fast_html(html: str, *, url: str = "") -> FastHtmlResult:
    html_len = len(html or "")
    if not html:
        return FastHtmlResult(ok=False, html_len=0, reason="empty_html")
    if LexborHTMLParser is None:
        return FastHtmlResult(ok=False, html_len=html_len, reason="selectolax_unavailable")

    try:
        tree = LexborHTMLParser(html)
    except Exception as exc:
        return FastHtmlResult(ok=False, html_len=html_len, reason=f"parse_error:{type(exc).__name__}")

    try:
        for node in tree.css("script,style,noscript,svg,iframe"):
            try:
                node.decompose()
            except Exception:
                pass
    except Exception:
        pass

    title = _extract_title(tree)
    content_node, content_selector = _select_content_node(tree)
    text = _node_text(content_node)
    if not text:
        try:
            text = _node_text(tree.root)
        except Exception:
            text = ""
    date_text, date_dt = _extract_date(text)
    links = _extract_links(tree, url)
    text_len = len(text or "")
    ok = bool(title or text_len >= 30 or date_dt)
    return FastHtmlResult(
        ok=ok,
        title=title,
        text=text,
        date_text=date_text,
        date_dt=date_dt,
        links=links,
        content_selector=content_selector,
        html_len=html_len,
        text_len=text_len,
        reason="" if ok else "insufficient_fast_result",
    )
