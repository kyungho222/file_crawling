from __future__ import annotations

import asyncio
import json
import logging
import json
import time
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx
import requests
from bs4 import BeautifulSoup, Tag
from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import AnyHttpUrl, BaseModel, Field
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

logger = logging.getLogger("header_crawler")

# Debug logging helper (no file writes)
def _debug_log(*, location: str, message: str, data: Dict[str, Any], hypothesis_id: str) -> None:
    try:
        payload = {
            "sessionId": "debug-session",
            "runId": "run1",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        logging.getLogger("backend.shared.board_header").debug(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass
router = APIRouter(prefix="/api")

BOARD_PATTERNS = [
    "list.do",
    "board",
    "bbs",
    "notice",
    "news",
    "gallery",
    "faq",
    "qna",
    "post",
    "article",
    "anno",
    "notify",
    "announce",
]
BOARD_EXCLUDE_SUBSTRINGS = [
    "contents","member", "event"
]
BOARD_LIST_PATTERNS = [
    "list.do",
    "alllist.do",
    "eventlist.do",
]
NON_BOARD_PATTERNS = [
    "view.do",
    "forinsert.do",
    "apply.do",
    "deptgdc.do",
    "main.do",
    "map.do",
    "status.do",
    "contents.do",
]

SKIP_DEPTH_PATTERNS = [
    "/login",
    "/logout",
    "/join",
    "/register",
    "/signup",
    "/search",
    "/ajax",
    "/api/",
    "/rest/",
    "/json",
    "javascript:",
    "mailto:",
    "tel:",
]

FALLBACK_MENU_TITLES = {"fallback", "header", "sitemap"}
SITEMAP_TEXT_HINTS = {
    "사이트맵",
    "전체메뉴",
    "전체 메뉴",
    "sitemap",
    "site map",
    "site-map",
}
SITEMAP_HREF_HINTS = {
    "sitemap",
    "siteMap",
    "menuall",
    "allmenu",
}
AUTH_MENU_TITLES = {"로그인", "회원가입", "login", "sign up", "signup", "join"}

RAW_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
}

DEFAULT_HEADERS = dict(RAW_DEFAULT_HEADERS)
REQUEST_TIMEOUT_SECONDS = float(os.getenv("CRAWLER_REQUEST_TIMEOUT_SECONDS", "30"))
CONNECT_TIMEOUT_SECONDS = float(os.getenv("CRAWLER_CONNECT_TIMEOUT_SECONDS", "15"))
REQUEST_TIMEOUT = httpx.Timeout(REQUEST_TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT_SECONDS)
HTTPX_RETRY_COUNT = int(os.getenv("CRAWLER_HTTPX_RETRY_COUNT", "2"))
HTTPX_RETRY_BACKOFF_SECONDS = float(os.getenv("CRAWLER_HTTPX_RETRY_BACKOFF_SECONDS", "0.5"))
HTTPX_TRUST_ENV = os.getenv("CRAWLER_HTTPX_TRUST_ENV", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}
MAX_CONCURRENCY = int(os.getenv("CRAWLER_MAX_CONCURRENCY", "5"))
AJAX_IDLE_WAIT_MS = int(os.getenv("AJAX_IDLE_WAIT_MS", "5000"))
AJAX_IDLE_STABLE_MS = int(os.getenv("AJAX_IDLE_STABLE_MS", "600"))
GNB_READY_TIMEOUT_MS = int(os.getenv("GNB_READY_TIMEOUT_MS", "5000"))
MAX_DEPTGDC_TAB_FETCH = int(os.getenv("DEPTGDC_TAB_MAX_FETCH", "5"))


@dataclass(frozen=True)
class MenuLink:
    label: str
    url: str
    reg_date: Optional[str] = field(default=None)
    children: List["MenuLink"] = field(default_factory=list)


@dataclass(frozen=True)
class MenuGroup:
    menu: str
    links: List[MenuLink]
    region: str = "global"


class CrawlRequest(BaseModel):
    url: AnyHttpUrl = Field(..., description="Target URL")
    debug: bool = Field(False, description="Include debug info")


class BatchCrawlRequest(BaseModel):
    urls: List[AnyHttpUrl] = Field(..., description="Target URL list")
    debug: bool = Field(False, description="Include debug info")


class CrawlError(BaseModel):
    source_url: AnyHttpUrl = Field(..., description="Failed URL")
    detail: str = Field(..., description="Failure detail")


class QueryLink(BaseModel):
    label: str = Field(..., description="Link label")
    url: str = Field(..., description="Link URL")
    reg_date: Optional[str] = Field(None, description="Registration date")


class CrawlResponse(BaseModel):
    source_url: AnyHttpUrl = Field(..., description="Requested URL")
    groups: List[MenuGroup] = Field(..., description="Sitemap groups")
    debug_info: Optional[Dict[str, Any]] = Field(None, description="Debug info")
    query_links: List[QueryLink] = Field(default_factory=list, description="Board candidates")
    board_list_urls: List[str] = Field(default_factory=list, description="Board list URLs")
    board_list_links: List[QueryLink] = Field(default_factory=list, description="Board list links")


class BatchCrawlResponse(BaseModel):
    successes: List[CrawlResponse] = Field(default_factory=list)
    failures: List[CrawlError] = Field(default_factory=list)


def ensure_url_scheme(url: str) -> str:
    if not re.match(r"^[a-zA-Z]+://", url):
        return f"http://{url}"
    return url


def _normalize_label(text: str) -> str:
    return " ".join(text.split())


def _is_supported_scheme(target_url: str) -> bool:
    try:
        parsed = urlparse(target_url)
    except ValueError:
        return False
    if not parsed.scheme:
        return True
    return parsed.scheme in ("http", "https")


def _is_same_origin(target_url: str, base_url: str) -> bool:
    try:
        target = urlparse(target_url)
        base = urlparse(base_url)
    except ValueError:
        return False
    if target.scheme and target.scheme not in ("http", "https"):
        return False
    if not target.netloc:
        return True
    return target.netloc == base.netloc


def _coerce_url(raw: str, base_url: str) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()
    if not raw or raw == "#":
        return None
    if raw.lower().startswith(("javascript:", "mailto:", "tel:", "data:")):
        return None
    absolute = urljoin(base_url, raw)
    if not _is_supported_scheme(absolute):
        return None
    if not _is_same_origin(absolute, base_url):
        return None
    return absolute


_ONCLICK_URL_RE = re.compile(r"['\"](?P<url>https?://[^'\"]+|/[^'\"]+|[^'\"]+\.do[^'\"]*)['\"]")


def _extract_onclick_url(tag: Tag, base_url: str) -> Optional[str]:
    onclick = tag.get("onclick")
    if not onclick:
        return None
    match = _ONCLICK_URL_RE.search(onclick)
    if not match:
        return None
    candidate = match.group("url")
    return _coerce_url(candidate, base_url)


def _extract_link_info(tag: Tag, base_url: str) -> Optional[Tuple[str, str, Optional[str]]]:
    anchor = tag.find("a", href=True)
    if anchor:
        label = _normalize_label(anchor.get_text(" ", strip=True))
        url = _coerce_url(anchor.get("href"), base_url)
    else:
        button = tag.find("button")
        label = _normalize_label(button.get_text(" ", strip=True)) if button else ""
        url = _extract_onclick_url(tag, base_url)
    if not label or not url:
        return None

    reg_date = None
    date_match = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", label)
    if date_match:
        reg_date = f"{date_match.group(1)}-{date_match.group(2).zfill(2)}-{date_match.group(3).zfill(2)}"

    return label, url, reg_date


def _find_child_menu_lists(li: Tag) -> List[Tag]:
    candidates = []
    for selector in [":scope > ul", ":scope > div > ul", ":scope > div > div > ul"]:
        found = li.select_one(selector)
        if found and found.name == "ul":
            candidates.append(found)
    if not candidates:
        found = li.find("ul")
        if found and found.name == "ul":
            candidates.append(found)
    return candidates


def _build_menu_link(li: Tag, base_url: str, depth: int = 1, max_depth: int = 5) -> Optional[MenuLink]:
    if depth > max_depth:
        return None
    info = _extract_link_info(li, base_url)
    if not info:
        return None
    label, url, reg_date = info
    children: List[MenuLink] = []
    for ul in _find_child_menu_lists(li):
        for child_li in ul.select(":scope > li"):
            child = _build_menu_link(child_li, base_url, depth + 1, max_depth)
            if child:
                children.append(child)
    return MenuLink(label=label, url=url, reg_date=reg_date, children=children)


def _select_top_level_lis(container: Tag) -> List[Tag]:
    top_level = container.select(":scope > li")
    if top_level:
        return top_level
    ul_container = container.select_one("ul")
    if ul_container:
        return ul_container.select(":scope > li")
    return container.select("li")


def _collect_containers(soup: BeautifulSoup) -> List[Tag]:
    selectors = [
        "header",
        "nav",
        "#gnb",
        ".gnb",
        ".menu",
        ".nav",
        "ul.menu-depth1",
        "ul.depth1",
    ]
    containers: List[Tag] = []
    for selector in selectors:
        found = soup.select(selector)
        for tag in found:
            if tag not in containers:
                containers.append(tag)
    if not containers and soup.body:
        containers.append(soup.body)
    return containers


def _extract_groups_from_container(container: Tag, base_url: str) -> List[MenuGroup]:
    groups: List[MenuGroup] = []
    for li in _select_top_level_lis(container):
        link = _build_menu_link(li, base_url)
        if not link:
            continue
        group_links = [link] + link.children
        groups.append(MenuGroup(menu=link.label, links=group_links, region="global"))
    return groups


def _fallback_groups_from_anchors(anchors: Iterable[Tag], base_url: str) -> List[MenuGroup]:
    links: List[MenuLink] = []
    seen = set()
    for a in anchors:
        href = a.get("href")
        url = _coerce_url(href, base_url)
        if not url:
            continue
        label = _normalize_label(a.get_text(" ", strip=True)) or url
        key = f"{label}|{url}"
        if key in seen:
            continue
        seen.add(key)
        links.append(MenuLink(label=label, url=url))
    if not links:
        return []
    return [MenuGroup(menu="fallback", links=links, region="global")]


def parse_sitemap(html: str, base_url: str, debug: bool = False) -> Tuple[List[MenuGroup], Optional[Dict[str, Any]]]:
    start = time.perf_counter()
    debug_info: Optional[Dict[str, Any]] = {"base_url": base_url, "start_time": start} if debug else None
    soup = BeautifulSoup(html, "html.parser")
    containers = _collect_containers(soup)
    if debug_info is not None:
        debug_info["containers"] = len(containers)

    for container in containers:
        groups = _extract_groups_from_container(container, base_url)
        if groups:
            if debug_info is not None:
                debug_info["source"] = "container"
                debug_info["groups_found"] = len(groups)
            return groups, debug_info

    anchors = soup.find_all("a", href=True)[:60]
    groups = _fallback_groups_from_anchors(anchors, base_url)
    if groups and debug_info is not None:
        debug_info["source"] = "fallback"
        debug_info["groups_found"] = len(groups)
    return groups, debug_info


def _parse_program_sitemap(
    html: str, base_url: str, debug: bool = False
) -> Tuple[List[MenuGroup], Optional[Dict[str, Any]]]:
    start = time.perf_counter()
    debug_info: Optional[Dict[str, Any]] = {"base_url": base_url, "start_time": start} if debug else None
    soup = BeautifulSoup(html, "html.parser")
    program = soup.select_one("div.program.sitemap")
    if not program:
        return [], debug_info

    tabs = {}
    for li in program.select("div.tab_box > ul > li"):
        tab_id = (li.get("id") or "").strip()
        label = _normalize_label(li.get_text(" ", strip=True))
        if tab_id and label:
            tabs[tab_id] = label

    groups: List[MenuGroup] = []
    tab_roots: Dict[str, Tuple[MenuLink, set]] = {}
    tab_contents = program.select("div.tab_content")
    for tab_content in tab_contents:
        tab_id = None
        for cls in tab_content.get("class", []):
            if isinstance(cls, str) and cls.isdigit():
                tab_id = cls
                break
        tab_label = tabs.get(tab_id or "", "").strip() or "sitemap"
        root_entry = tab_roots.get(tab_label)
        if root_entry:
            root, section_seen = root_entry
        else:
            root = MenuLink(label=tab_label, url=base_url, children=[])
            section_seen = set()
            tab_roots[tab_label] = (root, section_seen)
        for con in tab_content.select("div.sitemap_con"):
            h3 = con.find("h3")
            if not h3:
                continue
            section_label = ""
            section_url = base_url
            section_info = _extract_link_info(h3, base_url)
            if section_info:
                section_label, section_url, _ = section_info
            else:
                section_label = _normalize_label(h3.get_text(" ", strip=True))
            if not section_label:
                continue
            section_key = f"{section_label}|{section_url}"
            if section_key in section_seen:
                continue
            section_seen.add(section_key)
            section_children: List[MenuLink] = []
            for li in con.select("ul.depth2 > li"):
                child = _build_menu_link(li, base_url, depth=1, max_depth=5)
                if child:
                    section_children.append(child)
            root.children.append(MenuLink(label=section_label, url=section_url, children=section_children))
    for tab_label, (root, _) in tab_roots.items():
        if root.children:
            groups.append(MenuGroup(menu=tab_label, links=[root], region="sitemap"))

    if groups and debug_info is not None:
        debug_info["source"] = "program_sitemap"
        debug_info["groups_found"] = len(groups)
    return groups, debug_info


def _count_group_links(groups: List[MenuGroup]) -> int:
    total = 0
    for link in _flatten_group_links(groups):
        if link and getattr(link, "url", None):
            total += 1
    return total


def _is_weak_groups(groups: List[MenuGroup]) -> bool:
    if not groups:
        return True
    total_links = _count_group_links(groups)
    if total_links <= 5:
        return True
    menus = [str(group.menu or "").strip().lower() for group in groups]
    if menus and all(menu in AUTH_MENU_TITLES for menu in menus):
        return True
    unique_menus = {menu for menu in menus if menu}
    if unique_menus and (len(groups) / max(len(unique_menus), 1)) >= 3:
        return True
    return False


def _guess_sitemap_urls(base_url: str) -> List[str]:
    parsed = urlparse(base_url)
    base_origin = f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")
    candidates = [
        "/sitemap.do",
        "/siteMap.do",
        "/sitemap",
        "/sitemap.html",
        "/sitemap.htm",
        "/sitemap.jsp",
        "/sitemap.php",
    ]
    if (parsed.path or "").startswith("/www/"):
        candidates.extend(["/www/sitemap.do", "/www/sitemap"])
    return [urljoin(base_origin + "/", c.lstrip("/")) for c in candidates]


def _discover_sitemap_urls(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = set()
    for tag in soup.find_all(["a", "button"]):
        label = _normalize_label(tag.get_text(" ", strip=True))
        label_lower = label.lower()
        href = (tag.get("href") or "").strip()
        href_lower = href.lower()
        if any(hint in label_lower for hint in SITEMAP_TEXT_HINTS) or any(
            hint.lower() in href_lower for hint in SITEMAP_HREF_HINTS
        ):
            if tag.name == "a" and href:
                url = _coerce_url(href, base_url)
            else:
                url = _extract_onclick_url(tag, base_url)
            if url:
                urls.add(url)
    for guess in _guess_sitemap_urls(base_url):
        urls.add(guess)
    return list(urls)


async def _crawl_sitemap_candidate(
    client: httpx.AsyncClient, sitemap_url: str, debug: bool
) -> Tuple[List[MenuGroup], Optional[Dict[str, Any]]]:
    html, _ = await _fetch_html(client, sitemap_url)
    groups, debug_info = _parse_program_sitemap(html, sitemap_url, debug=debug)
    if not groups:
        groups, debug_info = parse_sitemap(html, sitemap_url, debug=debug)
        if debug_info is not None:
            debug_info["source"] = "sitemap_page"
    if debug_info is not None:
        debug_info["sitemap_url"] = sitemap_url
    return groups, debug_info


def _flatten_group_links(groups: List[MenuGroup]) -> List[MenuLink]:
    flattened: List[MenuLink] = []
    for group in groups:
        for link in group.links:
            flattened.append(link)
            if link.children:
                flattened.extend(_flatten_link_children(link.children))
    return flattened


def _flatten_link_children(children: List[MenuLink]) -> List[MenuLink]:
    flattened: List[MenuLink] = []
    for child in children:
        flattened.append(child)
        if child.children:
            flattened.extend(_flatten_link_children(child.children))
    return flattened


def _normalize_match_url(u: str) -> str:
    """sitemap 매칭용 정규화 (query 정렬/fragment 제거)."""
    try:
        p = urlparse(u)
        pairs = parse_qsl(p.query or "", keep_blank_values=True)
        pairs.sort()
        q = urlencode(pairs, doseq=True)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return urlunparse((scheme, netloc, p.path or "", "", q, ""))
    except Exception:
        return u


def _collect_descendant_urls(link: MenuLink) -> List[str]:
    urls: List[str] = []
    for child in link.children:
        if child.url:
            urls.append(child.url)
        if child.children:
            urls.extend(_collect_descendant_urls(child))
    return urls


def find_sitemap_descendants(groups: List[MenuGroup], target_url: str) -> List[str]:
    """
    sitemap 트리에서 target_url의 하위(자식/손자) URL들을 반환한다.
    - 매칭은 정규화된 URL 기준
    - 순서를 유지하며 중복 제거
    """
    if not groups or not target_url:
        return []
    target_key = _normalize_match_url(target_url)
    found: List[str] = []
    seen: set[str] = set()

    def _walk(link: MenuLink) -> None:
        nonlocal found
        if _normalize_match_url(link.url) == target_key:
            for u in _collect_descendant_urls(link):
                if not u or u in seen:
                    continue
                seen.add(u)
                found.append(u)
            return
        for child in link.children:
            _walk(child)

    for group in groups:
        for link in group.links:
            _walk(link)
    return found


def _render_sitemap_link(link: MenuLink, indent: int = 0) -> List[str]:
    prefix = "  " * indent + "- "
    label = link.label or link.url
    board_marker = " [BOARD]" if _is_board_candidate_url(link.url) else ""
    line = f"{prefix}[{label}]({link.url})"
    if link.reg_date:
        line = f"{line} ({link.reg_date})"
    line = f"{line}{board_marker}"
    lines = [line]
    for child in link.children:
        lines.extend(_render_sitemap_link(child, indent + 1))
    return lines


def _build_sitemap_markdown(groups: List[MenuGroup], base_url: str) -> str:
    lines = [f"# Sitemap for {base_url}", ""]
    if not groups:
        lines.append("_No sitemap groups found._")
        return "\n".join(lines)
    for group in groups:
        lines.append(f"## {group.menu}")
        if group.links:
            for link in group.links:
                lines.extend(_render_sitemap_link(link, 0))
        else:
            lines.append("- (empty)")
        lines.append("")
    candidates = [
        link
        for link in _extract_candidate_query_links(groups)
        if _is_board_candidate_url(link.url)
    ]
    list_links, list_urls = _split_board_list_links(candidates)
    lines.append("## 게시판 후보")
    if candidates:
        for link in candidates:
            label = link.label or link.url
            lines.append(f"- [{label}]({link.url})")
    else:
        lines.append("- (없음)")
    lines.append("")
    _ = list_links, list_urls
    return "\n".join(lines).rstrip() + "\n"


def build_sitemap_markdown(groups: List[MenuGroup], base_url: str) -> str:
    return _build_sitemap_markdown(groups, base_url)


def _get_base_origin(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme or "http"
    netloc = parsed.netloc
    return f"{scheme}://{netloc}".rstrip("/")


def _is_top_domain_url(url: str) -> bool:
    parsed = urlparse(url)
    path = (parsed.path or "").strip()
    return path in ("", "/") and not parsed.query and not parsed.fragment


def _safe_netloc(netloc: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", netloc or "unknown")


def _link_to_dict(link: MenuLink) -> Dict[str, Any]:
    return {
        "label": link.label,
        "url": link.url,
        "reg_date": link.reg_date,
        "is_board": _is_board_candidate_url(link.url),
        "children": [_link_to_dict(child) for child in link.children],
    }


def _group_to_dict(group: MenuGroup) -> Dict[str, Any]:
    return {
        "menu": group.menu,
        "region": group.region,
        "links": [_link_to_dict(link) for link in group.links],
    }


def _link_from_dict(raw: Dict[str, Any]) -> MenuLink:
    children = [_link_from_dict(child) for child in (raw.get("children") or [])]
    return MenuLink(
        label=str(raw.get("label") or ""),
        url=str(raw.get("url") or ""),
        reg_date=raw.get("reg_date"),
        children=children,
    )


def _group_from_dict(raw: Dict[str, Any]) -> MenuGroup:
    links = [_link_from_dict(link) for link in (raw.get("links") or [])]
    return MenuGroup(menu=str(raw.get("menu") or ""), links=links, region=str(raw.get("region") or "global"))


def _sitemap_cache_paths(base_origin: str) -> Tuple[str, str]:
    parsed = urlparse(base_origin)
    safe_netloc = _safe_netloc(parsed.netloc)
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "output", "sitemaps"))
    json_path = os.path.join(base_dir, f"sitemap_{safe_netloc}.json")
    md_path = os.path.join(base_dir, f"sitemap_{safe_netloc}.md")
    return json_path, md_path


def _load_sitemap_cache(base_origin: str) -> Optional[List[MenuGroup]]:
    json_path, _ = _sitemap_cache_paths(base_origin)
    if not os.path.exists(json_path):
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as fp:
            raw = json.load(fp)
        if not isinstance(raw, list):
            return None
        return [_group_from_dict(item) for item in raw]
    except Exception:
        return None


def _store_sitemap_cache(base_origin: str, groups: List[MenuGroup]) -> None:
    json_path, md_path = _sitemap_cache_paths(base_origin)
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    payload = [_group_to_dict(group) for group in groups]
    with open(json_path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)
    with open(md_path, "w", encoding="utf-8") as fp:
        fp.write(_build_sitemap_markdown(groups, base_origin))


def get_sitemap_cache_paths(base_origin: str) -> Tuple[str, str]:
    return _sitemap_cache_paths(base_origin)


def get_base_origin(url: str) -> str:
    return _get_base_origin(url)


def store_sitemap_cache(base_origin: str, groups: List[MenuGroup]) -> None:
    _store_sitemap_cache(base_origin, groups)


def load_sitemap_cache(base_origin: str) -> Optional[List[MenuGroup]]:
    return _load_sitemap_cache(base_origin)


def is_top_domain_url(url: str) -> bool:
    return _is_top_domain_url(url)


def is_board_candidate_url(url: str) -> bool:
    return _is_board_candidate_url(url)


def _is_list_page_url(u: str) -> bool:
    try:
        lu = (u or "").lower()
    except Exception:
        lu = str(u).lower()
    if "list.do" in lu or "list.asp" in lu or "list.jsp" in lu:
        return True
    try:
        path = (urlparse(u).path or "").lower()
        return path.endswith(("list.do", "list.asp", "list.jsp"))
    except Exception:
        return False


def _is_board_candidate_url(u: str) -> bool:
    if not isinstance(u, str) or not u:
        return False
    try:
        url_lower = u.lower()
    except Exception:
        url_lower = str(u).lower()
    if any(pattern in url_lower for pattern in NON_BOARD_PATTERNS):
        return False
    if not any(pattern in url_lower for pattern in BOARD_LIST_PATTERNS):
        return False
    if any(excl in url_lower for excl in BOARD_EXCLUDE_SUBSTRINGS):
        return False
    if any(s.lower() in url_lower for s in SKIP_DEPTH_PATTERNS):
        return False
    return True


def _is_deptgdc_url(u: str) -> bool:
    if not isinstance(u, str) or not u:
        return False
    try:
        return "deptgdc.do" in u.lower()
    except Exception:
        return "deptgdc.do" in str(u).lower()


def _get_query_param(url: str, key: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    qs = parse_qs(parsed.query or "")
    if key in qs and qs[key]:
        return qs[key][0]
    key_lower = key.lower()
    for k, v in qs.items():
        if k.lower() == key_lower and v:
            return v[0]
    return None


def _find_deptgdc_tab_links(html: str, base_url: str, dept_id: Optional[str]) -> List[MenuLink]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    found: List[MenuLink] = []
    seen = set()
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href")
        if not href:
            continue
        href_lower = href.lower()
        if "/bbs/" not in href_lower or "list.do" not in href_lower:
            continue
        abs_url = urljoin(base_url, href)
        if not _is_supported_scheme(abs_url) or not _is_same_origin(abs_url, base_url):
            continue
        if dept_id:
            link_dept_id = _get_query_param(abs_url, "deptId")
            if link_dept_id and link_dept_id != dept_id:
                continue
        if abs_url in seen:
            continue
        label = _normalize_label(anchor.get_text(" ", strip=True))
        if not label:
            label = abs_url
        found.append(MenuLink(label=label, url=abs_url))
        seen.add(abs_url)
    return found


async def _fetch_deptgdc_tab_links(client: httpx.AsyncClient, url: str) -> List[MenuLink]:
    dept_id = _get_query_param(url, "deptId")
    if not dept_id:
        return []
    try:
        html, _ = await _fetch_html(client, url)
    except Exception:
        return []
    return _find_deptgdc_tab_links(html, url, dept_id)


async def _enrich_groups_with_deptgdc_tabs(
    client: httpx.AsyncClient,
    groups: List[MenuGroup],
    debug_info: Optional[Dict[str, Any]] = None,
) -> List[MenuGroup]:
    if not groups or MAX_DEPTGDC_TAB_FETCH <= 0:
        return groups

    targets: List[str] = []
    seen_targets = set()
    for link in _flatten_group_links(groups):
        if _is_deptgdc_url(link.url) and link.url not in seen_targets:
            targets.append(link.url)
            seen_targets.add(link.url)
        if len(targets) >= MAX_DEPTGDC_TAB_FETCH:
            break

    if not targets:
        return groups

    tabs_by_url: Dict[str, List[MenuLink]] = {}
    for target_url in targets:
        tab_links = await _fetch_deptgdc_tab_links(client, target_url)
        if tab_links:
            tabs_by_url[target_url] = tab_links

    if not tabs_by_url:
        return groups

    def _merge_tabs(link: MenuLink) -> MenuLink:
        changed = False
        new_children: List[MenuLink] = []
        for child in link.children:
            merged_child = _merge_tabs(child)
            if merged_child is not child:
                changed = True
            new_children.append(merged_child)

        extra = tabs_by_url.get(link.url)
        if extra:
            existing_urls = {child.url for child in new_children}
            for tab in extra:
                if tab.url not in existing_urls:
                    new_children.append(tab)
            changed = True

        if not changed:
            return link
        return MenuLink(
            label=link.label,
            url=link.url,
            reg_date=link.reg_date,
            children=new_children,
        )

    enriched_groups: List[MenuGroup] = []
    for group in groups:
        enriched_groups.append(
            MenuGroup(
                menu=group.menu,
                region=group.region,
                links=[_merge_tabs(link) for link in group.links],
            )
        )

    if debug_info is not None:
        debug_info["deptgdc_tab_enriched_urls"] = len(tabs_by_url)
        debug_info["deptgdc_tab_enriched_links"] = sum(len(v) for v in tabs_by_url.values())

    return enriched_groups


def _extract_candidate_query_links(groups: List[MenuGroup]) -> List[MenuLink]:
    links = _flatten_group_links(groups)
    candidates: List[MenuLink] = []
    seen = set()

    for link in links:
        if not _is_board_candidate_url(link.url):
            continue
        if link.url in seen:
            continue
        seen.add(link.url)
        candidates.append(link)

    if candidates:
        list_first = [c for c in candidates if _is_list_page_url(c.url)]
        non_list = [c for c in candidates if not _is_list_page_url(c.url)]
        return list_first + non_list
    return candidates


def _split_board_list_links(links: List[MenuLink]) -> Tuple[List[MenuLink], List[str]]:
    list_links = [link for link in links if _is_list_page_url(link.url)]
    list_urls = [link.url for link in list_links]
    return list_links, list_urls


def evaluate_dynamic_retry(groups: List[MenuGroup], debug_info: Optional[Dict[str, Any]]) -> bool:
    if not groups:
        return True
    if any(group.menu in FALLBACK_MENU_TITLES for group in groups):
        return True
    if debug_info and debug_info.get("source") == "fallback":
        return True
    return False


def _wait_for_ajax_idle(page, tracker) -> bool:
    deadline = time.time() + (AJAX_IDLE_WAIT_MS / 1000.0)
    stable_start: Optional[float] = None
    while time.time() < deadline:
        if tracker["count"] == 0:
            if stable_start is None:
                stable_start = time.time()
            elif (time.time() - stable_start) * 1000 >= AJAX_IDLE_STABLE_MS:
                return True
        else:
            stable_start = None
        page.wait_for_timeout(100)
    return False


async def _httpx_get_with_retry(client: httpx.AsyncClient, url: str) -> httpx.Response:
    attempts = max(1, HTTPX_RETRY_COUNT + 1)
    last_exc: Optional[httpx.TimeoutException] = None
    for attempt in range(1, attempts + 1):
        try:
            return await client.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
        except httpx.TimeoutException as exc:
            last_exc = exc
            if attempt >= attempts:
                raise
            backoff = HTTPX_RETRY_BACKOFF_SECONDS * attempt
            await asyncio.sleep(backoff)
    if last_exc:
        raise last_exc
    raise httpx.TimeoutException("HTTPX retry exhausted")


def _fetch_html_via_requests(url: str) -> Tuple[str, int]:
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True)
    resp.raise_for_status()
    return resp.text, resp.status_code


async def _fetch_html(client: httpx.AsyncClient, url: str) -> Tuple[str, int]:
    try:
        response = await _httpx_get_with_retry(client, url)
        response.raise_for_status()
        return response.text, response.status_code
    except (httpx.RequestError, httpx.HTTPStatusError):
        return await asyncio.to_thread(_fetch_html_via_requests, url)


async def _crawl_static(client: httpx.AsyncClient, url: str, debug: bool) -> Tuple[List[MenuGroup], Optional[Dict[str, Any]], List[MenuLink], List[str]]:
    html, _ = await _fetch_html(client, url)
    groups, debug_info = parse_sitemap(html, url, debug=debug)
    candidates = _extract_candidate_query_links(groups)
    list_links, list_urls = _split_board_list_links(candidates)
    if debug_info is not None:
        debug_info["board_candidates"] = len(candidates)
        debug_info["board_list_urls"] = len(list_urls)
    return groups, debug_info, candidates, list_urls


async def _crawl_dynamic(url: str, debug: bool) -> Tuple[List[MenuGroup], Optional[Dict[str, Any]], List[MenuLink], List[str]]:
    debug_info: Optional[Dict[str, Any]] = {"source": "dynamic"} if debug else None
    html_content = ""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=DEFAULT_HEADERS.get("User-Agent"),
            locale="ko-KR",
        )
        page = await context.new_page()
        try:
            ajax_tracker = {"count": 0}

            def _track_ajax_start(request):
                if request.resource_type == "xhr":
                    ajax_tracker["count"] += 1

            def _track_ajax_end(request):
                if request.resource_type == "xhr" and ajax_tracker["count"] > 0:
                    ajax_tracker["count"] -= 1

            page.on("request", _track_ajax_start)
            page.on("requestfinished", _track_ajax_end)
            page.on("requestfailed", _track_ajax_end)

            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            _wait_for_ajax_idle(page, ajax_tracker)
            try:
                await page.wait_for_function(
                    "document.querySelectorAll('nav a, header a, .gnb a').length >= 3",
                    timeout=GNB_READY_TIMEOUT_MS,
                )
            except PlaywrightTimeoutError:
                pass
            html_content = await page.content()
        finally:
            await context.close()
            await browser.close()

    groups, debug_info = parse_sitemap(html_content, url, debug=debug)
    candidates = _extract_candidate_query_links(groups)
    list_links, list_urls = _split_board_list_links(candidates)
    if debug_info is not None:
        debug_info["source"] = "dynamic"
        debug_info["board_candidates"] = len(candidates)
        debug_info["board_list_urls"] = len(list_urls)
    return groups, debug_info, candidates, list_urls


async def _crawl_url(
    client: httpx.AsyncClient,
    url: str,
    debug: bool,
) -> Tuple[List[MenuGroup], Optional[Dict[str, Any]], List[MenuLink], List[str]]:
    base_origin = _get_base_origin(url)
    if not _is_top_domain_url(url):
        cached_groups = _load_sitemap_cache(base_origin)
        if cached_groups and not _is_weak_groups(cached_groups):
            debug_info = {"source": "sitemap_cache", "base_origin": base_origin} if debug else None
            enriched_groups = await _enrich_groups_with_deptgdc_tabs(client, cached_groups, debug_info)
            candidates = _extract_candidate_query_links(enriched_groups)
            _, list_urls = _split_board_list_links(candidates)
            return enriched_groups, debug_info, candidates, list_urls

    groups, debug_info, candidates, list_urls = await _crawl_static(client, url, debug)
    if evaluate_dynamic_retry(groups, debug_info):
        dynamic_groups, dynamic_debug, dynamic_candidates, dynamic_list_urls = await _crawl_dynamic(url, debug)
        if dynamic_groups:
            groups = dynamic_groups
            debug_info = dynamic_debug
            candidates = dynamic_candidates
            list_urls = dynamic_list_urls
            if _is_top_domain_url(url):
                try:
                    _store_sitemap_cache(base_origin, groups)
                except Exception:
                    pass
            groups = await _enrich_groups_with_deptgdc_tabs(client, groups, debug_info)
            candidates = _extract_candidate_query_links(groups)
            _, list_urls = _split_board_list_links(candidates)
            return groups, debug_info, candidates, list_urls
    if not groups:
        raise ValueError("No sitemap groups found")

    # sitemap page fallback for weak header menus
    if _is_weak_groups(groups):
        try:
            html, _ = await _fetch_html(client, url)
            sitemap_urls = _discover_sitemap_urls(html, url)
            best_groups: List[MenuGroup] = []
            best_debug: Optional[Dict[str, Any]] = None
            best_count = 0
            for sitemap_url in sitemap_urls:
                try:
                    sitemap_groups, sitemap_debug = await _crawl_sitemap_candidate(client, sitemap_url, debug)
                except Exception:
                    continue
                if not sitemap_groups:
                    continue
                count = _count_group_links(sitemap_groups)
                if count > best_count:
                    best_groups = sitemap_groups
                    best_debug = sitemap_debug
                    best_count = count
            if best_groups and best_count > _count_group_links(groups):
                groups = best_groups
                debug_info = best_debug
                candidates = _extract_candidate_query_links(groups)
                _, list_urls = _split_board_list_links(candidates)
        except Exception:
            pass
    if _is_top_domain_url(url):
        try:
            _store_sitemap_cache(base_origin, groups)
        except Exception:
            pass
    groups = await _enrich_groups_with_deptgdc_tabs(client, groups, debug_info)
    candidates = _extract_candidate_query_links(groups)
    _, list_urls = _split_board_list_links(candidates)
    return groups, debug_info, candidates, list_urls


def _build_response(
    url: str | AnyHttpUrl,
    groups: List[MenuGroup],
    debug_info: Optional[Dict[str, Any]],
    candidates: List[MenuLink],
    list_urls: List[str],
) -> CrawlResponse:
    query_links = [QueryLink(label=link.label, url=link.url, reg_date=link.reg_date) for link in candidates]
    list_links = [QueryLink(label=link.label, url=link.url, reg_date=link.reg_date) for link in candidates if _is_list_page_url(link.url)]
    # region agent log
    try:
        board_ids = set()
        board_param_ids = set()
        samples: List[str] = []
        label_samples: List[Dict[str, str]] = []
        for link in candidates:
            try:
                bid = _extract_board_id(urlparse(str(link.url)).path or "")
            except Exception:
                bid = None
            try:
                bpid = _extract_board_param(str(link.url))
            except Exception:
                bpid = None
            if bid:
                board_ids.add(bid)
            if bpid:
                board_param_ids.add(bpid)
            if len(samples) < 5:
                samples.append(str(link.url))
            if len(label_samples) < 5:
                try:
                    label_samples.append(
                        {"label": str(getattr(link, "label", "") or "")[:80], "url": str(link.url)}
                    )
                except Exception:
                    pass
        _debug_log(
            location="backend/board_header.py:_build_response",
            message="header candidates board id distribution",
            data={
                "source_url": str(url),
                "candidates_count": len(candidates),
                "list_urls_count": len(list_urls),
                "board_ids": sorted(board_ids)[:10],
                "board_param_ids": sorted(board_param_ids)[:10],
                "sample_urls": samples,
                "label_samples": label_samples,
            },
            hypothesis_id="H1",
        )
    except Exception:
        pass
    # endregion
    return CrawlResponse(
        source_url=url,
        groups=groups,
        debug_info=debug_info,
        query_links=query_links,
        board_list_urls=list_urls,
        board_list_links=list_links,
    )


@router.post("/crawl", response_model=CrawlResponse)
async def crawl_header(payload: CrawlRequest) -> CrawlResponse:
    raw_url_str = str(payload.url)
    url_str = ensure_url_scheme(raw_url_str)

    async with httpx.AsyncClient(
        headers=DEFAULT_HEADERS,
        follow_redirects=True,
        timeout=REQUEST_TIMEOUT,
        trust_env=HTTPX_TRUST_ENV,
    ) as client:
        try:
            groups, debug_info, candidates, list_urls = await _crawl_url(client, url_str, payload.debug)
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc)) from exc
        except httpx.TimeoutException as exc:
            raise HTTPException(status_code=504, detail=f"Timeout: {url_str}") from exc
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Network error: {url_str}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Request failed: {url_str}") from exc

    return _build_response(url_str, groups, debug_info, candidates, list_urls)


@router.post("/crawl/stream")
async def crawl_stream(payload: CrawlRequest) -> StreamingResponse:
    raw_url_str = str(payload.url)
    url_str = ensure_url_scheme(raw_url_str)

    queue: asyncio.Queue[Optional[str]] = asyncio.Queue()

    async def enqueue(event: Dict[str, Any]) -> None:
        await queue.put(json.dumps(jsonable_encoder(event), ensure_ascii=False))

    async def worker() -> None:
        async with httpx.AsyncClient(
            headers=DEFAULT_HEADERS,
            follow_redirects=True,
            timeout=REQUEST_TIMEOUT,
            trust_env=HTTPX_TRUST_ENV,
        ) as client:
            try:
                await enqueue({"event": "progress", "stage": "started", "data": {"url": url_str}})
                groups, debug_info, candidates, list_urls = await _crawl_url(client, url_str, payload.debug)
                response_payload = _build_response(url_str, groups, debug_info, candidates, list_urls)
                await enqueue({"event": "complete", "data": response_payload})
            except Exception as exc:
                await enqueue({"event": "error", "detail": str(exc)})
            finally:
                await queue.put(None)

    worker_task = asyncio.create_task(worker())

    async def event_generator():
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item + "\n"
        finally:
            if not worker_task.done():
                worker_task.cancel()

    return StreamingResponse(event_generator(), media_type="application/json")


@router.post("/crawl/batch", response_model=BatchCrawlResponse)
async def crawl_batch(payload: BatchCrawlRequest) -> BatchCrawlResponse:
    if not payload.urls:
        raise HTTPException(status_code=400, detail="urls is empty")

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    successes: List[CrawlResponse] = []
    failures: List[CrawlError] = []

    async with httpx.AsyncClient(
        headers=DEFAULT_HEADERS,
        follow_redirects=True,
        timeout=REQUEST_TIMEOUT,
        trust_env=HTTPX_TRUST_ENV,
    ) as client:

        async def worker(target_url: AnyHttpUrl) -> None:
            url_str = ensure_url_scheme(str(target_url))
            async with sem:
                try:
                    groups, debug_info, candidates, list_urls = await _crawl_url(client, url_str, payload.debug)
                    successes.append(_build_response(url_str, groups, debug_info, candidates, list_urls))
                except Exception as exc:
                    failures.append(CrawlError(source_url=target_url, detail=str(exc)))

        await asyncio.gather(*(worker(url) for url in payload.urls))

    return BatchCrawlResponse(successes=successes, failures=failures)


@router.post("/crawl/sitemap/md", response_class=PlainTextResponse)
async def crawl_sitemap_markdown(payload: CrawlRequest) -> PlainTextResponse:
    raw_url_str = str(payload.url)
    url_str = ensure_url_scheme(raw_url_str)

    async with httpx.AsyncClient(
        headers=DEFAULT_HEADERS,
        follow_redirects=True,
        timeout=REQUEST_TIMEOUT,
        trust_env=HTTPX_TRUST_ENV,
    ) as client:
        try:
            groups, debug_info, _, _ = await _crawl_url(client, url_str, payload.debug)
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=exc.response.status_code, detail=str(exc)) from exc
        except httpx.TimeoutException as exc:
            raise HTTPException(status_code=504, detail=f"Timeout: {url_str}") from exc
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail=f"Network error: {url_str}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Request failed: {url_str}") from exc

    markdown = _build_sitemap_markdown(groups, url_str)
    if payload.debug and debug_info:
        markdown = markdown + "\n<!-- debug_info: " + json.dumps(debug_info, ensure_ascii=False) + " -->\n"
    _setup_logging()
    logger.info("Sitemap markdown generated | endpoint=/api/crawl/sitemap/md | url=%s", url_str)
    return PlainTextResponse(markdown, media_type="text/markdown")


def _setup_logging() -> None:
    if logger.handlers:
        return
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)


def create_app() -> FastAPI:
    _setup_logging()
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router)
    return app


app = create_app()
