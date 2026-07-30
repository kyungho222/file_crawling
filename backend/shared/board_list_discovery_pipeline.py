from __future__ import annotations

import asyncio
import html as html_lib
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import aiohttp

from backend.shared.content_scope import select_search_root
from backend.shared.detail_page_utils import is_detail_page_url
from db.maria_operations import maria_execute_query

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]


logger = logging.getLogger("backend.shared.board_list_discovery_pipeline")

DETAIL_QUERY_KEYS = {
    "nttid",
    "nttno",
    "q_bbscttsn",
    "q_bbscttSn".lower(),
    "bbscttsn",
    "articleid",
    "articleno",
    "seq",
    "idx",
    "no",
    "ctrtacctbookmngno",
}
PAGE_PARAM_CANDIDATES = ("q_currPage", "pageIndex", "pageNo", "page", "curPage")
DATE_RE = re.compile(r"(20\d{2})[-./](\d{1,2})[-./](\d{1,2})")
HREF_RE = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.IGNORECASE)
URL_ATTR_RE = re.compile(r"""(href|onclick|data-url|data-href)\s*=\s*(['"])(.*?)\2""", re.IGNORECASE | re.DOTALL)
ANCHOR_RE = re.compile(r"""<a\b(?P<attrs>[^>]*)>(?P<body>.*?)</a\s*>""", re.IGNORECASE | re.DOTALL)
DETAIL_PATH_RE = re.compile(
    r"""(?P<url>[^"'()\s<>]*?(?:BD_selectBbs|selectBbsNttView|bbsView|boardView|view|detail)\.do[^"'()<>\s]*)""",
    re.IGNORECASE,
)
JS_QUOTED_RE = re.compile(r"""['"]([^'"]+)['"]""")
BOARD_LIST_ROOT_SELECTORS = (
    "table.bbsList",
    "table[class*='bbsList']",
    "ul.rp-tblstyle1",
    "ul.rowFluid",
    ".gallery2",
    ".tabPhoto",
    ".board_list",
    ".board-list",
    ".bbs_list",
    ".bbs-list",
    ".list_table",
    ".table_style2",
)
INVALID_LIST_TITLE_KEYWORDS = (
    "삭제",
    "삭제된",
    "비공개",
    "없는 게시글",
    "존재하지",
    "오류",
    "에러",
    "error",
    "not found",
)


@dataclass
class ExplorationBoardTarget:
    row_id: Any
    url: str
    chat_bot_id: str = ""
    page_rule: Dict[str, Any] = field(default_factory=dict)
    collect_days: int = 1
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BoardListPost:
    url: str
    title: str = ""
    published_at: Optional[date] = None
    source_page: str = ""


@dataclass
class BoardListExtraction:
    posts: List[BoardListPost] = field(default_factory=list)
    start_urls: List[str] = field(default_factory=list)
    out_of_range_old_count: int = 0
    out_of_range_new_count: int = 0
    old_samples: List[Dict[str, Any]] = field(default_factory=list)
    new_samples: List[Dict[str, Any]] = field(default_factory=list)
    stopped_by_old_date: bool = False


@dataclass
class BoardListDiscoveryResult:
    target: ExplorationBoardTarget
    ok: bool
    pages_visited: int = 0
    posts: List[BoardListPost] = field(default_factory=list)
    start_urls: List[str] = field(default_factory=list)
    stopped_by_old_date: bool = False
    out_of_range_old_count: int = 0
    out_of_range_new_count: int = 0
    old_samples: List[Dict[str, Any]] = field(default_factory=list)
    new_samples: List[Dict[str, Any]] = field(default_factory=list)
    error: str = ""


def _parse_date(value: Any) -> Optional[date]:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value or "")
    match = DATE_RE.search(text)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except Exception:
        return None


def _is_same_host(candidate: str, base_url: str) -> bool:
    try:
        cand_host = urlparse(candidate).netloc.lower()
        base_host = urlparse(base_url).netloc.lower()
        return not cand_host or not base_host or cand_host == base_host
    except Exception:
        return True


def _has_detail_query(url: str) -> bool:
    try:
        query = parse_qs(urlparse(url).query or "", keep_blank_values=False)
    except Exception:
        return False
    return any(key.lower() in DETAIL_QUERY_KEYS and values for key, values in query.items())


def _is_post_url(url: str, base_url: str) -> bool:
    if not url or not _is_same_host(url, base_url):
        return False
    low = url.lower()
    if "selectbbslist.do" in low:
        return False
    if "bd_selectbbs.do" in low and _has_detail_query(url):
        return True
    if _has_detail_query(url):
        return True
    try:
        return is_detail_page_url(url)
    except Exception:
        return any(token in low for token in ("view.do", "detail.do", "read.do"))


def _normalize_candidate(raw: str, base_url: str) -> str:
    text = (raw or "").strip().replace("&amp;", "&")
    if not text or text.startswith(("#", "mailto:", "tel:")):
        return ""
    if text.lower().startswith("javascript:"):
        return ""
    return urljoin(base_url, text)


def _gm_detail_from_numbers(text: str, base_url: str) -> str:
    if "BD_selectBbsList.do" not in base_url:
        return ""
    if "q_bbscttsn" in (text or "").lower():
        return ""
    numbers = re.findall(r"\b\d{3,}\b", text or "")
    if not numbers:
        return ""
    try:
        parsed = urlparse(base_url)
        base_query = parse_qs(parsed.query or "")
        bbs_code = (base_query.get("q_bbsCode") or base_query.get("q_bbscode") or [""])[0]
        if not bbs_code:
            return ""
        q_bbsctt_sn = numbers[-1]
        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path.replace("BD_selectBbsList.do", "BD_selectBbs.do"),
                "",
                urlencode({"q_bbsCode": bbs_code, "q_bbscttSn": q_bbsctt_sn}),
                "",
            )
        )
    except Exception:
        return ""


def _gm_contract_detail_from_opview(text: str, base_url: str) -> str:
    if "gm.go.kr" not in (urlparse(base_url).netloc or "").lower():
        return ""
    try:
        parsed = urlparse(base_url)
    except Exception:
        return ""
    path = parsed.path or ""
    if "/bidContractInfo/contractInfo/" not in path:
        return ""
    match = re.search(r"opView\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", text or "", re.IGNORECASE)
    if not match:
        return ""
    contract_no = match.group(1).strip()
    if not contract_no:
        return ""
    view_path = path
    if view_path.endswith("contractList.do"):
        view_path = view_path[: -len("contractList.do")] + "contractView.do"
    elif view_path.endswith("accountList.do"):
        view_path = view_path[: -len("accountList.do")] + "accountView.do"
    elif view_path.endswith("hdContractList.do"):
        view_path = view_path[: -len("hdContractList.do")] + "hdContractView.do"
    else:
        view_path = view_path.rsplit("/", 1)[0] + "/contractView.do"
    query = parse_qs(parsed.query or "", keep_blank_values=True)
    query["ctrtAcctBookMngNo"] = [contract_no]
    return urlunparse((parsed.scheme, parsed.netloc, view_path, "", urlencode(query, doseq=True), ""))


def _candidate_urls_from_tag(tag: Any, base_url: str) -> List[str]:
    values: List[str] = []
    href = str(tag.get("href") or "").strip() if getattr(tag, "get", None) else ""
    if href:
        values.append(href)
    for attr in ("onclick", "data-url", "data-href"):
        value = str(tag.get(attr) or "").strip() if getattr(tag, "get", None) else ""
        if value:
            values.append(value)

    out: List[str] = []
    for value in values:
        direct = _normalize_candidate(value, base_url)
        if direct:
            out.append(direct)
        for quoted in JS_QUOTED_RE.findall(value):
            cand = _normalize_candidate(quoted, base_url)
            if cand:
                out.append(cand)
        for match in DETAIL_PATH_RE.finditer(value):
            cand = _normalize_candidate(match.group("url"), base_url)
            if cand:
                out.append(cand)
        gm_url = _gm_detail_from_numbers(value, base_url)
        if gm_url:
            out.append(gm_url)
        gm_contract_url = _gm_contract_detail_from_opview(value, base_url)
        if gm_contract_url:
            out.append(gm_contract_url)
    return out


def _candidate_urls_from_static_value(value: str, base_url: str, *, allow_direct: bool = True) -> List[str]:
    out: List[str] = []
    if allow_direct:
        direct = _normalize_candidate(value, base_url)
        if direct:
            out.append(direct)
    for quoted in JS_QUOTED_RE.findall(value or ""):
        cand = _normalize_candidate(quoted, base_url)
        if cand:
            out.append(cand)
    for match in DETAIL_PATH_RE.finditer(value or ""):
        cand = _normalize_candidate(match.group("url"), base_url)
        if cand:
            out.append(cand)
    gm_url = _gm_detail_from_numbers(value, base_url)
    if gm_url:
        out.append(gm_url)
    gm_contract_url = _gm_contract_detail_from_opview(value, base_url)
    if gm_contract_url:
        out.append(gm_contract_url)
    return out


def _clean_static_link_title(raw_html: str) -> str:
    text = re.sub(r"<script\b[^>]*>.*?</script\s*>", " ", raw_html or "", flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b[^>]*>.*?</style\s*>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _is_invalid_list_title(title: str) -> bool:
    normalized = re.sub(r"\s+", "", (title or "").strip().lower())
    if not normalized:
        return False
    return any(re.sub(r"\s+", "", keyword.lower()) in normalized for keyword in INVALID_LIST_TITLE_KEYWORDS)


def _explicit_board_list_roots(soup: Any) -> List[Any]:
    roots: List[Any] = []
    if not soup:
        return roots
    for selector in BOARD_LIST_ROOT_SELECTORS:
        try:
            for node in soup.select(selector):
                if node not in roots:
                    roots.append(node)
        except Exception:
            continue
    return roots


def extract_post_items_from_list_html_fast(html: str, *, page_url: str) -> List[Dict[str, str]]:
    """Extract static post URL candidates with list-page anchor text only."""
    if not html:
        return []
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    def add(candidate: str, title: str = "") -> None:
        url = (candidate or "").strip()
        if not url or url in seen or url == page_url:
            return
        if _is_invalid_list_title(title):
            return
        if not _is_post_url(url, page_url):
            return
        seen.add(url)
        item = {"url": url}
        clean_title = (title or "").strip()
        if clean_title:
            item["title"] = clean_title
        out.append(item)

    if BeautifulSoup:
        try:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
            for root in _explicit_board_list_roots(soup):
                for anchor in root.find_all("a"):
                    title = anchor.get_text(" ", strip=True)
                    for candidate in _candidate_urls_from_tag(anchor, page_url):
                        add(candidate, title)
        except Exception:
            logger.debug("[BoardListDiscovery] explicit fast extraction failed", exc_info=True)
    if out:
        return out

    for anchor in ANCHOR_RE.finditer(html):
        attrs = anchor.group("attrs") or ""
        title = _clean_static_link_title(anchor.group("body") or "")
        for attr_match in URL_ATTR_RE.finditer(attrs):
            attr = str(attr_match.group(1) or "").lower()
            for candidate in _candidate_urls_from_static_value(
                attr_match.group(3),
                page_url,
                allow_direct=attr != "onclick",
            ):
                add(candidate, title)

    if not out:
        for match in URL_ATTR_RE.finditer(html):
            attr = str(match.group(1) or "").lower()
            for candidate in _candidate_urls_from_static_value(
                match.group(3),
                page_url,
                allow_direct=attr != "onclick",
            ):
                add(candidate)

        for match in DETAIL_PATH_RE.finditer(html):
            add(_normalize_candidate(match.group("url"), page_url))

    return out


def extract_post_urls_from_list_html_fast(html: str, *, page_url: str) -> List[str]:
    """Extract only static post URL candidates without DOM/date/title parsing."""
    return [item["url"] for item in extract_post_items_from_list_html_fast(html, page_url=page_url)]


def _row_text_for(tag: Any) -> str:
    for parent_name in ("tr", "li", "div"):
        parent = tag.find_parent(parent_name)
        if parent:
            return parent.get_text(" ", strip=True)
    return tag.get_text(" ", strip=True)


def _board_list_roots(soup: Any, base_url: str) -> List[Any]:
    roots = _explicit_board_list_roots(soup)
    if roots:
        return roots
    return [select_search_root(soup, base_url=base_url)]


def _sample(post_url: str, title: str, published_at: Optional[date]) -> Dict[str, Any]:
    return {"url": post_url, "title": title, "published_at": published_at.isoformat() if published_at else ""}


def extract_posts_from_list_html(
    html: str,
    *,
    page_url: str,
    start_date: Any = None,
    end_date: Any = None,
    require_date_in_range: bool = True,
    old_date_break_count: int = 3,
) -> BoardListExtraction:
    extraction = BoardListExtraction()
    if not html or not BeautifulSoup:
        return extraction

    start = _parse_date(start_date)
    end = _parse_date(end_date)
    soup = BeautifulSoup(html, "html.parser")  # type: ignore[operator]
    roots = _board_list_roots(soup, page_url)
    seen: set[str] = set()
    post_by_url: Dict[str, BoardListPost] = {}

    def consider(tag: Any, url: str, title: str, row_text: str) -> None:
        if not _is_post_url(url, page_url):
            return
        title = re.sub(r"\s+", " ", str(title or "").strip())
        if url in seen:
            existing = post_by_url.get(url)
            if existing is not None and title and not str(existing.title or "").strip():
                existing.title = title
                for sample in extraction.old_samples + extraction.new_samples:
                    if sample.get("url") == url and not sample.get("title"):
                        sample["title"] = title
            return
        published_at = _parse_date(row_text)
        if require_date_in_range and published_at and start and published_at < start:
            extraction.out_of_range_old_count += 1
            if len(extraction.old_samples) < 5:
                extraction.old_samples.append(_sample(url, title, published_at))
            if extraction.out_of_range_old_count >= max(1, int(old_date_break_count or 3)):
                extraction.stopped_by_old_date = True
            return
        if require_date_in_range and published_at and end and published_at > end:
            extraction.out_of_range_new_count += 1
            if len(extraction.new_samples) < 5:
                extraction.new_samples.append(_sample(url, title, published_at))
            return
        seen.add(url)
        post = BoardListPost(url=url, title=title, published_at=published_at, source_page=page_url)
        post_by_url[url] = post
        extraction.posts.append(post)
        extraction.start_urls.append(url)
    for root in roots:
        for anchor in root.find_all("a"):
            row_text = _row_text_for(anchor)
            title = anchor.get_text(" ", strip=True)
            for candidate in _candidate_urls_from_tag(anchor, page_url):
                consider(anchor, candidate, title, row_text)

    if not extraction.posts:
        for match in DETAIL_PATH_RE.finditer(html):
            candidate = _normalize_candidate(match.group("url"), page_url)
            consider(soup, candidate, "", "")

    return extraction


async def _fetch_html(
    url: str,
    *,
    timeout_sec: float = 20.0,
    session: Optional[aiohttp.ClientSession] = None,
    html_cache: Optional[Dict[str, str]] = None,
) -> str:
    cache_key = url.strip()
    if html_cache is not None and cache_key in html_cache:
        return html_cache[cache_key]
    headers = {
        "User-Agent": "Mozilla/5.0 board-gap-dashboard/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        env_timeout = float(os.getenv("BOARD_LIST_DISCOVERY_FETCH_TIMEOUT_SEC", "") or 0)
    except Exception:
        env_timeout = 0.0
    total_timeout = max(3.0, env_timeout or float(timeout_sec or 20.0))
    timeout = aiohttp.ClientTimeout(total=total_timeout, connect=min(10.0, total_timeout), sock_read=total_timeout)
    owns_session = session is None
    active_session = session
    try:
        if active_session is None:
            active_session = aiohttp.ClientSession(timeout=timeout, headers=headers)
        async with active_session.get(url) as response:
                if response.status >= 400:
                    logger.warning("[BoardListDiscovery] fetch failed | status=%s url=%s", response.status, url)
                    if html_cache is not None:
                        html_cache[cache_key] = ""
                    return ""
                html = await response.text(errors="ignore")
                if html_cache is not None:
                    html_cache[cache_key] = html
                return html
    except asyncio.TimeoutError:
        logger.warning("[BoardListDiscovery] fetch timeout | timeout_sec=%.1f url=%s", total_timeout, url)
        if html_cache is not None:
            html_cache[cache_key] = ""
        return ""
    except aiohttp.ClientError as exc:
        logger.warning("[BoardListDiscovery] fetch client error | error=%s url=%s", exc, url)
        if html_cache is not None:
            html_cache[cache_key] = ""
        return ""
    finally:
        if owns_session and active_session is not None:
            await active_session.close()


def _page_url(url: str, page_no: int, page_param: str = "pageIndex") -> str:
    if page_no <= 1:
        return url
    parsed = urlparse(url)
    pairs = parse_qs(parsed.query or "", keep_blank_values=True)
    pairs[str(page_param or "pageIndex")] = [str(page_no)]
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(pairs, doseq=True), parsed.fragment))


def _guess_page_param(base_url: str, soup: Any, page_rule: Optional[Dict[str, Any]] = None) -> str:
    if page_rule:
        value = page_rule.get("page_param") or page_rule.get("pageParam")
        if value:
            return str(value)
    query = parse_qs(urlparse(base_url).query or "")
    for key in PAGE_PARAM_CANDIDATES:
        if key in query:
            return key
    text = str(soup or "")
    lowered = text.lower()
    for key in PAGE_PARAM_CANDIDATES:
        if key.lower() in lowered:
            return key
    return "pageIndex"


def flatten_start_urls(results: List[BoardListDiscoveryResult]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for result in results or []:
        for url in result.start_urls:
            if url and url not in seen:
                seen.add(url)
                out.append(url)
    return out


async def load_board_targets_from_exploration(
    *,
    db_name: str,
    chat_bot_id: Optional[str] = None,
    target_date: Any = None,
    source_urls: Optional[List[str]] = None,
) -> List[ExplorationBoardTarget]:
    if source_urls:
        return [
            ExplorationBoardTarget(row_id=None, url=url, chat_bot_id=chat_bot_id or "", raw={"url": url})
            for url in source_urls
            if url
        ]
    start_date = None
    end_date = None
    if isinstance(target_date, (list, tuple)) and len(target_date) >= 2:
        start_date = _parse_date(target_date[0])
        end_date = _parse_date(target_date[1])
    else:
        start_date = _parse_date(target_date)
        end_date = start_date

    where = ["LOWER(TRIM(CAST(COALESCE(`type`, '') AS CHAR))) = 'board'"]
    params: List[Any] = []
    if chat_bot_id:
        where.append("(`chat_bot_id` = %s OR `chat_bot_id` IS NULL OR `chat_bot_id` = '')")
        params.append(chat_bot_id)
    where.append("(COALESCE(`is_active`, 1) = 1)")
    where.append("(COALESCE(LOWER(`merge_status`), '') <> 'duplicate')")
    where.append("(COALESCE(LOWER(TRIM(CAST(study_status AS CHAR))), '') <> 'delete')")
    query = (
        "SELECT `id`, `learn_list_id`, `url`, `chat_bot_id`, `subject`, `memo`, `memo1`, "
        "`type`, `is_active`, `merge_status` "
        "FROM `ASADAL_CRAWLING_EXPLORATION` "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY `id` DESC LIMIT 5000"
    )
    try:
        rows = await maria_execute_query(query, tuple(params), fetch=True, dbname=db_name)
    except Exception:
        logger.exception("[BoardListDiscovery] DB target loading failed | db=%s chat_bot_id=%s", db_name, chat_bot_id)
        return []

    targets: List[ExplorationBoardTarget] = []
    seen: set[str] = set()
    for row in rows or []:
        url = str((row or {}).get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        targets.append(
            ExplorationBoardTarget(
                row_id=(row or {}).get("id"),
                url=url,
                chat_bot_id=str((row or {}).get("chat_bot_id") or chat_bot_id or ""),
                page_rule={},
                start_date=start_date,
                end_date=end_date,
                raw=dict(row or {}),
            )
        )
    return targets


async def run_board_list_discovery_pipeline(
    *,
    db_name: str,
    chat_bot_id: Optional[str] = None,
    target_date: Any = None,
    old_date_break_count: int = 3,
    update_success: bool = False,
    source_urls: Optional[List[str]] = None,
    max_pages: int = 1,
    require_date_in_range: bool = True,
) -> List[BoardListDiscoveryResult]:
    targets = await load_board_targets_from_exploration(
        db_name=db_name,
        chat_bot_id=chat_bot_id,
        target_date=target_date,
        source_urls=source_urls,
    )
    results: List[BoardListDiscoveryResult] = []
    headers = {
        "User-Agent": "Mozilla/5.0 board-gap-dashboard/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    html_cache: Dict[str, str] = {}
    try:
        env_timeout = float(os.getenv("BOARD_LIST_DISCOVERY_FETCH_TIMEOUT_SEC", "") or 0)
    except Exception:
        env_timeout = 0.0
    total_timeout = max(3.0, env_timeout or 20.0)
    timeout = aiohttp.ClientTimeout(total=total_timeout, connect=min(10.0, total_timeout), sock_read=total_timeout)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        for target in targets:
            posts: List[BoardListPost] = []
            pages_visited = 0
            old_count = 0
            new_count = 0
            old_samples: List[Dict[str, Any]] = []
            new_samples: List[Dict[str, Any]] = []
            stopped = False
            page_param = str(target.page_rule.get("page_param") or target.page_rule.get("pageParam") or "pageIndex")
            try:
                for page_no in range(1, max(1, int(max_pages or 1)) + 1):
                    current_url = target.url if page_no == 1 else _page_url(target.url, page_no, page_param)
                    html = await _fetch_html(current_url, session=session, html_cache=html_cache)
                    if not html:
                        break
                    pages_visited += 1
                    if page_no == 1 and BeautifulSoup:
                        page_param = _guess_page_param(target.url, BeautifulSoup(html, "html.parser"), target.page_rule)  # type: ignore[operator]
                    extraction = extract_posts_from_list_html(
                        html,
                        page_url=current_url,
                        start_date=target.start_date,
                        end_date=target.end_date,
                        require_date_in_range=require_date_in_range,
                        old_date_break_count=old_date_break_count,
                    )
                    posts.extend(extraction.posts)
                    old_count += extraction.out_of_range_old_count
                    new_count += extraction.out_of_range_new_count
                    old_samples.extend(extraction.old_samples[: max(0, 5 - len(old_samples))])
                    new_samples.extend(extraction.new_samples[: max(0, 5 - len(new_samples))])
                    stopped = extraction.stopped_by_old_date
                    if stopped or not extraction.posts:
                        break
                urls = [post.url for post in posts]
                results.append(
                    BoardListDiscoveryResult(
                        target=target,
                        ok=pages_visited > 0,
                        pages_visited=pages_visited,
                        posts=posts,
                        start_urls=urls,
                        stopped_by_old_date=stopped,
                        out_of_range_old_count=old_count,
                        out_of_range_new_count=new_count,
                        old_samples=old_samples,
                        new_samples=new_samples,
                        error="" if pages_visited else "no list page fetched",
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("[BoardListDiscovery] target failed | url=%s", target.url)
                results.append(BoardListDiscoveryResult(target=target, ok=False, error=str(exc)))
    return results



