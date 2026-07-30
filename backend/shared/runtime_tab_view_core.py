from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Iterable, List, Optional, Sequence
from urllib.parse import parse_qs, urljoin, urlparse, parse_qsl, urlencode, urlunparse

from utils.url import ensure_url_scheme
from backend.shared.board_header import get_base_origin, load_sitemap_cache

try:
    import requests  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    requests = None  # type: ignore[assignment]

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
except Exception:  # pragma: no cover
    async_playwright = None  # type: ignore[assignment]
    PlaywrightTimeoutError = Exception  # type: ignore[assignment]

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore[assignment]

logger = logging.getLogger("runtime_tab_view.core")


class RuntimeTabView:
    """
    Encapsulates the stateful behavior of runtime_tab_view (caches, regex cache, etc.)
    Provides the same high-level async APIs: resolve_start_urls_to_list_pages,
    extract_views_from_list_pages, resolve_runtime_start_urls.
    """

    _VIEW_HINT_RE = re.compile(r"(view|detail|read)\.do", re.IGNORECASE)
    _LIST_HINT_RE = re.compile(r"list\.(do|asp|jsp)", re.IGNORECASE)

    _NAV_CONTAINER_TAGS = ("nav", "header", "footer", "aside")
    _NAV_CONTAINER_HINTS = (
        "menu",
        "gnb",
        "lnb",
        "snb",
        "sidebar",
        "side",
        "sidemenu",
        "leftmenu",
        "rightmenu",
        "nav",
        "header",
        "footer",
        "topmenu",
        "quick",
    )

    def __init__(self) -> None:
        # caches previously module-level globals
        self._list_view_cache: dict[str, tuple[float, List[str]]] = {}
        self._list_view_empty_cache: dict[str, float] = {}
        self._list_file_cache: dict[str, tuple[float, List[str]]] = {}
        self._list_file_empty_cache: dict[str, float] = {}
        self._skip_list_regex_cache: Optional[re.Pattern[str]] = None

    # --- utilities ---
    def _debug_log(self, *, location: str, message: str, data: dict, hypothesis_id: str) -> None:
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
            logging.getLogger("backend.shared.runtime_tab_view").debug(json.dumps(payload, ensure_ascii=False))
        except Exception:
            pass

    def _env_float(self, name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)) or default)
        except Exception:
            return default

    def _env_int(self, name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)) or default)
        except Exception:
            return default

    def _skip_list_regex(self) -> Optional[re.Pattern[str]]:
        if self._skip_list_regex_cache is not None:
            return self._skip_list_regex_cache
        raw = (os.getenv("RUNTIME_TAB_VIEW_SKIP_LIST_REGEX", "") or "").strip()
        if raw.lower() in {"0", "false", "off", "no"}:
            return None
        if not raw:
            raw = r"eventlist\.do"
        try:
            self._skip_list_regex_cache = re.compile(raw, re.IGNORECASE)
        except re.error:
            self._skip_list_regex_cache = None
        return self._skip_list_regex_cache

    def _should_skip_list_url(self, url: str) -> bool:
        if not url:
            return False
        rx = self._skip_list_regex()
        return bool(rx and rx.search(url))

    def _normalize_host(self, host: str) -> str:
        host = (host or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host

    def _is_list_page_url(self, u: str) -> bool:
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

    def _is_view_page_url(self, u: str) -> bool:
        try:
            lu = (u or "").lower()
        except Exception:
            lu = str(u).lower()
        # URL만으로는 판단하지 않고, 구조 검증을 우선한다.
        # 여기서는 "view 힌트"만 가볍게 표시하고, 최종 판단은 HTML 구조로 한다.
        return bool(self._VIEW_HINT_RE.search(lu))

    def _is_deptgdc_url(self, u: str) -> bool:
        if not isinstance(u, str) or not u:
            return False
        try:
            return "deptgdc.do" in u.lower()
        except Exception:
            return "deptgdc.do" in str(u).lower()

    def _get_query_param(self, url: str, key: str) -> Optional[str]:
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

    def _extract_board_id(self, path: str) -> Optional[str]:
        try:
            m = re.search(r"/bbs/([^/]+)/", path or "", re.IGNORECASE)
        except Exception:
            m = None
        if not m:
            return None
        return m.group(1)

    def _extract_menu_no(self, url: str) -> Optional[str]:
        for key in (
            "menuNo",
            "mid",
            "menuno",
            "menu_no",
            "menu",
            "menu_cd",
            "ctgryCd",
            "ctgry_cd",
            "ctgrycd",
            "categoryCd",
            "category_cd",
        ):
            val = self._get_query_param(url, key)
            if val:
                return val
        return None

    def _extract_board_param(self, url: str) -> Optional[str]:
        for key in ("bbsId", "bbs_id", "bbsCd", "bbs_cd", "boardId", "board_id"):
            val = self._get_query_param(url, key)
            if val:
                return val
        return None

    def _is_same_board_scope(self, list_url: str, candidate_url: str) -> bool:
        try:
            base = urlparse(list_url)
            cand = urlparse(candidate_url)
        except Exception:
            return False
        if self._normalize_host(base.netloc) != self._normalize_host(cand.netloc):
            return False
        try:
            if (base.path or "").strip() in ("", "/") and not base.query and not base.fragment:
                return True
        except Exception:
            pass
        base_board = self._extract_board_id(base.path or "") or None
        base_menu = self._extract_menu_no(list_url) or None
        cand_board_param = self._extract_board_param(candidate_url) or None
        cand_board = self._extract_board_id(cand.path or "") or None
        cand_menu = self._extract_menu_no(candidate_url) or None

        logger.debug(
            "[RuntimeTab][BoardScope] base=%s base_board=%s base_menu=%s candidate=%s cand_board_param=%s cand_board=%s cand_menu=%s",
            list_url,
            base_board,
            base_menu,
            candidate_url,
            cand_board_param,
            cand_board,
            cand_menu,
        )

        if base_board:
            if cand_board_param:
                res = cand_board_param.lower() == base_board.lower()
                logger.debug("[RuntimeTab][BoardScope] compare cand_board_param %s == %s -> %s", cand_board_param, base_board, res)
                return res
            if cand_board:
                res = cand_board.lower() == base_board.lower()
                logger.debug("[RuntimeTab][BoardScope] compare cand_board %s == %s -> %s", cand_board, base_board, res)
                return res
            logger.debug("[RuntimeTab][BoardScope] base has board_id but candidate doesn't -> deny")
            return False

        if base_menu and cand_menu:
            res = base_menu == cand_menu
            logger.debug("[RuntimeTab][BoardScope] compare menu %s == %s -> %s", base_menu, cand_menu, res)
            return res

        logger.debug("[RuntimeTab][BoardScope] no board_id and no menuNo -> allow")
        return True

    def _dedupe_keep_order(self, items: Iterable[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for item in items:
            if not item or item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out

    def _looks_like_detail_html(self, html: str) -> bool:
        if not html:
            return False
        text = ""
        if BeautifulSoup:
            try:
                soup = BeautifulSoup(html, "html.parser")
                text = soup.get_text(" ", strip=True)
            except Exception:
                text = html
        else:
            text = html
        if not text:
            return False
        # 등록일/작성일/게시일 등 라벨 + 날짜 패턴 동시 존재 여부로 상세 페이지 추정
        label_re = re.compile(r"(등록일|작성일|게시일|작성\s*일|등록\s*일|게시\s*일|작성\s*날짜|등록\s*날짜|일자|날짜)")
        date_re = re.compile(r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2})")
        if label_re.search(text) and date_re.search(text):
            return True
        # 보조 힌트: 본문/조회수 등 상세 페이지에 흔한 요소
        if re.search(r"(조회수|조회\s*수|본문|내용|첨부파일|첨부\s*파일)", text):
            if date_re.search(text):
                return True
        return False

    async def _confirm_detail_by_structure(self, url: str, *, allow_playwright: bool, session: Any = None, browser: Any = None) -> bool:
        try:
            html = await self._fetch_static_html(url, session=session)
        except Exception:
            html = None
        if not html and allow_playwright:
            try:
                html = await self._fetch_dynamic_html(url, browser=browser)
            except Exception:
                html = None
        return bool(html and self._looks_like_detail_html(html))

    def _normalize_list_cache_key(self, u: str) -> str:
        try:
            p = urlparse(u)
            pairs = parse_qsl(p.query or "", keep_blank_values=True)
            filtered = [
                (k, v)
                for (k, v) in pairs
                if k.lower() not in ("pageindex", "pageno", "page", "curpage", "page_no", "page_index")
            ]
            filtered.sort()
            q = urlencode(filtered, doseq=True)
            scheme = (p.scheme or "https").lower()
            netloc = (p.netloc or "").lower()
            if netloc.startswith("www."):
                netloc = netloc[4:]
            return urlunparse((scheme, netloc, p.path or "", "", q, ""))
        except Exception:
            return u

    def _is_top_domain_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            path = (parsed.path or "").strip()
            return path in ("", "/") and not parsed.query and not parsed.fragment
        except Exception:
            return False

    # --- BeautifulSoup based extractors ---
    def _is_nav_or_sidebar_anchor(self, a) -> bool:
        try:
            for parent in a.parents:
                name = (getattr(parent, "name", "") or "").lower()
                if name in self._NAV_CONTAINER_TAGS:
                    return True
                try:
                    pid = parent.get("id") or ""
                except Exception:
                    pid = ""
                try:
                    classes = parent.get("class") or []
                except Exception:
                    classes = []
                if isinstance(classes, str):
                    classes = [classes]
                for token in [pid, *classes]:
                    if not token:
                        continue
                    lt = str(token).lower()
                    if any(h in lt for h in self._NAV_CONTAINER_HINTS):
                        return True
        except Exception:
            return False
        return False

    def _flatten_sitemap_links(self, links: Iterable[Any]) -> List[str]:
        urls: List[str] = []
        for link in links:
            try:
                url = getattr(link, "url", None) or ""
            except Exception:
                url = ""
            if url:
                urls.append(str(url))
            try:
                children = getattr(link, "children", None) or []
            except Exception:
                children = []
            if children:
                urls.extend(self._flatten_sitemap_links(children))
        return urls

    def _load_sitemap_list_urls(self, base_url: str) -> List[str]:
        try:
            base_origin = get_base_origin(base_url)
        except Exception:
            return []
        groups = load_sitemap_cache(base_origin)
        if not groups:
            return []
        urls: List[str] = []
        for group in groups:
            try:
                group_links = getattr(group, "links", None) or []
            except Exception:
                group_links = []
            urls.extend(self._flatten_sitemap_links(group_links))
        list_urls = [u for u in urls if self._is_list_page_url(u)]
        uniq: List[str] = []
        seen = set()
        for u in list_urls:
            key = self._normalize_list_cache_key(u)
            if key in seen:
                continue
            seen.add(key)
            uniq.append(u)
        return uniq

    def _extract_pagination_links_from_html(self, html: str, base_url: str) -> List[str]:
        if not html or not BeautifulSoup:
            return []
        try:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[arg-type]
        except Exception:
            return []
        try:
            base_board_id = self._extract_board_id(urlparse(base_url).path or "")
        except Exception:
            base_board_id = None
        cache_key = self._normalize_list_cache_key(base_url)
        pagination_params = ("pageindex", "pageno", "page", "curpage", "page_no", "page_index")
        found: List[str] = []
        for a in soup.find_all("a", href=True):
            if self._is_nav_or_sidebar_anchor(a):
                continue
            href = (a.get("href") or "").strip()
            if not href or href.startswith("#"):
                continue
            href_lower = href.lower()
            if href_lower.startswith("javascript:"):
                continue
            is_list_page = self._LIST_HINT_RE.search(href_lower) or ("list.do" in href_lower or "list.asp" in href_lower or "list.jsp" in href_lower)
            has_pagination_param = any(param in href_lower for param in pagination_params)
            if not (is_list_page and has_pagination_param):
                continue
            try:
                full = urljoin(base_url, href)
            except Exception:
                continue
            if self._normalize_list_cache_key(full) != cache_key:
                continue
            if base_board_id:
                try:
                    link_board_id = self._extract_board_id(urlparse(full).path or "")
                except Exception:
                    link_board_id = None
                if link_board_id and link_board_id.lower() != base_board_id.lower():
                    continue
            if not self._is_same_board_scope(base_url, full):
                continue
            found.append(full)
        return self._dedupe_keep_order(found)

    def _extract_view_links_from_html(self, html: str, base_url: str) -> List[str]:
        if not html or not BeautifulSoup:
            return []
        try:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[arg-type]
        except Exception:
            return []
        try:
            base_board_id = self._extract_board_id(urlparse(base_url).path or "")
        except Exception:
            base_board_id = None
        found: List[str] = []
        for a in soup.find_all("a", href=True):
            if self._is_nav_or_sidebar_anchor(a):
                continue
            href = (a.get("href") or "").strip()
            if not href or href.startswith("#"):
                continue
            if href.lower().startswith("javascript:"):
                continue
            if self._VIEW_HINT_RE.search(href):
                full = urljoin(base_url, href)
                if base_board_id:
                    try:
                        link_board_id = self._extract_board_id(urlparse(full).path or "")
                    except Exception:
                        link_board_id = None
                    if link_board_id and link_board_id.lower() != base_board_id.lower():
                        continue
                if not self._is_same_board_scope(base_url, full):
                    continue
                found.append(full)
        return self._dedupe_keep_order(found)

    def _extract_file_links_from_html(self, html: str, base_url: str) -> List[str]:
        if not html or not BeautifulSoup:
            return []
        try:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[arg-type]
        except Exception:
            return []
        try:
            base_board_id = self._extract_board_id(urlparse(base_url).path or "")
        except Exception:
            base_board_id = None
        file_exts = (
            ".pdf", ".hwp", ".hwpx", ".doc", ".docx", ".xls", ".xlsx",
            ".ppt", ".pptx", ".zip", ".rar", ".7z", ".txt", ".csv",
            ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tif", ".tiff",
            ".mp4", ".mp3", ".avi", ".mov", ".wmv",
        )
        url_hints = ("filedown", "download", "file", "attach", "attachment", "atchfile", "atchfileid", "fileid")
        found: List[str] = []
        seen_urls: set[str] = set()

        def _push(full: str) -> None:
            if not full or full in seen_urls:
                return
            if base_board_id:
                try:
                    link_board_id = self._extract_board_id(urlparse(full).path or "")
                except Exception:
                    link_board_id = None
                if link_board_id and link_board_id.lower() != base_board_id.lower():
                    return
            if not self._is_same_board_scope(base_url, full):
                return
            seen_urls.add(full)
            found.append(full)

        try:
            from backend.file.file_meta_extractor import extract_file_download_links
        except Exception:
            extract_file_download_links = None  # type: ignore[assignment]

        if extract_file_download_links:
            try:
                for item in extract_file_download_links(html, base_url):
                    full = str((item or {}).get("url") or "").strip()
                    _push(full)
            except Exception:
                pass

        for a in soup.find_all("a", href=True):
            if self._is_nav_or_sidebar_anchor(a):
                continue
            href = (a.get("href") or a.get("data-href") or a.get("data-url") or "").strip()
            if not href or href.startswith("#"):
                continue
            href_lower = href.lower()
            if href_lower.startswith("javascript:"):
                continue
            looks_like_file = (
                any(ext in href_lower for ext in file_exts) or
                any(hint in href_lower for hint in url_hints)
            )
            if not looks_like_file:
                continue
            try:
                full = urljoin(base_url, href)
            except Exception:
                full = href
            _push(full)
        return found

    def _extract_deptgdc_tab_list_links(self, html: str, base_url: str, dept_id: Optional[str]) -> List[str]:
        if not html or not BeautifulSoup:
            return []
        try:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[arg-type]
        except Exception:
            return []
        found: List[str] = []
        for anchor in soup.find_all("a", href=True):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            href_lower = href.lower()
            if "/bbs/" not in href_lower or not self._LIST_HINT_RE.search(href_lower):
                continue
            full = urljoin(base_url, href)
            if dept_id:
                link_dept_id = self._get_query_param(full, "deptId")
                if link_dept_id and link_dept_id != dept_id:
                    continue
            found.append(full)
        return self._dedupe_keep_order(found)

    def _extract_generic_tab_list_links(self, html: str, base_url: str) -> List[str]:
        if not html or not BeautifulSoup:
            return []
        try:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[arg-type]
        except Exception:
            return []
        found: List[str] = []
        skipped_nav = 0
        skipped_scope = 0
        for anchor in soup.find_all("a", href=True):
            href = (anchor.get("href") or "").strip()
            if not href:
                continue
            href_lower = href.lower()
            if "/bbs/" not in href_lower or not self._LIST_HINT_RE.search(href_lower):
                continue
            if self._is_nav_or_sidebar_anchor(anchor):
                skipped_nav += 1
                continue
            full = urljoin(base_url, href)
            if not self._is_same_board_scope(base_url, full):
                skipped_scope += 1
                continue
            found.append(full)
        deduped = self._dedupe_keep_order(found)
        try:
            board_ids = set()
            board_param_ids = set()
            samples = []
            context_samples = []
            for u in deduped:
                try:
                    b = self._extract_board_id(urlparse(u).path or "")
                except Exception:
                    b = None
                try:
                    bp = self._extract_board_param(u)
                except Exception:
                    bp = None
                if b:
                    board_ids.add(b)
                if bp:
                    board_param_ids.add(bp)
                if len(samples) < 5:
                    samples.append(u)
            try:
                for anchor in soup.find_all("a", href=True):
                    href = (anchor.get("href") or "").strip()
                    if not href:
                        continue
                    href_lower = href.lower()
                    if "/bbs/" not in href_lower or not self._LIST_HINT_RE.search(href_lower):
                        continue
                    if len(context_samples) >= 5:
                        break
                    text = (anchor.get_text(" ", strip=True) or "")[:80]
                    parents = []
                    for p in list(anchor.parents)[:3]:
                        try:
                            pid = p.get("id") or ""
                        except Exception:
                            pid = ""
                        try:
                            classes = p.get("class") or []
                        except Exception:
                            classes = []
                        if isinstance(classes, str):
                            classes = [classes]
                        parents.append(
                            {
                                "tag": getattr(p, "name", "") or "",
                                "id": str(pid)[:80],
                                "class": [str(c)[:50] for c in classes][:4],
                            }
                        )
                    context_samples.append(
                        {
                            "url": urljoin(base_url, href),
                            "text": text,
                            "parents": parents,
                        }
                    )
            except Exception:
                pass
            self._debug_log(
                location="backend/runtime_tab_view_core.py:_extract_generic_tab_list_links",
                message="generic tab list links extracted",
                data={
                    "base_url": base_url,
                    "found_count": len(deduped),
                    "skipped_nav": skipped_nav,
                    "skipped_scope": skipped_scope,
                    "board_ids": sorted(board_ids)[:10],
                    "board_param_ids": sorted(board_param_ids)[:10],
                    "samples": samples,
                    "context_samples": context_samples,
                },
                hypothesis_id="H2",
            )
        except Exception:
            pass
        return deduped

    # --- fetching helpers ---
    async def _fetch_static_html(self, url: str, session: Any = None) -> Optional[str]:
        if session and aiohttp:
            try:
                timeout_sec = float(os.getenv("RUNTIME_TAB_VIEW_TIMEOUT_SEC", "4") or "4")
                async with session.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=timeout_sec,
                    allow_redirects=True,
                ) as resp:
                    if resp.status >= 400:
                        return None
                    return await resp.text()
            except Exception:
                return None

        if not requests:
            return None
        try:
            timeout_sec = float(os.getenv("RUNTIME_TAB_VIEW_TIMEOUT_SEC", "4") or "4")
        except Exception:
            timeout_sec = 4.0

        def _req() -> str:
            r = requests.get(
                url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=timeout_sec,
                allow_redirects=True,
            )
            r.raise_for_status()
            return r.text

        try:
            return await asyncio.to_thread(_req)
        except Exception:
            return None

    async def _fetch_dynamic_html(self, url: str, browser: Any = None) -> Optional[str]:
        if not async_playwright and not browser:
            return None
        try:
            timeout_sec = float(os.getenv("RUNTIME_TAB_VIEW_PLAYWRIGHT_TIMEOUT_SEC", "8") or "8")
        except Exception:
            timeout_sec = 8.0
        timeout_ms = max(3000, int(timeout_sec * 1000))
        html_content = ""

        if browser:
            try:
                context = await browser.new_context(
                    ignore_https_errors=True,
                    user_agent="Mozilla/5.0",
                    locale="ko-KR",
                )
                page = await context.new_page()
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=timeout_ms)
                    except PlaywrightTimeoutError:
                        pass
                    html_content = await page.content()
                finally:
                    await context.close()
            except Exception:
                pass
            return html_content or None

        async with async_playwright() as p:  # type: ignore[misc]
            browser_local = await p.chromium.launch(headless=True)
            context = await browser_local.new_context(
                ignore_https_errors=True,
                user_agent="Mozilla/5.0",
                locale="ko-KR",
            )
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    await page.wait_for_load_state("networkidle", timeout=timeout_ms)
                except PlaywrightTimeoutError:
                    pass
                html_content = await page.content()
            finally:
                await context.close()
                await browser_local.close()
        return html_content or None

    async def _get_html_with_fallback(self, url: str, allow_playwright: bool, session: Any = None, browser: Any = None) -> Optional[str]:
        html = await self._fetch_static_html(url, session=session)
        if html:
            return html
        if not allow_playwright:
            return None
        return await self._fetch_dynamic_html(url, browser=browser)

    def _prune_cache(self) -> None:
        max_size = max(100, self._env_int("RUNTIME_TAB_VIEW_CACHE_MAX", 2000))
        if len(self._list_view_cache) > max_size:
            self._list_view_cache.clear()
        if len(self._list_view_empty_cache) > max_size:
            self._list_view_empty_cache.clear()
        if len(self._list_file_cache) > max_size:
            self._list_file_cache.clear()
        if len(self._list_file_empty_cache) > max_size:
            self._list_file_empty_cache.clear()

    # --- public APIs (methods) ---
    async def _expand_list_to_views(self, list_url: str, allow_playwright: bool, session: Any = None, browser: Any = None) -> List[str]:
        if not list_url or not self._is_list_page_url(list_url):
            return []
        if self._should_skip_list_url(list_url):
            cache_key = self._list_cache_key(list_url)
            self._list_view_empty_cache[cache_key] = time.monotonic()
            self._prune_cache()
            logger.info("[runtime_tab_view] list->view skip by pattern | url=%s", list_url)
            return []
        cache_key = self._list_cache_key(list_url)
        now = time.monotonic()
        empty_ttl = max(10.0, self._env_float("RUNTIME_TAB_VIEW_EMPTY_TTL_SEC", 180.0))
        cache_ttl = max(30.0, self._env_float("RUNTIME_TAB_VIEW_CACHE_TTL_SEC", 300.0))
        empty_ts = self._list_view_empty_cache.get(cache_key)
        if empty_ts is not None:
            if now - empty_ts <= empty_ttl:
                return []
            self._list_view_empty_cache.pop(cache_key, None)
        cached = self._list_view_cache.get(cache_key)
        if cached is not None:
            cached_ts, cached_views = cached
            if now - cached_ts <= cache_ttl:
                return [v for v in cached_views if self._is_same_board_scope(list_url, v)]
            self._list_view_cache.pop(cache_key, None)
        html = await self._fetch_static_html(list_url, session=session)
        views = self._extract_view_links_from_html(html or "", list_url)
        if not views and allow_playwright:
            dynamic_html = await self._fetch_dynamic_html(list_url, browser=browser)
            views = self._extract_view_links_from_html(dynamic_html or "", list_url)
        if views:
            self._list_view_cache[cache_key] = (time.monotonic(), list(views))
            self._prune_cache()
        else:
            self._list_view_empty_cache[cache_key] = time.monotonic()
            self._prune_cache()
        return views

    async def _expand_list_to_files(self, list_url: str, allow_playwright: bool, session: Any = None, browser: Any = None) -> List[str]:
        if not list_url or not self._is_list_page_url(list_url):
            return []
        if self._should_skip_list_url(list_url):
            cache_key = self._list_cache_key(list_url)
            self._list_file_empty_cache[cache_key] = time.monotonic()
            self._prune_cache()
            logger.info("[runtime_tab_view] list->file skip by pattern | url=%s", list_url)
            return []
        cache_key = self._list_cache_key(list_url)
        now = time.monotonic()
        empty_ttl = max(10.0, self._env_float("RUNTIME_TAB_VIEW_EMPTY_TTL_SEC", 180.0))
        cache_ttl = max(30.0, self._env_float("RUNTIME_TAB_VIEW_CACHE_TTL_SEC", 300.0))
        empty_ts = self._list_file_empty_cache.get(cache_key)
        if empty_ts is not None:
            if now - empty_ts <= empty_ttl:
                return []
            self._list_file_empty_cache.pop(cache_key, None)
        cached = self._list_file_cache.get(cache_key)
        if cached is not None:
            cached_ts, cached_files = cached
            if now - cached_ts <= cache_ttl:
                return [f for f in cached_files if self._is_same_board_scope(list_url, f)]
            self._list_file_cache.pop(cache_key, None)
        html = await self._fetch_static_html(list_url, session=session)
        files = self._extract_file_links_from_html(html or "", list_url)
        if not files and allow_playwright:
            dynamic_html = await self._fetch_dynamic_html(list_url, browser=browser)
            files = self._extract_file_links_from_html(dynamic_html or "", list_url)
        if files:
            self._list_file_cache[cache_key] = (time.monotonic(), list(files))
            self._prune_cache()
        else:
            self._list_file_empty_cache[cache_key] = time.monotonic()
            self._prune_cache()
        return files

    async def _discover_deptgdc_list_tabs(self, url: str, allow_playwright: bool, session: Any = None, browser: Any = None) -> List[str]:
        dept_id = self._get_query_param(url, "deptId")
        html = await self._get_html_with_fallback(url, allow_playwright, session=session, browser=browser)
        return self._extract_deptgdc_tab_list_links(html or "", url, dept_id)

    async def _discover_generic_list_tabs(self, url: str, allow_playwright: bool, session: Any = None, browser: Any = None) -> List[str]:
        html = await self._get_html_with_fallback(url, allow_playwright, session=session, browser=browser)
        return self._extract_generic_tab_list_links(html or "", url)

    # Public entrypoints that match previous module-level functions
    async def resolve_start_urls_to_list_pages(self, start_urls: Sequence[str], *, allow_playwright: bool = True) -> List[str]:
        if not start_urls:
            return []
        allow_playwright = bool(allow_playwright) and str(
            os.getenv("RUNTIME_TAB_VIEW_PLAYWRIGHT", "0")
        ).lower() in {"1", "true", "yes", "on"}
        normalized = [ensure_url_scheme(u) for u in start_urls if u]
        out: List[str] = []
        dropped = 0
        tab_hits = 0
        generic_tab_hits = 0
        session = None
        if aiohttp:
            session = aiohttp.ClientSession()
        p_instance = None
        browser = None
        if allow_playwright and async_playwright:
            try:
                p_instance = await async_playwright().start()
                browser = await p_instance.chromium.launch(headless=True)
            except Exception:
                pass
        try:
            for u in normalized:
                if not u:
                    continue
                if self._is_top_domain_url(u):
                    sitemap_list_urls = self._load_sitemap_list_urls(u)
                    if sitemap_list_urls:
                        tab_hits += len(sitemap_list_urls)
                        out.extend(sitemap_list_urls)
                        logger.info(
                            "[runtime_tab_view] sitemap list urls applied | url=%s count=%s",
                            u,
                            len(sitemap_list_urls),
                        )
                        continue
                if self._is_deptgdc_url(u):
                    tabs = await self._discover_deptgdc_list_tabs(u, allow_playwright, session=session, browser=browser)
                    if not tabs:
                        dropped += 1
                        logger.info("[runtime_tab_view] deptgdc tabs not found; skip | url=%s", u)
                        continue
                    tab_hits += len(tabs)
                    for tab_url in tabs:
                        if self._should_skip_list_url(tab_url):
                            dropped += 1
                            logger.info("[runtime_tab_view] list skip by pattern | url=%s", tab_url)
                            continue
                        out.append(tab_url)
                    continue
                # generic tab discovery (configurable)
                try:
                    generic_tab_on = str(os.getenv("RUNTIME_TAB_VIEW_GENERIC", "1")).strip().lower() in (
                        "1", "true", "yes", "on",
                    )
                except Exception:
                    generic_tab_on = True

                # heuristic: suspect board if URL contains common board indicators (e.g. /bbs/ or query params)
                u_lower = (u or "").lower()
                suspect_board = "/bbs/" in u_lower or "bbsid" in u_lower or "boardid" in u_lower or "bbs=" in u_lower or ("?" in u and "=" in u)

                # If suspected or generic discovery enabled, attempt generic tab discovery
                if generic_tab_on or suspect_board:
                    tabs = await self._discover_generic_list_tabs(u, allow_playwright, session=session, browser=browser)
                    if tabs:
                        uniq_by_key: List[str] = []
                        seen_keys = set()
                        for t in tabs:
                            key = self._normalize_list_cache_key(t)
                            if key in seen_keys:
                                continue
                            seen_keys.add(key)
                            uniq_by_key.append(t)
                        if len(uniq_by_key) >= 2:
                            generic_tab_hits += len(uniq_by_key)
                            for tab_url in uniq_by_key:
                                if self._should_skip_list_url(tab_url):
                                    dropped += 1
                                    logger.info("[runtime_tab_view] list skip by pattern | url=%s", tab_url)
                                    continue
                                out.append(tab_url)
                            continue
                if self._is_list_page_url(u):
                    if self._should_skip_list_url(u):
                        dropped += 1
                        logger.info("[runtime_tab_view] list skip by pattern | url=%s", u)
                        continue
                    out.append(u)
                    continue
                if self._is_view_page_url(u):
                    out.append(u)
                    logger.info("[runtime_tab_view] view url passthrough (no-verify) | url=%s", u)
                    continue
                # non-list URL은 검증 없이 바로 통과
                out.append(u)
                logger.info("[runtime_tab_view] non-list url passthrough (no-verify) | url=%s", u)
                continue
                dropped += 1
                logger.info("[runtime_tab_view] non-list url skipped | url=%s", u)
        finally:
            if session:
                await session.close()
            if browser:
                await browser.close()
            if p_instance:
                await p_instance.stop()
        out = self._dedupe_keep_order(out)
        logger.info(
            "[runtime_tab_view] resolve_start_urls_to_list_pages done | in=%s out=%s tabs=%s generic_tabs=%s dropped=%s",
            len(normalized),
            len(out),
            tab_hits,
            generic_tab_hits,
            dropped,
        )
        return out

    async def extract_views_from_list_pages(self, list_urls: Sequence[str], *, mode: str = "board", allow_playwright: bool = True, enable_pagination: bool = True, max_pages_per_list: Optional[int] = None) -> List[str]:
        if not list_urls:
            return []
        allow_playwright = bool(allow_playwright) and str(
            os.getenv("RUNTIME_TAB_VIEW_PLAYWRIGHT", "0")
        ).lower() in {"1", "true", "yes", "on"}
        allow_playwright_list = allow_playwright and str(
            os.getenv("RUNTIME_TAB_VIEW_LIST_PLAYWRIGHT", "0")
        ).lower() in {"1", "true", "yes", "on"}
        if max_pages_per_list is None:
            try:
                max_pages_per_list = int(os.getenv("RUNTIME_TAB_VIEW_MAX_PAGES_PER_LIST", "10") or "10")
            except Exception:
                max_pages_per_list = 10
        out: List[str] = []
        dropped = 0
        view_hits = 0
        file_hits = 0
        session = None
        if aiohttp:
            session = aiohttp.ClientSession()
        p_instance = None
        browser = None
        if allow_playwright and async_playwright:
            try:
                p_instance = await async_playwright().start()
                browser = await p_instance.chromium.launch(headless=True)
            except Exception:
                pass
        try:
            try:
                conc = int(os.getenv("RUNTIME_TAB_VIEW_CONCURRENCY", "10") or "10")
            except Exception:
                conc = 10
            conc = max(1, min(conc, 20))
            sem = asyncio.Semaphore(conc)
            try:
                total_timeout = float(os.getenv("RUNTIME_TAB_VIEW_TOTAL_TIMEOUT_SEC", "30") or "30")
            except Exception:
                total_timeout = 30.0
            total_timeout = max(10.0, min(total_timeout, 300.0))
            try:
                list_timeout = float(os.getenv("RUNTIME_TAB_VIEW_LIST_TIMEOUT_SEC", "12") or "12")
            except Exception:
                list_timeout = 12.0
            list_timeout = max(3.0, min(list_timeout, 30.0))

            async def _process_list_page(list_url: str) -> List[str]:
                async with sem:
                    all_results: List[str] = []
                    pages_visited: set[str] = set()
                    pages_to_visit: List[str] = [list_url]
                    page_count = 0
                    while pages_to_visit and page_count < max_pages_per_list:
                        current_page = pages_to_visit.pop(0)
                        cache_key = self._normalize_list_cache_key(current_page)
                        if cache_key in pages_visited:
                            continue
                        pages_visited.add(cache_key)
                        page_count += 1
                        try:
                            html = await self._fetch_static_html(current_page, session=session)
                            if not html and allow_playwright_list:
                                html = await self._fetch_dynamic_html(current_page, browser=browser)
                            if not html:
                                continue
                            if mode == "file":
                                page_results = self._extract_file_links_from_html(html, current_page)
                                nonlocal file_hits
                                file_hits += len(page_results)
                            else:
                                page_results = self._extract_view_links_from_html(html, current_page)
                                nonlocal view_hits
                                view_hits += len(page_results)
                            all_results.extend(page_results)
                            if enable_pagination and page_count < max_pages_per_list:
                                pagination_links = self._extract_pagination_links_from_html(html, current_page)
                                for pag_link in pagination_links:
                                    pag_cache_key = self._normalize_list_cache_key(pag_link)
                                    if pag_cache_key not in pages_visited:
                                        if pag_link not in pages_to_visit:
                                            pages_to_visit.append(pag_link)
                        except asyncio.TimeoutError:
                            break
                        except Exception:
                            continue
                    return all_results

            task_map = {asyncio.create_task(_process_list_page(u)): u for u in list_urls}
            t_total = time.monotonic()
            for fut in asyncio.as_completed(task_map):
                elapsed_total = time.monotonic() - t_total
                if elapsed_total > total_timeout:
                    for t in task_map:
                        if not t.done():
                            t.cancel()
                    break
                try:
                    results = await fut
                except Exception:
                    results = []
                if results:
                    out.extend(results)
                else:
                    dropped += 1
                    try:
                        if mode == "file":
                            logger.info("[runtime_tab_view] list->file empty; skip | url=%s", task_map.get(fut))
                        else:
                            logger.info("[runtime_tab_view] list->view empty; skip | url=%s", task_map.get(fut))
                    except Exception:
                        pass
        finally:
            if session:
                await session.close()
            if browser:
                await browser.close()
            if p_instance:
                await p_instance.stop()
        out = self._dedupe_keep_order(out)
        if mode == "file":
            logger.info(
                "[runtime_tab_view] extract_views_from_list_pages done | mode=%s in=%s out=%s files=%s dropped=%s",
                mode,
                len(list_urls),
                len(out),
                file_hits,
                dropped,
            )
        else:
            logger.info(
                "[runtime_tab_view] extract_views_from_list_pages done | mode=%s in=%s out=%s views=%s dropped=%s",
                mode,
                len(list_urls),
                len(out),
                view_hits,
                dropped,
            )
        return out

    async def resolve_runtime_start_urls(self, start_urls: Sequence[str], *, mode: str = "board", allow_playwright: bool = True, strict_view_only: bool = False) -> List[str]:
        if not start_urls:
            return []
        normalized = [ensure_url_scheme(u) for u in start_urls if u]
        direct_views: List[str] = []
        list_candidates: List[str] = []
        unknown_urls: List[str] = []
        session = None
        if aiohttp:
            session = aiohttp.ClientSession()
        try:
            for u in normalized:
                if self._is_view_page_url(u):
                    direct_views.append(u)
                    continue
                if self._is_list_page_url(u):
                    list_candidates.append(u)
                    continue
                # 검증/판별 없이 상세로 처리 (바로 워크플로우 시작)
                direct_views.append(u)
        finally:
            if session:
                await session.close()
        list_pages = list_candidates
        if unknown_urls:
            list_pages.extend(
                await self.resolve_start_urls_to_list_pages(unknown_urls, allow_playwright=allow_playwright)
            )
        list_pages = self._dedupe_keep_order(list_pages)
        enable_pagination = (mode == "file")
        views_from_lists = await self.extract_views_from_list_pages(list_pages, mode=mode, allow_playwright=allow_playwright, enable_pagination=enable_pagination)
        out = direct_views + views_from_lists
        out = self._dedupe_keep_order(out)
        if strict_view_only:
            out = [u for u in out if self._is_view_page_url(u)]
        return out

    # compatibility helpers for names used in module-level wrapper
    def list_cache_key(self, url: str) -> str:
        return self._normalize_list_cache_key(url)

    # alias for backward compatibility
    _list_cache_key = _normalize_list_cache_key
