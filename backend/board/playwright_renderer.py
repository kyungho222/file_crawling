import asyncio
import re
from typing import Optional, Tuple, List
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin

from backend.shared.config import Config
from backend.shared.playwright_optimizations import (
    apply_stealth_if_needed,
    configure_context_for_crawl,
)

try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    async_playwright = None
    PlaywrightTimeoutError = Exception  # type: ignore[misc,assignment]


class PlaywrightRenderError(RuntimeError):
    """Playwright 렌더링 실패를 명시적으로 나타내는 예외."""


_browser_lock = asyncio.Lock()
_semaphore = asyncio.Semaphore(max(1, (Config.PLAYWRIGHT_MAX_CONCURRENT or 1)))
_playwright_instance = None
_browser = None


async def _ensure_browser():
    """
    프로세스당 Chromium 브라우저 1개를 재사용한다.
    - 이미 연결된 브라우저가 있으면 새로 launch 하지 않는다.
    - 끊긴 경우에만 해당 Browser만 정리 후, 동일 Playwright 드라이버로 launch 1회 시도한다.
    """
    global _playwright_instance, _browser
    if async_playwright is None:
        raise PlaywrightRenderError("Playwright가 설치되어 있지 않습니다.")
    async with _browser_lock:
        if _browser is not None and hasattr(_browser, "is_connected") and _browser.is_connected():
            return _browser

        if _browser is not None:
            try:
                await _browser.close()
            except Exception:
                pass
            _browser = None

        if _playwright_instance is None:
            _playwright_instance = await async_playwright().start()

        headless = bool(getattr(Config, "PLAYWRIGHT_HEADLESS", True))
        try:
            _browser = await _playwright_instance.chromium.launch(
                headless=headless,
                args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"],
                timeout=Config.PLAYWRIGHT_TIMEOUT * 1000,
            )
        except Exception as e:
            raise PlaywrightRenderError(f"Playwright chromium.launch 실패: {e}") from e
        return _browser


def _list_url_for_page_index(base_list_url: str, page_index: int) -> str:
    """목록 URL에 pg/pageIndex(1-based) 쿼리를 붙인 URL 반환."""
    if page_index <= 1:
        return base_list_url.rstrip("/") + ("/" if not base_list_url.rstrip("/").endswith("/") else "")
    try:
        parsed = urlparse(base_list_url)
        path = parsed.path or ""
        if not path.endswith("/"):
            path = path + "/"
        query = parse_qs(parsed.query or "", keep_blank_values=True)
        query["pg"] = [str(page_index)]
        query["pageIndex"] = [str(page_index)]
        new_query = urlencode(query, doseq=True)
        return urlunparse((parsed.scheme or "https", parsed.netloc, path, parsed.params, new_query, parsed.fragment))
    except Exception:
        sep = "&" if "?" in base_list_url else "?"
        return f"{base_list_url.rstrip('/')}{sep}pg={page_index}&pageIndex={page_index}"


def _extract_mng_no_from_href(href: str) -> str:
    """href에서 ctrtAcctBookMngNo 값을 대소문자 무관하게 추출."""
    if not href:
        return ""
    try:
        p = urlparse(href)
        q = parse_qs(p.query or "", keep_blank_values=True)
        for k, v in q.items():
            if (k or "").lower() == "ctrtacctbookmngno" and v:
                return (v[0] or "").strip()
    except Exception:
        pass
    m = re.search(r"ctrtacctbookmngno\s*=\s*([^&]+)", href or "", re.I)
    return (m.group(1) if m else "").strip()


async def _open_one_page_and_find_link(
    browser,
    page_url: str,
    target_id: str,
    page_num: int,
    skip_titles: tuple,
    list_wait_ms: int,
    max_scroll_to_find: int,
) -> Tuple[int, Optional[str], Optional[object], Optional[object]]:
    """한 목록 페이지를 열고 target_id에 해당하는 상세 링크 href를 찾아 (page_num, detail_href, context, page) 반환. 없으면 href=None."""
    context = None
    page = None
    try:
        context = await browser.new_context(ignore_https_errors=True, viewport={"width": 1920, "height": 1080})
        await configure_context_for_crawl(context, page_url)
        page = await context.new_page()
        await apply_stealth_if_needed(page, page_url)
        await page.goto(page_url, wait_until="domcontentloaded", timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
        if list_wait_ms > 0:
            await page.wait_for_timeout(list_wait_ms)
        try:
            await page.wait_for_selector("tbody tr, .serch_sub_result_ul li, a[href*='ctrt'], a[href*='Ctrt']", timeout=3000)
        except Exception:
            pass
        target_norm = (target_id or "").strip().lower()
        skip_title_set = {str(t or "").strip() for t in skip_titles}
        for _ in range(max_scroll_to_find):
            found = await page.evaluate(
                """({ targetId, skipTitles }) => {
                    const norm = String(targetId || "").trim().toLowerCase();
                    if (!norm) return "";
                    const skip = new Set((skipTitles || []).map(v => String(v || "").trim()));
                    const anchors = Array.from(document.querySelectorAll("a[href]"));
                    for (const a of anchors) {
                        const hrefRaw = String(a.getAttribute("href") || "");
                        if (!hrefRaw) continue;
                        const txt = String((a.textContent || "").trim());
                        if (skip.has(txt)) continue;
                        const m = hrefRaw.match(/ctrtacctbookmngno\\s*=\\s*([^&]+)/i);
                        if (!m || !m[1]) continue;
                        if (String(m[1]).trim().toLowerCase() === norm) {
                            return hrefRaw;
                        }
                    }
                    return "";
                }""",
                {"targetId": target_norm, "skipTitles": list(skip_title_set)},
            )
            _h = (found or "").strip()
            if _h:
                if _h.startswith("/"):
                    _h = urljoin(page_url, _h)
                return (page_num, _h, context, page)
            await page.evaluate("window.scrollBy(0, 400)")
            await page.wait_for_timeout(80)
        return (page_num, None, context, page)
    except Exception:
        if context:
            try:
                await context.close()
            except Exception:
                pass
        return (page_num, None, None, None)


async def render_page_via_playwright_hybrid_click(
    list_url: str,
    target_id: str,
    *,
    wait_until: Optional[str] = None,
    list_wait_ms: int = 400,
    detail_wait_ms: int = 800,
    wait_for_selector: Optional[str] = ".ctrtAcctBook, .detail_view, .serch_result_wrap",
    max_scroll_to_find: int = 3,
    max_pages_to_search: int = 50,
    pagination_next_text: tuple = ("다음", "다음 페이지", ">", "›", "»", "next"),
    parallel_pages: int = 12,
    sequential_fallback: bool = False,
) -> Tuple[str, str]:
    """
    춘천시청 계약정보 상세 보조 경로(LLM 스펙): `navigate_from` 이 빈 본문일 때만 사용.

    - `pageIndex` 를 여러 개 병렬로 열어 `ctrtAcctBookMngNo` 링크를 찾은 뒤 상세로 이동/클릭.
    - `sequential_fallback=False` 가 기본(순차 '다음' 페이징은 느려 비활성).
    """
    browser = await _ensure_browser()
    wait_until = wait_until or getattr(Config, "PLAYWRIGHT_WAIT_UNTIL", "networkidle")
    skip_titles = ("번호", "제목", "No", "NO")

    async with _semaphore:
        # 1) 병렬로 여러 목록 페이지를 동시에 열어 각각에서 a[href] 탐색
        batch_size = min(parallel_pages, max_pages_to_search)
        for start in range(0, max_pages_to_search, batch_size):
            page_indices: List[int] = [start + i + 1 for i in range(batch_size) if start + i < max_pages_to_search]
            if not page_indices:
                break
            urls = [_list_url_for_page_index(list_url, pi) for pi in page_indices]
            tasks = [
                _open_one_page_and_find_link(
                    browser, urls[i], target_id, page_indices[i] - 1, skip_titles, list_wait_ms, max_scroll_to_find
                )
                for i in range(len(urls))
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            winner_ctx = None
            winner_page = None
            detail_href = None
            for r in results:
                if isinstance(r, Exception):
                    continue
                _pnum, href, ctx, pg = r
                if href and ctx and pg:
                    detail_href = href
                    winner_ctx = ctx
                    winner_page = pg
                    print(f"[Hybrid-Debug] 병렬 탐색 {_pnum + 1}페이지에서 target_id={target_id} 발견")
                    break
            for r in results:
                if isinstance(r, Exception):
                    continue
                _pnum, href, ctx, pg = r
                if ctx is not None and ctx is not winner_ctx:
                    try:
                        await ctx.close()
                    except Exception:
                        pass
            if detail_href and winner_ctx and winner_page:
                try:
                    await winner_page.goto(detail_href, wait_until="domcontentloaded", timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
                    if detail_wait_ms > 0:
                        await winner_page.wait_for_timeout(detail_wait_ms)
                    if wait_for_selector:
                        try:
                            await winner_page.wait_for_selector(wait_for_selector, state="visible", timeout=5000)
                        except Exception:
                            pass
                    html_content = await winner_page.content()
                    final_url = winner_page.url
                    return html_content, final_url
                finally:
                    try:
                        await winner_ctx.close()
                    except Exception:
                        pass
                break

        # 2) 병렬 배치만으로 못 찾은 경우: 기본은 여기서 종료(순차 페이징은 매우 느림).
        #    정말 필요할 때만 sequential_fallback=True 로 순차 "다음" 클릭 재시도.
        if not sequential_fallback:
            return "", list_url

        context = await browser.new_context(ignore_https_errors=True, viewport={"width": 1920, "height": 1080})
        await configure_context_for_crawl(context, list_url)
        page = await context.new_page()
        await apply_stealth_if_needed(page, list_url)
        try:
            async def _find_matching_link(links_locator):
                _cnt = await links_locator.count()
                target_norm = (target_id or "").strip().lower()
                for _i in range(_cnt):
                    _loc = links_locator.nth(_i)
                    try:
                        await _loc.wait_for(state="visible", timeout=500)
                        _t = (await _loc.inner_text()).strip() or ""
                        _h = await _loc.get_attribute("href") or ""
                        found_mng = _extract_mng_no_from_href(_h).lower()
                        if found_mng and found_mng == target_norm and _t not in skip_titles:
                            return _loc
                    except Exception:
                        continue
                return None

            print(f"[Hybrid] 목록 순차 탐색 (target_id={target_id}): {list_url}")
            await page.goto(list_url, wait_until="domcontentloaded", timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
            locator = None
            for page_num in range(max_pages_to_search):
                try:
                    await page.wait_for_selector("tbody tr, .serch_sub_result_ul li", timeout=4000)
                except Exception:
                    pass
                list_scope = ".serch_sub_result_ul, .serch_result_list, .board_list, [class*='result'], tbody, .list"
                all_on_page = page.locator(f"{list_scope} a[href*='ctrt'], {list_scope} a[href*='Ctrt']")
                for scroll_attempt in range(max_scroll_to_find):
                    locator = await _find_matching_link(all_on_page)
                    if locator is not None:
                        break
                    await page.evaluate("window.scrollBy(0, 400)")
                    await page.wait_for_timeout(100)
                if locator is not None:
                    print(f"[Hybrid-Debug] {page_num + 1}페이지에서 target_id={target_id} 발견")
                    break
                try:
                    await page.wait_for_selector(".pagination, .paging, .page_nav", timeout=1500)
                except Exception:
                    pass
                next_clicked = False
                for _txt in pagination_next_text:
                    _next = page.locator(f"a:has-text('{_txt}'), button:has-text('{_txt}'), [class*='paging'] a:has-text('{_txt}')").first
                    try:
                        await _next.wait_for(state="visible", timeout=1200)
                        async with page.expect_navigation(wait_until="domcontentloaded", timeout=4000):
                            await _next.click()
                        next_clicked = True
                        print(f"[Hybrid-Debug] {page_num + 1}페이지 -> 다음 페이지로 이동 ({_txt})")
                        break
                    except Exception:
                        continue
                if not next_clicked:
                    raise PlaywrightRenderError(f"ID {target_id} 매칭 링크를 찾지 못함 (마지막 페이지: {page_num + 1})")

            print(f"[Hybrid] 타겟 클릭 진입: {target_id}")
            async with page.expect_navigation(wait_until="domcontentloaded", timeout=10000):
                await locator.click()
            await asyncio.sleep(detail_wait_ms / 1000)
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, state="visible", timeout=5000)
                except Exception:
                    pass
            html_content = await page.content()
            return html_content, page.url
        finally:
            await context.close()


async def render_page_via_playwright(
    target_url: str,
    *,
    headers: Optional[dict] = None,
    referer: Optional[str] = None,
    wait_until: Optional[str] = None,
    extra_wait_ms: Optional[int] = None,
    wait_for_selector: Optional[str] = None,
    wait_for_selector_timeout_ms: Optional[int] = None,
) -> Tuple[str, str]:
    """단일 URL을 Playwright로 열어 HTML 반환."""
    browser = await _ensure_browser()
    wait_until = wait_until or getattr(Config, "PLAYWRIGHT_WAIT_UNTIL", "domcontentloaded")
    async with _semaphore:
        context = await browser.new_context(
            ignore_https_errors=True,
            viewport={"width": 1920, "height": 1080},
            extra_http_headers=dict(headers or {}),
        )
        await configure_context_for_crawl(context, target_url)
        page = await context.new_page()
        await apply_stealth_if_needed(page, target_url)
        try:
            await page.goto(target_url, wait_until=wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000, referer=referer or "")
            if extra_wait_ms:
                await page.wait_for_timeout(extra_wait_ms)
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, state="visible", timeout=wait_for_selector_timeout_ms or 8000)
                except Exception:
                    pass
            return await page.content(), page.url
        finally:
            await context.close()


async def render_page_via_playwright_click_from(
    list_url: str,
    detail_url: str,
    *,
    wait_until: Optional[str] = None,
    list_wait_ms: int = 800,
    detail_wait_ms: int = 2000,
    wait_for_selector: Optional[str] = None,
    wait_for_selector_timeout_ms: Optional[int] = 8000,
) -> Tuple[str, str]:
    """Open a board list first, click the matching detail link if present, then return HTML."""
    browser = await _ensure_browser()
    wait_until = wait_until or "domcontentloaded"
    async with _semaphore:
        context = await browser.new_context(ignore_https_errors=True, viewport={"width": 1920, "height": 1080})
        await configure_context_for_crawl(context, list_url)
        page = await context.new_page()
        await apply_stealth_if_needed(page, list_url)
        try:
            await page.goto(list_url, wait_until=wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
            if list_wait_ms > 0:
                await page.wait_for_timeout(list_wait_ms)
            clicked = await page.evaluate(
                """
                ({ detailUrl }) => {
                    const abs = (value) => {
                        try { return new URL(value || "", location.href).href; } catch (_) { return ""; }
                    };
                    const target = abs(detailUrl);
                    const targetLower = target.toLowerCase();
                    let targetParams = {};
                    try {
                        const u = new URL(target);
                        for (const [k, v] of u.searchParams.entries()) {
                            targetParams[k.toLowerCase()] = String(v || "").toLowerCase();
                        }
                    } catch (_) {}
                    const importantKeys = ["q_bbscttsn", "bbscttsn", "nttid", "seq", "id", "no"];
                    const nodes = Array.from(document.querySelectorAll("a[href], a[onclick], button[onclick]"));
                    let best = null;
                    for (const el of nodes) {
                        const href = abs(el.getAttribute("href") || "");
                        const onclick = String(el.getAttribute("onclick") || "");
                        const haystack = `${href} ${onclick}`.toLowerCase();
                        if (href && href.toLowerCase() === targetLower) {
                            best = el;
                            break;
                        }
                        let hits = 0;
                        for (const key of importantKeys) {
                            const val = targetParams[key.toLowerCase()];
                            if (val && haystack.includes(val)) hits += 1;
                        }
                        if (hits >= 1) {
                            best = el;
                            break;
                        }
                    }
                    if (!best) return false;
                    try { best.scrollIntoView({ block: "center", inline: "center" }); } catch (_) {}
                    try { best.click(); return true; } catch (_) { return false; }
                }
                """,
                {"detailUrl": detail_url},
            )
            if clicked:
                try:
                    await page.wait_for_load_state(wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
                except Exception:
                    pass
                if detail_wait_ms > 0:
                    await page.wait_for_timeout(detail_wait_ms)
                if wait_for_selector:
                    try:
                        await page.wait_for_selector(wait_for_selector, state="visible", timeout=wait_for_selector_timeout_ms or 8000)
                    except Exception:
                        pass
                return await page.content(), page.url
            await page.goto(detail_url, wait_until=wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
            if detail_wait_ms > 0:
                await page.wait_for_timeout(detail_wait_ms)
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, state="visible", timeout=wait_for_selector_timeout_ms or 8000)
                except Exception:
                    pass
            return await page.content(), page.url
        finally:
            await context.close()


async def render_page_via_playwright_navigate_from(
    list_url: str,
    detail_url: str,
    *,
    wait_until: Optional[str] = None,
    list_wait_ms: int = 400,
    detail_wait_ms: int = 800,
    wait_for_selector: Optional[str] = None,
    wait_for_selector_timeout_ms: Optional[int] = 6000,
) -> Tuple[str, str]:
    """
    춘천시청 계약정보 **Session warming**: 동일 컨텍스트에서 `list_url` 로드 후 `detail_url` 로 이동해
    쿠키·세션을 맞춘 뒤 HTML을 반환한다. (Headless Chromium, 컨텍스트 1회 단위)
    """
    browser = await _ensure_browser()
    wait_until = wait_until or "domcontentloaded"
    async with _semaphore:
        context = await browser.new_context(ignore_https_errors=True, viewport={"width": 1920, "height": 1080})
        await configure_context_for_crawl(context, list_url)
        page = await context.new_page()
        await apply_stealth_if_needed(page, list_url)
        try:
            await page.goto(list_url, wait_until=wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
            if list_wait_ms > 0:
                await page.wait_for_timeout(list_wait_ms)
            await page.goto(detail_url, wait_until=wait_until, timeout=Config.PLAYWRIGHT_PAGE_TIMEOUT * 1000)
            if detail_wait_ms > 0:
                await page.wait_for_timeout(detail_wait_ms)
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, state="visible", timeout=wait_for_selector_timeout_ms or 6000)
                except Exception:
                    pass
            return await page.content(), page.url
        finally:
            await context.close()


async def shutdown_playwright_renderer() -> None:
    """모듈 전역 Chromium/Playwright를 종료한다(스크립트 종료 시 Windows 파이프 deallocator 경고 완화)."""
    global _playwright_instance, _browser
    async with _browser_lock:
        try:
            if _browser is not None:
                await _browser.close()
        except Exception:
            pass
        _browser = None
        try:
            if _playwright_instance is not None:
                await _playwright_instance.stop()
        except Exception:
            pass
        _playwright_instance = None
