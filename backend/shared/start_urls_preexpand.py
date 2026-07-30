import asyncio
import json
import logging
import os
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from utils.runtime_flags import is_no_limits_mode
from utils.crawl_url_normalizer import canonicalize_crawl_url
from backend.shared.board_header import find_sitemap_descendants, get_base_origin, load_sitemap_cache

try:
    import requests  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    requests = None  # type: ignore[assignment]

try:
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore[assignment]

def _debug_log(*, location: str, message: str, data: Dict[str, object], hypothesis_id: str) -> None:
    try:
        log = logging.getLogger("backend.shared.start_urls_preexpand")
        if not log.isEnabledFor(logging.DEBUG):
            return
        payload = {
            "sessionId": "debug-session",
            "runId": "run1",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        log.debug(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


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


def _is_nav_or_sidebar_anchor(a) -> bool:
    try:
        for parent in a.parents:
            name = (getattr(parent, "name", "") or "").lower()
            if name in _NAV_CONTAINER_TAGS:
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
                if any(h in lt for h in _NAV_CONTAINER_HINTS):
                    return True
    except Exception:
        return False
    return False


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


def _get_query_param(url: str, key: str) -> Optional[str]:
    """URL?癒?퐣 ?諭???묒눖?????뵬沃섎챸苑??곕뗄??"""
    try:
        from urllib.parse import parse_qs
        parsed = urlparse(url)
        qs = parse_qs(parsed.query or "")
        if key in qs and qs[key]:
            return qs[key][0]
        key_lower = key.lower()
        for k, v in qs.items():
            if k.lower() == key_lower and v:
                return v[0]
    except Exception:
        pass
    return None


def _extract_menu_no(url: str) -> Optional[str]:
    """野껊슣???menuNo夷똠tgryCd ??筌롫뗀???브쑬履??묒눖???곕뗄??"""
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
        val = _get_query_param(url, key)
        if val:
            return val
    return None


_PAGE_QUERY_KEYS = {
    "pageindex",
    "pageno",
    "pgno",
    "page",
    "curpage",
    "page_no",
    "page_index",
}


def _extract_board_id(path: str) -> Optional[str]:
    """野껊슣???ID ?곕뗄??(/bbs/{id}/..., /board/{id}/...)."""
    try:
        import re
        m = re.search(r"/(?:bbs|board)/([^/]+)/", path or "", re.IGNORECASE)
    except Exception:
        m = None
    if not m:
        return None
    return m.group(1)


def _normalize_list_cache_key(u: str) -> str:
    """
    揶쏆늿? 野껊슣???list URL 筌?Ŋ?????類?뇣??(pageIndex/pageNo/page ????륁뵠筌????뵬沃섎챸苑???볤탢).

    雅뚯눘?? ????λ땾??筌?Ŋ?????類?뇣?遺? ?袁る퉸 ??륁뵠筌????뵬沃섎챸苑ｇ몴???볤탢??몃빍??
    ??쇱젫 ??륁뵠筌?筌ｌ꼶???_expand_list_to_views_router ???????삘뀲 嚥≪뮇彛?癒?퐣 ??묐뻬??몃빍??
    ??륁뵠筌????뵬沃섎챸苑ｅ첎? ??釉??URL??揶쏆늿? 野껊슣??癒?몵嚥??紐꾨뻼??뤿연 餓λ쵎??獄쎻뫗? 獄?域밸챶竊?遺용퓠 ?????몃빍??
    """
    return canonicalize_crawl_url(u) or str(u or "").strip().lower().rstrip("/")

def _canonicalize_url(u: str) -> str:
    """獄쎻뫖揆 餓λ쵎??獄쎻뫗?: utils.url????덉뵬???類?뇣????λ땾 ????"""
    return canonicalize_crawl_url(u) or u


def _view_identity_key(u: str) -> str:
    """??덉뵬 野껊슣?녷묾? view URL 餓λ쵎????볤탢???? 筌뤴뫖以?野꺜????륁뵠筌왖 query????뽰뇚??뺣뼄."""
    try:
        p = urlparse(u)
        pairs = parse_qsl(p.query or "", keep_blank_values=True)
        drop = _PAGE_QUERY_KEYS | {"lists", "keyfield", "keyword", "deptfield", "searchfield", "searchword"}
        filtered = [(k, v) for (k, v) in pairs if k.lower() not in drop]
        filtered.sort()
        q = urlencode(filtered, doseq=True)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        return urlunparse((scheme, netloc, p.path or "", "", q, ""))
    except Exception:
        return _canonicalize_url(u)


def _preexpand_output_identity_key(u: str) -> str:
    try:
        path = (urlparse(u).path or "").lower()
        if _is_list_page_url(u):
            return "list:" + _normalize_list_cache_key(u)
        if "view.do" in path or "detail.do" in path or "read.do" in path:
            return "view:" + _view_identity_key(u)
        return "url:" + _canonicalize_url(u)
    except Exception:
        return str(u or "").strip()


def _expand_single_list_url_with_sitemap(ordered: List[str]) -> List[str]:
    """
    ??μ뵬 list URL????낆젾??野껋럩??sitemap 筌?Ŋ??癒?퐣 ??륁맄 URL??筌≪뼚釉???釉??뺣뼄.
    - ??륁맄揶쎛 ??됱몵筌?[?癒?궚 + ??륁맄????獄쏆꼹??
    - ??곸몵筌??癒?궚 域밸챶?嚥?獄쏆꼹??
    """
    try:
        allow_sitemap_expand = str(os.getenv("START_URLS_PREEXPAND_INCLUDE_SITEMAP_DESC", "0")).strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
    except Exception:
        allow_sitemap_expand = False
    if not allow_sitemap_expand:
        return ordered
    if len(ordered) != 1:
        return ordered
    root_url = ordered[0]
    if not _is_list_page_url(root_url):
        return ordered
    try:
        base_origin = get_base_origin(root_url)
        groups = load_sitemap_cache(base_origin)
        if not groups:
            return ordered
        descendants = find_sitemap_descendants(groups, root_url)
        if not descendants:
            return ordered
        # ??野껊슣???ID ?곕뗄??(?袁り숲筌?疫꿸퀣?)
        try:
            root_board_id = _extract_board_id(urlparse(root_url).path or "")
        except Exception:
            root_board_id = None

        # region agent log
        try:
            other_board_samples: List[str] = []
            other_board_count = 0
            for u in descendants:
                try:
                    u_board_id = _extract_board_id(urlparse(u).path or "") or None
                except Exception:
                    u_board_id = None
                if root_board_id and u_board_id and u_board_id.lower() != root_board_id.lower():
                    other_board_count += 1
                    if len(other_board_samples) < 5:
                        other_board_samples.append(f"{u_board_id}:{u}")
            _debug_log(
                location="backend/start_urls_preexpand.py:_expand_single_list_url_with_sitemap",
                message="sitemap descendants board id scan",
                data={
                    "root_url": root_url,
                    "root_board_id": root_board_id,
                    "descendants_count": len(descendants or []),
                    "other_board_count": other_board_count,
                    "other_board_samples": other_board_samples,
                },
                hypothesis_id="H3",
            )
        except Exception:
            pass
        # endregion

        final: List[str] = []
        seen: set[str] = set()
        for u in [root_url] + descendants:
            if not u or u in seen:
                continue
            
            # ??野껊슣???ID ??깊뒄 ??? ?類ㅼ뵥
            if root_board_id:
                try:
                    u_board_id = _extract_board_id(urlparse(u).path or "")
                except Exception:
                    u_board_id = None
                if u_board_id and u_board_id.lower() != root_board_id.lower():
                    continue

            seen.add(u)
            final.append(u)
        return final
    except Exception:
        return ordered


def _build_page_url(base_page_url: str, page_no: int, param_name: str) -> str:
    try:
        p = urlparse(base_page_url)
        pairs = parse_qsl(p.query or "", keep_blank_values=True)
        qd: Dict[str, str] = {}
        for k, v in pairs:
            qd[str(k)] = str(v)
        qd[param_name] = str(page_no)
        new_pairs = sorted(qd.items(), key=lambda x: x[0])
        q = urlencode(new_pairs, doseq=True)
        return urlunparse((p.scheme or "https", p.netloc, p.path, "", q, ""))
    except Exception:
        return base_page_url


def _guess_page_param(page_url: str, soup_obj) -> str:
    try:
        qkeys = {k.lower() for (k, _v) in parse_qsl(urlparse(page_url).query or "", keep_blank_values=True)}
    except Exception:
        qkeys = set()
    for cand in ("pageindex", "pageno", "pgno", "page", "curpage"):
        if cand in qkeys:
            return (
                "pageIndex"
                if cand == "pageindex"
                else ("pageNo" if cand == "pageno" else ("pgno" if cand == "pgno" else ("page" if cand == "page" else "curPage")))
            )
    try:
        for nm in ("pageIndex", "pageNo", "pgno", "page", "curPage"):
            tag = soup_obj.find("input", attrs={"name": nm})
            if tag is not None:
                return nm
    except Exception:
        pass
    return "pageIndex"


async def _fetch_static_html(
    url: str,
    *,
    session: Optional[object] = None,
    html_cache: Optional[Dict[str, Optional[str]]] = None,
) -> Optional[str]:
    if not requests:
        return None
    cache_key = _canonicalize_url(url)
    if html_cache is not None and cache_key in html_cache:
        return html_cache[cache_key]

    try:
        timeout_sec = float(os.getenv("START_URLS_PREEXPAND_REQUEST_TIMEOUT_SEC", "3") or "3")
    except Exception:
        timeout_sec = 3.0

    def _req() -> str:
        requester = session if session is not None else requests
        r = requester.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=timeout_sec,
            allow_redirects=True,
        )
        r.raise_for_status()
        return r.text

    try:
        html = await asyncio.to_thread(_req)
        if html_cache is not None:
            html_cache[cache_key] = html
        return html
    except Exception:
        if html_cache is not None:
            html_cache[cache_key] = None
        return None


async def _expand_list_to_views_router(
    list_url: str,
    *,
    session: Optional[object] = None,
    html_cache: Optional[Dict[str, Optional[str]]] = None,
) -> List[str]:
    """
    start_urls ??ｍ?癒?퐣 list ??륁뵠筌왖??view URL??살쨮 '沃섎챶?? ?類ㅼ삢??뺣뼄.
    - query_links_only(max_depth=0) 筌뤴뫀諭?癒?퐣??view 餓λ쵐???곗쨮 ??뽰삂??롫즲嚥???됱젟???類μ넇???關湲?
    - ??쎈솭 ?????귐딅뮞??獄쏆꼹??疫꿸퀣??嚥≪뮇彛?fallback)
    """
    if not list_url or not _is_list_page_url(list_url):
        return []
    if not BeautifulSoup:
        return []

    # NOTE:
    # ??????遺욧퍕???怨뺤뵬 start_urls pre-expand ??ｍ??"筌ㅼ뮆? ??륁뵠筌왖/筌ㅼ뮆? view ?? ?怨밸립????볤탢??뺣뼄.

    cache_key = _normalize_list_cache_key(list_url)
    pages_seen: set[str] = set()
    pages_to_visit: List[str] = [list_url]
    pages_queued: set[str] = {_canonicalize_url(list_url)}
    view_seen: set[str] = set()
    views: List[str] = []

    while pages_to_visit:
        page_url = pages_to_visit.pop(0)
        canon_page = _canonicalize_url(page_url)
        pages_queued.discard(canon_page)
        if canon_page in pages_seen:
            continue
        if _normalize_list_cache_key(page_url) != cache_key:
            continue
        pages_seen.add(canon_page)

        html = await _fetch_static_html(page_url, session=session, html_cache=html_cache)
        if not html:
            continue

        try:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore[arg-type]
        except Exception:
            continue

        # 1) view 筌띻낱寃??곕뗄??(view.do/detail.do/read.do + nttId/num)
        try:
            # 野꺜???룐뫂?? ?⑤벊?????곫에?野껉퀣??
            try:
                from backend.shared.content_scope import select_search_root  # local import to avoid cycle
                search_root = select_search_root(soup, base_url=page_url)
            except Exception:
                search_root = soup

            # list_url?癒?퐣 野껊슣???ID ?곕뗄??(?袁り숲筌?疫꿸퀣?)
            try:
                base_board_id = _extract_board_id(urlparse(list_url).path or "")
            except Exception:
                base_board_id = None
            
            for a in search_root.find_all("a", href=True):
                if _is_nav_or_sidebar_anchor(a):
                    continue
                href = (a.get("href") or "").strip()
                if not href or href.startswith("#"):
                    continue
                lh = href.lower()
                if lh.startswith("javascript:"):
                    continue
                # ??揶쏆뮇苑? nttId 筌ｋ똾寃???볤탢, view.do揶쎛 ??釉??筌뤴뫀諭?筌띻낱寃??곕뗄??
                if "view.do" in lh or "detail.do" in lh or "read.do" in lh:
                    full = urljoin(page_url, href)
                    
                    # ??野껊슣???ID 疫꿸퀡而??袁り숲筌?(B0000367 ??
                    if base_board_id:
                        try:
                            link_board_id = _extract_board_id(urlparse(full).path or "")
                        except Exception:
                            link_board_id = None
                        if link_board_id and link_board_id.lower() != base_board_id.lower():
                            continue
                    
                    view_key = _view_identity_key(full)
                    if view_key in view_seen:
                        continue
                    view_seen.add(view_key)
                    views.append(full)
        except Exception:
            pass

        # 2) href 疫꿸퀡而?pagination 筌띻낱寃?list + page param)
        try:
            for a in search_root.find_all("a", href=True):
                if _is_nav_or_sidebar_anchor(a):
                    continue
                href = (a.get("href") or "").strip()
                if not href or href.startswith("#"):
                    continue
                lh = href.lower()
                if lh.startswith("javascript:"):
                    continue
                if ("list.do" in lh or "list.asp" in lh or "list.jsp" in lh) and any(k in lh for k in ("pageindex=", "pageno=", "pgno=", "page=", "curpage=")):
                    full = urljoin(page_url, href)
                    if _normalize_list_cache_key(full) != cache_key:
                        continue
                    cfull = _canonicalize_url(full)
                    if cfull not in pages_seen and cfull not in pages_queued:
                        pages_to_visit.append(full)
                        pages_queued.add(cfull)
        except Exception:
            pass

        # 3) ??疫꿸퀡而? hidden pageIndex/pageNo + ??쇱벉 ??륁뵠筌왖 ?醫롰뀤 (best-effort)
        try:
            page_param = _guess_page_param(page_url, soup)
            # ??ъ쁽 筌띻낱寃뺝첎? ??됱몵筌?筌ㅼ뮆?롥첎?域뱀눘荑귝틦??筌??癒?퉳
            nums = []
            for a in search_root.find_all("a"):
                if _is_nav_or_sidebar_anchor(a):
                    continue
                try:
                    t = (a.get_text() or "").strip()
                    if t.isdigit():
                        nums.append(int(t))
                except Exception:
                    continue
            if nums:
                max_num = max(nums)
                # ??댭?筌렺??揶쎛筌왖 ??낅즲嚥???쀫립
                for pn in range(2, min(max_num, 10) + 1):
                    full = _build_page_url(page_url, pn, page_param)
                    if _normalize_list_cache_key(full) != cache_key:
                        continue
                    cfull = _canonicalize_url(full)
                    if cfull not in pages_seen and cfull not in pages_queued:
                        pages_to_visit.append(full)
                        pages_queued.add(cfull)
        except Exception:
            pass

    return views


async def expand_query_links_to_start_urls(query_urls: List[str]) -> List[str]:
    """
    query_links(list/page) -> view 筌띻낱寃뺞에??類ㅼ삢(best-effort).
    - ??쎈솭/??볦퍢?λ뜃?????癒?궚 query_urls 獄쏆꼹??
    """
    # region agent log
    _debug_log(
        location="backend/start_urls_preexpand.py:expand_query_links_to_start_urls:entry",
        message="preexpand entry",
        data={
            "query_urls_count": len(query_urls or []),
            "query_urls_sample": (query_urls or [])[:5],
        },
        hypothesis_id="H1",
    )
    # endregion
    if not query_urls:
        return []
    if not requests or not BeautifulSoup:
        return query_urls

    # 餓λ쵎????볤탢(??뽮퐣 ?醫?)
    ordered: List[str] = []
    seen0: set[str] = set()
    for u in query_urls:
        if not u:
            continue
        if u in seen0:
            continue
        seen0.add(u)
        ordered.append(u)

    # ?諭??野껊슣???URL 1揶???낆젾 ?? sitemap ??륁맄 筌띻낱寃뺟몴???釉??뺣뼄.
    ordered = _expand_single_list_url_with_sitemap(ordered)

    list_urls = [u for u in ordered if _is_list_page_url(u)]
    non_list_urls = [u for u in ordered if not _is_list_page_url(u)]
    if not list_urls:
        return ordered

    expanded_views: List[str] = []
    list_cache: Dict[str, List[str]] = {}
    html_cache: Dict[str, Optional[str]] = {}
    session = requests.Session() if requests else None
    t0 = time.time()

    # ?醫묓닔 ??곸겫 ??됱젟??
    # header_crawl??筌띾‘? query_links(??롪컶~??륁퓝)??獄쏆꼹???????됰선,
    # 筌뤴뫀諭?list URL????쀬돳??렽?view??沃섎챶???類ㅼ삢??롢늺 ?遺욧퍕??"筌롫뜆??野껉퍔荑?? 癰귣똻??????덈뼄.
    # ?怨뺤뵬??疫꿸퀡??첎誘れ몵嚥?(1) ????list 揶쏆뮇?? (2) ????볦퍢 ??됯텦???遺얜뼄.
    # - ?袁⑹뒄 ??ENV嚥??怨밸샨/??곸젫 揶쎛??    # - ?얜똻???筌뤴뫀諭?is_no_limits_mode)?癒?퐣???怨밸립????곸젫??뺣뼄.
    try:
        max_list_keys = int(os.getenv("START_URLS_PREEXPAND_MAX_LIST_KEYS", "30") or "30")
    except Exception:
        max_list_keys = 30
    try:
        total_budget_sec = float(os.getenv("START_URLS_PREEXPAND_TOTAL_BUDGET_SEC", "10") or "10")
    except Exception:
        total_budget_sec = 10.0
    if is_no_limits_mode():
        max_list_keys = 0
        total_budget_sec = 0.0
    try:
        per_list_timeout_sec = float(os.getenv("START_URLS_PREEXPAND_PER_LIST_TIMEOUT_SEC", "2.5") or "2.5")
    except Exception:
        per_list_timeout_sec = 2.5

    list_keys_in_order: List[str] = []
    key_to_url: Dict[str, str] = {}
    for lu0 in list_urls:
        k0 = _normalize_list_cache_key(lu0)
        if k0 in key_to_url:
            continue
        key_to_url[k0] = lu0
        list_keys_in_order.append(k0)

    limited_keys = list_keys_in_order
    if max_list_keys and max_list_keys > 0:
        limited_keys = list_keys_in_order[: max_list_keys]

    processed_lists = 0
    for _idx, key in enumerate(limited_keys, 1):
        if total_budget_sec and total_budget_sec > 0:
            if (time.time() - t0) >= total_budget_sec:
                break
        lu = key_to_url.get(key) or ""
        key = _normalize_list_cache_key(lu)
        if key in list_cache:
            expanded_views.extend(list_cache[key])
            processed_lists = _idx
            continue
        try:
            views = await asyncio.wait_for(
                _expand_list_to_views_router(lu, session=session, html_cache=html_cache),
                timeout=per_list_timeout_sec,
            )
        except asyncio.TimeoutError:
            views = []
        except Exception:
            views = []
        list_cache[key] = views
        expanded_views.extend(views)
        processed_lists = _idx
    if session is not None:
        try:
            session.close()
        except Exception:
            pass

    if expanded_views:
        final: List[str] = []
        seenf: set[str] = set()
        for u in (non_list_urls + expanded_views + list_urls):
            if not u:
                continue
            final_key = _preexpand_output_identity_key(u)
            if final_key in seenf:
                continue
            seenf.add(final_key)
            final.append(u)
        logging.getLogger("backend.shared.start_urls_preexpand").info(
            "[StartUrlsPreexpand] expanded | input=%s lists=%s unique_lists=%s processed_lists=%s expanded_views=%s final=%s elapsed_ms=%s budget_sec=%s per_list_timeout_sec=%s",
            len(query_urls or []),
            len(list_urls),
            len(list_keys_in_order),
            processed_lists,
            len(expanded_views),
            len(final),
            int((time.time() - t0) * 1000),
            total_budget_sec,
            per_list_timeout_sec,
        )
        return final

    # budget/?怨밸립??곗쨮 ?類ㅼ삢????쀫립??뤿?椰꾧퀡援?view??筌?筌≪뼚釉??겹늺 ?癒?궚??域밸챶?嚥?????
    logging.getLogger("backend.shared.start_urls_preexpand").info(
        "[StartUrlsPreexpand] no_expansion | input=%s lists=%s unique_lists=%s processed_lists=%s output=%s elapsed_ms=%s budget_sec=%s per_list_timeout_sec=%s",
        len(query_urls or []),
        len(list_urls),
        len(list_keys_in_order),
        processed_lists,
        len(ordered),
        int((time.time() - t0) * 1000),
        total_budget_sec,
        per_list_timeout_sec,
    )
    return ordered
