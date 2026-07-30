import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse, urlunparse

from backend.shared.board_header import CrawlResponse
from backend.shared.start_urls_preexpand import expand_query_links_to_start_urls
from backend.shared.url_pattern_identity import canonical_url_key
from utils.url import ensure_url_scheme

logger = logging.getLogger("backend.shared.start_urls_generation")


@dataclass
class StartUrlsResolution:
    start_urls: List[Any]
    use_query_links_only: bool = False
    override_source: str = ""


def _normalize_start_url_items(items: List[Any]) -> List[Any]:
    normalized: List[Any] = []
    seen: set[str] = set()
    for item in items or []:
        if not item:
            continue
        if isinstance(item, dict):
            target_url = item.get("url", "")
            if not target_url:
                continue
            normalized_url = _ensure_seed_url_scheme(target_url)
            dedupe_key = _start_url_identity_key(normalized_url)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            item_copy = dict(item)
            item_copy["url"] = normalized_url
            normalized.append(item_copy)
            continue
        normalized_url = _ensure_seed_url_scheme(item)
        if normalized_url:
            dedupe_key = _start_url_identity_key(normalized_url)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            normalized.append(normalized_url)
    return normalized


def normalize_known_start_url_alias(url: Any) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").rstrip("/")
        if host.endswith("gm.go.kr") and path == "/pt/disclosure/bidContractInfo/contractInfo":
            return urlunparse(
                (
                    parsed.scheme or "https",
                    parsed.netloc or "www.gm.go.kr",
                    "/pt/disclosure/bidContractInfo/contractInfo/contractList.do",
                    "",
                    "q_optionalYn=N",
                    "",
                )
            )
        if host.endswith("gm.go.kr") and path.endswith("/pt/user/bbs/BD_selectBbs.do"):
            query = parse_qs(parsed.query or "", keep_blank_values=True)
            has_board_code = bool(query.get("q_bbsCode") or query.get("q_bbscode"))
            has_post_id = bool(query.get("q_bbscttSn") or query.get("q_bbscttsn"))
            if has_board_code and not has_post_id:
                return urlunparse(
                    (
                        parsed.scheme or "https",
                        parsed.netloc or "www.gm.go.kr",
                        path[: -len("BD_selectBbs.do")] + "BD_selectBbsList.do",
                        "",
                        parsed.query,
                        "",
                    )
                )
        if path.endswith("selectBbsNttView.do"):
            query = parse_qs(parsed.query or "", keep_blank_values=True)
            has_board_code = bool(query.get("bbsNo") or query.get("bbsno"))
            has_post_id = bool(query.get("nttNo") or query.get("nttno"))
            if has_board_code and not has_post_id:
                return urlunparse(
                    (
                        parsed.scheme or "https",
                        parsed.netloc,
                        path[: -len("selectBbsNttView.do")] + "selectBbsNttList.do",
                        "",
                        parsed.query,
                        "",
                    )
                )
    except Exception:
        return text
    return text


def _ensure_seed_url_scheme(url: Any) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    if re.match(r"^https?://", text, flags=re.IGNORECASE):
        return normalize_known_start_url_alias(text)
    return normalize_known_start_url_alias(ensure_url_scheme(text))


def _start_url_identity_key(url: str) -> str:
    """Deduplicate equivalent detail URLs with paging/search/list query noise removed."""
    return canonical_url_key(url) or str(url or "").strip().lower().rstrip("/")


def _header_link_labels_by_url(header_response: CrawlResponse) -> Dict[str, str]:
    try:
        board_list_links = list(getattr(header_response, "board_list_links", []) or [])
    except Exception:
        board_list_links = []

    label_by_url: Dict[str, str] = {}
    for link in board_list_links:
        try:
            raw_link_url = ensure_url_scheme(getattr(link, "url", "") or "")
        except Exception:
            raw_link_url = ""
        if not raw_link_url:
            continue
        try:
            raw_label = str(getattr(link, "label", "") or "").strip()
        except Exception:
            raw_label = ""
        if raw_label:
            label_by_url[raw_link_url] = raw_label
    return label_by_url


def _with_title_hint(url: str, title_hint: str) -> Any:
    if title_hint:
        return {"url": url, "title": title_hint, "subject": title_hint}
    return url


def _item_url(item: Any) -> str:
    try:
        if isinstance(item, dict):
            return str(item.get("url") or "").strip()
        return str(item or "").strip()
    except Exception:
        return ""


def _preserve_header_list_urls() -> bool:
    try:
        return str(os.getenv("BOARD_CONTENT_PRESERVE_LIST_URLS_FROM_HEADER", "1")).strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
    except Exception:
        return True


async def _resolve_override_start_urls(data: Dict[str, Any], job_id: str) -> Optional[StartUrlsResolution]:
    override_payload = data.get("start_urls_override")
    if not isinstance(override_payload, list) or not override_payload:
        return None

    try:
        override_source = str(data.get("start_urls_override_source") or "").strip()
    except Exception:
        override_source = ""

    normalized_override = _normalize_start_url_items(override_payload)
    if not normalized_override:
        return None

    (logger.debug if override_source in ("file_crawl_post_db", "file_crawl_post_db_stream") else logger.info)(
        "[StartUrlsResolver] override resolved | job_id=%s source=%s count=%s sample=%s",
        job_id,
        override_source or "override",
        len(normalized_override),
        normalized_override[:3],
    )
    partial_content_relearn_enabled = str(
        os.getenv("PARTIAL_CONTENT_RELEARN_ENABLED", "0") or "0"
    ).strip().lower() in {"1", "true", "yes", "on", "y"}
    if override_source == "partial_content_relearn" and not partial_content_relearn_enabled:
        logger.info(
            "[PartialContent][Blocked] dispatch override ignored | job_id=%s count=%s",
            job_id,
            len(normalized_override),
        )
        return None
    if override_source == "sitemap_board_list":
        data["board_list_urls"] = normalized_override
        return StartUrlsResolution(normalized_override, use_query_links_only=True, override_source=override_source)
    if override_source == "sitemap_board":
        try:
            expanded = await expand_query_links_to_start_urls(normalized_override)
            start_urls = expanded if expanded else normalized_override
        except Exception:
            start_urls = normalized_override
        return StartUrlsResolution(start_urls, use_query_links_only=True, override_source=override_source)
    return StartUrlsResolution(normalized_override, override_source=override_source)


async def _resolve_board_list_start_urls(header_response: CrawlResponse, data: Dict[str, Any]) -> Optional[StartUrlsResolution]:
    try:
        board_list_urls = list(header_response.board_list_urls or [])
    except Exception:
        board_list_urls = []
    if not board_list_urls:
        return None

    normalized_board_list_urls: List[str] = []
    normalized_board_start_urls: List[Any] = []
    label_by_url = _header_link_labels_by_url(header_response)
    for raw_url in board_list_urls:
        normalized_url = ensure_url_scheme(raw_url)
        normalized_board_list_urls.append(normalized_url)
        normalized_board_start_urls.append(_with_title_hint(normalized_url, label_by_url.get(normalized_url, "")))

    if _preserve_header_list_urls():
        start_urls = normalized_board_start_urls
    else:
        try:
            expanded = await expand_query_links_to_start_urls(normalized_board_list_urls)
            start_urls = expanded if expanded else normalized_board_list_urls
        except Exception:
            start_urls = normalized_board_list_urls
    data["board_list_urls"] = normalized_board_list_urls
    return StartUrlsResolution(start_urls, use_query_links_only=True)


async def _resolve_query_link_start_urls(header_response: CrawlResponse) -> Optional[StartUrlsResolution]:
    try:
        query_links = list(header_response.query_links or [])
    except Exception:
        query_links = []
    if not query_links:
        return None

    normalized_query_urls: List[str] = []
    normalized_query_start_urls: List[Any] = []
    for link in query_links:
        raw_url = getattr(link, "url", None)
        if not raw_url:
            continue
        normalized_url = ensure_url_scheme(raw_url)
        normalized_query_urls.append(normalized_url)
        try:
            title_hint = str(getattr(link, "label", "") or "").strip()
        except Exception:
            title_hint = ""
        normalized_query_start_urls.append(_with_title_hint(normalized_url, title_hint))

    if _preserve_header_list_urls():
        start_urls = normalized_query_start_urls
    else:
        try:
            expanded = await expand_query_links_to_start_urls(normalized_query_urls)
            start_urls = expanded if expanded else normalized_query_urls
        except Exception:
            start_urls = normalized_query_urls
    return StartUrlsResolution(start_urls, use_query_links_only=True)


def filter_start_urls_by_content_board_id(
    start_urls: List[Any],
    contents: Any,
    *,
    job_id: str = "",
) -> tuple[List[Any], bool]:
    if not start_urls or not contents:
        return start_urls, False
    try:
        target_item = contents[0] if isinstance(contents, list) and contents else contents
        target_url = _item_url(target_item)
        m_target = re.search(r"/(?:bbs|board)/([a-zA-Z0-9_]+)", target_url, re.IGNORECASE)
        if not m_target:
            return start_urls, False

        target_bid = m_target.group(1)
        logger.info("[Dispatch] Target BID determined: %s (from %s)", target_bid, target_url)

        filtered_urls: List[Any] = []
        filtered_seen: set[str] = set()
        extracted_bids = set()
        for item in start_urls:
            item_url = _item_url(item)
            m_item = re.search(r"/(?:bbs|board)/([a-zA-Z0-9_]+)", item_url, re.IGNORECASE)
            include_item = False
            if m_item:
                bid = m_item.group(1)
                extracted_bids.add(bid)
                if bid.lower() == target_bid.lower():
                    include_item = True
            else:
                include_item = True
            if include_item:
                dedupe_key = _start_url_identity_key(ensure_url_scheme(item_url))
                if dedupe_key in filtered_seen:
                    continue
                filtered_seen.add(dedupe_key)
                filtered_urls.append(item)

        if not filtered_urls:
            logger.warning(
                "[Dispatch] All start_urls filtered out by board_id (no match for %s); no main-page fallback | job_id=%s",
                target_bid,
                job_id,
            )
            return [], True

        if len(filtered_urls) < len(start_urls):
            logger.info(
                "[Dispatch] Filtered start_urls by board_id | target=%s before=%s after=%s removed_bids=%s",
                target_bid,
                len(start_urls),
                len(filtered_urls),
                list(extracted_bids - {target_bid}),
            )

        target_url_norm = ensure_url_scheme(target_url)
        target_identity_key = _start_url_identity_key(target_url_norm)
        target_path = (urlparse(target_url_norm).path or "").lower()
        target_is_list = target_path.endswith(("list.do", "list.asp", "list.jsp"))
        if not target_is_list:
            is_present = False
            for x in filtered_urls:
                x_url = _item_url(x)
                if x_url and _start_url_identity_key(ensure_url_scheme(x_url)) == target_identity_key:
                    is_present = True
                    break
            if not is_present:
                filtered_urls.insert(0, target_url_norm)
        return filtered_urls, False
    except Exception as exc:
        logger.exception("[Dispatch] Failed to filter start_urls by board_id: %s", exc)
        return start_urls, False


async def resolve_start_urls(
    data: Dict[str, Any],
    *,
    contents: Any,
    job_id: str,
    header_response: Optional[CrawlResponse] = None,
) -> StartUrlsResolution:
    initial_override_source = ""
    if isinstance(data.get("start_urls_override"), list) and data.get("start_urls_override"):
        try:
            initial_override_source = str(data.get("start_urls_override_source") or "").strip()
        except Exception:
            initial_override_source = ""

    resolution = await _resolve_override_start_urls(data, job_id)
    if resolution is None and header_response and getattr(header_response, "board_list_urls", None):
        resolution = await _resolve_board_list_start_urls(header_response, data)
    if resolution is None and header_response and getattr(header_response, "query_links", None):
        resolution = await _resolve_query_link_start_urls(header_response)
    if resolution is None:
        resolution = StartUrlsResolution([], override_source=initial_override_source)
    elif initial_override_source and not resolution.override_source:
        resolution.override_source = initial_override_source

    filtered_start_urls, all_filtered = filter_start_urls_by_content_board_id(
        resolution.start_urls,
        contents,
        job_id=job_id,
    )
    if all_filtered:
        return StartUrlsResolution(
            filtered_start_urls,
            use_query_links_only=False,
            override_source=resolution.override_source,
        )
    resolution.start_urls = filtered_start_urls
    return resolution


__all__ = [
    "StartUrlsResolution",
    "filter_start_urls_by_content_board_id",
    "resolve_start_urls",
    "_normalize_start_url_items",
    "normalize_known_start_url_alias",
    "_start_url_identity_key",
]
