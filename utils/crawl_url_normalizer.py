from __future__ import annotations

import posixpath
import re
from typing import Any
from urllib.parse import parse_qsl, quote, unquote, urlencode, urljoin, urlparse, urlunparse


CRAWL_URL_PAGE_QUERY_KEYS = {
    "pageindex",
    "pageno",
    "pgno",
    "page",
    "curpage",
    "currpage",
    "q_currpage",
    "page_no",
    "page_index",
    "pagenum",
    "pageunit",
    "pagesize",
    "recordcountperpage",
    "row",
    "rows",
    "searchcnd",
    "searchwrd",
    "searchcategory",
    "searchctg",
    "searchctgry",
    "searchctgory",
    "start",
    "offset",
}

CRAWL_URL_TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "msclkid",
    "igshid",
    "ysclid",
    "_ga",
    "_gl",
    "mc_cid",
    "mc_eid",
}

CRAWL_URL_VOLATILE_QUERY_KEYS = {
    "session",
    "sessionid",
    "sid",
    "phpsessid",
    "jsessionid",
    "_csrf",
    "cachebust",
    "cachebuster",
    "cb",
    "csrf",
    "device",
    "from",
    "ref",
    "referrer",
    "returnurl",
    "return_url",
    "sessionkey",
    "timestamp",
    "token",
    "access_token",
    "refresh_token",
    "tracking",
    "trackingid",
    "browser",
    "resolution",
    "screen",
    "screensize",
    "viewport",
    "ts",
    "userid",
}


def canonicalize_crawl_url(url: Any, base_url: str | None = None) -> str:
    """
    Common crawling URL normalization for board/file/etc. dedupe keys.

    Rules:
    - lowercase normalized URL
    - normalize dot/double-slash path segments
    - remove default ports
    - remove trailing slash
    - remove tracking, session, and paging query parameters
    - remove fragment
    - sort query pairs
    """
    raw = str(url or "").strip()
    if not raw:
        return ""
    if raw.startswith(("javascript:", "mailto:", "#")):
        return ""
    if raw.startswith("//"):
        raw = "https:" + raw
    if not re.match(r"^https?://", raw, flags=re.IGNORECASE):
        if base_url:
            raw = urljoin(base_url, raw)
        else:
            raw = "https://" + raw.lstrip("/")
    raw = re.sub(r";jsessionid=[^/?#]+", "", raw, flags=re.IGNORECASE)
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw.lower().rstrip("/")

    scheme = (parsed.scheme or "https").lower()
    netloc = (parsed.netloc or "").lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    if scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    elif scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    path = unquote(parsed.path or "/").lower()
    path = posixpath.normpath(path)
    if not path.startswith("/"):
        path = "/" + path
    path = quote(path, safe="/")
    path = re.sub(r"/{2,}", "/", path)
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    pairs = []
    for key, value in parse_qsl(parsed.query or "", keep_blank_values=False):
        key_text = str(key or "").strip().lower()
        if not key_text:
            continue
        if (
            key_text.startswith("utm_")
            or key_text in CRAWL_URL_TRACKING_QUERY_KEYS
            or key_text in CRAWL_URL_VOLATILE_QUERY_KEYS
            or key_text in CRAWL_URL_PAGE_QUERY_KEYS
        ):
            continue
        pairs.append((key_text, str(value or "").strip().lower()))
    pairs.sort(key=lambda item: (item[0], item[1]))
    query = urlencode(pairs, doseq=True)
    return urlunparse((scheme, netloc, path, "", query, "")).rstrip("/")
