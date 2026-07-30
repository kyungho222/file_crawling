from __future__ import annotations

from typing import Dict, Any, Optional
from urllib.parse import parse_qsl, urlparse
import hashlib

from utils.hash_policy import hash_generation_disabled
from utils.url import canonicalize_url_for_dedup


def _extract_id_from_item(it: Dict[str, Any]) -> Optional[str]:
    if not it:
        return None
    meta = it.get("original_meta") if isinstance(it.get("original_meta"), dict) else {}
    for k in ("id", "article_id", "post_id", "nttid", "num"):
        v = meta.get(k) or it.get(k)
        if v:
            return str(v)
    try:
        p = urlparse(it.get("url") or "")
        q = dict(parse_qsl(p.query or "", keep_blank_values=True))
        for k in ("nttid", "num", "id", "article_id"):
            if k in q and q.get(k):
                return str(q.get(k))
    except Exception:
        pass
    return None


def _host_path_of_canon(canon: str) -> str:
    try:
        p = urlparse(canon or "")
        host = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/")
        return f"{host}{path}"
    except Exception:
        return canon or ""


def _simple_fingerprint(it: Dict[str, Any]) -> str:
    s = (it.get("name") or "") + "|" + (it.get("source_page") or "") + "|" + (str(it.get("size") or ""))
    s = s.strip().lower()
    if hash_generation_disabled():
        return s[:8192] if len(s) > 8192 else s
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def make_seen_state() -> Dict[str, Any]:
    return {"raw_urls": set(), "by_id": {}, "by_canon": {}, "processed": []}


def is_duplicate_and_record(item: Dict[str, Any], seen_state: Dict[str, Any]) -> bool:
    """
    Decide if item is duplicate based on ID + canonical URL scoring.
    If considered new, record into seen_state.
    """
    if not item or not seen_state:
        return False

    url = item.get("url")
    if not url:
        return True

    raw_urls = seen_state.setdefault("raw_urls", set())
    by_id = seen_state.setdefault("by_id", {})
    by_canon = seen_state.setdefault("by_canon", {})
    processed = seen_state.setdefault("processed", [])

    # cheap raw URL dedupe
    if url in raw_urls:
        return True

    item_id = _extract_id_from_item(item)
    try:
        canon = canonicalize_url_for_dedup(url) or url
    except Exception:
        canon = url

    # immediate id match
    if item_id and item_id in by_id:
        return True

    # exact canonical match
    if canon in by_canon:
        return True

    # scoring against existing entries
    W_ID = 0.7
    W_URL = 0.3
    DUPLICATE_THRESHOLD = 0.8
    DISTINCT_THRESHOLD = 0.4

    best = 0.0
    item_hp = _host_path_of_canon(canon)
    for s_canon, s_url in by_canon.items():
        # find s_id for s_url
        s_id = None
        for k, v in by_id.items():
            if v == s_url:
                s_id = k
                break
        id_score = 1.0 if (item_id and s_id and item_id == s_id) else 0.0
        if s_canon == canon:
            url_score = 1.0
        elif _host_path_of_canon(s_canon) == item_hp:
            url_score = 0.8
        else:
            url_score = 0.0
        combined = W_ID * id_score + W_URL * url_score
        if combined > best:
            best = combined

    if best >= DUPLICATE_THRESHOLD:
        return True
    if best <= DISTINCT_THRESHOLD:
        # accept as new
        raw_urls.add(url)
        if item_id:
            by_id[item_id] = url
        by_canon[canon] = url
        processed.append(item)
        return False

    # ambiguous: lightweight fingerprint
    fp_new = _simple_fingerprint(item)
    for p in processed:
        if _simple_fingerprint(p) == fp_new:
            return True

    # treat as new
    raw_urls.add(url)
    if item_id:
        by_id[item_id] = url
    by_canon[canon] = url
    processed.append(item)
    return False

