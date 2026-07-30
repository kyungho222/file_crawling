import sys, os
from urllib.parse import urlparse, parse_qsl, quote

sys.path.insert(0, os.getcwd())

from utils.url import canonicalize_url_for_dedup

# 복제된 필터 세트 (utils.url의 동작과 동일하게 유지)
TRACKING_KEYS = {
    "fbclid", "gclid", "msclkid", "igshid", "ysclid", "_ga", "_gl", "mc_cid", "mc_eid"
}
VOLATILE_KEYS = {
    "session", "sessionid", "sid", "phpsessid", "jsessionid", "jsessionid", "_csrf", "csrf", "token", "access_token", "refresh_token"
}
PAGE_AND_SEARCH_KEYS = {
    "buffer", "currentpage", "page", "pageindex", "pageno", "pagenum", "pg", "offset", "start", "startrow", "startindex",
    "limit", "row", "rows", "searchdata", "searchtext", "searchkeyword", "searchcondition", "searchtype", "searchorder",
    "searchstate", "sort", "order", "orderby", "sortorder", "viewtype", "view", "p", "search", "query", "keyword"
}

def url_to_tokens(u: str):
    """URL -> filtered (key,value) 토큰 리스트 반환 (빈값/추적/세션/페이지 키 제거)"""
    p = urlparse(u)
    pairs = parse_qsl(p.query or "", keep_blank_values=True)
    out = []
    seen = set()
    for k, v in pairs:
        kk = (k or "").strip().lower()
        vv = (v or "").strip()
        if not kk:
            continue
        if vv == "":
            continue
        if kk.startswith("utm_"):
            continue
        if kk in TRACKING_KEYS:
            continue
        if kk in VOLATILE_KEYS:
            continue
        if kk in PAGE_AND_SEARCH_KEYS:
            continue
        if (kk, vv) in seen:
            continue
        seen.add((kk, vv))
        out.append((kk, vv))
    out.sort(key=lambda x: (x[0], x[1]))
    return out


urls = [
    "https://www.yuc.co.kr/www/brd/m_434/view.do?company_cd=&company_nm=&itm_seq_1=0&itm_seq_2=0&multi_itm_seq=0&seq=16494&srchFr=&srchTo=&srchTp=&srchWord=",
    "https://www.yuc.co.kr/www/brd/m_434/view.do?seq=16494",
    "https://www.yuc.co.kr/www/brd/m_434/view.do?seq=16494&srchFr=&srchTo=&srchWord=&srchTp=&multi_itm_seq=0&itm_seq_1=0&itm_seq_2=0&company_cd=&company_nm=&page=2",
    "https://www.yuc.co.kr/www/brd/m_434/view.do?company_cd=&company_nm=&itm_seq_1=0&itm_seq_2=0&multi_itm_seq=0&page=1&seq=16494&srchFr=&srchTo=&srchTp=&srchWord=",
]

for u in urls:
    tokens = url_to_tokens(u)
    token_set = set(tokens)

    normalized = canonicalize_url_for_dedup(u)

    norm_query = urlparse(normalized).query

    token_key = "&".join([f"{k}={quote(v, safe='')}" for k, v in tokens])

    base = normalized.split("?", 1)[0]
    constructed = base + ("?" + token_key if token_key else "")
