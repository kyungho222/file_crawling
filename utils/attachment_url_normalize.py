"""
LEARN_LIST.content·파일 크롤 첨부 URL 전용 정규화.

portal/health·menuNo 등으로만 달라지는 동일 파일을 한 키로 묶기 위해
`canonicalize_url_for_dedup` 이후 추가 규칙을 적용한다.
"""

from __future__ import annotations

from posixpath import normpath
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlunparse

from utils.url import (
    canonicalize_url_for_dedup,
    _MULTI_SLASH_RE,
    _PAGE_AND_SEARCH_KEYS,
    _TRACKING_KEYS_EXACT_LOWER,
    _VOLATILE_KEYS_EXACT_LOWER,
)

_ATTACHMENT_NAV_STRIP_KEYS = frozenset(
    {
        "menumo",
        "menu_no",
        "menuno",
        "menucd",
        "menu_cd",
        "lmenu_cd",
        "lmenumcd",
        "topmenu",
        "submenu",
        "tabid",
        "tab_id",
        "link",
        "mid",
        "m_id",
        "pageunit",
        "pagesize",
        "recordcountperpage",
        "pageroffset",
        "searchword",
        "searchwrd",
        "search_ctg",
        "searchdctg",
    }
)

_ATTACHMENT_ID_QUERY_KEYS = frozenset(
    {
        "sys_file_nm",
        "atchmnflno",
        "atchfileid",
        "filesn",
    }
)


def canonicalize_attachment_url_for_learn_list(url: str, base_url: str | None = None) -> str:
    """
    LEARN_LIST.content 및 파일 크롤 중복 판단 전용 정규화.

    - 먼저 canonicalize_url_for_dedup(트래킹·페이지네이션 제거 등) 적용
    - 포털/보건 등에서 붙는 menuNo·pageUnit 등 네비 파라미터 추가 제거
    - 전자정부식 atchFileId + fileSn 이 있으면 경로를 /cmm/fms/FileDown.do 로 통일하고 해당 쿼리만 유지
    - 다운로드 성격 URL에서만 경로 첫 세그먼트 portal|health|main|web|site 제거 시도
    """
    u = canonicalize_url_for_dedup(url, base_url)
    if not u:
        return ""

    try:
        p = urlparse(u)
    except Exception:
        return u

    scheme = (p.scheme or "https").lower()
    netloc = (p.netloc or "").lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]

    path = normpath(p.path or "/")
    path = _MULTI_SLASH_RE.sub("/", path)
    pl = path.lower()
    if netloc.endswith("gachi.chungbuk.go.kr") and "/portal/cmmn/file/filedown.do" in pl:
        return urlunparse((scheme, netloc, path, "", p.query or "", ""))
    looks_file = (
        "filedown" in pl
        or "/fms/" in pl
        or "/cmm/" in pl
        or "/cmmn/" in pl
        or "/file" in pl
        or "download" in pl
        or "attach" in pl
    )
    parts = [x for x in path.split("/") if x]
    if looks_file and parts and parts[0].lower() in ("portal", "health", "main", "web", "site"):
        parts = parts[1:]
        path = "/" + "/".join(parts) if parts else "/"
        path = normpath(path)
        path = _MULTI_SLASH_RE.sub("/", path)
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    path = quote(unquote(path), safe="/")

    pairs = parse_qsl(p.query or "", keep_blank_values=False)
    kept: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for k, v in pairs:
        kk = (k or "").strip().lower()
        vv = (v or "").strip()
        if not kk or not vv:
            continue
        if kk in _ATTACHMENT_NAV_STRIP_KEYS:
            continue
        if kk.startswith("utm_"):
            continue
        if kk in _TRACKING_KEYS_EXACT_LOWER:
            continue
        if kk in _VOLATILE_KEYS_EXACT_LOWER:
            continue
        if kk in _PAGE_AND_SEARCH_KEYS:
            continue
        if (kk, vv) in seen:
            continue
        seen.add((kk, vv))
        kept.append((k.strip(), vv))

    by_l = {(a or "").lower(): b for a, b in kept}
    atch_v = by_l.get("atchfileid")
    sn_v = by_l.get("filesn")
    if atch_v is not None and sn_v is not None and str(atch_v).strip() and str(sn_v).strip():
        narrow = [("atchFileId", str(atch_v).strip()), ("fileSn", str(sn_v).strip())]
        query = urlencode(narrow, doseq=True)
        file_path = "/cmm/fms/FileDown.do"
        return urlunparse((scheme, netloc, file_path, "", query, ""))

    kept.sort(key=lambda x: (x[0].lower(), x[1]))
    query = urlencode(kept, doseq=True)
    return urlunparse((scheme, netloc, path, "", query, ""))


def extract_sys_file_nm_from_attachment_url(url: str | None) -> str | None:
    """
    첨부 다운로드 URL 쿼리에서 `sys_file_nm` 값을 추출한다(예: e-minwon FileDownNew.jsp).

    게시글이 달라도 동일 저장 파일이면 서버측 식별자는 보통 동일하므로, content 전체
    정규화 일치가 실패할 때 LEARN_LIST `content` LIKE 사전 중복 판별에 사용한다.
    """
    if not url or not isinstance(url, str):
        return None
    try:
        p = urlparse(url.strip())
        for k, v in parse_qsl(p.query or "", keep_blank_values=False):
            if (k or "").strip().lower() == "sys_file_nm" and (v or "").strip():
                return unquote(v.strip())
    except Exception:
        return None
    return None


def extract_attachment_key_candidates(url: str | None) -> list[str]:
    """
    해시 없이 첨부 URL 안의 비교 가능한 안정 키만 뽑는다.

    같은 파일인데 게시글/메뉴 파라미터만 달라지는 경우를 잡기 위한 보조 식별자다.
    """
    if not url or not isinstance(url, str):
        return []

    try:
        p = urlparse(url.strip())
        pairs = parse_qsl(p.query or "", keep_blank_values=False)
    except Exception:
        return []

    grouped: dict[str, list[str]] = {}
    for k, v in pairs:
        kk = (k or "").strip().lower()
        vv = unquote((v or "").strip())
        if not kk or not vv or kk not in _ATTACHMENT_ID_QUERY_KEYS:
            continue
        grouped.setdefault(kk, []).append(vv)

    out: list[str] = []
    seen: set[str] = set()

    def _add(val: str, *, min_len: int = 4) -> None:
        s = str(val or "").strip()
        if len(s) < min_len:
            return
        key = s.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(s)

    for val in grouped.get("sys_file_nm", []):
        _add(val, min_len=4)
    for val in grouped.get("atchmnflno", []):
        _add(val, min_len=4)
    atch_ids = grouped.get("atchfileid", [])
    file_sns = grouped.get("filesn", [])
    if atch_ids and file_sns:
        for atch_id in atch_ids:
            for file_sn in file_sns:
                _add(f"atchFileId={atch_id}&fileSn={file_sn}", min_len=8)
    else:
        for val in atch_ids:
            _add(val, min_len=6)

    return out


def sql_like_contains_pattern(substring: str) -> str:
    """
    `WHERE content LIKE %s ESCAPE '!'` 와 함께 쓸 `%…%` 부분 일치 패턴.
    `!`, `%`, `_` 는 LIKE 와일드카드가 아니도록 `!` 이스케이프로 처리한다
    (백슬래시는 Python/SQL 문자열·f-string과 충돌하므로 사용하지 않음).
    """
    if not substring:
        return "%"
    s = str(substring).replace("!", "!!").replace("%", "!%").replace("_", "!_")
    return "%" + s + "%"
