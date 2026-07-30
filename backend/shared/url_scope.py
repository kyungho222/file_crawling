from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional
from urllib.parse import urlparse


def scope_include_scheme() -> bool:
    try:
        v = str(os.getenv("CRAWL_SCOPE_INCLUDE_SCHEME", "0") or "0").strip().lower()
    except Exception:
        v = "0"
    return v in ("1", "true", "yes", "on")


def _coerce_scope_source(raw: Any) -> Any:
    if isinstance(raw, (list, tuple, set)):
        for item in raw:
            txt = str(item or "").strip()
            if txt:
                return item
        return ""
    return raw


def _as_parseable_url(raw: Any) -> str:
    raw = _coerce_scope_source(raw)
    txt = str(raw or "").strip()
    if not txt:
        return ""
    if "://" not in txt:
        if txt.startswith("//"):
            return f"https:{txt}"
        return f"https://{txt}"
    return txt


def _canonicalize_scope_host(host: Any) -> str:
    txt = str(host or "").strip().lower()
    if txt.startswith("www."):
        return txt[4:]
    return txt


def _is_jongno_host(host: Any) -> bool:
    return _canonicalize_scope_host(host) == "jongno.go.kr"


def _is_jongno_council_host(host: Any) -> bool:
    return _canonicalize_scope_host(host) == "council.jongno.go.kr"


_JONGNO_MAIN_PATH_ALIASES = {
    ("jongno.go.kr", "/mayormain.do"): ("/mayor/", "/mayorMain.do"),
    ("jongno.go.kr", "/portalmain.do"): ("/portal/", "/portalMain.do"),
    ("jongno.go.kr", "/healthmain.do"): ("/health/", "/healthMain.do"),
    ("jongno.go.kr", "/reserv/main.do"): ("/reserv/", "/reserv/main.do"),
    ("council.jongno.go.kr", "/council/main.do"): ("/council/", "/council/main.do"),
}

_SCOPE_HOST_ALIAS_GROUPS = (
    frozenset({"gangdong.go.kr", "gdfac.or.kr"}),
)

_PREFERRED_SCOPE_PATH_ALIASES = {
    ("dongjak.go.kr", "/portal/main/main.do"): ("/portal/bbs/", "/portal/"),
}

_ROOT_SERVICE_ENTRY_ALIASES = {
    "portalmain.do": ("/portal/", "portal"),
    "healthmain.do": ("/health/", "health"),
    "mayormain.do": ("/mayor/", "mayor"),
}

_ROOT_ENTRY_FILENAMES = frozenset(
    {
        "index.do",
        "index.html",
        "index.htm",
        "main.do",
        "main.html",
        "main.htm",
        *tuple(_ROOT_SERVICE_ENTRY_ALIASES.keys()),
    }
)


@dataclass(frozen=True)
class ScopeIdentity:
    host: str
    service_prefix: str = ""
    entry_type: str = ""


def _normalized_url_path(path: Any) -> str:
    txt = str(path or "").strip().lower()
    if not txt.startswith("/"):
        txt = "/" + txt
    return txt.rstrip("/") or "/"


def _jongno_main_path_alias(host: Any, path: Any) -> str:
    alias = _JONGNO_MAIN_PATH_ALIASES.get(
        (_canonicalize_scope_host(host), _normalized_url_path(path)),
        ("", ""),
    )
    return alias[0]


def _jongno_main_paths_for_prefix(host: Any, path_prefix: str) -> List[str]:
    canonical_host = _canonicalize_scope_host(host)
    return [
        alias_path
        for (alias_host, _path), (alias_prefix, alias_path) in _JONGNO_MAIN_PATH_ALIASES.items()
        if alias_host == canonical_host and alias_prefix == path_prefix
    ]


def _jongno_scope_path_alias(raw: Any) -> str:
    """
    Jongno's mayor site enters through /mayorMain.do while detail URLs live below /mayor/.
    Keep this alias local to jongno.go.kr so generic single-file paths still behave as root scope.
    """
    try:
        parsed = urlparse(_as_parseable_url(raw))
    except Exception:
        return ""
    return _jongno_main_path_alias(parsed.hostname, parsed.path)


def _preferred_scope_path_alias(raw: Any) -> str:
    try:
        parsed = urlparse(_as_parseable_url(raw))
    except Exception:
        return ""
    alias = _PREFERRED_SCOPE_PATH_ALIASES.get(
        (_canonicalize_scope_host(parsed.hostname), _normalized_url_path(parsed.path)),
        ("", ""),
    )
    return alias[0]


def fallback_scope_path_prefixes(raw: Any, primary_prefix: Any = "") -> List[str]:
    try:
        parsed = urlparse(_as_parseable_url(raw))
    except Exception:
        return []
    aliases = _PREFERRED_SCOPE_PATH_ALIASES.get(
        (_canonicalize_scope_host(parsed.hostname), _normalized_url_path(parsed.path)),
        ("", ""),
    )
    normalized_primary = normalize_scope_path_prefix(primary_prefix)
    out: List[str] = []
    for candidate in aliases[1:]:
        normalized = normalize_scope_path_prefix(candidate)
        if normalized and normalized != normalized_primary and normalized not in out:
            out.append(normalized)
    return out


def extract_scope_host(raw: Any) -> str:
    try:
        parsed = urlparse(_as_parseable_url(raw))
        return _canonicalize_scope_host(parsed.hostname)
    except Exception:
        return ""


def extract_scope_origin(raw: Any) -> str:
    try:
        parsed = urlparse(_as_parseable_url(raw))
        host = _canonicalize_scope_host(parsed.hostname)
        if not host:
            return ""
        scheme = str(parsed.scheme or "https").strip().lower()
        return f"{scheme}://{host}"
    except Exception:
        return ""


def normalize_scope_path_prefix(path_prefix: Any) -> str:
    try:
        raw = str(path_prefix or "").strip()
    except Exception:
        raw = ""
    if not raw:
        return ""
    try:
        if "://" in raw or raw.startswith("//"):
            raw = str(urlparse(_as_parseable_url(raw)).path or "").strip() or "/"
    except Exception:
        pass
    while "//" in raw:
        raw = raw.replace("//", "/")
    if not raw.startswith("/"):
        raw = "/" + raw
    if raw == "/":
        return "/"
    return raw.rstrip("/") + "/"


def scope_path_prefix_enabled(data: Any) -> bool:
    if not isinstance(data, dict):
        return True
    enabled_raw = data.get("scope_path_prefix_enabled")
    if enabled_raw is None:
        enabled_raw = data.get("start_urls_prefix_enabled")
    if enabled_raw is None:
        enabled_raw = data.get("apply_scope_path_prefix")
    if enabled_raw is None:
        enabled_raw = data.get("use_scope_path_prefix")
    return str(enabled_raw).strip().lower() not in {"0", "false", "no", "off", "n"}


def extract_scope_path_prefix(raw: Any) -> str:
    """
    URL 범위를 호스트보다 한 단계 더 좁힐 때 쓰는 경로 prefix를 반환한다.

    현재 규칙은 "도메인 뒤 첫 번째 path segment" 까지다.
    예:
    - https://www.yongin.go.kr/water/wttnkManage/BD_select... -> /water/
    - https://example.go.kr/web/bbs/list.do -> /web/

    루트 바로 아래 단일 파일형 경로(`/list.do`)는 과도한 누락을 피하려고
    기존처럼 호스트 전체 범위(`/`)로 유지한다.
    """
    alias = _jongno_scope_path_alias(raw)
    if alias:
        return alias
    alias = _preferred_scope_path_alias(raw)
    if alias:
        return alias

    try:
        parsed = urlparse(_as_parseable_url(raw))
        path = str(parsed.path or "").strip()
    except Exception:
        path = ""
    if not path or path == "/":
        return ""
    while "//" in path:
        path = path.replace("//", "/")
    parts = [part for part in path.split("/") if part]
    if not parts:
        return ""
    first = str(parts[0] or "").strip()
    if not first:
        return ""
    if len(parts) == 1 and "." in first:
        return "/"
    return f"/{first}/"


def extract_service_scope(raw: Any) -> ScopeIdentity:
    """
    Return the service-level URL scope used for selecting seed candidates.

    The scope is intentionally stricter than host-only matching, but broader
    than a detail page directory. For example:
    - /reserv/main.do -> /reserv/
    - /portalMain.do -> /portal/
    - /NGLMS/1336/highView -> /NGLMS/
    """
    try:
        parsed = urlparse(_as_parseable_url(raw))
    except Exception:
        return ScopeIdentity(host="")

    host = _canonicalize_scope_host(parsed.hostname)
    path = str(parsed.path or "").strip()
    while "//" in path:
        path = path.replace("//", "/")
    parts = [str(part or "").strip() for part in path.split("/") if str(part or "").strip()]

    alias = _jongno_main_path_alias(host, path) or _preferred_scope_path_alias(raw)
    if alias:
        return ScopeIdentity(host=host, service_prefix=normalize_scope_path_prefix(alias), entry_type="main")

    if not parts:
        return ScopeIdentity(host=host)

    filename = parts[0].lower()
    root_alias = _ROOT_SERVICE_ENTRY_ALIASES.get(filename)
    if root_alias:
        prefix, entry_type = root_alias
        return ScopeIdentity(host=host, service_prefix=normalize_scope_path_prefix(prefix), entry_type=entry_type)

    first = parts[0]
    first_lower = first.lower()
    if first_lower in _ROOT_ENTRY_FILENAMES:
        return ScopeIdentity(host=host, entry_type="main")
    if first_lower.isdigit():
        return ScopeIdentity(host=host)
    return ScopeIdentity(host=host, service_prefix=normalize_scope_path_prefix(f"/{first}/"))


def extract_service_scope_path_prefix(raw: Any) -> str:
    return extract_service_scope(raw).service_prefix


def extract_precise_scope_path_prefix(raw: Any) -> str:
    """
    contents URL 이 가리키는 실제 하위 경로를 가능한 한 그대로 보존한다.

    예:
    - https://www.miryang.go.kr/myr/ -> /myr/
    - https://www.miryang.go.kr/myr/board/list.do -> /myr/board/
    - https://www.example.go.kr/list.do -> /
    """
    alias = _jongno_scope_path_alias(raw)
    if alias:
        return alias
    alias = _preferred_scope_path_alias(raw)
    if alias:
        return alias

    try:
        parsed = urlparse(_as_parseable_url(raw))
        path = str(parsed.path or "").strip()
    except Exception:
        path = ""

    if not path or path == "/":
        return "/"

    while "//" in path:
        path = path.replace("//", "/")

    if path.endswith("/"):
        prefix = path
    else:
        last_segment = str(path.rsplit("/", 1)[-1] or "").strip()
        if not last_segment:
            prefix = "/"
        elif (
            "." in last_segment
            or re.search(r"\.(?:html?|php|asp|aspx|jsp)$", last_segment, re.IGNORECASE)
            or re.search(r"(?:^|[_-])(do|proc|view|list|detail|read)$", last_segment, re.IGNORECASE)
        ):
            prefix = path.rsplit("/", 1)[0] if "/" in path else "/"
            if not prefix:
                prefix = "/"
            if not prefix.endswith("/"):
                prefix += "/"
        else:
            prefix = path.rstrip("/") + "/"

    if not prefix.startswith("/"):
        prefix = "/" + prefix
    return prefix or "/"


def extract_scope_identity(raw: Any, *, include_scheme: Optional[bool] = None) -> str:
    use_scheme = scope_include_scheme() if include_scheme is None else bool(include_scheme)
    if use_scheme:
        return extract_scope_origin(raw)
    return extract_scope_host(raw)


def extract_scope_identities(raw_input: Any, *, include_scheme: Optional[bool] = None) -> List[str]:
    if raw_input is None:
        return []
    if isinstance(raw_input, (list, tuple, set)):
        values: Iterable[Any] = raw_input
    else:
        values = [raw_input]

    seen = set()
    out: List[str] = []
    for value in values:
        ident = extract_scope_identity(value, include_scheme=include_scheme)
        if ident and ident not in seen:
            seen.add(ident)
            out.append(ident)
    return out


def _sql_escape_literal(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def _host_scope_aliases(host: str) -> List[str]:
    txt = _canonicalize_scope_host(host)
    if not txt:
        return []
    base_hosts = [txt]
    for group in _SCOPE_HOST_ALIAS_GROUPS:
        if txt in group:
            base_hosts = list(group)
            break
    aliases: List[str] = []
    for base_host in base_hosts:
        aliases.append(base_host)
        if not base_host.startswith("www."):
            aliases.append(f"www.{base_host}")
    return list(dict.fromkeys(aliases))


def build_sql_scope_condition(
    column: str,
    identities: List[str],
    *,
    include_scheme: Optional[bool] = None,
    path_prefix: Optional[str] = None,
) -> str:
    use_scheme = scope_include_scheme() if include_scheme is None else bool(include_scheme)
    if not identities:
        return ""
    normalized_path_prefix = normalize_scope_path_prefix(path_prefix)

    clauses: List[str] = []
    for ident in identities:
        if not ident:
            continue
        if use_scheme:
            parsed = urlparse(_as_parseable_url(ident))
            scheme = str(parsed.scheme or "https").strip().lower()
            for host_alias in _host_scope_aliases(parsed.hostname):
                esc = _sql_escape_literal(f"{scheme}://{host_alias}")
                if normalized_path_prefix:
                    esc_path = _sql_escape_literal(normalized_path_prefix)
                    base = f"{esc}{esc_path}"
                    if normalized_path_prefix == "/":
                        clauses.append(f"{column} = '{esc}'")
                        clauses.append(f"{column} LIKE '{esc}/%%'")
                    else:
                        clauses.append(f"{column} LIKE '{base}%%'")
                        for alias_path in _jongno_main_paths_for_prefix(host_alias, normalized_path_prefix):
                            esc_alias_path = _sql_escape_literal(alias_path)
                            clauses.append(f"{column} = '{esc}{esc_alias_path}'")
                            clauses.append(f"{column} LIKE '{esc}{esc_alias_path}?%%'")
                else:
                    clauses.append(f"{column} = '{esc}'")
                    clauses.append(f"{column} LIKE '{esc}/%%'")
            continue

        for host_alias in _host_scope_aliases(ident):
            host = _sql_escape_literal(host_alias)
            for scheme in ("https", "http"):
                origin = f"{scheme}://{host}"
                if normalized_path_prefix:
                    esc_path = _sql_escape_literal(normalized_path_prefix)
                    if normalized_path_prefix == "/":
                        clauses.append(f"{column} = '{origin}'")
                        clauses.append(f"{column} LIKE '{origin}/%%'")
                    else:
                        clauses.append(f"{column} LIKE '{origin}{esc_path}%%'")
                        for alias_path in _jongno_main_paths_for_prefix(host_alias, normalized_path_prefix):
                            esc_alias_path = _sql_escape_literal(alias_path)
                            clauses.append(f"{column} = '{origin}{esc_alias_path}'")
                            clauses.append(f"{column} LIKE '{origin}{esc_alias_path}?%%'")
                else:
                    clauses.append(f"{column} = '{origin}'")
                    clauses.append(f"{column} LIKE '{origin}/%%'")

    deduped = list(dict.fromkeys(clauses))
    if not deduped:
        return ""
    return "(" + " OR ".join(deduped) + ")"


def url_matches_scope_identities(
    url: Any,
    identities: List[str],
    *,
    include_scheme: Optional[bool] = None,
    path_prefix: Optional[str] = None,
) -> bool:
    if not identities:
        ident_ok = True
    else:
        ident = extract_scope_identity(url, include_scheme=include_scheme)
        if not ident:
            return False
        normalized = {str(x or "").strip().lower() for x in identities if str(x or "").strip()}
        expanded = set(normalized)
        if not (scope_include_scheme() if include_scheme is None else bool(include_scheme)):
            for value in normalized:
                expanded.update(_host_scope_aliases(value))
        normalized = expanded
        ident_ok = ident in normalized
    if not ident_ok:
        return False
    normalized_path_prefix = normalize_scope_path_prefix(path_prefix)
    if not normalized_path_prefix:
        return True
    try:
        parsed = urlparse(_as_parseable_url(url))
        path = str(parsed.path or "").strip() or "/"
    except Exception:
        return False
    if normalized_path_prefix == "/":
        return True
    if _jongno_main_path_alias(parsed.hostname, path) == normalized_path_prefix:
        return True
    return path.startswith(normalized_path_prefix)
