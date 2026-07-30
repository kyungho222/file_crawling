from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import quote, urlparse


_SUPPORTED_EXT_RE = re.compile(
    r"\.(pdf|hwp|hwpx|doc|docx|xls|xlsx|ppt|pptx|zip|rar|7z|txt|csv|jpg|jpeg|png|gif)(?:$|\W)",
    re.IGNORECASE,
)


def is_gm_nftc_bbs_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(str(url))
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        return (host == "gm.go.kr" or host.endswith(".gm.go.kr")) and "/user/nftcbbs/" in path
    except Exception:
        u = str(url or "").lower()
        return "gm.go.kr" in u and "/user/nftcbbs/" in u


def build_gm_eminwon_download_url(*, user_file_nm: str, sys_file_nm: str, file_path: str) -> str:
    return (
        "https://eminwon.gm.go.kr/emwp/jsp/ofr/FileDownNew.jsp"
        f"?user_file_nm={quote(user_file_nm or '')}"
        f"&sys_file_nm={quote(sys_file_nm or '')}"
        f"&file_path={quote(file_path or '', safe='/')}"
    )


def _parse_object_fields(raw: str) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for key, value in re.findall(r"""["']([^"']+)["']\s*:\s*["']([^"']*)["']""", raw or ""):
        fields[str(key).strip()] = str(value).strip()
    return fields


def extract_gm_nftc_filelist_attachments(html: str | None, base_url: str | None = None) -> List[Dict[str, str]]:
    if not html or (base_url and not is_gm_nftc_bbs_url(base_url)):
        return []

    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(r"fileList\.push\s*\(\s*\{(?P<body>.*?)\}\s*\)\s*;", str(html), re.DOTALL | re.IGNORECASE):
        fields = _parse_object_fields(match.group("body") or "")
        sys_file_nm = fields.get("sysFileNm") or fields.get("sys_file_nm") or ""
        file_nm = fields.get("fileNm") or fields.get("file_nm") or ""
        file_path = fields.get("filePath") or fields.get("file_path") or ""
        if not (sys_file_nm and file_nm and file_path):
            continue
        if not _SUPPORTED_EXT_RE.search(file_nm):
            continue
        href = build_gm_eminwon_download_url(
            user_file_nm=file_nm,
            sys_file_nm=sys_file_nm,
            file_path=file_path,
        )
        key = href.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": file_nm, "href": href})
    return out


def extract_gm_filedownnow_url(js_text: Any, base_url: str | None = None) -> str:
    text = str(js_text or "")
    if not text:
        return ""
    if base_url and not is_gm_nftc_bbs_url(base_url):
        return ""
    m = re.search(
        r"fileDownNow\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
        text,
        re.IGNORECASE,
    )
    if not m:
        return ""
    sys_file_nm = (m.group(1) or "").strip()
    file_nm = (m.group(2) or "").strip()
    file_path = (m.group(3) or "").strip()
    if not (sys_file_nm and file_nm and file_path):
        return ""
    return build_gm_eminwon_download_url(
        user_file_nm=file_nm,
        sys_file_nm=sys_file_nm,
        file_path=file_path,
    )
