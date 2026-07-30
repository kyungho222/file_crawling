from __future__ import annotations

import re
from urllib.parse import parse_qsl, quote, urljoin, urlparse


ANSEONG_FILE_HOSTS = {
    "anseong.go.kr",
    "www.anseong.go.kr",
    "asimc.or.kr",
    "www.asimc.or.kr",
    "asimc.han.kr",
}


def is_anseong_file_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        parsed = urlparse(url if "://" in str(url) else f"https://{url}")
        host = (parsed.netloc or "").lower().split("@")[-1].split(":")[0]
        return host in ANSEONG_FILE_HOSTS
    except Exception:
        return False


def resolve_anseong_yhlib_download_url(js_text: str, base_url: str | None = None) -> str:
    if not js_text or not is_anseong_file_url(base_url):
        return ""
    try:
        m = re.search(
            r"yhlib\.file\.download\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
            str(js_text),
            re.IGNORECASE,
        )
    except Exception:
        m = None
    if not m:
        return ""

    atch_file_id = (m.group(1) or "").strip()
    file_sn = (m.group(2) or "").strip()
    if not atch_file_id or not file_sn:
        return ""

    candidate = (
        "/common/file/download.do"
        f"?atchFileId={quote(atch_file_id)}"
        f"&fileSn={quote(file_sn)}"
    )
    try:
        return urljoin(base_url or "", candidate) if base_url else candidate
    except Exception:
        return candidate


def clean_anseong_attachment_name(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        return ""
    match = re.search(
        r"첨부파일\s*\((.{1,220}\.(?:pdf|hwp|hwpx|doc|docx|xls|xlsx|ppt|pptx|csv|zip|rar|7z|txt|jpg|jpeg|png|gif))\)",
        text,
        re.IGNORECASE,
    )
    return (match.group(1).strip() if match else text).strip()


def extract_anseong_attachment_key_candidates(value: str | None, base_url: str | None = None) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    if not (is_anseong_file_url(base_url) or is_anseong_file_url(raw)):
        return []

    values = [raw]
    resolved = resolve_anseong_yhlib_download_url(raw, base_url)
    if resolved:
        values.append(resolved)

    out: list[str] = []
    seen: set[str] = set()

    def add(token: str) -> None:
        s = str(token or "").strip()
        if len(s) < 8:
            return
        key = s.lower()
        if key in seen:
            return
        seen.add(key)
        out.append(s)

    for item in values:
        try:
            parsed = urlparse(item)
            for key, val in parse_qsl(parsed.query or "", keep_blank_values=False):
                if (key or "").strip().lower() in {"atchfileid", "filesn", "filename", "path"}:
                    add(val)
        except Exception:
            pass

        if "yhlib.file.download" in item.lower():
            try:
                for match in re.finditer(r"['\"]([^'\"]{8,})['\"]", item):
                    token = (match.group(1) or "").strip()
                    if not any(ch in token for ch in ("/", "\\", "?", "&", "=")):
                        add(token)
            except Exception:
                pass

    return out
