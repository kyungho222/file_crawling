from __future__ import annotations

import re
from typing import Any

_CHARSET_RE = re.compile(r"charset\s*=\s*['\"]?([A-Za-z0-9._\-]+)", re.IGNORECASE)
_META_CHARSET_RE = re.compile(
    rb"<meta\b[^>]*(?:charset\s*=\s*['\"]?([A-Za-z0-9._\-]+)|content\s*=\s*['\"][^'\"]*charset\s*=\s*([A-Za-z0-9._\-]+))",
    re.IGNORECASE,
)
_HANGUL_RE = re.compile(r"[\uac00-\ud7a3]")


def _normalize_charset(value: Any) -> str:
    text = str(value or "").strip().strip("'\"").lower()
    aliases = {
        "ks_c_5601-1987": "cp949",
        "ks_c_5601": "cp949",
        "x-windows-949": "cp949",
        "windows-949": "cp949",
        "ms949": "cp949",
        "euckr": "euc-kr",
        "utf8": "utf-8",
    }
    return aliases.get(text, text)


def _charset_from_content_type(content_type: Any) -> str:
    match = _CHARSET_RE.search(str(content_type or ""))
    return _normalize_charset(match.group(1)) if match else ""


def _charset_from_meta(raw: bytes) -> str:
    head = bytes(raw or b"")[:8192]
    match = _META_CHARSET_RE.search(head)
    if not match:
        return ""
    return _normalize_charset(match.group(1) or match.group(2) or b"")


def _decode_score(text: str) -> tuple[int, int, int]:
    replacement_count = text.count("\ufffd")
    hangul_count = len(_HANGUL_RE.findall(text))
    return (replacement_count * 10, -hangul_count, -len(text))


def decode_html_response_bytes(raw: bytes | bytearray | memoryview | None, content_type: Any = "") -> str:
    data = bytes(raw or b"")
    if not data:
        return ""

    candidates: list[str] = []
    for enc in (
        _charset_from_content_type(content_type),
        _charset_from_meta(data),
        "utf-8-sig",
        "utf-8",
        "cp949",
        "euc-kr",
    ):
        enc = _normalize_charset(enc)
        if enc and enc not in candidates:
            candidates.append(enc)

    decoded: list[tuple[tuple[int, int, int], str]] = []
    for enc in candidates:
        try:
            text = data.decode(enc, errors="replace")
        except LookupError:
            continue
        if text:
            decoded.append((_decode_score(text), text))

    if decoded:
        decoded.sort(key=lambda item: item[0])
        return decoded[0][1]
    return data.decode("utf-8", errors="replace")