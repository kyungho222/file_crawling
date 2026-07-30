"""
문서 전용 다운로드(DOWNLOAD_DOC_ONLY) 정책과 동일한 기준을 공유합니다.

- 다운로드 워커: Content-Type·파일명 기반 차단
- 스캔/큐 투입: URL·표시 파일명만으로 명확히 비문서인 경우만 선제 제외
  (.do/.jsp 등 동적 엔드포인트는 확장자가 없으면 제외하지 않음)
"""
from __future__ import annotations

import os
import re
from urllib.parse import urlparse, unquote

from config.constants import DOC_EXTENSIONS, ARCHIVE_EXTENSIONS, IMG_EXTENSIONS
from utils.file import strip_trailing_file_size

_DOC_EXTS = {e.lower() for e in (DOC_EXTENSIONS or [])}
_ARCHIVE_EXTS = {e.lower() for e in (ARCHIVE_EXTENSIONS or [])}
_IMG_EXTS = {e.lower() for e in (IMG_EXTENSIONS or [])}

# 스캔 단계에서만: 동적 웹 엔드포인트 확장자 — 파일 실제 형식이 아니므로 여기서 차단하지 않음
_AMBIGUOUS_HANDLER_EXTS = frozenset(
    {
        ".do",
        ".jsp",
        ".jspx",
        ".asp",
        ".aspx",
        ".php",
        ".action",
        ".cgi",
        ".html",
        ".htm",
    }
)

# 이미지·압축 외 명확히 비문서(멀티미디어/실행 등) — 스캔 단계 선제 제외용
_EXTRA_NON_DOC_EXTS = frozenset(
    {
        ".mp4",
        ".avi",
        ".mkv",
        ".webm",
        ".mov",
        ".wmv",
        ".mp3",
        ".wav",
        ".flac",
        ".aac",
        ".m4a",
        ".exe",
        ".dll",
        ".dmg",
        ".apk",
        ".webp",
        ".svg",
        ".ico",
        ".ai",
        ".eps",
        ".psd",
        ".indd",
    }
)

_SCAN_SKIP_EXTS = _ARCHIVE_EXTS | _IMG_EXTS | _EXTRA_NON_DOC_EXTS
_KNOWN_EXTS = _DOC_EXTS | _ARCHIVE_EXTS | _IMG_EXTS | _EXTRA_NON_DOC_EXTS


def _env_bool(key: str, default: str = "1") -> bool:
    try:
        return str(os.getenv(key, default)).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return default == "1"


DOWNLOAD_DOC_ONLY = _env_bool("DOWNLOAD_DOC_ONLY", "1")


def ext_of_name(name: str) -> str:
    text = strip_trailing_file_size(unquote(str(name or "")).strip())
    if text:
        # Attachment labels often include a size suffix, e.g. "notice.hwp [29.0 KByte]".
        # Prefer the first known extension token over the last dot in the size text.
        pattern = r"(?i)({})(?=$|[\s\]\)\}},;:])".format(
            "|".join(re.escape(ext) for ext in sorted(_KNOWN_EXTS, key=len, reverse=True))
        )
        match = re.search(pattern, text)
        if match:
            return match.group(1).lower()
    try:
        base = os.path.basename(text)
    except Exception:
        base = text
    dot = base.rfind(".")
    if dot == -1:
        return ""
    return base[dot:].lower()


def is_blocked_non_document(filename: str, content_type: str) -> bool:
    """문서류만 허용: zip/이미지/오디오/비디오 등은 차단 (다운로드 워커와 동일)."""
    if not DOWNLOAD_DOC_ONLY:
        return False
    ct = (content_type or "").lower()
    ext = ext_of_name(filename)
    if ext in _ARCHIVE_EXTS:
        return True
    if ext in _IMG_EXTS:
        return True
    if ext and ext not in _DOC_EXTS:
        return True
    if ct.startswith("image/") or ct.startswith("video/") or ct.startswith("audio/"):
        return True
    if "application/zip" in ct or "application/x-zip" in ct or "application/x-7z" in ct:
        return True
    if any(token in ct for token in ("illustrator", "postscript", "photoshop", "indesign")):
        return True
    return False


def should_skip_attachment_at_scan(file_url: str, display_name: str = "") -> bool:
    """
    링크 수집(스캔) 단계에서 큐 부하를 줄이기 위한 선제 필터.
    URL 경로 또는 표시 파일명에 '명확한' 비문서 확장자가 있을 때만 True.
    """
    if not DOWNLOAD_DOC_ONLY:
        return False

    def _labels() -> list[str]:
        out: list[str] = []
        dn = (display_name or "").strip()
        if dn:
            out.append(dn)
        try:
            u = unquote((file_url or "").strip())
            path = (urlparse(u).path or "").rstrip("/")
            if path:
                out.append(os.path.basename(path))
        except Exception:
            pass
        return out

    for label in _labels():
        ext = ext_of_name(label)
        if not ext:
            continue
        if ext in _AMBIGUOUS_HANDLER_EXTS:
            continue
        if ext not in _DOC_EXTS:
            return True
        if ext in _SCAN_SKIP_EXTS:
            return True
    return False
