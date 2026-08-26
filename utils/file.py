# utils/file.py
"""
파일 관련 유틸리티 함수
"""
import hashlib
import os
import re
import time
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from uuid import uuid4


def _safe_storage_filename_max_bytes() -> int:
    """디스크 저장 시 파일명(바이트) 상한. ENAMETOOLONG 등 방지. 기본 200, 범위 32~250."""
    try:
        v = int((os.getenv("SAFE_STORAGE_FILENAME_MAX_BYTES") or "200").strip() or "200")
    except ValueError:
        v = 200
    return max(32, min(v, 250))


def truncate_filename_to_max_bytes(filename: str, max_bytes: int | None = None, encoding: str = "utf-8") -> str:
    """
    UTF-8 바이트 길이 기준으로 파일명을 잘라 OS 저장 오류(파일명 과다 등)를 방지한다.
    확장자는 가능하면 유지하고 stem만 잘라낸다. 멀티바이트 문자 경계를 깨지 않도록 조정한다.
    """
    if not filename:
        return filename
    limit = max_bytes if max_bytes is not None else _safe_storage_filename_max_bytes()
    raw = filename.encode(encoding, errors="surrogatepass")
    if len(raw) <= limit:
        return filename
    dot = filename.rfind(".")
    if dot > 0:
        stem, ext = filename[:dot], filename[dot:]
        ext_b = ext.encode(encoding, errors="surrogatepass")
        if len(ext_b) >= limit:
            out = raw[:limit]
            while out and (out[-1] & 0xC0) == 0x80:
                out = out[:-1]
            return out.decode(encoding, errors="ignore")
        budget = limit - len(ext_b)
        stem_b = stem.encode(encoding, errors="surrogatepass")
        if len(stem_b) <= budget:
            return stem + ext
        truncated = stem_b[:budget]
        while truncated and (truncated[-1] & 0xC0) == 0x80:
            truncated = truncated[:-1]
        return truncated.decode(encoding, errors="ignore") + ext
    out = raw[:limit]
    while out and (out[-1] & 0xC0) == 0x80:
        out = out[:-1]
    return out.decode(encoding, errors="ignore")


def file_identity_dedupe_key(
    filename: str,
    size_bytes: int | None = None,
    *,
    merge_hwpx_into_hwp: bool | None = None,
) -> tuple[str, str, int]:
    """
    활용 단계에서 파일명+크기 기준으로 중복 묶기용 키.
    - merge_hwpx_into_hwp: True면 .hwp / .hwpx 를 동일 확장자 그룹으로 본다.
      미지정 시 환경변수 FILE_DEDUPE_HWPX_AS_HWP=1 이면 True.
    """
    if merge_hwpx_into_hwp is None:
        merge_hwpx_into_hwp = (os.getenv("FILE_DEDUPE_HWPX_AS_HWP") or "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
    p = Path(str(filename or ""))
    stem = (p.stem or "file").lower()
    ext = (p.suffix or "").lower().lstrip(".")
    if merge_hwpx_into_hwp and ext in ("hwp", "hwpx"):
        ext = "hwp"
    try:
        sz = int(size_bytes) if size_bytes is not None else -1
    except (TypeError, ValueError):
        sz = -1
    return (stem, ext, sz)


_TRAILING_FILE_SIZE_RE = re.compile(
    r"""
    \s*
    (?:[\[\(]\s*)?
    \d+(?:[.,]\d+)?
    \s*
    (?:bytes?|b|kb|kbyte|kbytes|mb|mbyte|mbytes|gb|gbyte|gbytes|tb|tbyte|tbytes)
    (?:\s*[\]\)])?
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)


_DISPLAY_FILE_SIZE_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(bytes?|b|kb|kbyte|kbytes|mb|mbyte|mbytes|gb|gbyte|gbytes|tb|tbyte|tbytes)\b",
    re.IGNORECASE,
)
_DISPLAY_FILE_SIZE_FACTORS = {
    "b": 1,
    "byte": 1,
    "bytes": 1,
    "kb": 1024,
    "kbyte": 1024,
    "kbytes": 1024,
    "mb": 1024 ** 2,
    "mbyte": 1024 ** 2,
    "mbytes": 1024 ** 2,
    "gb": 1024 ** 3,
    "gbyte": 1024 ** 3,
    "gbytes": 1024 ** 3,
    "tb": 1024 ** 4,
    "tbyte": 1024 ** 4,
    "tbytes": 1024 ** 4,
}


def parse_display_file_size_bytes(value: Any) -> int | None:
    """Parse a human-readable size embedded in an attachment label."""
    matches = list(_DISPLAY_FILE_SIZE_RE.finditer(str(value or "")))
    if not matches:
        return None
    match = matches[-1]
    try:
        amount = float(match.group(1).replace(",", "."))
        factor = _DISPLAY_FILE_SIZE_FACTORS[match.group(2).lower()]
        return max(0, int(amount * factor))
    except (KeyError, TypeError, ValueError):
        return None


def strip_trailing_file_size(filename: str) -> str:
    text = str(filename or "").strip()
    if not text:
        return ""
    while True:
        cleaned = _TRAILING_FILE_SIZE_RE.sub("", text).strip()
        if cleaned == text:
            stem, ext = os.path.splitext(text)
            if ext:
                stem_cleaned = _TRAILING_FILE_SIZE_RE.sub("", stem).strip()
                if stem_cleaned and stem_cleaned != stem:
                    cleaned = f"{stem_cleaned}{ext}"
        if cleaned == text:
            return cleaned
        text = cleaned



_DOWNLOAD_ACTION_LABELS = {
    "file download",
    "download file",
    "download",
    "view",
    "open",
    "shortcut",
    "attachment",
    "attached file",
    "file",
    "preview",
    "document view",
    "data view",
    "save",
    "\ud30c\uc77c\ubc1b\uae30",
    "\ud30c\uc77c \ubc1b\uae30",
    "\ud30c\uc77c\ub2e4\uc6b4\ub85c\ub4dc",
    "\ud30c\uc77c \ub2e4\uc6b4\ub85c\ub4dc",
    "\ub2e4\uc6b4\ub85c\ub4dc",
    "\ub0b4\ub824\ubc1b\uae30",
    "\ubc1b\uae30",
    "\uc800\uc7a5",
    "\uc5f4\uae30",
    "\ubcf4\uae30",
    "\ubc14\ub85c\uac00\uae30",
    "\ubc14\ub85c\ubcf4\uae30",
    "\ubc14\ub85c\ub4e3\uae30",
    "\ucca8\ubd80\ud30c\uc77c",
    "\ucca8\ubd80 \ud30c\uc77c",
    "\ucca8\ubd80",
    "\ubb38\uc11c\ubcf4\uae30",
    "\ubb38\uc11c \ubcf4\uae30",
    "\uc790\ub8cc\ubcf4\uae30",
    "\uc790\ub8cc \ubcf4\uae30",
    "\ubbf8\ub9ac\ubcf4\uae30",
    "\ubbf8\ub9ac \ubcf4\uae30",
    "\uc6d0\ubb38\ubcf4\uae30",
    "\uc6d0\ubb38 \ubcf4\uae30",
}


_FILE_ACTION_EXTS = "hwpx|hwp|xlsx|xls|pptx|ppt|docx|doc|pdf|csv|txt|zip|rar|7z|jpg|jpeg|png|gif|bmp|webp|tif|tiff"


def strip_file_type_display_prefix(filename: str) -> str:
    """Remove icon/alt-text prefixes such as ``pdf document`` before names."""
    text = re.sub(r"\s+", " ", str(filename or "").strip())
    if not text:
        return ""
    ext_tokens = _FILE_ACTION_EXTS
    text = re.sub(
        rf"(?i)^(?:{ext_tokens})\s+(?:document|file|attachment)\s+(?=.+\.(?:{_FILE_ACTION_EXTS})(?:\W|$))",
        "",
        text,
    ).strip()
    text = re.sub(
        rf"(?i)^(?:{ext_tokens})\s+(?:\ubb38\uc11c|\ud30c\uc77c|\ucca8\ubd80\ud30c\uc77c|\ucca8\ubd80\s*\ud30c\uc77c)\s+(?=.+\.(?:{_FILE_ACTION_EXTS})(?:\W|$))",
        "",
        text,
    ).strip()
    text = re.sub(
        rf"(?i)^(?:{ext_tokens})\s+(?=.+\.(?:{_FILE_ACTION_EXTS})(?:\W|$))",
        "",
        text,
    ).strip()
    return text

def _action_label_pattern(label: str) -> str:
    label_norm = re.sub(r"\s+", " ", str(label or "")).strip()
    return re.escape(label_norm).replace(r"\ ", r"\s+")


def strip_fallback_download_label(filename: str) -> str:
    """Remove trailing UI action labels from fallback attachment names.

    Labels are trimmed only at the end or after a known file extension, so real
    filenames such as ``download_manual.pdf`` are preserved.
    """
    text = strip_file_type_display_prefix(strip_trailing_file_size(str(filename or "").strip()))
    if not text:
        return ""
    labels = sorted(_DOWNLOAD_ACTION_LABELS, key=len, reverse=True)
    download_count_re = re.compile(
        r"\s*(?:[\[\(]\s*)?(?:\ub2e4\uc6b4\ub85c\ub4dc|\ub0b4\ub824\ubc1b\uae30|\ubc1b\uae30|download)\s*\d+\s*(?:\ud68c|times?)?\s*(?:[\]\)])?\s*$",
        re.IGNORECASE,
    )

    for _ in range(6):
        before = text
        text = download_count_re.sub("", text).strip()
        for label in labels:
            label_pattern = _action_label_pattern(label)
            if not label_pattern:
                continue
            stripped = re.sub(
                rf"\s*(?:[\[\(\{{<|:_\-/\s]*)?{label_pattern}\s*(?:[\]\)\}}>]*)$",
                "",
                text,
                flags=re.IGNORECASE,
            ).strip(" -_|:")
            if stripped != text:
                text = stripped
                break
        text = strip_file_type_display_prefix(strip_trailing_file_size(text)).strip()
        if text == before:
            break
    if not text:
        return ""

    for label in labels:
        label_pattern = _action_label_pattern(label)
        if not label_pattern:
            continue
        match = re.match(
            rf"^(?P<name>.+?\.(?:{_FILE_ACTION_EXTS}))\s*(?:[\[\(\{{<|:_\-/\s]*)?{label_pattern}\s*(?:[\]\)\}}>]*)$",
            text,
            flags=re.IGNORECASE,
        )
        if match:
            return match.group("name").strip(" -_|:")

    stem, ext = os.path.splitext(text)
    target = stem if ext else text
    normalized = re.sub(r"\s+", " ", target).strip(" -_|:")
    if not normalized:
        return text
    lowered = normalized.lower()
    for label in labels:
        label_norm = re.sub(r"\s+", " ", label).strip().lower()
        if not label_norm:
            continue
        if lowered == label_norm:
            return ""
        for sep in (" ", "_", "-", "|", ":"):
            suffix = sep + label_norm
            if lowered.endswith(suffix):
                candidate = normalized[: -len(suffix)].strip(" -_|:")
                if candidate:
                    return f"{candidate}{ext}" if ext else candidate
    return text
def sanitize_filename(filename: str) -> str:
    """
    파일명을 안전하게 처리합니다.
    한국어(한글), 영문, 숫자, 공백 및 일부 특수문자(._-)를 허용합니다.
    """
    filename = strip_trailing_file_size(filename)
    if not filename:
        return "downloaded_file"
    
    # 허용된 문자만 남기기 (한글 범위: AC00-D7A3)
    safe_chars = []
    for c in filename:
        # 한글, 알파벳, 숫자, 공백, . _ - 허용
        if '가' <= c <= '힣' or c.isalpha() or c.isdigit() or c in (' ', '.', '_', '-'):
            safe_chars.append(c)
    
    safe_filename = "".join(safe_chars).strip()
    
    # 윈도우/리눅스 예약어 및 길이 제한 처리 (선택적)
    if not safe_filename:
        return "downloaded_file"
    
    return safe_filename

def get_file_extension(filename: str) -> str:
    """
    파일명에서 확장자를 추출합니다.
    """
    filename = strip_trailing_file_size(filename)
    if not filename or '.' not in filename:
        return ""
    
    return filename.split('.')[-1]


def make_safe_storage_filename(subject: str) -> str:
    """
    PHP와 동일한 방식으로 디스크 저장용 안전한 파일명을 생성합니다.
    - 저장명: md5(subject + time + uniqid) + "." + ext
    - DB에는 content=경로+이 저장명, subject=원본 파일명으로 저장.
    """
    # 변경: 원본 파일명을 가능한 한 그대로 사용하도록 반환합니다.
    # - 파일명은 sanitize_filename으로 안전화합니다.
    # - 확장자가 존재하면 유지합니다.
    # 기존 방식(무작위 해시명)은 더 이상 사용하지 않습니다.
    if not subject:
        return "downloaded_file"

    ext = get_file_extension(subject or "")
    safe_name = sanitize_filename(subject) or ""
    # sanitize가 확장자를 제거했을 수 있으므로 확장자 보장
    if ext and not safe_name.lower().endswith("." + ext.lower()):
        safe_name = f"{safe_name}.{ext}"
    # 최종 폴백
    if not safe_name:
        safe_name = "downloaded_file"
    return truncate_filename_to_max_bytes(safe_name)


def preserve_file_learning_subject(value: Any) -> str:
    """Preserve one source filename for both DBs without punctuation rewriting."""
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        if "://" in text:
            text = urlparse(text).path or text
    except Exception:
        pass
    if "/" in text or "\\" in text:
        text = os.path.basename(text.replace("\\", "/"))
    try:
        text = unquote(text)
    except Exception:
        pass
    # NFC only makes canonically equivalent Unicode byte-stable. It does not
    # remove spaces, brackets, middle dots, or any other filename punctuation.
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"[\x00-\x1f\x7f]", "", text)
    return text.strip()


def format_file_size(size_bytes: int) -> str:
    """
    파일 크기를 사람이 읽기 쉬운 형태로 포맷팅합니다.
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def ensure_directory(path: str) -> Path:
    """
    디렉토리가 존재하지 않으면 생성합니다.
    """
    dir_path = Path(path)
    dir_path.mkdir(parents=True, exist_ok=True)
    return dir_path
