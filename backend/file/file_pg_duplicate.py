"""Stable PG duplicate identities for file crawling."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Optional, Tuple

from utils.file import strip_fallback_download_label, strip_trailing_file_size


# 원본 파일명과 실제 바이트 수로 PG 중복 비교용 식별자를 만듭니다.
def build_file_pg_duplicate_fingerprint(file_name: Any, file_size: Any) -> Optional[Tuple[str, int]]:
    try:
        size = int(file_size or 0)
    except (TypeError, ValueError):
        size = 0
    if size <= 0:
        return None
    try:
        name = strip_trailing_file_size(strip_fallback_download_label(str(file_name or "")) or "")
        name = re.sub(
            r"\s*\(\s*\d+(?:\.\d+)?\s*(?:b|kb|mb|gb)\s*(?:,\s*[^)]*)?\)\s*$",
            "",
            name,
            flags=re.IGNORECASE,
        )
        name = unicodedata.normalize("NFC", name).casefold()
        name = re.sub(r"[\W_]+", "", name, flags=re.UNICODE)
    except Exception:
        name = ""
    if not name:
        return None
    return name, size
