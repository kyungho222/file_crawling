"""Helpers for separating source display names from generated storage filenames."""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import unquote


_GENERATED_STORAGE_NAME_RE = re.compile(
    r"^(?:minwonprint\d+(?:[_-]\d+)+|conveminwon[_-]?\d+[_-]warrant)$",
    re.IGNORECASE,
)


def is_generated_attachment_storage_name(value: Any) -> bool:
    """Return whether a filename is a known non-display storage convention."""
    try:
        filename = os.path.basename(unquote(str(value or "").strip()))
        stem, _ = os.path.splitext(filename)
    except Exception:
        return False
    return bool(_GENERATED_STORAGE_NAME_RE.fullmatch(stem or ""))
