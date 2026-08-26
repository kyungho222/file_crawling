"""Small, dependency-free contract for persisted SimHash values."""

from __future__ import annotations

import re
from typing import Any, Optional


_SIMHASH_HEX_RE = re.compile(r"^[0-9a-fA-F]{32}$")


def normalize_simhash_hex(value: Any) -> Optional[str]:
    """Return a canonical 128-bit SimHash string, or ``None`` when invalid."""
    text = str(value or "").strip()
    if not _SIMHASH_HEX_RE.fullmatch(text):
        return None
    return text.lower()