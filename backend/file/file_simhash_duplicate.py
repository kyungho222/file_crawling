"""Stable file duplicate identity built from an external decimal SimHash."""

from __future__ import annotations

from typing import Any, Optional, Tuple

from backend.shared.file_simhash_generation import normalize_decimal_simhash


FileSimhashDuplicateKey = Tuple[str, int, int]


def build_file_simhash_claim_key(simhash_decimal: Any) -> Optional[str]:
    """Return the cross-job claim key for a MariaDB hash duplicate gate."""
    return normalize_decimal_simhash(simhash_decimal)


def build_file_simhash_duplicate_key(
    *,
    simhash_decimal: Any,
    normalized_length: Any,
    file_size: Any,
) -> Optional[FileSimhashDuplicateKey]:
    """Return the exact file-body identity used for PG duplicate checks."""
    normalized_hash = normalize_decimal_simhash(simhash_decimal)
    try:
        parsed_length = int(normalized_length or 0)
    except (TypeError, ValueError):
        parsed_length = 0
    try:
        parsed_size = int(file_size or 0)
    except (TypeError, ValueError):
        parsed_size = 0
    if not normalized_hash or parsed_length <= 0 or parsed_size <= 0:
        return None
    return normalized_hash, parsed_length, parsed_size
