"""Mask fixed sensitive terms from file text before embedding."""

from __future__ import annotations

from typing import Tuple


# Keep file-learning masking rules in one place. Source files remain unchanged.
FILE_LEARNING_TEXT_MASK_RULES: Tuple[Tuple[str, str], ...] = (
    ("953-5553", "***-****"),
)


def mask_file_learning_text(text: str) -> tuple[str, int]:
    """Return learning-only masked text and the number of replacements."""
    masked = str(text or "")
    replacement_count = 0
    for target, replacement in FILE_LEARNING_TEXT_MASK_RULES:
        count = masked.count(target)
        if count:
            masked = masked.replace(target, replacement)
            replacement_count += count
    return masked, replacement_count
