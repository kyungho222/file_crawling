"""Small reusable policy helpers for retrying transient file downloads."""

from __future__ import annotations

import random


def download_failed_retry_delay_sec(
    base_delay_sec: float,
    attempt: int,
    *,
    jitter_ratio: float = 0.15,
) -> float:
    """Return a bounded exponential retry delay; attempt one uses the base."""
    base = max(1.0, min(float(base_delay_sec or 1.0), 1800.0))
    retry_attempt = max(1, min(int(attempt or 1), 5))
    delay = min(1800.0, base * (2 ** (retry_attempt - 1)))
    jitter = max(0.0, min(float(jitter_ratio or 0.0), 0.5))
    if jitter <= 0.0:
        return round(delay, 3)
    return round(max(1.0, min(1800.0, delay * random.uniform(1.0 - jitter, 1.0 + jitter))), 3)
