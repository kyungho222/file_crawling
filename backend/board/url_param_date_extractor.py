from __future__ import annotations

import re
from datetime import datetime
from typing import Iterable
from urllib.parse import parse_qs, urlparse


DEFAULT_URL_DATE_PARAMS = ("reqYmd",)


def extract_date_from_url_param(
    url: str | None,
    *,
    param_names: Iterable[str] = DEFAULT_URL_DATE_PARAMS,
) -> datetime | None:
    """Extract a registration date from YYYYMMDD-style URL query parameters."""
    if not url:
        return None

    try:
        query = parse_qs(urlparse(str(url or "")).query)
    except Exception:
        return None

    query_by_lower = {str(key).lower(): values for key, values in query.items()}
    for param_name in param_names:
        values = query_by_lower.get(str(param_name or "").lower()) or []
        if not values:
            continue

        raw = str(values[0] or "").strip()
        if not re.fullmatch(r"\d{8}", raw):
            continue

        try:
            return datetime.strptime(raw, "%Y%m%d")
        except ValueError:
            continue

    return None


def extract_req_ymd_date(url: str | None) -> datetime | None:
    """Extract the reqYmd query parameter as a datetime."""
    return extract_date_from_url_param(url, param_names=("reqYmd",))
