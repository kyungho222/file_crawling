from __future__ import annotations

import re
from typing import Optional
from urllib.parse import parse_qs, urlparse


def normalize_host(host: str) -> str:
    host = (host or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def extract_board_id(path: str) -> Optional[str]:
    try:
        m = re.search(r"/bbs/([^/]+)/", path or "", re.IGNORECASE)
    except Exception:
        m = None
    if not m:
        return None
    return m.group(1)


def extract_menu_no(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    qs = parse_qs(parsed.query or "")
    for key in (
        "menuNo",
        "menuno",
        "menu_no",
        "menu",
        "menu_cd",
        "ctgryCd",
        "ctgry_cd",
        "ctgrycd",
        "categoryCd",
        "category_cd",
    ):
        if key in qs and qs[key]:
            return str(qs[key][0])
        for k, v in qs.items():
            if k.lower() == key.lower() and v:
                return str(v[0])
    return None


def extract_board_param(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    qs = parse_qs(parsed.query or "")
    for key in ("bbsId", "bbs_id", "bbsCd", "bbs_cd", "boardId", "board_id"):
        if key in qs and qs[key]:
            return str(qs[key][0])
        for k, v in qs.items():
            if k.lower() == key.lower() and v:
                return str(v[0])
    return None
