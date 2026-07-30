from __future__ import annotations

import os
from typing import Any, Optional


def _db_alias_map() -> dict[str, str]:
    raw = str(os.getenv("DB_NAME_ALIAS_MAP", "") or "").strip()
    out: dict[str, str] = {}
    if raw:
        for part in raw.split(","):
            if "=" not in part:
                continue
            src, dst = part.split("=", 1)
            src = src.strip().lower()
            dst = dst.strip()
            if src and dst:
                out[src] = dst
    # f1 프론트에서 account db_name으로 naraone이 들어오지만, 개발/파일크롤러 MariaDB에는
    # 해당 스키마가 없는 배포가 있어 기존 dev_user 스키마로 보정한다.
    out.setdefault("naraone", "dev_user")
    return out


def normalize_db_name(value: Optional[str], default: Optional[str] = None) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return default
    return _db_alias_map().get(text.lower(), text)


def resolve_db_name(obj: Any, default: Optional[str] = None) -> Optional[str]:
    """
    다양한 입력(dict/metadata)에서 DB명을 최대한 안정적으로 추출한다.
    - legacy/혼용 키: db_name, dbname, account_name, dbName 등
    """
    if obj is None:
        return default

    # 이미 문자열이면 그대로 사용
    if isinstance(obj, str):
        v = obj.strip()
        return normalize_db_name(v, default)

    getter = getattr(obj, "get", None)
    if callable(getter):
        for key in ("db_name", "dbname", "account_name", "accountName", "dbName", "db"):
            try:
                raw = getter(key)
            except Exception:
                raw = None
            if raw is None:
                continue
            try:
                text = str(raw).strip()
            except Exception:
                text = ""
            if text:
                return normalize_db_name(text, default)

    return default

