from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Optional

DEFAULT_TIMEZONE = "Asia/Seoul"


def _resolve_timezone(tz_name: Optional[str] = None) -> ZoneInfo:
    """지정된 이름 또는 기본값에 해당하는 `ZoneInfo` 인스턴스를 반환한다."""
    name = (tz_name or DEFAULT_TIMEZONE).strip()
    if not name:
        return ZoneInfo(DEFAULT_TIMEZONE)
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        # 지정된 타임존을 찾을 수 없는 경우 UTC로 폴백
        return ZoneInfo("UTC")


def get_local_now(tz_name: Optional[str] = None) -> datetime:
    """현재 시각을 지정된 타임존으로 반환한다."""
    tz = _resolve_timezone(tz_name)
    return datetime.now(tz)


def to_timezone(dt: datetime, tz_name: Optional[str] = None) -> datetime:
    """UTC 또는 타임존 미지정 datetime을 안전하게 변환한다."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    tz = _resolve_timezone(tz_name)
    return dt.astimezone(tz)


__all__ = ["get_local_now", "to_timezone", "DEFAULT_TIMEZONE"]

