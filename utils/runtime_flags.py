import os


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def is_no_limits_mode() -> bool:
    """
    무제한 크롤링 모드 여부.

    - CRAWL_NO_LIMITS=1 (또는 true/yes/on) 이면 True
    - 기본 False
    """
    return _is_truthy(os.getenv("CRAWL_NO_LIMITS"))


def unlimited_cap(default_cap: int, *, huge: int) -> int:
    """
    CRAWL_NO_LIMITS=1이면 cap을 매우 크게(huge) 설정한다.
    기본 모드에서는 default_cap 반환.
    """
    return huge if is_no_limits_mode() else default_cap


