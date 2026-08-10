# core/crawler/browser_launch.py
"""
Chromium launch 인자 화이트리스트 및 필터.

- 보안/안정성: 허용된 인자만 사용하여 임의 실행 인자 주입 방지.
- global_pool.py, manager.py에서 공통 사용 (브라우저 재사용 시 동일 정책 적용).
- 동시 브라우저 수 제한: BROWSER_LAUNCH_SEMAPHORE (launch 시 acquire, close 시 release).
"""
from __future__ import annotations

import asyncio
import os
from typing import List

# 동시에 띄울 수 있는 브라우저 최대 개수 (전역 제한)
try:
    _max = int(os.getenv("CRAWLER_MAX_CONCURRENT_BROWSERS", "5") or "5")
except Exception:
    _max = 5
_max = max(1, min(_max, 64))
BROWSER_LAUNCH_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(_max)

# A relaunch can leave an old Browser alive while an in-flight fallback releases
# its lease. Keep one retired browser at most so Chromium processes cannot pile up.
MAX_RETIRED_BROWSERS = 1
RETIRED_BROWSER_FORCE_CLOSE_SECONDS = 30.0

# 허용된 Chromium launch 인자만 사용 (제한 추가, Zombie 방지 포함)
ALLOWED_CHROMIUM_LAUNCH_ARGS: frozenset = frozenset({
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--no-zygote",
    "--disable-setuid-sandbox",
    "--disable-software-rasterizer",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-sync",
    "--metrics-recording-only",
    "--mute-audio",
    "--no-first-run",
    "--disable-default-apps",
    # Zombie/백그라운드 프로세스 방지
    "--disable-renderer-backgrounding",
    "--disable-backgrounding-occluded-windows",
    "--disable-background-timer-throttling",
})


def filter_launch_args(args: List[str]) -> List[str]:
    """허용 목록에 있는 인자만 반환. 그 외 인자는 무시."""
    if not args:
        return []
    return [a for a in args if a in ALLOWED_CHROMIUM_LAUNCH_ARGS]


def get_default_launch_args() -> List[str]:
    """기본 권장 launch 인자 (화이트리스트 내, Zombie 방지 포함)."""
    return [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--no-zygote",
        "--disable-setuid-sandbox",
        "--disable-software-rasterizer",
        "--disable-renderer-backgrounding",
        "--disable-backgrounding-occluded-windows",
    ]


# 타임아웃 강제 설정 (env 오버라이드 가능)
def get_default_navigation_timeout_ms() -> int:
    """Context 기본 네비게이션 타임아웃(ms)."""
    try:
        v = int(os.getenv("CRAWLER_DEFAULT_NAVIGATION_TIMEOUT_MS", "60000") or "60000")
    except Exception:
        v = 60000
    return max(5000, min(v, 300000))


def get_default_timeout_ms() -> int:
    """Context 기본 작업 타임아웃(ms)."""
    try:
        v = int(os.getenv("CRAWLER_DEFAULT_TIMEOUT_MS", "30000") or "30000")
    except Exception:
        v = 30000
    return max(3000, min(v, 120000))
