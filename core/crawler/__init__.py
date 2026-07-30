# core/crawler/__init__.py
"""
core.crawler 패키지 진입점.

주의:
- 여기서 무거운 모듈(engine 등)을 import하면 하위 모듈(queues 등)만 사용하려 해도
  전체 워커/학습 의존성이 같이 로딩되어 ImportError(예: optional deps)로 터질 수 있다.
- 따라서 run_crawler는 lazy import로 제공한다.
"""

from __future__ import annotations

from .progress import Progress


def run_crawler(*args, **kwargs):
    from .engine import run_crawler as _run_crawler

    return _run_crawler(*args, **kwargs)


__all__ = ["run_crawler", "Progress"]
