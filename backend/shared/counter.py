import threading
from typing import Dict, Any


class Counter:
    """간단한 프로세스 내 메트릭 카운터.

    - thread-safe
    - inc(name, n) / set(name, value) / get / snapshot 제공
    - 영속화나 외부 저장소 연동은 별도 구현 예정(publish_hook 등록 등)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._metrics: Dict[str, int] = {}

    def inc(self, name: str, n: int = 1) -> None:
        if not name:
            return
        try:
            n = int(n)
        except Exception:
            n = 1
        with self._lock:
            self._metrics[name] = int(self._metrics.get(name, 0) or 0) + max(0, n)

    def set(self, name: str, value: int) -> None:
        try:
            value = int(value)
        except Exception:
            value = 0
        with self._lock:
            self._metrics[name] = value

    def get(self, name: str, default: int = 0) -> int:
        with self._lock:
            return int(self._metrics.get(name, default) or default)

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._metrics)


# 싱글톤 인스턴스: from backend.shared.counter import counter
counter = Counter()

