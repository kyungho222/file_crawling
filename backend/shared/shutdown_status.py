from __future__ import annotations

from typing import Any, Mapping


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def resolve_shutdown_crawl_status(stats: Mapping[str, Any] | None) -> str:
    """
    앱 종료 시 강제 종료되는 워크플로우의 최종 상태를 정한다.

    정책:
    - 탐색 수량(scan_count)이 1개 이상이면 실제 탐색이 시작된 작업이므로 `stop`
    - 탐색 수량이 0이거나 없으면 시작만 됐거나 실패한 작업으로 보고 `error`
    """
    if not isinstance(stats, Mapping):
        return "error"
    scan_count = _to_int(stats.get("scan_count"))
    return "stop" if scan_count > 0 else "error"
