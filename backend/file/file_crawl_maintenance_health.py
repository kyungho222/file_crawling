"""Execution-health contract for the file maintenance dashboard."""

from __future__ import annotations


def build_execution_health(
    status: str,
    *,
    task_alive: bool,
    activity_age_seconds: float | None,
) -> dict[str, object]:
    normalized = str(status or "").strip().lower()
    age = max(0, int(activity_age_seconds or 0))
    if normalized in {"completed", "failed", "stopped"}:
        return {"state": normalized, "activity_age_seconds": age}
    if normalized == "awaiting_approval":
        return {"state": "awaiting_approval", "activity_age_seconds": age}
    if not task_alive:
        return {"state": "dead", "activity_age_seconds": age}
    if age > 30:
        return {"state": "waiting", "activity_age_seconds": age}
    return {"state": "processing", "activity_age_seconds": age}
