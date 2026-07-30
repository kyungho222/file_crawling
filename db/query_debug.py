import logging
import os
import re
import time
from collections import defaultdict
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger("db.query_debug")

_TABLE_PATTERNS: Tuple[Tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bfrom\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE), "FROM"),
    (re.compile(r"\binto\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE), "INTO"),
    (re.compile(r"\bupdate\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE), "UPDATE"),
    (re.compile(r"\btable\s+`?([A-Za-z0-9_]+)`?", re.IGNORECASE), "TABLE"),
)

_state: Dict[str, Any] = {
    "last_emit_ts": 0.0,
    "total_count": 0,
    "total_errors": 0,
    "total_slow": 0,
    "sum_ms": 0.0,
    "by_sig": defaultdict(lambda: {"count": 0, "errors": 0, "slow": 0, "sum_ms": 0.0, "max_ms": 0.0}),
}


def _enabled() -> bool:
    return str(os.getenv("DB_OVERLOAD_DEBUG", "0")).strip().lower() in ("1", "true", "yes", "on")


def _slow_ms() -> float:
    try:
        v = float(os.getenv("DB_OVERLOAD_DEBUG_SLOW_MS", "300") or "300")
    except Exception:
        v = 300.0
    return max(10.0, min(v, 60_000.0))


def _emit_interval_sec() -> float:
    try:
        v = float(os.getenv("DB_OVERLOAD_DEBUG_INTERVAL_SEC", "30") or "30")
    except Exception:
        v = 30.0
    return max(3.0, min(v, 600.0))


def _topn() -> int:
    try:
        v = int(os.getenv("DB_OVERLOAD_DEBUG_TOPN", "5") or "5")
    except Exception:
        v = 5
    return max(1, min(v, 20))


def _query_signature(query: str) -> str:
    q = " ".join(str(query or "").split())
    if not q:
        return "UNKNOWN"
    op = q.split(" ", 1)[0].upper()
    table = "?"
    for pat, _tag in _TABLE_PATTERNS:
        m = pat.search(q)
        if m:
            table = m.group(1)
            break
    return f"{op} {table}"


def _maybe_emit(now: float) -> None:
    interval = _emit_interval_sec()
    if (now - float(_state.get("last_emit_ts", 0.0) or 0.0)) < interval:
        return
    _state["last_emit_ts"] = now

    by_sig = _state.get("by_sig") or {}
    ranked = sorted(
        by_sig.items(),
        key=lambda kv: (float(kv[1].get("sum_ms", 0.0) or 0.0), int(kv[1].get("count", 0) or 0)),
        reverse=True,
    )
    top = ranked[: _topn()]
    top_s = ", ".join(
        f"{sig}:cnt={st['count']} err={st['errors']} slow={st['slow']} avg={((st['sum_ms']/st['count']) if st['count'] else 0):.1f}ms max={st['max_ms']:.1f}ms"
        for sig, st in top
    )
    total_count = int(_state.get("total_count", 0) or 0)
    total_errors = int(_state.get("total_errors", 0) or 0)
    total_slow = int(_state.get("total_slow", 0) or 0)
    sum_ms = float(_state.get("sum_ms", 0.0) or 0.0)
    avg_ms = (sum_ms / total_count) if total_count else 0.0
    logger.warning(
        "[DB-OVERLOAD] summary | total=%s err=%s slow=%s avg=%.1fms top=[%s]",
        total_count,
        total_errors,
        total_slow,
        avg_ms,
        top_s,
    )


def record_db_query(*, query: str, dbname: Optional[str], elapsed_ms: float, ok: bool, fetch: bool, error: Optional[Exception] = None) -> None:
    if not _enabled():
        return
    sig = _query_signature(query)
    now = time.monotonic()
    slow_th = _slow_ms()

    st = _state["by_sig"][sig]
    st["count"] += 1
    st["sum_ms"] += float(elapsed_ms or 0.0)
    if elapsed_ms > float(st["max_ms"]):
        st["max_ms"] = float(elapsed_ms)
    _state["total_count"] += 1
    _state["sum_ms"] += float(elapsed_ms or 0.0)

    if (not ok) and error is not None:
        st["errors"] += 1
        _state["total_errors"] += 1

    if elapsed_ms >= slow_th:
        st["slow"] += 1
        _state["total_slow"] += 1
        logger.warning(
            "[DB-OVERLOAD] slow query | db=%s sig=%s elapsed=%.1fms fetch=%s ok=%s err=%s",
            dbname,
            sig,
            elapsed_ms,
            fetch,
            ok,
            str(error)[:240] if error else "",
        )

    _maybe_emit(now)

