from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Mapping, Optional



def _crawl_trace_verbose_enabled() -> bool:
    return str(os.getenv("CRAWL_TRACE_VERBOSE", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}


def _crawl_trace_delay_other_enabled() -> bool:
    return str(os.getenv("CRAWL_TRACE_DELAY_OTHER", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}

def elapsed_ms(start: Optional[float]) -> float:
    if not start:
        return 0.0
    return (time.perf_counter() - float(start)) * 1000.0


def _compact(value: Any, *, limit: int = 220) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.replace("\r", " ").replace("\n", " ").strip()
        return text[:limit] if len(text) > limit else text
    if isinstance(value, Mapping):
        return {str(k): _compact(v, limit=limit) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_compact(v, limit=limit) for v in list(value)[:12]]
    return value


def _format_number(value: Any, *, digits: int = 1) -> Optional[str]:
    try:
        number = float(value)
    except Exception:
        return None
    text = f"{number:.{max(0, int(digits))}f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _fmt(value: Any, *, key: str = "") -> str:
    value = _compact(value)
    key_l = str(key or "").lower()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if key_l.endswith("_ms"):
            formatted = _format_number(value, digits=1)
            if formatted is not None:
                return formatted
        if key_l.endswith("_sec") or key_l.endswith("_seconds"):
            formatted = _format_number(value, digits=1)
            if formatted is not None:
                return formatted
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
        except Exception:
            return str(value)
    return str(value)


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _delay_cause(
    *,
    phase: str,
    action: str,
    state: str,
    level: int,
    elapsed_ms: Any,
    queue_wait_ms: Any,
    fields: Mapping[str, Any],
) -> str:
    action_l = str(action or "").strip().lower()
    phase_l = str(phase or "").strip().lower()
    state_l = str(state or "").strip().lower()
    slow = bool(fields.get("slow"))
    elapsed_value = _as_float(elapsed_ms)
    queue_wait_value = _as_float(queue_wait_ms)
    throttle_wait_value = _as_float(fields.get("throttle_wait_ms"))
    duplicate_ms_value = _as_float(fields.get("duplicate_ms"))

    is_delay_log = (
        level >= logging.WARNING
        or slow
        or state_l in {"slow", "timeout", "timeout_overrun", "discard_timeout_overrun_result", "queue_timeout"}
        or (elapsed_value is not None and elapsed_value >= 1000.0)
        or (queue_wait_value is not None and queue_wait_value >= 1000.0)
    )
    if not is_delay_log:
        return ""

    if action_l == "domain_fetch_guard":
        if state_l == "queue_timeout" or (queue_wait_value is not None and queue_wait_value >= 1000.0):
            return "domain_queue"
        if throttle_wait_value is not None and throttle_wait_value >= 500.0:
            return "domain_throttle"
        return "domain_guard"
    if action_l in {"static_fetch_response", "static_fetch_body_read"}:
        return "fetch_io"
    if action_l.endswith("_fetch") or "fetch" in action_l:
        return "fetch"
    if action_l in {"detail_preparse_soup", "detail_parse", "detail_process"} or "soup" in action_l:
        return "parse"
    if phase_l == "selection" and action_l in {"detail_prefetch", "detail_fetch"}:
        return "parse_or_empty"
    if action_l in {"learn_list_duplicate_lookup", "detail_db_duplicate_context"}:
        return "db_duplicate"
    if action_l.startswith("learn_list_insert") or action_l.startswith("learn_list_batch"):
        if duplicate_ms_value is not None and duplicate_ms_value >= 1000.0:
            return "db_duplicate"
        return "db_save"
    if phase_l == "save":
        return "db_save"
    if phase_l == "redis":
        return "redis_sse"
    return "other"



def _format_trace_summary(
    *,
    phase: str,
    action: str,
    state: str,
    cause: str,
    elapsed_ms: Any,
    queue_wait_ms: Any,
    job_id: Any,
    url: Any,
    fields: Mapping[str, Any],
) -> str:
    label = str(cause or "").strip() or "\ucd94\uc801"
    label_map = {
        "other": "\uc694\uc57d",
        "fetch": "\uac00\uc838\uc624\uae30",
        "fetch_io": "\uac00\uc838\uc624\uae30IO",
        "parse": "\ud30c\uc2f1",
        "parse_or_empty": "\ud30c\uc2f1\ub610\ub294\ube48\ubcf8\ubb38",
        "db_duplicate": "DB\uc911\ubcf5\uac80\uc0ac",
        "db_save": "DB\uc800\uc7a5",
        "redis_sse": "Redis/SSE",
        "domain_queue": "\ub3c4\uba54\uc778\ub300\uae30",
        "domain_throttle": "\ub3c4\uba54\uc778\ub300\uae30\uc2dc\uac04",
        "domain_guard": "\ub3c4\uba54\uc778\uac00\ub4dc",
    }
    label = label_map.get(label, label)
    lines = ["[\ud06c\ub864\ub9c1\uc694\uc57d]"]
    lines.append(f"\uc885\ub958={label}")
    lines.append(f"\ub2e8\uacc4={phase}.{action}")
    lines.append(f"\uc0c1\ud0dc={state}")
    if job_id not in (None, ""):
        lines.append(f"\uc791\uc5c5ID={job_id}")
    if elapsed_ms is not None:
        try:
            lines.append(f"\uc18c\uc694={float(elapsed_ms) / 1000.0:.1f}\ucd08")
        except Exception:
            lines.append(f"\uc18c\uc694ms={elapsed_ms}")
    if queue_wait_ms is not None:
        try:
            lines.append(f"\ud050\ub300\uae30={float(queue_wait_ms) / 1000.0:.1f}\ucd08")
        except Exception:
            lines.append(f"\ud050\ub300\uae30ms={queue_wait_ms}")
    reason = fields.get("reason") or fields.get("status") or fields.get("message")
    if reason:
        lines.append(f"\uc0ac\uc720={_fmt(reason, key='reason')}")
    counts = fields.get("counts")
    if counts is not None:
        lines.append(f"\uc9d1\uacc4={_fmt(counts, key='counts')}")
    if url:
        lines.append(f"URL={_fmt(url, key='url')}")
    return "\n".join(lines)

def crawl_trace(
    logger: logging.Logger,
    *,
    phase: str,
    action: str,
    state: str,
    job_id: Any = None,
    level: int = logging.INFO,
    elapsed_ms: Any = None,
    queue_wait_ms: Any = None,
    workers: Any = None,
    counts: Any = None,
    url: Any = None,
    **fields: Any,
) -> None:
    if not logger:
        return
    action_l = str(action or "").strip().lower()
    state_l = str(state or "").strip().lower()
    channel_l = str(fields.get("channel") or "").strip().lower()
    if (
        action_l == "domain_fetch_guard"
        and state_l == "queue_timeout"
        and channel_l in {"", "http"}
        and str(os.getenv("BOARD_DOMAIN_QUEUE_TIMEOUT_WARNING", "0") or "0").strip().lower()
        not in {"1", "true", "yes", "on"}
    ):
        level = min(int(level), logging.INFO)
    if state_l == "start":
        level = logging.DEBUG
    parts = [
        f"phase={phase}",
        f"action={action}",
        f"state={state}",
    ]
    if job_id not in (None, ""):
        parts.append(f"job_id={job_id}")
    if elapsed_ms is not None:
        try:
            elapsed_value = float(elapsed_ms)
            parts.append(f"elapsed_ms={elapsed_value:.1f}")
            if elapsed_value >= 1000.0:
                parts.append(f"time_sec={elapsed_value / 1000.0:.1f}")
        except Exception:
            parts.append(f"elapsed_ms={elapsed_ms}")
    if queue_wait_ms is not None:
        try:
            queue_wait_value = float(queue_wait_ms)
            parts.append(f"queue_wait_ms={queue_wait_value:.1f}")
            if queue_wait_value >= 1000.0:
                parts.append(f"queue_wait_sec={queue_wait_value / 1000.0:.1f}")
        except Exception:
            parts.append(f"queue_wait_ms={queue_wait_ms}")
    if workers is not None:
        parts.append(f"workers={_fmt(workers, key='workers')}")
    if counts is not None:
        parts.append(f"counts={_fmt(counts, key='counts')}")
    if url:
        parts.append(f"url={_fmt(url, key='url')}")
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={_fmt(value, key=key)}")
    cause = _delay_cause(
        phase=phase,
        action=action,
        state=state,
        level=level,
        elapsed_ms=elapsed_ms,
        queue_wait_ms=queue_wait_ms,
        fields=fields,
    )
    if level < logging.WARNING:
        level = logging.DEBUG
        if not _crawl_trace_verbose_enabled():
            return
    if str(os.getenv("CRAWL_TRACE_COMPACT_SUMMARY", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}:
        logger.log(
            level,
            _format_trace_summary(
                phase=phase,
                action=action,
                state=state,
                cause=cause,
                elapsed_ms=elapsed_ms,
                queue_wait_ms=queue_wait_ms,
                job_id=job_id,
                url=url,
                fields=fields,
            ),
        )
        return
    prefix = f"[DelayCause:{cause}] " if cause else ""
    logger.log(level, prefix + "[CrawlTrace] " + " ".join(parts))

