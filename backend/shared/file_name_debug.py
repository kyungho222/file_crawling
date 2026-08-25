from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Optional


_ROOT = Path(__file__).resolve().parents[2]
_LOG_DIR = _ROOT / "logs"
_LOG_PATH = _LOG_DIR / "filename_debug.log"
_SENTINEL_PATH = _LOG_DIR / "filename_debug.on"


def file_name_debug_enabled() -> bool:
    return False


def file_name_debug_log_path() -> str:
    return str(_LOG_PATH)


def emit_file_name_debug(
    *,
    component: str,
    location: str,
    data: Optional[dict[str, Any]] = None,
    logger: Any = None,
) -> None:
    if not file_name_debug_enabled():
        return

    payload = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "pid": os.getpid(),
        "component": component,
        "location": location,
        "data": data or {},
    }

    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        with _LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass

    if logger is not None:
        try:
            logger.debug(
                "[FileNameDebug][%s][%s] %s",
                component,
                location,
                json.dumps(data or {}, ensure_ascii=False, default=str),
            )
        except Exception:
            pass
