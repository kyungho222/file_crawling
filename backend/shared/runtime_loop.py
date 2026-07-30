import asyncio
import faulthandler
import importlib.metadata
import logging
import os
import shlex
import signal
import sys
import traceback
from typing import Iterable
from typing import Any, Dict, Optional


_TRUTHY = {"1", "true", "yes", "on"}
_VALID_UVICORN_LOOPS = {"auto", "asyncio", "uvloop"}
_FAULTHANDLER_INITIALIZED = False


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in _TRUTHY


def _log_unsafe_uvloop_detection() -> bool:
    """
    Linux에서 uvloop가 감지되더라도 기본적으로는 무음 self-heal 한다.
    운영 진단이 필요할 때만 env로 로그를 켠다.
    """
    return _env_flag("RUNTIME_LOOP_LOG_UNSAFE_UVLOOP", False)


def _quoted_argv(argv: Iterable[str]) -> str:
    try:
        return " ".join(shlex.quote(str(part)) for part in argv)
    except Exception:
        return " ".join(str(part) for part in argv)


def _package_version(name: str) -> Optional[str]:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None
    except Exception:
        return None


def is_linux() -> bool:
    return sys.platform.startswith("linux")


def is_windows() -> bool:
    return sys.platform == "win32"


def resolve_uvicorn_loop_mode() -> str:
    raw = str(os.getenv("UVICORN_LOOP", "") or "").strip().lower()
    if raw in _VALID_UVICORN_LOOPS:
        return raw
    if is_linux() and _env_flag("UVICORN_FORCE_ASYNCIO_ON_LINUX", True):
        return "asyncio"
    return "auto"


def apply_runtime_event_loop_policy(logger: Optional[logging.Logger] = None) -> None:
    if is_windows():
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            if logger:
                logger.info("[RuntimeLoop] WindowsProactorEventLoopPolicy enabled")
        except AttributeError:
            asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
            if logger:
                logger.info("[RuntimeLoop] DefaultEventLoopPolicy enabled on Windows fallback")
        return

    if is_linux() and resolve_uvicorn_loop_mode() == "asyncio":
        try:
            asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
            if logger:
                logger.info("[RuntimeLoop] Linux asyncio event loop policy forced")
        except Exception as exc:
            if logger:
                logger.warning("[RuntimeLoop] failed to force Linux asyncio policy: %s", exc)


def enable_runtime_faulthandler(logger: Optional[logging.Logger] = None) -> None:
    global _FAULTHANDLER_INITIALIZED

    if _FAULTHANDLER_INITIALIZED:
        return
    if not _env_flag("ENABLE_PYTHON_FAULTHANDLER", True):
        return

    try:
        os.environ.setdefault("PYTHONFAULTHANDLER", "1")
        if not faulthandler.is_enabled():
            faulthandler.enable(file=sys.stderr, all_threads=True)
        if hasattr(signal, "SIGUSR1"):
            try:
                faulthandler.register(signal.SIGUSR1, file=sys.stderr, all_threads=True, chain=True)
            except (OSError, RuntimeError, ValueError):
                pass
        _FAULTHANDLER_INITIALIZED = True
        if logger:
            logger.info("[RuntimeLoop] faulthandler enabled")
    except Exception as exc:
        if logger:
            logger.warning("[RuntimeLoop] failed to enable faulthandler: %s", exc)


def _should_suppress_windows_closed_pipe_unraisable(unraisable: Any) -> bool:
    if not is_windows():
        return False

    exc = getattr(unraisable, "exc_value", None)
    if not isinstance(exc, ValueError):
        return False
    if "closed pipe" not in str(exc).lower():
        return False

    err_msg = str(getattr(unraisable, "err_msg", "") or "").lower()
    if "deallocator" not in err_msg:
        return False

    tb = getattr(unraisable, "exc_traceback", None)
    if tb is None:
        return False

    try:
        files = {
            str(frame.filename).replace("/", "\\").lower()
            for frame in traceback.extract_tb(tb)
        }
    except Exception:
        return False

    return (
        any(path.endswith("\\asyncio\\base_subprocess.py") for path in files)
        and any(path.endswith("\\asyncio\\windows_utils.py") for path in files)
    )


def enable_windows_asyncio_unraisable_filter(logger: Optional[logging.Logger] = None) -> None:
    """
    Windows + Playwright 종료 시 간헐적으로 발생하는 asyncio closed-pipe deallocator 잡음만 숨긴다.
    실제 예외는 그대로 통과시킨다.
    """
    if not is_windows() or not hasattr(sys, "unraisablehook"):
        return

    prev_hook = sys.unraisablehook
    if getattr(prev_hook, "_suppresses_windows_asyncio_closed_pipe", False):
        return

    def _filtered_unraisablehook(unraisable) -> None:
        if _should_suppress_windows_closed_pipe_unraisable(unraisable):
            if logger and _env_flag("RUNTIME_LOOP_LOG_SUPPRESSED_UNRAISABLE", False):
                logger.debug("[RuntimeLoop] suppressed Windows asyncio closed-pipe unraisable")
            return
        prev_hook(unraisable)

    setattr(_filtered_unraisablehook, "_suppresses_windows_asyncio_closed_pipe", True)
    sys.unraisablehook = _filtered_unraisablehook


def initialize_runtime_hardening(
    logger: Optional[logging.Logger] = None,
    *,
    component: str = "runtime",
) -> None:
    apply_runtime_event_loop_policy(logger)
    enable_runtime_faulthandler(logger)
    enable_windows_asyncio_unraisable_filter(logger)
    if logger:
        snapshot = get_runtime_loop_snapshot(component=component)
        logger.info(
            "[RuntimeLoop] init component=%s configured_loop=%s force_asyncio_linux=%s "
            "uvloop_installed=%s uvloop_version=%s faulthandler=%s",
            snapshot["component"],
            snapshot["configured_loop"],
            snapshot["force_asyncio_on_linux"],
            snapshot["uvloop_installed"],
            snapshot["uvloop_version"],
            snapshot["faulthandler_enabled"],
        )


def get_runtime_loop_snapshot(*, component: str = "runtime") -> Dict[str, Any]:
    try:
        policy = asyncio.get_event_loop_policy()
        policy_name = type(policy).__name__
    except Exception as exc:
        policy_name = f"unavailable:{type(exc).__name__}"

    loop_name = None
    loop_module = None
    try:
        running_loop = asyncio.get_running_loop()
        loop_name = type(running_loop).__name__
        loop_module = type(running_loop).__module__
    except RuntimeError:
        loop_name = None
        loop_module = None
    except Exception as exc:
        loop_name = f"unavailable:{type(exc).__name__}"
        loop_module = None

    uvloop_version = _package_version("uvloop")
    return {
        "component": component,
        "pid": os.getpid(),
        "platform": sys.platform,
        "python": sys.version.split()[0],
        "configured_loop": resolve_uvicorn_loop_mode(),
        "force_asyncio_on_linux": _env_flag("UVICORN_FORCE_ASYNCIO_ON_LINUX", True),
        "event_loop_policy": policy_name,
        "running_loop_name": loop_name,
        "running_loop_module": loop_module,
        "uvloop_installed": bool(uvloop_version),
        "uvloop_version": uvloop_version,
        "faulthandler_enabled": faulthandler.is_enabled(),
    }


def log_runtime_loop_snapshot(
    logger: Optional[logging.Logger] = None,
    *,
    component: str = "runtime",
) -> None:
    if logger is None:
        return

    snapshot = get_runtime_loop_snapshot(component=component)
    logger.info(
        "[RuntimeLoop] state component=%s pid=%s platform=%s python=%s configured_loop=%s "
        "policy=%s running_loop=%s running_loop_module=%s uvloop_installed=%s uvloop_version=%s "
        "faulthandler=%s",
        snapshot["component"],
        snapshot["pid"],
        snapshot["platform"],
        snapshot["python"],
        snapshot["configured_loop"],
        snapshot["event_loop_policy"],
        snapshot["running_loop_name"],
        snapshot["running_loop_module"],
        snapshot["uvloop_installed"],
        snapshot["uvloop_version"],
        snapshot["faulthandler_enabled"],
    )

    if _is_unsafe_linux_uvloop(snapshot) and _log_unsafe_uvloop_detection():
        logger.critical(
            "[RuntimeLoop] uvloop is running on Linux although asyncio is configured. "
            "If this process was started via uvicorn CLI, force '--loop asyncio' or set UVICORN_LOOP=asyncio."
        )


def _is_unsafe_linux_uvloop(snapshot: Dict[str, Any]) -> bool:
    return (
        is_linux()
        and snapshot["configured_loop"] == "asyncio"
        and isinstance(snapshot["running_loop_module"], str)
        and snapshot["running_loop_module"].startswith("uvloop")
    )


def _rewrite_uvicorn_loop_args(argv: list[str]) -> list[str]:
    rewritten = list(argv)
    idx = 0
    while idx < len(rewritten):
        arg = rewritten[idx]
        if arg == "--loop" and idx + 1 < len(rewritten):
            rewritten[idx + 1] = "asyncio"
            return rewritten
        if isinstance(arg, str) and arg.startswith("--loop="):
            rewritten[idx] = "--loop=asyncio"
            return rewritten
        idx += 1
    rewritten.extend(["--loop", "asyncio"])
    return rewritten


def _build_uvicorn_reexec_argv() -> Optional[list[str]]:
    argv = list(sys.argv)
    if not argv:
        return None

    argv0 = os.path.basename(str(argv[0])).lower()
    if "uvicorn" in argv0:
        return [sys.executable, "-m", "uvicorn", *_rewrite_uvicorn_loop_args(argv[1:])]

    if len(argv) >= 3 and argv[1] == "-m" and str(argv[2]).lower() == "uvicorn":
        return [sys.executable, "-m", "uvicorn", *_rewrite_uvicorn_loop_args(argv[3:])]

    return None


def _maybe_reexec_into_safe_uvicorn_loop(
    logger: Optional[logging.Logger] = None,
    *,
    component: str,
) -> bool:
    if not _env_flag("UVICORN_RUNTIME_LOOP_REEXEC_ON_UNSAFE", True):
        return False
    if os.getenv("UVICORN_RUNTIME_LOOP_REEXEC_ATTEMPTED", "").strip() == "1":
        return False

    argv = _build_uvicorn_reexec_argv()
    if not argv:
        if logger:
            logger.warning(
                "[RuntimeLoop] unsafe uvloop detected at %s but current argv does not look like uvicorn; cannot self re-exec",
                component,
            )
        return False

    env = os.environ.copy()
    env["UVICORN_RUNTIME_LOOP_REEXEC_ATTEMPTED"] = "1"
    env["UVICORN_FORCE_ASYNCIO_ON_LINUX"] = "1"
    env["UVICORN_LOOP"] = "asyncio"

    if logger and _log_unsafe_uvloop_detection():
        logger.critical(
            "[RuntimeLoop] unsafe uvloop detected at %s; re-execing process with safe asyncio loop: %s",
            component,
            _quoted_argv(argv),
        )

    os.execvpe(sys.executable, argv, env)
    return True


def ensure_safe_runtime_loop(
    logger: Optional[logging.Logger] = None,
    *,
    component: str = "runtime",
) -> None:
    snapshot = get_runtime_loop_snapshot(component=component)
    if not _is_unsafe_linux_uvloop(snapshot):
        return

    message = (
        "Unsafe Linux runtime loop detected: uvloop is active although asyncio is required. "
        "Use 'uvicorn ... --loop asyncio' or set UVICORN_LOOP=asyncio before startup."
    )
    if logger and _log_unsafe_uvloop_detection():
        logger.critical("[RuntimeLoop] %s", message)
    if _maybe_reexec_into_safe_uvicorn_loop(logger, component=component):
        return
    if _env_flag("UVICORN_REJECT_UNSAFE_UVLOOP_ON_LINUX", True):
        raise RuntimeError(message)
