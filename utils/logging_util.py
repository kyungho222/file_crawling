import logging
import threading
from typing import Optional
import contextvars


class LoggerSingleton:
    _instance = None
    _lock = threading.Lock()
    _loggers = {}

    @classmethod
    def get_logger(cls, logger_name="default", level=logging.INFO):
        with cls._lock:
            if logger_name not in cls._loggers:
                logger = logging.getLogger(logger_name)
                logger.setLevel(level)
                logger.propagate = False  # 부모 로거로의 전파 방지

                if not logger.handlers:
                    console_handler = logging.StreamHandler()
                    console_handler.setLevel(level)
                    formatter = logging.Formatter(
                        "%(asctime)s - %(name)s - %(levelname)s - [press=%(press_name)s] - [%(filename)s:%(lineno)d] - %(message)s"
                    )
                    console_handler.setFormatter(formatter)
                    # press_name 컨텍스트 필터 추가
                    console_handler.addFilter(_PressContextFilter())
                    logger.addHandler(console_handler)

                cls._loggers[logger_name] = logger

            return cls._loggers[logger_name]


# === Logging Context: press_name ===
_press_name_ctx: contextvars.ContextVar[str] = contextvars.ContextVar("press_name", default="-")


class _PressContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            value = _press_name_ctx.get()
        except Exception:
            value = "-"
        # record에 press_name 속성이 없으면 주입
        if not hasattr(record, "press_name"):
            setattr(record, "press_name", value if value else "-")
        return True


def set_press_name(press_name: Optional[str]) -> None:
    """현재 비동기 컨텍스트에 언론사명을 설정한다."""
    try:
        _press_name_ctx.set((press_name or "").strip() or "-")
    except Exception:
        _press_name_ctx.set("-")


def clear_press_name() -> None:
    """현재 비동기 컨텍스트의 언론사명을 초기화한다."""
    try:
        _press_name_ctx.set("-")
    except Exception:
        pass
