"""Compatibility package: expose backend.config namespace that forwards to top-level config."""
try:
    # 기존 코드와의 호환을 위해 backend.config.Config를 참조 가능하게 합니다.
    from config.settings import Config  # type: ignore
    __all__ = ["Config", "settings"]
except Exception:
    # 실패 시 기존 동작(단순 노출) 유지
    __all__ = ["settings"]

