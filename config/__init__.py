# config/__init__.py
"""
Compatibility shim: expose `settings` and `Config` for older imports.
Many modules expect `from config import Config`. Provide `Config` alias
pointing to the singleton `settings` instance for compatibility.
"""
from .settings import settings, Settings
from .constants import *

# Backwards-compatible alias: some code imports `Config`
Config = settings

__all__ = ["settings", "Config", "Settings"]
