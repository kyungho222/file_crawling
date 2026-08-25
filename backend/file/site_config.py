"""Domain-scoped selector configuration for the file crawler.

The canonical location is ``services/site_config/domains/<domain>.json`` at the
repository root.  Loaders treat absent or malformed files as optional so a
site configuration never blocks the common extraction fallback.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union
from urllib.parse import urlparse


_DOMAIN_CONFIG_DIR = Path(__file__).resolve().parents[2] / "services" / "site_config" / "domains"
_CONFIG_CACHE: Dict[str, Tuple[int, Dict[str, Any]]] = {}


def canonical_file_site_domain(url_or_domain: str) -> str:
    """Return the one filename-safe domain key used by file crawler configs."""
    raw = str(url_or_domain or "").strip().lower()
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"//{raw}")
    domain = (parsed.hostname or "").strip().lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def file_site_config_path(
    url_or_domain: str,
    *,
    config_dir: Optional[Union[str, Path]] = None,
) -> Path:
    domain = canonical_file_site_domain(url_or_domain)
    if not domain:
        raise ValueError("A domain is required for file site configuration")
    root = Path(config_dir) if config_dir is not None else _DOMAIN_CONFIG_DIR
    return root / f"{domain}.json"


def load_file_site_config(
    url_or_domain: str,
    *,
    config_dir: Optional[Union[str, Path]] = None,
) -> Dict[str, Any]:
    """Load the current domain JSON, returning an empty config on any failure."""
    try:
        path = file_site_config_path(url_or_domain, config_dir=config_dir)
        stat = path.stat()
    except (OSError, ValueError):
        return {}

    cache_key = str(path)
    cached = _CONFIG_CACHE.get(cache_key)
    if cached and cached[0] == stat.st_mtime_ns:
        return dict(cached[1])

    try:
        with path.open("r", encoding="utf-8") as config_file:
            data = json.load(config_file)
        config = data if isinstance(data, dict) else {}
    except (OSError, TypeError, ValueError):
        config = {}
    _CONFIG_CACHE[cache_key] = (stat.st_mtime_ns, config)
    return dict(config)


def save_file_site_config(
    url_or_domain: str,
    config: Dict[str, Any],
    *,
    config_dir: Optional[Union[str, Path]] = None,
) -> str:
    """Persist an explicitly supplied domain selector config in the canonical path."""
    if not isinstance(config, dict):
        raise TypeError("File site configuration must be a JSON object")
    path = file_site_config_path(url_or_domain, config_dir=config_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as config_file:
            json.dump(config, config_file, ensure_ascii=False, indent=2)
            config_file.write("\n")
        os.replace(temporary_path, path)
    finally:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
    _CONFIG_CACHE.pop(str(path), None)
    return str(path)
