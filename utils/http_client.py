from __future__ import annotations

import os
from typing import Optional

import requests


_TRUTHY = {"1", "true", "yes", "y", "on"}


def _env_trust_env(name: str, default: str = "0") -> bool:
    try:
        return str(os.getenv(name, default)).strip().lower() in _TRUTHY
    except Exception:
        return default in _TRUTHY


def get_requests_session(*, headers: Optional[dict] = None, trust_env: Optional[bool] = None) -> requests.Session:
    """
    requests 세션 생성 (기본: 환경 프록시 비활성화).
    - CRAWLER_REQUESTS_TRUST_ENV=1 로 설정 시 환경 프록시 사용.
    """
    session = requests.Session()
    if headers:
        session.headers.update(headers)
    if trust_env is None:
        trust_env = _env_trust_env("CRAWLER_REQUESTS_TRUST_ENV", "0")
    session.trust_env = bool(trust_env)
    if not session.trust_env:
        session.proxies = {}
    return session


def requests_get(
    url: str,
    *,
    headers: Optional[dict] = None,
    timeout: Optional[float] = None,
    allow_redirects: bool = True,
    trust_env: Optional[bool] = None,
    **kwargs,
) -> requests.Response:
    """
    프록시 환경 변수에 의존하지 않는 요청 헬퍼.
    """
    session = get_requests_session(headers=headers, trust_env=trust_env)
    try:
        return session.get(url, timeout=timeout, allow_redirects=allow_redirects, **kwargs)
    finally:
        try:
            session.close()
        except Exception:
            pass


def get_aiohttp_session(
    *,
    headers: Optional[dict] = None,
    timeout: Optional[object] = None,
    connector: Optional[object] = None,
    trust_env: Optional[bool] = None,
    **kwargs,
):
    """
    aiohttp 세션 생성 (기본: 환경 프록시 비활성화).
    - CRAWLER_AIOHTTP_TRUST_ENV=1 로 설정 시 환경 프록시 사용.
    """
    import aiohttp  # local import for optional dependency

    if trust_env is None:
        trust_env = _env_trust_env("CRAWLER_AIOHTTP_TRUST_ENV", "0")
    return aiohttp.ClientSession(
        headers=headers,
        timeout=timeout,
        connector=connector,
        trust_env=bool(trust_env),
        **kwargs,
    )

# --- Optional shared aiohttp session / connector (app-level reuse) ---
_global_aiohttp_connector = None
_global_aiohttp_session = None

def init_global_aiohttp(*, limit: int = 100, enable_cleanup_closed: bool = True, **connector_kwargs) -> None:
    """
    Initialize a shared aiohttp connector/session for reuse across the app.
    Call this at application startup if you want a global session.
    """
    global _global_aiohttp_connector, _global_aiohttp_session
    try:
        import aiohttp
    except Exception:
        return
    if _global_aiohttp_connector is None:
        try:
            _global_aiohttp_connector = aiohttp.TCPConnector(limit=limit, enable_cleanup_closed=enable_cleanup_closed, **connector_kwargs)
        except Exception:
            try:
                _global_aiohttp_connector = aiohttp.TCPConnector(limit=limit, enable_cleanup_closed=enable_cleanup_closed)  # type: ignore[arg-type]
            except Exception:
                _global_aiohttp_connector = None
    if _global_aiohttp_session is None and _global_aiohttp_connector is not None:
        try:
            _global_aiohttp_session = aiohttp.ClientSession(connector=_global_aiohttp_connector)
        except Exception:
            _global_aiohttp_session = None

async def close_global_aiohttp() -> None:
    """
    Close the shared aiohttp session and connector. Call at application shutdown.
    """
    global _global_aiohttp_session, _global_aiohttp_connector
    try:
        if _global_aiohttp_session is not None:
            try:
                await _global_aiohttp_session.close()
            except Exception:
                pass
            _global_aiohttp_session = None
    except Exception:
        pass
    try:
        if _global_aiohttp_connector is not None:
            try:
                await _global_aiohttp_connector.close()
            except Exception:
                pass
            _global_aiohttp_connector = None
    except Exception:
        pass

def get_shared_aiohttp_session():
    """Return the shared aiohttp.ClientSession if initialized, else None."""
    return _global_aiohttp_session

