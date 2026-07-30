from typing import Dict

from fastapi import Request

from config.settings import settings


def build_cors_headers(request: Request) -> Dict[str, str]:
    """
    CORS 미들웨어가 누락된 응답에 대한 보정용 헤더 생성.
    - 설정된 allowlist/regex 기반으로 허용 origin을 계산한다.
    """
    headers: Dict[str, str] = {}
    try:
        origin = request.headers.get("origin")
    except Exception:
        origin = None
    try:
        allowlist = set(getattr(settings, "CORS_ORIGINS", []) or [])
    except Exception:
        allowlist = set()
    try:
        allow_all = "*" in allowlist or allowlist == {"*"}
    except Exception:
        allow_all = False

    if allow_all:
        if origin:
            headers["Access-Control-Allow-Origin"] = origin
            headers["Vary"] = "Origin"
        else:
            headers["Access-Control-Allow-Origin"] = "*"
    elif origin:
        # allowlist 또는 regex 허용
        if origin in allowlist:
            headers["Access-Control-Allow-Origin"] = origin
            headers["Vary"] = "Origin"
        else:
            try:
                import re

                pattern = getattr(settings, "CORS_ORIGIN_REGEX_PATTERN", None)
                if pattern and re.match(pattern, origin):
                    headers["Access-Control-Allow-Origin"] = origin
                    headers["Vary"] = "Origin"
            except Exception:
                pass

    if "Access-Control-Allow-Origin" in headers:
        if not allow_all:
            headers["Access-Control-Allow-Credentials"] = "true"
        headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With"
    return headers

