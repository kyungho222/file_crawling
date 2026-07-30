from __future__ import annotations

import logging
import os
from typing import Any


logger = logging.getLogger("tools.file_dashboard.integration")


def is_enabled() -> bool:
    raw = str(os.getenv("FILE_DASHBOARD_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def include_public_routes(app: Any) -> bool:
    if not is_enabled():
        logger.info("[파일대시보드] 공개 라우트 비활성화 | env=FILE_DASHBOARD_ENABLED")
        return False

    from tools.file_dashboard.router import router as public_router

    app.include_router(public_router)
    logger.info("[파일대시보드] 공개 라우트 등록 완료")
    return True


def include_backend_routes(host_router: Any) -> bool:
    if not is_enabled():
        logger.info("[파일대시보드] 백엔드 API 라우트 비활성화 | env=FILE_DASHBOARD_ENABLED")
        return False

    from tools.file_dashboard.router import api_router

    host_router.include_router(api_router)
    logger.info("[파일대시보드] 백엔드 API 라우트 등록 완료")
    return True
