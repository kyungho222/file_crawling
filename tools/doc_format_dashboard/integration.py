from __future__ import annotations

import logging
import os
from typing import Any


logger = logging.getLogger("tools.doc_format_dashboard.integration")


def is_enabled() -> bool:
    raw = str(os.getenv("DOC_FORMAT_DASHBOARD_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def include_public_routes(app: Any) -> bool:
    if not is_enabled():
        logger.info("[DocFormatDashboard] public routes disabled by DOC_FORMAT_DASHBOARD_ENABLED")
        return False

    from tools.doc_format_dashboard.router import api_router, router as public_router

    app.include_router(public_router)
    app.include_router(api_router)
    logger.info("[DocFormatDashboard] public routes included")
    return True


def include_backend_routes(host_router: Any) -> bool:
    if not is_enabled():
        logger.info("[DocFormatDashboard] backend API routes disabled by DOC_FORMAT_DASHBOARD_ENABLED")
        return False

    from tools.doc_format_dashboard.router import api_router

    host_router.include_router(api_router)
    logger.info("[DocFormatDashboard] backend API routes included")
    return True
