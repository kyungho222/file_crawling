from __future__ import annotations

import logging
import os
from typing import Any


logger = logging.getLogger("tools.file_crawl_dashboard.integration")


def is_enabled() -> bool:
    raw = str(os.getenv("FILE_CRAWL_DASHBOARD_ENABLED", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def include_public_routes(app: Any) -> bool:
    if not is_enabled():
        logger.info("[FileCrawlDashboard] public routes disabled by FILE_CRAWL_DASHBOARD_ENABLED")
        return False

    from tools.file_crawl_dashboard.router import router as public_router

    app.include_router(public_router)
    logger.info("[FileCrawlDashboard] public routes included")
    return True
