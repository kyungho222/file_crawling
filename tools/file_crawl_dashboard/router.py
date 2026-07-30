from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from tools.file_crawl_dashboard.paths import dashboard_html_path


router = APIRouter(prefix="/file-crawl-dashboard", tags=["file-crawl-dashboard"])


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def file_crawl_dashboard_page() -> HTMLResponse:
    html_path = dashboard_html_path()
    return HTMLResponse(
        html_path.read_text(encoding="utf-8"),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )
