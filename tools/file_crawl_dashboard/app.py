from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from tools.file_crawl_dashboard.integration import include_public_routes


app = FastAPI(title="File Crawl Dashboard", version="1.0.0")
include_public_routes(app)


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/file-crawl-dashboard")
