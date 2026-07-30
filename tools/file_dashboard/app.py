from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from tools.file_dashboard.integration import include_backend_routes, include_public_routes


app = FastAPI(title="파일 대시보드", version="1.0.0")
include_public_routes(app)
include_backend_routes(app)


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/file-dashboard")
