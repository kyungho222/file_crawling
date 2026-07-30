from __future__ import annotations

from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = MODULE_DIR / "frontend"
DASHBOARD_HTML_NAME = "doc_format_dashboard.html"


def dashboard_html_path() -> Path:
    return FRONTEND_DIR / DASHBOARD_HTML_NAME
