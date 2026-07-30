from __future__ import annotations

from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = MODULE_DIR / "frontend"
DASHBOARD_HTML_NAME = "file_dashboard.html"
SEED_URLS_COUNT_HTML_NAME = "seed_urls_count.html"


def dashboard_html_path() -> Path:
    return FRONTEND_DIR / DASHBOARD_HTML_NAME


def seed_urls_count_html_path() -> Path:
    return FRONTEND_DIR / SEED_URLS_COUNT_HTML_NAME
