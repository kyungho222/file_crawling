"""Regression check for file-crawl start URL summary diagnostics."""

from __future__ import annotations

import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


from backend.shared.file_crawl_post_urls import build_file_crawl_start_url_summary


def main() -> None:
    summary = build_file_crawl_start_url_summary(
        db_name="sample",
        chat_bot_id="bot-1",
        job_id="job-1",
        sql_rows=12,
        scanned=10,
        deduped=2,
        domain_skipped=1,
        missing_query_skipped=3,
        query_pattern_skipped=1,
        final_start_urls=3,
        record_type="page",
        require_active=True,
        dedupe_urls=True,
        date_filter_enabled=True,
        target_domains=["example.go.kr"],
        path_prefix="/board",
        learn_list_id=42,
    )

    assert summary["job_id"] == "job-1"
    assert summary["sql_rows"] == 12
    assert summary["final_start_urls"] == 3
    assert summary["filters"] == {
        "record_type": "page",
        "active_only": True,
        "dedupe_urls": True,
        "date_filter_enabled": True,
        "target_domains": ["example.go.kr"],
        "path_prefix": "/board",
        "learn_list_id": 42,
    }
    print("file crawl start URL summary contract ok")


if __name__ == "__main__":
    main()
