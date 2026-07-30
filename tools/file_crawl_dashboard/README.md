# File Crawl Dashboard

Standalone dashboard for starting and monitoring `colle=file` crawls.

Public route:

```text
/file-crawl-dashboard
```

The page calls existing backend APIs:

```text
POST /Ai_Pro_filecrawler/backend/session/start
GET  /Ai_Pro_filecrawler/c1/crawl_sse/{db_name}/{job_id}
POST /Ai_Pro_filecrawler/c1/crawl_stop/{job_id}
POST /Ai_Pro_filecrawler/backend/file/preview-homepage-categories
POST /Ai_Pro_filecrawler/backend/file/sync-homepage-categories
```

Disable route registration with:

```text
FILE_CRAWL_DASHBOARD_ENABLED=0
```

## Local pre-learn test server

Run the dashboard without mounting the full production `backend.app`:

```powershell
python scripts\run_file_crawl_dashboard_local.py --host 127.0.0.1 --port 8013
```

Open:

```text
http://127.0.0.1:8013/file-crawl-dashboard
```

This local server serves the existing dashboard HTML and provides compatibility endpoints for:

```text
POST /Ai_Pro_filecrawler/backend/session/start
GET  /Ai_Pro_filecrawler/c1/crawl_sse/{db_name}/{job_id}
POST /Ai_Pro_filecrawler/c1/crawl_stop/{job_id}
```

The local dashboard request is forced to `file_pipeline_skip_learning=true` and `enable_learning=false` so the crawl stops before PG learning.