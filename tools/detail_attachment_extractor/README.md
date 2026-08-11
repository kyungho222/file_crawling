# Detail Attachment Extractor

This folder isolates the detail-page attachment discovery portion of file crawling.

```powershell
python tools\detail_attachment_extractor\extract_detail_attachments.py "https://example.go.kr/board/view.do?seq=1"
```

The command prints JSON with the detail URL and discovered document attachment URLs. It does not access the database, create queues, download files, save files, or start learning.

## Dashboard

```powershell
Set-Location tools\detail_attachment_extractor
python -m uvicorn dashboard_server:app --host 127.0.0.1 --port 8091
```

Open `http://127.0.0.1:8091` in a browser.

`source_reference/` contains copies of the original project modules used as the extraction reference:

- `fast_attachment_extractor.py`
- `fast_attachment_producer.py`
