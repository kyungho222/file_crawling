# File Crawl Runtime Harness

`scripts/verify_file_crawl_runtime.py` replays file-crawl detail extraction and the
production `download_worker` without writing to DB or Redis. Learning and web
storage synchronization are also disabled. Downloaded files are isolated under
`tmp/file_crawl_harness/<job-id>` by default.

## Local self-test

The self-test starts a local HTTP fixture with 35 attachments. It injects one
HTTP 500 response and one response that exceeds the worker HTTP timeout. With
the default queue size of 30, the report must show producer backpressure, 33
successful downloads, two explicit skips, and terminal handling for all 35
attachments. The timeout case verifies that one stuck download does not leave
later queue items unprocessed.

```powershell
python scripts\verify_file_crawl_runtime.py --self-test
```

## Replay a detail page

Extraction only is the default:

```powershell
python scripts\verify_file_crawl_runtime.py `
  --post-url "https://example.go.kr/board/detail?id=1"
```

Run the production download worker into the isolated local directory:

```powershell
python scripts\verify_file_crawl_runtime.py `
  --post-url "https://example.go.kr/board/detail?id=1" `
  --download
```

## Replay a captured payload

Supported URL keys include `post_url`, `post_urls`, `detail_url`, `url_list`,
`start_urls`, and `target_urls`. Attachment records may be supplied under
`attachments`, `files`, or `file_urls`.

```powershell
python scripts\verify_file_crawl_runtime.py `
  --payload tmp\runtime_case.json `
  --download `
  --output tmp\runtime_case_report.json
```

Remove passwords, API keys, cookies, and authorization headers before saving an
operation payload. The harness ignores DB connection fields and does not call
DB, Redis, learning, or web-storage APIs.

## Offline HTML replay

```powershell
python scripts\verify_file_crawl_runtime.py `
  --html-file tmp\detail.html `
  --base-url "https://example.go.kr/board/detail?id=1"
```

## Result interpretation

The console and JSON report both contain three explicit sections:

- tests: What was tested, its expected result, actual result, and pass/fail.
- observations: Download errors, extraction failures, filtering, queue
  backpressure, and whether an event was intentionally injected by the harness.
- result: passed, needs_review, or failed.

For the self-test, the HTTP 500 is an expected_failure, not a product defect.
The queue-full observation is expected too: it proves that producer
backpressure pauses detail exploration until a download worker consumes work.

For a live URL replay, warning or error observations are unexpected and include
the relevant post URL, file URL, stage, reason, and detail.
