# Public SimHash

## Purpose

`public_simhash.py` renders one public URL with Playwright, classifies the page as a board or a general webpage, extracts subject, content, and metadata, and returns a 128-bit SimHash.

- Board pages use board-specific title candidates before generic headings.
- General webpages use the shared content extraction rules.
- The response includes `page_type`, `title_source`, and `content_source` for extraction verification.

## Files required by each use case

### Crawler already has subject and content

Copy `simhash_matcher/public_simhash.py` and install `simhash`.

```bash
python -m pip install simhash==2.1.2
```

Use `check_hash()` for MariaDB exact or threshold comparison.

### URL rendering and SimHash generation

Copy `simhash_matcher/public_simhash.py`, install dependencies, and install Chromium.

```bash
python -m pip install -r requirements.txt
python -m playwright install chromium
```

### External HTTP API

Also copy `public_simhash_api.py`. Set an API token before exposing the service.

```powershell
$env:PUBLIC_SIMHASH_API_TOKEN = "a-long-random-secret"
uvicorn public_simhash_api:app --host 0.0.0.0 --port 8000
```

## External API

- Health: `GET /health`
- Generate: `POST /public_simhash`
- API docs: `/docs`

```bash
curl -X POST "http://server-address:8000/public_simhash" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: API_TOKEN" \
  -d '{"url":"https://example.com/post/123"}'
```

Request:

```json
{"url": "https://example.com/post/123"}
```

Response fields:

```json
{
  "url": "https://example.com/post/123",
  "simhash": "32-character lowercase hexadecimal value",
  "skipped": false,
  "extracted": {
    "subject": "Post title",
    "content": "Extracted text content",
    "metadata": {},
    "page_type": "board",
    "title_source": ".p-table__subject_text",
    "content_source": ".content-node"
  }
}
```

Failures return `skipped: true` with `skip_reason`. A missing token returns HTTP `503`; an invalid token returns HTTP `401`.

## DB duplicate check

```python
from simhash_matcher.public_simhash import check_hash

result = check_hash(
    connection,
    subject,
    content,
    table="ASADAL_ce77dc5e9fd4_LEARN_LIST",
    max_hamming_distance=0,
)
```

`max_hamming_distance=0` is exact-only. A positive value enables similar duplicate detection and returns `hamming_distance`. The comparison uses the fixed `hash` column and never creates or changes a table or column.
