# File Crawler Domain Configurations

The file crawler reads a domain configuration before parsing a detail page.
Each file is stored directly under this directory using the normalized domain:

```text
services/site_config/domains/
  sen.go.kr.json
  gwangjin.go.kr.json
```

`www.` is normalized away, so `www.sen.go.kr` and `sen.go.kr` share
`sen.go.kr.json`.

```json
{
  "attachment_selectors": [".print-box-wrap a[href]", "a[download][href]"],
  "attachment_name_attributes": ["download", "title", "aria-label"],
  "detail_title_selectors": ["h1", ".view-title"],
  "breadcrumb_selectors": [".breadcrumb", ".location"],
  "breadcrumb_category_index": -2,
  "breadcrumb_title_fallback": false
}
```

- `attachment_selectors`: CSS selectors considered before common attachment discovery.
- `attachment_name_attributes`: attributes checked first for the displayed attachment name.
- `attachment_url_hints`: site-specific URL fragments that identify an attachment endpoint.
- `detail_title_selectors`: optional selectors for a detail-page title.
- `breadcrumb_selectors`: optional selectors considered before common breadcrumb discovery.
- `breadcrumb_category_index`: breadcrumb token index used for `cate2`.
- `breadcrumb_title_fallback`: allows HTML `<title>` as the last breadcrumb fallback.

Protected attachment endpoints can also define an optional transport policy:

```json
{
  "download": {
    "force_https": true,
    "prewarm_source_page": true,
    "include_origin": true,
    "http_retries": 3,
    "playwright_fallback": true
  }
}
```

- `force_https`: upgrades an extracted HTTP attachment URL before requesting it.
- `prewarm_source_page`: loads the detail page with the same HTTP session first, then forwards session cookies.
- `include_origin`: explicitly includes or omits the Origin header; omitted means the common policy applies.
- `http_retries`: bounded direct HTTP attempts (1-3).
- `playwright_fallback`: permits one browser fallback after direct HTTP failure.

Use `attachment_url_hints` only when a valid attachment endpoint lacks a
common filename extension or common download pattern. It identifies the URL;
the displayed document filename continues to come from the attachment link.

Extraction never writes a configuration automatically. A selector should only
be saved here through an explicit configuration action after it has been
verified for the site.
