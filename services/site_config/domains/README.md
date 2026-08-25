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
- `detail_title_selectors`: optional selectors for a detail-page title.
- `breadcrumb_selectors`: optional selectors considered before common breadcrumb discovery.
- `breadcrumb_category_index`: breadcrumb token index used for `cate2`.
- `breadcrumb_title_fallback`: allows HTML `<title>` as the last breadcrumb fallback.

Extraction never writes a configuration automatically. A selector should only
be saved here through an explicit configuration action after it has been
verified for the site.
