# Legacy File Breadcrumb Profiles

The canonical selector configuration location is now:

```text
services/site_config/domains/<normalized-domain>.json
```

For example, `services/site_config/domains/gwangjin.go.kr.json` contains
`breadcrumb_selectors`, `breadcrumb_category_index`, and
`breadcrumb_title_fallback`. The file crawler loads that configuration first.

This directory remains a read-only compatibility fallback for previously
registered breadcrumb profiles. Add or update selector rules in
`services/site_config/domains/`, not here.

Each domain has its own directory and profile file:

```text
backend/file/breadcrumb_profiles/
  gwangjin.go.kr/
    gwangjin.go.kr.json
```

The file crawler first looks up the exact hostname and then its `www.` variant.

```json
{
  "selectors": [".breadcrumb", ".location"],
  "category_index": -2,
  "title_fallback": false
}
```

- `selectors`: CSS selectors tried before the common breadcrumb selectors.
- `category_index`: token index used for `cate2`; `-2` is the default.
- `title_fallback`: whether to derive tokens from the HTML title when selectors fail.

An absent, malformed, or non-matching profile falls back to the common selectors.
