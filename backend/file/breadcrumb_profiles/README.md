# File Breadcrumb Profiles

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
