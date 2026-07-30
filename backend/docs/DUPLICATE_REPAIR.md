# Duplicate Repair

Duplicate repair updates existing `LEARN_LIST` rows when a duplicate URL is found.

## Current Preset

Frequent on/off changes should use one env line:

```env
BOARD_DUPLICATE_REPAIR=off
```

To turn duplicate repair on:

```env
BOARD_DUPLICATE_REPAIR=on
```

The switch applies:

`off`:

```env
BOARD_DUPLICATE_REPAIR_FEATURES=off
BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES=off
BOARD_DUPLICATE_REPAIR_SOURCES=exploration
BOARD_DUPLICATE_REPAIR_ENABLE_TITLE=0
BOARD_DUPLICATE_REPAIR_ENABLE_IMMEDIATE_TITLE=0
BOARD_DUPLICATE_REPAIR_ENABLE_SUMMARY=0
```

`on`:

```env
BOARD_DUPLICATE_REPAIR_FEATURES=category,parsed_fields,title
BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES=category,parsed_fields
BOARD_DUPLICATE_REPAIR_SOURCES=exploration,learn_list
BOARD_DUPLICATE_REPAIR_ENABLE_TITLE=1
BOARD_DUPLICATE_REPAIR_ENABLE_IMMEDIATE_TITLE=1
BOARD_DUPLICATE_REPAIR_ENABLE_SUMMARY=0
```

`BOARD_FEATURE_PRESET=auto_classification_only` may still be used for automatic classification. `BOARD_DUPLICATE_REPAIR=on/off` is applied after the preset and controls only duplicate repair.

## Presets

| Preset | Effect |
| --- | --- |
| `auto_classification_only` | Duplicate repair OFF, automatic classification application ON |
| `auto_category_only` | Alias of `auto_classification_only` |
| `duplicate_repair_on` | Duplicate repair ON, automatic classification application ON |
| `manual` | Do not modify individual env flags |

## Duplicate Repair Flags

`BOARD_DUPLICATE_REPAIR_FEATURES` controls background repair.

```env
BOARD_DUPLICATE_REPAIR_FEATURES=off
```

Multiple features are comma-separated:

```env
BOARD_DUPLICATE_REPAIR_FEATURES=exploration,category,parsed_fields,title
```

`BOARD_DUPLICATE_REPAIR_SOURCES` controls where duplicate repair candidates are read from:

```env
BOARD_DUPLICATE_REPAIR_SOURCES=exploration,learn_list
```

- `exploration`: existing behavior; repair when a duplicate is encountered during the current run.
- `learn_list`: repair existing `LEARN_LIST` rows only. It does not enqueue new crawl URLs and does not increment scan/save/study counters.

The `learn_list` source uses only `LEARN_LIST.content` as the source URL. It does not fall back to any alternate URL column.

The `learn_list` source intentionally ignores the `exploration` repair feature because it is scoped to already registered row metadata. It can update category, title, author, and summary according to the enabled repair features.

Optional limits:

```env
BOARD_DUPLICATE_REPAIR_LEARN_LIST_LIMIT=500
BOARD_DUPLICATE_REPAIR_LEARN_LIST_STATUS=N,Y
```

`BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES` controls synchronous repair when a duplicate is detected.

```env
BOARD_DUPLICATE_REPAIR_IMMEDIATE_FEATURES=off
```

Title and summary repair also require their runtime switches:

```env
BOARD_DUPLICATE_REPAIR_ENABLE_TITLE=0
BOARD_DUPLICATE_REPAIR_ENABLE_IMMEDIATE_TITLE=0
BOARD_DUPLICATE_REPAIR_ENABLE_SUMMARY=0
```

## Related Code

- Feature preset expansion: `config/settings.py`
- Feature alias parsing: `backend/shared/duplicate_repair_features.py`
- Runtime feature filtering: `backend/board/board_content_workflow.py`
