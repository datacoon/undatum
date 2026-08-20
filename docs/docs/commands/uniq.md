---
title: "uniq"
description: "undatum uniq command reference"
---
# `uniq`

Extracts all unique values from specified field(s). **`--fields` is required.**

Default stdout format is CSV. Use `--format-out json` (or an `--output` path ending in `.json`) for JSON.

```bash
# Single field
undatum uniq --fields category data.jsonl

# Multiple fields (unique combinations)
undatum uniq --fields status,region data.jsonl
undatum uniq --fields city --format-out json --output cities.json data.csv
undatum uniq --fields city workbook.xlsx --table Sheet2
undatum uniq --fields capital_city.lat nested.jsonl --flatten-nested
```

**Key options:** `--fields` (required), `--filter` / `--filter-expr`, `--format-out csv|json`, `--output`, `--engine auto|duckdb|iterable`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
