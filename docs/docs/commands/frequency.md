---
title: "frequency"
description: "undatum frequency command reference"
---
# `frequency`

Calculates frequency distribution for specified fields. **`--fields` is required.**

Default stdout format is CSV. Use `--format-out json` (or an `--output` path ending in `.json`) for JSON.

```bash
undatum frequency --fields category data.jsonl
undatum frequency --fields status,region data.csv
undatum frequency --fields city --format-out json --output freq.json data.csv
undatum frequency --fields city workbook.xlsx --table Sheet2
undatum frequency --fields capital_city.lat nested.jsonl --flatten-nested
```

**Key options:** `--fields` (required), `--filter` / `--filter-expr`, `--format-out csv|json`, `--output`, `--engine auto|duckdb|python`, `--threads`, `--duckdb-threads`, `--duckdb-memory`, `--duckdb-temp-dir`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
