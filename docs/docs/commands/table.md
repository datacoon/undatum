---
title: "table"
description: "undatum table command reference"
---
# `table`

Displays data in a formatted, aligned table for inspection. Uses the rich library for terminal output. This is display-only: there is no `--output` flag. Cell values longer than 50 characters are truncated with `...`.

```bash
# Display first 20 rows (default)
undatum table data.csv

# Display with custom limit
undatum table data.jsonl --limit 50

# Display only specific fields
undatum table data.csv --fields name,email,status
undatum table workbook.xlsx --table Sheet2
undatum table nested.jsonl --flatten-nested
```

**Key options:** `--limit` (default 20), `--fields`, `--table` / `--sheet`, `--flatten-nested`. See [shared options](/commands/shared-options).
