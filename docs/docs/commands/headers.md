---
title: "headers"
description: "undatum headers command reference"
---
# `headers`

Extracts field names from data files (CSV, JSON Lines, BSON, XML, Excel, and other iterabledata sources).

Default scan `--limit` is 10000. `--format-out json` (or `--output` ending in `.json`) writes JSON instead of text.

```bash
undatum headers data.jsonl
undatum headers data.csv --limit 50000
undatum headers data.csv --format-out json --output fields.json
undatum headers workbook.xlsx --table Sheet2
```

**Key options:** `--limit` (default 10000), `--format-out`, `--output`, `--zipfile`, `--table` / `--sheet`. See [shared options](/commands/shared-options).
