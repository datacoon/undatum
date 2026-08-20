---
title: "headers"
description: "undatum headers command reference"
---
# `headers`

Extracts field names from data files. Works with CSV, JSON Lines, BSON, and XML files.

```bash
undatum headers data.jsonl
undatum headers data.csv --limit 50000
undatum headers data.csv --format-out json --output fields.json
undatum headers workbook.xlsx --table Sheet2
```
