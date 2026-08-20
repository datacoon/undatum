---
title: "enum"
description: "undatum enum command reference"
---
# `enum`

Adds row numbers, UUIDs, or constant values to records. Useful for adding unique identifiers or sequential numbers.

```bash
# Add row numbers (default field: row_id, starts at 1)
undatum enum data.csv output.csv

# Add UUIDs
undatum enum data.jsonl --field id --type uuid output.jsonl

# Add constant value
undatum enum data.csv --field status --type constant --value "active" output.csv

# Custom starting number
undatum enum data.jsonl --field sequence --start 100 output.jsonl
undatum enum workbook.xlsx --table Sheet2 --field row_id out.jsonl
```
