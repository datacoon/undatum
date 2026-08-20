---
title: "enum"
description: "undatum enum command reference"
---
# `enum`

Adds row numbers, UUIDs, or constant values to records. Useful for adding unique identifiers or sequential numbers.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Add row numbers (default field: row_id, starts at 1)
undatum enum data.csv --output output.csv

# Add UUIDs
undatum enum data.jsonl --field id --type uuid --output output.jsonl

# Add constant value
undatum enum data.csv --field status --type constant --value "active" --output output.csv

# Custom starting number
undatum enum data.jsonl --field sequence --start 100 --output output.jsonl
undatum enum workbook.xlsx --table Sheet2 --field row_id --output out.jsonl
```

**Key options:** `--field`, `--type` (`sequence`, `uuid`, `constant`), `--start`, `--value`, `--output`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
