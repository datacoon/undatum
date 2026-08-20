---
title: "sort"
description: "undatum sort command reference"
---
# `sort`

Sorts rows by one or more columns. Supports multiple sort keys, ascending/descending order, and numeric sorting.

Write with `--output`. A trailing path is not a positional argument (`convert` is the command that takes `INPUT OUTPUT`).

```bash
# Sort by single column ascending
undatum sort data.csv --by name --output output.csv

# Sort by multiple columns
undatum sort data.jsonl --by name,age --output output.jsonl

# Sort descending
undatum sort data.csv --by date --desc --output output.csv

# Numeric sort: --numeric takes a field list, not an output path
undatum sort data.csv --by price --numeric price --output output.csv
undatum sort workbook.xlsx --table Sheet2 --by city --output out.jsonl
undatum sort nested.jsonl --by capital_city.lat --flatten-nested --numeric capital_city.lat --output out.jsonl
```

**Key options:**
- `--by` — comma-separated sort fields
- `--desc` — descending order
- `--numeric FIELD[,FIELD…]` — treat those fields as numbers
- `--output` — output path (stdout if omitted)
- `--low-memory` — external merge sort (spill runs to disk)
- `--engine auto|duckdb|python`
- `--filetype` — override type detection (this command does **not** take `--format-in`)

Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
