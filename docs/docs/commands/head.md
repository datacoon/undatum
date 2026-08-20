---
title: "head"
description: "undatum head command reference"
---
# `head`

Extracts the first N rows from a data file. Useful for quick data inspection.

Without `--output`, rows are written as **JSONL to stdout** even when the input is CSV. The output format follows the `--output` extension when writing a file.

```bash
# Extract first 10 rows (default) as JSONL on stdout
undatum head data.csv

# Extract first 20 rows
undatum head data.jsonl --n 20

# Save to file (not a positional path)
undatum head data.csv --n 5 --output output.csv

# Named Excel sheet
undatum head workbook.xlsx --table Sheet2 --n 5

# Nested JSONL: unfold dict fields onto dotted paths
undatum head nested.jsonl --flatten-nested --n 5
```

**Key options:** `--n` (default 10), `--output`, `--table` / `--sheet`, `--flatten-nested`, `--trust`. See [shared options](/commands/shared-options).
