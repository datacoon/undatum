---
title: "tail"
description: "undatum tail command reference"
---
# `tail`

Extracts the last N rows from a data file. Uses efficient buffering for large files.

Without `--output`, rows are written as **JSONL to stdout** even when the input is CSV. The output format follows the `--output` extension when writing a file.

```bash
# Extract last 10 rows (default) as JSONL on stdout
undatum tail data.csv

# Extract last 50 rows
undatum tail data.jsonl --n 50

# Save to file (not a positional path)
undatum tail data.csv --n 20 --output output.csv

# Named Excel sheet
undatum tail workbook.xlsx --table Sheet2 --n 5

# Nested JSONL: unfold dict fields onto dotted paths
undatum tail nested.jsonl --flatten-nested --n 5
```

**Key options:** `--n` (default 10), `--output`, `--table` / `--sheet`, `--flatten-nested`. See [shared options](/commands/shared-options).
