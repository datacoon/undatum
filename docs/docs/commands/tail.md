---
title: "tail"
description: "undatum tail command reference"
---
# `tail`

Extracts the last N rows from a data file. Uses efficient buffering for large files.

```bash
# Extract last 10 rows (default)
undatum tail data.csv

# Extract last 50 rows
undatum tail data.jsonl --n 50

# Save to file
undatum tail data.csv --n 20 output.csv

# Named Excel sheet
undatum tail workbook.xlsx --table Sheet2 --n 5

# Nested JSONL: unfold dict fields onto dotted paths
undatum tail nested.jsonl --flatten-nested --n 5
```
