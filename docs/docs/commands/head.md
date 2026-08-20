---
title: "head"
description: "undatum head command reference"
---
# `head`

Extracts the first N rows from a data file. Useful for quick data inspection.

```bash
# Extract first 10 rows (default)
undatum head data.csv

# Extract first 20 rows
undatum head data.jsonl --n 20

# Save to file
undatum head data.csv --n 5 output.csv

# Named Excel sheet
undatum head workbook.xlsx --table Sheet2 --n 5

# Nested JSONL: unfold dict fields onto dotted paths
undatum head nested.jsonl --flatten-nested --n 5
```
