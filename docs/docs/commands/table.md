---
title: "table"
description: "undatum table command reference"
---
# `table`

Displays data in a formatted, aligned table for inspection. Uses the rich library for beautiful terminal output.

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
