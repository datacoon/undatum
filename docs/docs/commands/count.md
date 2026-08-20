---
title: "count"
description: "undatum count command reference"
---
# `count`

Counts the number of rows in a data file. With DuckDB engine, counting is instant for supported formats.

```bash
# Count rows in CSV file
undatum count data.csv

# Count rows in JSONL file
undatum count data.jsonl

# Use DuckDB engine for faster counting
undatum count data.parquet --engine duckdb

# Named Excel sheet
undatum count workbook.xlsx --table Sheet2
```
