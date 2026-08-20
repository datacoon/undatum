---
title: "select"
description: "undatum select command reference"
---
# `select`

Selects and reorders columns from files. Supports filtering, nested dot-notation fields, and engine selection. When the DuckDB engine is used, filter expressions are pushed to SQL when possible and results can be written directly via `COPY` for CSV, JSON, and Parquet output.

`--filter` is the documented flag (`--filter-expr` is an alias). Syntax: [Basic usage](/getting-started/basic-usage).

```bash
undatum select --fields name,email,status data.jsonl
undatum select --fields name,email --filter "`status` == 'active'" data.jsonl
undatum select --fields user.name,user.email --engine duckdb data.jsonl
undatum select --fields name,email --engine duckdb --output subset.csv data.jsonl
undatum select --fields name --table Sheet2 workbook.xlsx
undatum select --fields name,capital_city.lat --flatten-nested nested.jsonl
```
