---
title: "sort"
description: "undatum sort command reference"
---
# `sort`

Sorts rows by one or more columns. Supports multiple sort keys, ascending/descending order, and numeric sorting.

```bash
# Sort by single column ascending
undatum sort data.csv --by name output.csv

# Sort by multiple columns
undatum sort data.jsonl --by name,age output.jsonl

# Sort descending
undatum sort data.csv --by date --desc output.csv

# Numeric sort
undatum sort data.csv --by price --numeric output.csv
undatum sort workbook.xlsx --table Sheet2 --by city out.jsonl
undatum sort nested.jsonl --by capital_city.lat --flatten-nested --numeric capital_city.lat
```
