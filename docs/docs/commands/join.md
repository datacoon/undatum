---
title: "join"
description: "undatum join command reference"
---
# `join`

Performs relational joins between two files. Supports inner, left, right, and full outer joins.

```bash
# Inner join by key field
undatum join data1.csv data2.csv --on email --type inner output.csv

# Left join (keep all rows from first file)
undatum join data1.jsonl data2.jsonl --on id --type left output.jsonl

# Right join (keep all rows from second file)
undatum join data1.csv data2.csv --on id --type right output.csv

# Full outer join (keep all rows from both files)
undatum join data1.jsonl data2.jsonl --on id --type full output.jsonl
undatum join workbook.xlsx other.xlsx --table Sheet2 --table2 Cities --on city out.jsonl
undatum join left.jsonl right.jsonl --on capital_city.lat --flatten-nested out.jsonl
```
