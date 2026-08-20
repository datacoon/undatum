---
title: "exclude"
description: "undatum exclude command reference"
---
# `exclude`

Removes rows from input file where keys match exclusion file. Uses hash-based lookup for performance.

```bash
# Exclude rows by key
undatum exclude data.csv blacklist.csv --on email output.csv

# Exclude with multiple key fields
undatum exclude data.jsonl exclude.jsonl --on id,email output.jsonl
undatum exclude workbook.xlsx skip.csv --table Sheet2 --on city out.jsonl
undatum exclude nested.jsonl skip.jsonl --on capital_city.lat --flatten-nested out.jsonl
```
