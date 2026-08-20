---
title: "rename"
description: "undatum rename command reference"
---
# `rename`

Renames fields by exact mapping or regex patterns.

```bash
# Rename by exact mapping
undatum rename data.csv --map "old_name:new_name,old2:new2" output.csv

# Rename using regex
undatum rename data.jsonl --pattern "^prefix_" --replacement "" output.jsonl
undatum rename workbook.xlsx --table Sheet2 --map "city:city_name" out.jsonl
```
