---
title: "uniq"
description: "undatum uniq command reference"
---
# `uniq`

Extracts all unique values from specified field(s).

```bash
# Single field
undatum uniq --fields category data.jsonl

# Multiple fields (unique combinations)
undatum uniq --fields status,region data.jsonl
undatum uniq --fields city --format-out json --output cities.json data.csv
undatum uniq --fields city workbook.xlsx --table Sheet2
undatum uniq --fields capital_city.lat nested.jsonl --flatten-nested
```
