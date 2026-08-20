---
title: "split"
description: "undatum split command reference"
---
# `split`

Splits datasets into multiple files based on chunk size or field values.

```bash
# Split by chunk size
undatum split --chunksize 10000 data.jsonl

# Split by field value
undatum split --fields category data.jsonl
undatum split workbook.xlsx --table Sheet2 --fields city --dirname out/
undatum split nested.jsonl --fields capital_city.lat --flatten-nested --dirname out/
```
