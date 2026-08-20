---
title: "diff"
description: "undatum diff command reference"
---
# `diff`

Compares two files and shows differences (added, removed, and changed rows).

```bash
# Compare files by key
undatum diff file1.csv file2.csv --key id
undatum diff workbook.xlsx other.xlsx --table Sheet2 --table2 Cities --key city
undatum diff nested1.jsonl nested2.jsonl --key name --flatten-nested

# Ignore order and show summary only (good for CI)
undatum diff file1.parquet file2.parquet --ignore-order --summary-only

# Output detailed diff to Markdown with numeric tolerance
undatum diff file1.csv file2.csv \
  --key user_id \
  --numeric-tolerance 0.001 \
  --output-format markdown \
  --output diff.md

# Fail CI when change thresholds are exceeded
undatum diff file1.csv file2.csv \
  --key id \
  --max-added-rows 10 \
  --max-removed-rows 5 \
  --max-changed-rows 0
```
