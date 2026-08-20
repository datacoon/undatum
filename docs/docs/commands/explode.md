---
title: "explode"
description: "undatum explode command reference"
---
# `explode`

Splits a column by separator into multiple rows. Creates one row per value, duplicating other fields.

```bash
# Explode comma-separated values
undatum explode data.csv --field tags --separator "," output.csv

# Explode pipe-separated values
undatum explode data.jsonl --field categories --separator "|" output.jsonl
```
