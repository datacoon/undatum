---
title: "slice"
description: "undatum slice command reference"
---
# `slice`

Extracts specific rows by range or index list. Supports efficient DuckDB-based slicing for supported formats.

```bash
# Slice by range
undatum slice data.csv --start 100 --end 200 output.csv

# Slice by specific indices
undatum slice data.jsonl --indices 1,5,10,20 output.jsonl
```
