---
title: "fill"
description: "undatum fill command reference"
---
# `fill`

Fills empty or null values with specified values or strategies (forward-fill, backward-fill).

```bash
# Fill with constant value
undatum fill data.csv --fields name,email --value "N/A" output.csv

# Forward fill (use previous value)
undatum fill data.jsonl --fields status --strategy forward output.jsonl

# Backward fill (use next value)
undatum fill data.csv --fields category --strategy backward output.csv
```
