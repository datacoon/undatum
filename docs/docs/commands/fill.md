---
title: "fill"
description: "undatum fill command reference"
---
# `fill`

Fills empty or null values with specified values or strategies (forward-fill, backward-fill).

Write with `--output`. A trailing path is not a positional argument.

```bash
# Fill with constant value
undatum fill data.csv --fields name,email --value "N/A" --output output.csv

# Forward fill (use previous value)
undatum fill data.jsonl --fields status --strategy forward --output output.jsonl

# Backward fill (use next value)
undatum fill data.csv --fields category --strategy backward --output output.csv
```

**Key options:** `--fields`, `--value`, `--strategy forward|backward`, `--output`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
