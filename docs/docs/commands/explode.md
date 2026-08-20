---
title: "explode"
description: "undatum explode command reference"
---
# `explode`

Splits a column by separator into multiple rows. Creates one row per value, duplicating other fields.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Explode comma-separated values
undatum explode data.csv --field tags --separator "," --output output.csv

# Explode pipe-separated values
undatum explode data.jsonl --field categories --separator "|" --output output.jsonl
```

**Key options:** `--field`, `--separator`, `--output`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
