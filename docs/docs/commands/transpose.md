---
title: "transpose"
description: "undatum transpose command reference"
---
# `transpose`

Swaps rows and columns, handling headers appropriately.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Transpose CSV file
undatum transpose data.csv --output output.csv

# Transpose JSONL file
undatum transpose data.jsonl --output output.jsonl
```

**Key options:** `--output`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
