---
title: "reverse"
description: "undatum reverse command reference"
---
# `reverse`

Reverses the order of rows in a data file.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Reverse rows
undatum reverse data.csv --output output.csv

# Reverse JSONL file
undatum reverse data.jsonl --output output.jsonl
undatum reverse workbook.xlsx --table Sheet2 --output out.jsonl
```

**Key options:** `--output`, `--engine`, `--filetype` (this command does **not** take `--format-in`). Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
