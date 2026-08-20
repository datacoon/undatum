---
title: "cat"
description: "undatum cat command reference"
---
# `cat`

Concatenates files by rows or columns.

Write with `--output`. A trailing path is not a positional argument. Input files are positional; the output path is not.

```bash
# Concatenate files by rows (vertical; default --mode rows)
undatum cat file1.csv file2.csv --mode rows --output output.csv

# Concatenate files by columns (horizontal)
undatum cat file1.csv file2.csv --mode columns --output output.csv
```

**Key options:** `--mode rows|columns` (default `rows`), `--output`. Also accepts `--table`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
