---
title: "sample"
description: "undatum sample command reference"
---
# `sample`

Randomly selects rows from a data file using reservoir sampling. Either `--n` or `--percent` is required.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Sample fixed number of rows
undatum sample data.csv --n 1000 --output output.csv

# Sample by percentage
undatum sample data.jsonl --percent 10 --output output.jsonl
```

**Key options:** `--n`, `--percent`, `--output`, `--engine`, `--format-in`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
