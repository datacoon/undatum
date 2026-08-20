---
title: "exclude"
description: "undatum exclude command reference"
---
# `exclude`

Removes rows from input file where keys match exclusion file. Uses hash-based lookup for performance.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Exclude rows by key
undatum exclude data.csv blacklist.csv --on email --output output.csv

# Exclude with multiple key fields
undatum exclude data.jsonl exclude.jsonl --on id,email --output output.jsonl
undatum exclude workbook.xlsx skip.csv --table Sheet2 --on city --output out.jsonl
undatum exclude nested.jsonl skip.jsonl --on capital_city.lat --flatten-nested --output out.jsonl
```

**Key options:** `--on`, `--output`, `--table` / `--sheet` and `--table2` / `--sheet2` for each file. Also accepts `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
