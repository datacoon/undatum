---
title: "rename"
description: "undatum rename command reference"
---
# `rename`

Renames fields by exact mapping or regex patterns.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Rename by exact mapping
undatum rename data.csv --map "old_name:new_name,old2:new2" --output output.csv

# Rename using regex
undatum rename data.jsonl --pattern "^prefix_" --replacement "" --output output.jsonl
undatum rename workbook.xlsx --table Sheet2 --map "city:city_name" --output out.jsonl
```

**Key options:** `--map`, `--pattern`, `--replacement`, `--output`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
