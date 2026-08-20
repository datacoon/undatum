---
title: "slice"
description: "undatum slice command reference"
---
# `slice`

Extracts specific rows by range or index list. Supports efficient DuckDB-based slicing for supported formats.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Slice by range
undatum slice data.csv --start 100 --end 200 --output output.csv

# Slice by specific indices
undatum slice data.jsonl --indices 1,5,10,20 --output output.jsonl
```

**Key options:**
- `--start`, `--end` — 0-based range
- `--indices` — comma-separated row indices
- `--output` — output path (stdout if omitted)
- `--engine`
- `--filetype` — override type detection (this command does **not** take `--format-in`)

Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
