---
title: "dedup"
description: "undatum dedup command reference"
---
# `dedup`

Removes duplicate rows. Can deduplicate by all fields or specified key fields.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Deduplicate by all fields
undatum dedup data.csv --output output.csv

# Deduplicate by key fields
undatum dedup data.jsonl --key-fields email --output output.jsonl

# Keep last duplicate
undatum dedup data.csv --key-fields id --keep last --output output.csv
```

**Key options:**
- `--key-fields` — comma-separated keys (all fields if omitted)
- `--keep first|last`
- `--output` — output path (stdout if omitted)
- `--low-memory`, `--engine`
- `--filetype` — override type detection (this command does **not** take `--format-in`)

Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
