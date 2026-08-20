---
title: "replace"
description: "undatum replace command reference"
---
# `replace`

Performs string replacement in specified fields. Supports simple string replacement and regex-based replacement.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Simple string replacement
undatum replace data.csv --field name --pattern "Mr\." --replacement "Mr" --output output.csv

# Regex replacement
undatum replace data.jsonl --field email --pattern "@old.com" --replacement "@new.com" --regex --output output.jsonl

# Global replacement (all occurrences; default is first match only)
undatum replace data.csv --field text --pattern "old" --replacement "new" --global-replace --output output.csv
```

**Key options:**
- `--field`, `--pattern`, `--replacement`
- `--regex` — treat `--pattern` as regex
- `--global-replace` — replace every match in the field (not `--global`)
- `--output` — output path (stdout if omitted)

Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
