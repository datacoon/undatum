---
title: "fixlengths"
description: "undatum fixlengths command reference"
---
# `fixlengths`

Ensures all rows have the same number of fields by padding shorter rows or truncating longer rows. Useful for data cleaning workflows.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Pad rows with empty string (default)
undatum fixlengths data.csv --strategy pad --output output.csv

# Pad with custom value
undatum fixlengths data.jsonl --strategy pad --value "N/A" --output output.jsonl

# Truncate longer rows
undatum fixlengths data.csv --strategy truncate --output output.csv
```

**Key options:** `--strategy pad|truncate`, `--value` (pad filler), `--output`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
