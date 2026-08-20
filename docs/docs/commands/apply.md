---
title: "apply"
description: "undatum apply command reference"
---
# `apply`

Applies a Python `process(record)` function, or a registered transform plugin, to each record.

```bash
# Script: the file must define process(record) -> dict
undatum apply --script transform.py data.jsonl output.jsonl

# Registered transform plugin
undatum apply data.jsonl --plugin example-transform --output out.jsonl

# Subset first, then transform
undatum apply --script transform.py --filter '`status` == "active"' data.jsonl out.jsonl
```

Install transform plugins via the `undatum.plugins` entry point; see [Plugins](/integrations/plugins).

## Script contract

The script is loaded with `runpy.run_path`. It **must** define `process`:

```python
def process(record: dict) -> dict:
    record["name"] = str(record.get("name", "")).strip()
    return record
```

Missing `process` is a validation error. The function runs twice internally (schema pass, then write).

## Options

- `--script PATH` — Python file with `process`
- `--plugin NAME` — registered transform plugin instead of a script
- `--filter` / `--filter-expr` — comparison expression before the transform ([shared options](/commands/shared-options))
- `--output` — output path (stdout JSONL if omitted)
- `--flatten-nested`, `--on-error`, `--error-log`, `--table`, `--quotechar` — [shared options](/commands/shared-options)
