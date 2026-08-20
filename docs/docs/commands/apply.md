---
title: "apply"
description: "undatum apply command reference"
---
# `apply`

Applies a Python `process(record)` function, or a registered transform plugin, to each record. Either `--script` or `--plugin` is required.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Script: the file must define process(record) -> dict
undatum apply --script transform.py data.jsonl --output output.jsonl

# Registered transform plugin
undatum apply data.jsonl --plugin example-transform --output out.jsonl

# Subset first, then transform
undatum apply --script transform.py --filter '`status` == "active"' data.jsonl --output out.jsonl
```

Install transform plugins via the `undatum.plugins` entry point; see [Plugins](/integrations/plugins).

## Script contract

The script is loaded with `runpy.run_path`. It **must** define `process`:

```python
def process(record: dict) -> dict:
    record["name"] = str(record.get("name", "")).strip()
    return record
```

Missing `process` is a validation error. The function runs twice internally: a schema pass over at most 1000 records, then the write pass.

## Options

- `--script PATH` — Python file with `process`
- `--plugin NAME` — registered transform plugin instead of a script
- `--filter` / `--filter-expr` — comparison expression before the transform ([shared options](/commands/shared-options))
- `--output` — output path (stdout JSONL if omitted)
- `--format-in`, `--zipfile`, `--delimiter`, `--encoding`, `--start-page`, `--trust`
- `--flatten-nested`, `--max-nested-depth`, `--keep-nested-parents` / `--no-keep-nested-parents`, `--on-error`, `--error-log`, `--table`, `--quotechar` — [shared options](/commands/shared-options)
