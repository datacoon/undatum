---
title: "flatten"
description: "undatum flatten command reference"
---
# `flatten`

Flattens nested data structures into key-value / dotted-path records (distinct from `--flatten-nested` on other commands, which unfolds nested fields in place).

```bash
undatum flatten data.jsonl
undatum flatten data.jsonl --filter '`type` == "city"' --output flat.jsonl
```

`flatten` does not take `--flatten-nested` (it already emits dotted paths). It does accept `--output`, `--filter`, `--table` / `--sheet`, `--format-in`, `--quotechar`, `--trust`, `--on-error`, `--error-log`, `--delimiter`, and `--encoding`. See [shared CLI options](/commands/shared-options).
