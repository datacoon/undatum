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

`--filter` uses the same comparison syntax as `select`. See [shared CLI options](/commands/shared-options).
