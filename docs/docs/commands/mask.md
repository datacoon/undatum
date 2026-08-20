---
title: "mask"
description: "undatum mask command reference"
---
# `mask`

Masks sensitive fields for anonymization. Supports redaction, deterministic hashing (preserves joins), and type-compatible randomization.

```bash
# Redact email and phone fields
undatum mask data.csv --fields email,phone --method redact --output masked.csv

# Hash user IDs (deterministic, preserves joins)
undatum mask data.jsonl --fields user_id --method hash --salt my-salt --output masked.jsonl

# Randomize age and email fields
undatum mask data.csv --fields age,email --method randomize --output masked.csv
```

**Masking methods:**
- `redact` (default) - replace values with a fixed token (`***`)
- `hash` - deterministic one-way hash; the same input always produces the same output, so joins across files are preserved. Use `--salt` for additional security
- `randomize` - replace values with random but type-compatible values
