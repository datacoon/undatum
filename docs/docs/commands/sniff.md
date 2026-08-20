---
title: "sniff"
description: "undatum sniff command reference"
---
# `sniff`

Reports file properties: detected or assumed encoding, field types, record count, and the CSV delimiter **currently in use**.

`sniff` does **not** auto-detect the CSV delimiter. The CLI default is `--delimiter ,`. A semicolon-separated file is reported as a single column unless you pass `--delimiter ";"`. Use [`analyze`](/commands/analyze) when you need delimiter auto-detection.

Encoding is often `unknown` when sniff cannot determine it; pass `--encoding` to override.

```bash
# Report file properties (text output)
undatum sniff data.csv

# Semicolon CSV: pass the delimiter
undatum sniff data.csv --delimiter ";"

# Output as JSON (also --format-out json, or --output ending in .json)
undatum sniff data.jsonl --format json

# Output as YAML
undatum sniff data.csv --format yaml --output sniff.yaml
undatum sniff nested.jsonl --flatten-nested --format json
```

**Key options:** `--delimiter`, `--encoding`, `--format` / `--format-out` (`text`, `json`, `yaml`), `--output`, `--flatten-nested`, `--table` / `--sheet`. See [shared options](/commands/shared-options).
