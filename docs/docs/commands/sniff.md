---
title: "sniff"
description: "undatum sniff command reference"
---
# `sniff`

Detects file properties including delimiter, encoding, field types, and record count.

```bash
# Detect file properties (text output)
undatum sniff data.csv

# Output sniff results as JSON
undatum sniff data.jsonl --format json

# Output as YAML
undatum sniff data.csv --format yaml
undatum sniff nested.jsonl --flatten-nested --format json
```
