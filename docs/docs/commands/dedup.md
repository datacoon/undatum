---
title: "dedup"
description: "undatum dedup command reference"
---
# `dedup`

Removes duplicate rows. Can deduplicate by all fields or specified key fields.

```bash
# Deduplicate by all fields
undatum dedup data.csv output.csv

# Deduplicate by key fields
undatum dedup data.jsonl --key-fields email output.jsonl

# Keep last duplicate
undatum dedup data.csv --key-fields id --keep last output.csv
```
