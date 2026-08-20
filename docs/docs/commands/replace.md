---
title: "replace"
description: "undatum replace command reference"
---
# `replace`

Performs string replacement in specified fields. Supports simple string replacement and regex-based replacement.

```bash
# Simple string replacement
undatum replace data.csv --field name --pattern "Mr\." --replacement "Mr" output.csv

# Regex replacement
undatum replace data.jsonl --field email --pattern "@old.com" --replacement "@new.com" --regex output.jsonl

# Global replacement (all occurrences)
undatum replace data.csv --field text --pattern "old" --replacement "new" --global output.csv
```
