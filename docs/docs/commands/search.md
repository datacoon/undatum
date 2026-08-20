---
title: "search"
description: "undatum search command reference"
---
# `search`

Filters rows using regex patterns. Searches across specified fields or all fields.

```bash
# Search across all fields
undatum search data.csv --pattern "error|warning"

# Search in specific fields
undatum search data.jsonl --pattern "^[0-9]+$" --fields id,code

# Case-insensitive search
undatum search data.csv --pattern "ERROR" --ignore-case
```
