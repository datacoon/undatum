---
title: "fixlengths"
description: "undatum fixlengths command reference"
---
# `fixlengths`

Ensures all rows have the same number of fields by padding shorter rows or truncating longer rows. Useful for data cleaning workflows.

```bash
# Pad rows with empty string (default)
undatum fixlengths data.csv --strategy pad output.csv

# Pad with custom value
undatum fixlengths data.jsonl --strategy pad --value "N/A" output.jsonl

# Truncate longer rows
undatum fixlengths data.csv --strategy truncate output.csv
```
