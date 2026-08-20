---
title: "formats"
description: "undatum formats command reference"
---
# `formats`

Inspect the iterabledata format catalog (140+ formats). The list reflects installed optional dependencies and runtime capabilities on your machine.

```bash
# All formats with read/write flags
undatum formats list

# Writable outputs only (useful before choosing a conversion target)
undatum formats list --writable

# Read-only inputs
undatum formats list --read-only

# Full capability matrix (bulk, streaming, totals, tables, nested, maturity, native bulk)
undatum formats list --capabilities

# Named tables/sheets in a workbook or database
undatum formats tables workbook.xlsx
undatum formats tables data.sqlite --json

# Single format (aliases, extra, memory, selection, codecs)
undatum formats describe parquet
undatum formats describe geojson --json

# Export catalog for tooling or CI checks
undatum formats export --output formats.json
```
