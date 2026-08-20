---
title: "web"
description: "undatum web command reference"
---
# `web`

Local browser UI for the same sampled session as `tui` (not a public Data API).
Requires `pip install "undatum[web]"`. Binds to `127.0.0.1:8765` by default.

```bash
pip install "undatum[web]"
undatum web data.csv
undatum web data.parquet --limit 500 --no-open
undatum web workbook.xlsx --table Sheet2
undatum web nested.jsonl --flatten-nested --no-open
```

Open a path or `s3://` / `gs://` / `az://` URI, or upload a file (streamed to a temp directory).
The page shows a bounded sample, equivalent CLI lines, profile, frequency,
filter, SQL (default `LIMIT 500`), export, convert `--low-memory`, validate,
mask, and pipeline YAML export. Use `undatum api serve` for a read-only machine
API.
