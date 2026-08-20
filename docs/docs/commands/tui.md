---
title: "tui"
description: "undatum tui command reference"
---
# `tui`

Interactive terminal UI for exploring a **sample** of a dataset (not the whole file).
Requires `pip install "undatum[tui]"` and a real TTY.

```bash
pip install "undatum[tui]"
undatum tui data.csv
undatum tui data.parquet --limit 500
undatum tui workbook.xlsx --table Sheet2
undatum tui nested.jsonl --flatten-nested
```

Keys: `q` quit, `?` help, `o` open, `s` profile, `f` frequency on the selected
column, `/` filter the sample, `e` export the current view, `w` convert/save as
(full file, `--low-memory`), `v` validate the sample, `m` mask preview, `p`
export pipeline YAML, `:` command palette, `ctrl+s` SQL (default `LIMIT 500` on
the `data` view), `Tab` cycle panes. From the file picker, `u` opens a local
path or `s3://` / `gs://` / `az://` URI. The status line shows the equivalent CLI command. Recent
files are stored as paths only in `~/.undatum/tui-history.json`. Use `table` /
`profile` / `sql` when you are not on an interactive terminal.
