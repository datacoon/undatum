---
title: "join"
description: "undatum join command reference"
---
# `join`

Performs relational joins between two files. Supports inner, left, right, and full outer joins.

Write with `--output`. A trailing path is not a positional argument.

```bash
# Inner join by key field
undatum join data1.csv data2.csv --on email --type inner --output output.csv

# Left join (keep all rows from first file)
undatum join data1.jsonl data2.jsonl --on id --type left --output output.jsonl

# Right join (keep all rows from second file)
undatum join data1.csv data2.csv --on id --type right --output output.csv

# Full outer join (keep all rows from both files)
undatum join data1.jsonl data2.jsonl --on id --type full --output output.jsonl
undatum join workbook.xlsx other.xlsx --table Sheet2 --table2 Cities --on city --output out.jsonl
undatum join left.jsonl right.jsonl --on capital_city.lat --flatten-nested --output out.jsonl
```

**Key options:**
- `--on` — join key field(s)
- `--type inner|left|right|full`
- `--output` — output path (stdout if omitted)
- `--table` / `--sheet` and `--table2` / `--sheet2` — named tables for each file
- `--filetype1` / `--filetype2` — override type detection per file
- `--engine`, `--progress` / `--no-progress`

Also accepts `--flatten-nested`, `--on-error`, `--error-log`, and `--quotechar` ([shared options](/commands/shared-options)).
