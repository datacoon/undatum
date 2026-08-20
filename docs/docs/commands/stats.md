---
title: "stats / profile"
description: "undatum stats / profile command reference"
---
# `stats / profile`

Generates statistics and profiling metrics about your dataset. DuckDB is selected automatically for supported formats (CSV, JSONL, JSON, Parquet) and is typically much faster.

`profile` is a **pure alias** of `stats` (same options and help).

```bash
# Basic statistics
undatum stats data.jsonl

# Same command
undatum profile data.csv

# Date detection is on by default; disable with --no-checkdates
undatum stats data.csv --no-checkdates

# Force DuckDB
undatum stats data.parquet --engine duckdb

# Machine-readable JSON (also used when --output ends in .json)
undatum stats data.csv --format-out json --output stats.json

# HTML or Markdown profiling report (also inferred from --output extension)
undatum stats data.csv --format-out html --output profile.html
undatum stats data.csv --output profile.md

# Nested JSONL: unfold dict fields onto dotted paths
undatum stats nested.jsonl --flatten-nested --format-out json
undatum stats nested.jsonl --flatten-nested --max-nested-depth 2
undatum stats nested.jsonl --flatten-nested --no-keep-nested-parents

# Named Excel sheet
undatum stats workbook.xlsx --table Sheet2
```

**What is filled depends on the engine.** DuckDB populates missing rates, type categories, and distribution stats. `--engine iterable` still reports field names, uniqueness, and lengths; `mean` / `median` / `stddev` / `type_category` / `missing_rate` are often empty.

**DuckDB statistics include:**
- Field types and array flags
- Missing value rates (count and percentage)
- Cardinality (distinct counts and percentages)
- Type inference: `categorical`, `numerical`, or `text`
- Distribution for numerical fields: **mean, median, min, max, stddev** (not percentiles)
- Unique value counts and percentages
- Min/max/average lengths
- Date field detection (`--checkdates` / `--no-checkdates`; default on)

**Other options:** `--dictshare`, `--threads`, `--progress` / `--no-progress`, `--zipfile`, `--engine auto|duckdb|iterable`, `--format-out json|html|markdown`. Also accepts `--table`, `--flatten-nested`, `--on-error`, `--error-log`, `--quotechar`, and `--trust` ([shared options](/commands/shared-options)).

#### Profiling metrics (DuckDB)

**Missing value analysis:** count and percentage of missing/null values per field. Example: `5 (2.5%)` means 5 missing values out of 200 records.

**Cardinality:** distinct count and percentage of distinct values. High cardinality (IDs, timestamps) vs low cardinality (status, category).

**Type inference:**
- **categorical** — low cardinality, typically string-like
- **numerical** — numeric types
- **text** — everything else (there is no `Mixed` category)

**Distribution (numerical fields):** mean (μ), median (m), min, max, standard deviation. Example: `μ=42.5, m=40.0`.

#### Use cases

```bash
# Profile dataset to identify quality issues
undatum profile customer_data.csv

# Look for high missing rates, unexpected cardinality, and min/max outliers

# Understand structure before processing
undatum profile new_dataset.jsonl --format-out json --output profile.json
```
