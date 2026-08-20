---
title: "sql"
description: "undatum sql command reference"
---
# `sql`

Run ad-hoc DuckDB SQL queries over data files (CSV, JSONL, Parquet, and other DuckDB-readable formats). A single input file can be referenced as the view `data`; every file is also registered as a view named after its file stem.

```bash
# Aggregate a CSV
undatum sql "SELECT city, COUNT(*) AS n FROM data GROUP BY city" cities.csv

# Join two files (views named after file stems: orders, users)
undatum sql "SELECT * FROM orders JOIN users USING (user_id)" orders.csv users.parquet

# Save the result as Parquet
undatum sql "SELECT * FROM data WHERE amount > 100" sales.jsonl --output big.parquet --format parquet
```

Output formats: `jsonl` (default), `csv`, `parquet` (requires `--output`). DuckDB resources can be tuned with `--duckdb-threads` and `--duckdb-memory`. This is the ad-hoc query command for files; `undatum db query` runs SQL against a database URI.
