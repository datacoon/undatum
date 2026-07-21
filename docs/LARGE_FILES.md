# Large-file conversion

undatum streams by default, but some paths (especially Parquet writes and whole-dataset
operations like sort/dedup) need explicit low-memory behavior for multi-GB inputs.

## Recommended flags

```bash
# Prefer DuckDB spill-to-disk when the format is duckable (csv/jsonl/parquet + gz/zst)
undatum convert huge.jsonl.zst huge.parquet --low-memory

# Force smaller iterabledata write batches even when DuckDB is unavailable
undatum convert data.xml data.parquet --low-memory --engine python --batch-size 5000

# External merge sort / disk-backed dedup
undatum sort data.jsonl --by ts --low-memory --output sorted.jsonl
undatum dedup data.jsonl --key-fields id --low-memory --output unique.jsonl
```

## Notes

- Gzip-compressed duckable formats are eligible for the DuckDB engine (`gz` / `gzip` codec ids).
- `--low-memory` on convert prefers DuckDB `COPY ... TO` for Parquet/CSV/JSONL when possible.
- Sort automatically spills after ~100k buffered rows; `--low-memory` forces spill immediately.
- Dedup spills unique keys to a temporary SQLite store when the in-memory set grows large, or
  immediately with `--low-memory`.
- Temp files use the system temp directory unless `--duckdb-temp-dir` / temp options are set.

See also: [Quickstarts](QUICKSTART.md), issue-oriented roadmap in
`docs/undatum-improvement-recommendations.md`.
