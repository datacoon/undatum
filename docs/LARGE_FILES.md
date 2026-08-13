# Large-file conversion

undatum streams by default, but some paths (especially Parquet writes and whole-dataset
operations like sort/dedup) need explicit low-memory behavior for multi-GB inputs.

## Recommended flags

```bash
# Prefer DuckDB spill-to-disk when the format is duckable (csv/jsonl/parquet + gz/zst)
undatum convert huge.jsonl.zst huge.parquet --low-memory

# Force smaller iterabledata write batches even when DuckDB is unavailable
undatum convert data.xml data.parquet --low-memory --engine python --batch-size 5000

# Parquet row-group size (iterable path; DuckDB COPY ignores this flag)
undatum convert data.csv data.parquet --row-group-size 100000 --batch-size 50000 --engine iterable

# Multiprocessing for CPU-bound Python-engine convert (GitHub #18 / P1.8)
# Uses process-pool chunk batches; preserves row order; omit --threads for sequential
undatum convert big.csv out.jsonl --engine python --threads 8

# Parallel rule-file validation / iterable stats
undatum validate data.csv --rules rules.yml --threads 4
undatum stats data.csv --engine iterable --threads 4

# External merge sort / disk-backed dedup
undatum sort data.jsonl --by ts --low-memory --output sorted.jsonl
undatum dedup data.jsonl --key-fields id --low-memory --output unique.jsonl
```

## Multiprocessing notes

- `--threads N` opts into undatum process-pool chunk parallelism on **Python/iterable**
  paths. Small files may be slower due to process startup overhead.
- DuckDB already parallelizes internally. Prefer `--engine duckdb` / `--duckdb-threads`
  for duckable formats; undatum does **not** wrap DuckDB `COPY` in an extra process pool.
- Bulk directory conversion (`--recursive`) already uses `--threads` as concurrent file workers.
- Order-sensitive whole-file ops (`sort`, global `dedup`) are not parallelized this way.

## Notes

- Gzip-compressed duckable formats are eligible for the DuckDB engine (`gz` / `gzip` codec ids).
- `--low-memory` on convert prefers DuckDB `COPY ... TO` for Parquet/CSV/JSONL when possible.
- Sort automatically spills after ~100k buffered rows; `--low-memory` forces spill immediately.
- Dedup spills unique keys to a temporary SQLite store when the in-memory set grows large, or
  immediately with `--low-memory`.
- Temp files use the system temp directory unless `--duckdb-temp-dir` / temp options are set.
- iterabledata 1.0.17+ keeps Parquet/Arrow writes in bounded batches (`row_group_size` / flush
  batches) and exposes codec profiles `fast` / `balanced` / `max`; `undatum convert --row-group-size`
  forwards the Parquet flush threshold (skips DuckDB COPY); `undatum repack` defaults
  to maximum container or format-native compression.

See also: [Quickstarts](QUICKSTART.md), [Format support](FORMAT_SUPPORT.md), issue-oriented
roadmap in `dev/docs/undatum-improvement-recommendations.md`.
