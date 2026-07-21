# Change: Streaming Parquet Writes and Low-Memory Mode

## Why
Issue #34 reports OOM converting 5–8 GB jsonl.zst → parquet even with 96 GB RAM (open since
2024-11). Large Parquet conversions must stream/batch writes and spill to disk; users also
asked for an explicit low-memory mode and docs on large-file behavior.

## What Changes
- Stream Parquet output in batches (pyarrow `ParquetWriter` per row-group) instead of buffering
  the full dataset.
- Route large conversions through DuckDB (spill-to-disk) when the engine selector allows.
- Add `--low-memory` (or equivalent) mode that prefers streaming/spill paths over in-memory
  materialization.
- Document large-file conversion behavior and recommended flags.

## Impact
- Affected specs: `data-processing`
- Affected code: `undatum/cmds/converter.py`, engine selector, progress/batch helpers, docs
- Related issues: #34 (highest-impact open external bug)
- Dependencies: pairs with `fix-gzip-duckdb-codec-routing` and `add-pyarrow-default-dependency`
