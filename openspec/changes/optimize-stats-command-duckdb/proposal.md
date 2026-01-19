# Change: Optimize Stats Command with DuckDB Engine

## Why

The `undatum stats` command currently processes all file formats using the iterable engine, which processes files row-by-row. This approach is slow for large files, especially on formats that DuckDB supports natively (CSV, JSONL, JSON, Parquet). Based on comprehensive analysis (see `dev/docs/STATS_COMMAND_DUCKDB_IMPROVEMENT_REPORT.md`), the command can be significantly optimized by leveraging DuckDB's columnar processing capabilities.

**Current Issues:**
1. **Slow performance**: Row-by-row processing makes statistics generation slow for large files (minutes to hours for 1M+ rows)
2. **Underutilized capabilities**: DuckDB is already a dependency and used by other commands (`counter`, `analyzer`, `selector`), but stats command doesn't use it
3. **Inefficient aggregations**: Manual unique value tracking and length calculations done in Python loops instead of optimized SQL
4. **Missing engine choice**: Users cannot select DuckDB engine for faster processing on supported formats

**Expected Benefits:**
- **10-100x performance improvement** for supported formats (CSV, JSONL, JSON, Parquet)
- **Consistent patterns** with other commands that already use DuckDB engine selection
- **Better scalability** for large datasets
- **Maintains accuracy** by using DuckDB's proven aggregation functions

## What Changes

- **ADDED**: Engine detection logic in `StatProcessor.stats()` method - automatically selects DuckDB for supported formats when `engine='auto'`
- **ADDED**: `stats_duckdb()` method in `StatProcessor` class - computes statistics using DuckDB SQL queries
- **ADDED**: DuckDB path for basic statistics - uses `duckdb_decompose()` with `use_summarize=True` for unique counts and uniqueness percentages
- **ADDED**: DuckDB path for length statistics - custom SQL queries for min/max/avg length calculations
- **ADDED**: DuckDB path for dictionary construction - uses `GROUP BY` with `COUNT(*)` for efficient frequency analysis
- **ADDED**: Type detection sampling - samples values for type detection (hybrid approach maintaining current behavior)
- **MODIFIED**: `stats()` method in `StatProcessor` - adds engine detection and DuckDB path with fallback to iterable engine
- **MODIFIED**: CLI `stats` command in `undatum/core.py` - adds `--engine` option (auto/duckdb/iterable) for explicit engine selection
- **MODIFIED**: Progress indication - updates progress bar implementation to work with DuckDB's two-phase approach (count rows, then compute statistics)

All changes maintain backward compatibility. The iterable engine remains as the default fallback for unsupported formats or when DuckDB queries fail.

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/cmds/statistics.py` - Add DuckDB engine detection and statistics computation
  - `undatum/core.py` - Add `--engine` option to `stats` command
  - `undatum/common/schema_utils.py` - Reuse existing `duckdb_decompose()` function (no changes needed)
- **Dependencies**: `duckdb` is already a project dependency (no new dependencies)
- **Supported formats**: CSV, JSONL, JSON, Parquet with compression (gzip, zstd, raw)
- **Backward compatibility**: Fully backward compatible - iterable engine remains for unsupported formats and error cases
