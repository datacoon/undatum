# Change: Improve DuckDB Operations

## Why

Undatum currently uses DuckDB for some operations (stats, counter, selector, ingester), but many
commands that could benefit from DuckDB's columnar processing still use pure Python row-by-row
processing. This limits performance on large datasets, especially for formats DuckDB supports
natively (CSV, JSONL, JSON, Parquet).

**Current Issues:**
1. **Limited DuckDB usage**: Only a few commands leverage DuckDB despite it being a dependency
2. **No engine selection**: Users cannot choose between DuckDB and Python engines
3. **Missing performance tuning**: DuckDB configuration options are not exposed
4. **Inconsistent patterns**: Some commands use DuckDB, others don't, with no unified approach

**Expected Benefits:**
- **10-100x performance improvement** for supported formats on large datasets
- **Consistent engine selection** across all commands
- **Better scalability** for big data workloads
- **User control** over engine selection and performance tuning

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 1.1)

## What Changes

- **ADDED**: Engine selector (`--engine auto|duckdb|python`) to commands: `sort`, `frequency`, `uniq`,
  `sample`, `search`, `dedup`, `slice`, `join`
- **ADDED**: Automatic engine detection (`--engine auto`) that selects DuckDB for supported formats
  (CSV, JSON, Parquet) when operation is expressible as SQL
- **ADDED**: DuckDB tuning options:
  - `--duckdb-threads N` - Set number of threads for DuckDB
  - `--duckdb-memory <bytes|MB|GB>` - Set memory limit for DuckDB
  - `--duckdb-temp-dir /path/to/tmp` - Set temporary directory for DuckDB
- **MODIFIED**: Command implementations to support DuckDB engine path with fallback to Python engine
- **MODIFIED**: Unified engine selection logic shared across commands

All changes maintain backward compatibility. Python engine remains default fallback for unsupported
formats or when DuckDB queries fail.

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/cmds/sorter.py` - Add DuckDB engine for sort operations
  - `undatum/cmds/searcher.py` - Add DuckDB engine for search operations
  - `undatum/cmds/deduplicator.py` - Add DuckDB engine for dedup operations
  - `undatum/cmds/slicer.py` - Add DuckDB engine for slice operations
  - `undatum/cmds/joiner.py` - Add DuckDB engine for join operations
  - `undatum/cmds/sampler.py` - Add DuckDB engine for sample operations
  - `undatum/cmds/statistics.py` - Extend existing DuckDB support (already has some)
  - `undatum/core.py` - Add `--engine` and DuckDB tuning options to CLI commands
  - New shared module for engine selection and DuckDB configuration
- **Dependencies**: `duckdb` (already a project dependency, no new dependencies)
- **Supported formats**: CSV, JSONL, JSON, Parquet with compression (gzip, zstd, raw)
- **Backward compatibility**: Fully backward compatible - Python engine remains default for
  unsupported formats and error cases
