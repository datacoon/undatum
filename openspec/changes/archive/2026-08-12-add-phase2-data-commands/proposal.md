# Change: Add Phase 2 Data Processing Commands

## Why

Phase 2 focuses on medium-complexity commands that are essential for common data processing workflows. These commands enable sorting, sampling, searching, deduplication, and data cleaning operations that complement undatum's existing capabilities. Based on research of xsv, qsv, Miller, and csvtk, these operations are frequently used in data pipelines.

These commands address critical needs:
- Data organization (`sort`, `dedup`)
- Data exploration (`sample`, `search`)
- Data cleaning (`fill`, `rename`, `explode`, `replace`)
- File operations (`cat`)

## What Changes

- **ADDED**: `sort` command - Sort rows by one or more columns (with external merge for large files)
- **ADDED**: `sample` command - Random sampling of rows (reservoir sampling algorithm)
- **ADDED**: `search` command - Regex-based search and filtering across fields
- **ADDED**: `dedup` command - Remove duplicate rows (in-memory and external approaches)
- **ADDED**: `fill` command - Fill empty/null values with specified values or strategies
- **ADDED**: `rename` command - Rename fields by exact mapping or regex patterns
- **ADDED**: `explode` command - Split column by separator into multiple rows
- **ADDED**: `replace` command - String replacement in specific fields (simple and regex)
- **ADDED**: `cat` command - Concatenate files by rows or columns

All commands will:
- Support all undatum formats (CSV, JSONL, BSON, XML, etc.)
- Use streaming where possible for memory efficiency
- Leverage DuckDB engine for performance when beneficial
- Support filtering and field selection where applicable
- Follow existing command patterns in `undatum/cmds/`

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/core.py` - Add new command definitions
  - `undatum/cmds/` - New command classes (one per command)
  - `README.md` - Update documentation with new commands
- **Dependencies**: No new dependencies required (uses standard library `re` for regex)
- **Backward compatibility**: No breaking changes, only additions
