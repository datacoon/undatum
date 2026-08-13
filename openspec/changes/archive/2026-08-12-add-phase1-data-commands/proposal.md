# Change: Add Phase 1 Data Processing Commands

## Why

undatum currently lacks several fundamental data inspection and manipulation commands that are standard in tools like xsv, qsv, and Miller. These simple, high-value commands would provide immediate utility to users and establish patterns for future command additions. Based on research of similar tools, Phase 1 focuses on low-complexity, high-utility commands that can be implemented quickly.

These commands address common needs:
- Quick data inspection (`count`, `table`, `head`, `tail`)
- Simple transformations (`reverse`, `enum`, `fixlengths`)
- Better user experience for data exploration

## What Changes

- **ADDED**: `count` command - Count rows in data files (supports all formats)
- **ADDED**: `table` command - Pretty-print data as aligned table for inspection
- **ADDED**: `reverse` command - Reverse the order of rows
- **ADDED**: `enum` command - Add row numbers, UUIDs, or constants to records
- **ADDED**: `head` command - Extract first N rows
- **ADDED**: `tail` command - Extract last N rows
- **ADDED**: `fixlengths` command - Ensure all rows have same number of fields (padding/truncation)

All commands will:
- Support all undatum formats (CSV, JSONL, BSON, XML, etc.)
- Use existing streaming infrastructure
- Leverage DuckDB engine when beneficial
- Follow existing command patterns in `undatum/cmds/`
- Support filtering and field selection where applicable

## Impact

- **Affected specs**: `data-processing` capability (new)
- **Affected code**:
  - `undatum/core.py` - Add new command definitions
  - `undatum/cmds/` - New command classes (one per command)
  - `README.md` - Update documentation with new commands
- **Dependencies**: No new dependencies required (uses existing `rich` library for `table`)
- **Backward compatibility**: No breaking changes, only additions
