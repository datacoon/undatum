# Change: Add Phase 3 Advanced Data Processing Commands

## Why

Phase 3 focuses on complex, high-value commands that enable sophisticated data workflows. These commands provide relational operations, data comparison, advanced transformations, and enhanced format detection. Based on research of xsv, qsv, and csvtk, these operations are essential for complex data pipelines and analysis workflows.

These commands address advanced needs:
- Relational operations (`join`, `diff`, `exclude`)
- Data reshaping (`transpose`)
- Enhanced detection (`sniff`)
- Format improvements (enhanced `slice`, enhanced `fmt`)

## What Changes

- **ADDED**: `join` command - Relational joins between two files (inner, left, right, full outer)
- **ADDED**: `diff` command - Compare two files and show differences
- **ADDED**: `exclude` command - Remove rows from one file based on keys in another file
- **ADDED**: `transpose` command - Swap rows and columns
- **ADDED**: `sniff` command - Detect file properties (delimiter, encoding, types, record count)
- **MODIFIED**: Enhanced `slice` command - Dedicated slicing with range support (currently partial via `convert`)
- **MODIFIED**: Enhanced `fmt` command - CSV-specific formatting options (currently partial via `convert`)

All commands will:
- Support all undatum formats where applicable
- Use streaming where possible for memory efficiency
- Leverage DuckDB engine for performance when beneficial
- Support filtering and field selection where applicable
- Follow existing command patterns in `undatum/cmds/`

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/core.py` - Add new command definitions and enhance existing
  - `undatum/cmds/` - New command classes and enhancements
  - `undatum/cmds/converter.py` - Enhance `fmt` capabilities
  - `README.md` - Update documentation
- **Dependencies**: No new dependencies required
- **Backward compatibility**: No breaking changes; enhancements to existing commands maintain backward compatibility
