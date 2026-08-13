# Change: Remove dictquery Dependency and Replace with Hybrid Filter Solution

## Why

The `dictquery` library is used in only **2 files** with **8 total usage locations** for filtering dictionary records. Removing this dependency reduces external dependencies and simplifies the project's dependency tree. Based on review analysis (see `DICTQUERY_REMOVAL_REVIEW.md`), a hybrid approach using `mistql` (already a dependency) and DuckDB SQL WHERE clauses will provide equivalent or better functionality with improved performance.

Key motivations:
- **Reduce dependencies**: Eliminate `dictquery>=0.5.0` from requirements
- **Leverage existing infrastructure**: Use `mistql` already in codebase for querying
- **Improve performance**: Use DuckDB SQL for DuckDB engine paths
- **Unified filtering**: Create a common filter utility module for consistency
- **Better maintainability**: Reduce code surface by using established libraries

## What Changes

- **REMOVED**: `dictquery` dependency from `requirements.txt`, `setup.py`, and `pyproject.toml`
- **REMOVED**: `dictquery` imports from `undatum/cmds/validator.py` and `undatum/cmds/selector.py`
- **ADDED**: `undatum/common/filter.py` - New filter matching utility module with mistql adapter
- **MODIFIED**: `undatum/cmds/validator.py` - Replace `dq.match()` calls (3 locations) with new filter utility
- **MODIFIED**: `undatum/cmds/selector.py` - Replace `dq.match()` calls (5 locations) with new filter utility
- **ENHANCED**: DuckDB engine paths in `selector.py` - Add WHERE clause support for filtering when using DuckDB
- **MODIFIED**: `openspec/project.md` - Update tech stack to remove dictquery reference

All changes maintain backward compatibility for filter expression syntax through an adapter layer. The implementation uses a hybrid approach:
1. **DuckDB engine**: SQL WHERE clauses for optimal performance
2. **Iterable engine**: mistql wrapper for dictionary filtering
3. **Simple fallback**: Lightweight parser for basic expressions if needed

**BREAKING**: Filter expression syntax differs from dictquery - mistql uses `&&`, `||`, `!` instead of `AND`, `OR`, `NOT`. Users may need to update filter expressions if using logical operators.

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/cmds/validator.py` - Replace dictquery usage (3 locations)
  - `undatum/cmds/selector.py` - Replace dictquery usage (5 locations) 
  - `undatum/common/filter.py` - New utility module (NEW)
  - `requirements.txt` - Remove dictquery dependency
  - `setup.py` - Remove dictquery from install_requires
  - `pyproject.toml` - Remove dictquery from dependencies and mypy overrides
  - `openspec/project.md` - Update tech stack documentation
- **Dependencies**: 
  - **Removed**: `dictquery>=0.5.0`
  - **Used**: `mistql` (already a dependency) for filter matching
- **Backward compatibility**: Filter expressions use mistql syntax (`&&`, `||`, `!`). Users may need to update expressions that used dictquery syntax (`AND`, `OR`, `NOT`).

**Implementation Status**: ✅ COMPLETE - See `IMPLEMENTATION_SUMMARY.md` for details.
