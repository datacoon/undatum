# Change: Enhance Stats with Advanced Profiling

## Why

The current `stats` command provides basic field statistics, but lacks advanced profiling features that data scientists and analysts need for comprehensive dataset understanding. Enhancing stats with missing value rates, cardinality analysis, type inference, and distribution information would make undatum a more complete data profiling tool.

**Current Issues:**
1. **Limited statistics**: Missing value rates and cardinality not shown
2. **No type inference**: Doesn't distinguish categorical vs numerical fields
3. **Basic distributions**: No percentile information or distribution summaries
4. **No profile alias**: Users must remember to use `stats` for profiling

**Expected Benefits:**
- **Comprehensive profiling** with missing values, cardinality, and distributions
- **Type inference** to identify categorical vs numerical fields
- **Distribution summaries** with percentiles and statistical measures
- **Profile alias** for intuitive command naming
- **Better data understanding** for analysts and data scientists

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 3.2)

## What Changes

- **ENHANCED**: `undatum stats` command with additional profiling metrics:
  - Missing value rates per field
  - Distinct counts and cardinality percentages
  - Type inference (categorical vs numerical)
  - Distribution info (mean, median, percentiles for numerics)
- **ADDED**: `undatum profile` command as alias for `stats`
- **ADDED**: Enhanced output format with profiling sections
- **ADDED**: Optional JSON output for programmatic access to profiling data

All changes are backward compatible. Existing `stats` functionality remains unchanged, with new metrics added.

## Impact

- **Affected specs**: `data-analysis` capability
- **Affected code**:
  - `undatum/cmds/statistics.py` - Enhance StatProcessor class
  - `undatum/core.py` - Add `profile` command alias
  - Output formatting enhancements
- **Dependencies**: No new dependencies (uses existing statistics infrastructure)
- **Backward compatibility**: Fully backward compatible - existing stats output unchanged, new metrics added
