# Design: Remove dictquery Dependency - Hybrid Filter Solution

## Context

The `dictquery` library is used in only 2 files (`validator.py`, `selector.py`) with 8 total usage locations for filtering dictionary records. All usages follow the same pattern: `dq.match(record, filter_expression)` to determine if a record matches a boolean query expression.

Current usage:
- **validator.py**: 3 locations (CSV, JSONL, BSON format handling)
- **selector.py**: 5 locations (frequency calculation, select method, split method)

The codebase already has `mistql` as a dependency (used in `undatum/cmds/query.py`), which provides similar query capabilities. Additionally, DuckDB engine paths in selector.py could benefit from SQL WHERE clauses for filtering.

## Goals

- Remove `dictquery` dependency completely
- Maintain backward compatibility for filter expressions where possible
- Improve performance by using DuckDB SQL WHERE clauses for DuckDB engine paths
- Create unified filter utility for consistency across codebase
- Minimize code changes and complexity

## Non-Goals

- Adding new filter expression syntax features beyond what dictquery provides
- Supporting all possible filter expression edge cases immediately (can be enhanced iteratively)
- Implementing a full query language parser from scratch

## Decisions

### Decision 1: Use Hybrid Approach

**Decision**: Implement filtering using a hybrid approach:
1. **DuckDB engine paths**: Use SQL WHERE clauses
2. **Iterable engine paths**: Use mistql wrapper
3. **Simple cases**: Consider lightweight parser if mistql overhead is significant

**Rationale**:
- Leverages existing infrastructure (DuckDB, mistql) without adding dependencies
- Optimal performance for DuckDB paths (native SQL)
- Consistent with current architecture where DuckDB and iterable engines coexist
- Minimal new code surface

**Alternatives Considered**:
- **Option A**: Use only mistql for all paths
  - Pros: Single implementation, consistent
  - Cons: Doesn't leverage DuckDB's native SQL capabilities
- **Option B**: Implement custom lightweight parser
  - Pros: No dependencies, full control
  - Cons: Significant development effort, maintenance burden, must handle all edge cases
- **Option C**: Keep dictquery
  - Pros: No migration needed
  - Cons: Maintains unnecessary dependency, prevents consolidation

### Decision 2: Filter Utility Module Location

**Decision**: Create `undatum/common/filter.py` as a new utility module.

**Rationale**:
- Follows existing pattern (`undatum/common/` contains shared utilities like `functions.py`, `iterable.py`, `scheme.py`)
- Centralized location for filter-related functionality
- Can be imported by multiple command modules
- Easy to test in isolation

**Alternatives Considered**:
- Embedding in `utils.py`: Too generic, would mix concerns
- Per-module implementations: Code duplication, inconsistent behavior

### Decision 3: Filter Expression Syntax Compatibility

**Decision**: Create adapter/wrapper layer to maintain compatibility with dictquery syntax where possible.

**Rationale**:
- Minimizes breaking changes for users
- Allows gradual migration if syntax differs
- Provides clear error messages if expressions need adjustment

**Implementation Strategy**:
1. Verify mistql syntax compatibility with dictquery expressions
2. If compatible: direct passthrough
3. If incompatible: create translation layer for common patterns
4. Document any syntax differences or limitations

**Fallback**: If mistql syntax differs significantly, provide clear migration guide and examples.

### Decision 4: DuckDB WHERE Clause Translation

**Decision**: Implement basic WHERE clause translation for common filter patterns.

**Rationale**:
- Significant performance benefit for DuckDB engine paths
- DuckDB is already used for frequency and uniq operations in selector.py
- Basic translation covers most common use cases (==, !=, <, >, <=, >=, AND, OR)

**Implementation Approach**:
- Support basic comparison operators and logical operators
- Translate nested key access to SQL dot notation where applicable
- Fall back to iterable engine filtering for complex expressions not easily translated
- Document limitations clearly

**Future Enhancement**: Can expand translation support iteratively as needed.

## Architecture

### Filter Utility Module Structure

```
undatum/common/filter.py
├── match_filter(record, filter_expr) -> bool
│   ├── Main entry point for filter matching
│   └── Delegates to mistql-based implementation
│
├── match_filter_mistql(record, filter_expr) -> bool
│   ├── Uses mistql to evaluate filter expression
│   └── Handles syntax translation if needed
│
└── translate_filter_to_sql(filter_expr) -> str | None
    ├── Translates basic filter expressions to SQL WHERE clause
    ├── Returns None if translation not possible
    └── Used by DuckDB engine paths
```

### Integration Points

1. **validator.py**: Replace `dq.match()` calls with `match_filter()` from filter utility
2. **selector.py**: 
   - Replace `dq.match()` calls with `match_filter()` for iterable paths
   - Use `translate_filter_to_sql()` for DuckDB paths in `get_duckdb_fields_freq()` and `get_duckdb_fields_uniq()`
3. **Future commands**: Can use `match_filter()` for consistent filtering

### Filter Expression Flow

```
User provides filter expression
    ↓
validator.py / selector.py
    ↓
[If DuckDB engine] → translate_filter_to_sql() → SQL WHERE clause → DuckDB
    ↓
[If iterable engine] → match_filter() → match_filter_mistql() → mistql → Result
```

## Risks / Trade-offs

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Filter syntax incompatibility | High | Medium | Verify compatibility in Phase 1, create translation layer if needed |
| Performance regression (mistql vs dictquery) | Medium | Low | Benchmark mistql performance, optimize hot paths if needed |
| DuckDB SQL translation complexity | Medium | Medium | Start with basic patterns, fall back to iterable engine for complex cases |
| Missing edge cases | Medium | Low | Comprehensive testing with real filter expressions, iterative enhancement |
| Breaking existing filter expressions | High | Low | Thorough testing, migration guide if syntax changes |

### Trade-offs

1. **Simplicity vs. Performance**: Using mistql for all paths is simpler but misses DuckDB optimization opportunity. Hybrid approach adds complexity but improves performance.

2. **Compatibility vs. Clean Slate**: Maintaining dictquery syntax compatibility adds translation complexity but preserves user experience. Clean slate would be simpler but breaks existing usage.

3. **Feature Completeness vs. Time to Market**: Starting with basic filter support and enhancing iteratively allows faster delivery, but may not cover all edge cases immediately.

## Migration Plan

### Phase 1: Assessment (Tasks 1.1-1.5)
- Collect real filter expressions
- Verify mistql compatibility
- Document syntax differences
- Create test cases

### Phase 2: Implementation (Tasks 2.1-5.10)
- Create filter utility module
- Implement mistql adapter
- Add DuckDB WHERE clause support
- Replace dictquery usage in validator.py and selector.py

### Phase 3: Testing (Tasks 7.1-7.12)
- Comprehensive test suite
- Performance testing
- Backward compatibility verification

### Phase 4: Cleanup (Tasks 6.1-9.6)
- Remove dependencies
- Update documentation
- Final verification

### Rollback Strategy

If issues arise during migration:
1. Keep dictquery import as fallback temporarily
2. Feature flag to switch between dictquery and new implementation
3. Can revert changes in isolated commits if needed

## Open Questions

1. **Q**: Does mistql support all dictquery syntax patterns we use?
   - **A**: Needs verification in Phase 1. If not, translation layer required.

2. **Q**: What is the performance difference between mistql and dictquery?
   - **A**: Benchmark during Phase 3. If mistql is significantly slower, consider lightweight parser for simple cases.

3. **Q**: Should we support dictquery syntax exactly or migrate to mistql-native syntax?
   - **A**: Prefer compatibility first, but document preferred syntax for new usage.

4. **Q**: How complex should DuckDB WHERE clause translation be?
   - **A**: Start simple (basic operators), expand iteratively based on real usage patterns.

## References

- Review document: `DICTQUERY_REMOVAL_REVIEW.md`
- Current dictquery usage: `undatum/cmds/validator.py`, `undatum/cmds/selector.py`
- mistql usage example: `undatum/cmds/query.py`
- Related utilities: `undatum/utils.py` - `get_dict_value()` for nested key access
- DuckDB integration: `undatum/cmds/selector.py` - `get_duckdb_fields_freq()`, `get_duckdb_fields_uniq()`
