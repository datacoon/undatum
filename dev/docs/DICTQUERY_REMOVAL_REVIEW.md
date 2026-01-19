# dictquery Dependency Removal Review

## Executive Summary

The `dictquery` library is used in **2 files** with **8 total usage locations** for filtering dictionary records based on query expression strings. This review analyzes the dependency and provides recommendations for removal.

---

## Current Usage Analysis

### Files Using dictquery

1. **`undatum/cmds/validator.py`** (3 usages)
   - Lines 61, 77, 97: `dq.match(r, options['filter'])` 
   - Used to filter CSV, JSONL, and BSON records before validation

2. **`undatum/cmds/selector.py`** (5 usages)
   - Line 107: `query_obj.match(r, filter_expr)` in `get_iterable_fields_freq()`
   - Line 287: `dq.match(r, options['filter'])` in `select()` method
   - Lines 377, 403, 419: `dq.match(r, options['filter'])` in `split()` method for CSV and JSONL formats

### Functionality Provided

`dictquery.match(data, query_string)` evaluates boolean expressions on dictionaries:

- **Comparison operators**: `==`, `!=`, `<`, `<=`, `>`, `>=`
- **String matching**: `LIKE` (with wildcards), `MATCH` (regex)
- **Membership**: `IN`, `CONTAINS`
- **Logical operators**: `AND`, `OR`, `NOT`
- **Nested keys**: Supports dot notation (e.g., `user.name`)
- **Special values**: `NOW`, `REGEXP`, arrays

### Query Expression Examples
```python
"age >= 12"
"gender == 'female' AND age > 18"
"`user.name` LIKE 'John*'"
"`user.email` MATCH /\w+@\w+\.com/"
"status IN ['active', 'pending']"
```

---

## Dependencies Declared

- `requirements.txt`: `dictquery>=0.5.0`
- `setup.py`: `'dictquery>=0.5.0'` (line 42)
- `pyproject.toml`: `"dictquery>=0.5.0"` (line 20)
- `pyproject.toml`: mypy override (line 127)

---

## Removal Options & Recommendations

### Option 1: Use `mistql` (RECOMMENDED) ⭐

**Pros:**
- Already in the codebase (`undatum/cmds/query.py` uses it)
- More feature-rich and actively maintained
- Better performance for complex queries
- Similar syntax and capabilities

**Cons:**
- May require syntax translation/adapter if filter expressions differ
- Need to test compatibility with existing filter expressions

**Implementation:**
- Create a wrapper/adapter function: `def match_filter(record, filter_expr)`
- Translate dictquery syntax to mistql if needed, or document migration
- Replace `dq.match()` calls with the wrapper

**Code Example:**
```python
from mistql import query as mistql_query

def match_filter(record, filter_expr):
    """Match record against filter expression using mistql."""
    # mistql returns filtered result, so convert to boolean
    result = mistql_query(f"filter({filter_expr})", [record])
    return len(result) > 0

# Or use mistql's built-in filtering
def match_filter(record, filter_expr):
    """Match record against filter expression."""
    from mistql import query
    # Assuming filter_expr is already in mistql format
    # May need syntax translation layer
    return query(f"?{filter_expr}", record) is not None
```

---

### Option 2: Implement Lightweight Filter Parser

**Pros:**
- No external dependencies
- Full control over syntax
- Can optimize for specific use cases
- Minimal code surface

**Cons:**
- Requires implementing parser and evaluator
- Must handle edge cases (nested keys, escaping, etc.)
- More maintenance burden
- Potential security concerns with `eval()` (avoid using `eval()`)

**Implementation Approach:**
```python
import re
import operator as op
from typing import Any, Dict

def match_filter(record: Dict[str, Any], filter_expr: str) -> bool:
    """Simple filter matcher for common patterns."""
    # Parse simple expressions like "field == 'value'", "age >= 18"
    # Use AST module for safe parsing
    # Support basic operators and nested key access
    pass
```

**Considerations:**
- Use Python's `ast` module for safe expression parsing
- Support common patterns: `==`, `!=`, `<`, `>`, `<=`, `>=`, `IN`, `AND`, `OR`
- Use `get_dict_value()` utility for nested key access (already exists in codebase)

---

### Option 3: Use DuckDB SQL for DuckDB Engine Paths

**Pros:**
- For files processed via DuckDB engine, use SQL `WHERE` clauses
- No additional dependency
- Very powerful and performant
- Already integrated with DuckDB usage

**Cons:**
- Only works when using DuckDB engine
- Need separate solution for iterable engine paths
- Filter expressions would need SQL translation

**Implementation:**
- For `get_duckdb_fields_freq()` and `get_duckdb_fields_uniq()`, add WHERE clause support
- Keep Option 1 or 2 for iterable engine paths

---

### Option 4: Hybrid Approach (RECOMMENDED for Migration)

**Strategy:**
1. **For DuckDB engine**: Use SQL WHERE clauses (Option 3)
2. **For iterable engine**: Use mistql wrapper (Option 1)
3. **For simple cases**: Fall back to lightweight parser for basic expressions

**Benefits:**
- Leverages existing infrastructure (DuckDB, mistql)
- Minimal new code
- Better performance for DuckDB paths
- Consistent filtering across engines

---

## Migration Plan

### Phase 1: Assessment
1. ✅ Audit all `dictquery` usage locations (COMPLETE - 8 locations found)
2. Collect sample filter expressions from real usage
3. Test compatibility between dictquery and mistql syntax
4. Identify which filters are simple enough for lightweight parser

### Phase 2: Implementation
1. Create filter matching utility module (`undatum/common/filter.py`)
2. Implement mistql adapter/wrapper
3. Add DuckDB WHERE clause support where applicable
4. Update all `dq.match()` calls to use new utility

### Phase 3: Testing
1. Write unit tests for filter expressions
2. Test with real data files
3. Verify backward compatibility with existing filter syntax
4. Performance testing vs. dictquery

### Phase 4: Cleanup
1. Remove `dictquery` from `requirements.txt`
2. Remove from `setup.py`
3. Remove from `pyproject.toml` (dependencies + mypy override)
4. Remove imports from `validator.py` and `selector.py`
5. Update documentation

---

## Compatibility Considerations

### Filter Expression Syntax

**dictquery syntax examples:**
```python
"age >= 12"
"`user.name` == 'John'"
"status IN ['active', 'pending']"
"email MATCH /\w+@\w+\.com/"
```

**mistql syntax** (may differ - needs verification):
```python
# Check mistql documentation for exact syntax
```

**Recommendation:**
- If syntax differs significantly, create a translation layer
- Or document syntax changes and provide migration guide
- Consider supporting both during transition period

---

## Risk Assessment

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Filter syntax incompatibility | High | Medium | Test with real expressions, provide migration guide |
| Performance regression | Medium | Low | Benchmark mistql vs dictquery, optimize hot paths |
| Missing features | Medium | Low | Audit feature parity, implement missing features |
| Breaking changes | High | Low | Comprehensive testing, staged rollout |

---

## Recommendation Summary

**Primary Recommendation:** Use **Option 1 (mistql)** with **Option 4 (Hybrid)** approach:

1. **Short term**: Create mistql wrapper in `undatum/common/filter.py`
2. **Enhancement**: Add DuckDB WHERE clause support for DuckDB engine paths
3. **Fallback**: For very simple expressions, consider lightweight parser if mistql overhead is too high

**Rationale:**
- mistql is already a project dependency
- Reduces total external dependencies
- Provides better query capabilities
- DuckDB integration offers performance benefits
- Maintains flexibility for future enhancements

---

## Files to Modify

1. **`undatum/cmds/validator.py`**
   - Remove: `import dictquery as dq` (line 8)
   - Replace: `dq.match()` calls (lines 61, 77, 97)

2. **`undatum/cmds/selector.py`**
   - Remove: `import dictquery as dq` (line 9)
   - Replace: `dq.match()` calls (lines 107, 287, 377, 403, 419)

3. **`undatum/common/filter.py`** (NEW)
   - Create filter matching utility with mistql adapter

4. **`requirements.txt`**
   - Remove: `dictquery>=0.5.0`

5. **`setup.py`**
   - Remove: `'dictquery>=0.5.0'` from install_requires

6. **`pyproject.toml`**
   - Remove: `"dictquery>=0.5.0"` from dependencies
   - Remove: `"dictquery.*"` from mypy overrides

---

## Next Steps

1. **Verify mistql syntax compatibility** with existing filter expressions
2. **Create proof-of-concept** mistql wrapper
3. **Test with sample filter expressions** from real usage
4. **Implement migration** following the phase plan above
5. **Update documentation** to reflect filter expression syntax

---

## References

- [dictquery PyPI](https://pypi.org/project/dictquery/)
- [mistql PyPI](https://pypi.org/project/mistql/)
- Current usage: `undatum/cmds/query.py` (mistql example)
- Related utilities: `undatum/utils.py` - `get_dict_value()` for nested key access
