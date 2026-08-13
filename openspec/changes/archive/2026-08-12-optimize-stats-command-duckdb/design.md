# Design: Optimize Stats Command with DuckDB Engine

## Context

The `stats` command currently processes all file formats using the iterable engine, which reads files row-by-row and performs statistics computation in Python. This approach works but is slow for large files, especially formats that DuckDB supports natively.

Other commands in undatum (`counter`, `analyzer`, `selector`, `joiner`, `sorter`) already use DuckDB for performance-critical operations, establishing a clear pattern for engine selection and DuckDB integration.

## Goals / Non-Goals

### Goals
- Achieve 10-100x performance improvement for supported formats (CSV, JSONL, JSON, Parquet)
- Maintain 100% accuracy compared to iterable engine output
- Follow existing patterns from other commands (`counter.py`, `analyzer.py`)
- Maintain backward compatibility (iterable engine as fallback)
- Support all existing statistics features (unique counts, lengths, types, dictionaries)

### Non-Goals
- Replace iterable engine entirely (must remain as fallback)
- Support DuckDB-only file formats (only formats already supported by iterable)
- Change statistics output format or structure
- Add new statistics that don't exist in current implementation

## Decisions

### Decision 1: Engine Selection Pattern
**Decision**: Reuse the `_detect_engine()` pattern from `counter.py` and `selector.py`

**Rationale**:
- Consistency with existing codebase
- Proven pattern that works well
- Automatic detection based on file format and compression

**Implementation**:
```python
def _detect_engine(fromfile, engine, filetype, compression):
    if engine == 'auto':
        if filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS:
            return 'duckdb'
        return 'iterable'
    return engine
```

**Alternatives Considered**:
- Always use DuckDB when available (rejected: may fail for edge cases, needs fallback)
- Separate command for DuckDB statistics (rejected: adds complexity, breaks unified interface)

### Decision 2: Statistics Computation Strategy
**Decision**: Hybrid approach using DuckDB for aggregations + sampling for type detection

**Components**:
1. **Basic statistics**: Use `duckdb_decompose()` with `use_summarize=True` (already proven in `analyzer.py`)
2. **Length statistics**: Custom SQL queries with `MIN(LENGTH(...))`, `MAX(LENGTH(...))`, `AVG(LENGTH(...))`
3. **Type detection**: Sample first N records, use existing `guess_datatype()` (maintains current behavior)
4. **Dictionary construction**: Use `GROUP BY` with `COUNT(*)` for frequencies

**Rationale**:
- Leverages existing, tested code (`duckdb_decompose`)
- Maintains type detection accuracy (current `guess_datatype()` logic)
- Efficient for large files (SQL aggregations vs Python loops)

**Alternatives Considered**:
- Pure DuckDB approach (rejected: DuckDB type inference may differ, loses current type detection nuances)
- Pure iterable approach (rejected: too slow, defeats purpose of optimization)

### Decision 3: Error Handling and Fallback
**Decision**: Try DuckDB first, fallback to iterable on any error

**Rationale**:
- DuckDB may fail for edge cases (malformed files, unusual formats)
- Fallback ensures command always works
- Transparent to user (same output format)

**Implementation**:
```python
try:
    if detected_engine == 'duckdb':
        return _stats_duckdb(fromfile, options)
except Exception as e:
    logging.warning(f'DuckDB stats failed, falling back to iterable: {e}')
    # Fall through to iterable engine
if detected_engine == 'iterable':
    return _stats_iterable(fromfile, options)  # Current implementation
```

**Alternatives Considered**:
- Fail fast on DuckDB error (rejected: poor user experience)
- Retry with different DuckDB options (rejected: adds complexity, iterable is reliable fallback)

### Decision 4: Progress Indication
**Decision**: Two-phase progress indication for DuckDB engine

**Phases**:
1. **Row counting**: Fast `COUNT(*)` query with progress indication
2. **Statistics computation**: Single operation indicator (DuckDB queries are fast)

**Rationale**:
- DuckDB queries execute as single operations (no row-by-row progress)
- User still gets feedback about progress
- Matches pattern where counting is fast, computation is fast

**Alternatives Considered**:
- No progress for DuckDB (rejected: inconsistent with iterable engine)
- Chunked processing for progress (rejected: adds complexity, DuckDB is already fast)

### Decision 5: Exact vs Approximate Unique Counts
**Decision**: Use exact `COUNT(DISTINCT ...)` for unique counts

**Rationale**:
- Current implementation uses exact counts
- DuckDB's `approx_unique` from `SUMMARIZE` may differ slightly
- Accuracy is more important than minor performance difference

**Implementation**:
```sql
SELECT COUNT(DISTINCT column_name) FROM read_csv('file.csv')
```

**Alternatives Considered**:
- Use `approx_unique` from SUMMARIZE (rejected: may cause inconsistencies with iterable engine)

### Decision 6: Nested Structure Handling
**Decision**: Reuse `duckdb_decompose()` function which already handles nesting

**Rationale**:
- Already tested and used in `analyzer.py`
- Handles recursive unnesting correctly
- Produces field paths in correct format (dot-separated)

**Implementation**:
- Use `duckdb_decompose()` with `path='*'` and `recursive=True`
- Results already include nested field paths (e.g., `user.address.city`)

**Alternatives Considered**:
- Custom nested structure handling (rejected: reinventing tested code)

## Architecture

### Component Structure

```
StatProcessor
├── stats() [main entry point]
│   ├── _detect_engine() [engine selection]
│   ├── _stats_duckdb() [DuckDB path]
│   │   ├── _compute_duckdb_basic_stats() [uses duckdb_decompose]
│   │   ├── _compute_duckdb_length_stats() [SQL queries]
│   │   ├── _detect_types_from_sample() [sampling + guess_datatype]
│   │   └── _compute_duckdb_dictionaries() [GROUP BY queries]
│   └── _stats_iterable() [existing implementation, unchanged]
```

### Data Flow

**DuckDB Path:**
1. Detect engine → DuckDB selected
2. Count rows (for progress)
3. Compute basic stats (`duckdb_decompose` with `use_summarize=True`)
4. Compute length stats (custom SQL per field)
5. Sample for type detection (hybrid approach)
6. Build dictionaries (GROUP BY for low-cardinality fields)
7. Combine into profile structure
8. Display Rich table (unchanged)

**Iterable Path (unchanged):**
1. Detect engine → Iterable selected
2. Process rows one-by-one
3. Accumulate statistics in dictionaries
4. Post-process and display

### Integration Points

**External Dependencies:**
- `duckdb` - Already a dependency
- `undatum/common/schema_utils.py` - `duckdb_decompose()` function
- `undatum/constants.py` - `DUCKABLE_FILE_TYPES`, `DUCKABLE_CODECS`
- `iterable.helpers.detect` - `detect_file_type()` function

**No changes needed to:**
- `undatum/common/schema_utils.py` (reuse existing function)
- `undatum/constants.py` (constants already defined)
- Other commands (no coupling)

## Risks / Trade-offs

### Risk 1: Statistics Accuracy Differences
**Risk**: DuckDB computations may produce slightly different results than iterable engine

**Mitigation**:
- Use exact `COUNT(DISTINCT ...)` instead of approximate
- Test with various datasets to verify matching results
- Maintain iterable engine as reference implementation
- Document any known differences (if discovered)

**Impact**: Low - Both engines should produce identical results

### Risk 2: DuckDB Query Failures
**Risk**: DuckDB queries may fail for edge cases (malformed files, unusual formats)

**Mitigation**:
- Comprehensive error handling with fallback to iterable
- Test with various edge cases
- Log warnings when fallback occurs
- User can explicitly select iterable engine if needed

**Impact**: Low - Fallback mechanism ensures reliability

### Risk 3: Memory Usage for Large Dictionaries
**Risk**: Building dictionaries for many low-cardinality fields may use significant memory

**Mitigation**:
- DuckDB's GROUP BY is more memory-efficient than Python dicts
- Dictionary construction only for fields below `dictshare` threshold (existing logic)
- If needed, could add memory warnings or limits

**Impact**: Low - Should be better than current implementation

### Risk 4: Nested Structure Handling Differences
**Risk**: Complex nested JSON may be handled differently between engines

**Mitigation**:
- Reuse proven `duckdb_decompose()` function
- Test with various nesting depths
- Compare output between engines during testing

**Impact**: Low - `duckdb_decompose()` is already tested

### Risk 5: Performance Not Meeting Expectations
**Risk**: DuckDB optimization may not achieve expected 10-100x speedup

**Mitigation**:
- Benchmark before and after implementation
- Profile to identify bottlenecks if needed
- DuckDB is already proven fast in other commands

**Impact**: Low - Even modest improvement (2-5x) would be valuable

## Trade-offs

### Accuracy vs Performance
- **Trade-off**: Type detection sampling vs full type detection
- **Decision**: Sample for types (maintains behavior, still fast)
- **Impact**: Negligible accuracy difference, significant performance gain

### Complexity vs Performance
- **Trade-off**: Adding DuckDB path increases code complexity
- **Decision**: Accept complexity for 10-100x performance gain
- **Impact**: Code is more complex but well-structured and testable

### Compatibility vs Performance
- **Trade-off**: Could remove iterable engine, force DuckDB-only
- **Decision**: Keep both engines, fallback ensures compatibility
- **Impact**: Slightly more code but ensures reliability

## Implementation Phases

### Phase 1: Engine Detection (Low Risk)
- Add `_detect_engine()` function
- Add CLI `--engine` option
- Test engine selection logic

### Phase 2: Basic DuckDB Statistics (Medium Risk)
- Implement `_compute_duckdb_basic_stats()` using `duckdb_decompose()`
- Integrate into `stats()` method
- Test with CSV, JSONL files

### Phase 3: Complete Statistics (Medium Risk)
- Add length statistics queries
- Add type detection sampling
- Add dictionary construction
- Test all statistics match iterable engine

### Phase 4: Progress and Polish (Low Risk)
- Update progress indication
- Error handling improvements
- Documentation

## Testing Strategy

### Unit Tests
- Test each component independently
- Mock DuckDB responses for predictable testing
- Test edge cases (empty files, NULL values, etc.)

### Integration Tests
- Test full workflows with real files
- Compare outputs between engines (must match)
- Test fallback scenarios

### Performance Tests
- Benchmark before/after on various file sizes
- Verify speedup expectations met
- Profile to identify any bottlenecks

## Open Questions

None currently - all technical decisions made, design is complete.
