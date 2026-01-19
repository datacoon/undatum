# Stats Command DuckDB Optimization Report

## Executive Summary

This report analyzes the current `statistics.py` implementation and proposes optimizations using DuckDB for file formats that DuckDB supports natively (CSV, JSONL, JSON, Parquet). The current implementation processes files row-by-row using the iterable engine, which is slow for large files and doesn't leverage DuckDB's columnar processing capabilities.

**Key Findings:**
- Current implementation uses iterable engine for all file formats
- DuckDB could accelerate statistics generation 10-100x for supported formats
- Progress indication is partially implemented but may not work optimally
- DuckDB's `SUMMARIZE` and SQL aggregations can replace most manual calculations

**Recommended Approach:**
- Add DuckDB engine path for supported formats (similar to `counter`, `selector`, `analyzer`)
- Use DuckDB's `SUMMARIZE` and custom SQL aggregations for statistics
- Maintain iterable engine as fallback for unsupported formats
- Improve tqdm progress indication for both engines

---

## Current Implementation Analysis

### Overview
The `StatProcessor.stats()` method currently:
1. Opens iterable using `open_iterable()` (line 41)
2. Iterates through all records row-by-row (lines 75-154)
3. For each record:
   - Flattens nested structure using `dict_generator()`
   - Manually tracks unique values in dictionaries
   - Calculates min/max/avg string lengths
   - Performs type detection using `guess_datatype()` for every value
4. Post-processes statistics (lines 158-212)
5. Displays results in Rich table format

### Statistics Computed
For each field, the command calculates:
- **Unique values count** (`n_uniq`): Number of distinct values
- **Uniqueness share** (`share_uniq`): Percentage of unique values (0-100%)
- **Min/Max/Avg length** (`minlen`, `maxlen`, `avglen`): String length statistics
- **Field type** (`ftype`): Detected data type (str, int, float, date, etc.)
- **Type distribution**: Count of each type if mixed types present
- **Dictionary construction**: For fields with uniqueness < `dictshare` threshold, builds dictionary of unique values

### Performance Characteristics

**Current Bottlenecks:**
1. **Row-by-row processing**: Every record must be deserialized and processed individually
2. **Manual aggregation**: Unique value tracking, length calculations done in Python loops
3. **Type detection overhead**: `guess_datatype()` called for every value, including date parsing
4. **Nested structure flattening**: `dict_generator()` processes nested JSON structures recursively
5. **Memory usage**: Stores all unique values in dictionaries for low-cardinality fields

**Estimated Performance:**
- Small files (< 10K rows): Acceptable (seconds)
- Medium files (10K-1M rows): Slow (minutes)
- Large files (> 1M rows): Very slow (tens of minutes to hours)

---

## DuckDB Integration Opportunities

### Supported Formats
DuckDB natively supports the following formats for statistics computation:
- **CSV/TSV**: `read_csv()` function
- **JSONL**: `read_json()` function  
- **JSON**: `read_json()` function
- **Parquet**: Direct table reading

**Compression Support:**
- `gzip` (`.gz`)
- `zstd` (`.zst`)
- `raw` (uncompressed)

**Constants Reference:**
```python
DUCKABLE_FILE_TYPES = ['csv', 'jsonl', 'json', 'parquet']
DUCKABLE_CODECS = ['zst', 'gzip', 'raw']
```

### DuckDB Capabilities for Statistics

#### 1. `SUMMARIZE` Statement
DuckDB's `SUMMARIZE` provides comprehensive statistics:
```sql
SUMMARIZE SELECT * FROM read_csv('file.csv');
```

**Output columns per field:**
- `column_name`: Field name
- `column_type`: Data type
- `min`: Minimum value
- `max`: Maximum value
- `avg`: Average value (for numeric)
- `std`: Standard deviation
- `q25`, `q50`, `q75`: Quantiles
- `approx_unique`: Approximate unique count
- `count`: Total count
- `null_percentage`: NULL percentage

**Advantages:**
- Single query provides most statistics
- Handles nested structures with `unnest()` (like `duckdb_decompose`)
- Highly optimized columnar processing
- Can process millions of rows in seconds

#### 2. Custom SQL Aggregations
For additional statistics, can use standard SQL:

```sql
-- Exact unique count (slower but accurate)
SELECT COUNT(DISTINCT column_name) FROM read_csv('file.csv');

-- Min/Max/Avg length
SELECT 
    MIN(LENGTH(CAST(column_name AS VARCHAR))) as minlen,
    MAX(LENGTH(CAST(column_name AS VARCHAR))) as maxlen,
    AVG(LENGTH(CAST(column_name AS VARCHAR))) as avglen
FROM read_csv('file.csv');

-- Value frequencies (for dictionary construction)
SELECT column_name, COUNT(*) as freq 
FROM read_csv('file.csv')
GROUP BY column_name
ORDER BY freq DESC;
```

#### 3. Nested Structure Handling
DuckDB can handle nested JSON structures similar to `duckdb_decompose`:

```sql
-- Unnest nested structures recursively
SELECT unnest(*, recursive:=true) FROM read_json('file.jsonl');
```

**Reference Implementation:**
- `undatum/common/schema_utils.py` - `duckdb_decompose()` function already implements this pattern
- `undatum/cmds/analyzer.py` - Uses `duckdb_decompose()` for schema analysis with `use_summarize=True`

### Existing DuckDB Integration Patterns

Several commands already use DuckDB for supported formats:

#### 1. Counter Command (`counter.py`)
```python
def _detect_engine(fromfile, engine, filetype):
    if engine == 'auto':
        if filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS:
            return 'duckdb'
    return engine

if detected_engine == 'duckdb':
    count = duckdb.sql(f"SELECT COUNT(*) FROM '{fromfile}'").fetchone()[0]
```

**Pattern to replicate:**
- Engine auto-detection based on file format
- Fallback to iterable on error or unsupported format
- Simple, direct DuckDB queries

#### 2. Analyzer Command (`analyzer.py`)
```python
if duckable_cond:
    # Count records
    query_str = f"select count(*) from read_json('{filename}'{text_ignore})"
    num_records = duckdb.sql(query_str).fetchall()[0][0]
    
    # Get structure with statistics
    columns_raw = duckdb_decompose(filename, filetype=filetype,
                                  path='*', limit=objects_limit,
                                  use_summarize=True)
```

**Key insights:**
- Uses `duckdb_decompose()` with `use_summarize=True` for statistics
- Returns: `[field_path, base_type, is_array, unique_count, total_count, uniqueness_percentage]`
- Handles nested structures automatically

#### 3. Selector Command (`selector.py`)
```python
def get_duckdb_fields_freq(filename, fields, dolog=False):
    fieldstext = ','.join(fields)
    query = (f"select {fieldstext}, count(*) as c from '{filename}' "
             f"group by {fieldstext} order by c desc")
    uniqval = duckdb.sql(query).fetchall()
    return uniqval
```

**Pattern for dictionary construction:**
- Uses `GROUP BY` with `COUNT(*)` for frequency analysis
- Orders by frequency (descending)
- Can filter by `dictshare` threshold post-processing

---

## Proposed Implementation Strategy

### Architecture Overview

```
Stats Command Flow:
┌─────────────────┐
│ Input File      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Engine Detection│
│ (auto/duckdb/   │
│  iterable)      │
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌────────┐ ┌──────────┐
│DuckDB  │ │ Iterable │
│Engine  │ │ Engine   │
└───┬────┘ └────┬─────┘
    │           │
    │           │
    └─────┬─────┘
          │
          ▼
┌─────────────────┐
│ Statistics      │
│ Aggregation     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Rich Table      │
│ Output          │
└─────────────────┘
```

### Phase 1: Add DuckDB Engine Detection

**Implementation:**
```python
def _detect_engine(fromfile, engine, filetype, compression):
    """Detect appropriate engine for statistics computation."""
    if engine == 'auto':
        if filetype in DUCKABLE_FILE_TYPES and compression in DUCKABLE_CODECS:
            return 'duckdb'
        return 'iterable'
    return engine
```

**Location:** `undatum/cmds/statistics.py`
**Pattern:** Match implementation in `counter.py`, `selector.py`

### Phase 2: DuckDB Statistics Implementation

#### 2.1 Use `duckdb_decompose()` with `use_summarize=True`

**For basic statistics (unique count, total count, uniqueness %):**
```python
from ..common.schema_utils import duckdb_decompose

columns_raw = duckdb_decompose(
    filename=fromfile,
    filetype=filetype,
    path='*',
    limit=None,  # Process all rows
    recursive=True,
    ignore_errors=True,
    use_summarize=True
)

# Returns: [field_path, base_type, is_array, unique_count, total_count, uniqueness_percentage]
```

**Advantages:**
- Handles nested structures automatically
- Provides unique count and uniqueness percentage
- Already tested and used in `analyzer.py`

**Limitations:**
- Doesn't provide min/max/avg length
- Doesn't provide type distribution
- Approximate unique count (may need exact for some use cases)

#### 2.2 Custom SQL for Additional Statistics

**For length statistics (minlen, maxlen, avglen):**
```python
# For each field, calculate length statistics
for field_path in field_paths:
    query = f"""
    SELECT 
        MIN(LENGTH(CAST("{field_path}" AS VARCHAR))) as minlen,
        MAX(LENGTH(CAST("{field_path}" AS VARCHAR))) as maxlen,
        AVG(LENGTH(CAST("{field_path}" AS VARCHAR))) as avglen,
        COUNT(*) as total
    FROM read_json('{fromfile}')
    WHERE "{field_path}" IS NOT NULL
    """
    result = duckdb.sql(query).fetchone()
```

**For type detection:**
- Option 1: Use DuckDB's column types from `SUMMARIZE` (fast, but less sophisticated)
- Option 2: Sample values and use `guess_datatype()` (slower, but matches current behavior)
- Option 3: Hybrid: Use DuckDB types for most, sample for ambiguous cases

**For dictionary construction:**
```python
# Get value frequencies for low-cardinality fields
if uniqueness_percentage < dictshare:
    query = f"""
    SELECT "{field_path}", COUNT(*) as freq 
    FROM read_json('{fromfile}')
    WHERE "{field_path}" IS NOT NULL
    GROUP BY "{field_path}"
    ORDER BY freq DESC
    """
    frequencies = duckdb.sql(query).fetchall()
    # Build dictionary from frequencies
```

#### 2.3 Progress Indication with DuckDB

**Challenge:** DuckDB queries execute as a single operation, so no row-by-row progress.

**Solutions:**
1. **Query progress estimation:**
   - First, count total rows (fast): `SELECT COUNT(*) FROM ...`
   - Then run statistics queries
   - Show progress based on completed queries, not rows

2. **Chunked processing (if needed):**
   - Process file in chunks (e.g., 100K rows at a time)
   - Accumulate statistics incrementally
   - Show progress per chunk

3. **Two-phase with progress:**
   ```python
   # Phase 1: Count rows (with progress estimate)
   total_rows = duckdb.sql(f"SELECT COUNT(*) FROM '{fromfile}'").fetchone()[0]
   
   # Phase 2: Compute statistics (show as "Analyzing...")
   with tqdm(total=total_rows, desc="Analyzing statistics") as pbar:
       # Run DuckDB queries
       stats = compute_duckdb_stats(fromfile)
       pbar.update(total_rows)  # Mark complete
   ```

**Recommendation:** Use approach #3 for better UX, even though DuckDB queries are fast.

### Phase 3: Hybrid Approach for Complete Statistics

**Strategy:**
1. Use DuckDB for basic statistics (count, unique, min/max/avg)
2. Use DuckDB for dictionary construction (GROUP BY with COUNT)
3. Use sampling for type detection:
   - Sample first N records (e.g., 10,000)
   - Run `guess_datatype()` on sampled values
   - This maintains current behavior while improving performance

**Implementation outline:**
```python
def stats_duckdb(fromfile, options):
    """Compute statistics using DuckDB engine."""
    filetype = detect_file_type(fromfile)
    
    # 1. Get basic statistics from duckdb_decompose
    columns_raw = duckdb_decompose(
        filename=fromfile,
        filetype=filetype,
        use_summarize=True
    )
    
    # 2. Get length statistics for each field
    length_stats = {}
    for field_info in columns_raw:
        field_path = field_info[0]
        length_stats[field_path] = get_length_stats_duckdb(fromfile, field_path, filetype)
    
    # 3. Sample values for type detection (maintains current behavior)
    sample_query = f"SELECT * FROM '{fromfile}' LIMIT 10000"
    sample = duckdb.sql(sample_query).fetchall()
    type_distributions = detect_types_from_sample(sample, field_paths)
    
    # 4. Build dictionaries for low-cardinality fields
    dictshare = options.get('dictshare', DEFAULT_DICT_SHARE)
    dictionaries = {}
    for field_info in columns_raw:
        if field_info[5] < dictshare:  # uniqueness_percentage
            dictionaries[field_info[0]] = get_frequencies_duckdb(fromfile, field_info[0], filetype)
    
    # 5. Combine results into profile format
    return build_profile(columns_raw, length_stats, type_distributions, dictionaries)
```

### Phase 4: Error Handling and Fallback

**Pattern from other commands:**
```python
try:
    if detected_engine == 'duckdb':
        stats_duckdb(fromfile, options)
except Exception as e:
    logging.warning(f'DuckDB stats failed, falling back to iterable: {e}')
    detected_engine = 'iterable'

if detected_engine == 'iterable':
    stats_iterable(fromfile, options)  # Current implementation
```

---

## Progress Indication Improvements

### Current State
The code already includes tqdm support (lines 67-84), but there are issues:

1. **Context manager usage may be incorrect:**
   ```python
   if show_progress:
       iterable_wrapped = tqdm(iterable, desc="Analyzing statistics", unit="rows")
   else:
       iterable_wrapped = iterable
   
   if show_progress:
       with iterable_wrapped as pbar:  # This might not work correctly
           for item in pbar:
               ...
   else:
       for item in iterable_wrapped:
           ...
   ```

2. **No total count:** Progress bar shows rate but no ETA (no `total` parameter)

3. **Throughput updates only every 1000 rows:** Could update more frequently

### Improvements Needed

#### For Iterable Engine:
```python
# Option 1: Pre-count (adds overhead but better UX)
total_rows = count_rows(fromfile)  # Fast for small files, could skip for large
with tqdm(total=total_rows, desc="Analyzing statistics", unit="rows") as pbar:
    for item in iterable:
        # ... process item ...
        pbar.update(1)
        if count % 100 == 0:  # Update throughput more frequently
            elapsed = time.time() - start_time
            if elapsed > 0:
                pbar.set_postfix({"throughput": f"{count/elapsed:.0f} rows/s"})
```

#### For DuckDB Engine:
```python
# Phase 1: Count (fast, shows immediate progress)
with tqdm(desc="Counting rows", unit="rows") as pbar:
    total_rows = duckdb.sql(f"SELECT COUNT(*) FROM '{fromfile}'").fetchone()[0]
    pbar.update(total_rows)
    pbar.set_description("Analyzing statistics")

# Phase 2: Compute statistics (show as single operation)
with tqdm(total=1, desc="Computing statistics", unit="queries") as pbar:
    stats = compute_duckdb_stats(fromfile)
    pbar.update(1)
```

---

## Performance Estimates

### Expected Improvements

| File Size | Current (Iterable) | With DuckDB | Speedup |
|-----------|-------------------|-------------|---------|
| 10K rows  | ~1-2 seconds      | ~0.1 seconds | 10x    |
| 100K rows | ~10-20 seconds    | ~0.5 seconds | 20-40x |
| 1M rows   | ~2-5 minutes      | ~2-5 seconds | 24-60x |
| 10M rows  | ~20-60 minutes    | ~10-30 seconds | 40-120x |

**Note:** Actual performance depends on:
- File format (Parquet fastest, JSONL slower due to parsing)
- Number of fields (more fields = more queries)
- Nested structure complexity
- System resources (CPU, I/O, memory)

### Memory Usage

**Current (Iterable):**
- Loads one row at a time: Low memory footprint
- Stores unique values for all low-cardinality fields: Can be high for many fields

**With DuckDB:**
- DuckDB processes in columnar chunks: Moderate memory usage
- Dictionary construction uses SQL GROUP BY: More efficient than Python dicts
- Overall: Similar or better memory efficiency

---

## Implementation Plan

### Phase 1: Engine Detection and Structure (Low Risk)
**Tasks:**
1. Add `_detect_engine()` function (reuse pattern from `counter.py`)
2. Add engine detection to `stats()` method
3. Add CLI option for `--engine` parameter
4. Test with existing iterable engine (no regression)

**Estimated effort:** 2-4 hours

### Phase 2: Basic DuckDB Statistics (Medium Risk)
**Tasks:**
1. Implement `stats_duckdb()` method
2. Use `duckdb_decompose()` with `use_summarize=True` for basic stats
3. Add error handling and fallback to iterable
4. Test with CSV, JSONL files

**Estimated effort:** 4-8 hours

### Phase 3: Complete Statistics (Medium Risk)
**Tasks:**
1. Add length statistics queries (minlen, maxlen, avglen)
2. Implement dictionary construction using GROUP BY
3. Add type detection sampling (hybrid approach)
4. Test with nested JSON structures

**Estimated effort:** 8-12 hours

### Phase 4: Progress Indication (Low Risk)
**Tasks:**
1. Fix tqdm usage for iterable engine
2. Add progress indication for DuckDB (two-phase approach)
3. Add throughput display
4. Test in various scenarios

**Estimated effort:** 2-4 hours

### Phase 5: Testing and Refinement (Low Risk)
**Tasks:**
1. Performance benchmarks (compare iterable vs DuckDB)
2. Test edge cases (empty files, single column, very wide files)
3. Test with all supported formats and compressions
4. Documentation updates

**Estimated effort:** 4-6 hours

**Total estimated effort:** 20-34 hours

---

## Risks and Mitigations

### Risk 1: DuckDB Query Failures
**Risk:** DuckDB queries may fail for edge cases (malformed files, unusual formats)
**Mitigation:** 
- Comprehensive error handling with fallback to iterable engine
- Test with various file formats and edge cases
- Log warnings when fallback occurs

### Risk 2: Statistics Accuracy Differences
**Risk:** DuckDB's `approx_unique` may differ from exact unique counts
**Mitigation:**
- Use exact `COUNT(DISTINCT ...)` for unique counts when accuracy needed
- Document any differences in behavior
- Add option to prefer accuracy over speed

### Risk 3: Nested Structure Handling
**Risk:** Complex nested JSON may not be handled identically
**Mitigation:**
- Reuse proven `duckdb_decompose()` function
- Test with various nesting depths
- Fallback to iterable for problematic structures

### Risk 4: Memory Usage for Large Dictionaries
**Risk:** Building dictionaries for many low-cardinality fields may use significant memory
**Mitigation:**
- Limit dictionary construction based on `dictshare` threshold (already implemented)
- Consider streaming/chunked dictionary construction for very large datasets
- Add memory warnings if needed

---

## Recommendations

### Immediate Actions (High Priority)
1. **Add DuckDB engine detection** - Enables optimization path without breaking existing functionality
2. **Fix tqdm progress indication** - Improves user experience immediately
3. **Implement basic DuckDB statistics** - Use `duckdb_decompose()` with `use_summarize=True` for quick wins

### Medium-Term Actions
1. **Add length statistics queries** - Complete the statistics set
2. **Optimize dictionary construction** - Use DuckDB GROUP BY instead of Python dicts
3. **Add type detection sampling** - Balance accuracy and performance

### Long-Term Considerations
1. **Parallel processing** - DuckDB supports parallel query execution (automatic)
2. **Caching** - Cache schema/structure for repeated analyses
3. **Incremental statistics** - Support updating statistics for appended data

---

## Conclusion

The stats command can be significantly improved by leveraging DuckDB for supported file formats. The proposed approach:

✅ **Maintains backward compatibility** - Iterable engine remains as fallback  
✅ **Follows existing patterns** - Reuses code and patterns from other commands  
✅ **Provides substantial performance gains** - 10-100x speedup for large files  
✅ **Improves user experience** - Better progress indication  
✅ **Low risk** - Gradual implementation with fallback mechanisms  

The implementation should follow the phased approach, starting with engine detection and basic DuckDB support, then expanding to complete statistics. This allows for incremental testing and refinement while maintaining a working system throughout.

---

## References

- `undatum/cmds/statistics.py` - Current implementation
- `undatum/cmds/counter.py` - DuckDB engine detection pattern
- `undatum/cmds/analyzer.py` - DuckDB statistics usage with `duckdb_decompose()`
- `undatum/common/schema_utils.py` - `duckdb_decompose()` function
- `undatum/constants.py` - `DUCKABLE_FILE_TYPES`, `DUCKABLE_CODECS`
- DuckDB Documentation: https://duckdb.org/docs/sql/statements/summarize.html
