# Performance Optimization Analysis for Undatum

## Executive Summary

This document identifies performance bottlenecks and provides optimization recommendations for the undatum data processing library. The analysis focuses on data processing operations, memory usage, I/O operations, and algorithmic improvements.

---

## Critical Performance Issues

### 1. **Inefficient List-Based Key Tracking (High Impact)**

**Location**: Multiple files including `converter.py`, `selector.py`, `utils.py`, `statistics.py`, `transformer.py`

**Problem**: Using lists with `if k not in keys` followed by `keys.append(k)` results in O(n²) complexity.

**Examples**:
```python
# utils.py:176-177
if k not in keys:
    keys.append(k)

# converter.py:494-495
if k not in keys:
    keys.append(k)
```

**Impact**: For large datasets with many unique keys, this creates significant slowdown.

**Recommendation**: Use sets for O(1) lookup and insertion:
```python
keys = set()  # Instead of []
if k not in keys:
    keys.add(k)  # Instead of append
```

**Files to fix**:
- `undatum/utils.py` (line 176-177, 494-495)
- `undatum/cmds/converter.py` (lines 482-500)
- `undatum/cmds/selector.py` (lines 189-190)
- `undatum/cmds/statistics.py` (line 105)
- `undatum/cmds/transformer.py` (lines 50-51)

**Estimated Performance Gain**: 10-100x improvement for schema extraction operations

---

### 2. **Unnecessary List Conversions (Medium Impact)**

**Location**: Multiple files

**Problem**: Converting dict.keys(), dict.items(), dict.values() to lists unnecessarily creates copies and consumes memory.

**Examples**:
```python
# statistics.py:65, 83, 91, 113
if k not in list(fielddata.keys()):
list(fieldtypes.keys())

# analyzer.py:339, 349, 380, 390
list(candidates.keys())

# utils.py:85
for key, value in list(indict.items()):
```

**Impact**: Unnecessary memory allocations and CPU cycles, especially in loops.

**Recommendation**: Use dict views directly:
```python
# Instead of: list(fielddata.keys())
# Use: fielddata.keys()

# Instead of: list(indict.items())
# Use: indict.items()
```

**Files to fix**:
- `undatum/cmds/statistics.py` (lines 65, 83, 91, 113, 118, 121, 124)
- `undatum/cmds/analyzer.py` (lines 339, 349, 380, 390)
- `undatum/utils.py` (line 85)

**Estimated Performance Gain**: 5-20% memory reduction, 2-5% CPU improvement

---

### 3. **Inefficient JSON Serialization (Medium Impact)**

**Location**: `converter.py:158`

**Problem**: Using standard `json.dumps()` instead of `orjson.dumps()` even though orjson is imported and commented out.

```python
# converter.py:158
output.write(json.dumps(j, ensure_ascii=False).encode('utf8'))
#        output.write(orjson.dumps(j, ensure_ascii=False).encode('utf8', ))
```

**Impact**: 2-3x slower JSON serialization.

**Recommendation**: Use orjson consistently:
```python
output.write(orjson.dumps(j, option=orjson.OPT_APPEND_NEWLINE))
```

**Files to fix**:
- `undatum/cmds/converter.py` (line 158)

**Estimated Performance Gain**: 2-3x faster CSV to JSONL conversion

---

### 4. **File Operations Without Context Managers (Low-Medium Impact)**

**Location**: Multiple conversion functions in `converter.py`

**Problem**: Files opened with `open()` but not using context managers, relying on manual `.close()`.

**Examples**:
```python
# converter.py:90-109
ins = open(fromname, 'rb')
outf = open(toname, 'wb')
# ... processing ...
ins.close()
outf.close()
```

**Impact**: Risk of resource leaks if exceptions occur, and slightly less efficient.

**Recommendation**: Use context managers:
```python
with open(fromname, 'rb') as ins, open(toname, 'wb') as outf:
    # processing
```

**Files to fix**:
- `undatum/cmds/converter.py` (multiple functions)
- `undatum/utils.py` (detect_encoding, detect_delimiter)

**Estimated Performance Gain**: Better resource management, reduced risk of leaks

---

### 5. **String Join Operations in Hot Loops (Medium Impact)**

**Location**: `utils.py`, `selector.py`, `statistics.py`

**Problem**: Repeatedly calling `".".join()` in tight loops.

**Examples**:
```python
# utils.py:175, 493
k = ".".join(i[:-1])

# statistics.py:60
k = '.'.join(i[:-1])
```

**Impact**: String concatenation creates new string objects, especially expensive in loops.

**Recommendation**: Cache joined strings or use tuple keys where possible.

**Files to fix**:
- `undatum/utils.py`
- `undatum/cmds/statistics.py`
- `undatum/cmds/selector.py`

**Estimated Performance Gain**: 5-15% improvement in key generation

---

### 6. **dict_generator Creating Unnecessary Copies (High Impact)**

**Location**: `utils.py:85`

**Problem**: `list(indict.items())` creates a copy of all items before iteration.

```python
for key, value in list(indict.items()):
```

**Impact**: For large dictionaries, this doubles memory usage during iteration.

**Recommendation**: Iterate directly over items():
```python
for key, value in indict.items():
```

**Files to fix**:
- `undatum/utils.py` (line 85)

**Estimated Performance Gain**: 50% memory reduction for large dicts

---

### 7. **Inefficient Dictionary Key Checking (Low Impact)**

**Location**: Multiple files

**Problem**: Using `.keys()` when direct membership test is sufficient.

**Examples**:
```python
# functions.py:17, converter.py:512
if prefix[0] not in adict.keys():
if k not in row.keys():
```

**Impact**: Small overhead from method call and view creation.

**Recommendation**: Use direct membership:
```python
if prefix[0] not in adict:
if k not in row:
```

**Files to fix**:
- `undatum/common/functions.py` (line 17)
- `undatum/cmds/converter.py` (line 512)
- `undatum/cmds/analyzer.py` (lines 112, 117, 129, 134)

**Estimated Performance Gain**: 1-3% improvement in dictionary operations

---

### 8. **Batch Processing Inefficiencies (Medium Impact)**

**Location**: `selector.py:282-299`

**Problem**: Writing chunks only every 1000 records in select operation, but chunk management could be better.

**Example**:
```python
chunk = []
for r in iterable.iter():
    # ... processing ...
    if n % 1000 == 0:
        writer.write_items(chunk)
        chunk = []
    else:
        chunk.append(r_selected)
```

**Impact**: Chunk accumulation could be more efficient with pre-allocated lists.

**Recommendation**: Pre-allocate chunk list and use slicing:
```python
chunk_size = 1000
chunk = [None] * chunk_size
chunk_idx = 0
for r in iterable.iter():
    # ... processing ...
    chunk[chunk_idx] = r_selected
    chunk_idx += 1
    if chunk_idx >= chunk_size:
        writer.write_items(chunk)
        chunk = [None] * chunk_size
        chunk_idx = 0
```

**Files to fix**:
- `undatum/cmds/selector.py` (lines 282-299)

**Estimated Performance Gain**: 5-10% improvement in batch writing

---

### 9. **Repeated Encoding Detection (Low Impact)**

**Location**: `common/iterable.py`

**Problem**: Encoding detection called multiple times for same file.

**Impact**: Unnecessary file I/O and CPU usage.

**Recommendation**: Cache encoding detection results.

**Files to fix**:
- `undatum/common/iterable.py` (lines 79-91)

**Estimated Performance Gain**: Eliminates redundant file reads

---

### 10. **Memory Inefficient JSON Loading (Medium Impact)**

**Location**: `converter.py:345-346`

**Problem**: Loading entire JSON file into memory at once.

```python
source_data = json.load(source)
```

**Impact**: For large JSON files, this can cause memory issues.

**Recommendation**: Use streaming JSON parser or limit memory usage:
```python
# For arrays, consider streaming parser
# Or use orjson.loads() which is more memory efficient
```

**Files to fix**:
- `undatum/cmds/converter.py` (lines 345-346, 371)

**Estimated Performance Gain**: Reduced memory footprint for large files

---

## Algorithmic Improvements

### 11. **Optimize get_dict_keys Function (High Impact)**

**Location**: `utils.py:166-178`

**Current Implementation**:
```python
def get_dict_keys(iterable, limit=1000):
    n = 0
    keys = []
    for item in iterable:
        if limit and n > limit:
            break
        n += 1
        dk = dict_generator(item)
        for i in dk:
            k = ".".join(i[:-1])
            if k not in keys:
                keys.append(k)
    return keys
```

**Problems**:
1. Uses list for keys (O(n) lookup)
2. String join in hot loop
3. Returns list instead of set

**Recommendation**:
```python
def get_dict_keys(iterable, limit=1000):
    n = 0
    keys = set()  # Use set for O(1) operations
    for item in iterable:
        if limit and n > limit:
            break
        n += 1
        dk = dict_generator(item)
        for i in dk:
            k = ".".join(i[:-1])
            keys.add(k)  # O(1) instead of O(n)
    return sorted(keys)  # Return sorted list if order matters
```

**Estimated Performance Gain**: 10-100x for large datasets

---

### 12. **Optimize strip_dict_fields (Medium Impact)**

**Location**: `utils.py:61-75`

**Problem**: Creates new list for each record, iterates multiple times.

**Recommendation**: Use set operations for field filtering:
```python
def strip_dict_fields(record, fields, startkey=0):
    if not fields:
        return record
    
    # Pre-compute lookup set for O(1) checks
    localf_set = {field[startkey] for field in fields if len(field) > startkey}
    
    # Single pass through keys
    keys_to_remove = [k for k in record.keys() if k not in localf_set]
    for k in keys_to_remove:
        del record[k]
    
    # Recursively process nested dicts
    for k in record.keys():
        if isinstance(record[k], dict):
            record[k] = strip_dict_fields(record[k], fields, startkey + 1)
    
    return record
```

**Estimated Performance Gain**: 20-30% improvement

---

## Memory Optimizations

### 13. **Use Generators More Effectively**

**Location**: Multiple files

**Problem**: Some operations create intermediate lists unnecessarily.

**Recommendation**: Use generator expressions where possible:
```python
# Instead of creating list:
all_keys = [k for k in keys]

# Use generator:
all_keys = (k for k in keys)
```

---

### 14. **Batch Size Optimization**

**Location**: `converter.py`, `ingester.py`

**Problem**: Fixed batch sizes may not be optimal for all use cases.

**Recommendation**: Consider adaptive batch sizing based on:
- Available memory
- Record size
- Processing speed

---

## I/O Optimizations

### 15. **Buffered Writing**

**Location**: All file writing operations

**Problem**: Some write operations may not be optimally buffered.

**Recommendation**: Ensure appropriate buffer sizes:
```python
with open(filename, 'wb', buffering=8192*4) as f:  # 32KB buffer
    # write operations
```

---

## Code Quality Improvements

### 16. **Remove Dead Code**

**Location**: Multiple files

**Examples**:
- Commented out orjson usage
- Unused variables
- Duplicate implementations

---

## Priority Ranking

1. **Critical (Do First)**:
   - Issue #1: List-based key tracking → Use sets
   - Issue #6: dict_generator list() conversion
   - Issue #11: Optimize get_dict_keys

2. **High Priority**:
   - Issue #2: Unnecessary list() conversions
   - Issue #3: JSON serialization with orjson
   - Issue #12: Optimize strip_dict_fields

3. **Medium Priority**:
   - Issue #5: String join operations
   - Issue #8: Batch processing
   - Issue #10: Memory inefficient JSON loading

4. **Low Priority (Technical Debt)**:
   - Issue #4: Context managers
   - Issue #7: Dictionary key checking
   - Issue #9: Encoding detection caching

---

## Measurement Recommendations

1. **Add profiling**: Use `cProfile` or `py-spy` to identify actual bottlenecks
2. **Benchmark specific operations**: Create benchmarks for:
   - Schema extraction
   - File conversion
   - Statistics generation
3. **Memory profiling**: Use `memory_profiler` to track memory usage
4. **Add timing logs**: Instrument key functions with timing information

---

## Implementation Strategy

1. **Phase 1** (Quick Wins - 1-2 days):
   - Fix Issue #1 (sets instead of lists)
   - Fix Issue #6 (remove list() in dict_generator)
   - Fix Issue #3 (use orjson)

2. **Phase 2** (Medium Effort - 1 week):
   - Fix Issue #2 (remove unnecessary list() conversions)
   - Fix Issue #11 (optimize get_dict_keys)
   - Fix Issue #12 (optimize strip_dict_fields)

3. **Phase 3** (Code Quality - Ongoing):
   - Fix Issue #4 (context managers)
   - Fix Issue #7 (dictionary operations)
   - Code cleanup and documentation

---

## Expected Overall Performance Gains

- **Schema Extraction**: 10-50x improvement for large datasets
- **File Conversion**: 2-3x improvement (especially JSON operations)
- **Memory Usage**: 20-50% reduction
- **Statistics Generation**: 5-10x improvement for large datasets

---

## Testing Recommendations

1. **Performance regression tests**: Add benchmarks that run on CI
2. **Large file tests**: Test with files > 1GB
3. **Memory leak tests**: Ensure memory is properly freed
4. **Concurrency tests**: If adding parallel processing later

---

## Additional Recommendations

1. **Consider using multiprocessing** for independent record processing
2. **Add progress bars** (already using tqdm in some places)
3. **Consider streaming parsers** for very large files
4. **Add caching** for expensive operations (schema detection, encoding detection)
5. **Consider using Cython** for hot loops if further optimization needed

---

## Notes

- All optimizations should maintain backward compatibility
- Performance improvements should be verified with real-world datasets
- Consider adding performance metrics/monitoring
- Document any trade-offs (e.g., memory vs. speed)

