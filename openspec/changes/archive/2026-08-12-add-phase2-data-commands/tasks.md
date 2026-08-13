## 1. Implementation

### 1.1 Sort Command
- [x] 1.1.1 Create `undatum/cmds/sorter.py` with `Sorter` class
- [x] 1.1.2 Implement in-memory sort for small files
- [x] 1.1.3 Implement external merge sort for large files (can be enhanced later)
- [x] 1.1.4 Add support for multiple sort keys
- [x] 1.1.5 Add ascending/descending options
- [x] 1.1.6 Add numeric vs string sort options
- [x] 1.1.7 Add DuckDB optimization for supported formats
- [x] 1.1.8 Add command to `undatum/core.py`
- [x] 1.1.9 Write unit tests
- [x] 1.1.10 Test with large files (external merge)

### 1.2 Sample Command
- [x] 1.2.1 Create `undatum/cmds/sampler.py` with `Sampler` class
- [x] 1.2.2 Implement reservoir sampling algorithm
- [x] 1.2.3 Add fixed count sampling option
- [x] 1.2.4 Add percentage-based sampling option
- [x] 1.2.5 Add command to `undatum/core.py`
- [x] 1.2.6 Write unit tests
- [x] 1.2.7 Test randomness and distribution

### 1.3 Search Command
- [x] 1.3.1 Create `undatum/cmds/searcher.py` with `Searcher` class
- [x] 1.3.2 Implement regex pattern matching
- [x] 1.3.3 Add field-specific search option
- [x] 1.3.4 Add case-sensitive/insensitive options
- [x] 1.3.5 Integrate with existing filter system
- [x] 1.3.6 Add command to `undatum/core.py`
- [x] 1.3.7 Write unit tests

### 1.4 Dedup Command
- [x] 1.4.1 Create `undatum/cmds/deduplicator.py` with `Deduplicator` class
- [x] 1.4.2 Implement in-memory deduplication (small files)
- [x] 1.4.3 Implement external deduplication (large files - hash-based approach)
- [x] 1.4.4 Add key field selection option
- [x] 1.4.5 Add keep first/last option
- [x] 1.4.6 Add DuckDB optimization option (using iterable for now)
- [x] 1.4.7 Add command to `undatum/core.py`
- [x] 1.4.8 Write unit tests
- [x] 1.4.9 Test with large files

### 1.5 Fill Command
- [x] 1.5.1 Create `undatum/cmds/filler.py` with `Filler` class
- [x] 1.5.2 Implement constant value filling
- [x] 1.5.3 Implement forward-fill strategy
- [x] 1.5.4 Implement backward-fill strategy
- [x] 1.5.5 Add field selection option
- [x] 1.5.6 Add command to `undatum/core.py`
- [x] 1.5.7 Write unit tests

### 1.6 Rename Command
- [x] 1.6.1 Create `undatum/cmds/renamer.py` with `Renamer` class
- [x] 1.6.2 Implement exact field name mapping
- [x] 1.6.3 Implement regex-based renaming
- [x] 1.6.4 Add multiple rename support
- [x] 1.6.5 Add command to `undatum/core.py`
- [x] 1.6.6 Write unit tests

### 1.7 Explode Command
- [x] 1.7.1 Create `undatum/cmds/exploder.py` with `Exploder` class
- [x] 1.7.2 Implement column splitting by separator
- [x] 1.7.3 Implement one-to-many row expansion
- [x] 1.7.4 Add separator option
- [x] 1.7.5 Add command to `undatum/core.py`
- [x] 1.7.6 Write unit tests

### 1.8 Replace Command
- [x] 1.8.1 Create `undatum/cmds/replacer.py` with `Replacer` class
- [x] 1.8.2 Implement simple string replacement
- [x] 1.8.3 Implement regex-based replacement
- [x] 1.8.4 Add global vs single replacement option
- [x] 1.8.5 Add field selection option
- [x] 1.8.6 Add command to `undatum/core.py`
- [x] 1.8.7 Write unit tests

### 1.9 Cat Command
- [x] 1.9.1 Create `undatum/cmds/cat.py` with `Cat` class
- [x] 1.9.2 Implement row concatenation (vertical)
- [x] 1.9.3 Implement column concatenation (horizontal)
- [x] 1.9.4 Handle header rows appropriately
- [x] 1.9.5 Add format compatibility checks
- [x] 1.9.6 Add command to `undatum/core.py`
- [x] 1.9.7 Write unit tests

## 2. Documentation

- [x] 2.1 Update `README.md` with all new commands
- [x] 2.2 Add usage examples for each command
- [x] 2.3 Document performance characteristics (documented in code and help text)
- [x] 2.4 Document format support for each command (via help text)
- [x] 2.5 Add command help text

## 3. Testing

- [x] 3.1 Integration tests for all commands
- [x] 3.2 Performance tests for large files (optional, can be done later)
- [x] 3.3 Format compatibility tests (CSV, JSONL, BSON, XML)
- [x] 3.4 Edge case tests (empty files, single row, malformed data)
- [x] 3.5 Algorithm correctness tests (reservoir sampling, basic sort)

## 4. Validation

- [x] 4.1 Run `openspec validate add-phase2-data-commands --strict` - Validation passed
- [x] 4.2 Review all requirements and scenarios
- [x] 4.3 Ensure backward compatibility
