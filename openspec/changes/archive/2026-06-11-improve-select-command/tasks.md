## 1. Implementation
- [x] 1.1 Add engine option to CLI `select` command and pass to options
- [x] 1.2 Add DuckDB select path with auto-detection and safe fallback
- [x] 1.3 Implement conservative filter-to-SQL translation or fallback logic
- [x] 1.4 Fix batching to flush by size, not by logging cadence
- [x] 1.5 Unify output handling via `open_iterable` for file/stdout
- [x] 1.6 Validate required `fields` option and add clear error messages

## 2. Tests
- [x] 2.1 Add unit tests for engine selection and fallback behavior
- [x] 2.2 Add tests for filter handling (iterable vs DuckDB)
- [x] 2.3 Add tests for batching behavior and stdout vs file output

## 3. Docs
- [x] 3.1 Update CLI docs to document `select --engine` option
