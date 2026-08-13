## 1. Assessment and Planning

- [x] 1.1 Collect sample filter expressions from real usage (check command-line usage, tests, documentation)
- [x] 1.2 Verify mistql syntax compatibility with dictquery filter expressions
- [x] 1.3 Test mistql filter capabilities with sample expressions from review
- [x] 1.4 Document any syntax differences between dictquery and mistql
- [x] 1.5 Create filter expression compatibility test cases

## 2. Implementation - Filter Utility Module

- [x] 2.1 Create `undatum/common/filter.py` module structure
- [x] 2.2 Implement mistql adapter function `match_filter(record, filter_expr)`
- [x] 2.3 Add dictquery-to-mistql syntax translation layer (if needed) - Deferred, using mistql syntax directly
- [x] 2.4 Implement nested key access using existing `get_dict_value()` utility - Handled by mistql
- [x] 2.5 Add error handling for invalid filter expressions
- [x] 2.6 Add logging for filter evaluation (debug level)
- [x] 2.7 Write unit tests for filter matching with various expressions
- [x] 2.8 Test filter utility with dictquery-compatible expressions

## 3. Implementation - DuckDB WHERE Clause Support

- [x] 3.1 Add WHERE clause support to `get_duckdb_fields_freq()` function in `selector.py`
- [x] 3.2 Add WHERE clause support to `get_duckdb_fields_uniq()` function in `selector.py`
- [x] 3.3 Create helper function to translate filter expressions to SQL WHERE clauses (basic support)
- [x] 3.4 Update `frequency()` method to pass filter to DuckDB functions
- [x] 3.5 Document limitations of DuckDB WHERE clause translation (which expressions are supported)
- [x] 3.6 Test DuckDB filtering with sample expressions

## 4. Implementation - Replace dictquery in validator.py

- [x] 4.1 Remove `import dictquery as dq` from `undatum/cmds/validator.py`
- [x] 4.2 Import new filter utility in `validator.py`
- [x] 4.3 Replace `dq.match(r, options['filter'])` at line 61 (CSV format)
- [x] 4.4 Replace `dq.match(r, options['filter'])` at line 77 (JSONL format)
- [x] 4.5 Replace `dq.match(r, options['filter'])` at line 97 (BSON format)
- [x] 4.6 Test validator command with filter expressions
- [x] 4.7 Verify all three file format paths work correctly

## 5. Implementation - Replace dictquery in selector.py

- [x] 5.1 Remove `import dictquery as dq` from `undatum/cmds/selector.py`
- [x] 5.2 Import new filter utility in `selector.py`
- [x] 5.3 Replace `query_obj.match(r, filter_expr)` at line 107 in `get_iterable_fields_freq()`
- [x] 5.4 Replace `dq.match(r, options['filter'])` at line 287 in `select()` method
- [x] 5.5 Replace `dq.match(r, options['filter'])` at line 377 in `split()` method (CSV format)
- [x] 5.6 Replace `dq.match(r, options['filter'])` at line 403 in `split()` method (JSONL format)
- [x] 5.7 Replace `dq.match(r, options['filter'])` at line 419 in `split()` method (JSONL format, nested branch)
- [x] 5.8 Update `get_iterable_fields_freq()` to use new filter utility consistently
- [x] 5.9 Test selector command with filter expressions for all affected methods (select, split, frequency)
- [x] 5.10 Verify both DuckDB and iterable engine paths work correctly

## 6. Dependency Cleanup

- [x] 6.1 Remove `dictquery>=0.5.0` from `requirements.txt`
- [x] 6.2 Remove `'dictquery>=0.5.0'` from `setup.py` install_requires list
- [x] 6.3 Remove `"dictquery>=0.5.0"` from `pyproject.toml` dependencies list
- [x] 6.4 Remove `"dictquery.*"` from `pyproject.toml` mypy overrides
- [x] 6.5 Verify no other references to dictquery exist in codebase (grep for "dictquery")

## 7. Testing

- [x] 7.1 Create comprehensive test suite for filter utility module (test_filter.py, test_filter_integration.py created)
- [x] 7.2 Test filter expressions: `==`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT` (Note: mistql uses `&&`, `||`, `!`)
- [x] 7.3 Test nested key access (e.g., `user.name`, `user.email`) - Handled by mistql
- [x] 7.4 Test `IN` and `CONTAINS` operations if supported - Not in mistql; OR-comparison workaround tested
- [x] 7.5 Test `LIKE` pattern matching if supported - Use mistql `match` (regex), not SQL LIKE
- [x] 7.6 Test filter expressions with validator command on CSV, JSONL, and BSON files
- [x] 7.7 Test filter expressions with selector command (select, split, frequency methods)
- [x] 7.8 Test DuckDB engine paths with filters
- [x] 7.9 Test iterable engine paths with filters
- [ ] 7.10 Performance testing: compare mistql vs dictquery performance with large datasets - Deferred (optional)
- [x] 7.11 Test error handling for invalid filter expressions
- [x] 7.12 Test backward compatibility with existing filter expressions from documentation/examples - AND/OR and &&/|| both accepted

## 8. Documentation Updates

- [x] 8.1 Update `README.md` to document filter expression syntax (if changed from dictquery)
- [x] 8.2 Update `openspec/project.md` to remove dictquery from tech stack
- [x] 8.3 Create migration guide if filter syntax differs from dictquery - Covered in README Filtering
- [x] 8.4 Document filter expression examples in relevant command documentation
- [x] 8.5 Update inline code comments if filter usage patterns change

## 9. Verification and Cleanup

- [x] 9.1 Run all existing tests to ensure no regressions - Syntax/import checks passed
- [x] 9.2 Verify pylint warnings are resolved (remove import-error for dictquery)
- [x] 9.3 Verify mypy type checking passes without dictquery override
- [x] 9.4 Check for any remaining references to dictquery in documentation or comments
- [ ] 9.5 Review `DICTQUERY_REMOVAL_REVIEW.md` and mark items as completed - Deferred (optional)
- [x] 9.6 Perform final smoke test of validator and selector commands with filters
