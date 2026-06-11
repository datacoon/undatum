## 1. Error Handling Infrastructure

- [x] 1.1 Create `undatum/common/errors.py` with custom exception classes
  - [x] Define `UndatumError` base exception class
  - [x] Define `FileNotFoundError` with path suggestions
  - [x] Define `PermissionError` with actionable messages
  - [x] Define `ValidationError` for input validation
  - [x] Define `FormatError` for unsupported formats
  - [x] Define `ConfigurationError` for config issues
  - [x] Add error message formatting utilities

- [x] 1.2 Create error context helpers
  - [x] Add file path validation with suggestions for typos
  - [x] Add permission checking utilities
  - [x] Add format detection error helpers
  - [x] Add dependency checking utilities

- [x] 1.3 Update `undatum/core.py` error handling
  - [x] Add global exception handler for Typer commands
  - [x] Format exceptions before displaying to users
  - [x] Show full traceback only in verbose mode
  - [x] Ensure consistent error exit codes

## 2. Command Error Handling Refactoring

- [x] 2.1 Refactor file I/O commands
  - [x] `converter.py` - File not found, permission, format errors
  - [x] `ingester.py` - Database connection, file access errors
  - [x] `extractor.py` - Document access, OCR errors
  - [x] `schemer.py` - Schema extraction errors (already has some handling)
  - [x] `validator.py` - Validation rule errors

- [x] 2.2 Refactor data processing commands
  - [x] `statistics.py` - Engine selection, query errors
  - [x] `selector.py` - Query syntax errors
  - [x] `sorter.py` - Sort field errors
  - [x] `joiner.py` - Join key errors
  - [x] `deduplicator.py` - Deduplication errors

- [x] 2.3 Refactor transformation commands
  - [x] `transformer.py` - Transformation rule errors
  - [x] `replacer.py` - Replacement pattern errors
  - [x] `renamer.py` - Field name errors
  - [x] `masker.py` - Masking configuration errors

- [x] 2.4 Refactor workflow commands
  - [x] `pipeline.py` - Pipeline execution errors
  - [x] `pipeline_templates.py` - Template loading errors
  - [x] `examples.py` - Recipe execution errors

- [x] 2.5 Refactor database commands
  - [x] `db_query.py` - SQL query errors
  - [x] `db_load.py` - Database connection errors

- [x] 2.6 Refactor API commands
  - [x] `api.py` - API server errors (already has HTTPException handling - appropriate for web API)

- [x] 2.7 Refactor utility commands
  - [x] `counter.py` - Counting errors
  - [x] `sampler.py` - Sampling errors
  - [x] `head.py` / `tail.py` - File access errors
  - [x] `cat.py` - File concatenation errors

## 3. Error Message Improvements

- [x] 3.1 File-related errors
  - [x] "File not found" with path suggestions for typos
  - [x] "Permission denied" with actionable guidance
  - [x] "File format not supported" with supported formats list
  - [ ] "File is corrupted" with recovery suggestions (future enhancement)

- [x] 3.2 Input validation errors
  - [x] Field name errors with suggestions
  - [ ] Query syntax errors with examples (future enhancement)
  - [x] Invalid option combinations
  - [x] Missing required parameters

- [x] 3.3 Configuration errors
  - [x] Missing dependencies with installation instructions
  - [ ] Invalid configuration files (handled by existing parsers)
  - [ ] Environment variable errors (future enhancement)
  - [ ] API key errors (future enhancement)

- [x] 3.4 System errors
  - [x] Database connection errors
  - [ ] Network errors (future enhancement)
  - [ ] Memory errors with suggestions (future enhancement)
  - [ ] Timeout errors (future enhancement)

## 4. Testing and Documentation

- [x] 4.1 Write tests for error handling
  - [x] Test file not found scenarios
  - [x] Test permission error scenarios
  - [x] Test validation error scenarios
  - [x] Test error message formatting
  - [x] Test verbose mode behavior

- [x] 4.2 Update documentation
  - [x] Document error handling patterns (docs/ERROR_HANDLING_PATTERNS.md)
  - [x] Add troubleshooting guide (docs/ERROR_HANDLING.md)
  - [x] Document common error messages (docs/ERROR_HANDLING.md)
  - [x] Update command examples with error scenarios (README.md)

## 5. Validation

- [x] 5.1 Test all commands with error scenarios
  - [x] File not found (tested in test_error_handling.py)
  - [x] Invalid file format (tested via FormatError)
  - [x] Permission denied (tested in test_error_handling.py)
  - [x] Invalid input parameters (tested in command tests)
  - [x] Missing dependencies (tested via DependencyError)

- [x] 5.2 Verify error messages are user-friendly
  - [x] No internal implementation details
  - [x] Clear, actionable guidance
  - [x] Consistent formatting
  - [x] Appropriate error codes
