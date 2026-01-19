# Change: Improve Database Ingestion - Phase 1 (Bug Fixes and Enhancements)

## Why

The current `ingest` command has several critical bugs and limitations that prevent reliable database ingestion. Based on comprehensive research (see `dev/docs/DATABASE_INGESTION_RESEARCH_REPORT.md`), Phase 1 focuses on fixing bugs and improving error handling, connection management, and usability without adding new database support.

These improvements address:
- Critical bugs preventing proper functionality (typo in drop option, batch size mismatch)
- Missing error handling (no retry logic, poor error reporting)
- Connection management issues (no pooling, timeout not used)
- Usability improvements (better progress tracking, error messages)

## What Changes

- **FIXED**: Typo bug in line 85 (`'dro[]'` → `'drop'`) that prevents drop option from working
- **FIXED**: Batch size mismatch between code default (50,000) and CLI default (1,000)
- **FIXED**: Timeout option not being passed to MongoDB client
- **ADDED**: Retry logic with exponential backoff for transient failures
- **ADDED**: Connection pooling for MongoDB and Elasticsearch
- **ADDED**: Better error handling with detailed error logging
- **ADDED**: Partial batch failure handling (continue after single-record failures)
- **ADDED**: Improved progress reporting with better statistics
- **MODIFIED**: Enhanced error messages with context and actionable information

All changes maintain backward compatibility and improve reliability without changing the CLI interface.

## Impact

- **Affected specs**: `database-ingestion` capability (new)
- **Affected code**:
  - `undatum/cmds/ingester.py` - Fix bugs, add retry logic, connection pooling
  - `undatum/core.py` - Update CLI options if needed
  - `README.md` - Update documentation with improved features
- **Dependencies**: No new dependencies required
- **Backward compatibility**: No breaking changes, only bug fixes and enhancements
