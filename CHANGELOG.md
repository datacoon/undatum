# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.0] - 2026-06-11

### Added
- **`mask` command** - Anonymize sensitive fields with redact, deterministic hash, and randomize methods
- **`pipeline` commands** - Run and validate multi-step YAML/JSON workflows (`pipeline run`, `pipeline validate`)
- **`pipeline templates` commands** - List built-in pipeline templates and initialize pipelines from them (`pipeline templates list`, `pipeline templates init`)
- **`examples` commands** - Browse and run a built-in recipe library (`examples list`, `examples show`, `examples run`); recipes now ship inside the package
- **`plot` command** - Generate histogram, bar, scatter, and line charts with matplotlib
- **`db query` / `db load` commands** - Execute SQL against PostgreSQL/MySQL/SQLite and load files into database tables
- **Data API** - Serve files as a read-only HTTP API (`api discover`, `api serve`, `api run`) via the `api` extra
- **`package create` command** - Generate Frictionless Data Package descriptors
- **`extract` command** - Extract tables/text from PDF/DOC/DOCX/XLS/XLSX via the `extract` extra
- **`profile` command** - Alias for `stats`
- **Python SDK** - `Dataset` fluent API (`from undatum import Dataset`) with read/write, transforms, and analysis methods returning real values (`stats()`, `count()`, `head()`, `tail()`)
- **Plugin system** - Entry-point based plugins (`undatum.plugins` group) with `plugins list` / `plugins info` commands
- **Rich validation rules** - YAML/JSON rule files with severity levels for `validate`
- **Error handling framework** - `UndatumError` hierarchy with actionable messages, typo suggestions, and consistent exit codes
- **S3 support** - Read/write `s3://` URIs in major commands via the `s3` extra
- **Parallel processing infrastructure** - Chunked I/O, threading helpers, and progress bars
- New optional extras: `plot` (matplotlib), `s3` (boto3), `postgres` (psycopg2-binary), `mysql` (pymysql)
- **CI quality gates** - ruff, black, and coverage thresholds enforced in GitHub Actions; advisory mypy job; Python 3.12/3.13 added to the test matrix; `.pre-commit-config.yaml` added
- **`sql` command** - Ad-hoc DuckDB SQL queries over data files with jsonl/csv/parquet output (`undatum sql "SELECT ..." file.csv`)
- **`--version` flag** - Print the undatum version

### Changed
- `stats` (DuckDB and iterable engines) now returns a structured profile dictionary in addition to printing the profile table
- Recipes used by the `examples` command moved into the package (`undatum/recipes/`) so they work in PyPI installs
- Packaging is now fully `pyproject.toml`-based; legacy `setup.py` removed and templates/recipes declared as package data
- Removed unused direct dependency on `click`
- `core.py` split into per-domain CLI modules under `undatum/cli/` (data, pipeline, db, api, package, examples, plugins); `undatum.core` is now a thin assembly module
- Shared command scaffolding: `get_iterable_options` / `ITERABLE_OPTIONS_KEYS` centralized in `undatum/common/command_utils.py` (was duplicated in 31 modules) along with a `run_with_duckdb_fallback` helper
- Commands that previously logged an error and exited with code 0 on invalid parameters or unsupported output formats now raise `ValidationError` / `FormatError` (non-zero exit codes)
- Removed deprecated `IterableData` reader class; reading goes through `iterabledata`'s `open_iterable`. `DataWriter` is retained as the supported writer for open file objects (e.g. stdout)
- Logging configuration moved from import time (`undatum.core`) to the CLI entry point
- `ingester.py` (1,900 lines) decomposed into a package with one module per database backend (`undatum/cmds/ingester/`)
- `statistics.py` (1,200 lines) decomposed into a package with engine detection, DuckDB engine, and iterable engine modules (`undatum/cmds/statistics/`)
- S3 (`s3://`) input paths now work across all file-reading commands via a shared S3-aware opener
- `--progress` flag wired into `convert`, `validate`, and `join`
- `--threads` now configures the DuckDB engine across DuckDB-backed commands (stats, sort, dedup, search, join, select, slice, sample, sql)
- Connector plugins are now consulted in the shared I/O path: custom URI schemes (e.g. `myproto://...`) handled by an installed `ConnectorPlugin` work in all file-reading commands
- `plugins info` now lists the command names registered by command plugins

### Fixed
- DuckDB stats engine: implemented missing value, distribution, and type-category computations (previously the DuckDB path always fell back to the iterable engine)
- `Dataset` SDK methods `count()`, `head()`, `tail()` returned placeholder values; they now return actual results
- `Dataset.read()` options (encoding, delimiter, etc.) are now applied when iterating
- Unsupported database URI schemes (e.g. `http://`) now raise a clear error instead of being treated as SQLite paths
- Fixed YAML syntax error in the `api-serve-data` recipe

## [1.1.1] - 2026-01-19

### Added
- Added workflow and OpenSpec documentation for change proposals and agent workflows
- Added dataset documentation examples under `examples/doc/`

### Changed
- Expanded README with documentation pointers and dataset doc references

## [1.1.0] - 2026-01-18

### Added
- **Phase 1 Data Commands**: Added 7 new fundamental data processing commands:
  - `count` - Count rows in data files with DuckDB optimization for supported formats
  - `table` - Pretty-print data as aligned table for inspection using Rich library
  - `head` - Extract first N rows from files
  - `tail` - Extract last N rows using efficient buffering
  - `enum` - Add row numbers, UUIDs, or constant values to records
  - `reverse` - Reverse the order of rows in files
  - `fixlengths` - Normalize field counts by padding or truncating rows
- **Phase 2 Data Commands**: Added 9 new data cleaning and transformation commands:
  - `sort` - Sort rows by one or more columns with ascending/descending and numeric options
  - `sample` - Random sampling using reservoir sampling algorithm (fixed count or percentage)
  - `search` - Regex-based search and filtering across fields
  - `dedup` - Remove duplicate rows with key-field and keep-first/last options
  - `fill` - Fill empty/null values with constants or forward/backward fill strategies
  - `rename` - Rename fields by exact mapping or regex patterns
  - `explode` - Split columns by separator into multiple rows
  - `replace` - String replacement in fields with simple and regex support
  - `cat` - Concatenate files by rows (vertical) or columns (horizontal)
- **Phase 3 Data Commands**: Added 7 new advanced data processing commands:
  - `join` - Relational joins between files (inner, left, right, full outer) with hash-based and DuckDB SQL implementations
  - `diff` - Compare two files and show differences (added, removed, changed rows) with key-based comparison
  - `exclude` - Remove rows from input file where keys match exclusion file using hash lookup
  - `transpose` - Swap rows and columns with proper header handling
  - `sniff` - Detect file properties (delimiter, encoding, types, record count) with text/JSON/YAML output
  - `slice` - Extract specific rows by range or index list with DuckDB optimization
  - `fmt` - Reformat CSV data with delimiter, quote style, escape character, and line ending options
- **Schema Command Improvements**: Enhanced schema command with:
  - Full output format support (text/json/yaml) - previously ignored options now work
  - Working AI documentation with provider selection
  - Record counting included in schema output
  - Improved file format detection (XLSX, XLS, XML, DOCX)
  - Compression detection and reporting
  - Engine selection (auto/duckdb/iterable) for performance
  - Comprehensive error handling
  - Glob pattern support in bulk mode
  - Shared utilities (`schema_utils.py`) eliminating code duplication with analyzer
- **Schema Format Exports**: Added support for industry-standard schema formats:
  - `jsonschema` - JSON Schema (W3C/IETF standard) for API validation and OpenAPI specs
  - `avro` - Apache Avro schema format for Kafka message schemas and Hadoop pipelines
  - `parquet` - Parquet schema format for data lake schemas and Parquet file metadata
  - `cerberus` - Cerberus validation schema format (for backward compatibility with deprecated `scheme` command)
- **Stats Command DuckDB Optimization**: Added DuckDB engine support for statistics generation:
  - 10-100x faster statistics for CSV, JSONL, JSON, and Parquet files
  - Leverages DuckDB's `SUMMARIZE` and SQL aggregations for columnar processing
  - Automatic engine selection with fallback to iterable engine for unsupported formats
- **Database Ingestion Improvements**: Enhanced `ingest` command with:
  - MySQL support with auto-create table, upsert, and batch operations
  - SQLite support (file and in-memory) with PRAGMA optimizations, auto-create table, and upsert
  - Improved PostgreSQL, DuckDB, MongoDB, and Elasticsearch support

### Changed
- **Migrated to external iterabledata library**: All commands now use `open_iterable()` from the external `iterabledata` library instead of local `IterableData` class
- **Improved resource management**: All iterable operations now use try/finally blocks for proper resource cleanup
- **Batch write operations**: Commands now use `write_bulk()` for improved performance on large datasets
- **Iterator reset support**: Commands that need multiple passes over data now use `reset()` method when available
- **Schema command consolidation**: `scheme` command now redirects to `schema --format cerberus` with deprecation warning, unified schema interface with format selection
- **Stats command performance**: DuckDB engine provides dramatic performance improvements for supported formats

### Deprecated
- **Local IterableData class**: The `undatum.common.iterable.IterableData` class is deprecated and will be removed in a future version. Use `open_iterable()` from `iterable.helpers.detect` instead.
- **Local DataWriter class**: The `undatum.common.iterable.DataWriter` class is deprecated and will be removed in a future version. Use `open_iterable()` with `mode='w'` instead.
- **`scheme` command**: The `scheme` command is deprecated. Use `undatum schema --format cerberus` instead. The `scheme` command will show a deprecation warning but continues to work for backward compatibility.

### Fixed
- Fixed resource leaks in `statistics`, `textproc`, and `ingester` commands by properly closing iterable objects
- Fixed bug in `textproc.flatten()` where `fromfile` was used instead of `filename` parameter
- Fixed schema command output format options being ignored
- Fixed schema command AI documentation not working
- Fixed schema command missing record counting

## [1.0.18] - 2025-12-15

### Fixed
- Declared runtime dependencies in `pyproject.toml` and aligned `setup.py` so `pip install undatum` installs all required packages in clean environments

## [1.0.17] - 2025-12-12

### Changed
- **Improved CLI documentation**: Enhanced all command-line interface functions with detailed help text using Typer's `Annotated` types
- **Code refactoring**: Refactored analyzer output writing into separate `_write_analysis_output()` function for better maintainability
- **Better file handling**: Improved file output handling in analyzer command with proper context managers

### Fixed
- Fixed analyzer output not writing to files correctly when `--output` option was used
- Improved consistency between stdout and file output formatting

## [1.0.16] - 2025-12-12

### Added
- **Multi-provider AI support**: Added support for OpenAI, OpenRouter, Ollama, LM Studio, and Perplexity APIs
- **Structured AI output**: Replaced fragile text parsing with JSON Schema-based structured output for reliable AI responses
- **Flexible AI configuration**: Support for environment variables, config files (`undatum.yaml` or `~/.undatum/config.yaml`), and CLI arguments with proper precedence
- **AI provider factory**: New `get_ai_service()` function for easy provider instantiation
- **Enhanced error handling**: Proper exception classes (`AIServiceError`, `AIConfigurationError`, `AIAPIError`) with clear error messages
- **CLI arguments for AI**: Added `--ai-provider`, `--ai-model`, and `--ai-base-url` options to `analyze` command
- **Configuration management**: New `undatum/ai/config.py` module for unified configuration handling
- **Backward compatibility**: Old `get_fields_info()` and `get_description()` functions maintained for compatibility
- Enhanced code quality improvements and Pylint score improvements
- Better error handling and resource management

### Changed
- **AI system refactoring**: Completely refactored AI documentation system from Perplexity-only to multi-provider architecture
- **Structured responses**: All AI providers now use JSON Schema (`response_format: json_object`) instead of parsing CSV from markdown code blocks
- **Provider architecture**: Implemented abstract base class `AIService` with concrete provider implementations
- Improved code quality: fixed indentation, trailing whitespace, and formatting issues
- Refactored file operations to use `with` statements for better resource management
- Updated string formatting to use f-strings and lazy logging
- Fixed dangerous default arguments in function signatures
- Improved type hints and code documentation
- Updated `analyze` command to accept AI provider configuration
- Updated `schemer` command to use new AI service interface

### Fixed
- Fixed critical bug: added missing `_process_json_data` function in analyzer module
- Fixed bad indentation issues in `duckdb_decompose` function
- Fixed redefined builtin `id` parameter (renamed to `table_id`)
- Fixed unused imports and arguments
- Fixed dictionary iteration patterns (removed unnecessary `.keys()` calls)
- Fixed `isinstance()` calls to use tuple syntax for better performance
- Improved file handling with proper context managers
- **Fixed fragile AI response parsing**: Replaced error-prone text extraction with proper JSON parsing
- **Fixed AI service initialization**: Added proper error handling and fallback when AI service fails to initialize

## [1.0.15] - 2024-XX-XX

### Added
- Code quality improvements and linting fixes
- Better resource management with context managers
- Added `ingest` command for data ingestion
- Added globbing support for ingest command

### Changed
- Improved Pylint score from 6.30/10 to 7.60/10
- Refactored code for better maintainability
- Updated transformation (apply command) code to use iterabledata library
- Updated several commands to reuse iterabledata lib, more file formats supported by headers, frequency, stats and convert commands
- Replaced prettytables and tabulate with Rich library for better output formatting
- Updated analyze command to support automatic fields documentation generation

### Fixed
- Fixed JSON output for analyzer command
- Minor fixes and improvements

## [1.0.14] - 2024-XX-XX

### Added
- Added support to convert CSV and JSONL to ORC and AVRO formats
- Added parquet compression option
- Added encoding option for analyze command to allow manually set encoding
- Added formats conversion table to documentation

## [1.0.13] - 2022-04-20

### Fixed
- Fixed conversion xlsx-to-jsonl

### Added
- Added experimental command "query", not documented yet. Allows to use mistql query engine.

## [1.0.12] - 2022-01-30

### Added
- Added command "analyze" it provides human-readable information about data files: CSV, BSON, JSON lines, JSON, XML. Detects encoding, delimiters, type of files, fields with objects for JSON and XML files. Doesn't support Gzipped, ZIPped and other compressed files yet.

## [1.0.11] - 2022-01-30

### Changed
- Updated setup.py and requirements.txt to require certain versions of libs and Python 3.8

## [1.0.10] - 2022-01-29

### Added
- Added encoding and delimiter detection for commands: uniq, select, frequency and headers. Completely rewrote these functions. If options for encoding and delimiter set, they override detected. If not set, detected delimiter and encoding used.
- Added support of .parquet files to convert to. It's done in a simplest way using pandas "to_parquet" function.

## [1.0.9] - 2022-01-18

### Added
- Added support for CSV and BSON files for "stats" command

## [1.0.8] - 2021-07-14

### Changed
- Replaced json with orjson for some operations. Keep looking on performance changes and going to replace or json lib calls to orjson

## [1.0.7] - 2020-10-26

### Added
- Added initial code to convert JSON lines files to CSV

## [1.0.6] - 2020-04-20

### Added
- First public release on PyPI and updated github code

[Unreleased]: https://github.com/datacoon/undatum/compare/v1.1.1...HEAD
[1.1.1]: https://github.com/datacoon/undatum/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/datacoon/undatum/compare/v1.0.18...v1.1.0
[1.0.18]: https://github.com/datacoon/undatum/compare/v1.0.17...v1.0.18
[1.0.17]: https://github.com/datacoon/undatum/compare/v1.0.16...v1.0.17
[1.0.16]: https://github.com/datacoon/undatum/compare/v1.0.15...v1.0.16
[1.0.15]: https://github.com/datacoon/undatum/compare/v1.0.14...v1.0.15
[1.0.14]: https://github.com/datacoon/undatum/compare/v1.0.13...v1.0.14
[1.0.13]: https://github.com/datacoon/undatum/compare/v1.0.12...v1.0.13
[1.0.12]: https://github.com/datacoon/undatum/compare/v1.0.11...v1.0.12
[1.0.11]: https://github.com/datacoon/undatum/compare/v1.0.10...v1.0.11
[1.0.10]: https://github.com/datacoon/undatum/compare/v1.0.9...v1.0.10
[1.0.9]: https://github.com/datacoon/undatum/compare/v1.0.8...v1.0.9
[1.0.8]: https://github.com/datacoon/undatum/compare/v1.0.7...v1.0.8
[1.0.7]: https://github.com/datacoon/undatum/compare/v1.0.6...v1.0.7
[1.0.6]: https://github.com/datacoon/undatum/tree/v1.0.6
