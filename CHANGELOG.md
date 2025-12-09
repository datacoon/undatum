# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Enhanced code quality improvements and Pylint score improvements
- Better error handling and resource management

### Changed
- Improved code quality: fixed indentation, trailing whitespace, and formatting issues
- Refactored file operations to use `with` statements for better resource management
- Updated string formatting to use f-strings and lazy logging
- Fixed dangerous default arguments in function signatures
- Improved type hints and code documentation

### Fixed
- Fixed critical bug: added missing `_process_json_data` function in analyzer module
- Fixed bad indentation issues in `duckdb_decompose` function
- Fixed redefined builtin `id` parameter (renamed to `table_id`)
- Fixed unused imports and arguments
- Fixed dictionary iteration patterns (removed unnecessary `.keys()` calls)
- Fixed `isinstance()` calls to use tuple syntax for better performance
- Improved file handling with proper context managers

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

[Unreleased]: https://github.com/datacoon/undatum/compare/v1.0.15...HEAD
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
