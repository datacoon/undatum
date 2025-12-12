# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/datacoon/undatum/compare/v1.0.17...HEAD
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
