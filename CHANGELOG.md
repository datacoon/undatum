# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/datacoon/undatum/compare/v1.7.0...HEAD)

### Fixed

- **Release binaries** — stop gitignoring `packaging/undatum.spec` so tagged PyInstaller jobs can find the spec

## [1.7.0](https://github.com/datacoon/undatum/compare/v1.6.0...v1.7.0) - 2026-08-13

### Added

- `**--flatten-nested**` on inspect and file transforms (`analyze`, `select`, `head`, `tail`, `table`, `uniq`, `frequency`, `headers`, `sort`, `sample`, `search`, `dedup`, `fill`, `rename`, `mask`, `plot`, `validate`, `sniff`, `split`, `join`, `diff`, `exclude`, `tui`, `web`, `ingest` / `db load`, `doc`, `package create` / `add-resource`, and the other single-file row commands) — unfold nested dict / array-of-dict fields onto dotted paths (`--max-nested-depth` and `--keep-nested-parents` / `--no-keep-nested-parents`; keep-parents default on). SDK `Dataset.read(..., flatten_nested=True)` applies the same projection when iterating; agent tools (`frequency`, `deduplicate`, `mask_fields`, `sample_data`) accept `flatten_nested`
- `**Dataset.convert_many**` — SDK bulk convert (`to_ext`, `filename_pattern`; same as `convert --recursive`)
- `**defaults.quotechar**` / `UNDATUM_QUOTECHAR` — default CSV quote character (same config path as `delimiter`)
- `**--error-log PATH**` — append iterabledata parse errors as JSONL (use with `--on-error skip` or `warn`)
- `**ai filter --flatten-nested**` — unfold nested fields into dotted paths for schema context and `--apply`; `--max-nested-depth` and `--keep-nested-parents` / `--no-keep-nested-parents` pass through when flattening
- `**ai filter --sample-size**` — rows sampled when inferring schema context for a file (engine default 10000 when omitted)
- `**schema --validate --sample-size**` — rows sampled when inferring the schema used for validation (engine default 10000 when omitted)
- `**ai suggest --sample-size**` — override how many sample rows are sent to the suggestion engine (default 5 when omitted)
- `**--quotechar**` — CSV quote character on convert, inspect, transforms, `split`, `ingest` / `db load`, `package`, `tui`, `web`, `plot`, `ai filter`, and `ai suggest` (iterabledata default `"`)
- `**convert --row-group-size**` — Parquet write row-group size (iterabledata `row_group_size`; defaults to the writer batch size when omitted; skips DuckDB COPY)
- `**convert --level**` — explicit codec compression level (iterabledata `codecargs.compression_level`; overrides `--profile`; skips DuckDB COPY)
- `**convert --filename-pattern**` — bulk output names with `{name}`, `{stem}`, `{ext}` (used with `--recursive`)
- `**convert --batch-size**` on native batch — forwarded as iterabledata `BatchSelection.batch_size` (Parquet/Arrow scanner chunks)
- `**ai doc --job-id**` — stable job identifier for documentation progress and JSON results (engine generates one when omitted)
- `**ai doc --sample-size` / `--detect-constraints` / `--statistics` / `--temperature` / `--max-tokens**` — pass-throughs into iterabledata documentation generation (`--no-detect-constraints` and `--no-statistics` disable the defaults; temperature and max tokens are omitted so engine defaults apply)
- `**ai doc --progress**` — print iterabledata documentation stages to stderr
- `**formats describe**` — show read/write memory, selection pushdown, codecs, source constraints, flat/tabular, native bulk I/O, and example iterableargs from the catalog
- `**--on-error raise|skip|warn**` — iterabledata parse-error policy on convert, inspect, file transforms, `split`, `tui`, `web`, `ingest` / `db load`, `package`, and `ai filter` / `ai suggest`; DuckDB paths skip to iterable when `skip` or `warn` is set
- `**--table` / `--sheet**` on convert, inspect, file transforms, `split`, `tui`, and `web`; two-file commands also take `--table2`; `ingest` / `db load` use `--source-table`; `package create` / `add-resource`, `ai filter`, and `ai suggest` take `--table`; agent tools (`frequency`, `deduplicate`, `mask_fields`, `sample_data`) accept `table`; SDK methods inherit `table=` from `Dataset.read()`
- `**ai doc --context**` — JSON prompt context; `--include-field-descriptions` and `--validate-output` on the non-block generate path
- `**schema --validate**` — check rows against an inferred schema (`--strict` flags fields not present in that schema); keep `undatum validate` for rule packs
- `**ai doc` default blocks** now include `agent_skill` and `codebook`
- `**convert --use-totals**` — use format-reported row totals for progress when available
- **SDK** — `Dataset.read(..., table=)` iterates the named sheet/table; `Dataset.stats(flatten_nested=True)` unfolds nested fields
- `**undatum formats tables SOURCE**` — list named sheets/tables before convert
- `**ai suggest --apply**` — apply a suggested transform spec (confirm unless `--yes`)
- `**ai doc --tables` / `--cache` / `--pii-mask-samples**` — pass-throughs into iterabledata documentation generation
- `**convert --write-mode**` — lakehouse append / overwrite / error / ignore / create
- `**--trust**` — acknowledge pickle deserialization risk on convert, stats, schema, select, and head
- `**formats list --capabilities**` — maturity, native bulk read/write, and extra columns
- **Native batch convert** — `--native-batch`, `--columns`, `--row-range START:END`; auto-enabled with `--low-memory` when both formats advertise native bulk I/O
- `**--profile fast|balanced|max**` on `convert` — codec performance profiles for compressed output (same profiles as `repack`)
- `**--keep-nested-parents**` on `stats` (default on) and `schema` (default off) — with `--flatten-nested`, keep parent dict/array fields alongside dotted children
- `**--max-nested-depth**` on `stats` and `schema` — with `--flatten-nested`, cap unfold depth (engine default 5)
- `**--flatten-nested**` on `stats` and `schema` — unfold nested dict / array-of-dict fields onto dotted paths
- **iterabledata extras as undatum extras** — `undatum[lakehouse]`, `[gis]`, `[scientific]`, `[access]`, `[compression]`
- `**undatum web**` — optional local browser session (`pip install "undatum[web]"`) over the same sampled processors as the TUI; default bind `127.0.0.1:8765`; HTMX-boosted forms; one action at a time; not a spreadsheet and not the read-only Data API
- `**undatum tui**` — optional Textual explorer (`pip install "undatum[tui]"`) that previews a bounded sample (default 200 rows); profile (`s`), frequency (`f`), sample filter (`/`), export (`e`), SQL (`ctrl+s`, default LIMIT 500), command palette (`:`), convert/save-as (`w`), validate sample (`v`), mask preview (`m`), pipeline YAML (`p`), and `s3://` open from the file picker (`u`); not a spreadsheet editor
- **Data API hardening** — optional `--api-key` / `UNDATUM_API_KEY`, `--cors-origins`, `s3://` resource paths, and JSON Schema validation for API configs
- `**stats --format-out json**` — machine-readable profiling output (also used when `--output` ends in `.json`)
- `**stats` HTML/Markdown reports** — `--format-out html|markdown` (also inferred from `.html` / `.md` output paths)
- **Plugin I/O and transforms** — connector plugins on the iterable path, `undatum apply --plugin`, and `undatum plugins validate`
- **Schema for Excel/XML/DOCX** — `schema` uses analyzer-style extraction for xlsx/xls/xml/docx
- **SDK result objects** — `Dataset.stats()` returns `StatsResult`; `head`/`tail` return `QueryResult`
- **Single-binary release artifacts** — PyInstaller linux/mac/win builds and smoke tests on tagged releases
- `**plot --filter` / `--aggregate**` — filter records before plotting; bar charts support `count`/`sum`/`mean` with `--value-field` and `--top-n`
- `**frequency` / `uniq --filter**` — DuckDB `WHERE` pushdown when the expression translates to SQL; otherwise the same comparison subset is evaluated in-process
- **JSON analysis output** — `headers`, `frequency`, and `uniq` accept `--format-out json` (also inferred from a `.json` output path); `sniff` accepts `--format-out json` / `.json` output
- **CLI defaults config** — `defaults:` in `undatum.yaml` or `~/.undatum/config.yaml` (and `UNDATUM_*` env vars) set engine, threads, progress, encoding, delimiter, and format_out; inspect with `undatum config show`
- **`undatum(1)` man page** — generated from the CLI (`make man`); installed to `share/man/man1`
- **`analyze --format-out`** — JSON/YAML/Markdown also inferred from `.json` / `.yaml` / `.md` output paths; omitted `--engine` no longer disables DuckDB
- **`pipeline doc`** — Mermaid flowchart (Markdown by default) from a pipeline YAML/JSON spec
- Pipeline validation accepts current commands (`sql`, `plot`, `repack`, `profile`, …) by reading the live CLI (`tui` and `web` are excluded)
- **`jsonl-normalization` pipeline template** and example connector/transform plugins
- Dependabot config for pip security patches and GitHub Actions updates
- **`--threads` process-pool parallelism** — opt-in chunk parallelism for Python-engine `convert`, `validate`, `stats`, and `frequency` (GitHub #18); ordered output; DuckDB paths stay single-process

### Changed

- `**iterabledata>=1.0.18**` is now a floor dependency so convert/stats/schema can use codec profiles, nested flatten, and the current format catalog
- `**--filter**` evaluates comparison/boolean expressions (`==`, `AND`/`OR` or `&&`/`||`) in-process with no MistQL; unsupported syntax (`LIKE`, `IN`, `match`) errors and points to `undatum sql`

### Removed

- `**undatum query**` and the `**mistql**` dependency — use `undatum sql` for DuckDB SQL over files, or `select --filter` for comparison filters (`undatum db query` is unchanged)

### Fixed

- `**pipeline run**` executes steps in-process (Typer argv mapping) instead of treating
`registered_commands` as a dict; `input`/`output` map to positionals for `convert`/`count`/`sql`,
`keys` maps to `--key-fields`, temp outputs are only injected for commands that accept an output
path, and later steps can use `$step_name` or the previous step's output
- Raised minimum dependency versions to address historical Snyk advisories for `setuptools`,
`dnspython`, `numpy`, and `zipp`.

## [1.6.0](https://github.com/datacoon/undatum/compare/v1.5.0...v1.6.0) - 2026-07-22

### Added

- `**repack` command** — recompress container codecs (`.gz`/`.zst`/…) at max by default, or rewrite Parquet/ORC/AVRO with native compression; supports `--level` and `--progress`
- `**--low-memory` on convert/sort/dedup** — spill-to-disk paths for large files (DuckDB COPY for duckable→Parquet converts; external merge sort; disk-backed exact dedup)
- `**db dump**` — export a table or SQL query to Parquet/CSV/JSONL
- **CI install-gate** — clean-venv wheel install smoke for convert/stats across Python versions
- `**pyarrow**` as a default dependency so Parquet works out of the box
- Quote-aware CSV delimiter sniffing via `csv.Sniffer` over multi-line samples
- Excel `--start-page` support on `uniq`, `frequency`, and `select`
- Docs: format support matrix, task quickstarts, tool positioning, uv/pipx install paths
- `**package add-resource` and `package validate` subcommands** — extend existing packages and validate descriptors (full validation with optional `undatum[frictionless]`)
- `**Dataset.package()` SDK method** — programmatic Frictionless Data Package generation
- **Pipeline `package` step** — direct Packager integration for `create`, `add-resource`, and `validate`
- `**undatum[frictionless]` optional extra** — installs `frictionless` for full package validation
- `**ai doc` schema enrichment** — SDMX-style field-name hints, LLM field-name remapping to canonical columns, and BOM-stripped JSONL keys for block-based schema documentation
- **CSV delimiter auto-detection** — semicolon, tab, and pipe delimiters detected automatically when `--delimiter` is omitted (analyze, convert, select, doc, package, and shared read paths)

### Changed

- `**DUCKABLE_CODECS**` accepts iterabledata's `"gz"` id (and `"gzip"`) so gzipped files route to DuckDB
- Repo hygiene: removed committed `pylint_report.txt`, `data.csv`, and IDE dirs; fixed CHANGELOG placeholder dates
- `**package create**` — emits Frictionless profile/resource metadata, inferred coverage fields, schema uniqueness constraints, wired read options (`delimiter`, `encoding`, `tagname`, etc.), single-pass `--autodoc`, Rich success output, portable relative resource paths, and optional `--zip` archive output
- **Shared schema type mapping** — Frictionless/JSON Schema conversions centralized in `schema_utils`
- `**analyze**` — DuckDB-accelerated tabular analysis, per-field uniqueness statistics, S3 URI support, and improved nested JSON/XML table handling
- `**ai doc**` — uses block-based schema generation with post-enrichment; preserves the original source filename in output
- `**select**` — DuckDB `COPY` fast path for CSV/JSON/Parquet output; dot-notation nested field selection; filter expressions pushed to SQL when translatable
- `**filter` SQL translation** — comparison and boolean expressions translate to DuckDB `WHERE` clauses for accelerated filtering
- `**convert**` — removed legacy format-specific converters; routes through iterabledata with shared delimiter resolution
- **Shared `command_utils`** — centralized iterable read options, CSV delimiter resolution, and DuckDB read expressions
- **Docs synced with iterabledata 1.0.14–1.0.18** — format matrix and README now cover Avro writes, codec profiles (`fast`/`balanced`/`max`), GeoJSON Text Sequence / TAR / genomic VCF, Zarr / FlatGeobuf / genomic-interval / OTLP profiles, experimental open-data GIS/scientific/business formats, Paimon and DuckLake, and Delta/Iceberg write support (via `iterabledata[lakehouse]` and related extras). Catalog described as 140+ formats; `formats list --capabilities` remains authoritative for the installed engine

### Fixed

- `**repack` progress bar** — closing an indeterminate tqdm bar (`total=None`) no longer raises `TypeError: bool() undefined…`
- `**package create**` — uses `UndatumError` hierarchy for missing inputs/files; avoids duplicate LLM calls when `--autodoc` is enabled
- `**analyze**` — handles empty record sets without failing schema inference
- `**uniq**` — DuckDB engine keeps results instead of reporting an unsupported engine

### Notes

- **iterabledata engine upgrades** (inherited when `pip` resolves a recent iterabledata on Python 3.10+; undatum still declares `requires-python >= 3.9`):
  - **1.0.14** — Avro write support; ORC schema inference for unusual column names; DuckDB/SQLite default table names from output filename
  - **1.0.16** — `geojsonseq`, read-only `tar` multi-member containers, `genomic_vcf`; XXE-hardened XML; stricter parse-error policy on some formats
  - **1.0.17** — codec performance profiles; native Parquet/Arrow batch conversion; Zarr, GeoParquet, FlatGeobuf, BED/GFF, CRAM, OTLP profiles; bounded columnar I/O
  - **1.0.18** — experimental open-data format pack (FileGDB, MIF, LAS, Access, MAT, SEG-Y, GRIB2, IATI, …); Paimon Row/Mosaic/tables and DuckLake; Delta Lake and Iceberg bounded writes (Hudi remains read-only)

## [1.5.0](https://github.com/datacoon/undatum/compare/v1.4.0...v1.5.0) - 2026-06-29

### Added

- `**api openapi` command** — export OpenAPI 3.x schema from an API config without starting the server (`--output`, `--format json|yaml`)
- **Data API discovery endpoint** — `GET /` returns resource list and documentation links
- **Data API startup banner** — prints base URL, resource endpoints, and `/docs` links when the server starts
- `**httpx**` added to the `api` optional extra (required for HTTP integration tests)

### Changed

- **Data API list responses** — endpoints now return `{data, pagination}` instead of a bare JSON array (**breaking** for API clients)
- **Data API OpenAPI** — per-resource query parameters, field schemas, and `field__op` filter documentation in Swagger UI
- **Data API sorting** — `sort=field` and `sort=-field` aliases supported alongside `order_by` / `order_dir`
- **Data API pagination** — optional `include_total=true` adds total matching row count to the response envelope
- **CI** — test job installs `undatum[api]` so Data API HTTP tests run in CI

### Fixed

- **Data API** — discover stores absolute file paths; warns on resource name collisions; validates files at serve time; skips composite primary-key detail routes; serializes DuckDB types (dates, decimals, UUIDs) to JSON-safe values
- **Data API** — `serve`, `run`, and `openapi` raise a clear `DependencyError` when the `api` extra is not installed
- `**api-serve-data` recipe** — default flow uses `api run`; config path defaults to `api-config.yml`

## [1.4.0](https://github.com/datacoon/undatum/compare/v1.3.0...v1.4.0) - 2026-06-26

### Added

- `**ai` commands** - AI-assisted workflows backed by iterabledata's `iterable.ai` stack: `ai doc` (block-based dataset documentation with metadata enrichment and PII-safe sampling), `ai filter` (natural-language/DSL to filter translation, with `--apply` to execute and stream matching rows), `ai plan` (declarative conversion planning), and `ai suggest` (transform suggestions). Supports OpenAI, Anthropic, Gemini, Azure, OpenRouter, Ollama, LM Studio, and Perplexity, defaulting to undatum's existing AI configuration
- `**formats` commands** - `formats list` surfaces iterabledata's full format catalog; `--capabilities` shows the runtime capability matrix (read/write/streaming/etc.) per format, with machine-readable JSON output
- `**mcp` commands** - `mcp serve` starts a Model Context Protocol stdio server exposing undatum's agent tools; `mcp tools` lists the available tools. New `undatum-mcp` console script
- **Agent tools (`undatum.tools`)** - 17 JSON-schema agent tools for LLM function calling: the 12 iterabledata foundation tools (detect, schema, stats, convert, validate, analyze, documentation, etc.) re-exported as the single source of truth, plus 5 undatum-specific tools (`query_sql`, `frequency`, `deduplicate`, `mask_fields`, `sample_data`). Includes OpenAI/Anthropic tool definitions, a unified `call_tool`, and a LangChain `get_tools()` adapter
- **SDK DataFrame & typed-row interop** - `Dataset.to_pandas()`, `to_polars()`, `to_dask()`, `as_dataclasses()`, and `as_pydantic()`, delegating to iterabledata's adapters
- **Bulk conversion** - `convert --recursive` (with `--to-ext`) converts a directory or glob pattern, treating OUTPUT as a directory
- **Extended database engines** - `db query` and file-reading commands now reach MS SQL Server (`mssql://`, `sqlserver://`), ClickHouse (`clickhouse://`), MongoDB (`mongodb://`), and Elasticsearch/OpenSearch (`elasticsearch://`, `opensearch://`) via iterabledata's read-only drivers; driver options can be passed through the URI query string (`?collection=`, `?index=`, `?limit=`, ...). New `undatum/common/db_source.py`
- **Cloud storage URIs beyond S3** - GCS (`gs://`/`gcs://`), Azure (`az://`/`abfs://`/`abfss://`), and `s3a://` are opened natively via iterabledata's fsspec support; `s3://` writes are now supported (delegated to iterabledata) instead of raising
- New optional extras: `mcp`, `langchain`, `polars`, `dask`, `cloud` (fsspec/s3fs/gcsfs/adlfs), `mssql` (pyodbc), `clickhouse` (clickhouse-driver)

### Changed

- `convert` now routes through iterabledata's engine, supporting any format it can read/write (100+ formats, including cloud URIs) as input or output. Read-only and schema-required output formats (protobuf, Cap'n Proto, Thrift) fail fast with actionable, capability-aware error messages and writable-format suggestions
- `SUPPORTED_FILE_TYPES`, `COMPRESSED_FILE_TYPES`, `TEXT_DATA_TYPES`, and `BINARY_FILE_TYPES` are now derived at import time from iterabledata's registries (with static fallbacks when iterabledata is unavailable), so undatum recognizes every format and codec the underlying engine supports
- `doc` command metadata extraction (keywords, geographic/temporal coverage, language, theme) and semantic-type/PII detection now delegate to `iterable.ai.metadata` and `iterable.ai.semantic` while keeping CLI output backward compatible

## [1.3.0](https://github.com/datacoon/undatum/compare/v1.1.1...v1.3.0) - 2026-06-11

### Added

- `**mask` command** - Anonymize sensitive fields with redact, deterministic hash, and randomize methods
- `**pipeline` commands** - Run and validate multi-step YAML/JSON workflows (`pipeline run`, `pipeline validate`)
- `**pipeline templates` commands** - List built-in pipeline templates and initialize pipelines from them (`pipeline templates list`, `pipeline templates init`)
- `**examples` commands** - Browse and run a built-in recipe library (`examples list`, `examples show`, `examples run`); recipes now ship inside the package
- `**plot` command** - Generate histogram, bar, scatter, and line charts with matplotlib
- `**db query` / `db load` commands** - Execute SQL against PostgreSQL/MySQL/SQLite and load files into database tables
- **Data API** - Serve files as a read-only HTTP API (`api discover`, `api serve`, `api run`) via the `api` extra
- `**package create` command** - Generate Frictionless Data Package descriptors
- `**extract` command** - Extract tables/text from PDF/DOC/DOCX/XLS/XLSX via the `extract` extra
- `**profile` command** - Alias for `stats`
- **Python SDK** - `Dataset` fluent API (`from undatum import Dataset`) with read/write, transforms, and analysis methods returning real values (`stats()`, `count()`, `head()`, `tail()`)
- **Plugin system** - Entry-point based plugins (`undatum.plugins` group) with `plugins list` / `plugins info` commands
- **Rich validation rules** - YAML/JSON rule files with severity levels for `validate`
- **Error handling framework** - `UndatumError` hierarchy with actionable messages, typo suggestions, and consistent exit codes
- **S3 support** - Read/write `s3://` URIs in major commands via the `s3` extra
- **Parallel processing infrastructure** - Chunked I/O, threading helpers, and progress bars
- New optional extras: `plot` (matplotlib), `s3` (boto3), `postgres` (psycopg2-binary), `mysql` (pymysql)
- **CI quality gates** - ruff, black, and coverage thresholds enforced in GitHub Actions; advisory mypy job; Python 3.12/3.13 added to the test matrix; `.pre-commit-config.yaml` added
- `**sql` command** - Ad-hoc DuckDB SQL queries over data files with jsonl/csv/parquet output (`undatum sql "SELECT ..." file.csv`)
- `**--version` flag** - Print the undatum version

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

## [1.1.1](https://github.com/datacoon/undatum/compare/v1.1.0...v1.1.1) - 2026-01-19

### Added

- Added workflow and OpenSpec documentation for change proposals and agent workflows
- Added dataset documentation examples under `examples/doc/`

### Changed

- Expanded README with documentation pointers and dataset doc references

## [1.1.0](https://github.com/datacoon/undatum/compare/v1.0.18...v1.1.0) - 2026-01-18

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
- `**scheme` command**: The `scheme` command is deprecated. Use `undatum schema --format cerberus` instead. The `scheme` command will show a deprecation warning but continues to work for backward compatibility.

### Fixed

- Fixed resource leaks in `statistics`, `textproc`, and `ingester` commands by properly closing iterable objects
- Fixed bug in `textproc.flatten()` where `fromfile` was used instead of `filename` parameter
- Fixed schema command output format options being ignored
- Fixed schema command AI documentation not working
- Fixed schema command missing record counting

## [1.0.18](https://github.com/datacoon/undatum/compare/v1.0.17...v1.0.18) - 2025-12-15

### Fixed

- Declared runtime dependencies in `pyproject.toml` and aligned `setup.py` so `pip install undatum` installs all required packages in clean environments

## [1.0.17](https://github.com/datacoon/undatum/compare/v1.0.16...v1.0.17) - 2025-12-12

### Changed

- **Improved CLI documentation**: Enhanced all command-line interface functions with detailed help text using Typer's `Annotated` types
- **Code refactoring**: Refactored analyzer output writing into separate `_write_analysis_output()` function for better maintainability
- **Better file handling**: Improved file output handling in analyzer command with proper context managers

### Fixed

- Fixed analyzer output not writing to files correctly when `--output` option was used
- Improved consistency between stdout and file output formatting

## [1.0.16](https://github.com/datacoon/undatum/compare/v1.0.15...v1.0.16) - 2025-12-12

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

## [1.0.15](https://github.com/datacoon/undatum/compare/v1.0.14...v1.0.15) - 2024-12-12

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

## [1.0.14](https://github.com/datacoon/undatum/compare/v1.0.13...v1.0.14) - 2024-11-20

### Added

- Added support to convert CSV and JSONL to ORC and AVRO formats
- Added parquet compression option
- Added encoding option for analyze command to allow manually set encoding
- Added formats conversion table to documentation

## [1.0.13](https://github.com/datacoon/undatum/compare/v1.0.12...v1.0.13) - 2022-04-20

### Fixed

- Fixed conversion xlsx-to-jsonl

### Added

- Added experimental command "query", not documented yet. Allows to use mistql query engine.

## [1.0.12](https://github.com/datacoon/undatum/compare/v1.0.11...v1.0.12) - 2022-01-30

### Added

- Added command "analyze" it provides human-readable information about data files: CSV, BSON, JSON lines, JSON, XML. Detects encoding, delimiters, type of files, fields with objects for JSON and XML files. Doesn't support Gzipped, ZIPped and other compressed files yet.

## [1.0.11](https://github.com/datacoon/undatum/compare/v1.0.10...v1.0.11) - 2022-01-30

### Changed

- Updated setup.py and requirements.txt to require certain versions of libs and Python 3.8

## [1.0.10](https://github.com/datacoon/undatum/compare/v1.0.9...v1.0.10) - 2022-01-29

### Added

- Added encoding and delimiter detection for commands: uniq, select, frequency and headers. Completely rewrote these functions. If options for encoding and delimiter set, they override detected. If not set, detected delimiter and encoding used.
- Added support of .parquet files to convert to. It's done in a simplest way using pandas "to_parquet" function.

## [1.0.9](https://github.com/datacoon/undatum/compare/v1.0.8...v1.0.9) - 2022-01-18

### Added

- Added support for CSV and BSON files for "stats" command

## [1.0.8](https://github.com/datacoon/undatum/compare/v1.0.7...v1.0.8) - 2021-07-14

### Changed

- Replaced json with orjson for some operations. Keep looking on performance changes and going to replace or json lib calls to orjson

## [1.0.7](https://github.com/datacoon/undatum/compare/v1.0.6...v1.0.7) - 2020-10-26

### Added

- Added initial code to convert JSON lines files to CSV

## [1.0.6](https://github.com/datacoon/undatum/tree/v1.0.6) - 2020-04-20

### Added

- First public release on PyPI and updated github code

