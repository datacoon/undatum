# undatum

> A powerful command-line tool for data processing and analysis

**Version:** 1.7.0

**undatum** (pronounced *un-da-tum*) is a modern CLI tool designed to make working with large datasets as simple and efficient as possible. It provides a unified interface for converting, analyzing, validating, and transforming data across multiple formats.

## Features

- **140+ formats via iterabledata**: CSV, JSON, JSON Lines, BSON, XML, XLS/XLSX, Parquet, AVRO, ORC, plus geospatial, lakehouse (Delta/Iceberg/Lance/DuckLake/Paimon), scientific, RDF, log, config, graph, and feed formats. Run `undatum formats list` to see every supported format and its read/write capabilities.
- **Compression support**: GZ, XZ, BZ2, ZIP, ZSTD, LZ4, 7Z, Brotli, Snappy, LZO
- **Multi-cloud I/O**: Read and write `s3://`, `gs://`/`gcs://`, and `az://`/`abfs://`/`abfss://` URIs natively via iterabledata (`pip install "undatum[cloud]"`)
- **Database sources**: Read from PostgreSQL, MySQL/MariaDB, SQLite, MS SQL Server, ClickHouse, MongoDB, and Elasticsearch/OpenSearch (`undatum db query`, `undatum db dump`)
- **Optional TUI and web UI**: Explore a bounded sample in the terminal (`undatum tui`) or a local browser (`undatum web`); not a spreadsheet and not the Data API
- **Low memory footprint**: Streams data for efficient processing of large files
- **Automatic detection**: Encoding, delimiters (comma, semicolon, tab, pipe), and file types
- **Frictionless Data Packaging**: Create, extend, and validate `datapackage.json` descriptors with schema inference, coverage metadata, and optional AI autodoc (`undatum package`)
- **Data validation**: Built-in rules for emails, URLs, and custom validators
- **Advanced statistics**: Field analysis, frequency calculations, and date detection
- **Flexible filtering**: Comparison `--filter` expressions on `select` / `frequency` / `uniq` / `plot` (and others); DuckDB SQL for `LIKE`, `IN`, joins (`undatum sql`)
- **Bulk conversion**: Convert whole directories or glob patterns in parallel (`undatum convert --recursive`)
- **Schema generation**: Automatic schema detection and generation
- **Database ingestion**: Ingest data to MongoDB, PostgreSQL, DuckDB, MySQL, SQLite, and Elasticsearch with retry logic and error handling
- **Ad-hoc SQL on files**: Run DuckDB SQL over CSV, JSONL, Parquet, and other formats (`undatum sql`)
- **AI-powered tooling**: Dataset documentation, natural-language filtering, conversion planning, and transform suggestions via iterabledata's AI stack with many LLM providers (OpenAI, Anthropic, Gemini, Azure, OpenRouter, Ollama, LM Studio, Perplexity) — see `undatum ai`
- **Agent tools & MCP server**: Expose undatum operations to LLM agents as JSON tools (`undatum.tools`), LangChain `StructuredTool`s, or a Model Context Protocol stdio server (`undatum mcp serve`)
- **Format catalog**: Inspect formats and capabilities (`undatum formats list|describe|export|tables`, including maturity and native-bulk columns via `undatum formats list --capabilities`)
- **DataFrame & typed-row interop**: Convert datasets to pandas/Polars/Dask or iterate rows as dataclasses/Pydantic models from the `Dataset` SDK
- **Optional Data API**: Serve file-backed datasets over HTTP (FastAPI + DuckDB) with interactive OpenAPI docs, static schema export, and filtering/pagination

## Documentation

- [`docs/QUICKSTART.md`](docs/QUICKSTART.md) — task-oriented first success paths
- [`docs/SCENARIOS.md`](docs/SCENARIOS.md) — usage scenarios by role: pick your goal, get verified commands
- [`docs/FORMAT_SUPPORT.md`](docs/FORMAT_SUPPORT.md) — honest format capability matrix
- [`docs/POSITIONING.md`](docs/POSITIONING.md) — undatum vs miller / DuckDB / csvkit
- [`docs/LARGE_FILES.md`](docs/LARGE_FILES.md) — multi-GB convert/sort/dedup guidance
- [`CHANGELOG.md`](CHANGELOG.md) for version history (current release: **1.7.0**)
- `WORKFLOW_GUIDE.md` for contributor workflow and OpenSpec usage
- `openspec/` for change proposals, specs, and implementation summaries
- `examples/doc/` for dataset documentation output samples
- `docs/ERROR_HANDLING.md` for troubleshooting common errors
- `docs/ERROR_HANDLING_PATTERNS.md` for error handling patterns (developers)

## Installation

### Using uv or pipx (recommended for CLI use)

```bash
uv tool install undatum
# or
pipx install undatum
```

### Using pip

```bash
pip install --upgrade pip setuptools
pip install undatum
```

Dependencies are declared in `pyproject.toml` and will be installed automatically by modern versions of `pip` (23+), including **pyarrow** for Parquet. If you see missing-module errors after installation, upgrade `pip` and retry.

### macOS

Preferred paths:

```bash
brew install pipx && pipx install undatum
# or
uv tool install undatum
```

A Homebrew formula for undatum itself is tracked separately; `pipx`/`uv` are the supported macOS install methods today.

Release tags also publish **PyInstaller single-file binaries** (Linux, macOS, Windows) as GitHub Actions artifacts. These are ops-oriented; `pipx`/`uv` remain the supported install paths for most users.

A man page ships with the package (`man undatum` after install, or `make man` to regenerate `man/undatum.1`).

### Optional extras

Some features require optional dependencies, installed as extras. This is the canonical list; feature sections elsewhere in the docs link back here.

| Extra | Enables |
|-------|---------|
| `api` | Data API server (`undatum api`, FastAPI + uvicorn + httpx) |
| `extract` | Document extraction (`undatum extract`, PDF/DOC/DOCX tables and text) |
| `plot` | Plotting (`undatum plot`, matplotlib) |
| `tui` | Interactive terminal UI (`undatum tui`, Textual) |
| `web` | Local web UI (`undatum web`, FastAPI + Jinja2) |
| `mcp` | MCP server for AI agents (`undatum mcp serve`) |
| `langchain` | LangChain agent tools |
| `polars`, `dask` | DataFrame interop from the `Dataset` SDK |
| `s3` | S3 cloud storage support (boto3) |
| `cloud` | Multi-cloud storage via fsspec (S3 + GCS + Azure) |
| `postgres`, `mysql`, `mssql`, `clickhouse` | Database connectors |
| `frictionless` | Full Frictionless Data Package validation |
| `lakehouse` | Delta / Iceberg / Lance / DuckLake / Hudi via iterabledata |
| `gis` | Geospatial and LiDAR formats |
| `scientific` | MATLAB, geophysical, and HDF5 formats |
| `access` | Microsoft Access (`.mdb` / `.accdb`) |
| `compression` | Extra codecs (snappy, brotli, lzo) |

```bash
pip install "undatum[api]"
pip install "undatum[extract]"
pip install "undatum[plot]"
pip install "undatum[tui]"
pip install "undatum[web]"
pip install "undatum[mcp]"
pip install "undatum[langchain]"
pip install "undatum[polars]"
pip install "undatum[dask]"
pip install "undatum[s3]"
pip install "undatum[cloud]"
pip install "undatum[postgres]"
pip install "undatum[mysql]"
pip install "undatum[mssql]"
pip install "undatum[clickhouse]"
pip install "undatum[frictionless]"
pip install "undatum[lakehouse]"
pip install "undatum[gis]"
pip install "undatum[scientific]"
pip install "undatum[access]"
pip install "undatum[compression]"

# Combine extras in one install
pip install "undatum[extract,api]"
```

After installation both `undatum` and the shorter `data` command are available:

```bash
undatum --version
undatum headers data.csv
data headers data.csv   # same thing
```

### Shell completion

Typer provides built-in shell completion. Install it for your shell:

```bash
# Bash
undatum --install-completion bash

# Zsh
undatum --install-completion zsh

# Fish
undatum --install-completion fish
```

To preview completion scripts without installing:

```bash
undatum --show-completion bash
```

### Requirements

- Python 3.9 or greater

### Install from source

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install .
# or build distributables
python -m pip install build && python -m build
```

## Quick Start

```bash
# Inspect supported formats
undatum formats list --capabilities

# AI block-based documentation
undatum ai doc data.csv

# Bulk-convert a directory to Parquet
undatum convert ./raw ./processed --recursive --to-ext parquet
undatum convert ./raw ./out --recursive --to-ext jsonl --filename-pattern "{stem}.converted.jsonl"

# Print version
undatum --version

# Get file headers
undatum headers data.jsonl

# Analyze file structure
undatum analyze data.jsonl

# Generate dataset documentation
undatum doc data.jsonl --format markdown --output docs/dataset.md

# Create and validate a Frictionless Data Package
undatum package create data.csv --output datapackage.json
undatum package validate datapackage.json

# Extract tables from a PDF
undatum extract report.pdf --output-format csv --output report.csv

# Serve a CSV as a read-only API (prints resource URLs and /docs on startup)
undatum api run data.csv

# Export OpenAPI schema without running the server
undatum api openapi --config api.yml --output openapi.json

# Generate API config (YAML) for multiple files
undatum api discover data.csv other.parquet --output api.yml

# Serve from config
undatum api serve --config api.yml

# Get statistics
undatum stats data.csv

# Run ad-hoc SQL over a file
undatum sql "SELECT city, COUNT(*) AS n FROM data GROUP BY city" cities.csv

# Convert XML to JSON Lines
undatum convert --tagname item data.xml data.jsonl

# Get unique values
undatum uniq --fields category data.jsonl

# Calculate frequency
undatum frequency --fields status data.csv

# Count rows
undatum count data.csv

# View first 10 rows
undatum head data.jsonl

# View last 10 rows
undatum tail data.csv

# Display formatted table
undatum table data.csv --limit 20

# Explore a sample in the terminal or a local browser (optional extras)
# pip install "undatum[tui]" && undatum tui data.csv
# pip install "undatum[web]" && undatum web data.csv
```

## Commands

All commands are available as `undatum <command>` or via the shorter `data` alias (`data convert ...` is identical to `undatum convert ...`).

**Top-level data commands:** `convert`, `extract`, `analyze`, `doc`, `stats` (`profile`), `validate`, `schema`, `schema_bulk`, `sql`, `select`, `search`, `mask`, `plot`, `ingest`, `tui`, `web`, and the transform/inspection commands documented below.

**Command groups:**

| Group | Subcommands |
|-------|-------------|
| `ai` | `doc`, `filter`, `plan`, `suggest` |
| `api` | `discover`, `serve`, `run`, `openapi` |
| `db` | `query`, `load`, `dump` |
| `package` | `create`, `add-resource`, `validate` |
| `pipeline` | `run`, `validate`, `templates list`, `templates init` |
| `formats` | `list`, `describe`, `export` |
| `mcp` | `serve`, `tools` |
| `examples` | `list`, `show`, `run` |
| `plugins` | `list`, `info` |

See also: [Cloud Storage](#cloud-storage-support) · [Python SDK](#python-sdk) · [AI Agent Tools & MCP](#ai-agent-tools-and-mcp-server) · [Pipeline Workflows](#pipeline-workflows)

### `analyze`

Analyzes data files and provides human-readable insights about structure, encoding, fields, and data types. With `--autodoc`, automatically generates field descriptions and dataset summaries using AI.

```bash
# Basic analysis
undatum analyze data.jsonl

# With AI-powered documentation
undatum analyze data.jsonl --autodoc

# Using specific AI provider
undatum analyze data.jsonl --autodoc --ai-provider openai --ai-model gpt-4o-mini

# Output to file
undatum analyze data.jsonl --output report.yaml --autodoc

# Named Excel sheet
undatum analyze workbook.xlsx --table Sheet2

# Nested JSONL: unfold dict fields onto dotted paths
undatum analyze nested.jsonl --flatten-nested
```

**Output includes:**
- File type, encoding, compression
- Number of records and fields
- Field types and structure
- Per-field uniqueness statistics (unique count, total count, uniqueness %)
- Table detection for nested data (JSON/XML)
- AI-generated field descriptions (with `--autodoc`)
- AI-generated dataset summary (with `--autodoc`)

**Read options** (auto-detected when omitted):
- `--delimiter` — CSV/TSV separator (comma, semicolon, tab, or pipe)
- `--quotechar` — CSV quote character
- `--encoding` — file encoding
- `--engine` — `auto` (default) or `duckdb` for accelerated tabular analysis
- `--table` / `--sheet` — named Excel sheet or multi-table source

**AI Provider Options:**
- `--ai-provider`: Provider id (`openai`, `anthropic`, `gemini`, `azure`, `openrouter`, `ollama`, `lmstudio`, `perplexity`)
- `--ai-model`: Model name (provider-specific)
- `--ai-base-url`: Custom API endpoint URL

**Supported AI Providers:**

1. **OpenAI** (default if `OPENAI_API_KEY` is set)
   ```bash
   export OPENAI_API_KEY=sk-...
   undatum analyze data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini
   ```

2. **Anthropic**
   ```bash
   export ANTHROPIC_API_KEY=sk-ant-...
   undatum analyze data.csv --autodoc --ai-provider anthropic --ai-model claude-3-5-haiku-latest
   ```

3. **Google Gemini**
   ```bash
   export GEMINI_API_KEY=...
   undatum analyze data.csv --autodoc --ai-provider gemini --ai-model gemini-2.0-flash
   ```

4. **Azure OpenAI**
   ```bash
   export AZURE_OPENAI_API_KEY=...
   export AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
   undatum analyze data.csv --autodoc --ai-provider azure --ai-model gpt-4o-mini
   ```

5. **OpenRouter** (unified API for many hosted models)
   ```bash
   export OPENROUTER_API_KEY=sk-or-...
   undatum analyze data.csv --autodoc --ai-provider openrouter --ai-model openai/gpt-4o-mini
   ```

6. **Ollama** (local models, no API key required)
   ```bash
   # Start Ollama and pull a model first: ollama pull llama3.2
   undatum analyze data.csv --autodoc --ai-provider ollama --ai-model llama3.2
   # Or set custom URL: export OLLAMA_BASE_URL=http://localhost:11434
   ```

7. **LM Studio** (local models, OpenAI-compatible API)
   ```bash
   # Start LM Studio and load a model
   undatum analyze data.csv --autodoc --ai-provider lmstudio --ai-model local-model
   # Or set custom URL: export LMSTUDIO_BASE_URL=http://localhost:1234/v1
   ```

8. **Perplexity** (backward compatible, uses `PERPLEXITY_API_KEY`)
   ```bash
   export PERPLEXITY_API_KEY=pplx-...
   undatum analyze data.csv --autodoc --ai-provider perplexity
   ```

**Configuration Methods:**

AI provider can be configured via:
1. **Environment variables** (lowest precedence):
   ```bash
   export UNDATUM_AI_PROVIDER=openai
   export OPENAI_API_KEY=sk-...
   ```

2. **Config file** (medium precedence):
   Create `undatum.yaml` in your project root or `~/.undatum/config.yaml`:
   ```yaml
   ai:
     provider: openai
     api_key: ${OPENAI_API_KEY}  # Can reference env vars
     model: gpt-4o-mini
     timeout: 30
   defaults:
     engine: duckdb
     threads: 4
     progress: true
     encoding: utf8
     # delimiter: ";"   # omit to keep CSV auto-detection
     # quotechar: "'"   # omit to keep iterabledata default '"'
     format_out: json
   ```

   Inspect the resolved values with `undatum config show`. Environment variables
   `UNDATUM_ENGINE`, `UNDATUM_THREADS`, `UNDATUM_PROGRESS`, `UNDATUM_ENCODING`,
   `UNDATUM_DELIMITER`, `UNDATUM_QUOTECHAR`, and `UNDATUM_FORMAT_OUT` are the
   lowest-precedence source.
   Explicit CLI flags always win.

3. **CLI arguments** (highest precedence):
   ```bash
   undatum analyze data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini
   ```

### `doc`

Generates dataset documentation with schema, statistics, and samples in Markdown (default), JSON, YAML, or text. Supports AI-powered descriptions with `--autodoc`. Also available as the `document` alias.

```bash
# Markdown documentation (default)
undatum doc data.jsonl

# JSON documentation with samples
undatum doc data.jsonl --format json --sample-size 5 --output report.json
undatum doc nested.jsonl --flatten-nested --format json

# With AI-powered descriptions
undatum doc data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini
```

**Output includes:**
- Dataset metadata and summary counts
- Schema fields with types and descriptions
- Field-level uniqueness statistics (when available)
- Sample records (configurable via `--sample-size`)

**Extended metadata and PII options:**
- `--semantic-types`: annotate fields with semantic types (requires `metacrafter` CLI)
- `--pii-detect`: detect PII fields and include a PII summary (requires `metacrafter` CLI)
- `--pii-mask-samples`: redact detected PII values in samples (use with `--pii-detect`)

```bash
# Semantic typing and PII summary
undatum doc data.csv --semantic-types --pii-detect --format json

# Mask PII values in samples
undatum doc data.csv --pii-detect --pii-mask-samples --format json
```

**Optional dependencies:**
- `metacrafter` (for semantic types and PII detection)
- `langdetect` (for language detection in metadata)

### `ai`

AI-assisted workflows backed by iterabledata's `iterable.ai` stack. Subcommands: `doc`, `filter`, `plan`, and `suggest`. Supports OpenAI, Anthropic, Gemini, Azure OpenAI, OpenRouter, Ollama, LM Studio, and Perplexity — configure via `undatum.yaml`, environment variables, or CLI flags (see [AI Provider Options](#analyze) under `analyze`).

```bash
# Block-based dataset documentation (markdown default). Default blocks:
# general, schema, quality, examples, statistics, agent_skill, codebook
undatum ai doc data.csv

# JSON output with selected blocks; schema enrichment maps LLM field names
# to canonical columns and fills SDMX-style hints (e.g. FREQ:Frequency)
undatum ai doc data.csv --format json --blocks general,schema,quality

# Natural-language filter translation (use --apply to stream matching rows)
undatum ai filter data.csv "active users in New York" --apply
undatum ai filter workbook.xlsx "city is Dushanbe" --table Sheet2 --apply
undatum ai filter "age > 30" data.csv --sample-size 500
undatum ai filter "name == 'Alice'" quoted.csv --quotechar "'"
undatum ai filter "lat > 40" nested.jsonl --flatten-nested --apply
undatum ai filter "lat > 40" nested.jsonl --flatten-nested --max-nested-depth 2 --no-keep-nested-parents

# Conversion planning and transform suggestions
undatum ai plan data.csv --to parquet
undatum ai suggest data.csv "normalize phone numbers"
undatum ai suggest data.csv "normalize phone numbers" --sample-size 20

# Apply a suggested transform spec (JSONL; --yes skips the confirm prompt)
undatum ai suggest data.csv "rename id to user_id" --apply --yes --output out.jsonl

# Document a named Excel sheet; cache and mask PII samples
undatum ai doc workbook.xlsx --tables Sheet2 --cache --pii-mask-samples
undatum ai doc data.csv --context '{"title": "City register"}'
undatum ai doc data.csv --progress
undatum ai doc data.csv --sample-size 20 --no-detect-constraints --no-statistics
undatum ai doc data.csv --temperature 0.2 --max-tokens 2048
undatum ai doc data.csv --job-id run-42 --progress
```

For block-based documentation with schema enrichment, prefer `ai doc` over legacy `analyze --autodoc` / `schema --autodoc` workflows.

### `package`

Generates, extends, and validates Frictionless Data Package descriptors (`datapackage.json`) from one or more data files. Supports optional package metadata, schema inference, and AI-powered metadata generation with `--autodoc`.

```bash
# Create datapackage.json for a single file
undatum package create data.csv --output datapackage.json
undatum package create workbook.xlsx --table Sheet2 --output datapackage.json
undatum package create nested.jsonl --flatten-nested --output datapackage.json

# Create a package directory with data file copies
undatum package create data.csv --package-dir out/package

# Zip the materialized package directory
undatum package create data.csv --package-dir out/package --zip out/package.zip

# Add another resource to an existing package
undatum package add-resource out/package/datapackage.json new.csv

# Validate a package descriptor
undatum package validate out/package/datapackage.json

# Provide metadata and enable AI metadata generation
undatum package create data.csv --title "Sales data" --keywords sales,finance \
  --autodoc --ai-provider openai --ai-model gpt-4o-mini
```

**Subcommands:**
- `create` — generate a new descriptor (default workflow)
- `add-resource` — append resources to an existing descriptor
- `validate` — validate descriptor structure (full checks with `pip install undatum[frictionless]`)

**Metadata options:**
- `--name`, `--title`, `--description`, `--keywords`
- `--licenses` (semicolon-separated entries, e.g. `name=MIT;name=ODC-PDDL-1.0`)
- `--sources` (semicolon-separated entries, e.g. `title=World Bank,path=https://...`)
- `--contributors` (semicolon-separated entries, e.g. `title=Jane Doe,email=jane@example.com`)
- `--version` - Package version string

**Features:**
- **Frictionless profile**: Emits `profile: tabular-data-package` with resource `format`/`mediatype`
- **Schema inference**: Automatically infers field types, descriptions, and uniqueness constraints
- **Multiple resources**: Package multiple files as separate resources
- **Remote URIs**: Support for HTTP/HTTPS URLs as resource paths
- **Package directory**: Bundle `datapackage.json` with data file copies
- **AI metadata**: Use `--autodoc` to generate metadata with AI assistance (single-pass, no duplicate LLM calls)
- **Streaming-safe**: Processes large datasets without loading everything into memory
- **Python SDK**: `Dataset.read("data.csv").package(output="datapackage.json")`

**Additional options:**
- `--package-dir`: Create a package directory with data file copies
- `--zip`: Create a ZIP archive of the package directory (requires `--package-dir`)
- `--autodoc`: Enable AI-powered metadata generation (reuses `doc` command logic)
- `--engine`: Processing engine (`auto` or `duckdb`)
- `--delimiter`, `--encoding`, `--tagname`, `--start-line`, `--start-page`: Passed through to analysis and sampling
- `--objects-limit`: Maximum objects to analyze for schema inference (default: 10000)
- `--sample-size`: Number of sample records for metadata inference (default: 10)

### `api`

Serves files as a read-only HTTP API (FastAPI + DuckDB). Supports CSV, JSON/JSONL, and Parquet files. Requires the `api` extra:

```bash
pip install "undatum[api]"
```

`discover` works without the extra; `serve`, `run`, and `openapi` require it and show an install hint if missing.

**Subcommands:**

| Command | Description |
|---------|-------------|
| `api discover` | Infer schema from files and write a YAML/JSON API config |
| `api serve` | Start the HTTP server from a config file |
| `api run` | Discover in memory and serve immediately (no config file) |
| `api openapi` | Export OpenAPI 3.x schema without starting the server |

```bash
# Discover resources and serve in one step
undatum api run data.csv

# Generate an API config (YAML) for multiple files
undatum api discover data.csv other.parquet --output api.yml

# Serve from a config file
undatum api serve --config api.yml --host 127.0.0.1 --port 8000

# Optional API key (or UNDATUM_API_KEY) and CORS for browser clients
undatum api serve --config api.yml --api-key "$UNDATUM_API_KEY" --cors-origins https://app.example.com

# Export OpenAPI schema to a file
undatum api openapi --config api.yml --output openapi.json
undatum api openapi --config api.yml --output openapi.yaml --format yaml
```

On startup, the server prints a banner with the base URL, resource endpoints, and links to `/docs`, `/redoc`, and `/openapi.json`.

**Endpoints:**

- `GET /` — API discovery (resource list and documentation links)
- `GET /{resource}` — list records with filtering, sorting, and pagination
- `GET /{resource}/{pk}` — fetch a single record (when a single-column primary key is inferred or configured)
- `GET /docs` — interactive Swagger UI
- `GET /redoc` — ReDoc documentation
- `GET /openapi.json` — OpenAPI schema

**List response format:**

```json
{
  "data": [{ "id": 1, "name": "Alice" }],
  "pagination": { "limit": 50, "offset": 0, "count": 1, "total": 100 }
}
```

The `total` field is included only when `include_total=true` is passed (may be slower on large files).

See [Data API security](docs/DATA_API.md) for API keys, CORS, reverse-proxy guidance, and `s3://` resource paths.

**Query parameters:**

- **Filters:** `field__op=value` where `op` is one of `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `like` (or `field=value` as shorthand for `eq`)
- **Sorting:** `order_by=field` with `order_dir=asc|desc`, or `sort=field` / `sort=-field` (descending alias)
- **Pagination:** `limit` (default 50, max 1000), `offset`, and optional `include_total=true`

**Discover options:**

- `--output` — write config to a file (stdout if omitted)
- `--format-in` — override format detection (`csv`, `json`, `jsonl`, `parquet`)
- `--config-format` — `yaml` or `json`
- `--default-limit`, `--max-limit` — pagination defaults for generated config
- `--allowed-ops` — comma-separated filter operators

**Serve / run options:**

- `--host` — bind address (default: `127.0.0.1`)
- `--port` — bind port (default: `8000`)

**Example requests:**

```bash
curl "http://127.0.0.1:8000/sales?limit=10"
curl "http://127.0.0.1:8000/sales?amount__gt=100&order_by=sold_at&order_dir=desc"
curl "http://127.0.0.1:8000/sales/42"
```

**Security notes:**

- The API is read-only; no mutations are possible
- Binds to `127.0.0.1` by default; there is no built-in authentication, so put it behind a reverse proxy with auth before exposing it publicly

See also: [`examples/api/api-example.md`](examples/api/api-example.md)

### `mask`

Masks sensitive fields for anonymization. Supports redaction, deterministic hashing (preserves joins), and type-compatible randomization.

```bash
# Redact email and phone fields
undatum mask data.csv --fields email,phone --method redact --output masked.csv

# Hash user IDs (deterministic, preserves joins)
undatum mask data.jsonl --fields user_id --method hash --salt my-salt --output masked.jsonl

# Randomize age and email fields
undatum mask data.csv --fields age,email --method randomize --output masked.csv
```

**Masking methods:**
- `redact` (default) - replace values with a fixed token (`***`)
- `hash` - deterministic one-way hash; the same input always produces the same output, so joins across files are preserved. Use `--salt` for additional security
- `randomize` - replace values with random but type-compatible values

### `extract`

Extracts tables or text from PDF/DOC/DOCX/XLS/XLSX files and outputs CSV, JSON, NDJSON, Parquet,
or a Frictionless Data Package. PDF extraction supports table, text, or OCR modes.

```bash
# PDF tables to CSV
undatum extract report.pdf --output-format csv --output report.csv

# Extract tables from multiple files
undatum extract data/*.pdf --output-format parquet --output-dir out/

# PDF text extraction for specific pages
undatum extract report.pdf --method text --pages 1-3 --output-format ndjson --output report.ndjson
```

**Optional dependencies:**
- `pdfplumber` (PDF tables/text)
- `pdf2image` + `pytesseract` (OCR)
- `textract` (legacy .doc)

### `convert`

Converts data between any formats supported by iterabledata (140+, see `undatum formats list`). Reading and writing are handled by the iterabledata engine, including cloud URIs (`s3://`, `gs://`, `az://`). Use `--recursive` to bulk-convert a directory or glob pattern.

```bash
# XML to JSON Lines
undatum convert --tagname item data.xml data.jsonl

# CSV to Parquet
undatum convert data.csv data.parquet

# JSON Lines to CSV
undatum convert data.jsonl data.csv

# Convert from S3 to local
undatum convert s3://my-bucket/data.csv output.jsonl

# Bulk-convert a directory of CSVs to Parquet
undatum convert ./raw ./processed --recursive --to-ext parquet
undatum convert ./raw ./out --recursive --to-ext jsonl --filename-pattern "{stem}.converted.jsonl"

# Convert local to S3
undatum convert input.csv s3://my-bucket/output.parquet

# Convert S3 to S3
undatum convert s3://bucket/input.jsonl s3://bucket/output.parquet
```

**Cloud storage:** Input and output paths support `s3://`, `gs://`/`gcs://`, and `az://`/`abfs://` URIs when the cloud extra is installed. See [Cloud Storage Support](#cloud-storage-support).

**Key options:**
- `--format-in` / `--format-out` — override format detection
- `--table` / `--sheet` — named table or Excel sheet (keep `--start-page` for a 0-based index)
- `--native-batch` / `--columns` / `--row-range` — native columnar batch convert (auto with `--low-memory` when both formats support it); `--batch-size` also sizes native scanner chunks
- `--profile fast|balanced|max` — codec performance profile for compressed output
- `--level N` — explicit compression level for compressed output (overrides `--profile`; skips DuckDB COPY)
- `--write-mode append|overwrite|error|ignore|create` — lakehouse write mode (Delta / Iceberg / DuckLake / Lance)
- `--row-group-size N` — Parquet write row-group size (skips DuckDB COPY; pair with `--batch-size` if you need groups smaller than convert's write batches)
- `--use-totals` — use format-reported row totals for progress when available
- `--trust` — acknowledge pickle deserialization risk
- `--on-error raise|skip|warn` — parse-error policy for malformed rows (default: raise)
- `--error-log PATH` — append skipped/warned parse errors as JSONL
- `--delimiter`, `--quotechar`, `--encoding`, `--tagname` — passed through to the reader (delimiter auto-detected for CSV when omitted)
- `--recursive` / `--to-ext` / `--filename-pattern` — bulk-convert directories or globs (`{name}`, `{stem}`, `{ext}` in the output name)
- `--flatten` — flatten nested records to a flat schema
- `--atomic` — write to a temp file and rename on success (local paths only)
- `--threads`, `--batch-size`, `--progress` — throughput and feedback controls (`--threads` enables process-pool chunk parallelism for single-file Python-engine convert; also used as concurrent workers for `--recursive` bulk convert)

### `repack`

Recompress a file at **maximum compression by default**, preserving the data format.

- Container codecs (`.gz`, `.zst`, `.bz2`, `.xz`, `.lz4`, …): stream-recompress with the same codec at max strength (or `--level`).
- Built-in formats (Parquet / ORC / AVRO): rewrite using native compression (Parquet defaults to `zstd`).
- Omitting OUTPUT rewrites the input atomically.

```bash
# In-place max recompress of a gzip file
undatum repack data.csv.gz

# Explicit output and faster level
undatum repack data.jsonl.zst out.jsonl.zst --level 3

# Parquet → zstd (built-in compression)
undatum repack data.parquet out.parquet

# Wrap an uncompressed file into a codec container
undatum repack data.csv data.csv.zst
```

**Key options:**
- `--level` / `-l` — compression level (overrides default maximum)
- `--compression` — override codec (container or format-native)
- `--progress` / `--no-progress` — progress bar (default: on)

**Supported conversions:**

`convert` uses iterabledata's engine, so any **readable** format can be converted to any **writable** one — there is no fixed pairwise matrix. The live catalog depends on installed optional dependencies; inspect it on your machine:

```bash
# All formats with read/write flags
undatum formats list

# Formats that can be used as conversion output
undatum formats list --writable

# Read-only inputs (e.g. ARFF, Hudi, GPX, HDF5, TAR)
undatum formats list --read-only

# Capability matrix (bulk, streaming, tables, nested, maturity, native bulk)
undatum formats list --capabilities

# List sheets/tables in a workbook or SQLite/lakehouse source
undatum formats tables workbook.xlsx
undatum formats tables data.sqlite --json

# Single-format details (aliases, optional extras, limitations)
undatum formats describe parquet

# Machine-readable catalog export
undatum formats export --output formats.json
```

**Common examples:**

| Use case | Example |
|----------|---------|
| Tabular text → columnar | `undatum convert data.csv data.parquet` |
| Columnar → tabular text | `undatum convert data.parquet data.csv` |
| JSON Lines ↔ CSV | `undatum convert data.jsonl data.csv` |
| Excel → JSON Lines | `undatum convert sheet.xlsx sheet.jsonl` |
| XML → JSON Lines | `undatum convert --tagname item feed.xml feed.jsonl` |
| Geospatial | `undatum convert points.geojson points.parquet` |
| GeoJSON Text Sequence | `undatum convert features.geojsonl features.parquet` |
| Bulk directory/glob | `undatum convert ./raw ./out --recursive --to-ext parquet` |

**Format families** (non-exhaustive; run `formats list` for the full set):

| Family | Examples |
|--------|----------|
| Tabular text | `csv` (alias: `tsv`), `jsonl` (alias: `ndjson`), `annotatedcsv`, `csvw`, `fwf`, `ssv` |
| Columnar / analytics | `parquet`, `orc`, `avro`, `arrow`, `geoparquet`, `zarr`, `ddb` |
| Lakehouse | `delta`, `iceberg`, `lance`, `ducklake`, `paimon` (Hudi remains read-only) |
| Documents / config | `json`, `yml` (alias: `yaml`), `xml`, `toml` |
| Geospatial | `geojson`, `geojsonseq`, `gml`, `gpx`, `shp`, `gpkg`, `kml`, `fgdb`, `mif`, `las` |
| Scientific / statistical | `h5`, `nc`, `mat`, `segy`, `grib2`, `sas`, `sav`, `dta` (many are read-only) |
| Containers | `zip`, `tar` (read-only multi-member), WebDataset |
| Logs / feeds | `log`, `gelf`, `cef`, `rss`, `kafka` |
| Graph / RDF | `graphml`, `gexf`, `jsonld`, `nt`, `ttl`, `trig`, `hdt` |

See [`docs/FORMAT_SUPPORT.md`](docs/FORMAT_SUPPORT.md) for extras (`undatum[lakehouse]`, `undatum[gis]`, `undatum[scientific]`) and version notes.

**Limitations:**

- **Read-only formats** can be inputs but not outputs — check with `formats list --writable`.
- **Schema-required outputs** (`protobuf`, `capnp`, `thrift`) need an externally supplied schema or message class and cannot be used as generic conversion targets.
- Override detection when the file extension is ambiguous: `--format-in` / `--format-out` (see `undatum convert --help`).
- Lakehouse and many open-data formats need the matching **iterabledata** optional extra and Python 3.10+.

### `count`

Counts the number of rows in a data file. With DuckDB engine, counting is instant for supported formats.

```bash
# Count rows in CSV file
undatum count data.csv

# Count rows in JSONL file
undatum count data.jsonl

# Use DuckDB engine for faster counting
undatum count data.parquet --engine duckdb

# Named Excel sheet
undatum count workbook.xlsx --table Sheet2
```

### `head`

Extracts the first N rows from a data file. Useful for quick data inspection.

```bash
# Extract first 10 rows (default)
undatum head data.csv

# Extract first 20 rows
undatum head data.jsonl --n 20

# Save to file
undatum head data.csv --n 5 output.csv

# Named Excel sheet
undatum head workbook.xlsx --table Sheet2 --n 5

# Nested JSONL: unfold dict fields onto dotted paths
undatum head nested.jsonl --flatten-nested --n 5
```

### `tail`

Extracts the last N rows from a data file. Uses efficient buffering for large files.

```bash
# Extract last 10 rows (default)
undatum tail data.csv

# Extract last 50 rows
undatum tail data.jsonl --n 50

# Save to file
undatum tail data.csv --n 20 output.csv

# Named Excel sheet
undatum tail workbook.xlsx --table Sheet2 --n 5

# Nested JSONL: unfold dict fields onto dotted paths
undatum tail nested.jsonl --flatten-nested --n 5
```

### `enum`

Adds row numbers, UUIDs, or constant values to records. Useful for adding unique identifiers or sequential numbers.

```bash
# Add row numbers (default field: row_id, starts at 1)
undatum enum data.csv output.csv

# Add UUIDs
undatum enum data.jsonl --field id --type uuid output.jsonl

# Add constant value
undatum enum data.csv --field status --type constant --value "active" output.csv

# Custom starting number
undatum enum data.jsonl --field sequence --start 100 output.jsonl
undatum enum workbook.xlsx --table Sheet2 --field row_id out.jsonl
```

### `reverse`

Reverses the order of rows in a data file.

```bash
# Reverse rows
undatum reverse data.csv output.csv

# Reverse JSONL file
undatum reverse data.jsonl output.jsonl
undatum reverse workbook.xlsx --table Sheet2 out.jsonl
```

### `table`

Displays data in a formatted, aligned table for inspection. Uses the rich library for beautiful terminal output.

```bash
# Display first 20 rows (default)
undatum table data.csv

# Display with custom limit
undatum table data.jsonl --limit 50

# Display only specific fields
undatum table data.csv --fields name,email,status
undatum table workbook.xlsx --table Sheet2
undatum table nested.jsonl --flatten-nested
```

### `tui`

Interactive terminal UI for exploring a **sample** of a dataset (not the whole file).
Requires `pip install "undatum[tui]"` and a real TTY.

```bash
pip install "undatum[tui]"
undatum tui data.csv
undatum tui data.parquet --limit 500
undatum tui workbook.xlsx --table Sheet2
undatum tui nested.jsonl --flatten-nested
```

Keys: `q` quit, `?` help, `o` open, `s` profile, `f` frequency on the selected
column, `/` filter the sample, `e` export the current view, `w` convert/save as
(full file, `--low-memory`), `v` validate the sample, `m` mask preview, `p`
export pipeline YAML, `:` command palette, `ctrl+s` SQL (default `LIMIT 500` on
the `data` view), `Tab` cycle panes. From the file picker, `u` opens a local
path or `s3://` URI. The status line shows the equivalent CLI command. Recent
files are stored as paths only in `~/.undatum/tui-history.json`. Use `table` /
`profile` / `sql` when you are not on an interactive terminal.

### `web`

Local browser UI for the same sampled session as `tui` (not a public Data API).
Requires `pip install "undatum[web]"`. Binds to `127.0.0.1:8765` by default.

```bash
pip install "undatum[web]"
undatum web data.csv
undatum web data.parquet --limit 500 --no-open
undatum web workbook.xlsx --table Sheet2
undatum web nested.jsonl --flatten-nested --no-open
```

Open a path or `s3://` URI, or upload a file (streamed to a temp directory).
The page shows a bounded sample, equivalent CLI lines, profile, frequency,
filter, SQL (default `LIMIT 500`), export, convert `--low-memory`, validate,
mask, and pipeline YAML export. Use `undatum api serve` for a read-only machine
API.

### `fixlengths`

Ensures all rows have the same number of fields by padding shorter rows or truncating longer rows. Useful for data cleaning workflows.

```bash
# Pad rows with empty string (default)
undatum fixlengths data.csv --strategy pad output.csv

# Pad with custom value
undatum fixlengths data.jsonl --strategy pad --value "N/A" output.jsonl

# Truncate longer rows
undatum fixlengths data.csv --strategy truncate output.csv
```

### `headers`

Extracts field names from data files. Works with CSV, JSON Lines, BSON, and XML files.

```bash
undatum headers data.jsonl
undatum headers data.csv --limit 50000
undatum headers data.csv --format-out json --output fields.json
undatum headers workbook.xlsx --table Sheet2
```

### `stats` / `profile`

Generates comprehensive statistics and profiling metrics about your dataset. With DuckDB engine, statistics generation is 10-100x faster for supported formats (CSV, JSONL, JSON, Parquet).

```bash
# Basic statistics
undatum stats data.jsonl

# Enhanced profiling (alias)
undatum profile data.csv

# With date detection
undatum stats data.csv --checkdates

# Using DuckDB engine
undatum stats data.parquet --engine duckdb

# Machine-readable JSON (also used when --output ends in .json)
undatum stats data.csv --format-out json --output stats.json

# HTML or Markdown profiling report (also inferred from --output extension)
undatum stats data.csv --format-out html --output profile.html
undatum stats data.csv --output profile.md

# Nested JSONL: unfold dict fields onto dotted paths
undatum stats nested.jsonl --flatten-nested --format-out json
undatum stats nested.jsonl --flatten-nested --max-nested-depth 2
undatum stats nested.jsonl --flatten-nested --no-keep-nested-parents

# Named Excel sheet
undatum stats workbook.xlsx --table Sheet2
```

**Statistics include:**
- Field types and array flags
- **Missing value rates** (count and percentage)
- **Cardinality analysis** (distinct counts and percentages)
- **Type inference** (categorical vs numerical classification)
- **Distribution statistics** for numerical fields (mean, median, percentiles, min/max, stddev)
- Unique value counts and percentages
- Min/max/average lengths
- Date field detection

**Performance:** DuckDB engine automatically selected for supported formats, providing columnar processing and SQL-based aggregations for faster statistics.

**Profile Command:** The `profile` command is an alias for `stats` with a focus on data profiling and quality metrics.

#### Profiling Metrics Explained

The enhanced statistics output provides comprehensive data profiling:

**Missing Value Analysis:**
- Shows count and percentage of missing/null values per field
- Helps identify data quality issues and incomplete records
- Example: `5 (2.5%)` means 5 missing values out of 200 records (2.5%)

**Cardinality Analysis:**
- **Distinct count**: Number of unique values in a field
- **Cardinality percentage**: Percentage of distinct values (distinct/total)
- **High cardinality**: Fields with many unique values (e.g., IDs, timestamps)
- **Low cardinality**: Fields with few unique values (e.g., status codes, categories)
- Example: `150 (75%)` means 150 distinct values out of 200 records

**Type Inference:**
- **Categorical**: Fields with low cardinality, typically string-like values (e.g., status, category, country)
- **Numerical**: Fields with numeric types and high cardinality (e.g., age, price, score)
- **Mixed**: Fields that don't clearly fit categorical or numerical patterns
- Helps understand data structure and choose appropriate analysis methods

**Distribution Statistics (Numerical Fields):**
- **Mean (μ)**: Average value
- **Median (m)**: Middle value (50th percentile)
- **Percentiles**: 25th, 75th, 90th, 95th, 99th percentiles for outlier detection
- **Min/Max**: Range of values
- **Standard deviation**: Measure of data spread
- Example output: `μ=42.5, m=40.0` shows mean of 42.5 and median of 40.0

#### Use Cases

**Data Quality Assessment:**
```bash
# Profile dataset to identify quality issues
undatum profile customer_data.csv

# Look for:
# - High missing value rates (>10% may indicate data collection issues)
# - Unexpected cardinality (e.g., status field with 1000+ unique values)
# - Outliers in numerical fields (check min/max vs percentiles)
```

**Schema Discovery:**
```bash
# Understand dataset structure before processing
undatum profile new_dataset.jsonl

# Use type inference to:
# - Identify categorical fields for grouping/aggregation
# - Identify numerical fields for statistical analysis
# - Plan appropriate data transformations
```

**Data Exploration Workflows:**
```bash
# Quick profiling as part of ETL pipeline
undatum profile raw_data.csv > profile_report.txt

# Use profiling metrics to:
# - Decide on data cleaning strategies (fill missing values, handle outliers)
# - Choose appropriate aggregation methods
# - Validate data after transformations
```

### `frequency`

Calculates frequency distribution for specified fields.

```bash
undatum frequency --fields category data.jsonl
undatum frequency --fields status,region data.csv
undatum frequency --fields city --format-out json --output freq.json data.csv
undatum frequency --fields city workbook.xlsx --table Sheet2
undatum frequency --fields capital_city.lat nested.jsonl --flatten-nested
```

### `uniq`

Extracts all unique values from specified field(s).

```bash
# Single field
undatum uniq --fields category data.jsonl

# Multiple fields (unique combinations)
undatum uniq --fields status,region data.jsonl
undatum uniq --fields city --format-out json --output cities.json data.csv
undatum uniq --fields city workbook.xlsx --table Sheet2
undatum uniq --fields capital_city.lat nested.jsonl --flatten-nested
```

### `sort`

Sorts rows by one or more columns. Supports multiple sort keys, ascending/descending order, and numeric sorting.

```bash
# Sort by single column ascending
undatum sort data.csv --by name output.csv

# Sort by multiple columns
undatum sort data.jsonl --by name,age output.jsonl

# Sort descending
undatum sort data.csv --by date --desc output.csv

# Numeric sort
undatum sort data.csv --by price --numeric output.csv
undatum sort workbook.xlsx --table Sheet2 --by city out.jsonl
undatum sort nested.jsonl --by capital_city.lat --flatten-nested --numeric capital_city.lat
```

### `sample`

Randomly selects rows from a data file using reservoir sampling algorithm.

```bash
# Sample fixed number of rows
undatum sample data.csv --n 1000 output.csv

# Sample by percentage
undatum sample data.jsonl --percent 10 output.jsonl
```

### `search`

Filters rows using regex patterns. Searches across specified fields or all fields.

```bash
# Search across all fields
undatum search data.csv --pattern "error|warning"

# Search in specific fields
undatum search data.jsonl --pattern "^[0-9]+$" --fields id,code

# Case-insensitive search
undatum search data.csv --pattern "ERROR" --ignore-case
```

### `dedup`

Removes duplicate rows. Can deduplicate by all fields or specified key fields.

```bash
# Deduplicate by all fields
undatum dedup data.csv output.csv

# Deduplicate by key fields
undatum dedup data.jsonl --key-fields email output.jsonl

# Keep last duplicate
undatum dedup data.csv --key-fields id --keep last output.csv
```

### `fill`

Fills empty or null values with specified values or strategies (forward-fill, backward-fill).

```bash
# Fill with constant value
undatum fill data.csv --fields name,email --value "N/A" output.csv

# Forward fill (use previous value)
undatum fill data.jsonl --fields status --strategy forward output.jsonl

# Backward fill (use next value)
undatum fill data.csv --fields category --strategy backward output.csv
```

### `rename`

Renames fields by exact mapping or regex patterns.

```bash
# Rename by exact mapping
undatum rename data.csv --map "old_name:new_name,old2:new2" output.csv

# Rename using regex
undatum rename data.jsonl --pattern "^prefix_" --replacement "" output.jsonl
undatum rename workbook.xlsx --table Sheet2 --map "city:city_name" out.jsonl
```

### `explode`

Splits a column by separator into multiple rows. Creates one row per value, duplicating other fields.

```bash
# Explode comma-separated values
undatum explode data.csv --field tags --separator "," output.csv

# Explode pipe-separated values
undatum explode data.jsonl --field categories --separator "|" output.jsonl
```

### `replace`

Performs string replacement in specified fields. Supports simple string replacement and regex-based replacement.

```bash
# Simple string replacement
undatum replace data.csv --field name --pattern "Mr\." --replacement "Mr" output.csv

# Regex replacement
undatum replace data.jsonl --field email --pattern "@old.com" --replacement "@new.com" --regex output.jsonl

# Global replacement (all occurrences)
undatum replace data.csv --field text --pattern "old" --replacement "new" --global output.csv
```

### `cat`

Concatenates files by rows or columns.

```bash
# Concatenate files by rows (vertical)
undatum cat file1.csv file2.csv --mode rows output.csv

# Concatenate files by columns (horizontal)
undatum cat file1.csv file2.csv --mode columns output.csv
```

### `join`

Performs relational joins between two files. Supports inner, left, right, and full outer joins.

```bash
# Inner join by key field
undatum join data1.csv data2.csv --on email --type inner output.csv

# Left join (keep all rows from first file)
undatum join data1.jsonl data2.jsonl --on id --type left output.jsonl

# Right join (keep all rows from second file)
undatum join data1.csv data2.csv --on id --type right output.csv

# Full outer join (keep all rows from both files)
undatum join data1.jsonl data2.jsonl --on id --type full output.jsonl
undatum join workbook.xlsx other.xlsx --table Sheet2 --table2 Cities --on city out.jsonl
undatum join left.jsonl right.jsonl --on capital_city.lat --flatten-nested out.jsonl
```

### `diff`

Compares two files and shows differences (added, removed, and changed rows).

```bash
# Compare files by key
undatum diff file1.csv file2.csv --key id
undatum diff workbook.xlsx other.xlsx --table Sheet2 --table2 Cities --key city
undatum diff nested1.jsonl nested2.jsonl --key name --flatten-nested

# Ignore order and show summary only (good for CI)
undatum diff file1.parquet file2.parquet --ignore-order --summary-only

# Output detailed diff to Markdown with numeric tolerance
undatum diff file1.csv file2.csv \
  --key user_id \
  --numeric-tolerance 0.001 \
  --output-format markdown \
  --output diff.md

# Fail CI when change thresholds are exceeded
undatum diff file1.csv file2.csv \
  --key id \
  --max-added-rows 10 \
  --max-removed-rows 5 \
  --max-changed-rows 0
```

### `exclude`

Removes rows from input file where keys match exclusion file. Uses hash-based lookup for performance.

```bash
# Exclude rows by key
undatum exclude data.csv blacklist.csv --on email output.csv

# Exclude with multiple key fields
undatum exclude data.jsonl exclude.jsonl --on id,email output.jsonl
undatum exclude workbook.xlsx skip.csv --table Sheet2 --on city out.jsonl
undatum exclude nested.jsonl skip.jsonl --on capital_city.lat --flatten-nested out.jsonl
```

### `transpose`

Swaps rows and columns, handling headers appropriately.

```bash
# Transpose CSV file
undatum transpose data.csv output.csv

# Transpose JSONL file
undatum transpose data.jsonl output.jsonl
```

### `sniff`

Detects file properties including delimiter, encoding, field types, and record count.

```bash
# Detect file properties (text output)
undatum sniff data.csv

# Output sniff results as JSON
undatum sniff data.jsonl --format json

# Output as YAML
undatum sniff data.csv --format yaml
undatum sniff nested.jsonl --flatten-nested --format json
```

### `slice`

Extracts specific rows by range or index list. Supports efficient DuckDB-based slicing for supported formats.

```bash
# Slice by range
undatum slice data.csv --start 100 --end 200 output.csv

# Slice by specific indices
undatum slice data.jsonl --indices 1,5,10,20 output.jsonl
```

### `fmt`

Reformats CSV data with specific formatting options (delimiter, quote style, escape character, line endings).

```bash
# Change delimiter
undatum fmt data.csv --delimiter ";" output.csv

# Change quote style
undatum fmt data.csv --quote always output.csv

# Change escape character
undatum fmt data.csv --escape backslash output.csv

# Change line endings
undatum fmt data.csv --line-ending crlf output.csv
```

### `select`

Selects and reorders columns from files. Supports filtering, nested dot-notation fields, and engine selection. When the DuckDB engine is used, filter expressions are pushed to SQL when possible and results can be written directly via `COPY` for CSV, JSON, and Parquet output.

```bash
undatum select --fields name,email,status data.jsonl
undatum select --fields name,email --filter "`status` == 'active'" data.jsonl
undatum select --fields user.name,user.email --engine duckdb data.jsonl
undatum select --fields name,email --engine duckdb --output subset.csv data.jsonl
undatum select --fields name --table Sheet2 workbook.xlsx
undatum select --fields name,capital_city.lat --flatten-nested nested.jsonl
```

### `split`

Splits datasets into multiple files based on chunk size or field values.

```bash
# Split by chunk size
undatum split --chunksize 10000 data.jsonl

# Split by field value
undatum split --fields category data.jsonl
undatum split workbook.xlsx --table Sheet2 --fields city --dirname out/
undatum split nested.jsonl --fields capital_city.lat --flatten-nested --dirname out/
```

### `validate`

Validates data against validation rules. Supports two modes: **rich validation with rule files** (recommended) and **legacy single-rule mode** (backward compatible).

#### Rich Validation with Rule Files

Use YAML/JSON rule files for comprehensive, reusable validation:

```bash
# Validate with rule file
undatum validate data.csv --rules validation-rules.yml
undatum validate workbook.xlsx --table Sheet2 --rules validation-rules.yml
undatum validate nested.jsonl --rules rules.yml --flatten-nested

# Filter by severity
undatum validate data.jsonl --rules rules.yml --severity error

# JSON output for CI/CD integration
undatum validate data.csv --rules rules.yml --output-format json

# Generate detailed violation report
undatum validate data.jsonl --rules rules.yml --violation-report violations.json

# Treat warnings as errors
undatum validate data.csv --rules rules.yml --fail-on-warnings
```

**Rule File Format:**

Rule files support field-level and cross-field validation with severity levels:

```yaml
rules:
  # Field-level rules
  - field: email
    name: Email Required
    description: Email field must be present
    required: true
    type: string
    format: email
    severity: error

  - field: age
    name: Age Range
    description: Age must be between 0 and 120
    type: number
    min: 0
    max: 120
    severity: warning

  - field: status
    name: Status Values
    type: string
    enum: [active, inactive, pending]
    severity: error

  # Cross-field validation
  - type: cross-field
    name: Date Range Validation
    description: End date must be after start date
    condition: "end_date >= start_date"
    fields: [start_date, end_date]
    severity: error
```

**Rule Types:**

- **Required**: `required: true` - Field must be present and non-empty
- **Type**: `type: string|number|integer|float|boolean` - Value type validation
- **Format**: `format: email|url|uuid` - Format validation
- **Range**: `min`, `max` for numbers; `min_length`, `max_length` for strings
- **Enum**: `enum: [value1, value2, ...]` - Whitelist validation
- **Pattern**: `pattern: 'regex'` - Regular expression validation
- **Custom**: `custom: 'rule_name'` - Use custom validation function from VALIDATION_RULEMAP
- **Cross-field**: `type: cross-field` with `condition` expression

**Severity Levels:**

- `error`: Hard errors that should block processing
- `warning`: Soft warnings that don't block processing
- `info`: Informational violations

**Violation Reporting:**

The validation command provides comprehensive reporting:

- **Summary statistics**: Total violations by severity, by field, by rule
- **Detailed violations**: Record-level violation details with context
- **JSON output**: Machine-readable format for CI/CD integration
- **Violation report file**: Detailed JSON report with all violations

**Example Rule Files:**

Example rule files are available in `examples/validation-rules/`:
- `basic-validation.yml` - Common field-level validation rules
- `cross-field-validation.yml` - Cross-field validation examples
- `complex-validation.yml` - Comprehensive validation scenario

#### Legacy Mode (Backward Compatible)

Simple single-rule validation for quick checks:

```bash
# Validate email addresses
undatum validate --rule common.email --fields email data.jsonl

# Validate Russian INN
undatum validate --rule ru.org.inn --fields VendorINN data.jsonl --mode stats

# Output invalid records
undatum validate --rule ru.org.inn --fields VendorINN data.jsonl --mode invalid
```

**Available built-in validation rules:**
- `common.email` - Email address validation
- `common.url` - URL validation
- `ru.org.inn` - Russian organization INN identifier
- `ru.org.ogrn` - Russian organization OGRN identifier
- `integer` - Integer validation

#### Validation Best Practices

1. **Use errors for critical issues**: Fields that must be correct for data processing
2. **Use warnings for data quality**: Issues that should be reviewed but don't block processing
3. **Organize rules by domain**: Group related rules in separate files (e.g., `user-validation.yml`, `order-validation.yml`)
4. **Version control rule files**: Track rule changes and share across teams
5. **Use cross-field rules sparingly**: They're more complex and slower to evaluate
6. **Test rules incrementally**: Start with basic rules, add complexity as needed

### `schema`

Generates data schemas from files. Supports multiple output formats including YAML, JSON, Cerberus, JSON Schema, Avro, and Parquet.

```bash
# Generate schema in default YAML format
undatum schema data.jsonl

# Generate schema in JSON Schema format
undatum schema data.jsonl --format jsonschema

# Generate schema in Avro format
undatum schema data.jsonl --format avro

# Generate schema in Parquet format
undatum schema data.jsonl --format parquet

# Generate Cerberus schema (for backward compatibility with deprecated `scheme` command)
undatum schema data.jsonl --format cerberus

# Save to file
undatum schema data.jsonl --output schema.yaml

# Nested JSONL: unfold dict fields onto dotted paths
undatum schema nested.jsonl --flatten-nested --format jsonschema
undatum schema nested.jsonl --flatten-nested --max-nested-depth 2
undatum schema nested.jsonl --flatten-nested --keep-nested-parents

# Named Excel sheet
undatum schema workbook.xlsx --table Sheet2

# Validate rows against the inferred schema (not rule-pack validation)
undatum schema data.jsonl --validate --outtype json
undatum schema data.jsonl --validate --strict
undatum schema data.jsonl --validate --sample-size 500

# Generate schema with AI-powered field documentation
undatum schema data.jsonl --autodoc --output schema.yaml
```

**Supported schema formats:**
- `yaml` (default) - YAML format with full schema details
- `json` - JSON format with full schema details
- `cerberus` - Cerberus validation schema format (for backward compatibility with deprecated `scheme` command)
- `jsonschema` - JSON Schema (W3C/IETF standard) - Use for API validation, OpenAPI specs, and tool integration
- `avro` - Apache Avro schema format - Use for Kafka message schemas and Hadoop data pipelines
- `parquet` - Parquet schema format - Use for data lake schemas and Parquet file metadata

**Use cases:**
- **JSON Schema**: API documentation, data validation in web applications, OpenAPI specifications
- **Avro**: Kafka message schemas, Hadoop ecosystem integration, schema registry compatibility
- **Parquet**: Data lake schemas, Parquet file metadata, analytics pipeline definitions
- **Cerberus**: Python data validation (`schema --format cerberus`; the legacy `scheme` command is deprecated)

**Examples:**

```bash
# Generate JSON Schema for API documentation
undatum schema api_data.jsonl --format jsonschema --output api_schema.json

# Generate Avro schema for Kafka
undatum schema events.jsonl --format avro --output events.avsc

# Generate Parquet schema for data lake
undatum schema data.csv --format parquet --output schema.json

# Generate Cerberus schema (deprecated, use schema command instead)
undatum schema data.jsonl --format cerberus --output validation_schema.json
```

**Note:** The `scheme` command is deprecated. Use `undatum schema --format cerberus` instead. The `scheme` command will show a deprecation warning but continues to work for backward compatibility.

### `schema_bulk`

Extracts schemas from multiple files at once using a glob pattern or directory path. Either extracts distinct unique schemas (`--mode distinct`, default) or one schema per file (`--mode perfile`).

```bash
# Distinct schemas across all CSV files in a directory
undatum schema_bulk "data/*.csv" --output schemas/

# One schema per file, JSON Schema format
undatum schema_bulk data/ --mode perfile --format jsonschema --output schemas/

# With AI-powered field documentation
undatum schema_bulk "data/*.jsonl" --autodoc --output schemas/
```

### `sql`

Run ad-hoc DuckDB SQL queries over data files (CSV, JSONL, Parquet, and other DuckDB-readable formats). A single input file can be referenced as the view `data`; every file is also registered as a view named after its file stem.

```bash
# Aggregate a CSV
undatum sql "SELECT city, COUNT(*) AS n FROM data GROUP BY city" cities.csv

# Join two files (views named after file stems: orders, users)
undatum sql "SELECT * FROM orders JOIN users USING (user_id)" orders.csv users.parquet

# Save the result as Parquet
undatum sql "SELECT * FROM data WHERE amount > 100" sales.jsonl --output big.parquet --format parquet
```

Output formats: `jsonl` (default), `csv`, `parquet` (requires `--output`). DuckDB resources can be tuned with `--duckdb-threads` and `--duckdb-memory`. This is the ad-hoc query command for files; `undatum db query` runs SQL against a database URI.

### `flatten`

Flattens nested data structures into key-value pairs.

```bash
undatum flatten data.jsonl
```

### `apply`

Applies a transformation script to each record in the file.

```bash
undatum apply --script transform.py data.jsonl output.jsonl
```

### `ingest`

Ingests data from files into databases. Supports MongoDB, PostgreSQL, DuckDB, MySQL, SQLite, and Elasticsearch with retry logic, progress tracking, and optional table auto-creation. For a simpler load syntax, see [`db load`](#db-query--db-load).

```bash
# Ingest to MongoDB
undatum ingest data.jsonl mongodb://localhost:27017 mydb mycollection
undatum ingest workbook.xlsx mongodb://localhost:27017 mydb cities --source-table Sheet2
undatum ingest nested.jsonl sqlite:///cities.db cities --dbtype sqlite --create-table --flatten-nested

# Ingest to PostgreSQL (append mode)
undatum ingest data.csv postgresql://user:pass@localhost:5432/mydb mytable --dbtype postgresql

# Ingest to PostgreSQL with auto-create table
undatum ingest data.jsonl postgresql://user:pass@localhost:5432/mydb mytable \
  --dbtype postgresql \
  --create-table

# Ingest to PostgreSQL with upsert (update on conflict)
undatum ingest data.jsonl postgresql://user:pass@localhost:5432/mydb mytable \
  --dbtype postgresql \
  --mode upsert \
  --upsert-key id

# Ingest to PostgreSQL (replace mode - truncates table first)
undatum ingest data.csv postgresql://user:pass@localhost:5432/mydb mytable \
  --dbtype postgresql \
  --mode replace

# Ingest to DuckDB (file database)
undatum ingest data.csv duckdb:///path/to/database.db mytable --dbtype duckdb

# Ingest to DuckDB (in-memory database)
undatum ingest data.jsonl duckdb:///:memory: mytable --dbtype duckdb

# Ingest to DuckDB with auto-create table
undatum ingest data.jsonl duckdb:///path/to/database.db mytable \
  --dbtype duckdb \
  --create-table

# Ingest to DuckDB with upsert
undatum ingest data.jsonl duckdb:///path/to/database.db mytable \
  --dbtype duckdb \
  --mode upsert \
  --upsert-key id

# Ingest to DuckDB with Appender API (streaming)
undatum ingest data.jsonl duckdb:///path/to/database.db mytable \
  --dbtype duckdb \
  --use-appender

# Ingest to MySQL
undatum ingest data.csv mysql://user:pass@localhost:3306/mydb mytable --dbtype mysql

# Ingest to MySQL with auto-create table
undatum ingest data.jsonl mysql://user:pass@localhost:3306/mydb mytable \
  --dbtype mysql \
  --create-table

# Ingest to MySQL with upsert
undatum ingest data.jsonl mysql://user:pass@localhost:3306/mydb mytable \
  --dbtype mysql \
  --mode upsert \
  --upsert-key id

# Ingest to SQLite (file database)
undatum ingest data.csv sqlite:///path/to/database.db mytable --dbtype sqlite

# Ingest to SQLite (in-memory database)
undatum ingest data.jsonl sqlite:///:memory: mytable --dbtype sqlite

# Ingest to SQLite with auto-create table
undatum ingest data.jsonl sqlite:///path/to/database.db mytable \
  --dbtype sqlite \
  --create-table

# Ingest to SQLite with upsert
undatum ingest data.jsonl sqlite:///path/to/database.db mytable \
  --dbtype sqlite \
  --mode upsert \
  --upsert-key id

# Ingest to Elasticsearch
undatum ingest data.jsonl https://elasticsearch:9200 myindex myindex --dbtype elasticsearch --api-key YOUR_API_KEY --doc-id id

# Ingest with options
undatum ingest data.csv mongodb://localhost:27017 mydb mycollection \
  --batch 5000 \
  --drop \
  --totals \
  --timeout 30 \
  --skip 100

# Ingest multiple files
undatum ingest "data/*.jsonl" mongodb://localhost:27017 mydb mycollection
```

**Key Features:**
- **Automatic retry**: Retries failed operations with exponential backoff (3 attempts)
- **Connection pooling**: Efficient connection management for all databases
- **Progress tracking**: Real-time progress bar with throughput (rows/second)
- **Error handling**: Continues processing after batch failures, logs detailed errors
- **Summary statistics**: Displays total rows, successful rows, failed rows, and throughput at completion
- **Connection validation**: Tests database connection before starting ingestion
- **PostgreSQL optimizations**: Uses COPY FROM for maximum performance (10-100x faster than INSERT)
- **Schema management**: Auto-create tables from data schema or validate existing schemas

**Options:**
- `--batch`: Batch size for ingestion (default: 1000, PostgreSQL recommended: 10000, DuckDB recommended: 50000, MySQL recommended: 10000, SQLite recommended: 5000)
- `--dbtype`: Database type: `mongodb` (default), `postgresql`, `postgres`, `duckdb`, `mysql`, `sqlite`, `elasticsearch`, or `elastic`
- `--drop`: Drop existing collection/table before ingestion (MongoDB, Elasticsearch)
- `--mode`: Ingestion mode for PostgreSQL/DuckDB/MySQL/SQLite: `append` (default), `replace`, or `upsert`
- `--create-table`: Auto-create table from data schema (PostgreSQL/DuckDB/MySQL/SQLite)
- `--upsert-key`: Field name(s) for conflict resolution in upsert mode (PostgreSQL/DuckDB/MySQL/SQLite, comma-separated for multiple keys)
- `--use-appender`: Use Appender API for DuckDB (streaming insertion, default: False)
- `--totals`: Show total record counts during ingestion (uses DuckDB for counting)
- `--timeout`: Connection timeout in seconds (positive values, default uses database defaults)
- `--skip`: Number of records to skip at the beginning
- `--api-key`: API key for database authentication (Elasticsearch)
- `--doc-id`: Field name to use as document ID (Elasticsearch, default: `id`)
- `--verbose`: Enable verbose logging output

**PostgreSQL-Specific Features:**
- **COPY FROM**: Fastest bulk loading method (100,000+ rows/second)
- **Upsert support**: `INSERT ... ON CONFLICT` for idempotent ingestion
- **Schema auto-creation**: Automatically creates tables with inferred types
- **Connection pooling**: Efficient connection reuse
- **Transaction management**: Atomic batch operations

**DuckDB-Specific Features:**
- **Fast batch inserts**: Optimized executemany for high throughput (200,000+ rows/second)
- **Appender API**: Streaming insertion for real-time data ingestion
- **Upsert support**: `INSERT ... ON CONFLICT` for idempotent ingestion
- **Schema auto-creation**: Automatically creates tables with inferred types
- **File and in-memory**: Supports both file-based and in-memory databases
- **No server required**: Embedded database, no separate server needed
- **Analytical database**: Optimized for analytical workloads and OLAP queries

**MySQL-Specific Features:**
- **Multi-row INSERT**: Efficient batch operations (10,000+ rows/second)
- **Upsert support**: `INSERT ... ON DUPLICATE KEY UPDATE` for idempotent ingestion
- **Schema auto-creation**: Automatically creates tables with inferred types
- **Connection management**: Efficient connection handling
- **Transaction support**: Atomic batch operations

**SQLite-Specific Features:**
- **PRAGMA optimizations**: Automatic performance tuning (synchronous=OFF, journal_mode=WAL)
- **Fast batch inserts**: Optimized executemany (10,000+ rows/second)
- **Upsert support**: `INSERT ... ON CONFLICT` for idempotent ingestion (SQLite 3.24+)
- **Schema auto-creation**: Automatically creates tables with inferred types
- **File and in-memory**: Supports both file-based and in-memory databases
- **No server required**: Embedded database, no separate server needed
- **Built-in**: Uses Python's built-in sqlite3 module, no dependencies required

**Error Handling:**
- Transient failures (connection timeouts, network errors) are automatically retried
- Partial batch failures are logged but don't stop ingestion
- Failed records are tracked and reported in the summary
- Detailed error messages help identify problematic data

**Performance:**
- Batch processing for efficient ingestion
- Connection pooling reduces overhead
- Progress tracking shows real-time throughput
- Optimized for large files with streaming support

**Example Output:**
```
Ingesting data.jsonl to mongodb://localhost:27017 with db mydb table mycollection
Ingesting to mongodb: 100%|████████████| 10000/10000 [00:05<00:00, 2000 rows/s]

Ingestion Summary:
  Total rows processed: 10000
  Successful rows: 10000
  Failed rows: 0
  Batches processed: 10
  Time elapsed: 5.00 seconds
  Average throughput: 2000 rows/second
```

### `db query` / `db load`

Database query, load, and dump commands for working with databases as first-class data sources and sinks. See also [`db dump`](#db-dump) below.

#### `db query`

Execute SQL queries against databases and output results in multiple formats.

```bash
# Query PostgreSQL and output JSONL
undatum db query "SELECT * FROM users LIMIT 100" --db postgresql://user:pass@host/db

# Query MySQL and save to file
undatum db query "SELECT name, email FROM customers WHERE status='active'" \
  --db mysql://user:pass@host:3306/mydb \
  --output results.jsonl

# Query SQLite and output CSV
undatum db query "SELECT * FROM data" --db sqlite:///path/to/db.db --output-format csv

# Query from SQL file
undatum db query --query-file query.sql --db postgresql://user:pass@host/db --output results.jsonl

# Output Parquet format
undatum db query "SELECT * FROM large_table" --db postgresql://... --output-format parquet --output data.parquet
```

**Supported Databases:**

| Engine | URI scheme | Notes |
|--------|------------|-------|
| PostgreSQL | `postgresql://user:pass@host:port/db` | Native driver |
| MySQL / MariaDB | `mysql://user:pass@host:port/db` | Native driver |
| SQLite | `sqlite:///path/to/db.db`, `sqlite:///:memory:` | Native driver |
| MS SQL Server | `mssql://`, `sqlserver://` | Via iterabledata; `pip install "undatum[mssql]"` |
| ClickHouse | `clickhouse://user:pass@host:9000/db` | Via iterabledata; `pip install "undatum[clickhouse]"` |
| MongoDB | `mongodb://host:27017/db?collection=name&limit=N` | Read-only; pass collection/limit in URI query string |
| Elasticsearch / OpenSearch | `elasticsearch://`, `opensearch://` | Read-only; pass `index=` in URI query string |

```bash
# ClickHouse
undatum db query "SELECT * FROM events LIMIT 100" --db clickhouse://user:pass@host:9000/db

# MongoDB collection (empty SQL argument; collection in URI)
undatum db query "" --db "mongodb://host:27017/mydb?collection=users&limit=100"

# Elasticsearch index
undatum db query "" --db "elasticsearch://host:9200?index=logs&limit=100"
```

**Output Formats:**
- `jsonl` (default) - JSON Lines format, one record per line
- `csv` - Comma-separated values format
- `parquet` - Parquet format (requires pandas and pyarrow)

**Features:**
- **Streaming support**: Results are streamed in batches for efficient memory usage
- **Large result sets**: Handles queries returning millions of rows
- **Server-side cursors**: Uses PostgreSQL named cursors for optimal performance
- **Column inference**: Automatically detects column names from query results

#### `db load`

Simplified interface for loading data files into databases. A convenience wrapper around the `ingest` command with cleaner syntax.

```bash
# Load data to PostgreSQL (append mode)
undatum db load data.parquet --db postgresql://user:pass@host/db --table users

# Load with replace mode
undatum db load data.csv --db mysql://user:pass@host:3306/mydb --table customers --mode replace

# Load with upsert
undatum db load data.jsonl --db postgresql://user:pass@host/db --table orders --mode upsert --upsert-key id

# Auto-create table from schema
undatum db load data.parquet --db sqlite:///db.db --table new_table --create-table
undatum db load workbook.xlsx --db sqlite:///db.db --table cities --source-table Sheet2 --create-table
undatum db load quoted.csv --db sqlite:///db.db --table people --create-table --quotechar "'"
undatum db load nested.jsonl --db sqlite:///db.db --table cities --create-table --flatten-nested
```

**Supported Databases:**
- PostgreSQL
- MySQL/MariaDB
- SQLite
- (Also supports DuckDB, MongoDB, Elasticsearch via underlying ingest command)

**Load Modes:**
- `append` (default) - Add records to existing table
- `replace` - Replace all data in table
- `upsert` - Update existing records or insert new ones (requires `--upsert-key`)

**Comparison with `ingest`:**

The `db load` command provides a simplified interface compared to `ingest`:
- Cleaner syntax: `db load file --db uri --table name` vs `ingest file uri db table --dbtype ...`
- Automatic database type detection from URI
- Focused on common use cases (append, replace, upsert)

Use `ingest` for:
- Advanced options (batch size, timeout, connection pooling)
- MongoDB and Elasticsearch (not yet supported by `db load`)
- Multiple file patterns
- Fine-grained control over ingestion process

**Database URI Formats:**

- **PostgreSQL**: `postgresql://user:password@host:port/database`
- **MySQL**: `mysql://user:password@host:port/database`
- **SQLite**: `sqlite:///path/to/db.db` or `sqlite:///:memory:`

#### `db dump`

Dump a database table or query result to a file. Results are streamed in batches for efficient memory usage; prefer Parquet for large dumps.

```bash
# Dump a whole table to Parquet
undatum db dump --db sqlite:///data.db --table users --output users.parquet

# Dump a query result to CSV
undatum db dump --db postgresql://user:pass@host/db --query "SELECT * FROM events" \
  --output events.csv --to csv

# Tune streaming batch size
undatum db dump --db mysql://user:pass@host:3306/mydb --table orders \
  --output orders.jsonl --to jsonl --batch-size 50000
```

**Options:**
- `--db` (required) - Database connection URI (same schemes as `db query`)
- `--output` (required) - Output file path
- `--table` - Table name to dump (alternative to `--query`)
- `--query` - SQL query to dump (alternative to `--table`)
- `--to` - Output format: `parquet` (default), `csv`, or `jsonl`
- `--batch-size` - Batch size for streaming results (default: 10000)

### `plot`

Generate data visualizations from data files. Supports histograms, bar charts, scatter plots, and line plots for quick data exploration.

```bash
# Generate histogram for numerical field
undatum plot data.csv --field age --type histogram --output age_dist.png

# Generate bar chart for categorical field
undatum plot data.csv --field status --type bar

# Generate scatter plot for two fields
undatum plot data.csv --field x,y --type scatter --output scatter.png

# Generate line plot
undatum plot data.csv --field value --type line --output trend.png

# Auto-detect plot type based on field type
undatum plot data.csv --field age --output age_plot.png

# Multiple fields in subplots
undatum plot data.csv --field age,income,score --type histogram --output distributions.png

# Customize plot appearance
undatum plot data.csv --field age --title "Age Distribution" \
  --xlabel "Age (years)" --ylabel "Frequency" \
  --width 12 --height 8 --dpi 150 --output age_plot.png

# Filter before plotting, keep the top categories
undatum plot data.csv --field city --type bar --filter '`status` == "active"' \
  --top-n 10 --output cities.png

# Bar chart of summed amounts by category
undatum plot data.csv --field city --type bar --aggregate sum --value-field amount \
  --output totals.png
undatum plot workbook.xlsx --table Sheet2 --field city --type bar --output cities.png
undatum plot nested.jsonl --field capital_city.lat --flatten-nested --type histogram --output lats.png
```

**Plot Types:**
- `histogram` - Distribution of numerical values (default for numerical fields)
- `bar` - Frequency of categorical values (default for categorical fields)
- `scatter` - Relationship between two numerical fields
- `line` - Time series or sequential data
- `auto` - Auto-detect based on field type (default)

**Output Formats:**
- PNG (default) - Raster image format
- SVG - Vector image format
- PDF - Print-ready document format

**Features:**
- **Auto-detection**: Automatically suggests appropriate plot type based on field data type
- **Multiple fields**: Generate multiple subplots for multiple fields
- **Customizable**: Control titles, labels, colors, size, and resolution
- **Multiple formats**: Save as PNG, SVG, or PDF
- **Display mode**: Show plot interactively if no output file specified

**Options:**
- `--field`: Field name(s) to plot (comma-separated for multiple)
- `--type`: Plot type (`histogram`, `bar`, `scatter`, `line`, or `auto`)
- `--output`: Output file path (if not specified, displays plot)
- `--format`: Output format (`png`, `svg`, or `pdf`)
- `--title`: Plot title
- `--xlabel`: X-axis label
- `--ylabel`: Y-axis label
- `--width`: Figure width in inches (default: 10)
- `--height`: Figure height in inches (default: 6)
- `--dpi`: Resolution for raster formats (default: 100)
- `--color`: Color scheme name (matplotlib colormap)
- `--style`: Matplotlib style name (e.g. `ggplot`)
- `--filter`: Filter expression applied before plotting
- `--aggregate`: Bar-chart aggregation (`count`, `sum`, `mean`, or `none`)
- `--value-field`: Numeric field to sum/mean when `--aggregate` is `sum` or `mean`
- `--top-n`: Keep the top N aggregated groups for bar charts

**Requirements:**
- Install the plot extra: `pip install "undatum[plot]"` (includes matplotlib)

### `examples`

Manage and execute example recipes for common data processing tasks. Provides a library of copy-paste ready recipes that demonstrate best practices.

```bash
# List all available recipes
undatum examples list

# List recipes by category
undatum examples list --category conversion

# Show recipe details
undatum examples show csv-to-jsonl

# Run a recipe with variables
undatum examples run csv-to-jsonl --var input=data.csv --var output=data.jsonl

# Preview commands without executing
undatum examples run data-validation --var input=data.jsonl --var rules=rules.yml --dry-run

# Interactive mode (prompt for variables)
undatum examples run database-query-export --interactive
```

**Recipe Categories:**
- **conversion** - Data format conversion recipes
- **validation** - Data validation and quality checks
- **database** - Database query and load operations
- **analysis** - Data profiling and analysis
- **transformation** - Data cleaning and transformation

**Available Recipes:**
- `csv-to-jsonl` — Convert CSV to JSONL format
- `data-validation` — Validate data using validation rules
- `database-query-export` — Query database and export results
- `data-profiling` — Profile dataset with statistics and documentation
- `data-cleaning` — Clean data by removing duplicates and filling missing values
- `api-serve-data` — Discover and serve a file-backed Data API

**Recipe Format:**

Recipes ship inside the package under `undatum/recipes/` (also mirrored in the repo at `examples/recipes/`):

```yaml
name: recipe-name
description: Recipe description
category: category-name
tags:
  - tag1
  - tag2

variables:
  input:
    description: Input file path
    required: true
  output:
    description: Output file path
    default: "output.jsonl"

commands:
  - description: Command description
    command: undatum convert ${input} ${output}

example: |
  undatum examples run recipe-name --var input=data.csv
```

**Features:**
- **Variable substitution**: Use `${variable}` or `$variable` in commands
- **Dry-run mode**: Preview commands before execution
- **Interactive mode**: Prompt for variable values
- **Category filtering**: Filter recipes by category or tag
- **Copy-paste ready**: Recipes are executable commands

### `plugins`

Manage and discover plugins that extend undatum functionality. Plugins can add custom commands, IO connectors, and transforms.

```bash
# List all installed plugins
undatum plugins list

# Show plugin information
undatum plugins info my-plugin

# Validate loaded plugins
undatum plugins validate
```

Connector plugins are consulted for non-`s3://` URIs in the I/O path. Transform plugins can be applied with `undatum apply data.jsonl --plugin my-transform`. Examples live in `examples/plugins/`.

**Plugin Types:**
- **Command plugins**: Add custom CLI commands
- **Connector plugins**: Add support for custom URI schemes and data sources
- **Transform plugins**: Add custom data transformation functions

**Creating Plugins:**

Plugins are Python packages that register with undatum via entry points. Example plugin:

```python
# setup.py or pyproject.toml
[project.entry-points."undatum.plugins"]
my-plugin = "mypackage.plugin:register"

# plugin.py
from undatum.plugins.base import CommandPlugin, Plugin
import typer

def register(undatum_app):
    return MyPlugin(undatum_app)

class MyPlugin(CommandPlugin):
    def __init__(self, app):
        super().__init__("my-plugin", "1.0.0", "My custom plugin")
        self.app = app
    
    def register_commands(self, app):
        @app.command()
        def my_command(input_file: str):
            """My custom command."""
            # Command implementation
            pass
```

**Plugin Discovery:**

Plugins are automatically discovered from installed packages via the `undatum.plugins` entry point group. No configuration needed - just install the plugin package and undatum will find it.

### `formats`

Inspect the iterabledata format catalog (140+ formats). The list reflects installed optional dependencies and runtime capabilities on your machine.

```bash
# All formats with read/write flags
undatum formats list

# Writable outputs only (useful before choosing a conversion target)
undatum formats list --writable

# Read-only inputs
undatum formats list --read-only

# Full capability matrix (bulk, streaming, totals, tables, nested, maturity, native bulk)
undatum formats list --capabilities

# Named tables/sheets in a workbook or database
undatum formats tables workbook.xlsx
undatum formats tables data.sqlite --json

# Single format (aliases, extra, memory, selection, codecs)
undatum formats describe parquet
undatum formats describe geojson --json

# Export catalog for tooling or CI checks
undatum formats export --output formats.json
```

### `pipeline`

Run and validate multi-step YAML/JSON workflows. Each step invokes an undatum command. See [Pipeline Workflows](#pipeline-workflows) for the full DSL, templates, and examples.

```bash
# Validate before running
undatum pipeline validate my-pipeline.yml

# Document the step graph as Markdown + Mermaid
undatum pipeline doc my-pipeline.yml --output pipeline.md

# Execute with variable overrides
undatum pipeline run my-pipeline.yml --var input=data.csv --var output=out/

# Dry-run (print resolved steps without executing)
undatum pipeline run my-pipeline.yml --dry-run

# List built-in templates and scaffold a new pipeline
undatum pipeline templates list
undatum pipeline templates init basic-cleaning --output pipeline.yml
```

Built-in templates: `basic-cleaning`, `data-quality`, `profile-dataset`, `s3-etl`.

### `mcp`

Expose undatum operations to MCP-compatible agents (Cursor, Claude Desktop, etc.) over stdio. Requires `pip install "undatum[mcp]"`. See also [AI Agent Tools and MCP Server](#ai-agent-tools-and-mcp-server).

```bash
# List tools exposed to agents
undatum mcp tools

# Start the stdio MCP server
undatum mcp serve

# Standalone entry point (equivalent)
undatum-mcp
```

## Cloud Storage Support

undatum reads and writes cloud object storage URIs natively through iterabledata. Install the appropriate extra before using cloud paths in commands like `convert`, `stats`, `count`, `mask`, and the Python SDK.

```bash
# AWS S3 only
pip install "undatum[s3]"

# S3 + Google Cloud Storage + Azure Blob (recommended for multi-cloud)
pip install "undatum[cloud]"
```

### Supported URI schemes

| Provider | URI examples | Credential setup |
|----------|--------------|------------------|
| **AWS S3** | `s3://bucket/path`, `s3a://bucket/path` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, `AWS_REGION`, or `~/.aws/credentials` |
| **Google Cloud Storage** | `gs://bucket/path`, `gcs://bucket/path` | Application Default Credentials, `GOOGLE_APPLICATION_CREDENTIALS`, or gcloud user credentials |
| **Azure Blob / ADLS** | `az://container/path`, `abfs://container/path`, `abfss://container/path` | `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, or Azure identity chain via `adlfs` |

### Usage examples

```bash
# Read from cloud storage
undatum stats gs://my-bucket/data.csv
undatum count s3://my-bucket/data.jsonl

# Write to cloud storage
undatum convert local.csv s3://my-bucket/output.parquet
undatum convert data.csv gs://my-bucket/output.parquet

# Cloud-to-cloud conversion
undatum convert s3://bucket/input.jsonl gs://bucket/output.parquet
undatum mask s3://bucket/data.csv --fields email --method hash az://container/masked.csv
```

**Supported commands:** Any command that accepts file paths — including `convert`, `stats`, `count`, `analyze`, `select`, `validate`, `ingest`, `mask`, and SDK `Dataset.read()` / `write()`.

**Notes:**
- Cloud I/O is streaming-aware; large files do not need to be downloaded manually first.
- Local-only options such as `--atomic` apply to local output paths only.
- For S3-only workflows, `undatum[s3]` is sufficient; use `undatum[cloud]` when you need GCS or Azure.

## Python SDK

Undatum provides a Python SDK for programmatic data processing with a fluent API that mirrors CLI commands.

### Quick Start

```python
from undatum import Dataset

# Read data
ds = Dataset.read("data.jsonl")
ds = Dataset.read("workbook.xlsx", table="Sheet2")
ds = Dataset.read("nested.jsonl", flatten_nested=True)

# Chain transformations
ds = ds.fill("age", value=0).dedup(keys=["user_id"]).sort("name")

# Compute statistics (unfold nested dict fields onto dotted paths)
stats = ds.stats()
stats = Dataset.read("nested.jsonl").stats(flatten_nested=True)

# Write output
ds.write("output.parquet")

# Bulk-convert a directory or glob (same as convert --recursive)
Dataset.convert_many("./raw", "./out", to_ext="jsonl")
Dataset.convert_many(
    "./raw",
    "./out",
    to_ext="jsonl",
    filename_pattern="{stem}.converted.jsonl",
)
```

### Transform Methods

```python
# Fill missing values
ds = ds.fill("age", value=0)
ds = ds.fill(["name", "email"], value="N/A")
ds = ds.fill("status", strategy="forward")

# Remove duplicates
ds = ds.dedup()  # By all fields
ds = ds.dedup(keys=["user_id", "email"])
ds = ds.dedup(keys=["id"], keep="last")

# Sort data
ds = ds.sort("name")
ds = ds.sort(["date", "price"], desc=True)
ds = ds.sort("age", numeric=True)

# Filter rows
ds = ds.filter(pattern="error|warning")
ds = ds.filter(pattern="active", fields=["status"])
ds = ds.filter(query="`price` > 100")

# Select fields
ds = ds.select(["name", "email"])
ds = ds.select("user_id", filter_expr="`status` == 'active'")

# Join datasets
ds1 = Dataset.read("users.jsonl")
ds2 = Dataset.read("orders.jsonl")
ds = ds1.join(ds2, keys=["user_id"], join_type="left")

# Sample data
ds = ds.sample(n=1000)
ds = ds.sample(percent=10.0)

# Mask sensitive fields
ds = ds.mask(["email", "phone"], method="redact")
ds = ds.mask("user_id", method="hash", salt="my-salt")
```

### Analysis Methods

```python
# Compute statistics
stats = ds.stats(checkdates=True, engine="duckdb")

# Count rows
n = ds.count()

# Get first/last rows
rows = ds.head(20)
rows = ds.tail(20)

# Generate a Frictionless Data Package descriptor
result = ds.package(output="datapackage.json")
result = ds.package(output="datapackage.json", package_dir="out/package", autodoc=True)
```

### DataFrame and Typed-Row Interop

Datasets can be handed off to DataFrame libraries or iterated as typed objects,
delegating to iterabledata's adapters:

```python
# DataFrame conversion (pandas is bundled; Polars/Dask via extras)
df = Dataset.read("data.jsonl").to_pandas()
pdf = Dataset.read("data.parquet").to_polars()   # pip install "undatum[polars]"
ddf = Dataset.read("big.jsonl").to_dask()        # pip install "undatum[dask]"

# Chunked pandas frames for large files
for chunk in Dataset.read("big.csv").to_pandas(chunksize=100_000):
    ...

# Typed iteration
from dataclasses import dataclass

@dataclass
class Person:
    name: str
    age: int

for person in Dataset.read("people.csv").as_dataclasses(Person):
    print(person.name)

from pydantic import BaseModel

class PersonModel(BaseModel):
    name: str
    age: int

for person in Dataset.read("people.csv").as_pydantic(PersonModel):
    print(person.age)
```

### Cloud Storage

```python
# AWS S3
ds = Dataset.read("s3://bucket/data.jsonl")
ds.write("s3://bucket/output.parquet")

# Google Cloud Storage
ds = Dataset.read("gs://bucket/data.csv")
ds.write("gs://bucket/output.parquet")

# Azure Blob Storage
ds = Dataset.read("az://container/data.jsonl")
ds.write("az://container/output.parquet")

# Chain transforms on cloud input
ds = Dataset.read("s3://bucket/input.csv")
ds = ds.fill("age", value=0).dedup(keys=["id"])
ds.write("gs://bucket/output.jsonl")
```

Install `pip install "undatum[cloud]"` (or `undatum[s3]` for S3 only). See [Cloud Storage Support](#cloud-storage-support) for credential setup.

### Method Chaining

All transform methods return new Dataset instances, enabling fluent pipelines:

```python
ds = (Dataset.read("data.jsonl")
      .fill("age", value=0)
      .dedup(keys=["user_id"])
      .sort("date", desc=True)
      .filter(query="`status` == 'active'")
      .select(["name", "email", "age"])
      .sample(n=1000))
ds.write("output.parquet")
```

## AI Agent Tools and MCP Server

undatum exposes its operations to LLM agents through a JSON tool layer that builds
on iterabledata's foundation tools and adds undatum-specific tools (ad-hoc DuckDB
SQL, value frequency, and confirm-gated dedup/mask/sample).

### JSON tools and function-calling schemas

```python
from undatum import tools
from undatum.tools import schemas

# Call a tool directly (returns {"ok": ..., "data"/"error": ...})
result = tools.detect_format("data.csv")
freq = tools.frequency("data.csv", "country")
freq = tools.frequency("nested.jsonl", "capital_city.lat", flatten_nested=True)

# Dispatch by name (handy for agent runtimes)
schemas.call_tool("query_sql", {"path": "data.parquet", "query": "SELECT * FROM data LIMIT 5"})

# Export schemas for LLM function calling
openai_fns = schemas.to_openai_functions()
anthropic_tools = schemas.to_anthropic_tools()
```

Write tools (`deduplicate`, `mask_fields`, `sample_data`) require `confirm=True`
to prevent accidental writes. Pass `flatten_nested=True` to unfold nested fields
onto dotted paths (same as `--flatten-nested` on the CLI).

### LangChain

```python
from undatum.tools.langchain import get_tools  # pip install "undatum[langchain]"

lc_tools = get_tools()  # list[StructuredTool]
```

### MCP server

Expose the tools to MCP-compatible agents (Claude Desktop, Cursor, etc.) over stdio:

```bash
pip install "undatum[mcp]"

# List the tools the server exposes
undatum mcp tools

# Run the stdio server (wire this command into your MCP client)
undatum mcp serve

# Standalone console entry point (equivalent)
undatum-mcp
```

## Pipeline Workflows

Undatum supports declarative pipeline workflows defined in YAML or JSON files. This enables version-controlled, repeatable data processing workflows.

### Quick Start

```bash
# Validate pipeline before running
undatum pipeline validate pipeline.yml

# Run pipeline
undatum pipeline run pipeline.yml

# Run with variable overrides
undatum pipeline run pipeline.yml --var input_bucket=my-bucket --var output_dir=/tmp
```

### Pipeline Specification Format

Pipeline files define a series of steps, each executing an undatum command.
`tui` and `web` are interactive sessions and are not valid pipeline steps.

```yaml
variables:
  input_bucket: ${AWS_S3_BUCKET}
  output_dir: /tmp/output

steps:
  - name: load_data
    command: convert
    args:
      input: s3://${input_bucket}/raw.ndjson
      output: ${output_dir}/data.parquet
      format_out: parquet
  
  - name: clean_data
    command: fill
    args:
      input: ${output_dir}/data.parquet
      output: ${output_dir}/data_cleaned.parquet
      fields: age
      value: 0
  
  - name: remove_duplicates
    command: dedup
    args:
      input: ${output_dir}/data_cleaned.parquet
      output: ${output_dir}/data_final.parquet
      keys: user_id
  
  - name: generate_stats
    command: stats
    args:
      input: ${output_dir}/data_final.parquet

  - name: publish_package
    command: package
    args:
      subcommand: create
      input: ${output_dir}/data_final.parquet
      output: ${output_dir}/datapackage.json
      package_dir: ${output_dir}/package
```

### Variable Substitution

Pipelines support variable substitution using `${VAR}` syntax:

- **Environment variables**: Automatically available (e.g., `${HOME}`, `${AWS_S3_BUCKET}`)
- **Pipeline variables**: Defined in `variables` section
- **CLI overrides**: Passed via `--var key=value` (highest precedence)

```bash
# Use environment variable
export AWS_S3_BUCKET=my-bucket
undatum pipeline run pipeline.yml

# Override via CLI
undatum pipeline run pipeline.yml --var output_dir=/custom/path
```

### Step Dependencies

Steps automatically use outputs from previous steps as inputs. If a step doesn't specify an output, a temporary file is created and passed to the next step.

```yaml
steps:
  - name: step1
    command: convert
    args:
      input: input.csv
      output: /tmp/step1.jsonl  # Explicit output
  
  - name: step2
    command: sort
    args:
      input: /tmp/step1.jsonl  # Uses step1 output
      output: /tmp/step2.jsonl
  
  - name: step3
    command: dedup
    args:
      input: /tmp/step2.jsonl  # Uses step2 output
      # No output specified - creates temp file
```

### Common Pipeline Patterns

**Data Cleaning Pipeline:**
```yaml
steps:
  - name: convert
    command: convert
    args:
      input: raw_data.xml
      output: /tmp/data.jsonl
      tagname: item
  
  - name: fill_missing
    command: fill
    args:
      input: /tmp/data.jsonl
      output: /tmp/data_filled.jsonl
      fields: age,status
      value: "N/A"
  
  - name: deduplicate
    command: dedup
    args:
      input: /tmp/data_filled.jsonl
      output: /tmp/data_clean.jsonl
      keys: user_id
  
  - name: mask_pii
    command: mask
    args:
      input: /tmp/data_clean.jsonl
      output: /tmp/data_anonymized.jsonl
      fields: email,phone
      method: hash
```

**Data Analysis Pipeline:**
```yaml
steps:
  - name: sample
    command: sample
    args:
      input: large_dataset.csv
      output: /tmp/sample.csv
      n: 10000
  
  - name: compute_stats
    command: stats
    args:
      input: /tmp/sample.csv
  
  - name: frequency_analysis
    command: frequency
    args:
      input: /tmp/sample.csv
      fields: category,status
```

**S3 Data Pipeline:**
```yaml
variables:
  bucket: ${AWS_S3_BUCKET}
  region: us-east-1

steps:
  - name: download_and_convert
    command: convert
    args:
      input: s3://${bucket}/raw/data.jsonl
      output: s3://${bucket}/processed/data.parquet
      format_out: parquet
  
  - name: mask_sensitive
    command: mask
    args:
      input: s3://${bucket}/processed/data.parquet
      output: s3://${bucket}/anonymized/data.parquet
      fields: email,ssn
      method: hash
```

### Pipeline Validation

Always validate pipelines before running:

```bash
# Validate syntax and commands
undatum pipeline validate pipeline.yml

# Dry run (validate without executing)
undatum pipeline run pipeline.yml --dry-run
```

Validation checks:
- Valid YAML/JSON syntax
- All steps have required fields (name, command, args)
- All commands are valid undatum commands
- Variable references are properly formatted

### Pipeline Best Practices

1. **Use variables for flexibility**: Define paths and configuration in the `variables` section
2. **Name steps descriptively**: Use clear, action-oriented names (e.g., `clean_data`, `mask_pii`)
3. **Validate before running**: Always run `pipeline validate` before execution
4. **Version control pipelines**: Store pipeline files in version control for reproducibility
5. **Use explicit outputs**: Specify output paths for important intermediate results
6. **Handle errors**: Pipelines stop on first error; design steps to fail fast

### Pipeline Templates

Undatum provides reusable pipeline templates for common workflows. Use templates to quickly bootstrap pipelines:

```bash
# List available templates
undatum pipeline templates list

# Initialize a template interactively
undatum pipeline templates init basic-cleaning

# Initialize with variables (non-interactive)
undatum pipeline templates init profile-dataset \
  --var input_file=data.csv \
  --var output_dir=./analysis \
  --non-interactive
```

**Available Templates:**
- `basic-cleaning` - Clean CSV/JSONL data (fill missing values, remove duplicates)
- `profile-dataset` - Profile dataset with sampling, statistics, and documentation
- `s3-etl` - Cloud ETL workflow (`s3://`, `gs://`, or local paths: convert, process, upload)
- `data-quality` - Data quality checks and validation

**Template Features:**
- Interactive variable prompts
- Variable defaults and validation
- Customizable workflows
- Best practices built-in

### Example Pipeline Files

Example pipelines are available in `examples/pipelines/`:
- `data-cleaning.yml` - Basic data cleaning workflow
- `s3-processing.yml` - Cloud data processing with S3
- `data-analysis.yml` - Data exploration and analysis
- `etl-pipeline.yml` - Complete ETL workflow

Run examples:
```bash
# Copy and customize an example
cp examples/pipelines/data-cleaning.yml my-pipeline.yml

# Or use a template
undatum pipeline templates init basic-cleaning --var input_file=data.csv

# Validate and run
undatum pipeline validate my-pipeline.yml
undatum pipeline run my-pipeline.yml
```

## Advanced Usage

### Working with Compressed Files

undatum can process files inside compressed containers (ZIP, GZ, BZ2, XZ, ZSTD) with minimal memory usage.

```bash
# Process file inside ZIP archive
undatum headers --format-in jsonl data.zip

# Process XZ compressed file
undatum uniq --fields country --format-in jsonl data.jsonl.xz
```

### Filtering Data

Filter rows with comparison expressions on commands that support `--filter` (`select`, `frequency`, `uniq`, `plot`, `validate`, `split`, and others). The same expression is pushed to DuckDB `WHERE` when possible, or evaluated in-process on the iterable path. For `LIKE`, `IN`, joins, and aggregations, use `undatum sql`.

```bash
# Filter by field value
undatum select --fields name,email --filter '`status` == "active"' data.jsonl

# Complex filters (AND/OR and &&/|| are both accepted)
undatum frequency --fields category --filter '`price` > 100 && `status` == "active"' data.jsonl

# DuckDB-accelerated select with SQL pushdown
undatum select --fields name,email --filter '`status` == "active"' --engine duckdb data.jsonl

# Unique values after a filter
undatum uniq --fields city --filter 'age >= 30' --engine duckdb data.jsonl

# Natural-language filter (translates to an expression; use --apply to run)
undatum ai filter data.csv "customers in California with orders over 1000" --apply
```

**Filter syntax:**
- Field names: `` `fieldname` `` (backticks optional for simple names)
- Strings: `"value"` or `'value'`
- Comparisons: `==`, `!=`, `>`, `<`, `>=`, `<=`
- Booleans: `AND` / `OR` or `&&` / `||` (both work)

**DuckDB pushdown:** comparisons, `AND`/`OR`/`&&`/`||`, parentheses, and simple identifiers are translated to `WHERE`. The following are not supported on `--filter` (use `undatum sql` instead):
- `IN` lists and SQL `LIKE`
- Nested dotted fields (`user.name`)
- Regex / `match`

**Migrating from older filters:** `AND`/`OR` and `&&`/`||` both work. Prefer double-quoted strings. There is no `IN` operator; write `status == "active" || status == "pending"`. The experimental MistQL `undatum query` command is gone; use [`sql`](#sql) or `select --filter`.

For ad-hoc SQL over files, use [`sql`](#sql) or [`db query`](#db-query--db-load) against a database URI.

### Custom Encoding and Delimiters

CSV/TSV delimiters (comma, semicolon, tab, pipe) and encoding are **auto-detected** when `--delimiter` / `--encoding` are omitted on supported commands (`convert`, `analyze`, `select`, `doc`, `package`, and shared read paths).

Override when needed:

```bash
undatum headers --encoding cp1251 --delimiter ";" data.csv
undatum convert --encoding utf-8 --delimiter "," data.csv data.jsonl
```

### Date Detection

Automatic date/datetime field detection:

```bash
undatum stats --checkdates data.jsonl
```

This uses the `qddate` library to automatically identify and parse date fields.

## Data Formats

undatum supports **140+ formats** through iterabledata (exact catalog depends on the iterabledata version and optional extras). Format detection is automatic from file extensions and content; override with `--format-in` / `--format-out` when needed. Run `undatum formats list` for the authoritative catalog on your installation. Prefer **iterabledata ≥ 1.0.18** on Python 3.10+ for lakehouse writes and the open-data format pack — see [`docs/FORMAT_SUPPORT.md`](docs/FORMAT_SUPPORT.md).

### Core tabular formats

| Format | Extensions / ids | Notes |
|--------|------------------|-------|
| **CSV / TSV** | `.csv`, `.tsv` (`csv`, alias `tsv`) | Delimiter and encoding auto-detected |
| **JSON Lines** | `.jsonl`, `.ndjson` (`jsonl`, alias `ndjson`) | One JSON object per line; ideal for streaming |
| **JSON** | `.json` | Array or object documents |
| **Parquet / ORC / Avro** | `.parquet`, `.orc`, `.avro` | Columnar and binary row formats; Avro is writable (iterabledata 1.0.14+) |
| **Arrow / Feather** | `.arrow`, `.feather` | Bounded batch I/O; native batch convert path available |
| **Excel** | `.xls`, `.xlsx`, `.xlsb`, `.ods` | Named sheet via `--table` / `--sheet`; `--start-page` for a 0-based index |
| **BSON** | `.bson` | Binary JSON (MongoDB) |
| **DuckDB / SQLite** | `.ddb`, `.duckdb`, `.sqlite`, `.db` | Table name defaults from output filename when omitted |

### Structured, geospatial, scientific, and lakehouse

- **XML** — convert with `--tagname` to specify the record element (XXE-hardened parsers in iterabledata 1.0.16+)
- **YAML / TOML / INI** — config and metadata formats (`yml`, `toml`, `ini`)
- **Geospatial** — `geojson`, `geojsonseq`, `geoparquet`, `fgb`, `gpx`, `shp`, `gpkg`, `kml`, FileGDB (`fgdb`), MapInfo MIF, LAS, …
- **Lakehouse** — Delta and Iceberg support bounded writes (1.0.18+); also Lance, DuckLake, Paimon; Hudi remains read-only. Install with `pip install "undatum[lakehouse]"`
- **Scientific / statistical** — `h5`, `nc`, `mat`, `segy`, `grib2`, `sas`, `sav`, `dta`, and others (many read-only)
- **Containers** — ZIP; read-only TAR multi-member archives (`tar` / `.tgz`); WebDataset
- **Graph / RDF** — `graphml`, `gexf`, `jsonld`, `nt`, `ttl`, `trig`, `hdt`, …

### Compression

Read and write through compressed containers without manual decompression: **GZ, XZ, BZ2, ZIP, ZSTD, LZ4, 7Z**, and other codecs supported by iterabledata. Codec profiles `fast` / `balanced` / `max` are available in iterabledata 1.0.17+; `undatum repack` defaults to maximum compression.

```bash
# Process JSONL inside a ZIP or XZ archive
undatum headers --format-in jsonl data.zip
undatum count data.jsonl.xz
```

### Choosing a format

| Use case | Recommended formats |
|----------|---------------------|
| Streaming ETL / logs | JSON Lines, CSV |
| Analytics / data lakes | Parquet, ORC, Avro, Delta, Iceberg |
| API interchange | JSON, JSON Schema |
| Packaging / catalogs | Frictionless Data Package (`undatum package`) |
| Geospatial pipelines | GeoJSON / GeoJSONSeq → GeoParquet |

Inspect read/write capabilities before converting:

```bash
undatum formats describe parquet
undatum formats list --writable --capabilities
```

## AI Provider Troubleshooting

### Common Issues

**Provider not found:**
```bash
# Error: No AI provider specified
# Solution: Set environment variable or use --ai-provider
export UNDATUM_AI_PROVIDER=openai
# or
undatum analyze data.csv --autodoc --ai-provider openai
```

**API key not found:**
```bash
# Error: API key is required
# Solution: Set provider-specific API key
export OPENAI_API_KEY=sk-...
export ANTHROPIC_API_KEY=sk-ant-...
export GEMINI_API_KEY=...
export AZURE_OPENAI_API_KEY=...
export OPENROUTER_API_KEY=sk-or-...
export PERPLEXITY_API_KEY=pplx-...
```

**Ollama connection failed:**
```bash
# Error: Connection refused
# Solution: Ensure Ollama is running and model is pulled
ollama serve
ollama pull llama3.2
# Or specify custom URL
export OLLAMA_BASE_URL=http://localhost:11434
```

**LM Studio connection failed:**
```bash
# Error: Connection refused
# Solution: Start LM Studio server and load a model
# In LM Studio: Start Server, then:
export LMSTUDIO_BASE_URL=http://localhost:1234/v1
```

**Structured output errors:**
- All providers now use JSON Schema for reliable parsing
- If a provider doesn't support structured output, it will fall back gracefully
- Check provider documentation for model compatibility

### Provider-Specific Notes

- **OpenAI**: Requires API key; models include `gpt-4o-mini`, `gpt-4o`, `gpt-3.5-turbo`
- **Anthropic**: Requires `ANTHROPIC_API_KEY`; models include Claude 3.5/3 Haiku and Sonnet families
- **Gemini**: Requires `GEMINI_API_KEY`; models include `gemini-2.0-flash` and Pro variants
- **Azure OpenAI**: Requires `AZURE_OPENAI_API_KEY` and `AZURE_OPENAI_ENDPOINT`
- **OpenRouter**: Unified API for hosted models from OpenAI, Anthropic, Google, Meta, and others
- **Ollama**: Local models, no API key; requires Ollama installed and running
- **LM Studio**: Local models via OpenAI-compatible API; requires LM Studio server running
- **Perplexity**: Requires API key; uses `sonar` model by default

## Performance Tips

1. **Use appropriate formats**: Parquet/ORC/Avro for analytics, JSONL for streaming
2. **DuckDB engine**: Pass `--engine duckdb` on `stats`, `select`, `count`, `sort`, `join`, and related commands for accelerated tabular workloads
3. **Multiprocessing (`--threads N`)**: For Python-engine `convert`, `validate` (rules), `stats`, and `frequency`, use process-pool chunk parallelism on multi-core machines. Example: `undatum convert big.csv out.jsonl --engine python --threads 8`. Prefer DuckDB for duckable formats instead of nesting pools. See [`docs/LARGE_FILES.md`](docs/LARGE_FILES.md).
4. **Compression**: Use ZSTD or GZIP for better compression ratios
5. **Chunking**: Split large files for parallel processing, or use `--batch-size` with `--threads`
6. **Filtering**: Apply filters early (`select --filter`, `search`) to reduce data volume; DuckDB pushdown is used when possible
7. **Streaming**: undatum streams data by default for low memory usage
8. **AI documentation**: Prefer `ai doc` for block-based output; use local providers (Ollama/LM Studio) for zero-cost runs
9. **Cloud I/O**: Read/write directly from `s3://`, `gs://`, or `az://` URIs instead of staging files locally

## AI-Powered Documentation

undatum offers several AI documentation paths:

| Command | Best for |
|---------|----------|
| `ai doc` | Block-based docs (general, schema, quality, …) with schema enrichment — **recommended** |
| `doc --autodoc` | Markdown/JSON/YAML dataset documentation with metadata and PII options |
| `analyze --autodoc` | Human-readable analysis report with field descriptions |
| `schema --autodoc` / `schema_bulk --autodoc` | Schema files with AI field descriptions |
| `package create --autodoc` | Frictionless Data Package metadata |

All paths share the same provider configuration (`undatum.yaml`, environment variables, CLI flags). Supported providers: OpenAI, Anthropic, Gemini, Azure OpenAI, OpenRouter, Ollama, LM Studio, Perplexity.

### Quick Examples

```bash
# Recommended: block-based documentation
undatum ai doc data.csv --format json --blocks general,schema,quality

# Legacy analyze autodoc (still supported)
undatum analyze data.csv --autodoc

# Dataset documentation with PII detection
undatum doc data.csv --autodoc --pii-detect --format markdown

# Schema with AI field descriptions
undatum schema data.csv --autodoc --format jsonschema --output schema.json
```

### Configuration File Example

Create `undatum.yaml` in your project:

```yaml
ai:
  provider: openai
  model: gpt-4o-mini
  timeout: 30
```

Or use `~/.undatum/config.yaml` for global settings:

```yaml
ai:
  provider: ollama
  model: llama3.2
  ollama_base_url: http://localhost:11434
```

### Language Support

Generate descriptions in different languages:

```bash
# English (default)
undatum analyze data.csv --autodoc --lang English

# Russian
undatum analyze data.csv --autodoc --lang Russian

# Spanish
undatum analyze data.csv --autodoc --lang Spanish
```

### What Gets Generated

With `--autodoc` enabled, the analyzer will:

1. **Field Descriptions**: Generate clear, concise descriptions for each field explaining what it represents
2. **Dataset Summary**: Provide an overall description of the dataset based on sample data

Example output:

```yaml
tables:
  - id: data.csv
    fields:
      - name: customer_id
        ftype: VARCHAR
        description: "Unique identifier for each customer"
      - name: purchase_date
        ftype: DATE
        description: "Date when the purchase was made"
    description: "Customer purchase records containing transaction details"
```

## Examples

### Data Pipeline Example

```bash
# 1. Analyze source data
undatum analyze source.xml

# 2. Convert to JSON Lines
undatum convert --tagname item source.xml data.jsonl

# 3. Validate data
undatum validate --rule common.email --fields email data.jsonl --mode invalid > invalid.jsonl

# 4. Get statistics
undatum stats data.jsonl > stats.json

# 5. Extract unique categories
undatum uniq --fields category data.jsonl > categories.txt

# 6. Convert to Parquet for analytics
undatum convert data.jsonl data.parquet
```

### Data Quality Check

```bash
# Check for duplicate emails
undatum frequency --fields email data.jsonl | grep -v "1$"

# Rich validation with rule file
undatum validate data.jsonl --rules examples/validation-rules/basic-validation.yml

# Legacy mode: Validate individual fields
undatum validate --rule common.email --fields email data.jsonl
undatum validate --rule common.url --fields website data.jsonl

# Generate schema with AI documentation
undatum schema data.jsonl --output schema.yaml --autodoc
```

### AI Documentation Workflow

```bash
# 1. Analyze dataset with AI-generated descriptions
undatum analyze sales_data.csv --autodoc --ai-provider openai --output analysis.yaml

# 2. Review generated field descriptions
cat analysis.yaml

# 3. Use descriptions in schema generation
undatum schema sales_data.csv --autodoc --output documented_schema.yaml

# 4. Bulk schema extraction with AI documentation
undatum schema_bulk ./data_dir --autodoc --output ./schemas --mode distinct
```

## Troubleshooting

undatum provides user-friendly error messages to help you resolve issues quickly. Common errors include:

### File Not Found
If you see a "File not found" error, undatum will suggest similar filenames if it detects a typo:
```bash
undatum convert data.cvs output.jsonl
# Error: File not found: 'data.cvs'
# Did you mean: 'data.csv'?
```

### Permission Denied
For permission errors, undatum provides specific guidance:
```bash
# Error: Permission denied: Cannot read '/path/to/data.csv'
# Fix: chmod +r /path/to/data.csv
```

### Missing Dependencies
For optional features, install the required dependencies:
```bash
# Error: Missing dependency: 'pyyaml'
# Install it with: pip install pyyaml
```

### Verbose Mode
For detailed error information including full tracebacks, use the `--verbose` flag:
```bash
undatum convert data.csv output.jsonl --verbose
```

For more information, see the [Error Handling Guide](docs/ERROR_HANDLING.md).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

For error handling patterns and best practices, see [Error Handling Patterns](docs/ERROR_HANDLING_PATTERNS.md).

## License

MIT License - see LICENSE file for details.

## Links

- [GitHub Repository](https://github.com/datacoon/undatum)
- [Changelog](CHANGELOG.md)
- [Issue Tracker](https://github.com/datacoon/undatum/issues)

## Support

For questions, issues, or feature requests, please open an issue on GitHub.
