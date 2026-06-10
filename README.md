# undatum

> A powerful command-line tool for data processing and analysis

**undatum** (pronounced *un-da-tum*) is a modern CLI tool designed to make working with large datasets as simple and efficient as possible. It provides a unified interface for converting, analyzing, validating, and transforming data across multiple formats.

## Features

- **Multi-format support**: CSV, JSON Lines, BSON, XML, XLS, XLSX, Parquet, AVRO, ORC
- **Compression support**: ZIP, XZ, GZ, BZ2, ZSTD
- **Low memory footprint**: Streams data for efficient processing of large files
- **Automatic detection**: Encoding, delimiters, and file types
- **Data validation**: Built-in rules for emails, URLs, and custom validators
- **Advanced statistics**: Field analysis, frequency calculations, and date detection
- **Flexible filtering**: Query and filter data using expressions
- **Schema generation**: Automatic schema detection and generation
- **Database ingestion**: Ingest data to MongoDB, PostgreSQL, DuckDB, MySQL, SQLite, and Elasticsearch with retry logic and error handling
- **AI-powered documentation**: Automatic field and dataset descriptions using multiple LLM providers (OpenAI, OpenRouter, Ollama, LM Studio, Perplexity) with structured JSON output
- **Optional Data API**: Serve file-backed datasets over HTTP with FastAPI + DuckDB

## Documentation

- `WORKFLOW_GUIDE.md` for contributor workflow and OpenSpec usage
- `openspec/` for change proposals, specs, and implementation summaries
- `examples/doc/` for dataset documentation output samples
- `docs/ERROR_HANDLING.md` for troubleshooting common errors
- `docs/ERROR_HANDLING_PATTERNS.md` for error handling patterns (developers)

## Installation

### Using pip (Recommended)

```bash
pip install --upgrade pip setuptools
pip install undatum
```

Dependencies are declared in `pyproject.toml` and will be installed automatically by modern versions of `pip` (23+). If you see missing-module errors after installation, upgrade `pip` and retry.

Optional extras:

```bash
# Data API (FastAPI + uvicorn)
pip install "undatum[api]"
```

### Requirements

- Python 3.9 or greater

### Install from source

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install .
# or build distributables
python setup.py sdist bdist_wheel
```

## Quick Start

```bash
# Get file headers
undatum headers data.jsonl

# Analyze file structure
undatum analyze data.jsonl

# Generate dataset documentation
undatum doc data.jsonl --format markdown --output docs/dataset.md

# Create a Frictionless Data Package
undatum package create data.csv --output datapackage.json

# Extract tables from a PDF
undatum extract report.pdf --output-format csv --output report.csv

# Serve a CSV as a read-only API
undatum api run data.csv

# Generate API config (YAML) for multiple files
undatum api discover data.csv other.parquet --output api.yml

# Serve from config
undatum api serve --config api.yml

# Get statistics
undatum stats data.csv

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
```

## Commands

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
```

**Output includes:**
- File type, encoding, compression
- Number of records and fields
- Field types and structure
- Table detection for nested data (JSON/XML)
- AI-generated field descriptions (with `--autodoc`)
- AI-generated dataset summary (with `--autodoc`)

**AI Provider Options:**
- `--ai-provider`: Choose provider (openai, openrouter, ollama, lmstudio, perplexity)
- `--ai-model`: Specify model name (provider-specific)
- `--ai-base-url`: Custom API endpoint URL

**Supported AI Providers:**

1. **OpenAI** (default if `OPENAI_API_KEY` is set)
   ```bash
   export OPENAI_API_KEY=sk-...
   undatum analyze data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini
   ```

2. **OpenRouter** (supports multiple models via unified API)
   ```bash
   export OPENROUTER_API_KEY=sk-or-...
   undatum analyze data.csv --autodoc --ai-provider openrouter --ai-model openai/gpt-4o-mini
   ```

3. **Ollama** (local models, no API key required)
   ```bash
   # Start Ollama and pull a model first: ollama pull llama3.2
   undatum analyze data.csv --autodoc --ai-provider ollama --ai-model llama3.2
   # Or set custom URL: export OLLAMA_BASE_URL=http://localhost:11434
   ```

4. **LM Studio** (local models, OpenAI-compatible API)
   ```bash
   # Start LM Studio and load a model
   undatum analyze data.csv --autodoc --ai-provider lmstudio --ai-model local-model
   # Or set custom URL: export LMSTUDIO_BASE_URL=http://localhost:1234/v1
   ```

5. **Perplexity** (backward compatible, uses `PERPLEXITY_API_KEY`)
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
   ```

3. **CLI arguments** (highest precedence):
   ```bash
   undatum analyze data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini
   ```

### `doc`

Generates dataset documentation with schema, statistics, and samples in Markdown (default), JSON, YAML, or text. Supports AI-powered descriptions with `--autodoc`.

```bash
# Markdown documentation (default)
undatum doc data.jsonl

# JSON documentation with samples
undatum doc data.jsonl --format json --sample-size 5 --output report.json

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

### `package`

Generates a Frictionless Data Package descriptor (`datapackage.json`) from one or more data files. Supports optional package metadata, schema inference, and AI-powered metadata generation with `--autodoc`.

```bash
# Create datapackage.json for a single file
undatum package create data.csv --output datapackage.json

# Create a package directory with data file copies
undatum package create data.csv --package-dir out/package

# Provide metadata and enable AI metadata generation
undatum package create data.csv --title "Sales data" --keywords sales,finance \
  --autodoc --ai-provider openai --ai-model gpt-4o-mini
```

**Metadata options:**
- `--name`, `--title`, `--description`, `--keywords`
- `--licenses` (semicolon-separated entries, e.g. `name=MIT;name=ODC-PDDL-1.0`)
- `--sources` (semicolon-separated entries, e.g. `title=World Bank,path=https://...`)
- `--contributors` (semicolon-separated entries, e.g. `title=Jane Doe,email=jane@example.com`)
- `--version` - Package version string

**Features:**
- **Schema inference**: Automatically infers field types and generates Frictionless schema
- **Multiple resources**: Package multiple files as separate resources
- **Remote URIs**: Support for HTTP/HTTPS URLs as resource paths
- **Package directory**: Bundle `datapackage.json` with data file copies
- **AI metadata**: Use `--autodoc` to generate metadata with AI assistance
- **Streaming-safe**: Processes large datasets without loading everything into memory

**Additional options:**
- `--package-dir`: Create a package directory with data file copies
- `--autodoc`: Enable AI-powered metadata generation (reuses `doc` command logic)
- `--engine`: Processing engine (`auto` or `duckdb`)
- `--objects-limit`: Maximum objects to analyze for schema inference (default: 10000)
- `--sample-size`: Number of sample records for metadata inference (default: 10)

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

Converts data between different formats. Supports CSV, JSON Lines, BSON, XML, XLS, XLSX, Parquet, AVRO, and ORC. Supports S3 URIs for cloud storage integration.

```bash
# XML to JSON Lines
undatum convert --tagname item data.xml data.jsonl

# CSV to Parquet
undatum convert data.csv data.parquet

# JSON Lines to CSV
undatum convert data.jsonl data.csv

# Convert from S3 to local
undatum convert s3://my-bucket/data.csv output.jsonl

# Convert local to S3
undatum convert input.csv s3://my-bucket/output.parquet

# Convert S3 to S3
undatum convert s3://bucket/input.jsonl s3://bucket/output.parquet
```

**S3 Support:**
- Input and output paths support S3 URIs (`s3://bucket/path`)
- AWS credentials via environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, `AWS_REGION`)
- Automatic temporary file handling for S3 operations

**Supported conversions:**

| From / To | CSV | JSONL | BSON | JSON | XLS | XLSX | XML | Parquet | ORC | AVRO |
|-----------|-----|-------|------|------|-----|------|-----|---------|-----|------|
| CSV       | -   | ✓     | ✓    | -    | -   | -    | -   | ✓       | ✓   | ✓    |
| JSONL    | ✓   | -     | -    | -    | -   | -    | -   | ✓       | ✓   | -    |
| BSON     | -   | ✓     | -    | -    | -   | -    | -   | -       | -   | -    |
| JSON     | -   | ✓     | -    | -    | -   | -    | -   | -       | -   | -    |
| XLS      | -   | ✓     | ✓    | -    | -   | -    | -   | -       | -   | -    |
| XLSX     | -   | ✓     | ✓    | -    | -   | -    | -   | -       | -   | -    |
| XML      | -   | ✓     | -    | -    | -   | -    | -   | -       | -   | -    |

### `count`

Counts the number of rows in a data file. With DuckDB engine, counting is instant for supported formats.

```bash
# Count rows in CSV file
undatum count data.csv

# Count rows in JSONL file
undatum count data.jsonl

# Use DuckDB engine for faster counting
undatum count data.parquet --engine duckdb
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
```

### `reverse`

Reverses the order of rows in a data file.

```bash
# Reverse rows
undatum reverse data.csv output.csv

# Reverse JSONL file
undatum reverse data.jsonl output.jsonl
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
```

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
```

### `uniq`

Extracts all unique values from specified field(s).

```bash
# Single field
undatum uniq --fields category data.jsonl

# Multiple fields (unique combinations)
undatum uniq --fields status,region data.jsonl
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
```

### `diff`

Compares two files and shows differences (added, removed, and changed rows).

```bash
# Compare files by key
undatum diff file1.csv file2.csv --key id

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

Selects and reorders columns from files. Supports filtering and engine selection.

```bash
undatum select --fields name,email,status data.jsonl
undatum select --fields name,email --filter "`status` == 'active'" data.jsonl
undatum select --fields name,email --engine duckdb data.jsonl
```

### `split`

Splits datasets into multiple files based on chunk size or field values.

```bash
# Split by chunk size
undatum split --chunksize 10000 data.jsonl

# Split by field value
undatum split --fields category data.jsonl
```

### `validate`

Validates data against validation rules. Supports two modes: **rich validation with rule files** (recommended) and **legacy single-rule mode** (backward compatible).

#### Rich Validation with Rule Files

Use YAML/JSON rule files for comprehensive, reusable validation:

```bash
# Validate with rule file
undatum validate data.csv --rules validation-rules.yml

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
- **Cerberus**: Python data validation (legacy, use `scheme` command or `schema --format cerberus`)

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

### `query`

Query data using MistQL query language (experimental).

```bash
undatum query data.jsonl "SELECT * WHERE status = 'active'"
```

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

Ingests data from files into databases. Supports MongoDB, PostgreSQL, and Elasticsearch with robust error handling, retry logic, and progress tracking.

```bash
# Ingest to MongoDB
undatum ingest data.jsonl mongodb://localhost:27017 mydb mycollection

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

Database query and load commands for working with databases as first-class data sources and sinks.

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
- PostgreSQL (`postgresql://user:pass@host:port/db`)
- MySQL/MariaDB (`mysql://user:pass@host:port/db`)
- SQLite (`sqlite:///path/to/db.db` or `sqlite:///:memory:`)

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

**Requirements:**
- `matplotlib` is required for plotting. Install with: `pip install matplotlib`

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
- `csv-to-jsonl` - Convert CSV to JSONL format
- `data-validation` - Validate data using validation rules
- `database-query-export` - Query database and export results
- `data-profiling` - Profile dataset with statistics and documentation
- `data-cleaning` - Clean data by removing duplicates and filling missing values

**Recipe Format:**

Recipes are defined in YAML files in `examples/recipes/` directory:

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
```

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

## Cloud Storage Support

### AWS S3 Integration

Undatum supports direct read/write operations with AWS S3 using S3 URIs (`s3://bucket/path`). This enables seamless integration with cloud data workflows without manual download/upload steps.

**Setup:**
```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1

# Option 2: AWS Profile
export AWS_PROFILE=my-profile

# Option 3: Default AWS credentials (~/.aws/credentials)
# No configuration needed if using default profile
```

**Usage Examples:**
```bash
# Read from S3
undatum stats s3://my-bucket/data.csv
undatum count s3://my-bucket/data.jsonl

# Write to S3
undatum convert local.csv s3://my-bucket/output.parquet

# S3 to S3 operations
undatum convert s3://bucket/input.jsonl s3://bucket/output.parquet
undatum mask s3://bucket/data.csv --fields email --method hash s3://bucket/masked.csv
```

**Supported Commands:**
- `convert` - S3 input and output
- `stats` - S3 input
- `count` - S3 input
- `ingest` - S3 input
- `mask` - S3 input and output
- All commands that accept file paths (with S3 URI support)

**Dependencies:**
- `boto3` (optional, install with `pip install boto3`)

## Python SDK

Undatum provides a Python SDK for programmatic data processing with a fluent API that mirrors CLI commands.

### Quick Start

```python
from undatum import Dataset

# Read data
ds = Dataset.read("data.jsonl")

# Chain transformations
ds = ds.fill("age", value=0).dedup(keys=["user_id"]).sort("name")

# Compute statistics
stats = ds.stats()

# Write output
ds.write("output.parquet")
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
```

### S3 Support

```python
# Read from S3
ds = Dataset.read("s3://bucket/data.jsonl")

# Write to S3
ds.write("s3://bucket/output.parquet")

# Chain with S3
ds = Dataset.read("s3://bucket/input.csv")
ds = ds.fill("age", value=0).dedup(keys=["id"])
ds.write("s3://bucket/output.jsonl")
```

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

Pipeline files define a series of steps, each executing an undatum command:

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
- `s3-etl` - S3-based ETL workflow (download, process, upload)
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

Most commands support filtering using expressions:

```bash
# Filter by field value
undatum select --fields name,email --filter "`status` == 'active'" data.jsonl

# Complex filters
undatum frequency --fields category --filter "`price` > 100" data.jsonl
```

**Filter syntax:**
- Field names: `` `fieldname` ``
- String values: `'value'`
- Operators: `==`, `!=`, `>`, `<`, `>=`, `<=`, `and`, `or`

### Date Detection

Automatic date/datetime field detection:

```bash
undatum stats --checkdates data.jsonl
```

This uses the `qddate` library to automatically identify and parse date fields.

### Custom Encoding and Delimiters

Override automatic detection:

```bash
undatum headers --encoding cp1251 --delimiter ";" data.csv
undatum convert --encoding utf-8 --delimiter "," data.csv data.jsonl
```

## Data Formats

### JSON Lines (JSONL)

JSON Lines is a text format where each line is a valid JSON object. It combines JSON flexibility with line-by-line processing capabilities, making it ideal for large datasets.

```jsonl
{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}
```

### CSV

Standard comma-separated values format. undatum automatically detects delimiters (comma, semicolon, tab) and encoding.

### BSON

Binary JSON format used by MongoDB. Efficient for binary data storage.

### XML

XML files can be converted to JSON Lines by specifying the tag name containing records.

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

- **OpenAI**: Requires API key, supports `gpt-4o-mini`, `gpt-4o`, `gpt-3.5-turbo`, etc.
- **OpenRouter**: Unified API for multiple providers, supports models from OpenAI, Anthropic, Google, etc.
- **Ollama**: Local models, no API key needed, but requires Ollama to be installed and running
- **LM Studio**: Local models, OpenAI-compatible API, requires LM Studio to be running
- **Perplexity**: Requires API key, uses `sonar` model by default

## Performance Tips

1. **Use appropriate formats**: Parquet/ORC for analytics, JSONL for streaming
2. **Compression**: Use ZSTD or GZIP for better compression ratios
3. **Chunking**: Split large files for parallel processing
4. **Filtering**: Apply filters early to reduce data volume
5. **Streaming**: undatum streams data by default for low memory usage
6. **AI Documentation**: Use local providers (Ollama/LM Studio) for faster, free documentation generation
7. **Batch Processing**: AI descriptions are generated per-table, consider splitting large datasets

## AI-Powered Documentation

The `analyze` command can automatically generate field descriptions and dataset summaries using AI when `--autodoc` is enabled. This feature supports multiple LLM providers and uses structured JSON output for reliable parsing.

### Quick Examples

```bash
# Basic AI documentation (auto-detects provider from environment)
undatum analyze data.csv --autodoc

# Use OpenAI with specific model
undatum analyze data.csv --autodoc --ai-provider openai --ai-model gpt-4o-mini

# Use local Ollama model
undatum analyze data.csv --autodoc --ai-provider ollama --ai-model llama3.2

# Use OpenRouter to access various models
undatum analyze data.csv --autodoc --ai-provider openrouter --ai-model anthropic/claude-3-haiku

# Output to YAML with AI descriptions
undatum analyze data.csv --autodoc --output schema.yaml --outtype yaml
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
- [Issue Tracker](https://github.com/datacoon/undatum/issues)

## Support

For questions, issues, or feature requests, please open an issue on GitHub.
