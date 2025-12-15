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
- **AI-powered documentation**: Automatic field and dataset descriptions using multiple LLM providers (OpenAI, OpenRouter, Ollama, LM Studio, Perplexity) with structured JSON output

## Installation

### Using pip (Recommended)

```bash
pip install --upgrade pip setuptools
pip install undatum
```

Dependencies are declared in `pyproject.toml` and will be installed automatically by modern versions of `pip` (23+). If you see missing-module errors after installation, upgrade `pip` and retry.

### Requirements

- Python 3.8 or greater

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

# Get statistics
undatum stats data.csv

# Convert XML to JSON Lines
undatum convert --tagname item data.xml data.jsonl

# Get unique values
undatum uniq --fields category data.jsonl

# Calculate frequency
undatum frequency --fields status data.csv
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

### `convert`

Converts data between different formats. Supports CSV, JSON Lines, BSON, XML, XLS, XLSX, Parquet, AVRO, and ORC.

```bash
# XML to JSON Lines
undatum convert --tagname item data.xml data.jsonl

# CSV to Parquet
undatum convert data.csv data.parquet

# JSON Lines to CSV
undatum convert data.jsonl data.csv
```

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

### `headers`

Extracts field names from data files. Works with CSV, JSON Lines, BSON, and XML files.

```bash
undatum headers data.jsonl
undatum headers data.csv --limit 50000
```

### `stats`

Generates detailed statistics about your dataset including field types, uniqueness, lengths, and more.

```bash
undatum stats data.jsonl
undatum stats data.csv --checkdates
```

**Statistics include:**
- Field types and array flags
- Unique value counts and percentages
- Min/max/average lengths
- Date field detection

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

### `select`

Selects and reorders columns from files. Supports filtering.

```bash
undatum select --fields name,email,status data.jsonl
undatum select --fields name,email --filter "`status` == 'active'" data.jsonl
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

Validates data against built-in or custom validation rules.

```bash
# Validate email addresses
undatum validate --rule common.email --fields email data.jsonl

# Validate Russian INN
undatum validate --rule ru.org.inn --fields VendorINN data.jsonl --mode stats

# Output invalid records
undatum validate --rule ru.org.inn --fields VendorINN data.jsonl --mode invalid
```

**Available validation rules:**
- `common.email` - Email address validation
- `common.url` - URL validation
- `ru.org.inn` - Russian organization INN identifier
- `ru.org.ogrn` - Russian organization OGRN identifier

### `schema`

Generates data schemas from files. Supports Cerberus and other schema formats.

```bash
undatum schema data.jsonl
undatum schema data.jsonl --output schema.yaml
```

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

# Validate all required fields
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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details.

## Links

- [GitHub Repository](https://github.com/datacoon/undatum)
- [Issue Tracker](https://github.com/datacoon/undatum/issues)

## Support

For questions, issues, or feature requests, please open an issue on GitHub.
