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

## Installation

### Using pip (Recommended)

```bash
pip install --upgrade pip setuptools
pip install undatum
```

### Requirements

- Python 3.8 or greater

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

Analyzes data files and provides human-readable insights about structure, encoding, fields, and data types.

```bash
undatum analyze data.jsonl
undatum analyze data.jsonl --output report.yaml --autodoc
```

**Output includes:**
- File type, encoding, compression
- Number of records and fields
- Field types and structure
- Table detection for nested data (JSON/XML)

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

## Performance Tips

1. **Use appropriate formats**: Parquet/ORC for analytics, JSONL for streaming
2. **Compression**: Use ZSTD or GZIP for better compression ratios
3. **Chunking**: Split large files for parallel processing
4. **Filtering**: Apply filters early to reduce data volume
5. **Streaming**: undatum streams data by default for low memory usage

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

# Generate schema for documentation
undatum schema data.jsonl --output schema.yaml
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
