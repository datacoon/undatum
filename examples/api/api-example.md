# API Command Examples

This directory contains examples for using the `undatum api` commands to expose data files as HTTP APIs.

## Quick Start

### 1. Discover and Serve in One Command

The simplest way to expose a data file as an API:

```bash
undatum api run data-20130920T1245.jsonl
```

This will:
- Automatically detect the file format
- Infer the schema and fields
- Start a FastAPI server on `http://127.0.0.1:8000`
- Expose the data at `/data_20130920t1245`

### 2. Two-Step Process: Discover then Serve

Generate a config file first, then serve from it:

```bash
# Step 1: Generate API config
undatum api discover data-20130920T1245.jsonl --output api-config.yml

# Step 2: Serve from config
undatum api serve --config api-config.yml --host 127.0.0.1 --port 8000
```

## API Endpoints

Once the server is running, you can access:

### List Endpoint
Get paginated list of records (response envelope):
```
GET http://127.0.0.1:8000/data_20130920t1245
```

Example response:
```json
{
  "data": [{ "...": "..." }],
  "pagination": { "limit": 50, "offset": 0, "count": 50 }
}
```

Add `include_total=true` to include the total matching row count.

### Filtering
Filter records using query parameters:
```
GET http://127.0.0.1:8000/data_20130920t1245?title__like=Правительственная
GET http://127.0.0.1:8000/data_20130920t1245?pubDate__ge=2014-03-01
```

Supported operators: `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `like`

### Pagination
Control page size and offset:
```
GET http://127.0.0.1:8000/data_20130920t1245?limit=10&offset=20
```

### Sorting
Sort results by field:
```
GET http://127.0.0.1:8000/data_20130920t1245?order_by=pubDate&order_dir=desc
GET http://127.0.0.1:8000/data_20130920t1245?sort=-pubDate
```

### Detail Endpoint (if primary key is defined)
Get a single record by primary key:
```
GET http://127.0.0.1:8000/data_20130920t1245/{guid}
```

### OpenAPI Documentation
Interactive API documentation (while the server is running):
```
http://127.0.0.1:8000/docs
```

Export OpenAPI schema without starting the server:
```bash
undatum api openapi --config api-config.yml --output openapi.json
```

## Configuration Options

### Discover Command Options

```bash
undatum api discover <files> \
  --output api-config.yml \          # Output config file path
  --format-in csv \                   # Override file format detection
  --config-format yaml \              # Config format: yaml or json
  --default-limit 50 \                # Default pagination limit
  --max-limit 1000 \                  # Maximum pagination limit
  --allowed-ops eq,ne,lt,gt,le,ge,like  # Allowed query operators
```

### Serve Command Options

```bash
undatum api serve \
  --config api-config.yml \           # Path to API config file
  --host 127.0.0.1 \                  # Host to bind (default: 127.0.0.1)
  --port 8000                         # Port to bind (default: 8000)
```

### Run Command Options

```bash
undatum api run <files> \
  --format-in csv \                   # Override file format detection
  --default-limit 50 \                # Default pagination limit
  --max-limit 1000 \                  # Maximum pagination limit
  --allowed-ops eq,ne,lt,gt,le,ge,like \  # Allowed query operators
  --host 127.0.0.1 \                  # Host to bind
  --port 8000                         # Port to bind
```

## Example Config File

See `api-config-example.yml` for a complete example of the API configuration format.

## Supported File Formats

- CSV (`.csv`)
- JSON Lines (`.jsonl`)
- Parquet (`.parquet`)

## Using the Recipe

You can also use the recipe system:

```bash
undatum examples run api-serve-data \
  --var input=data-20130920T1245.jsonl \
  --var host=127.0.0.1 \
  --var port=8000
```

## Testing the API

Once the server is running, you can test it with `curl`:

```bash
# Get first 10 records
curl "http://127.0.0.1:8000/data_20130920t1245?limit=10"

# Filter by title
curl "http://127.0.0.1:8000/data_20130920t1245?title__like=Правительственная"

# Sort by date
curl "http://127.0.0.1:8000/data_20130920t1245?order_by=pubDate&order_dir=desc&limit=5"
```

Or open the interactive docs in your browser:
```
http://127.0.0.1:8000/docs
```
