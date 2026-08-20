---
title: "db"
description: "undatum db command reference"
---
# `db`

Database query, load, and dump commands for working with databases as first-class data sources and sinks. See also [`db dump`](/commands/db) below.

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
