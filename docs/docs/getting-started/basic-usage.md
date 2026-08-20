---
title: "Basic usage"
description: "Compression, filters, encoding, and date detection"
---
# Basic usage

The CLI entry points are `undatum` and the shorter `data` alias.

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

**Migrating from older filters:** `AND`/`OR` and `&&`/`||` both work. Prefer double-quoted strings. There is no `IN` operator; write `status == "active" || status == "pending"`. The experimental MistQL `undatum query` command is gone; use [`sql`](/commands/sql) or `select --filter`.

For ad-hoc SQL over files, use [`sql`](/commands/sql) or [`db query`](/commands/db) against a database URI.

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

See also: [CLI reference](/commands/), [performance](/getting-started/performance).
