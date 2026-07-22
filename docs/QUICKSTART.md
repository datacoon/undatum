# Quickstarts

Short task-oriented paths to first success. For the full reference, see the [README](../README.md).

## CSV → Parquet in 30 seconds

```bash
pip install undatum   # or: uv tool install undatum
printf 'name,age,city\nAda,36,London\nGrace,85,NYC\n' > people.csv
undatum convert people.csv people.parquet
undatum stats people.parquet
```

For multi-GB inputs:

```bash
undatum convert huge.jsonl.zst huge.parquet --low-memory
```

## Validate a dataset before publishing

```bash
undatum validate data.csv
undatum analyze data.csv
undatum package create data.csv --output ./datapackage
```

## Query JSONL with SQL

```bash
undatum sql "SELECT city, COUNT(*) AS n FROM read_json_auto('events.jsonl') GROUP BY 1" 
# or use select / frequency for simpler extractions:
undatum frequency events.jsonl --fields city
undatum select events.jsonl --fields id,city,ts --filter "`city` == 'Berlin'"
```

## Dump a database table to Parquet

```bash
undatum db dump --db sqlite:///app.db --table users --output users.parquet --to parquet
```

## Next steps

- [Format support matrix](FORMAT_SUPPORT.md) — 140+ formats, lakehouse/open-data notes, extras
- [When to use undatum](POSITIONING.md)
- [Large files](LARGE_FILES.md)
- [Error handling](ERROR_HANDLING.md)
