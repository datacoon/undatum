# Quickstarts

Short task-oriented paths to first success. Not sure where to start? Pick your
role and goal in the [scenario index](SCENARIOS.md). For the full reference,
see the [README](../README.md).

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

Describe your expectations in a rules file, then validate against it:

```bash
cat > rules.yml <<'EOF'
rules:
  - field: email
    name: Email format
    required: true
    type: string
    format: email
    severity: error
EOF

undatum validate data.csv --rules rules.yml
undatum analyze data.csv
undatum package create data.csv --output datapackage.json
```

For a one-off check without a rules file, use legacy single-rule mode:

```bash
undatum validate data.csv --fields email --rule common.email
```

## Query JSONL with SQL

`undatum sql` takes the query first, then the input file(s). A single input is
available as the view `data`; multiple inputs are named after their file stems.

```bash
undatum sql "SELECT city, COUNT(*) AS n FROM data GROUP BY 1" events.jsonl
# or use frequency / select for simpler extractions:
undatum frequency events.jsonl --fields city
undatum select events.jsonl --fields id,city,ts --filter-expr '`city` == "Berlin"'
```

## Dump a database table to Parquet

```bash
undatum db dump --db sqlite:///app.db --table users --output users.parquet --to parquet
```

## Next steps

- [Usage scenarios by role](SCENARIOS.md) — task-oriented index for analysts, engineers, publishers, and more
- [Format support matrix](FORMAT_SUPPORT.md) — 140+ formats, lakehouse/open-data notes, extras
- [When to use undatum](POSITIONING.md)
- [Large files](LARGE_FILES.md)
- [Error handling](ERROR_HANDLING.md)
