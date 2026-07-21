## 1. Command Design
- [x] 1.1 Define `db dump` CLI options (connection, table/query, `--to` format, output path)
- [x] 1.2 Reuse connection helpers from `db query` / `db load`
- [x] 1.3 Stream results to Parquet/CSV/JSONL without full materialization where possible

## 2. Docs & Tests
- [x] 2.1 Document dump recipe and examples
- [x] 2.2 Integration tests with SQLite fixture at minimum
- [x] 2.3 Close or update issue #13
