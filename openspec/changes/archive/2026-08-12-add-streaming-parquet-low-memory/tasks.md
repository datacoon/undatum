## 1. Streaming Parquet Writer
- [x] 1.1 Implement batched Parquet write via pyarrow `ParquetWriter` / row groups
- [x] 1.2 Ensure compressed JSONL/CSV inputs stream into the writer without full materialization
- [x] 1.3 Preserve existing compression codec options for Parquet output

## 2. Low-Memory Mode
- [x] 2.1 Add `--low-memory` flag to convert (and document scope)
- [x] 2.2 Prefer DuckDB spill path when format is duckable under low-memory/auto
- [x] 2.3 Add docs section on large-file conversion behavior

## 3. Tests
- [x] 3.1 Unit/integration tests for batched Parquet write
- [x] 3.2 Memory-bounded test or documented manual verification recipe for multi-GB path
- [x] 3.3 Regression: small CSV → Parquet still works without requiring the flag
