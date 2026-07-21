## 1. Fix Codec Matching
- [x] 1.1 Confirm iterabledata codec id for `.gz` files (`"gz"` vs `"gzip"`)
- [x] 1.2 Update `DUCKABLE_CODECS` and/or normalize compression ids in engine selector
- [x] 1.3 Audit duplicate local `DUCKABLE_CODECS` definitions (e.g. ingester) for consistency

## 2. Tests
- [x] 2.1 Add unit test: gzip-compressed duckable format → DuckDB-eligible
- [x] 2.2 Add unit test: unknown codec still falls back to Python engine
- [x] 2.3 Run `pytest tests/test_engine_selector.py -v`
