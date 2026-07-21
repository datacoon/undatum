## 1. Dependency
- [x] 1.1 Decide default vs optional+hint (default preferred per roadmap)
- [x] 1.2 Add `pyarrow` to core dependencies in `pyproject.toml` (or extras + universal hints)
- [x] 1.3 Verify converter import paths no longer soft-fail silently on missing pyarrow

## 2. Docs & Tests
- [x] 2.1 Update README install/format notes for default Parquet support
- [x] 2.2 Add smoke test: convert CSV → Parquet in clean env with declared deps
- [x] 2.3 Confirm wheel extras matrix still documents any remaining optional formats
