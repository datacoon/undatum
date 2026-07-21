## 1. CI Job
- [x] 1.1 Add wheel build step producing `dist/*.whl`
- [x] 1.2 Create clean venv and `pip install dist/*.whl`
- [x] 1.3 Run smoke commands for core formats (CSV, JSONL, Parquet at minimum)
- [x] 1.4 Matrix across supported Python versions (align with existing CI)

## 2. Coverage
- [x] 2.1 Include `convert`, `stats`, and `validate` smoke invocations
- [x] 2.2 Assert import of modules that previously shipped missing (#19/#37 class)
- [x] 2.3 Document the gate in CONTRIBUTING or CI comments
