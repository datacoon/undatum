# Testing

## Layout

- `tests/test_*.py` — unit and integration tests
- `tests/conftest.py` — fixtures (`sample_csv_file`, `sample_jsonl_file`, optional `benchmark`)
- `tests/benchmarks/` — `@pytest.mark.benchmark` (skipped if pytest-benchmark is missing)
- `tests/fixtures/` — static sample files

Optional extras skip when missing (especially `undatum[api]`, TUI/web, MCP). CI installs `.[api]` for the test matrix.

## Commands

```bash
pytest
pytest tests/test_converter.py -v
make test
make test-cov
make check-all   # format-check + lint + type-check + test
```

Dev install: `make install-dev` or `pip install -e ".[dev]"`.

## CI

`.github/workflows/ci.yml` on `main`:

- lint (ruff, black) on Python 3.11
- docs (`docs/` npm ci && build)
- type-check (mypy, continue-on-error)
- test matrix Python 3.9–3.13

## What to run when changing a command

1. Find the wrapper in `undatum/cli/` and the implementation in `undatum/cmds/`.
2. Run the matching `tests/test_*.py`.
3. If you change CLI option names, grep tests and docs for the old flag (`--filter-expr` is kept as an alias of `--filter`).

## Related

- [Workflows](workflows.md)
- `CONTRIBUTING.md`, `docs/docs/development/contributing.md`
