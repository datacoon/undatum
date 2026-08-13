# Repository Analysis — undatum

- **Date**: 2026-07-16
- **Code state**: commit `428de01` ("release: v1.5.0", 2026-06-29) **plus 43 uncommitted files** (32 modified/added, +2344/−1020 lines; an in-progress change set)
- **Method**: static analysis (ruff, black, grep-based audits), docs/metadata review, full test suite run on Python 3.13 (`.venv`)

## 1. Snapshot

| Metric | Value |
|---|---|
| Package code | 121 `.py` files, 27,922 LOC (`undatum/`) |
| Tests | 60 test files, 810 test functions, 11,052 LOC (`tests/`) |
| Test result (this run) | **778 passed, 16 failed, 22 skipped** in ~13s |
| Commands | 45 top-level + 9 sub-apps (`ai`, `api`, `db`, `examples`, `formats`, `mcp`, `package`, `pipeline`, `plugins`) = 72 registered commands |
| Version | 1.5.0 (`undatum/__init__.py`, `pyproject.toml`, CHANGELOG all consistent) |
| Git | 78 commits, single contributor (Ivan Begtin), branch `master` |
| Largest files | `undatum/cli/data_commands.py` (2,518 LOC), `undatum/ai/providers.py` (1,369), `undatum/cmds/analyzer.py` (838) |

## 2. Key findings (action needed)

### 2.1 Working tree is mid-refactor; 16 tests failing — Critical
The uncommitted change set leaves the suite red:

- **9 failures caused by a deleted fixture**: `tests/fixtures/2cols6rows.csv` is deleted (`git status`: ` D`), but still referenced by `tests/test_doc_command.py:8,35,53`, `tests/test_sdk_interop.py:9`, `tests/test_tools.py:10`, `tests/test_iterabledata_migration.py:31`. All fail with `FileNotFoundError`. Either restore the fixture or migrate those tests to another fixture.
- **7 failures tied to in-progress edits**: `test_ai_cli.py::TestAiPlan::test_plan_uses_catalog`, `test_bulk_convert.py` (2), `test_converter_helpers.py::test_df_to_pyorc_schema_datetime`, `test_tools.py` (4: `test_frequency`, `test_query_sql`, `test_deduplicate_with_confirm`, `test_string_arguments`).

### 2.2 Lint/format gates currently red — Major
- `ruff check undatum/`: **15 errors** — 5× F401 (unused import), 4× B023 (function-uses-loop-variable, real bug risk in closures), 2× E731, 2× I001, 1× F841, 1× UP045. 7 auto-fixable via `ruff check --fix`.
- `black --check undatum/ tests/`: **18 files would be reformatted**.
- CI's `lint` job runs exactly these two checks — the current tree would fail CI. Note also: B023 (loop variable capture) deserves manual review, not just a blind fix.

### 2.3 CI triggers on `main`, but the repo lives on `master` — Major
`.github/workflows/ci.yml:3-7` triggers on pushes/PRs to `main`; the repository's default and only local branch is `master` (`origin/HEAD -> origin/master`). Push-triggered CI likely never runs on this repo — only PRs targeting `main` would. This explains how 2.1/2.2 went unnoticed.

### 2.4 Stale project metadata and artifacts — Major
- **`AGENTS.md` is badly stale**: claims version 1.1.1 (actual: 1.5.0), documents the old architecture ("commands implemented in `cmds/` and wired in `core.py`" — in reality `undatum/cli/` wrappers do the wiring and `undatum/tools/` exists but is unmentioned), and lists CI as "Python 3.9–3.11" (actual matrix: 3.9–3.13).
- **`dist/` contains 1.1.0/1.1.1 release artifacts** while the package is at 1.5.0 — remove or rebuild.
- **Stale coverage artifacts in the workdir**: `.coverage` (Jun 11) and `htmlcov/` (Jan 23) predate v1.4/v1.5; any numbers from them are misleading. Neither is git-tracked (good), but they should be regenerated or deleted.
- **Duplicate virtualenvs in the workdir**: both `venv/` (Python 3.9.6, has pytest-cov) and `.venv/` (Python 3.13.7, no pytest-cov). Consolidate to one to avoid "works in my venv" drift.

### 2.5 OpenSpec backlog not archived — Minor (process)
`openspec/changes/` holds **28 active change proposals**, but many correspond to already-shipped features: `add-mask-command`, `add-pipeline-command`, `add-plot-command`, `add-sql-command`, `add-python-sdk`, `add-s3-connector`, etc. (all present in CHANGELOG 1.3.0–1.5.0). Only 9 changes are under `openspec/changes/archive/`. Run `openspec archive` for completed changes; the active list currently misrepresents pending work. (Archiving of `add-frictionless-package-command` is already in progress in the dirty tree.)

### 2.6 God file: `undatum/cli/data_commands.py` — Minor
2,518 LOC implementing all 45 top-level command wrappers in one file. Splitting by domain (e.g. view/transform/schema/convert groups) would improve navigability; low urgency since the file is thin wrappers over `cmds/`.

### 2.7 Security notes — Minor
No hardcoded secrets, no unsafe `yaml.load`, no `pickle`, no `verify=False`, no bare `except:` found. Two flagged-by-design usages:
- `undatum/cmds/examples.py:321` — `subprocess.run(substituted_cmd, shell=True)` executes recipe commands from YAML with variable substitution. Acceptable for a local recipe runner, but it's a command-injection surface if recipes ever come from untrusted sources; document the trust boundary.
- `undatum/common/validation_rules.py:204` — `eval()` with `__builtins__` stripped for cross-field validation conditions. Reasonably sandboxed, but string-replacement of field names before eval is fragile (substring collisions); consider `ast` parsing or a real expression evaluator long-term.

### 2.8 Test coverage blind spots — Minor
- `undatum/cmds/db_load.py`, `pipeline_templates.py`, `plotter.py` are **not referenced by any test file** (grep-based; `db_load` may be partially exercised via `db query` tests — verify).
- No current coverage number exists (artifacts stale; `pytest-cov` missing from `.venv`). Install `pytest-cov` into the active venv and regenerate.
- 22 skipped tests, concentrated in `tests/test_ingester.py` (DB-dependent, expected).
- **4,199 warnings** per run, overwhelmingly `qddate`/`pyparsing` deprecations — noise that hides real warnings; consider pinning/filtering.

### 2.9 Dependency and packaging notes — Minor
- Core deps unpinned in `pyproject.toml`: `duckdb`, `elasticsearch`, `iterabledata` (the engine everything routes through since 1.4.0), `pydantic`, `pyyaml`, `typer`, `requests`, `xxhash` and others have no floor version. At minimum pin floors for `iterabledata`, `duckdb`, `pydantic`.
- `requires-python = ">=3.9"` and CI tests 3.9–3.13, but classifiers list only Python 3.9 — add 3.10–3.13 classifiers.
- Pre-commit pins (`ruff v0.6.9`, `black 24.8.0`) lag current releases; CI installs unpinned `ruff`/`black` — pin CI to the same versions as pre-commit to avoid formatter-version whack-a-mole (likely contributor to the 18 black-dirty files).
- `mypy` in CI is `continue-on-error: true` with a known backlog (tracked in `dev/docs/IMPROVEMENT_PLAN_2026-06.md`, workstream B1) — fine as a plan, but it means type-checking currently provides no gate.

## 3. What's in good shape

- **Layered architecture is clean**: `__main__` → `core.py` → `cli/` (wrappers) → `cmds/` (implementations) → `common/` (shared utils). Only 1 intra-`cmds` import; zero upward imports from `common/`; no circular-import smell. `openwiki/` docs match this structure exactly (fresh as of 2026-07-08, at HEAD).
- **Error handling is consistent**: 144 raises of the `UndatumError` hierarchy, 0 bare excepts, 0 generic `raise Exception`, only 5 `sys.exit` + 5 `typer.Exit` across `cmds/`; centralized handling in `__main__.py` via `handle_command_error`. Only 1 TODO/FIXME in the entire package.
- **README is comprehensive**: 49 command sections covering all 45 top-level commands plus sub-apps; install extras match `pyproject.toml`.
- **CHANGELOG is current and high quality**: Keep-a-Changelog format, version 1.5.0 entry matches code (e.g. `api openapi`, `{data, pagination}` envelope), and version strings are consistent across `__init__.py`/`pyproject.toml`/CHANGELOG.
- **Test fixtures are strong**: 25 fixture files spanning compression codecs (gz/xz/bz2/zst/lz4/br/zip) and formats (avro/orc/parquet/xls/xlsx/bson/jsonl).
- **Hygiene basics**: `htmlcov/`, `build/`, `dist/`, `venv/` are untracked; pre-commit hooks configured; plugin system with failure-isolated registration (`core.py:62-72`).

## 4. Recommended priority order

1. Restore `tests/fixtures/2cols6rows.csv` (or re-point the 4 test files) and fix the 7 remaining failures before committing the current change set.
2. Run `ruff check --fix` + review the 4 B023 closures manually, then `black undatum/ tests/`.
3. Fix CI branch triggers (`main` → `master`, or standardize on one branch name).
4. Update `AGENTS.md` (version, architecture map, CI matrix); delete stale `dist/` artifacts and stale coverage outputs.
5. `openspec archive` the ~20 shipped change proposals.
6. Regenerate coverage: install `pytest-cov` in one canonical venv, add a coverage gate/report to CI.
7. Add floor pins for `iterabledata`/`duckdb`/`pydantic`; align pre-commit and CI linter versions.
