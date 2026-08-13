<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# undatum — Agent Guide

## Project Overview

**undatum** is a Python command-line tool for data processing and analysis. It provides a unified interface for converting, analyzing, validating, and transforming data across multiple formats with a focus on low memory footprint through streaming.

- **Repository**: https://github.com/datacoon/undatum
- **Version**: 1.6.0
- **License**: MIT
- **Author**: Ivan Begtin <ivan@begtin.tech>
- **Python requirement**: >= 3.9

### Key Capabilities

- Multi-format I/O: CSV, JSON Lines, BSON, XML, XLS, XLSX, Parquet, AVRO, ORC, YAML, TSV, plus 140+ formats via `iterabledata` (geospatial, lakehouse, scientific; see `docs/FORMAT_SUPPORT.md`)
- Compression: ZIP, XZ, GZ, BZ2, ZSTD, LZ4, 7Z (codec profiles `fast`/`balanced`/`max` via iterabledata)
- Streaming processing for large files via `iterabledata`
- Automatic encoding, delimiter, and file type detection
- Data validation with built-in and custom rules
- Statistics and field analysis (including DuckDB-accelerated stats)
- Filtering and querying with expressions
- Schema generation and Frictionless Data Packaging
- Database ingestion: MongoDB, PostgreSQL, DuckDB, MySQL, SQLite, Elasticsearch
- AI-powered dataset documentation via multiple LLM providers
- Optional read-only Data API over files (FastAPI + DuckDB)
- Extensible plugin system

## Technology Stack

| Layer | Libraries |
|-------|-----------|
| CLI framework | `typer`, `click` |
| Console output | `rich` |
| Data processing | `pandas`, `duckdb`, `iterabledata` |
| Serialization | `orjson`, `jsonlines`, `bson`, `xmltodict`, `pyyaml`, `avro`, `pyorc` |
| Compression | `lz4`, `py7zr`, `pyzstd` |
| Excel | `openpyxl`, `xlrd`, `xlwt` |
| Validation | `validators`, `pydantic` |
| Query/filter | DuckDB SQL (`undatum sql`); comparison `--filter` expressions |
| AI providers | `requests` (OpenAI, OpenRouter, Ollama, LM Studio, Perplexity) |
| Testing | `pytest`, `pytest-cov`, `pytest-benchmark` |
| Linting/formatting | `black`, `ruff`, `pylint`, `mypy` |

## Project Structure

```
undatum/
├── __init__.py           # Package metadata (__version__, __author__)
├── __main__.py           # CLI entry point (undatum / data commands)
├── core.py               # Main Typer app and command registration
├── constants.py          # File type lists, date patterns, EU themes
├── utils.py              # General utilities
├── ai/                   # AI provider implementations
│   ├── base.py           # AIService abstract base and errors
│   ├── config.py         # AI configuration loading
│   ├── perplexity.py     # Legacy Perplexity provider
│   ├── providers.py      # OpenAI, OpenRouter, Ollama, LM Studio providers
│   └── schemas.py        # Pydantic schemas for AI output
├── cmds/                 # Individual CLI command implementations (~40 modules)
│   ├── analyzer.py       # analyze command
│   ├── api.py            # api subcommands
│   ├── converter.py      # convert command
│   ├── db_load.py        # db load command
│   ├── db_query.py       # db query command
│   ├── doc.py            # doc command (dataset documentation)
│   ├── extractor.py      # extract command (PDF/table extraction)
│   ├── pipeline.py       # pipeline command
│   ├── statistics.py     # stats command
│   ├── validator.py      # validate command
│   └── ...               # Many more commands
├── common/               # Shared utilities used by commands
│   ├── chunked_io.py     # Chunked file I/O helpers
│   ├── db_connection.py  # Database connection management
│   ├── duckdb_config.py  # DuckDB configuration helpers
│   ├── engine_selector.py# Processing engine selection
│   ├── errors.py         # UndatumError hierarchy and error handling
│   ├── filter.py         # Data filtering logic
│   ├── functions.py      # Dict helper functions (dot-notation access)
│   ├── iterable.py       # Iterable data wrappers
│   ├── masking.py        # Data masking utilities
│   ├── parallel.py       # Parallel processing helpers
│   ├── path_utils.py     # Path and S3 URI utilities
│   ├── pipeline_parser.py# Pipeline DSL parser
│   ├── progress.py       # Progress bar helpers
│   ├── s3_iterable.py    # S3-backed iterable data
│   ├── schema_utils.py   # Schema generation helpers
│   ├── scheme.py         # URL scheme handlers
│   └── validation_rules.py# Built-in validation rules
├── formats/              # Format-specific handlers
│   ├── docx.py           # Word document I/O
│   └── s3.py             # S3 writer and client
├── plugins/              # Plugin system
│   ├── base.py           # Plugin base classes
│   ├── manager.py        # Plugin loading and registration
│   └── registry.py       # Plugin registry
├── sdk/                  # Programmatic Python API
│   └── dataset.py        # Dataset class for method chaining
├── templates/            # Output templates
└── validate/             # Validation rule sets
    ├── commonrules.py    # Common validation rules
    └── ruscodes.py       # Russian-specific codes

tests/                    # Test suite
├── conftest.py           # Pytest fixtures and configuration
├── benchmarks/           # Performance benchmarks
├── fixtures/             # Test fixture files
└── test_*.py             # Unit and integration tests

openspec/                 # OpenSpec spec-driven development
├── AGENTS.md             # OpenSpec workflow instructions
├── project.md            # Project conventions
├── specs/                # Current capability specifications
└── changes/              # Active and archived change proposals

docs/                     # Documentation
├── ERROR_HANDLING.md     # Troubleshooting guide
└── ERROR_HANDLING_PATTERNS.md # Developer error handling patterns
```

## Build and Test Commands

All common tasks are available via `Makefile`:

```bash
# Install for development
make install-dev          # Installs package + black, ruff, mypy, pylint, pytest, pytest-cov, pre-commit, sphinx

# Testing
make test                 # pytest
make test-cov             # pytest --cov=undatum --cov-report=html --cov-report=term

# Code quality
make lint                 # ruff check undatum/ && pylint undatum/
make format               # black undatum/ tests/
make format-check         # black --check undatum/ tests/
make type-check           # mypy undatum/
make check-all            # format-check + lint + type-check + test
make ci                   # Alias for check-all

# Build and clean
make build                # python -m build
make clean                # Remove build artifacts, caches, .pyc files

# Documentation
make docs                 # cd docs && make html
make docs-serve           # cd docs && sphinx-autobuild . _build/html

# Pre-commit hooks
make pre-commit-install   # pre-commit install
make pre-commit-run       # pre-commit run --all-files
```

### Direct Commands (without make)

```bash
# Install package
pip install -e .

# Install with optional extras
pip install -e ".[extract,api]"

# Run tests
pytest -v --tb=short

# Run specific test file
pytest tests/test_converter.py -v

# Run with coverage
pytest --cov=undatum --cov-report=html --cov-report=term
```

## Code Style Guidelines

### Formatting

- **Black** with line length **100** (`pyproject.toml` configures both `black` and `ruff` to 100).
- Target Python version: **3.9**.

### Linting Rules (Ruff)

Selected rule sets:
- `E`, `W` — pycodestyle
- `F` — pyflakes
- `I` — isort
- `B` — flake8-bugbear
- `C4` — flake8-comprehensions
- `UP` — pyupgrade

Ignored:
- `E501` — line too long (handled by black)
- `B008` — function calls in argument defaults
- `C901` — too complex

### Pylint

Configured in `pyproject.toml` with max line length 100 and disabled warnings for:
- `too-few-public-methods`
- `too-many-arguments`
- `too-many-locals`
- `too-many-branches`
- `too-many-statements`
- `too-many-instance-attributes`
- `too-many-positional-arguments`

### Type Checking (mypy)

- `python_version = 3.9`
- `ignore_missing_imports = true` for most third-party libraries
- `check_untyped_defs = true`
- `no_implicit_optional = true`

### Docstrings

Use **Google-style docstrings** with `Args`, `Returns`, `Raises`, and `Example` sections.

Example:
```python
def example_function(param1: str, param2: int = 10) -> bool:
    """Brief description of the function.

    Longer description explaining what the function does.

    Args:
        param1: Description of param1.
        param2: Description of param2 (default: 10).

    Returns:
        Description of return value.

    Raises:
        ValueError: When param1 is invalid.

    Example:
        >>> result = example_function("test", 20)
        >>> print(result)
        True
    """
```

### Imports

- `known-first-party = ["undatum"]` in ruff isort config.
- Group imports: stdlib, third-party, first-party.

## Testing Instructions

### Test Organization

- Tests live in `tests/`.
- Naming convention: `test_*.py` for files, `test_*` for functions, `Test*` for classes.
- Fixtures in `tests/conftest.py`.
- Benchmarks in `tests/benchmarks/` (marked with `@pytest.mark.benchmark`).

### Key Fixtures

- `sample_csv_file` — Creates a temporary CSV with `name,age,city` columns.
- `sample_jsonl_file` — Creates a temporary JSONL with two records.
- `benchmark` — Pytest-benchmark fixture (skips if not installed).

### Running Tests

```bash
# All tests
pytest

# With verbose output
pytest -v

# Specific file
pytest tests/test_core.py -v

# With coverage
pytest --cov=undatum --cov-report=html

# Benchmarks only
pytest tests/benchmarks/ -v
```

### CI

GitHub Actions workflow (`.github/workflows/ci.yml`) runs on:
- Python 3.9, 3.10, 3.11
- Ubuntu latest
- Steps: checkout → setup Python → install package → install pytest, pytest-benchmark → run pytest

## Development Conventions

### Git Workflow

- Create feature branches from `master` (legacy) or `main` (CI targets `main`).
- Use **conventional commits**:
  - `feat:` new feature
  - `fix:` bug fix
  - `docs:` documentation
  - `refactor:` code improvement
- Update `CHANGELOG.md` for user-facing changes.

### OpenSpec Workflow

This project uses **OpenSpec** for spec-driven development. Always consult `openspec/AGENTS.md` when:
- Planning new features or breaking changes
- Making architectural changes
- Requests mention "proposal", "spec", "change", or "plan"

Quick OpenSpec commands:
```bash
openspec list             # Active changes
openspec list --specs     # Current capabilities
openspec validate [id] --strict   # Validate proposal
openspec archive [id] --yes       # Archive completed change
```

### Error Handling

All commands should use the `UndatumError` hierarchy (`undatum/common/errors.py`):

| Exception | Exit Code | Use Case |
|-----------|-----------|----------|
| `UndatumError` | 1 | Base / generic user error |
| `ValidationError` | 1 | Invalid input/parameters |
| `FormatError` | 1 | Unsupported file format |
| `FileNotFoundError` | 1 | Missing file (with typo suggestions) |
| `ConfigurationError` | 2 | Config issues |
| `DependencyError` | 2 | Missing optional dependency |
| `PermissionError` | 3 | File permission denied |
| `DatabaseError` | 3 | DB connection/query failure |

Error handling principles:
- Provide actionable guidance in error messages.
- Suggest similar file names for `FileNotFoundError`.
- Mask passwords in connection URIs for `DatabaseError`.
- Use `handle_command_error()` in `__main__.py` for graceful shutdown.

### Adding New Commands

1. Implement the command logic in `undatum/cmds/<module>.py` as a class.
2. Import and wire the command in `undatum/core.py` using Typer decorators.
3. Add tests in `tests/test_<module>.py` or extend existing test files.
4. Run `make check-all` before committing.

### Plugin Development

Plugins extend undatum without modifying core:
- Inherit from `CommandPlugin`, `ConnectorPlugin`, or `TransformPlugin` in `undatum/plugins/base.py`.
- Register via `plugin_manager.load_all_plugins(app)` (called automatically in `core.py`).

## Security Considerations

- **No secrets in code**: API keys for AI providers are loaded from environment variables or config files (`undatum.yaml`, `~/.undatum/config.yaml`).
- **Connection URI masking**: Database error messages mask passwords before display (`_mask_connection_uri` in `errors.py`).
- **S3 credentials**: Use standard AWS credential chain; never hardcode keys.
- **Input validation**: Use `validators` library and `pydantic` schemas for external inputs.

## Optional Features

Install with extras for additional capabilities:

```bash
# PDF/table extraction (pdfplumber, pdf2image, pytesseract, textract)
pip install "undatum[extract]"

# Data API server (FastAPI + uvicorn)
pip install "undatum[api]"
```

## Useful References

- `README.md` — User-facing documentation and quick start.
- `CONTRIBUTING.md` — Detailed contribution guidelines.
- `WORKFLOW_GUIDE.md` — OpenSpec workflow quick reference.
- `docs/ERROR_HANDLING.md` — Troubleshooting common errors.
- `docs/ERROR_HANDLING_PATTERNS.md` — Patterns for developers adding error handling.
- `CHANGELOG.md` — Version history.

## OpenWiki

This repository has documentation located in the /openwiki directory.

Start here:
- [OpenWiki quickstart](openwiki/quickstart.md)

OpenWiki includes repository overview, architecture notes, workflows, domain concepts, operations, integrations, testing guidance, and source maps.

When working in this repository, read the OpenWiki quickstart first, then follow its links to the relevant architecture, workflow, domain, operation, and testing notes.
