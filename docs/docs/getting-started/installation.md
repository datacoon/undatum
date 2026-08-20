---
title: "Installation"
description: "Install undatum with uv, pipx, pip, or from source"
---
# Installation

### Using uv or pipx (recommended for CLI use)

```bash
uv tool install undatum
# or
pipx install undatum
```

### Using pip

```bash
pip install --upgrade pip setuptools
pip install undatum
```

Dependencies are declared in `pyproject.toml` and will be installed automatically by modern versions of `pip` (23+), including **pyarrow** for Parquet. If you see missing-module errors after installation, upgrade `pip` and retry.

### macOS

Preferred paths:

```bash
brew install pipx && pipx install undatum
# or
uv tool install undatum
```

A Homebrew formula for undatum itself is tracked separately; `pipx`/`uv` are the supported macOS install methods today.

Release tags also publish **PyInstaller single-file binaries** (Linux, macOS, Windows) as GitHub Actions artifacts. These are ops-oriented; `pipx`/`uv` remain the supported install paths for most users.

A man page ships with the package (`man undatum` after install, or `make man` to regenerate `man/undatum.1`).

### Optional extras

Some features require optional dependencies, installed as extras. This is the canonical list; feature sections elsewhere in the docs link back here.

| Extra | Enables |
|-------|---------|
| `api` | Data API server (`undatum api`, FastAPI + uvicorn + httpx) |
| `extract` | Document extraction (`undatum extract`, PDF/DOC/DOCX tables and text) |
| `plot` | Plotting (`undatum plot`, matplotlib) |
| `tui` | Interactive terminal UI (`undatum tui`, Textual) |
| `web` | Local web UI (`undatum web`, FastAPI + Jinja2) |
| `mcp` | MCP server for AI agents (`undatum mcp serve`) |
| `langchain` | LangChain agent tools |
| `polars`, `dask` | DataFrame interop from the `Dataset` SDK |
| `s3` | S3 cloud storage support (boto3) |
| `gcs` | Google Cloud Storage (`gs://` / `gcs://`, gcsfs) |
| `azure` | Azure Blob / ADLS (`az://` / `abfs://`, adlfs) |
| `cloud` | Multi-cloud storage via fsspec (S3 + GCS + Azure) |
| `postgres`, `mysql`, `mssql`, `clickhouse` | Database connectors |
| `frictionless` | Full Frictionless Data Package validation |
| `lakehouse` | Delta / Iceberg / Lance / DuckLake / Hudi via iterabledata |
| `gis` | Geospatial and LiDAR formats |
| `scientific` | MATLAB, geophysical, and HDF5 formats |
| `access` | Microsoft Access (`.mdb` / `.accdb`) |
| `compression` | Extra codecs (snappy, brotli, lzo) |

```bash
pip install "undatum[api]"
pip install "undatum[extract]"
pip install "undatum[plot]"
pip install "undatum[tui]"
pip install "undatum[web]"
pip install "undatum[mcp]"
pip install "undatum[langchain]"
pip install "undatum[polars]"
pip install "undatum[dask]"
pip install "undatum[s3]"
pip install "undatum[gcs]"
pip install "undatum[azure]"
pip install "undatum[cloud]"
pip install "undatum[postgres]"
pip install "undatum[mysql]"
pip install "undatum[mssql]"
pip install "undatum[clickhouse]"
pip install "undatum[frictionless]"
pip install "undatum[lakehouse]"
pip install "undatum[gis]"
pip install "undatum[scientific]"
pip install "undatum[access]"
pip install "undatum[compression]"

# Combine extras in one install
pip install "undatum[extract,api]"
```

After installation both `undatum` and the shorter `data` command are available:

```bash
undatum --version
undatum headers data.csv
data headers data.csv   # same thing
```

### Shell completion

Typer provides built-in shell completion. Install it for your shell:

```bash
# Bash
undatum --install-completion bash

# Zsh
undatum --install-completion zsh

# Fish
undatum --install-completion fish
```

To preview completion scripts without installing:

```bash
undatum --show-completion bash
```

### Requirements

- Python 3.9 or greater

### Install from source

```bash
python -m pip install --upgrade pip setuptools wheel
python -m pip install .
# or build distributables
python -m pip install build && python -m build
```

## Next steps

- [Quick start](/getting-started/quick-start)
- [When to use undatum](/getting-started/when-to-use)
- [Format support](/formats/)
