---
title: "package"
description: "undatum package command reference"
---
# `package`

Generates, extends, and validates Frictionless Data Package descriptors (`datapackage.json`) from one or more data files. Supports optional package metadata, schema inference, and AI-powered metadata generation with `--autodoc`.

```bash
# Create datapackage.json for a single file
undatum package create data.csv --output datapackage.json
undatum package create workbook.xlsx --table Sheet2 --output datapackage.json
undatum package create nested.jsonl --flatten-nested --output datapackage.json

# Create a package directory with data file copies
undatum package create data.csv --package-dir out/package

# Zip the materialized package directory
undatum package create data.csv --package-dir out/package --zip out/package.zip

# Add another resource to an existing package
undatum package add-resource out/package/datapackage.json new.csv

# Validate a package descriptor
undatum package validate out/package/datapackage.json

# Provide metadata and enable AI metadata generation
undatum package create data.csv --title "Sales data" --keywords sales,finance \
  --autodoc --ai-provider openai --ai-model gpt-4o-mini
```

**Subcommands:**
- `create` — generate a new descriptor (default workflow)
- `add-resource` — append resources to an existing descriptor
- `validate` — validate descriptor structure (full checks with `pip install undatum[frictionless]`)

**Metadata options:**
- `--name`, `--title`, `--description`, `--keywords`
- `--licenses` (semicolon-separated entries, e.g. `name=MIT;name=ODC-PDDL-1.0`)
- `--sources` (semicolon-separated entries, e.g. `title=World Bank,path=https://...`)
- `--contributors` (semicolon-separated entries, e.g. `title=Jane Doe,email=jane@example.com`)
- `--version` - Package version string

**Features:**
- **Frictionless profile**: Emits `profile: tabular-data-package` with resource `format`/`mediatype`
- **Schema inference**: Automatically infers field types, descriptions, and uniqueness constraints
- **Multiple resources**: Package multiple files as separate resources
- **Remote URIs**: Support for HTTP/HTTPS URLs as resource paths
- **Package directory**: Bundle `datapackage.json` with data file copies
- **AI metadata**: Use `--autodoc` to generate metadata with AI assistance (single-pass, no duplicate LLM calls)
- **Streaming-safe**: Processes large datasets without loading everything into memory
- **Python SDK**: `Dataset.read("data.csv").package(output="datapackage.json")`

**Additional options:**
- `--package-dir`: Create a package directory with data file copies
- `--zip`: Create a ZIP archive of the package directory (requires `--package-dir`)
- `--autodoc`: Enable AI-powered metadata generation (reuses `doc` command logic)
- `--engine`: Processing engine (`auto` or `duckdb`)
- `--delimiter`, `--encoding`, `--tagname`, `--start-line`, `--start-page`: Passed through to analysis and sampling
- `--objects-limit`: Maximum objects to analyze for schema inference (default: 10000)
- `--sample-size`: Number of sample records for metadata inference (default: 10)
