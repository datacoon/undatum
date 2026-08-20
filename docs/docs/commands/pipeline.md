---
title: "pipeline"
description: "undatum pipeline command reference"
---
# `pipeline`

Run and validate multi-step YAML/JSON workflows. Each step invokes an undatum command in-process (not a subprocess). Interactive commands (`tui`, `web`) are not valid steps.

```bash
# Validate before running
undatum pipeline validate my-pipeline.yml

# Document the step graph as Markdown + Mermaid
undatum pipeline doc my-pipeline.yml --output pipeline.md

# Execute with variable overrides
undatum pipeline run my-pipeline.yml --var input=data.csv --var output=out/

# Dry-run (print resolved steps without executing)
undatum pipeline run my-pipeline.yml --dry-run

# List built-in templates and scaffold a new pipeline
undatum pipeline templates list
undatum pipeline templates init basic-cleaning --output pipeline.yml
```

Built-in templates: `basic-cleaning`, `data-quality`, `profile-dataset`, `s3-etl`, `jsonl-normalization`.

## Pipeline YAML

A pipeline file is a mapping with `steps` (required) and optional `variables`.

```yaml
variables:
  input_file: ${input_file}
  output_file: ${output_file}

steps:
  - name: convert_format
    command: convert
    args:
      input: ${input_file}
      output: /tmp/data.jsonl
      format_out: jsonl

  - name: drop_dupes
    command: dedup
    args:
      input: /tmp/data.jsonl
      output: ${output_file}
      keys: user_id
      keep: first
```

### Variables

Substitution at `pipeline run` uses `${name}` or `$name`, from (lowest to highest) process environment, the file's `variables:` map, then `--var name=value`. Later steps can refer to a previous step's output as `$step_name` when that step produced a file.

Shipped templates also use `${name:-default}` **in the template source**. `pipeline templates init` expands those defaults when writing the scaffolded YAML. The runtime parser does not implement `:-` defaults.

### Steps

Each step must have:

| Key | Meaning |
|-----|---------|
| `name` | Unique step id (used in logs and as `$name` for later outputs) |
| `command` | A live CLI command (`convert`, `dedup`, `sql`, `package`, …). `tui` and `web` are rejected. |
| `args` | Mapping of argument names to values |

`args` keys are mapped onto the command's Typer parameters:

- `input` / `input_file` / `fromfile` become the positional input path (`convert in.csv out.jsonl`, not `--input`).
- `output` / `output_file` / `to` become the positional or `--output` path, depending on the command.
- `keys` maps to `--key-fields` on commands that use that flag.
- `filter` maps to `--filter`.
- Remaining keys become `--kebab-case` options (`format_out` → `--format-out`). Booleans become flags.

If a command accepts an output path and the step omits `output`, the runner injects a temp JSONL file so the next step can use `$step_name`.

### `package` steps

`command: package` is special: set `subcommand` to `create`, `add-resource`, or `validate` (same as `undatum package …`).

```yaml
steps:
  - name: pack
    command: package
    args:
      subcommand: create
      input: data.csv
      output: datapackage.json
```

### Example: convert, filter, SQL

```yaml
variables:
  source: events.csv
  dest: berlin.parquet

steps:
  - name: subset
    command: select
    args:
      input: ${source}
      output: /tmp/subset.jsonl
      fields: id,city,ts
      filter: '`city` == "Berlin"'

  - name: to_parquet
    command: sql
    args:
      query: "SELECT * FROM data"
      input: /tmp/subset.jsonl
      output: ${dest}
      format: parquet
```

Validate with `undatum pipeline validate pipeline.yml` before `run`.
