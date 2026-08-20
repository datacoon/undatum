---
title: "pipeline"
description: "undatum pipeline command reference"
---
# `pipeline`

Run and validate multi-step YAML/JSON workflows. Each step invokes an undatum command. See [Pipeline Workflows](/commands/pipeline) for the full DSL, templates, and examples.

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

Built-in templates: `basic-cleaning`, `data-quality`, `profile-dataset`, `s3-etl`.
