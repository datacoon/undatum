---
title: "examples"
description: "undatum examples command reference"
---
# `examples`

Manage and execute example recipes for common data processing tasks. Provides a library of copy-paste ready recipes that demonstrate best practices.

```bash
# List all available recipes
undatum examples list

# List recipes by category
undatum examples list --category conversion

# Show recipe details
undatum examples show csv-to-jsonl

# Run a recipe with variables
undatum examples run csv-to-jsonl --var input=data.csv --var output=data.jsonl

# Preview commands without executing
undatum examples run data-validation --var input=data.jsonl --var rules=rules.yml --dry-run

# Interactive mode (prompt for variables)
undatum examples run database-query-export --interactive
```

**Recipe Categories:**
- **conversion** - Data format conversion recipes
- **validation** - Data validation and quality checks
- **database** - Database query and load operations
- **analysis** - Data profiling and analysis
- **transformation** - Data cleaning and transformation

**Available Recipes:**
- `csv-to-jsonl` — Convert CSV to JSONL format
- `data-validation` — Validate data using validation rules
- `database-query-export` — Query database and export results
- `data-profiling` — Profile dataset with statistics and documentation
- `data-cleaning` — Clean data by removing duplicates and filling missing values
- `api-serve-data` — Discover and serve a file-backed Data API

**Recipe Format:**

Recipes ship inside the package under `undatum/recipes/` (also mirrored in the repo at `examples/recipes/`):

```yaml
name: recipe-name
description: Recipe description
category: category-name
tags:
  - tag1
  - tag2

variables:
  input:
    description: Input file path
    required: true
  output:
    description: Output file path
    default: "output.jsonl"

commands:
  - description: Command description
    command: undatum convert ${input} ${output}

example: |
  undatum examples run recipe-name --var input=data.csv
```

**Features:**
- **Variable substitution**: Use `${variable}` or `$variable` in commands
- **Dry-run mode**: Preview commands before execution
- **Interactive mode**: Prompt for variable values
- **Category filtering**: Filter recipes by category or tag
- **Copy-paste ready**: Recipes are executable commands
