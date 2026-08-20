---
title: "schema"
description: "undatum schema command reference"
---
# `schema`

Generates data schemas from files. Supports multiple output formats including YAML, JSON, Cerberus, JSON Schema, Avro, and Parquet.

```bash
# Generate schema in default YAML format
undatum schema data.jsonl

# Generate schema in JSON Schema format
undatum schema data.jsonl --format jsonschema

# Generate schema in Avro format
undatum schema data.jsonl --format avro

# Generate schema in Parquet format
undatum schema data.jsonl --format parquet

# Generate Cerberus schema (for backward compatibility with deprecated `scheme` command)
undatum schema data.jsonl --format cerberus

# Save to file
undatum schema data.jsonl --output schema.yaml

# Nested JSONL: unfold dict fields onto dotted paths
undatum schema nested.jsonl --flatten-nested --format jsonschema
undatum schema nested.jsonl --flatten-nested --max-nested-depth 2
undatum schema nested.jsonl --flatten-nested --keep-nested-parents

# Named Excel sheet
undatum schema workbook.xlsx --table Sheet2

# Validate rows against the inferred schema (not rule-pack validation)
undatum schema data.jsonl --validate --outtype json
undatum schema data.jsonl --validate --strict
undatum schema data.jsonl --validate --sample-size 500

# Generate schema with AI-powered field documentation
undatum schema data.jsonl --autodoc --output schema.yaml
```

**Supported schema formats:**
- `yaml` (default) - YAML format with full schema details
- `json` - JSON format with full schema details
- `cerberus` - Cerberus validation schema format (for backward compatibility with deprecated `scheme` command)
- `jsonschema` - JSON Schema (W3C/IETF standard) - Use for API validation, OpenAPI specs, and tool integration
- `avro` - Apache Avro schema format - Use for Kafka message schemas and Hadoop data pipelines
- `parquet` - Parquet schema format - Use for data lake schemas and Parquet file metadata

**Use cases:**
- **JSON Schema**: API documentation, data validation in web applications, OpenAPI specifications
- **Avro**: Kafka message schemas, Hadoop ecosystem integration, schema registry compatibility
- **Parquet**: Data lake schemas, Parquet file metadata, analytics pipeline definitions
- **Cerberus**: Python data validation (`schema --format cerberus`; the legacy `scheme` command is deprecated)

**Examples:**

```bash
# Generate JSON Schema for API documentation
undatum schema api_data.jsonl --format jsonschema --output api_schema.json

# Generate Avro schema for Kafka
undatum schema events.jsonl --format avro --output events.avsc

# Generate Parquet schema for data lake
undatum schema data.csv --format parquet --output schema.json

# Generate Cerberus schema (deprecated, use schema command instead)
undatum schema data.jsonl --format cerberus --output validation_schema.json
```

**Note:** The `scheme` command is deprecated. Use `undatum schema --format cerberus` instead. The `scheme` command will show a deprecation warning but continues to work for backward compatibility.
