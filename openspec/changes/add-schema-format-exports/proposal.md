# Change: Add Standard Schema Format Exports

## Why

The schema command currently only supports custom YAML format and Cerberus format. Industry-standard schema formats like JSON Schema, Avro Schema, and Parquet Schema are not supported, limiting integration with other tools and systems.

JSON Schema is the most widely used schema format for APIs, data validation, and tool integration. Avro Schema is essential for Kafka and Hadoop ecosystems. Parquet Schema is important for Parquet-based data lake workflows.

Adding support for these standard formats will:
- Enable integration with standard tools and systems
- Support data pipeline workflows (Kafka, Hadoop, data lakes)
- Provide industry-standard schema definitions
- Improve interoperability with other data tools

## What Changes

- **ADDED**: `undatum/cmds/schemer.py` - `_to_json_schema()` method to convert TableSchema to JSON Schema format
- **ADDED**: `undatum/cmds/schemer.py` - `_to_avro()` method to convert TableSchema to Avro schema format
- **ADDED**: `undatum/cmds/schemer.py` - `_to_parquet()` method to convert TableSchema to Parquet schema format
- **ADDED**: `undatum/cmds/schemer.py` - Type mapping utilities for converting DuckDB types to standard schema types
- **MODIFIED**: `undatum/cmds/schemer.py` - Update `_convert_to_format()` to support new formats
- **MODIFIED**: `undatum/core.py` - Add format options to `--format` parameter help text

All changes are additive and maintain backward compatibility.

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/cmds/schemer.py` - Add format conversion methods
  - `undatum/core.py` - Update help text
- **Dependencies**: No new dependencies required (uses standard Python libraries)
- **Backward compatibility**: All existing functionality preserved, only adds new format options
