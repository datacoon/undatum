## 1. Add Format Support to Schema Command

- [x] 1.1 Add format conversion infrastructure
  - [x] 1.1.1 Create `_convert_to_format()` method in `Schemer` class
  - [x] 1.1.2 Implement `_to_cerberus()` converter (reuse logic from `generate_scheme()`)
  - [x] 1.1.3 Implement `_to_json_schema()` converter
  - [x] 1.1.4 Implement `_to_avro()` converter
  - [x] 1.1.5 Implement `_to_parquet()` converter
  - [x] 1.1.6 Add type mapping utilities for format conversion

- [x] 1.2 Update schema command interface
  - [x] 1.2.1 Add `--format` parameter to `schema` command in `core.py`
  - [x] 1.2.2 Update `extract_schema()` to accept and use format parameter
  - [x] 1.2.3 Update `extract_schema_bulk()` to support format selection
  - [x] 1.2.4 Ensure all formats work with output options (file/stdout)

## 2. Deprecate Scheme Command

- [x] 2.1 Add deprecation warning
  - [x] 2.1.1 Modify `scheme` command to show deprecation warning
  - [x] 2.1.2 Redirect `scheme` to `schema --format cerberus`
  - [x] 2.1.3 Update help text to indicate deprecation

- [x] 2.2 Update documentation
  - [x] 2.2.1 Update README.md to show unified `schema` command
  - [x] 2.2.2 Add migration guide from `scheme` to `schema --format cerberus` (included in README note)
  - [x] 2.2.3 Update examples to use new command (examples updated in README)

## 3. Testing

- [x] 3.1 Test format conversions
  - [x] 3.1.1 Test Cerberus format output matches current `scheme` output
  - [x] 3.1.2 Test JSON Schema format output
  - [x] 3.1.3 Test Avro format output
  - [x] 3.1.4 Test Parquet format output
  - [x] 3.1.5 Test YAML format (default) still works

- [x] 3.2 Test deprecation path
  - [x] 3.2.1 Test `scheme` command shows deprecation warning
  - [x] 3.2.2 Test `scheme` command still produces correct output
  - [x] 3.2.3 Test migration path works correctly

- [x] 3.3 Test backward compatibility
  - [x] 3.3.1 Test existing scripts using `scheme` command still work
  - [x] 3.3.2 Test bulk operations with different formats
