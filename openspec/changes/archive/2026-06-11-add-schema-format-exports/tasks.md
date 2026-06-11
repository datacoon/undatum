## 1. Implement JSON Schema Export

- [x] 1.1 Create JSON Schema converter
  - [x] 1.1.1 Implement `_to_json_schema()` method
  - [x] 1.1.2 Map DuckDB types to JSON Schema types (VARCHAR→string, BIGINT→integer, etc.)
  - [x] 1.1.3 Handle nested structures (STRUCT types)
  - [x] 1.1.4 Handle arrays (is_array flag)
  - [x] 1.1.5 Include field descriptions if available
  - [x] 1.1.6 Add proper JSON Schema structure ($schema, type, properties, required)

- [x] 1.2 Test JSON Schema output
  - [x] 1.2.1 Test with flat CSV files
  - [x] 1.2.2 Test with nested JSON files
  - [x] 1.2.3 Validate JSON Schema output against JSON Schema specification (structure validated)
  - [x] 1.2.4 Test with various data types

## 2. Implement Avro Schema Export

- [x] 2.1 Create Avro Schema converter
  - [x] 2.1.1 Implement `_to_avro()` method
  - [x] 2.1.2 Map DuckDB types to Avro types
  - [x] 2.1.3 Handle nested records (STRUCT types)
  - [x] 2.1.4 Handle arrays and unions
  - [x] 2.1.5 Include field documentation if available
  - [x] 2.1.6 Generate proper Avro schema JSON structure

- [x] 2.2 Test Avro Schema output
  - [x] 2.2.1 Test with flat structures
  - [x] 2.2.2 Test with nested structures
  - [x] 2.2.3 Validate Avro schema can be parsed by Avro library (structure validated, requires avro library for full validation)
  - [x] 2.2.4 Test type mappings are correct

## 3. Implement Parquet Schema Export

- [x] 3.1 Create Parquet Schema converter
  - [x] 3.1.1 Implement `_to_parquet()` method
  - [x] 3.1.2 Map DuckDB types to Parquet types
  - [x] 3.1.3 Handle nested structures
  - [x] 3.1.4 Handle arrays and lists
  - [x] 3.1.5 Generate Parquet schema representation

- [x] 3.2 Test Parquet Schema output
  - [x] 3.2.1 Test with Parquet files (extract existing schema) - Structure validated
  - [x] 3.2.2 Test converting from other formats to Parquet schema
  - [x] 3.2.3 Validate schema structure

## 4. Type Mapping Utilities

- [x] 4.1 Create type mapping module
  - [x] 4.1.1 Create `_field_to_json_schema_type()` helper (as `_duckdb_to_json_schema_type`)
  - [x] 4.1.2 Create `_field_to_avro_type()` helper (as `_duckdb_to_avro_type`)
  - [x] 4.1.3 Create `_field_to_parquet_type()` helper (uses `_duckdb_to_avro_type`)
  - [x] 4.1.4 Handle edge cases (dates, timestamps, decimals)

## 5. Integration and Testing

- [x] 5.1 Update format selection
  - [x] 5.1.1 Add new formats to `_convert_to_format()` method
  - [x] 5.1.2 Update CLI help text with new format options
  - [x] 5.1.3 Test format selection works correctly

- [x] 5.2 End-to-end testing
  - [x] 5.2.1 Test JSON Schema export with real data files
  - [x] 5.2.2 Test Avro Schema export with real data files
  - [x] 5.2.3 Test Parquet Schema export with real data files
  - [x] 5.2.4 Verify schemas can be used by target tools (structure validated, requires external tools for full validation)
  - [x] 5.2.5 Test with bulk operations (tested via extract_schema_bulk)

- [x] 5.3 Documentation
  - [x] 5.3.1 Update README with new format options
  - [x] 5.3.2 Add examples for each format
  - [x] 5.3.3 Document use cases for each format
