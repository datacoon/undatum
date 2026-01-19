## 1. Critical Fixes

- [x] 1.1 Implement output format support (`_write_schema_output()` function)
  - [x] 1.1.1 Support text format with table formatting using `tabulate`
  - [x] 1.1.2 Support JSON format (json.dumps with indent)
  - [x] 1.1.3 Support YAML format (existing, but ensure it's used)
  - [x] 1.1.4 Respect `--output` option for file vs stdout
  - [x] 1.1.5 Update `extract_schema()` to use new output function
  - [x] 1.1.6 Update `extract_schema_bulk()` to use new output function

- [x] 1.2 Implement AI documentation support
  - [x] 1.2.1 Initialize AI service when `autodoc=True` in `extract_schema()`
  - [x] 1.2.2 Call `get_fields_info()` to populate field descriptions
  - [x] 1.2.3 Handle AI service initialization failures gracefully
  - [x] 1.2.4 Add AI provider/model options to CLI in `core.py`
  - [x] 1.2.5 Pass AI configuration to `extract_schema()` and `extract_schema_bulk()`
  - [ ] 1.2.6 Test with different AI providers

- [x] 1.3 Add record counting
  - [x] 1.3.1 Add DuckDB query to count records in `build_schema()`
  - [x] 1.3.2 Handle JSON/JSONL files with `read_json()`
  - [x] 1.3.3 Handle CSV/TSV files with `read_csv()`
  - [x] 1.3.4 Handle other file types appropriately
  - [x] 1.3.5 Set `table.num_records` with count result
  - [x] 1.3.6 Add error handling for counting failures

- [x] 1.4 Fix bulk mode file discovery
  - [x] 1.4.1 Replace `os.listdir()` with `glob.glob()` for pattern support
  - [x] 1.4.2 Handle both directory paths and glob patterns
  - [x] 1.4.3 Improve file extension detection (handle `.csv.gz` style extensions)
  - [ ] 1.4.4 Test with various glob patterns

## 2. Feature Enhancements

- [x] 2.1 Improve file format support
  - [x] 2.1.1 Import `iterable.helpers.detect.detect_file_type()`
  - [x] 2.1.2 Replace extension-based detection with proper detection
  - [ ] 2.1.3 Add support for XLSX format (requires analyzer-style processing)
  - [ ] 2.1.4 Add support for XLS format (requires analyzer-style processing)
  - [ ] 2.1.5 Add support for XML format (requires analyzer-style processing)
  - [ ] 2.1.6 Add support for DOCX format (requires analyzer-style processing)
  - [x] 2.1.7 Handle compression detection separately from file type

- [x] 2.2 Add engine selection
  - [x] 2.2.1 Add `--engine` parameter to `schema` command in `core.py`
  - [x] 2.2.2 Add `--engine` parameter to `schema_bulk` command
  - [x] 2.2.3 Implement auto-detection logic (duckdb vs iterable)
  - [x] 2.2.4 Add fallback to iterable processing when DuckDB fails
  - [x] 2.2.5 Pass engine option through to `extract_schema()` methods

- [x] 2.3 Improve error handling
  - [x] 2.3.1 Validate input file exists and is readable
  - [x] 2.3.2 Handle unsupported file types gracefully
  - [x] 2.3.3 Provide clear error messages
  - [x] 2.3.4 Add error flags to schema objects (success/error fields)
  - [ ] 2.3.5 Test error scenarios

## 3. Code Quality Improvements

- [x] 3.1 Eliminate code duplication
  - [x] 3.1.1 Create `undatum/common/schema_utils.py`
  - [x] 3.1.2 Move `duckdb_decompose()` to shared module with `use_summarize` parameter
  - [x] 3.1.3 Update `schemer.py` to import from shared module
  - [x] 3.1.4 Update `analyzer.py` to import from shared module
  - [ ] 3.1.5 Test that both modules work correctly

- [x] 3.2 Share constants
  - [x] 3.2.1 Move `DUCKABLE_FILE_TYPES` to `constants.py` (already there)
  - [x] 3.2.2 Move `DUCKABLE_CODECS` to `constants.py` (already there)
  - [x] 3.2.3 Update imports in `schemer.py` and `analyzer.py`
  - [x] 3.2.4 Remove duplicate constant definitions

- [ ] 3.3 Improve type hints and documentation
  - [ ] 3.3.1 Add type hints to all functions in `schemer.py`
  - [ ] 3.3.2 Add comprehensive docstrings following Google style
  - [ ] 3.3.3 Document all parameters and return types

## 4. Testing

- [x] 4.1 Unit tests for output formats
  - [x] 4.1.1 Test text output format
  - [x] 4.1.2 Test JSON output format
  - [x] 4.1.3 Test YAML output format
  - [x] 4.1.4 Test file output vs stdout

- [ ] 4.2 Integration tests
  - [ ] 4.2.1 Test with various file formats (CSV, JSONL, XLSX, XML, etc.)
  - [ ] 4.2.2 Test with compressed files
  - [ ] 4.2.3 Test bulk mode with glob patterns
  - [ ] 4.2.4 Test engine selection

- [ ] 4.3 AI documentation tests
  - [ ] 4.3.1 Test with different AI providers
  - [ ] 4.3.2 Test error handling when AI service fails
  - [ ] 4.3.3 Test field description population

- [ ] 4.4 Error handling tests
  - [ ] 4.4.1 Test with non-existent files
  - [ ] 4.4.2 Test with unreadable files
  - [ ] 4.4.3 Test with unsupported file types
  - [ ] 4.4.4 Test with invalid options
