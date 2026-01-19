# Schema Command Review and Improvement Recommendations

**Date:** 2025-01-27  
**Reviewer:** AI Assistant  
**Command:** `undatum schema`, `undatum schema_bulk`, and `undatum scheme`  
**Files Reviewed:** `undatum/cmds/schemer.py`, `undatum/core.py`, `undatum/common/scheme.py`

## Executive Summary

The schema command provides basic schema extraction capabilities but lacks several features and consistency patterns found in other commands (particularly `analyze`). This review identifies gaps, inconsistencies, and opportunities for improvement by aligning with patterns used in the `analyzer`, `ingester`, and other commands.

## Current Implementation Analysis

### Strengths

1. **Core Functionality Works**: Basic schema extraction from CSV, JSON, JSONL, and Parquet files functions correctly
2. **DuckDB Integration**: Leverages DuckDB for efficient schema detection from nested structures
3. **Bulk Processing**: Supports processing multiple files with distinct/per-file modes
4. **Pydantic Models**: Uses well-structured `FieldSchema` and `TableSchema` models
5. **Recursive Schema Detection**: Handles nested structures up to 4 levels deep

### Critical Issues

#### 1. **Output Format Not Respected**
**Location:** `extract_schema()` method (line 223-226)

**Problem:**
- The `outtype` option is passed but ignored - always outputs YAML
- The `output` option is passed but ignored - always prints to stdout
- No support for JSON or text output formats

**Impact:** Users cannot control output format or destination, limiting integration with other tools.

**Comparison with `analyzer`:**
- `analyzer` properly implements `_write_analysis_output()` with text/json/yaml support
- `analyzer` handles file output vs stdout correctly

#### 2. **AI Documentation Not Implemented**
**Location:** `extract_schema()` method (line 223-226)

**Problem:**
- `autodoc` and `lang` options are passed but never used
- No AI service integration for field descriptions
- Missing feature that's advertised in CLI help text

**Impact:** Users cannot get AI-generated field descriptions in single-file mode, even though the option exists.

**Comparison with `analyzer`:**
- `analyzer` properly initializes AI service when `autodoc=True`
- `analyzer` calls `get_fields_info()` to populate field descriptions
- `analyzer` handles AI service initialization failures gracefully

#### 3. **Missing Record Count**
**Location:** `build_schema()` function (line 191-210)

**Problem:**
- Schema extraction doesn't count total records in the file
- `TableSchema.num_records` is never set (defaults to -1)
- Users can't see how many records were analyzed

**Impact:** Incomplete schema information, especially important for large files where only a sample is analyzed.

**Comparison with `analyzer`:**
- `analyzer` counts records using DuckDB queries
- `analyzer` reports total records in output

#### 4. **Limited File Format Support**
**Location:** `extract_schema_bulk()` method (line 229-290)

**Problem:**
- Hardcoded file extension list doesn't match analyzer's capabilities
- No support for XLSX, XLS, XML, DOCX formats that `analyzer` supports
- File type detection is simplistic (extension-based only)

**Impact:** Cannot extract schemas from common file formats that other commands support.

**Comparison with `analyzer`:**
- `analyzer` uses `iterable.helpers.detect.detect_file_type()` for proper detection
- `analyzer` handles compression detection separately
- `analyzer` supports 10+ file formats

#### 5. **No Compression Detection**
**Location:** `build_schema()` and `extract_schema_bulk()` methods

**Problem:**
- File type detection only looks at extension (line 193: `fileext = filename.rsplit('.', 1)[-1]`)
- Doesn't detect compression (gzip, zstd, etc.) separately
- Doesn't use the same detection utilities as other commands

**Impact:** May fail on compressed files or misidentify file types.

**Comparison with `analyzer`:**
- `analyzer` detects file type and compression separately
- `analyzer` uses `iterable.helpers.detect` for proper detection
- `analyzer` handles compressed files correctly

#### 6. **Code Duplication**
**Location:** `duckdb_decompose()` function (line 47-121)

**Problem:**
- Nearly identical function exists in `analyzer.py` (line 52-161)
- Only difference: analyzer uses `summarize` instead of `describe`
- Duplication makes maintenance harder

**Impact:** Bug fixes and improvements must be applied in multiple places.

**Recommendation:** Extract to shared utility module or have schemer import from analyzer.

#### 7. **No Engine Selection**
**Location:** `extract_schema()` and `extract_schema_bulk()` methods

**Problem:**
- No `engine` parameter (auto/duckdb/iterable) like `analyzer` has
- Always uses DuckDB, which may not work for all file types
- No fallback mechanism

**Impact:** Cannot handle files that DuckDB doesn't support well.

**Comparison with `analyzer`:**
- `analyzer` supports `engine` parameter with auto-detection
- `analyzer` falls back to iterable processing for unsupported formats

#### 8. **Inconsistent Error Handling**
**Location:** Throughout `schemer.py`

**Problem:**
- No error handling for unsupported file types
- No validation of input parameters
- Silent failures in some cases (e.g., `extract_schema_bulk` with invalid directory)

**Impact:** Poor user experience when errors occur.

**Comparison with `analyzer`:**
- `analyzer` sets `report.success = False` and `report.error` on failures
- `analyzer` provides clear error messages

#### 9. **Bulk Mode File Discovery Issues**
**Location:** `extract_schema_bulk()` method (line 231-239)

**Problem:**
- Uses `os.listdir()` which doesn't support glob patterns
- CLI accepts glob patterns but implementation doesn't handle them
- File extension matching is simplistic (only checks last extension)

**Impact:** Cannot use glob patterns like `data/*.csv` as documented.

**Comparison with other commands:**
- Most commands use `glob.glob()` for pattern matching
- `ingester` properly handles glob patterns

#### 10. **Missing Output Formatting**
**Location:** `extract_schema()` method

**Problem:**
- No human-readable text output format
- Always outputs raw YAML, even when `outtype=text`
- No table formatting for better readability

**Impact:** Schema output is not user-friendly for terminal viewing.

**Comparison with `analyzer`:**
- `analyzer` has beautiful text output with tables using `tabulate`
- `analyzer` formats numbers, file sizes, and descriptions nicely

#### 11. **Limited Schema Format Support**
**Location:** `extract_schema()` and `generate_scheme()` methods

**Problem:**
- `undatum schema` only outputs custom YAML format (Pydantic models), not standard formats
- `undatum scheme` only supports Cerberus format, despite `--stype` parameter claiming "other schema formats"
- No support for JSON Schema (most widely used standard)
- No support for Avro Schema, Parquet Schema, or other standard formats
- The `--stype` parameter in `scheme` command is completely ignored (line 406 in core.py)

**Impact:** Cannot integrate with tools that require standard schema formats. Users must manually convert schemas or use other tools.

**Current State:**
- **Cerberus Schema**: Supported via `undatum scheme` command (outputs JSON)
- **Custom YAML**: Supported via `undatum schema` command (Pydantic models)
- **JSON Schema**: ❌ NOT supported
- **Avro Schema**: ❌ NOT supported (converter can read Avro but can't export schema)
- **Parquet Schema**: ❌ NOT supported (can read Parquet but can't export schema)
- **XSD/XML Schema**: ❌ NOT supported
- **Protocol Buffers**: ❌ NOT supported

**Comparison with industry standards:**
- Most data tools support JSON Schema as the primary format
- Many tools require Avro Schema for data pipelines
- Parquet Schema is essential for Parquet-based workflows

#### 12. **Command Confusion: `schema` vs `scheme`**
**Location:** `core.py` - two separate commands

**Problem:**
- Two confusingly similar commands: `undatum schema` and `undatum scheme`
- Different purposes but similar names cause user confusion
- `schema` outputs YAML (Pydantic models), `scheme` outputs JSON (Cerberus)
- No clear documentation explaining when to use which
- Duplicate functionality with different implementations

**Impact:** Users don't know which command to use. The distinction between "schema" and "scheme" is unclear.

**Current Commands:**
- `undatum schema` → Extracts schema, outputs YAML (Pydantic models)
- `undatum schema_bulk` → Bulk schema extraction
- `undatum scheme` → Generates Cerberus validation schema, outputs JSON

**Recommendation:** Merge into unified `schema` command with format selection.

## Schema Format Support Analysis

### Currently Supported Formats

1. **Cerberus Schema** (via `undatum scheme`)
   - **Status**: ✅ Supported
   - **Output**: JSON format
   - **Use Case**: Python data validation with Cerberus library
   - **Limitation**: `--stype` parameter is ignored; only Cerberus is generated

2. **Custom YAML Format** (via `undatum schema`)
   - **Status**: ✅ Supported
   - **Output**: YAML serialization of Pydantic `TableSchema`/`FieldSchema` models
   - **Use Case**: Internal undatum format, not a standard
   - **Limitation**: Not compatible with standard tools

### Missing Standard Formats

1. **JSON Schema** ❌
   - **Industry Standard**: Most widely used schema format
   - **Use Cases**: API validation, data contracts, tool integration
   - **Priority**: **CRITICAL** - Should be highest priority addition

2. **Avro Schema** ❌
   - **Industry Standard**: Used in Kafka, Hadoop ecosystems
   - **Use Cases**: Data pipelines, streaming data
   - **Priority**: **HIGH** - Important for data engineering workflows

3. **Parquet Schema** ❌
   - **Industry Standard**: Native Parquet format
   - **Use Cases**: Parquet file metadata, data lake management
   - **Priority**: **MEDIUM** - Useful but less critical

4. **XSD/XML Schema** ❌
   - **Industry Standard**: XML validation
   - **Use Cases**: XML data validation, SOAP APIs
   - **Priority**: **LOW** - Less commonly needed

5. **Protocol Buffers Schema** ❌
   - **Industry Standard**: gRPC, microservices
   - **Use Cases**: Service contracts, binary serialization
   - **Priority**: **LOW** - Niche use case

### Schema Format Comparison

| Format | Status | Standard | Use Cases | Priority |
|--------|--------|----------|-----------|----------|
| Cerberus | ✅ | Python-specific | Python validation | Medium |
| Custom YAML | ✅ | Internal | Undatum internal | Low |
| JSON Schema | ❌ | W3C/IETF | APIs, validation, tools | **CRITICAL** |
| Avro Schema | ❌ | Apache | Kafka, Hadoop, pipelines | **HIGH** |
| Parquet Schema | ❌ | Apache | Parquet metadata | Medium |
| XSD | ❌ | W3C | XML validation | Low |
| Protocol Buffers | ❌ | Google | gRPC, services | Low |

## Command Consolidation Plan

### Current State

The codebase has two separate commands with overlapping functionality:

1. **`undatum schema`** (lines 470-491 in core.py)
   - Purpose: Extract schema structure
   - Output: YAML (Pydantic models)
   - Methods: `extract_schema()`, `extract_schema_bulk()`
   - Features: Bulk processing, AI documentation (planned)

2. **`undatum scheme`** (lines 397-423 in core.py)
   - Purpose: Generate validation schema
   - Output: JSON (Cerberus format)
   - Methods: `generate_scheme()`
   - Features: Cerberus validation schema generation

### Problems with Current Approach

1. **User Confusion**: Similar names (`schema` vs `scheme`) are confusing
2. **Duplicate Functionality**: Both extract schema information, just in different formats
3. **Inconsistent Interface**: Different parameters, different output formats
4. **Maintenance Burden**: Two code paths for similar functionality
5. **Missing Features**: `scheme` doesn't support bulk processing or AI documentation

### Proposed Unified Command

**Merge into single `undatum schema` command with format selection:**

```python
@app.command()
def schema(
    input_file: Annotated[str, typer.Argument(...)],
    format: Annotated[str, typer.Option(
        help="Schema format: 'yaml' (default), 'json', 'cerberus', 'jsonschema', 'avro', 'parquet'"
    )] = 'yaml',
    output: Annotated[str, typer.Option(...)] = None,
    # ... other options
):
    """Extract or generate schema from data file.
    
    Supports multiple schema formats:
    - yaml: Undatum internal format (default)
    - json: JSON format of internal schema
    - cerberus: Cerberus validation schema
    - jsonschema: JSON Schema (standard)
    - avro: Avro schema
    - parquet: Parquet schema
    """
```

### Migration Strategy

**Phase 1: Add format parameter to existing `schema` command**
- Add `--format` parameter supporting: `yaml`, `json`, `cerberus`
- Implement format converters
- Keep `scheme` command as deprecated alias

**Phase 2: Add standard schema formats**
- Implement JSON Schema export
- Implement Avro Schema export
- Implement Parquet Schema export

**Phase 3: Deprecate `scheme` command**
- Mark `scheme` as deprecated with warning
- Redirect to `schema --format cerberus`
- Update documentation

**Phase 4: Remove `scheme` command**
- Remove `scheme` command entirely
- Update all documentation and examples

### Implementation Details

**Unified Schema Generation:**

```python
class Schemer:
    def extract_schema(self, fromfile, options):
        """Extract schema in specified format."""
        # Build base schema
        table = build_schema(fromfile)
        
        # Apply AI documentation if requested
        if options.get('autodoc'):
            self._add_ai_documentation(table, options)
        
        # Convert to requested format
        format_type = options.get('format', 'yaml')
        schema_output = self._convert_to_format(table, format_type)
        
        # Write output
        self._write_output(schema_output, options)
    
    def _convert_to_format(self, table, format_type):
        """Convert TableSchema to requested format."""
        if format_type == 'yaml':
            return yaml.dump(table.model_dump(), Dumper=yaml.Dumper)
        elif format_type == 'json':
            return json.dumps(table.model_dump(), indent=4)
        elif format_type == 'cerberus':
            return self._to_cerberus(table)
        elif format_type == 'jsonschema':
            return self._to_json_schema(table)
        elif format_type == 'avro':
            return self._to_avro_schema(table)
        elif format_type == 'parquet':
            return self._to_parquet_schema(table)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
```

**Format Converters Needed:**

1. **Cerberus Converter** (existing in `generate_scheme()`)
   - Reuse logic from `generate_scheme_from_file()`
   - Convert `TableSchema` → Cerberus format

2. **JSON Schema Converter** (new)
   - Convert `TableSchema` → JSON Schema format
   - Map DuckDB types to JSON Schema types
   - Handle nested structures

3. **Avro Schema Converter** (new)
   - Convert `TableSchema` → Avro schema
   - Map types appropriately
   - Handle unions and optional fields

4. **Parquet Schema Converter** (new)
   - Extract from Parquet files directly
   - Or convert `TableSchema` → Parquet schema format

### Benefits of Consolidation

1. **Single Command**: One clear command for all schema operations
2. **Format Selection**: Users choose format via `--format` parameter
3. **Feature Parity**: All features available for all formats
4. **Easier Maintenance**: Single code path, easier to maintain
5. **Better UX**: Clearer interface, less confusion
6. **Extensibility**: Easy to add new formats in the future

## Detailed Recommendations

### Priority 1: Critical Fixes

#### 1.1 Implement Output Format Support
**Action:** Add output format handling similar to `analyzer._write_analysis_output()`

**Changes Needed:**
- Create `_write_schema_output()` function supporting text/json/yaml
- Respect `outtype` option from CLI
- Respect `output` option for file vs stdout
- Add human-readable text format with table formatting

**Example Implementation Pattern:**
```python
def _write_schema_output(table, options, output_stream):
    """Write schema to output stream in specified format."""
    if options['outtype'] == 'json':
        json_output = json.dumps(table.model_dump(), indent=4)
        output_stream.write(json_output)
    elif options['outtype'] == 'yaml':
        yaml_output = yaml.dump(table.model_dump(), Dumper=yaml.Dumper)
        output_stream.write(yaml_output)
    else:  # text format
        # Format as readable table
        from tabulate import tabulate
        # ... format fields as table
```

#### 1.2 Implement AI Documentation
**Action:** Add AI service integration for field descriptions

**Changes Needed:**
- Initialize AI service when `autodoc=True` (similar to analyzer)
- Call `get_fields_info()` to populate field descriptions
- Handle AI service initialization failures gracefully
- Support AI provider/model configuration options

**Example Implementation Pattern:**
```python
# In extract_schema()
if options.get('autodoc'):
    ai_service = get_ai_service(
        provider=options.get('ai_provider'),
        config=options.get('ai_config')
    )
    fields = [f.name for f in table.fields]
    descriptions = get_fields_info(fields, 
                                  language=options.get('lang', 'English'),
                                  ai_service=ai_service)
    for field in table.fields:
        if field.name in descriptions:
            field.description = descriptions[field.name]
```

#### 1.3 Add Record Counting
**Action:** Count total records in file during schema extraction

**Changes Needed:**
- Add DuckDB query to count records (similar to analyzer)
- Set `table.num_records` in `build_schema()`
- Handle cases where counting fails gracefully

**Example Implementation:**
```python
# In build_schema()
try:
    if filetype in ['json', 'jsonl']:
        query_str = f"select count(*) from read_json('{filename}')"
    elif filetype in ['csv', 'tsv']:
        query_str = f"select count(*) from read_csv('{filename}')"
    else:
        query_str = f"select count(*) from '{filename}'"
    num_records = duckdb.sql(query_str).fetchall()[0][0]
    table.num_records = num_records
except Exception as e:
    logging.warning(f"Could not count records: {e}")
    table.num_records = -1
```

### Priority 2: Feature Enhancements

#### 2.1 Improve File Format Support
**Action:** Use same file detection as `analyzer`

**Changes Needed:**
- Import and use `iterable.helpers.detect.detect_file_type()`
- Support XLSX, XLS, XML, DOCX formats
- Handle compression detection separately
- Add proper file type validation

**Example Implementation:**
```python
from iterable.helpers.detect import detect_file_type, TEXT_DATA_TYPES

# In build_schema()
ftype = detect_file_type(filename)
if ftype['success']:
    filetype = ftype['datatype'].id()
    compression = ftype['codec'].id() if ftype['codec'] else 'raw'
else:
    # Fallback to extension-based detection
    filetype = filename.rsplit('.', 1)[-1].lower()
```

#### 2.2 Add Engine Selection
**Action:** Support engine parameter like `analyzer`

**Changes Needed:**
- Add `engine` parameter to `extract_schema()` and `extract_schema_bulk()`
- Implement fallback logic for unsupported file types
- Use iterable processing when DuckDB doesn't work

**Example Implementation:**
```python
def extract_schema(self, fromfile, options):
    engine = options.get('engine', 'auto')
    filetype = detect_file_type(fromfile)
    
    if engine == 'auto':
        # Auto-detect best engine
        if filetype in DUCKABLE_FILE_TYPES:
            engine = 'duckdb'
        else:
            engine = 'iterable'
    
    if engine == 'duckdb':
        table = build_schema(fromfile)
    else:
        # Use iterable processing
        table = build_schema_iterable(fromfile)
```

#### 2.3 Fix Bulk Mode File Discovery
**Action:** Support glob patterns and improve file discovery

**Changes Needed:**
- Use `glob.glob()` instead of `os.listdir()`
- Handle both directory paths and glob patterns
- Improve file extension detection (handle multiple extensions like `.csv.gz`)

**Example Implementation:**
```python
import glob

def extract_schema_bulk(self, fromdir, options):
    # Support both directories and glob patterns
    if os.path.isdir(fromdir):
        pattern = os.path.join(fromdir, '*')
    else:
        pattern = fromdir
    
    files = []
    for ext in supported_exts:
        files.extend(glob.glob(f"{pattern}.{ext}"))
        files.extend(glob.glob(f"{pattern}.{ext}.*"))  # Compressed
```

#### 2.4 Improve Error Handling
**Action:** Add comprehensive error handling

**Changes Needed:**
- Validate input file exists and is readable
- Handle unsupported file types gracefully
- Provide clear error messages
- Set error flags in schema objects

**Example Implementation:**
```python
def build_schema(filename: str, objects_limit: int = 100000):
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File not found: {filename}")
    
    if not os.access(filename, os.R_OK):
        raise PermissionError(f"Cannot read file: {filename}")
    
    try:
        # ... schema extraction
    except Exception as e:
        logging.error(f"Schema extraction failed: {e}")
        table = TableSchema(id=os.path.basename(filename))
        table.success = False
        table.error = str(e)
        return table
```

### Priority 3: Code Quality Improvements

#### 3.1 Eliminate Code Duplication
**Action:** Share `duckdb_decompose()` between schemer and analyzer

**Recommendation Options:**
1. **Move to shared module**: Create `undatum/common/schema_utils.py`
2. **Import from analyzer**: Have schemer import from analyzer (if analyzer is always imported)
3. **Create base class**: Extract common schema functionality

**Preferred Approach:** Option 1 - Create shared utility module

**Example Structure:**
```python
# undatum/common/schema_utils.py
def duckdb_decompose(filename=None, frame=None, filetype=None, 
                    path="*", limit=10000000, recursive=True, 
                    root="", ignore_errors=True, use_summarize=False):
    """Decompose file or DataFrame structure using DuckDB.
    
    Args:
        use_summarize: If True, use summarize (for analyzer), 
                      else use describe (for schemer)
    """
    # Unified implementation
```

#### 3.2 Improve Type Hints
**Action:** Add comprehensive type hints

**Current State:** Minimal type hints
**Recommendation:** Add type hints to all functions for better IDE support and documentation

#### 3.3 Add Docstrings
**Action:** Improve function documentation

**Current State:** Some functions have docstrings, but they're inconsistent
**Recommendation:** Add comprehensive docstrings following Google/NumPy style

#### 3.4 Consistent Constants
**Action:** Share constants between modules

**Current State:** `DUCKABLE_FILE_TYPES`, `DUCKABLE_CODECS` defined in both schemer and analyzer
**Recommendation:** Move to `undatum/constants.py` or shared module

### Priority 4: Advanced Features

#### 4.1 Schema Comparison
**Action:** Add ability to compare schemas (similar to `diff` command)

**Use Case:** Compare schema evolution over time, validate schema consistency

#### 4.2 Schema Validation
**Action:** Validate data against extracted schema

**Use Case:** Check if new data matches expected schema

#### 4.3 Schema Merging
**Action:** Merge schemas from multiple files

**Use Case:** Create unified schema from multiple data sources

#### 4.4 Export to Standard Formats
**Action:** Export to JSON Schema, Avro Schema, Parquet Schema

**Use Case:** Integration with other tools and systems

**Implementation Priority:**
1. **JSON Schema** (CRITICAL) - Most widely used, should be first
2. **Avro Schema** (HIGH) - Important for data pipelines
3. **Parquet Schema** (MEDIUM) - Useful for Parquet workflows

**Example JSON Schema Converter:**
```python
def _to_json_schema(self, table: TableSchema) -> dict:
    """Convert TableSchema to JSON Schema format."""
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "array",
        "items": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
    
    for field in table.fields:
        prop_schema = self._field_to_json_schema(field)
        schema["items"]["properties"][field.name] = prop_schema
        if not field.is_optional:  # Would need to track this
            schema["items"]["required"].append(field.name)
    
    return schema

def _field_to_json_schema(self, field: FieldSchema) -> dict:
    """Convert FieldSchema to JSON Schema property."""
    type_map = {
        'VARCHAR': 'string',
        'BIGINT': 'integer',
        'DOUBLE': 'number',
        'BOOLEAN': 'boolean',
        'DATE': {'type': 'string', 'format': 'date'},
        'JSON': {'type': 'object'},
        'STRUCT': {'type': 'object'}
    }
    
    base_type = type_map.get(field.ftype, 'string')
    schema = {'type': base_type} if isinstance(base_type, str) else base_type
    
    if field.is_array:
        schema = {'type': 'array', 'items': schema}
    
    if field.description:
        schema['description'] = field.description
    
    return schema
```

#### 4.5 Merge `schema` and `scheme` Commands
**Action:** Consolidate into unified `schema` command with format selection

**Changes Needed:**
- Add `--format` parameter to `schema` command
- Implement format converters (cerberus, jsonschema, avro, parquet)
- Deprecate `scheme` command with migration path
- Update all documentation

**Migration Path:**
1. Add format support to `schema` command
2. Make `scheme` an alias that warns about deprecation
3. After deprecation period, remove `scheme` command

**Benefits:**
- Single command for all schema operations
- Consistent interface across formats
- Easier to maintain and extend

## Comparison Matrix

| Feature | schema | scheme | analyzer | ingester | Recommendation |
|---------|--------|--------|----------|----------|----------------|
| Output formats (text/json/yaml) | ❌ | ❌ | ✅ | N/A | **Add** |
| Schema formats (jsonschema/avro) | ❌ | ❌ | N/A | N/A | **Add** |
| Cerberus format | ❌ | ✅ | N/A | N/A | **Merge** |
| AI documentation | ❌ | ❌ | ✅ | N/A | **Add** |
| Record counting | ❌ | ❌ | ✅ | ✅ | **Add** |
| File format support | Limited | Limited | Extensive | Extensive | **Expand** |
| Compression detection | ❌ | ❌ | ✅ | ✅ | **Add** |
| Engine selection | ❌ | ❌ | ✅ | N/A | **Add** |
| Error handling | Basic | Basic | Comprehensive | Comprehensive | **Improve** |
| Glob pattern support | ❌ | ❌ | N/A | ✅ | **Add** |
| Human-readable output | ❌ | ❌ | ✅ | N/A | **Add** |
| Bulk processing | ✅ | ❌ | N/A | N/A | **Keep** |

## Implementation Priority

### Phase 1: Critical Fixes (Immediate)
1. Implement output format support (text/json/yaml)
2. Implement AI documentation
3. Add record counting
4. Fix bulk mode file discovery
5. Add JSON Schema export support (CRITICAL for tool integration)

### Phase 2: Feature Parity (Short-term)
1. Merge `schema` and `scheme` commands into unified interface
2. Improve file format support
3. Add compression detection
4. Add engine selection
5. Improve error handling
6. Add Avro Schema export

### Phase 3: Code Quality (Medium-term)
1. Eliminate code duplication
2. Improve type hints and documentation
3. Share constants between modules

### Phase 4: Advanced Features (Long-term)
1. Schema comparison
2. Schema validation
3. Schema merging
4. Parquet Schema export
5. XSD/XML Schema export (if needed)

## Testing Recommendations

1. **Unit Tests**: Test each output format (text/json/yaml)
2. **Integration Tests**: Test with various file formats and compression
3. **AI Tests**: Test AI documentation with different providers
4. **Error Tests**: Test error handling for invalid inputs
5. **Performance Tests**: Test with large files and bulk operations

## Conclusion

The schema command has solid foundations but needs significant improvements to match the quality and feature set of other commands, particularly the `analyzer` command. The most critical issues are:

1. **Output format not respected** - Users cannot control output
2. **AI documentation not implemented** - Advertised feature doesn't work
3. **Missing record counts** - Incomplete schema information
4. **Limited file format support** - Doesn't match other commands
5. **No standard schema format support** - Missing JSON Schema, Avro Schema, etc.
6. **Command confusion** - Two similar commands (`schema` vs `scheme`) cause confusion

**Key Recommendations:**

1. **Immediate Priority**: Add JSON Schema export support - this is the most widely used standard format and essential for tool integration
2. **Short-term Priority**: Merge `schema` and `scheme` commands into a unified interface with format selection
3. **Medium-term Priority**: Add Avro Schema and Parquet Schema export for data engineering workflows

By addressing these issues and following patterns established in the `analyzer` command, the schema command can become a powerful and consistent tool for schema extraction and documentation that integrates well with the broader data engineering ecosystem.

## References

- `undatum/cmds/analyzer.py` - Reference implementation for output formatting, AI integration, file detection
- `undatum/cmds/ingester.py` - Reference for schema inference patterns
- `undatum/common/scheme.py` - Cerberus schema generation implementation
- `undatum/utils.py` - File type detection utilities
- `undatum/cmds/schemer.py` - Current schema extraction implementation
- `undatum/core.py` - CLI command definitions for `schema` and `scheme`

## Schema Format Standards

- **JSON Schema**: https://json-schema.org/ - W3C/IETF standard, most widely used
- **Avro Schema**: https://avro.apache.org/docs/current/spec.html - Apache Avro specification
- **Parquet Schema**: https://parquet.apache.org/docs/ - Apache Parquet format specification
- **Cerberus**: https://docs.python-cerberus.org/ - Python validation library schema format
