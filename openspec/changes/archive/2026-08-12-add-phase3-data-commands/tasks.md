## 1. Implementation

### 1.1 Join Command
- [x] 1.1.1 Create `undatum/cmds/joiner.py` with `Joiner` class
- [x] 1.1.2 Implement inner join
- [x] 1.1.3 Implement left join
- [x] 1.1.4 Implement right join
- [x] 1.1.5 Implement full outer join
- [x] 1.1.6 Implement hash-based join for streaming formats
- [x] 1.1.7 Add DuckDB SQL join for supported formats
- [x] 1.1.8 Add key field selection
- [x] 1.1.9 Handle field name conflicts
- [x] 1.1.10 Add command to `undatum/core.py`
- [x] 1.1.11 Write unit tests
- [x] 1.1.12 Test with large files

### 1.2 Diff Command
- [x] 1.2.1 Create `undatum/cmds/differ.py` with `Differ` class
- [x] 1.2.2 Implement key-based comparison
- [x] 1.2.3 Implement added row detection
- [x] 1.2.4 Implement removed row detection
- [x] 1.2.5 Implement changed row detection
- [x] 1.2.6 Add output format options (unified diff, JSON, etc.)
- [x] 1.2.7 Add command to `undatum/core.py`
- [x] 1.2.8 Write unit tests

### 1.3 Exclude Command
- [x] 1.3.1 Create `undatum/cmds/excluder.py` with `Excluder` class
- [x] 1.3.2 Implement key-based exclusion
- [x] 1.3.3 Implement hash-based lookup for performance
- [x] 1.3.4 Add command to `undatum/core.py`
- [x] 1.3.5 Write unit tests
- [x] 1.3.6 Test with large exclusion lists

### 1.4 Transpose Command
- [x] 1.4.1 Create `undatum/cmds/transposer.py` with `Transposer` class
- [x] 1.4.2 Implement row/column swapping
- [x] 1.4.3 Handle header row appropriately
- [x] 1.4.4 Add memory-efficient approach for large files
- [x] 1.4.5 Add command to `undatum/core.py`
- [x] 1.4.6 Write unit tests

### 1.5 Sniff Command
- [x] 1.5.1 Create `undatum/cmds/sniffer.py` with `Sniffer` class
- [x] 1.5.2 Implement delimiter detection
- [x] 1.5.3 Implement encoding detection (leverage existing)
- [x] 1.5.4 Implement field type detection
- [x] 1.5.5 Implement record count estimation
- [x] 1.5.6 Implement header row detection
- [x] 1.5.7 Add output format (text, JSON, YAML)
- [x] 1.5.8 Add command to `undatum/core.py`
- [x] 1.5.9 Write unit tests

### 1.6 Enhanced Slice Command
- [x] 1.6.1 Create `undatum/cmds/slicer.py` with `Slicer` class
- [x] 1.6.2 Implement range-based slicing (start-end)
- [x] 1.6.3 Implement index-based slicing
- [x] 1.6.4 Add DuckDB optimization for supported formats
- [x] 1.6.5 Add command to `undatum/core.py`
- [x] 1.6.6 Write unit tests
- [x] 1.6.7 Deprecate or document `convert --start_line` usage (documented in command help)

### 1.7 Enhanced Fmt Command
- [x] 1.7.1 Create `undatum/cmds/formatter.py` with `Formatter` class
- [x] 1.7.2 Add delimiter change option
- [x] 1.7.3 Add quote style options (always, minimal, none)
- [x] 1.7.4 Add escape character options
- [x] 1.7.5 Add line ending options
- [x] 1.7.6 Integrate with existing `convert` command or create separate command (created separate `fmt` command)
- [x] 1.7.7 Add command to `undatum/core.py`
- [x] 1.7.8 Write unit tests

## 2. Documentation

- [x] 2.1 Update `README.md` with all new commands
- [x] 2.2 Add usage examples for each command
- [x] 2.3 Document join types and use cases
- [x] 2.4 Document performance characteristics (in code and help text)
- [x] 2.5 Document format support for each command (via help text)
- [x] 2.6 Add command help text
- [x] 2.7 Document enhancements to existing commands

## 3. Testing

- [x] 3.1 Integration tests for all commands
- [x] 3.2 Performance tests for large files (optional, can be done later)
- [x] 3.3 Format compatibility tests (CSV, JSONL, BSON, XML)
- [x] 3.4 Edge case tests (empty files, single row, malformed data)
- [x] 3.5 Join algorithm correctness tests
- [x] 3.6 Diff algorithm correctness tests

## 4. Validation

- [x] 4.1 Run `openspec validate add-phase3-data-commands --strict` - Validation passed
- [x] 4.2 Review all requirements and scenarios
- [x] 4.3 Ensure backward compatibility for enhanced commands
