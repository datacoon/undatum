## 1. Implementation

### 1.1 Core Infrastructure
- [x] 1.1.1 Create base command class pattern (if needed)
- [x] 1.1.2 Review existing command patterns in `undatum/cmds/`
- [x] 1.1.3 Set up test fixtures for new commands

### 1.2 Count Command
- [x] 1.2.1 Create `undatum/cmds/counter.py` with `Counter` class
- [x] 1.2.2 Implement row counting with streaming support
- [x] 1.2.3 Add DuckDB optimization for supported formats
- [x] 1.2.4 Add command to `undatum/core.py`
- [x] 1.2.5 Write unit tests
- [x] 1.2.6 Test with multiple formats (CSV, JSONL, BSON)

### 1.3 Table Command
- [x] 1.3.1 Create `undatum/cmds/table.py` with `Table` class
- [x] 1.3.2 Implement pretty-printing using `rich` library
- [x] 1.3.3 Add row limit and field selection options
- [x] 1.3.4 Add command to `undatum/core.py`
- [x] 1.3.5 Write unit tests
- [x] 1.3.6 Test table formatting with various data types

### 1.4 Reverse Command
- [x] 1.4.1 Create `undatum/cmds/reverser.py` with `Reverser` class
- [x] 1.4.2 Implement row reversal (buffering for large files)
- [x] 1.4.3 Add DuckDB optimization option (using iterable for now)
- [x] 1.4.4 Add command to `undatum/core.py`
- [x] 1.4.5 Write unit tests
- [x] 1.4.6 Test with large files (external approach)

### 1.5 Enum Command
- [x] 1.5.1 Create `undatum/cmds/enumerator.py` with `Enumerator` class
- [x] 1.5.2 Implement row number generation
- [x] 1.5.3 Add UUID generation option
- [x] 1.5.4 Add constant value option
- [x] 1.5.5 Add command to `undatum/core.py`
- [x] 1.5.6 Write unit tests

### 1.6 Head Command
- [x] 1.6.1 Create `undatum/cmds/head.py` with `Head` class
- [x] 1.6.2 Implement first N rows extraction
- [x] 1.6.3 Add command to `undatum/core.py`
- [x] 1.6.4 Write unit tests

### 1.7 Tail Command
- [x] 1.7.1 Create `undatum/cmds/tail.py` with `Tail` class
- [x] 1.7.2 Implement last N rows extraction (buffering for large files)
- [x] 1.7.3 Add command to `undatum/core.py`
- [x] 1.7.4 Write unit tests

### 1.8 Fixlengths Command
- [x] 1.8.1 Create `undatum/cmds/fixlengths.py` with `FixLengths` class
- [x] 1.8.2 Implement padding strategy (with default value)
- [x] 1.8.3 Implement truncation strategy
- [x] 1.8.4 Add command to `undatum/core.py`
- [x] 1.8.5 Write unit tests
- [x] 1.8.6 Test with malformed data

## 2. Documentation

- [x] 2.1 Update `README.md` with all new commands
- [x] 2.2 Add usage examples for each command
- [x] 2.3 Document format support for each command
- [x] 2.4 Add command help text

## 3. Testing

- [x] 3.1 Integration tests for all commands
- [x] 3.2 Performance tests for large files (optional, can be done later)
- [x] 3.3 Format compatibility tests (CSV, JSONL, BSON, XML)
- [x] 3.4 Edge case tests (empty files, single row, malformed data)

## 4. Validation

- [x] 4.1 Run `openspec validate add-phase1-data-commands --strict` - Validation passed
- [x] 4.2 Review all requirements and scenarios
- [x] 4.3 Ensure backward compatibility (no breaking changes, only additions)
