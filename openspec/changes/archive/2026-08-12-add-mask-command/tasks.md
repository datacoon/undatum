## 1. Masking Utilities
- [x] 1.1 Create `undatum/common/masking.py` module
  - Implement `redact()` method (fixed token replacement)
  - Implement `hash()` method (deterministic hashing)
  - Implement `randomize()` method (type-compatible random values)
- [x] 1.2 Add type detection for masking
  - Detect email addresses
  - Detect phone numbers
  - Detect SSN/ID numbers
  - Detect numeric ranges (ages, etc.)

## 2. Mask Command Implementation
- [x] 2.1 Create `undatum/cmds/masker.py` module
  - Implement `Masker` class
  - Add field selection logic
  - Add method selection logic
  - Integrate with masking utilities
- [x] 2.2 Support multiple formats
  - CSV masking
  - JSONL masking
  - JSON masking

## 3. CLI Integration
- [x] 3.1 Add `mask` command to `undatum/core.py`
  - Add `--fields` option (comma-separated list)
  - Add `--method` option (redact/hash/randomize)
  - Add input/output path arguments
- [x] 3.2 Update help text
  - Document masking methods
  - Add examples for each method
  - Document field selection

## 4. Testing
- [x] 4.1 Unit tests for masking utilities
  - Test redact method
  - Test hash method (deterministic)
  - Test randomize method
  - Test type detection
- [x] 4.2 Integration tests
  - Test mask command with CSV
  - Test mask command with JSONL
  - Test multiple fields
  - Test different methods
- [x] 4.3 Edge case tests
  - Test empty fields
  - Test non-existent fields
  - Test special characters

## 5. Documentation
- [x] 5.1 Update README with mask command examples
- [x] 5.2 Document masking methods and use cases
- [x] 5.3 Add data privacy best practices guide
