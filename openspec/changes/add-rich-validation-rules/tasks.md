## 1. Rule Definition Format
- [x] 1.1 Design YAML/JSON rule format
  - Field-level rules structure
  - Cross-field rules structure
  - Severity levels (error, warning, info)
  - Rule metadata (name, description)
- [x] 1.2 Create rule parser
  - Parse YAML/JSON rule files
  - Validate rule structure
  - Support rule inheritance/imports
  - (Rule inheritance/imports - future enhancement)
- [x] 1.3 Define rule types
  - Required field rules
  - Type validation (string, number, date, etc.)
  - Format validation (email, URL, regex)
  - Range validation (min/max for numbers, length for strings)
  - Enum/whitelist validation
  - Cross-field rules (dependencies, consistency)

## 2. Validation Engine
- [x] 2.1 Enhance Validator class
  - Support rule file input
  - Evaluate multiple rules per field
  - Track violations by severity
  - Support cross-field validation
- [x] 2.2 Implement rule evaluators
  - Field-level rule evaluation
  - Cross-field rule evaluation
  - Custom validation function support
  - Error message generation
- [x] 2.3 Add violation tracking
  - Collect violations with context
  - Group by severity and rule
  - Track record-level violations

## 3. Reporting
- [x] 3.1 Add violation summary
  - Total violations by severity
  - Violations by rule type
  - Violations by field
  - Pass/fail statistics
- [x] 3.2 Add detailed violation report
  - Record-level violation details
  - Field values and expected values
  - Rule descriptions
  - Optional record context
- [x] 3.3 Add JSON output format
  - Structured violation data
  - Machine-readable format
  - Integration with CI/CD pipelines

## 4. CLI Integration
- [x] 4.1 Update validate command
  - Add `--rules` option for rule file
  - Add `--severity` filter (errors only, warnings, all)
  - Add `--output-format` (text, json)
  - Maintain backward compatibility with existing CLI options
- [x] 4.2 Add validation-specific options
  - `--fail-on-warnings` flag
  - `--max-violations` limit
  - `--violation-report` output file

## 5. Rule Library
- [x] 5.1 Create common rule templates
  - Email validation rules
  - URL validation rules
  - Date format rules
  - Numeric range rules
- [x] 5.2 Add example rule files
  - Basic field validation example
  - Cross-field validation example
  - Complex validation scenario

## 6. Testing
- [ ] 6.1 Unit tests for rule parser
  - Test YAML/JSON parsing
  - Test rule structure validation
  - Test rule inheritance
- [ ] 6.2 Unit tests for validation engine
  - Test field-level rules
  - Test cross-field rules
  - Test severity handling
- [ ] 6.3 Integration tests
  - Test end-to-end validation with rule files
  - Test violation reporting
  - Test backward compatibility

## 7. Documentation
- [x] 7.1 Document rule format
  - YAML/JSON schema
  - Rule type reference
  - Examples for each rule type
- [x] 7.2 Update README
  - Add rule file examples
  - Document new CLI options
  - Add validation use cases
- [x] 7.3 Add validation best practices
  - When to use errors vs warnings
  - Rule organization strategies
  - Performance considerations
