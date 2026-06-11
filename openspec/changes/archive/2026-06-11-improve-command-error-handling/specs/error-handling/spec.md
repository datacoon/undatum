# Error Handling Specification

## ADDED Requirements

### Requirement: User-Friendly Error Messages
All commands SHALL display clear, actionable error messages to users instead of raw Python exceptions.

#### Scenario: File not found error
- **WHEN** a command is executed with a non-existent file path
- **THEN** the system SHALL display: "Error: File not found: '/path/to/file.csv'"
- **AND** suggest similar file names if available (typo detection)
- **AND** provide guidance: "Check that the file path is correct and the file exists"
- **AND** NOT display full Python traceback unless verbose mode is enabled

#### Scenario: Permission denied error
- **WHEN** a command attempts to access a file without read permissions
- **THEN** the system SHALL display: "Error: Permission denied: '/path/to/file.csv'"
- **AND** provide guidance: "Check file permissions. You may need to run: chmod +r /path/to/file.csv"
- **AND** NOT display full Python traceback unless verbose mode is enabled

#### Scenario: Invalid file format error
- **WHEN** a command receives a file with unsupported format
- **THEN** the system SHALL display: "Error: Unsupported file format: '.xyz'"
- **AND** list supported formats: "Supported formats: csv, jsonl, parquet, xlsx, ..."
- **AND** suggest format conversion if applicable
- **AND** NOT display full Python traceback unless verbose mode is enabled

#### Scenario: Validation error
- **WHEN** a command receives invalid input parameters
- **THEN** the system SHALL display: "Error: Invalid parameter: 'field_name'"
- **AND** explain what was wrong: "Field 'field_name' does not exist in the data"
- **AND** suggest valid alternatives if available
- **AND** NOT display full Python traceback unless verbose mode is enabled

#### Scenario: Missing dependency error
- **WHEN** a command requires an optional dependency that is not installed
- **THEN** the system SHALL display: "Error: Missing dependency: 'package_name'"
- **AND** provide installation instructions: "Install with: pip install package_name"
- **AND** explain which feature requires this dependency
- **AND** NOT display full Python traceback unless verbose mode is enabled

### Requirement: Consistent Error Handling Pattern
All commands SHALL follow a consistent error handling pattern using custom exception classes.

#### Scenario: Error handling in command classes
- **WHEN** a command encounters an error condition
- **THEN** the command SHALL raise an appropriate custom exception (UndatumError subclass)
- **AND** the exception SHALL include a user-friendly error message
- **AND** the exception SHALL include context information (file path, field name, etc.)
- **AND** the exception SHALL be caught by the command execution framework
- **AND** formatted and displayed to the user

#### Scenario: Error categorization
- **WHEN** an error occurs
- **THEN** the system SHALL categorize errors as:
  - User errors (invalid input, file not found, etc.) - exit code 1
  - Configuration errors (missing config, invalid settings) - exit code 2
  - System errors (permission denied, out of memory) - exit code 3
  - Internal errors (unexpected exceptions) - exit code 4
- **AND** display appropriate error messages for each category

### Requirement: Error Recovery Suggestions
Commands SHALL provide helpful suggestions for common error scenarios.

#### Scenario: File path typo detection
- **WHEN** a file path is provided that doesn't exist
- **THEN** the system SHALL check for similar file names in the directory
- **AND** if similar names are found, suggest: "Did you mean: '/path/to/similar_file.csv'?"
- **AND** use fuzzy matching to find close matches

#### Scenario: Field name suggestions
- **WHEN** a field name is provided that doesn't exist in the data
- **THEN** the system SHALL suggest similar field names if available
- **AND** display: "Field 'field_name' not found. Did you mean: 'field_name_similar'?"
- **AND** list all available field names if no close match is found

#### Scenario: Format conversion suggestions
- **WHEN** a file format is not supported for a specific operation
- **THEN** the system SHALL suggest converting the file to a supported format
- **AND** provide the conversion command: "Convert with: undatum convert input.xyz output.csv"

### Requirement: Verbose Mode Error Display
When verbose mode is enabled, commands SHALL display detailed error information including full tracebacks.

#### Scenario: Verbose error display
- **WHEN** an error occurs and verbose mode is enabled (--verbose flag)
- **THEN** the system SHALL display the full Python traceback
- **AND** include internal error details
- **AND** show the error location in the code
- **AND** display stack trace for debugging

#### Scenario: Non-verbose error display
- **WHEN** an error occurs and verbose mode is NOT enabled
- **THEN** the system SHALL display only the user-friendly error message
- **AND** NOT display Python traceback
- **AND** NOT display internal implementation details

### Requirement: Command Error Handling
All command classes SHALL implement consistent error handling using the error handling infrastructure.

#### Scenario: Command execution with error
- **WHEN** a command is executed and encounters an error
- **THEN** the command SHALL catch specific exceptions (FileNotFoundError, PermissionError, etc.)
- **AND** convert them to appropriate UndatumError subclasses
- **AND** include context information in the error message
- **AND** re-raise as UndatumError for consistent handling
- **AND** the framework SHALL catch UndatumError and format for display

#### Scenario: Input validation
- **WHEN** a command receives input parameters
- **THEN** the command SHALL validate inputs early (before processing)
- **AND** raise ValidationError with clear message if validation fails
- **AND** provide suggestions for fixing invalid inputs
- **AND** validate file existence and permissions before processing

#### Scenario: Error propagation
- **WHEN** an error occurs in a command's internal processing
- **THEN** the error SHALL be caught and wrapped in an appropriate UndatumError
- **AND** include original error context for debugging
- **AND** preserve error chain for verbose mode
- **AND** display user-friendly message in non-verbose mode
