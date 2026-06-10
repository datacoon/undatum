# Change: Improve Command Error Handling

## Why

Currently, when commands encounter errors (file not found, permission errors, invalid input, etc.), users receive full Python exception tracebacks instead of meaningful, actionable error messages. This creates a poor user experience and makes it difficult for users to understand what went wrong and how to fix it.

**Current Issues:**
1. **Raw exceptions exposed**: Users see full Python tracebacks with internal implementation details
2. **Inconsistent error messages**: Different commands handle errors differently, some better than others
3. **No actionable guidance**: Error messages don't explain what went wrong or suggest fixes
4. **Poor error categorization**: All errors look the same, making it hard to distinguish between user errors, system errors, and configuration issues
5. **No error recovery hints**: Users don't get suggestions for common mistakes (e.g., "Did you mean...?" for typos)

**Expected Benefits:**
- **User-friendly error messages** that explain what went wrong in plain language
- **Consistent error handling** across all commands
- **Actionable guidance** with suggestions for fixing common issues
- **Better error categorization** (user errors vs. system errors)
- **Improved debugging** with verbose mode showing detailed errors when needed
- **Professional CLI experience** matching modern CLI tool standards

## What Changes

- **MODIFIED**: All command classes in `undatum/cmds/` to use consistent error handling patterns
- **ADDED**: Custom exception classes for common error types (FileNotFoundError, PermissionError, ValidationError, etc.)
- **ADDED**: Error message formatting utilities in `undatum/common/errors.py`
- **MODIFIED**: Command execution in `undatum/core.py` to catch and format exceptions consistently
- **ADDED**: Error context helpers (file path validation, permission checks, format detection errors)
- **MODIFIED**: All commands to validate inputs early and provide clear error messages
- **ADDED**: Error recovery suggestions for common mistakes (file path typos, missing dependencies, etc.)

All changes are backward compatible. Commands continue to work the same way, but with better error messages.

## Impact

- **Affected specs**: `error-handling` capability (new)
- **Affected code**:
  - All files in `undatum/cmds/` (45+ command files)
  - `undatum/core.py` - Command execution and error handling
  - New `undatum/common/errors.py` - Error handling utilities
  - `undatum/common/path_utils.py` - File path validation helpers
- **Dependencies**: None (uses standard library)
- **Backward compatibility**: Fully backward compatible - only error message format changes
