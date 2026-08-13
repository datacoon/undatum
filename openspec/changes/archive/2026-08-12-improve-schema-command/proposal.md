# Change: Improve Schema Command Functionality

## Why

The `undatum schema` command has critical gaps that prevent it from being a reliable and feature-complete tool for schema extraction. Based on comprehensive review (see `dev/docs/SCHEMA_COMMAND_REVIEW.md`), the command:

1. **Ignores user options**: `--outtype` and `--output` parameters are passed but completely ignored
2. **Missing advertised features**: `--autodoc` option exists in CLI but doesn't work
3. **Incomplete information**: Doesn't count records, providing incomplete schema metadata
4. **Limited compatibility**: Doesn't support file formats that other commands (like `analyze`) support
5. **Poor error handling**: Silent failures and no validation

These issues prevent users from effectively using the schema command and create inconsistency with other commands in the codebase. The command needs to match the quality and feature set of the `analyze` command.

## What Changes

- **MODIFIED**: `undatum/cmds/schemer.py` - Implement output format support (text/json/yaml) respecting `--outtype` option
- **MODIFIED**: `undatum/cmds/schemer.py` - Implement AI documentation support using `--autodoc` option
- **MODIFIED**: `undatum/cmds/schemer.py` - Add record counting to schema extraction
- **MODIFIED**: `undatum/cmds/schemer.py` - Improve file format support using `iterable.helpers.detect`
- **MODIFIED**: `undatum/cmds/schemer.py` - Add compression detection
- **MODIFIED**: `undatum/cmds/schemer.py` - Add engine selection parameter (auto/duckdb/iterable)
- **MODIFIED**: `undatum/cmds/schemer.py` - Improve error handling with proper validation
- **MODIFIED**: `undatum/cmds/schemer.py` - Fix bulk mode to support glob patterns
- **MODIFIED**: `undatum/core.py` - Add AI provider/model options to schema command
- **ADDED**: `undatum/common/schema_utils.py` - Shared schema utilities to eliminate code duplication
- **MODIFIED**: `undatum/cmds/analyzer.py` - Use shared schema utilities
- **MODIFIED**: `undatum/constants.py` - Move shared constants (DUCKABLE_FILE_TYPES, DUCKABLE_CODECS)

All changes maintain backward compatibility for existing functionality while adding missing features.

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/cmds/schemer.py` - Major refactoring and feature additions
  - `undatum/cmds/analyzer.py` - Refactor to use shared utilities
  - `undatum/core.py` - Add CLI options for AI configuration
  - `undatum/common/schema_utils.py` - New shared utility module
  - `undatum/constants.py` - Add shared constants
- **Dependencies**: No new dependencies required (uses existing AI infrastructure)
- **Backward compatibility**: All existing functionality preserved, only adds missing features
