# Change: Consolidate Schema and Scheme Commands

## Why

Currently, the codebase has two confusingly similar commands:
- `undatum schema` - Extracts schema, outputs YAML (Pydantic models)
- `undatum scheme` - Generates Cerberus validation schema, outputs JSON

This creates user confusion about which command to use. The distinction between "schema" and "scheme" is unclear, and having two separate commands with overlapping functionality increases maintenance burden. Additionally, the `scheme` command has a `--stype` parameter that claims to support "other schema formats" but is completely ignored - only Cerberus is generated.

Consolidating into a single `schema` command with format selection will provide:
- Clearer user interface
- Unified feature set (bulk processing, AI docs available for all formats)
- Easier maintenance
- Better extensibility for adding new formats

## What Changes

- **MODIFIED**: `undatum/core.py` - Add `--format` parameter to `schema` command supporting: `yaml`, `json`, `cerberus`, `jsonschema`, `avro`, `parquet`
- **MODIFIED**: `undatum/cmds/schemer.py` - Add format conversion methods (`_to_cerberus()`, `_to_json_schema()`, `_to_avro()`, `_to_parquet()`)
- **MODIFIED**: `undatum/cmds/schemer.py` - Update `extract_schema()` to support format selection
- **DEPRECATED**: `undatum/core.py` - Mark `scheme` command as deprecated with migration warning
- **MODIFIED**: `undatum/core.py` - Make `scheme` command an alias that redirects to `schema --format cerberus` with deprecation warning
- **REMOVED**: `undatum/core.py` - Remove `scheme` command after deprecation period (future change)

**BREAKING**: The `scheme` command will be deprecated. Users should migrate to `undatum schema --format cerberus`. The deprecation warning will guide users during the transition period.

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/core.py` - Modify `schema` command, deprecate `scheme` command
  - `undatum/cmds/schemer.py` - Add format conversion methods
- **Dependencies**: No new dependencies required
- **Backward compatibility**: `scheme` command will continue to work during deprecation period, redirecting to new unified command
