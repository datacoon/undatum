# Change: Migrate to iterabledata Library and Leverage Advanced Features

## Why

The codebase currently has a mixed approach to iterable data handling:
- Local `IterableData` class in `undatum/common/iterable.py` supports only CSV, JSONL, and BSON
- External `iterabledata` library (v1.0.7) provides comprehensive format support (CSV, JSON, JSONL, BSON, XML, XLS, XLSX, Parquet, AVRO, ORC, Pickle) with advanced features
- Some commands already use the external library (`converter`, `transformer`, `ingester`, `textproc`, `statistics`, `analyzer`, partially `selector`)
- Some commands still use local `IterableData` (`query`, partially `selector`)
- The external library offers superior features: `reset()`, `write_bulk()`, context manager support, DuckDB engine integration, better compression support, and automatic format detection

This migration will:
- Unify data handling across all commands
- Enable advanced features like bulk writes and iterator reset
- Improve performance through DuckDB engine integration
- Reduce code maintenance burden by removing duplicate functionality
- Provide better format support and compression handling

## What Changes

- **BREAKING**: Remove or deprecate local `IterableData` class and `DataWriter` class from `undatum/common/iterable.py`
- Migrate `query` command to use `open_iterable` from external library
- Complete migration of `selector` command to use external library consistently
- Update all commands to leverage `write_bulk()` for batch writes instead of individual writes
- Implement context manager usage (`with` statements) for proper resource management
- Add `reset()` support where needed for multiple passes over data
- Integrate DuckDB engine option for performance-critical operations
- Standardize on `iterableargs` parameter passing across all commands
- Update examples to use external library API
- Remove unused local iterable code after migration

## Impact

- **Affected specs**: `data-processing` capability (new)
- **Affected code**:
  - `undatum/common/iterable.py` - Remove/deprecate local classes
  - `undatum/cmds/query.py` - Migrate to external library
  - `undatum/cmds/selector.py` - Complete migration
  - `undatum/cmds/converter.py` - Leverage `write_bulk()` and context managers
  - `undatum/cmds/transformer.py` - Use context managers and `reset()`
  - `examples/compressed/iterable.py` - Update to use external library
  - All commands using iterable data - Standardize on external library API
- **Dependencies**: External `iterabledata` library is already a dependency
- **Backward compatibility**: CLI interface remains unchanged, internal implementation changes only
