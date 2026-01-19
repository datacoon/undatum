# Design: Migrate to iterabledata Library

## Context

The undatum codebase currently maintains a local `IterableData` class that duplicates functionality provided by the external `iterabledata` library. While some commands have already migrated to the external library, others still use the local implementation, creating inconsistency and maintenance burden.

The external `iterabledata` library (v1.0.7) provides:
- Comprehensive format support (CSV, JSON, JSONL, BSON, XML, XLS, XLSX, Parquet, AVRO, ORC, Pickle)
- Advanced compression support (GZ, BZ2, XZ, ZIP, 7Z, LZ4, ZSTD)
- Performance features: `write_bulk()`, `reset()`, DuckDB engine integration
- Context manager support for proper resource management
- Automatic format and compression detection

## Goals / Non-Goals

### Goals
- Unify all commands to use external `iterabledata` library
- Leverage advanced features (`write_bulk()`, `reset()`, context managers)
- Improve performance through batch operations and DuckDB engine
- Reduce code maintenance by removing duplicate functionality
- Maintain backward compatibility at CLI level

### Non-Goals
- Changing CLI interface or command behavior (internal implementation only)
- Removing support for any currently supported formats
- Breaking existing workflows or scripts
- Adding new format support beyond what external library provides

## Decisions

### Decision: Complete Migration to External Library
**What**: Migrate all commands from local `IterableData` to `open_iterable()` from external library.

**Why**: 
- External library provides superior format support and features
- Reduces maintenance burden (single implementation to maintain)
- Enables advanced features like `write_bulk()` and `reset()`
- Consistent API across all commands

**Alternatives considered**:
- Keep both implementations: Rejected - creates maintenance burden and inconsistency
- Enhance local implementation: Rejected - external library already provides all needed features
- Gradual migration: Accepted - migrate command by command to minimize risk

### Decision: Use Context Managers for Resource Management
**What**: All iterable operations SHALL use `with` statements or explicit `close()` calls.

**Why**:
- Ensures proper resource cleanup even on errors
- Prevents file handle leaks
- Follows Python best practices

**Alternatives considered**:
- Rely on garbage collection: Rejected - not reliable for file handles
- Manual close() only: Accepted as fallback but `with` preferred

### Decision: Implement Batch Writes with write_bulk()
**What**: Commands writing data SHALL use `write_bulk()` for batch operations instead of individual `write()` calls.

**Why**:
- Significantly improves performance for large datasets
- Reduces I/O overhead
- Better memory efficiency

**Alternatives considered**:
- Keep individual writes: Rejected - performance impact too significant
- Always use bulk: Accepted - use bulk for batches, individual for single records

### Decision: Support DuckDB Engine Optionally
**What**: Performance-critical commands SHALL support DuckDB engine but SHALL fall back to iterable engine.

**Why**:
- DuckDB provides significant performance improvements for analytics operations
- Not all formats are supported by DuckDB
- Should be optional to maintain flexibility

**Alternatives considered**:
- Always use DuckDB: Rejected - format limitations
- Never use DuckDB: Rejected - missing performance opportunity
- Auto-select based on format: Accepted - best of both worlds

### Decision: Deprecate Then Remove Local Classes
**What**: Mark local `IterableData` and `DataWriter` as deprecated first, then remove after migration.

**Why**:
- Provides migration path for any external code using these classes
- Allows time for testing and validation
- Clear deprecation warnings guide users to new API

**Alternatives considered**:
- Remove immediately: Rejected - too risky, may break external code
- Keep forever: Rejected - defeats purpose of migration
- Deprecate then remove: Accepted - balanced approach

## Risks / Trade-offs

### Risk: Breaking External Code Using Local Classes
**Mitigation**: 
- Add deprecation warnings with clear migration instructions
- Keep classes during transition period
- Document migration path in CHANGELOG

### Risk: Performance Regression
**Mitigation**:
- Benchmark before and after migration
- Use `write_bulk()` for batch operations
- Leverage DuckDB engine where applicable
- Test with large files

### Risk: Format Support Differences
**Mitigation**:
- Verify external library supports all currently supported formats
- Test format detection and handling
- Maintain format-specific option passing via `iterableargs`

### Risk: Migration Complexity
**Mitigation**:
- Migrate command by command
- Comprehensive testing after each migration
- Keep local classes until all commands migrated
- Document any API differences

## Migration Plan

### Phase 1: Preparation
1. Document all current usages of local `IterableData` and `DataWriter`
2. Verify external library supports all required formats
3. Create test cases for each command using iterable data

### Phase 2: Command Migration
1. Migrate `query` command (simplest, isolated usage)
2. Complete `selector` command migration
3. Update `converter` to use `write_bulk()`
4. Update `transformer` to use context managers and `reset()`
5. Review and update other commands for consistency

### Phase 3: Advanced Features
1. Implement context managers across all commands
2. Add `write_bulk()` where applicable
3. Integrate DuckDB engine for performance-critical operations
4. Add `reset()` support where needed

### Phase 4: Cleanup
1. Mark local classes as deprecated
2. Remove unused imports
3. Update examples and documentation
4. Remove local classes after deprecation period

### Rollback Plan
- Keep local classes available during migration
- Can revert individual command migrations if issues found
- Full rollback possible by reverting commits

## Open Questions

- [ ] Should we maintain a compatibility shim for external code using local classes?
- [ ] What is the appropriate deprecation period before removal?
- [ ] Should DuckDB engine be the default for supported formats, or opt-in?
- [ ] How to handle format-specific options that differ between local and external implementations?
