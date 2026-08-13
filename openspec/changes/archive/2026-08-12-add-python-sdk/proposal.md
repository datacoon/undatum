# Change: Add Python SDK

## Why

Undatum is currently CLI-only, requiring users to shell out from Python scripts or notebooks. This
limits integration with Python workflows and makes programmatic usage cumbersome. A Python SDK
would enable seamless integration with Python data science workflows, notebooks, and applications.

**Current Issues:**
1. **CLI-only interface**: Must use subprocess or shell commands from Python
2. **No programmatic API**: Difficult to integrate into Python workflows
3. **Notebook friction**: Awkward to use in Jupyter notebooks
4. **Limited composability**: Hard to chain operations programmatically

**Expected Benefits:**
- **Native Python integration** for scripts and notebooks
- **Method chaining** for fluent data processing pipelines
- **Consistent behavior** between CLI and SDK
- **Better developer experience** for Python users

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 6.1)

## What Changes

- **ADDED**: `undatum.Dataset` class as primary SDK interface
- **ADDED**: `Dataset.read()` method for reading data from files
- **ADDED**: `Dataset.write()` method for writing data to files
- **ADDED**: Transform methods mapped from CLI commands:
  - `fill()`, `dedup()`, `sort()`, `filter()`, `select()`, etc.
- **ADDED**: Analysis methods:
  - `stats()`, `count()`, `head()`, `tail()`, etc.
- **ADDED**: Method chaining support for fluent pipelines
- **ADDED**: Consistent behavior with CLI commands

All changes are additive. CLI functionality remains unchanged.

## Impact

- **Affected specs**: `sdk` capability
- **Affected code**:
  - New `undatum/sdk/` package
  - `undatum/sdk/dataset.py` - Main Dataset class
  - `undatum/sdk/__init__.py` - Public API exports
  - Integration with existing command processors
- **Dependencies**: No new dependencies (reuse existing undatum modules)
- **Backward compatibility**: Fully backward compatible - CLI remains unchanged
