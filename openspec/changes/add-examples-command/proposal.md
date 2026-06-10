# Change: Add Examples Command for Recipe Library

## Why

Users often need to learn how to use `undatum` for common tasks, but currently must rely on documentation or trial-and-error. Adding an `examples` command would provide a library of copy-paste ready recipes for common data processing tasks, making undatum more accessible and reducing the learning curve.

**Current Issues:**
1. **No recipe library**: Users must figure out command combinations from scratch
2. **Learning curve**: New users don't know where to start for common tasks
3. **No quick reference**: Must search documentation for each use case
4. **Inconsistent patterns**: Users may not discover best practices

**Expected Benefits:**
- **Quick learning** - Users can see and run example recipes immediately
- **Copy-paste ready** - Recipes are executable commands
- **Common tasks covered** - Library of recipes for typical workflows
- **Best practices** - Examples demonstrate recommended patterns
- **Discoverability** - Users can browse available recipes

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 3.7)

## What Changes

- **ADDED**: `undatum examples` command group:
  - `examples list` - List available example recipes
  - `examples show <name>` - Display a specific recipe with description
  - `examples run <name>` - Execute a recipe (with optional variable substitution)
- **ADDED**: Recipe library structure:
  - YAML/JSON recipe files in `examples/recipes/` directory
  - Recipe metadata (name, description, category, tags)
  - Recipe commands with variable placeholders
  - Example data files for testing recipes
- **ADDED**: Recipe execution:
  - Variable substitution in recipe commands
  - Dry-run mode to preview commands
  - Interactive mode for user confirmation

All changes are additive. No existing functionality is modified.

## Impact

- **Affected specs**: `examples` capability (new)
- **Affected code**:
  - New `undatum/cmds/examples.py` module
  - New `undatum/core.py` - Add `examples` command group
  - New `examples/recipes/` directory with recipe files
- **Dependencies**: None (uses existing undatum commands)
- **Backward compatibility**: Fully backward compatible - new command group only
