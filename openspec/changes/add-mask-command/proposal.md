# Change: Add Mask Command for Data Anonymization

## Why

Undatum currently has no built-in support for protecting sensitive data (PII). Users working with
sensitive datasets need to mask or anonymize data for development, demos, or data sharing, but must
use external tools or custom scripts. Adding a `mask` command would make PII protection a
first-class feature.

**Current Issues:**
1. **No PII protection**: No built-in way to mask sensitive data
2. **External dependencies**: Users must use separate tools
3. **Workflow friction**: Extra steps required for data anonymization

**Expected Benefits:**
- **Built-in PII protection** for sensitive data
- **Safe data sharing** for development and demos
- **Multiple masking methods** for different use cases
- **Deterministic hashing** for join-preserving anonymization

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 4.1)

## What Changes

- **ADDED**: `undatum mask` command for data masking/anonymization
- **ADDED**: `--fields` option to specify fields to mask (comma-separated)
- **ADDED**: `--method` option with three methods:
  - `redact`: Replace with fixed token (e.g., `***`)
  - `hash`: Deterministic one-way hash (preserves joins, hides identities)
  - `randomize`: Replace with random but type-compatible values
- **ADDED**: Type-aware masking (emails, phone numbers, ages, etc.)
- **ADDED**: Support for CSV, JSONL, and other formats

All changes are additive. No existing functionality is modified.

## Impact

- **Affected specs**: `data-security` capability
- **Affected code**:
  - New `undatum/cmds/masker.py` module for mask command implementation
  - `undatum/core.py` - Add `mask` command to CLI
  - New `undatum/common/masking.py` module for masking utilities
- **Dependencies**: No new dependencies (use standard library `hashlib` for hashing)
- **Backward compatibility**: Fully backward compatible - new command only
