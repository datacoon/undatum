# Change: Improve Stats Command Progress Indication

## Why

The `undatum stats` command currently lacks user-facing progress indication, making it difficult to track progress during long-running operations on large datasets. Based on comprehensive review (see `dev/docs/STATS_COMMAND_REVIEW.md`), the command:

1. **No visible progress**: Only debug-level logging every 1000 records, providing no feedback to users
2. **Poor user experience**: Users cannot estimate completion time or verify the process is working
3. **Inconsistent with other commands**: Other commands (`converter.py`, `ingester.py`, `schemer.py`) already use `tqdm` for progress indication
4. **No throughput information**: Users cannot see processing rate (rows/second) to assess performance

This improvement follows the established patterns in other commands and significantly improves usability for large file processing.

## What Changes

- **MODIFIED**: `undatum/cmds/statistics.py` - Add `tqdm` progress bar to main processing loop
- **MODIFIED**: `undatum/cmds/statistics.py` - Add descriptive progress label ("Analyzing statistics")
- **MODIFIED**: `undatum/cmds/statistics.py` - Add unit parameter ("rows") to progress bar
- **MODIFIED**: `undatum/cmds/statistics.py` - Add optional throughput display (rows/second) with `set_postfix()`
- **MODIFIED**: `undatum/core.py` - Add optional `--progress` flag to show/hide progress bar (default: show)
- **MODIFIED**: `undatum/core.py` - Add optional `--no-progress` flag for non-interactive use

All changes maintain backward compatibility. The progress bar will be shown by default but can be disabled for non-interactive use cases.

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/cmds/statistics.py` - Add `tqdm` import and wrap iteration loop
  - `undatum/core.py` - Add progress control options to `stats` command
- **Dependencies**: `tqdm` is already a project dependency (listed in `requirements.txt` and `pyproject.toml`)
- **Backward compatibility**: Fully backward compatible - existing functionality unchanged, only adds visual feedback
