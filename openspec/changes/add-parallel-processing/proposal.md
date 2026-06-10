# Change: Add Parallel & Chunked Processing

## Why

Undatum currently processes data sequentially, which limits performance on large files and
multi-core systems. When DuckDB is not used (unsupported formats or forced Python engine),
operations can be slow for multi-GB files. Parallel processing and chunked I/O would enable
efficient processing of large datasets on modest hardware.

**Current Issues:**
1. **Sequential processing**: All operations run on a single thread
2. **Memory pressure**: Large files may be loaded entirely into memory
3. **No progress indication**: Users cannot track progress of long-running operations
4. **Underutilized hardware**: Multi-core systems are not leveraged

**Expected Benefits:**
- **2-8x performance improvement** on multi-core systems for CPU-bound operations
- **Constant memory usage** regardless of file size
- **Better user experience** with progress indicators
- **Scalability** for large files on modest hardware

## Implementation Reference

**Primary Reference Document:** `dev/docs/mirothinker/MIROTHINKER_IMPROVEMENT_ROADMAP.md` (Section 1.2)

## What Changes

- **ADDED**: Chunked streaming I/O for all commands
  - Read N lines/records at a time (configurable batch size)
  - Process and write output incrementally
  - Keep memory usage roughly constant
- **ADDED**: `--threads N` option for CPU-bound operations
  - Apply to `convert`, `stats`, `frequency`, `dedup`, `search`, etc.
  - Use multiprocessing or multithreading depending on workload
  - Default to number of CPU cores when not specified
- **ADDED**: `--progress` flag for progress indication
  - Textual or progress-bar style indicator
  - Show approximate percentage, estimated time, and throughput
  - Optional for all commands

All changes maintain backward compatibility. Single-threaded processing remains default when
`--threads` is not specified.

## Impact

- **Affected specs**: `data-processing` capability
- **Affected code**:
  - `undatum/cmds/converter.py` - Add chunked I/O and threading
  - `undatum/cmds/statistics.py` - Add parallel processing for stats
  - `undatum/cmds/searcher.py` - Add parallel search processing
  - `undatum/cmds/deduplicator.py` - Add parallel deduplication
  - `undatum/cmds/filler.py` - Add parallel processing
  - `undatum/core.py` - Add `--threads` and `--progress` options
  - New shared module for parallel processing utilities
  - New shared module for progress indication
- **Dependencies**: No new dependencies (use standard library `multiprocessing` and `threading`)
- **Backward compatibility**: Fully backward compatible - single-threaded processing remains default
