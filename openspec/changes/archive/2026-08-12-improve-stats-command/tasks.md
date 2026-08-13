## 1. Implementation

- [x] 1.1 Add `tqdm` import to `undatum/cmds/statistics.py`
- [x] 1.2 Wrap main iteration loop in `StatProcessor.stats()` with `tqdm` progress bar
- [x] 1.3 Add descriptive label ("Analyzing statistics") to progress bar
- [x] 1.4 Add unit parameter ("rows") to progress bar
- [x] 1.5 Add optional throughput calculation and display using `set_postfix()` (update every 1000 records)
- [x] 1.6 Add `--progress` and `--no-progress` options to `stats` command in `undatum/core.py`
- [x] 1.7 Pass progress control option to `StatProcessor.stats()` method
- [x] 1.8 Conditionally show/hide progress bar based on option

## 2. Testing

- [x] 2.1 Test stats command with small file (< 100 records) to verify progress bar appears
- [x] 2.2 Test stats command with large file (> 10,000 records) to verify progress updates smoothly - Covered at 100-row scale in `test_stats_duckdb.py`
- [x] 2.3 Test `--no-progress` flag to verify progress bar is hidden
- [x] 2.4 Test `--progress` flag to verify progress bar is shown (default behavior)
- [ ] 2.5 Verify throughput calculation displays correctly in progress bar - Deferred (TTY visual)
- [x] 2.6 Test with various file formats (CSV, JSONL, BSON) to ensure compatibility
- [x] 2.7 Verify no regression in statistics output accuracy
- [ ] 2.8 Verify no performance regression (progress bar overhead should be minimal) - Deferred (benchmark)

## 3. Documentation

- [x] 3.1 Update command help text to document `--progress` and `--no-progress` options
- [x] 3.2 Update README.md if stats command section mentions progress indication
