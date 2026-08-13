## 1. Streaming Sort
- [x] 1.1 Design external merge sort for record iterators
- [x] 1.2 Implement spill runs + k-way merge
- [x] 1.3 Preserve sort key / reverse / stable semantics compatible with current CLI

## 2. Streaming Dedup
- [x] 2.1 Implement disk-backed uniqueness tracking (exact by default)
- [x] 2.2 Preserve first-seen / key-field semantics of current `dedup`
- [x] 2.3 Clean up temp files on success and failure

## 3. Tests & Docs
- [x] 3.1 Unit tests for multi-run merge correctness
- [x] 3.2 Tests proving large synthetic inputs do not require full in-memory materialization
- [x] 3.3 Document large-file sort/dedup behavior and temp disk needs
