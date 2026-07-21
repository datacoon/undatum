## ADDED Requirements

### Requirement: External Merge Sort
The `sort` command SHALL support sorting datasets larger than available memory by spilling
sorted runs to temporary storage and merging them.

#### Scenario: Sort exceeds memory budget
- **WHEN** a user sorts a dataset that cannot fit in the configured memory budget
- **THEN** the command completes using external merge sort and writes correctly ordered output

#### Scenario: Small sort remains correct
- **WHEN** a user sorts a small file without special configuration
- **THEN** output ordering matches the configured sort keys

### Requirement: Disk-Backed Deduplication
The `dedup` command SHALL support exact deduplication of datasets larger than available memory
using disk-backed state rather than loading all keys into RAM.

#### Scenario: Dedup large file without full in-memory key set
- **WHEN** a user deduplicates a multi-gigabyte file by key fields
- **THEN** the command completes with exact uniqueness semantics without requiring the full key
  set to reside in memory

#### Scenario: Dedup preserves first-seen semantics
- **WHEN** duplicate keys appear in the input stream
- **THEN** the retained record matches the command's documented first-seen (or configured)
  policy
