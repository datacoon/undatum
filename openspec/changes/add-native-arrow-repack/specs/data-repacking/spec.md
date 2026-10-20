## ADDED Requirements

### Requirement: Native Arrow IPC Preservation
Repack SHALL support declared native Arrow IPC file/stream and Feather v2 profiles,
preserving framing, schema/field metadata, dictionary semantics, values/order and
significant batch/message metadata. Unsupported preservation profiles SHALL be refused
before publication. Native output SHALL use the common verified atomic lifecycle.

#### Scenario: Dictionary batches
- **WHEN** supported IPC input contains dictionary arrays across multiple batches
- **THEN** successful output retains dictionary semantics and batch boundaries
- **AND** nested values and floating-point bit patterns pass independent verification

#### Scenario: Empty IPC stream
- **WHEN** an empty supported IPC stream is repacked
- **THEN** the published artifact retains its schema and stream framing
- **AND** a zero-byte file is not published

#### Scenario: Unrepresentable custom metadata
- **WHEN** the selected writer cannot preserve significant custom message metadata
- **THEN** repack refuses the profile and leaves source and prior destination intact
