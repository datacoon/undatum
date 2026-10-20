# Change: Preserving native Arrow IPC and Feather repack

## Why

The preserving core handles Parquet, ORC, Avro and byte streams. Arrow IPC and Feather
need native batch/framing contracts: passing through records can lose dictionary
identity, schema metadata and custom batch messages.

## What Changes

- Add native Arrow IPC file/stream and Feather v2 adapters with framing preflight.
- Preserve schemas, field metadata, dictionary semantics, values/order, batch boundaries
  and significant custom message metadata, or refuse profiles the backend cannot retain.
- Apply verified compression candidates through the shared atomic publication and budgets.
- Keep Feather v1, encrypted or unsupported extensions, ZIP/7z and archive preservation
  out of this change; they require their own capabilities.

## Impact

- Affected specs: data-repacking.
- Affected code: dedicated Arrow adapter, format detection, CLI capabilities, tests/docs.
- Depends on acceptance of `improve-repack-preservation`; requires API checks on minimum
  PyArrow and fidelity benchmarks. No implementation is included in this follow-up.
