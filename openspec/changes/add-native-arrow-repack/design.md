# Native Arrow design

Detect IPC file versus stream framing by magic/message parsing independently of the
suffix. Feather v2 is an IPC file with a declared compatibility contract. Resolve
`none`, LZ4 and zstd codec/level capabilities before creating a stage.

Iterate record batches directly, retaining schema and batch boundaries. Inspect
custom metadata and dictionaries using native message APIs at the supported PyArrow
floor. If the writer cannot faithfully reproduce a property, reject the profile
rather than converting it to Python records. Empty files retain schema and framing.

Independently reopen the candidate, compare framing, schema/field metadata, batch
counts/boundaries, dictionaries and visible values (bitwise floats, nested nulls,
decimals and timestamps). Bind proof to candidate bytes. Reuse the common atomic
target, snapshots, metadata restoration and no-growth/dry-run policy. Large dictionaries
remain native memory costs; requested RSS limits use the existing worker monitor.

Benchmark size, encode/verify time, downstream Arrow scans, dictionary cardinality,
peak RSS and scratch against plain IPC and supported filerepack paths. Cover both
minimum and current dependency versions before advertising the capability.
