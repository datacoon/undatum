# Change: Bounded opt-in compression candidate trials

## Why

The preserving repack core provides a verified baseline and measured acceptance policy.
Some workloads can benefit from stronger gzip or alternate zstd/XZ settings, but more
CPU, scratch or memory does not reliably produce a smaller artifact. Trials should be
explicit, budgeted and compared with that verified baseline.

## What Changes

- Add an opt-in effort mode after `improve-repack-preservation` is accepted.
- Try a bounded number of compatible candidates using available pigz, libdeflate/zopfli,
  zstd and XZ implementations; missing optional tools do not disable the ordinary core.
- Independently verify every candidate, keep the smallest qualifying verified file,
  and preserve the baseline when trials fail, time out or provide no benefit.
- Report backend/version/settings, candidate size, verification and resources per trial.
- Keep current profiles and default single-candidate behavior unchanged.

## Impact

- Affected specs: data-repacking.
- Affected code: repacker orchestration, supervised workers, CLI and result schema.
- Requires benchmark evidence of size, encode/decode time, RSS and live scratch before
  enabling any backend by default. No implementation is included in this follow-up.
