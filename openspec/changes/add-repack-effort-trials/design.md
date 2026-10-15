# Bounded trial design

Start from the ordinary verified candidate and retain the source snapshot throughout.
Each alternative writes to an owned sibling stage, uses the same preservation adapter,
and receives the remaining operation deadline and decoded/RSS/live-scratch budgets.
Bound both trial count and simultaneous live candidates. Sequential trials with one
retained winner are the initial implementation; no unbounded process fan-out.

Tool paths must be resolved explicitly and invoked with argument arrays. Verify tool
availability and supported flags before trials. Capture version/settings and keep
binary payloads out of diagnostic output. Gzip header fields follow the core contract;
stronger compression must not discard a name/comment or silently transform BGZF.

Choose the smallest verified compatible representation. Equal size keeps the earlier
baseline. No-growth/min-savings and final source/destination/candidate guards still
control publication. If an alternative cannot preserve metadata, skip/refuse that
alternative; never weaken the contract to improve the size score.

Use deterministic low/high-cardinality fixtures and large-file cases. Publish all
failed/skipped trials alongside successful measurements, and compare decoding speed
as well as output size. Extend the JSON layout additively, or bump its version if
existing fields change meaning.
