# Change: Improve Repository Hygiene

## Why
Committed noise (e.g. `pylint_report.txt`, large fixture dumps, IDE dirs) and CHANGELOG
placeholder dates (`2024-XX-XX`) / missing 1.2.0 section signal low release maturity to
evaluators comparing undatum against miller/csvkit.

## What Changes
- Remove committed generated reports, oversized sample dumps, and IDE directories that should be
  gitignored.
- Fix CHANGELOG placeholder dates and restore/complete the missing 1.2.0 section as applicable.
- Update `.gitignore` so hygiene issues do not recur.

## Impact
- Affected specs: `release-quality`
- Affected code/docs: repo root artifacts, `.gitignore`, `CHANGELOG.md`
- No runtime behavior change
