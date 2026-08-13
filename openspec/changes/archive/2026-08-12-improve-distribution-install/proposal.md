# Change: Improve Distribution and Installation

## Why
Distribution is the #1 external pain theme: Homebrew formula #4 has been open 5+ years with
`help wanted`, and competing tools win ops contexts with static binaries. README should also
promote `uv tool install` / `pipx` as first-class paths.

## What Changes
- Ship or officially document a macOS Homebrew path (formula wrapping a binary, **or** honest
  docs for `brew install pipx && pipx install undatum` and close #4 accordingly).
- Provide single-binary release artifacts (PyInstaller / pex / shiv) for linux/mac/win per
  release.
- Document `uv tool install undatum` and `pipx install undatum` in the README header as
  first-class install methods.

## Impact
- Affected specs: `distribution`
- Affected code/docs: release workflow, README, optional packaging scripts, issue #4 resolution
- Related issues: #4
