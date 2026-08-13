# distribution Specification

## Purpose
TBD - created by archiving change improve-distribution-install. Update Purpose after archive.
## Requirements
### Requirement: First-Class Tool Install Documentation
Project documentation SHALL present `uv tool install undatum` and `pipx install undatum` as
first-class installation methods in the README install section.

#### Scenario: README lists modern tool installs
- **WHEN** a new user opens the README install section
- **THEN** `uv tool install` and `pipx install` instructions are visible near the top with
  pip/other methods

### Requirement: Official macOS Install Path
The project SHALL provide an official macOS installation path that either ships a Homebrew
formula or documents an honest supported alternative and closes the long-standing Homebrew
request accordingly.

#### Scenario: macOS user finds supported install
- **WHEN** a macOS user follows project docs to install undatum
- **THEN** they have a documented supported path (Homebrew formula and/or pipx/uv) without
  relying on an abandoned help-wanted issue alone

### Requirement: Single-Binary Release Artifacts
Release publishing SHALL include single-binary (or equivalent self-contained) artifacts for
major platforms so ops users can install without a pre-existing Python environment where
feasible.

#### Scenario: Release includes platform binaries
- **WHEN** a new undatum version is released
- **THEN** linux/mac/win self-contained artifacts are published alongside the Python package
  (or the release notes document a tracked exception)

