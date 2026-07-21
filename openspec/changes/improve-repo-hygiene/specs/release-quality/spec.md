## ADDED Requirements

### Requirement: Release Artifact Hygiene
The repository SHALL avoid committing generated reports, IDE metadata, and oversized local data
dumps that are not required fixtures, and SHALL keep CHANGELOG version sections complete with
real dates.

#### Scenario: No generated reports in tree
- **WHEN** a contributor clones the repository
- **THEN** generated artifacts such as pylint report dumps and IDE project dirs are absent or
  gitignored

#### Scenario: CHANGELOG dates are concrete
- **WHEN** a reader opens `CHANGELOG.md` for released versions
- **THEN** version headings use concrete dates (not placeholders like `2024-XX-XX`) and released
  versions have corresponding sections
