## MODIFIED Requirements
### Requirement: Recent files are paths only
The TUI MAY persist recently opened dataset locations for the file picker.
History SHALL store paths (or URIs) only, never sample row contents.

#### Scenario: Record opened path
- **WHEN** the user opens a dataset in the TUI
- **THEN** the system MAY append that path to `~/.undatum/tui-history.json`
- **AND** SHALL NOT write record values from the sample into that file

#### Scenario: Open an S3 URI
- **WHEN** the user enters an `s3://` URI in the file picker
- **THEN** the system SHALL open it through the existing cloud path helpers
- **AND** SHALL NOT require a local DirectoryTree listing of the bucket

#### Scenario: Open a GCS URI
- **WHEN** the user enters a `gs://` URI in the file picker
- **THEN** the system SHALL open it through the existing cloud path helpers
- **AND** SHALL NOT require a local DirectoryTree listing of the bucket

#### Scenario: Open an Azure URI
- **WHEN** the user enters an `az://` URI in the file picker
- **THEN** the system SHALL open it through the existing cloud path helpers
- **AND** SHALL NOT require a local DirectoryTree listing of the container
