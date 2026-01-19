## ADDED Requirements
### Requirement: Markdown metadata rendering
When documentation is emitted in `markdown` format, the system SHALL render
metadata attributes as readable key-value content rather than raw serialized
structures. Scalar values SHALL be rendered inline, list values SHALL be
rendered as comma-separated items, and object values SHALL be rendered as a
nested markdown list of key-value pairs. Empty or unknown values SHALL be
rendered as `-`.

#### Scenario: Metadata values are formatted consistently
- **WHEN** the system generates markdown documentation with metadata values
  containing scalars, lists, and objects
- **THEN** the metadata section renders readable values with lists and objects
  formatted as markdown content and empty values shown as `-`
