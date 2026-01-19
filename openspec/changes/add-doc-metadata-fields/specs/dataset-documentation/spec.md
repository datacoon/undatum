## ADDED Requirements
### Requirement: Extended metadata fields
The documentation report SHALL include extended metadata fields for title, keywords,
geographic coverage, temporal coverage, languages, and EU data theme.

#### Scenario: Metadata includes extended fields
- **WHEN** the system generates a documentation report
- **THEN** the metadata contains `title`, `keywords`, `geographic_coverage`,
  `temporal_coverage`, `languages`, and `data_theme` keys (empty or null if unknown)

### Requirement: EU data theme classification
The system SHALL classify datasets using the EU Data Theme controlled vocabulary
and include the label and URI in the documentation metadata when classification is available.

#### Scenario: Data theme is provided
- **WHEN** the system can classify the dataset theme
- **THEN** the documentation includes a `data_theme` object with a vocabulary label and URI

#### Scenario: Data theme is unavailable
- **WHEN** the system cannot confidently classify the dataset theme
- **THEN** the `data_theme` metadata value is empty or null

### Requirement: Optional semantic typing
When semantic typing is enabled, the system SHALL include semantic type annotations
for schema fields in the documentation output.

#### Scenario: Semantic types enabled
- **WHEN** the user runs `undatum doc <input-file> --semantic-types`
- **THEN** the documentation includes per-field `semantic_types` annotations

### Requirement: Optional PII detection
When PII detection is enabled, the system SHALL identify PII fields and include a PII
summary in the documentation output.

#### Scenario: PII detection enabled
- **WHEN** the user runs `undatum doc <input-file> --pii-detect`
- **THEN** the documentation includes a `pii_fields` summary with matched types

### Requirement: Optional PII masking for samples
When PII masking is enabled, the system SHALL redact sensitive values in sample records.

#### Scenario: PII masking enabled
- **WHEN** the user runs `undatum doc <input-file> --pii-detect --pii-mask-samples`
- **THEN** the sample records are redacted for detected PII fields

## MODIFIED Requirements
### Requirement: Optional AI augmentation
When `--autodoc` is enabled and an AI provider is configured, the system SHALL
enrich dataset summaries, field descriptions, and structured metadata; if AI
initialization fails, it SHALL continue without AI enhancements.

#### Scenario: AI provider unavailable
- **WHEN** `--autodoc` is enabled but the provider cannot initialize
- **THEN** the documentation is still generated without AI content

#### Scenario: Structured metadata generated
- **WHEN** `--autodoc` is enabled and the provider returns structured metadata
- **THEN** the documentation includes AI-enriched metadata values
