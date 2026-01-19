# Doc Command Improvement Report

**Date:** 2026-01-19  
**Purpose:** Research and recommendations for improving `undatum doc` output  
**Status:** Research Phase - No Code Changes

---

## Executive Summary

This report proposes enhancements to the `doc` command to generate richer dataset documentation, focusing on descriptive metadata and risk indicators. The key additions are: title, keywords, geographic coverage, temporal coverage, language detection, EU data theme classification, and PII/semantic type labeling via Metacrafter. The approach emphasizes a hybrid strategy: deterministic extraction from data when possible, and AI augmentation with structured prompts and confidence flags.

Primary reference for thematic classification is the EU Data Theme controlled vocabulary aligned with DCAT-AP.  
Source: [EU Data Theme vocabulary](https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/data-theme)

---

## 1. Current Behavior (Baseline)

The `doc` command produces a report that includes:
- **Metadata** (filename, size, type, compression, encoding)
- **Summary** (tables, records)
- **Schema** (fields, types, AI-generated descriptions when `--autodoc`)
- **Statistics** (DuckDB unique counts when supported)
- **Samples** (bounded record samples)

AI augmentation is already used for dataset and field descriptions; this establishes a foundation for additional AI-derived metadata.

---

## 2. Proposed Metadata Additions

### 2.1 Title
**Goal:** Human-readable dataset title.

**Extraction Strategy:**
- Rule-based: derive from filename stem (remove extensions, separators).
- AI: infer title from sample rows, schema, and dataset description.

**Output Shape:**
```json
{"title": "City Parking Violations (2021-2024)"}
```

### 2.2 Keywords
**Goal:** 5-15 topical tags for search and cataloging.

**Extraction Strategy:**
- Rule-based: schema name patterns and frequent categorical values.
- AI: structured output list, normalized casing.

**Output Shape:**
```json
{"keywords": ["parking", "violations", "city", "enforcement", "tickets"]}
```

### 2.3 Geographic Coverage
**Goal:** Identify spatial scope.

**Extraction Strategy:**
- Detect geographic columns (country, region, city, lat/lon).
- Sample values to identify countries/regions.
- AI summary for ambiguous cases.

**Output Shape:**
```json
{
  "geographic_coverage": {
    "countries": ["DE", "FR"],
    "regions": ["Bavaria"],
    "coordinates_present": true
  }
}
```

### 2.4 Temporal Coverage
**Goal:** Date/time range and granularity.

**Extraction Strategy:**
- Detect date/time columns and compute min/max (DuckDB).
- Infer granularity from field names and values.
- AI fallback if date parsing is inconsistent.

**Output Shape:**
```json
{
  "temporal_coverage": {
    "start": "2018-01-01",
    "end": "2024-12-31",
    "granularity": "daily"
  }
}
```

### 2.5 Language Information
**Goal:** Language(s) used in text fields.

**Extraction Strategy:**
- Detect language from sampled text fields.
- Aggregate to top languages with confidence scores.
- AI fallback for short or sparse text.

**Output Shape:**
```json
{
  "languages": [
    {"code": "en", "confidence": 0.92},
    {"code": "fr", "confidence": 0.06}
  ]
}
```

### 2.6 EU Data Theme Classification
**Goal:** Map dataset to controlled EU Data Theme vocabulary.

**Extraction Strategy:**
- AI classification restricted to EU Data Theme list.
- Optional keyword mapping for non-AI fallback.
- Store theme label and URI.

**Output Shape:**
```json
{
  "data_theme": {
    "label": "TRANSPORT",
    "uri": "http://publications.europa.eu/resource/authority/data-theme/TRAN"
  }
}
```

Source: [EU Data Theme vocabulary](https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/data-theme)

---

## 3. Metacrafter Integration (Semantic Types + PII)

Metacrafter provides rule-based semantic type detection and PII labeling using YAML rules and a semantic type registry. It enables detection of common PII classes (names, emails, phone numbers, IDs) and broader semantic types for schema enrichment.

Sources:  
- [Metacrafter on PyPI](https://pypi.org/project/metacrafter/)  
- [APIcrafter Semantic Type Registry](https://registry.apicrafter.io/)

**Doc Command Improvements Using Metacrafter:**
- Add a `semantic_types` field per schema column.
- Add a `pii_fields` summary section listing detected PII fields and matched rules.
- Optionally include confidence or rule IDs for traceability.

**Output Shape:**
```json
{
  "semantic_types": {
    "email": [{"type": "email", "source": "rule:email"}],
    "name": [{"type": "person_fullname", "source": "rule:person_fullname"}]
  },
  "pii_fields": [
    {"field": "email", "type": "email", "confidence": 0.96},
    {"field": "ssn", "type": "usssn", "confidence": 0.90}
  ]
}
```

**Operational Considerations:**
- Sample data for content-based matching to preserve streaming constraints.
- Keep rule registry versioned and cached to avoid runtime network dependencies.
- Allow opt-in flags (e.g., `--pii-detect`, `--semantic-types`) to avoid cost/latency by default.

---

## 4. AI Prompt Strategy (Structured Metadata)

To prevent hallucinations and ensure consistent output:

**Input context:**
- Schema with types
- Sample records
- Summary stats (if available)
- Existing AI description

**Output format:**
- Strict JSON with required keys
- Confidence fields per section
- Optional "evidence" strings pointing to columns or sample values

**Example output keys:**
```json
{
  "title": "...",
  "keywords": ["..."],
  "geographic_coverage": {...},
  "temporal_coverage": {...},
  "languages": [...],
  "data_theme": {...},
  "confidence": {
    "title": 0.72,
    "keywords": 0.81,
    "geographic_coverage": 0.64
  }
}
```

---

## 5. Risks and Mitigations

**Risk: Hallucinated metadata**  
- Mitigation: require evidence in AI output (columns or values referenced)
- Mitigation: include confidence scores and mark low-confidence outputs as tentative

**Risk: PII false positives/negatives**  
- Mitigation: emit both strict and "suspected" PII with thresholds
- Mitigation: show rule IDs and detection sources for auditability

**Risk: Privacy exposure in AI calls**  
- Mitigation: redact/mask sensitive columns before AI prompts
- Mitigation: keep AI augmentation opt-in and off by default

**Risk: Performance regressions**  
- Mitigation: sample only a small number of rows for content-based analysis
- Mitigation: reuse DuckDB where already enabled for statistics

**Risk: Language and locale bias**  
- Mitigation: report multiple candidate languages and confidence
- Mitigation: fall back to metadata-only when text is sparse

---

## 6. Recommended Report Layout (Doc Output)

To keep output consistent across formats:
- **Metadata**: title, keywords, language, data_theme, file details
- **Coverage**: geographic_coverage, temporal_coverage
- **Schema**: field list with semantic types and PII tags
- **Statistics**: unchanged
- **Samples**: unchanged (but mask PII if flagged)

---

## 7. Implementation Phases (No Code Yet)

**Phase 1: Deterministic extraction**
- Title from filename
- Temporal coverage from date columns
- Geographic hints from column names

**Phase 2: AI augmentation**
- Title, keywords, data_theme
- Language detection and coverage summaries

**Phase 3: Metacrafter integration**
- Semantic types per field
- PII summary and masking options

---

## 8. Testing and Validation

- Verify new metadata keys appear in JSON/YAML/Markdown/Text outputs
- Validate schema-level semantic types and PII detection shapes
- Confirm DuckDB-only computations are optional and failure-tolerant
- Check that AI failure falls back to deterministic metadata

---

## 9. Open Questions

- Should AI outputs be saved as raw JSON in the report for auditability?
- What default threshold should be used for PII classification confidence?
- Should PII-masked samples be the default when PII is detected?
- Do we want a CLI flag to emit a separate PII report file?
#+#+#+#+
# Doc Command Improvement Report

**Date:** 2026-01-19  
**Purpose:** Research and recommendations for improving `undatum doc` output  
**Status:** Research Phase - No Code Changes

---

## Executive Summary

This report proposes enhancements to the `doc` command to generate richer dataset documentation, focusing on descriptive metadata and risk indicators. The key additions are: title, keywords, geographic coverage, temporal coverage, language detection, EU data theme classification, and PII/semantic type labeling via Metacrafter. The approach emphasizes a hybrid strategy: deterministic extraction from data when possible, and AI augmentation with structured prompts and confidence flags.

Primary reference for thematic classification is the EU Data Theme controlled vocabulary aligned with DCAT-AP.  
Source: [EU Data Theme vocabulary](https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/data-theme)

---

## 1. Current Behavior (Baseline)

The `doc` command produces a report that includes:
- **Metadata** (filename, size, type, compression, encoding)
- **Summary** (tables, records)
- **Schema** (fields, types, AI-generated descriptions when `--autodoc`)
- **Statistics** (DuckDB unique counts when supported)
- **Samples** (bounded record samples)

AI augmentation is already used for dataset and field descriptions; this establishes a foundation for additional AI-derived metadata.

---

## 2. Proposed Metadata Additions

### 2.1 Title
**Goal:** Human-readable dataset title.

**Extraction Strategy:**
- Rule-based: derive from filename stem (remove extensions, separators).
- AI: infer title from sample rows, schema, and dataset description.

**Output Shape:**
```json
{"title": "City Parking Violations (2021-2024)"}
```

### 2.2 Keywords
**Goal:** 5-15 topical tags for search and cataloging.

**Extraction Strategy:**
- Rule-based: schema name patterns and frequent categorical values.
- AI: structured output list, normalized casing.

**Output Shape:**
```json
{"keywords": ["parking", "violations", "city", "enforcement", "tickets"]}
```

### 2.3 Geographic Coverage
**Goal:** Identify spatial scope.

**Extraction Strategy:**
- Detect geographic columns (country, region, city, lat/lon).
- Sample values to identify countries/regions.
- AI summary for ambiguous cases.

**Output Shape:**
```json
{
  "geographic_coverage": {
    "countries": ["DE", "FR"],
    "regions": ["Bavaria"],
    "coordinates_present": true
  }
}
```

### 2.4 Temporal Coverage
**Goal:** Date/time range and granularity.

**Extraction Strategy:**
- Detect date/time columns and compute min/max (DuckDB).
- Infer granularity from field names and values.
- AI fallback if date parsing is inconsistent.

**Output Shape:**
```json
{
  "temporal_coverage": {
    "start": "2018-01-01",
    "end": "2024-12-31",
    "granularity": "daily"
  }
}
```

### 2.5 Language Information
**Goal:** Language(s) used in text fields.

**Extraction Strategy:**
- Detect language from sampled text fields.
- Aggregate to top languages with confidence scores.
- AI fallback for short or sparse text.

**Output Shape:**
```json
{
  "languages": [
    {"code": "en", "confidence": 0.92},
    {"code": "fr", "confidence": 0.06}
  ]
}
```

### 2.6 EU Data Theme Classification
**Goal:** Map dataset to controlled EU Data Theme vocabulary.

**Extraction Strategy:**
- AI classification restricted to EU Data Theme list.
- Optional keyword mapping for non-AI fallback.
- Store theme label and URI.

**Output Shape:**
```json
{
  "data_theme": {
    "label": "TRANSPORT",
    "uri": "http://publications.europa.eu/resource/authority/data-theme/TRAN"
  }
}
```

Source: [EU Data Theme vocabulary](https://op.europa.eu/en/web/eu-vocabularies/dataset/-/resource?uri=http://publications.europa.eu/resource/dataset/data-theme)

---

## 3. Metacrafter Integration (Semantic Types + PII)

Metacrafter provides rule-based semantic type detection and PII labeling using YAML rules and a semantic type registry. It enables detection of common PII classes (names, emails, phone numbers, IDs) and broader semantic types for schema enrichment.

Sources:  
- [Metacrafter on PyPI](https://pypi.org/project/metacrafter/)  
- [APIcrafter Semantic Type Registry](https://registry.apicrafter.io/)

**Doc Command Improvements Using Metacrafter:**
- Add a `semantic_types` field per schema column.
- Add a `pii_fields` summary section listing detected PII fields and matched rules.
- Optionally include confidence or rule IDs for traceability.

**Output Shape:**
```json
{
  "semantic_types": {
    "email": [{"type": "email", "source": "rule:email"}],
    "name": [{"type": "person_fullname", "source": "rule:person_fullname"}]
  },
  "pii_fields": [
    {"field": "email", "type": "email", "confidence": 0.96},
    {"field": "ssn", "type": "usssn", "confidence": 0.90}
  ]
}
```

**Operational Considerations:**
- Sample data for content-based matching to preserve streaming constraints.
- Keep rule registry versioned and cached to avoid runtime network dependencies.
- Allow opt-in flags (e.g., `--pii-detect`, `--semantic-types`) to avoid cost/latency by default.

---

## 4. AI Prompt Strategy (Structured Metadata)

To prevent hallucinations and ensure consistent output:

**Input context:**
- Schema with types
- Sample records
- Summary stats (if available)
- Existing AI description

**Output format:**
- Strict JSON with required keys
- Confidence fields per section
- Optional "evidence" strings pointing to columns or sample values

**Example output keys:**
```json
{
  "title": "...",
  "keywords": ["..."],
  "geographic_coverage": {...},
  "temporal_coverage": {...},
  "languages": [...],
  "data_theme": {...},
  "confidence": {
    "title": 0.72,
    "keywords": 0.81,
    "geographic_coverage": 0.64
  }
}
```

---

## 5. Risks and Mitigations

**Risk: Hallucinated metadata**  
