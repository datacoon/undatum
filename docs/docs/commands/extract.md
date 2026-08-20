---
title: "extract"
description: "undatum extract command reference"
---
# `extract`

Extracts tables or text from PDF/DOC/DOCX/XLS/XLSX files and outputs CSV, JSON, NDJSON, Parquet,
or a Frictionless Data Package. PDF extraction supports table, text, or OCR modes.

```bash
# PDF tables to CSV
undatum extract report.pdf --output-format csv --output report.csv

# Extract tables from multiple files
undatum extract data/*.pdf --output-format parquet --output-dir out/

# PDF text extraction for specific pages
undatum extract report.pdf --method text --pages 1-3 --output-format ndjson --output report.ndjson
```

**Optional dependencies:**
- `pdfplumber` (PDF tables/text)
- `pdf2image` + `pytesseract` (OCR)
- `textract` (legacy .doc)
