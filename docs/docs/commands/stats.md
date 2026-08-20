---
title: "stats / profile"
description: "undatum stats / profile command reference"
---
# `stats / profile`

Generates comprehensive statistics and profiling metrics about your dataset. With DuckDB engine, statistics generation is 10-100x faster for supported formats (CSV, JSONL, JSON, Parquet).

```bash
# Basic statistics
undatum stats data.jsonl

# Enhanced profiling (alias)
undatum profile data.csv

# With date detection
undatum stats data.csv --checkdates

# Using DuckDB engine
undatum stats data.parquet --engine duckdb

# Machine-readable JSON (also used when --output ends in .json)
undatum stats data.csv --format-out json --output stats.json

# HTML or Markdown profiling report (also inferred from --output extension)
undatum stats data.csv --format-out html --output profile.html
undatum stats data.csv --output profile.md

# Nested JSONL: unfold dict fields onto dotted paths
undatum stats nested.jsonl --flatten-nested --format-out json
undatum stats nested.jsonl --flatten-nested --max-nested-depth 2
undatum stats nested.jsonl --flatten-nested --no-keep-nested-parents

# Named Excel sheet
undatum stats workbook.xlsx --table Sheet2
```

**Statistics include:**
- Field types and array flags
- **Missing value rates** (count and percentage)
- **Cardinality analysis** (distinct counts and percentages)
- **Type inference** (categorical vs numerical classification)
- **Distribution statistics** for numerical fields (mean, median, percentiles, min/max, stddev)
- Unique value counts and percentages
- Min/max/average lengths
- Date field detection

**Performance:** DuckDB engine automatically selected for supported formats, providing columnar processing and SQL-based aggregations for faster statistics.

**Profile Command:** The `profile` command is an alias for `stats` with a focus on data profiling and quality metrics.

#### Profiling Metrics Explained

The enhanced statistics output provides comprehensive data profiling:

**Missing Value Analysis:**
- Shows count and percentage of missing/null values per field
- Helps identify data quality issues and incomplete records
- Example: `5 (2.5%)` means 5 missing values out of 200 records (2.5%)

**Cardinality Analysis:**
- **Distinct count**: Number of unique values in a field
- **Cardinality percentage**: Percentage of distinct values (distinct/total)
- **High cardinality**: Fields with many unique values (e.g., IDs, timestamps)
- **Low cardinality**: Fields with few unique values (e.g., status codes, categories)
- Example: `150 (75%)` means 150 distinct values out of 200 records

**Type Inference:**
- **Categorical**: Fields with low cardinality, typically string-like values (e.g., status, category, country)
- **Numerical**: Fields with numeric types and high cardinality (e.g., age, price, score)
- **Mixed**: Fields that don't clearly fit categorical or numerical patterns
- Helps understand data structure and choose appropriate analysis methods

**Distribution Statistics (Numerical Fields):**
- **Mean (μ)**: Average value
- **Median (m)**: Middle value (50th percentile)
- **Percentiles**: 25th, 75th, 90th, 95th, 99th percentiles for outlier detection
- **Min/Max**: Range of values
- **Standard deviation**: Measure of data spread
- Example output: `μ=42.5, m=40.0` shows mean of 42.5 and median of 40.0

#### Use Cases

**Data Quality Assessment:**
```bash
# Profile dataset to identify quality issues
undatum profile customer_data.csv

# Look for:
# - High missing value rates (>10% may indicate data collection issues)
# - Unexpected cardinality (e.g., status field with 1000+ unique values)
# - Outliers in numerical fields (check min/max vs percentiles)
```

**Schema Discovery:**
```bash
# Understand dataset structure before processing
undatum profile new_dataset.jsonl

# Use type inference to:
# - Identify categorical fields for grouping/aggregation
# - Identify numerical fields for statistical analysis
# - Plan appropriate data transformations
```

**Data Exploration Workflows:**
```bash
# Quick profiling as part of ETL pipeline
undatum profile raw_data.csv > profile_report.txt

# Use profiling metrics to:
# - Decide on data cleaning strategies (fill missing values, handle outliers)
# - Choose appropriate aggregation methods
# - Validate data after transformations
```
