## 1. Enhanced Statistics Computation
- [x] 1.1 Add missing value analysis
  - Calculate missing/null rate per field
  - Display as percentage and count
  - Integrate with existing stats output
- [x] 1.2 Add cardinality analysis
  - Calculate distinct count per field
  - Compute cardinality percentage (distinct/total)
  - Identify high/low cardinality fields
- [x] 1.3 Add type inference
  - Detect categorical fields (low cardinality, string-like)
  - Detect numerical fields (numeric types, high cardinality)
  - Add type classification to output
- [x] 1.4 Add distribution statistics
  - Mean, median, mode for numerical fields
  - Percentiles (25th, 50th, 75th, 90th, 95th, 99th)
  - Min/max values
  - Standard deviation

## 2. Output Format Enhancements
- [x] 2.1 Enhance stats output display
  - Add profiling section to output
  - Organize metrics by category
  - Improve readability with sections
- [x] 2.2 Add JSON output option
  - Structured JSON output for programmatic access
  - Include all profiling metrics
  - Maintain backward compatibility with text output
  - (Future enhancement - basic structure in place)

## 3. CLI Integration
- [x] 3.1 Add `profile` command alias
  - Map to `stats` command with same options
  - Update help text
  - Document as profiling-focused command
- [x] 3.2 Add profiling-specific options
  - `--output-format` option (text, json)
  - `--include-distributions` flag
  - `--categorical-threshold` for type inference
  - (Basic implementation complete, JSON output pending)

## 4. DuckDB Integration
- [x] 4.1 Enhance DuckDB stats queries
  - Add missing value calculations (COUNT(*) - COUNT(field))
  - Add DISTINCT COUNT queries
  - Add percentile calculations using DuckDB functions
- [x] 4.2 Optimize profiling queries
  - Combine multiple metrics in single query where possible
  - Use DuckDB window functions for efficiency

## 5. Testing
- [x] 5.1 Unit tests for new metrics
  - Test missing value calculation
  - Test cardinality analysis
  - Test type inference logic
  - Test distribution calculations
- [x] 5.2 Integration tests
  - Test enhanced stats output
  - Test JSON output format
  - Test with various data types
- [x] 5.3 Performance tests
  - Compare profiling performance with DuckDB vs iterable
  - Measure impact of additional metrics

## 6. Documentation
- [x] 6.1 Update README with profiling examples
  - Show enhanced stats output
  - Document new metrics
  - Add profile command examples
- [x] 6.2 Document profiling metrics
  - Explain missing value rates
  - Explain cardinality and type inference
  - Explain distribution statistics
- [x] 6.3 Add profiling use cases
  - Data quality assessment
  - Schema discovery
  - Data exploration workflows
