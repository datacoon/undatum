## 1. Detection
- [x] 1.1 Implement multi-line sample + `csv.Sniffer` (with safe fallback)
- [x] 1.2 Preserve `--delimiter` / explicit options as override
- [x] 1.3 Integrate with `resolve_csv_delimiter` and convert/analyze paths

## 2. Tests
- [x] 2.1 Comma, semicolon, tab, pipe fixtures
- [x] 2.2 Quoted fields containing delimiter characters
- [x] 2.3 Fallback behavior when sniffing is inconclusive
