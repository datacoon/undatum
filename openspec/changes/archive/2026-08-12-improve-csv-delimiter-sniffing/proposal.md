# Change: Improve CSV Delimiter and Dialect Sniffing

## Why
Delimiter detection currently relies on first-line character counting, which silently
mis-parses quoted fields and atypical dialects. Replacing it with `csv.Sniffer` plus
quote-aware sampling over N lines is cheap and removes a whole class of silent errors.

## What Changes
- Replace (or augment) first-line delimiter counting in `detect_delimiter` with
  `csv.Sniffer`-based, quote-aware sampling over multiple lines.
- Detect dialect attributes needed for correct CSV reads (delimiter at minimum; quotechar when
  reliably sniffed).
- Keep explicit `--delimiter` override as highest precedence.
- Add tests for comma/semicolon/tab/pipe and quoted-delimiter edge cases.

## Impact
- Affected specs: `data-processing`
- Affected code: `undatum/utils.py` (`detect_delimiter`), `resolve_csv_delimiter` / analyzer and
  converter call sites
