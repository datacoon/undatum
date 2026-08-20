---
title: "fmt"
description: "undatum fmt command reference"
---
# `fmt`

Reformats tabular data as CSV with an explicit dialect (delimiter, quote style, escape character, line endings).

```bash
# Change delimiter
undatum fmt data.csv --delimiter ";" output.csv

# Change quote style
undatum fmt data.csv --quote always output.csv

# Change escape character
undatum fmt data.csv --escape backslash output.csv

# Change line endings
undatum fmt data.csv --line-ending crlf output.csv
```

## Options

| Flag | Values | Default |
|------|--------|---------|
| `--delimiter` | Any single character | `,` |
| `--quote` | `minimal`, `always`, `none`, `nonnumeric` | `minimal` |
| `--escape` | `double`, `backslash`, `none` | `double` |
| `--line-ending` | `unix`, `windows`, `crlf`, `mac` | `unix` |
| `--quotechar` | CSV quote character | iterabledata default `"` |
| `--encoding` | Text encoding | auto-detect when omitted |
| `--output` | Output path | stdout |

Also accepts `--table`, `--flatten-nested`, `--on-error`, and `--error-log` ([shared options](/commands/shared-options)).
