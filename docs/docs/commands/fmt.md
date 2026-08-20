---
title: "fmt"
description: "undatum fmt command reference"
---
# `fmt`

Reformats CSV data with specific formatting options (delimiter, quote style, escape character, line endings).

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
