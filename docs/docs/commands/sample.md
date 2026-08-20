---
title: "sample"
description: "undatum sample command reference"
---
# `sample`

Randomly selects rows from a data file using reservoir sampling algorithm.

```bash
# Sample fixed number of rows
undatum sample data.csv --n 1000 output.csv

# Sample by percentage
undatum sample data.jsonl --percent 10 output.jsonl
```
