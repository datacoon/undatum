# Change: Improve doc markdown metadata formatting

## Why
The `doc` command currently surfaces metadata values in markdown in a way that is
hard to read for lists and structured attributes. This change makes metadata
attributes consistently human-readable in markdown output.

## What Changes
- Define how metadata attributes are rendered in markdown output.
- Normalize list and object values into readable markdown structures.
- Represent empty or unknown metadata values consistently.

## Impact
- Affected specs: `dataset-documentation`
- Affected code: markdown output formatter in `undatum/cmds/doc.py` (and helpers)
