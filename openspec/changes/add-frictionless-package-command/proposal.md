# Change: Add Frictionless Data Package generation

## Why
Users need a standard, interoperable metadata bundle for datasets that undatum
already profiles and validates. Frictionless Data Package is a widely adopted
format that fits undatum's strengths without forcing extra overhead.

## What Changes
- Add a `package` command with `create` subcommand to generate `datapackage.json`.
- Add an option to materialize a full package directory that includes
  `datapackage.json` and the referenced data files.
- Export schema details for each resource using existing inference logic.
- Allow optional metadata fields (name, title, description, license, sources,
  contributors) via CLI flags.
- Add `--autodoc` and metadata options that reuse the `doc` command logic to
  generate required package metadata with LLM assistance when configured.
- Support multiple input files and remote URIs as resources.

## Impact
- Affected specs: data-packaging (new capability).
- Affected code: new command in `undatum/cmds/`, schema mapping utilities,
  docs/CLI help, tests.
