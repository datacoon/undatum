## Context
Install friction blocks ops adoption. Homebrew #4 is the longest-open external issue.

## Goals / Non-Goals
- Goals: official macOS install story; binary artifacts; README first-class tool installs.
- Non-Goals: reinventing package managers; supporting ancient OS targets on day one.

## Decisions
- Decision: Prefer documenting `pipx`/`uv` immediately; add binary artifacts in release CI;
  resolve #4 either with a real formula or an honest official pipx path.
- Alternatives considered: conda-only (narrower ops fit); delay binaries until after P0 (README
  uv/pipx can ship earlier).

## Risks / Trade-offs
- PyInstaller binaries increase release maintenance → automate in CI; smoke-test entrypoint.
- Homebrew formula maintenance burden → consider pipx-as-official if formula unsustainable.

## Migration Plan
- Docs-first, then binaries, then formula or close #4 with documented path.

## Open Questions
- PyInstaller vs shiv/pex for preferred artifact?
- Who maintains the Homebrew tap/formula long-term?
