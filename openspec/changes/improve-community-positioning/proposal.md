# Change: Improve Community and Positioning

## Why
Undatum has real usage (~105 downloads/day) but a razor-thin community surface: Discussions
disabled, zero external human PRs, 15 unattended Snyk bot PRs, and no presence on the
dbohdan/structured-text-tools list (highest-leverage discovery channel for CLI data engineers).
Downstream (cratedb-toolkit) is the strongest retention signal and needs nurturing. Agent
builders also need semver-strict CLI stability.

## What Changes
- Submit undatum to dbohdan/structured-text-tools **after** P0 items 1–3 so first impressions
  survive a 5-minute trial.
- Enable GitHub Discussions; ensure CONTRIBUTING.md and `good first issue` labels exist; respond
  on #34/#4 even with workarounds.
- Handle the Snyk PR queue (auto-merge patch bumps or close stale bots).
- Showcase/coordinate downstream (cratedb-toolkit); consider a downstream-compat CI check.
- Adopt stricter release discipline: semver-aware notes, loud deprecations, stable-command
  guarantee for agent builders.
- Explicitly defer/close niche items (Veriform #16 not planned; apply-patch scope or close #3).

## Impact
- Affected specs: `community`
- Affected surfaces: GitHub settings, CONTRIBUTING, release process, external list PR, issue
  triage — largely process/docs rather than runtime code
- Sequencing dependency: list submission waits on P0.1–P0.3
