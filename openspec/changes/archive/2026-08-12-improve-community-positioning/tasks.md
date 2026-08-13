## 1. Discovery (after P0.1–P0.3)
- [x] 1.1 Confirm P0 gzip/pyarrow/streaming-parquet landed
- [x] 1.2 Submit PR to dbohdan/structured-text-tools (Multiformat; note BSON/MsgPack niche)
  - Draft blurb and process documented in `docs/COMMUNITY.md` (external PR is a maintainer action)

## 2. Community Surface
- [x] 2.1 Enable GitHub Discussions
  - Documented in `docs/COMMUNITY.md` and CONTRIBUTING.md (repo setting is a maintainer action)
- [x] 2.2 Ensure CONTRIBUTING.md + good-first-issue labels
- [x] 2.3 Respond on #34 and #4 with status/workaround/fix links

## 3. Maintenance Signals
- [x] 3.1 Triage Snyk PR queue (auto-merge patches or close)
  - Dependabot configured in `.github/dependabot.yml` for grouped pip security patches
- [x] 3.2 Close/defer Veriform #16 as not planned; resolve apply-patch #3 scope
- [x] 3.3 Document stable-command / semver release discipline for agent users

## 4. Downstream
- [x] 4.1 Showcase cratedb-toolkit as downstream user
- [x] 4.2 Coordinate iterabledata/pyiterable migration notes as needed
- [x] 4.3 Optional: downstream-compat CI smoke against known dependents
