# Change: Add CI Install-Gate Smoke Tests

## Why
Two of five external issues ever filed were missing-dependency bugs in shipped releases (#19
xmltodict, #37 chardet breaking CrateDB CI). A matrix job that installs the built wheel into a
clean venv and runs smoke commands would have caught both.

## What Changes
- Add a CI job that builds the distribution, installs `dist/*.whl` into a fresh venv, and runs
  smoke commands (`convert`, `stats`, `validate`) against each core format.
- Fail the pipeline if imports or smoke commands fail due to missing declared dependencies.
- Keep the job independent of the editable-install test path used for unit tests.

## Impact
- Affected specs: `release-quality`
- Affected code: `.github/workflows/ci.yml` (or new workflow), optional smoke fixture files
- Related issues: #19, #37
