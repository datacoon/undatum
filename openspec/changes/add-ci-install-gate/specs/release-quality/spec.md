## ADDED Requirements

### Requirement: Clean-Install Wheel Smoke Gate
CI SHALL verify that a freshly installed wheel from the built distribution can import and run
core CLI smoke commands without missing declared dependencies.

#### Scenario: Wheel install on clean venv
- **WHEN** CI builds a wheel and installs it into a clean virtualenv
- **THEN** `undatum --help` (or equivalent entry point) runs successfully

#### Scenario: Core command smoke on installed wheel
- **WHEN** CI runs smoke convert/stats/validate against fixture files for core formats after
  clean wheel install
- **THEN** commands exit successfully and do not raise `ModuleNotFoundError` for packages that
  are declared dependencies
