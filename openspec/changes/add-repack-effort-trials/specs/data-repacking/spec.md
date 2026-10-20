## ADDED Requirements

### Requirement: Bounded High Effort Trials
Repack SHALL offer opt-in bounded compatible compression trials after establishing a
verified baseline. Every candidate SHALL pass the ordinary preservation contract.
Trial failure SHALL NOT invalidate a successful verified baseline. Trial count,
deadline, decoded work, native RSS and total live scratch SHALL remain bounded by
the declared controls. Default repack behavior SHALL remain a single candidate.

#### Scenario: Stronger gzip trial improves size
- **WHEN** an explicitly requested compatible stronger gzip trial verifies and is smaller
- **THEN** it becomes the candidate for the existing savings and publication policy
- **AND** preserved gzip headers and decoded bytes still meet the core contract

#### Scenario: Optional tool fails
- **WHEN** an optional tool is missing, fails or exceeds its trial budget
- **THEN** the result reports that trial's reason and retains the verified baseline
- **AND** no failed candidate is published

#### Scenario: Trials provide no benefit
- **WHEN** all verified alternatives are equal to or larger than the baseline
- **THEN** the baseline remains the winner
- **AND** trial measurements include unsuccessful alternatives
