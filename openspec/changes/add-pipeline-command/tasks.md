## 1. Pipeline Specification Format
- [x] 1.1 Define YAML/JSON pipeline schema
  - Step structure (name, command, args)
  - Variable substitution syntax
  - Input/output path handling
  - Error handling options
- [x] 1.2 Create pipeline parser module
  - YAML parsing with validation
  - JSON parsing support
  - Variable resolution (env vars, CLI overrides)
  - Path resolution and validation

## 2. Pipeline Execution Engine
- [x] 2.1 Create `undatum/cmds/pipeline.py` module
  - Implement `PipelineRunner` class
  - Step execution logic
  - Command invocation wrapper
  - Error handling and rollback
- [x] 2.2 Implement step dependencies
  - Track step outputs
  - Resolve input/output paths
  - Handle temporary files
  - Cleanup on success/failure

## 3. CLI Integration
- [x] 3.1 Add `pipeline` command group to `undatum/core.py`
  - `pipeline run <spec>` subcommand
  - `pipeline validate <spec>` subcommand
  - `--var key=value` for variable overrides
  - `--dry-run` flag for validation without execution
- [x] 3.2 Update help text
  - Document pipeline spec format
  - Add examples
  - Document variable substitution

## 4. Command Integration
- [x] 4.1 Create command invocation wrapper
  - Map pipeline step commands to CLI commands
  - Convert pipeline args to CLI options
  - Handle input/output paths
  - Support all existing commands
- [x] 4.2 Add command validation
  - Validate command names
  - Validate required arguments
  - Check file path existence (where applicable)

## 5. Testing
- [ ] 5.1 Unit tests for pipeline parser
  - Test YAML parsing
  - Test JSON parsing
  - Test variable substitution
  - Test path resolution
- [ ] 5.2 Integration tests
  - Test pipeline execution
  - Test step dependencies
  - Test error handling
  - Test with various commands
- [ ] 5.3 Edge case tests
  - Test invalid pipeline specs
  - Test missing variables
  - Test circular dependencies
  - Test file path errors

## 6. Documentation
- [x] 6.1 Create pipeline specification guide
  - YAML format documentation
  - Step definition examples
  - Variable substitution guide
- [x] 6.2 Update README with pipeline examples
  - Basic pipeline example
  - Multi-step workflow example
  - Variable usage examples
- [x] 6.3 Add pipeline best practices
  - Naming conventions
  - Error handling patterns
  - Reusability tips
  - Example pipeline files created
