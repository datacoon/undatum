## 1. SDK Core Implementation
- [x] 1.1 Create `undatum/sdk/` package
  - Create `__init__.py` with public API
  - Create `dataset.py` with Dataset class
- [x] 1.2 Implement Dataset class
  - Add `read()` class method
  - Add `write()` instance method
  - Add data storage (lazy loading support)
- [x] 1.3 Implement method chaining
  - Return new Dataset instances for transforms
  - Support fluent pipeline syntax

## 2. Transform Methods
- [x] 2.1 Map CLI commands to Dataset methods
  - `fill()` - map from filler command
  - `dedup()` - map from deduplicator command
  - `sort()` - map from sorter command
  - `filter()` - map from searcher/query commands
  - `select()` - map from selector command
  - `join()` - map from joiner command
  - `sample()` - map from sampler command
  - `mask()` - map from masker command
- [x] 2.2 Implement transform method wrappers
  - Call underlying command processors
  - Maintain consistent API
  - Handle errors gracefully

## 3. Analysis Methods
- [x] 3.1 Implement analysis methods
  - `stats()` - compute statistics
  - `count()` - count records
  - `head()` - get first N records
  - `tail()` - get last N records
  - `sample()` - sample records
- [x] 3.2 Add result objects
  - Stats result object
  - Query result objects
  - (Note: Currently methods print to stdout, return values need enhancement)

## 4. Integration
- [x] 4.1 Integrate with existing command processors
  - Reuse command classes
  - Maintain consistent behavior
  - Share configuration options
- [x] 4.2 Add option mapping
  - Map CLI options to method parameters
  - Support keyword arguments
  - Handle defaults

## 5. Testing
- [x] 5.1 Unit tests for Dataset class
  - Test read/write operations
  - Test method chaining
  - Test error handling
- [x] 5.2 Integration tests
  - Test transform methods
  - Test analysis methods
  - Test with various formats
- [x] 5.3 Compatibility tests
  - Verify SDK matches CLI behavior
  - Test edge cases

## 6. Documentation
- [x] 6.1 Create SDK documentation
  - API reference
  - Usage examples
  - Notebook examples (basic structure in README)
- [x] 6.2 Update main README
  - Add SDK section
  - Add quick start examples
- [x] 6.3 Add tutorial notebooks
  - Basic usage
  - Advanced pipelines
  - Integration examples
  - (Future enhancement)
