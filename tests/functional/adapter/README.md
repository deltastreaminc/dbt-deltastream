# Functional Tests for dbt-deltastream

This directory contains functional tests that verify the dbt-deltastream adapter works correctly with dbt Core functionality.

## Test Structure

The tests are organized following dbt adapter testing best practices:

### Current Tests

- **test_basic.py** - Core dbt functionality tests using dbt-tests-adapter framework
- **test_create_database.py** - Custom DeltaStream-specific database creation tests
- **test_caching.py** - Relation caching functionality tests (placeholder)
- **test_catalog.py** - Catalog generation functionality tests (placeholder)
- **test_relations.py** - Database relation handling tests (placeholder)
- **test_freshness.py** - Source freshness functionality tests (placeholder)
- **test_store_test_failures.py** - Store test failures functionality tests (placeholder)
- **test_constraints.py** - Constraint handling tests (skipped - needs verification of DeltaStream constraint support)

### Test Categories

1. **Basic Tests** - Essential functionality every dbt adapter should support
2. **Optional Tests** - Secondary functionality that may not be supported by all adapters
3. **Custom Tests** - DeltaStream-specific functionality tests

### Running Tests

```bash
# Run all functional tests
make integration-tests

# Run specific test file
uv run pytest tests/functional/adapter/test_basic.py -v

# Run only integration marked tests
uv run pytest tests/functional -m integration

# Skip integration tests (run unit tests only)
uv run pytest tests/functional -m "not integration"
```

### Environment Setup

Functional tests require environment variables to connect to a live DeltaStream environment:

```bash
export RUN_INTEGRATION_TESTS=1
export DELTASTREAM_API_TOKEN=your_token_here
export DELTASTREAM_ORGANIZATION_ID=your_org_id_here
export DELTASTREAM_DATABASE=your_database_name_here
export DELTASTREAM_SCHEMA=your_schema_name_here
export DELTASTREAM_URL=https://api.deltastream.io/v2  # optional
export DELTASTREAM_ROLE=your_role_here  # optional
```

### Adding New Tests

When adding new tests:

1. Follow the existing naming convention: `test_<functionality>.py`
2. Use the dbt-tests-adapter framework when possible
3. Add appropriate pytest markers (`@pytest.mark.integration`, `@pytest.mark.skip`, etc.)
4. Include proper docstrings explaining what functionality is being tested
5. For DeltaStream-specific functionality, create custom test classes

### Notes

- Some test files are currently placeholders waiting for correct import structure determination
- Constraint tests are skipped pending verification of DeltaStream constraint support
- Tests use the same pytest configuration as unit tests but require live environment
