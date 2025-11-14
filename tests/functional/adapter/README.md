# Functional Adapter Tests

This directory contains functional tests for the dbt-deltastream adapter.

## Table of Contents

- [How Integration Tests Work](#how-integration-tests-work)
- [Test Organization](#test-organization)
- [Running Tests](#running-tests)
- [Adding Integration Tests](#adding-integration-tests)
- [Resource Cleanup Best Practices](#resource-cleanup-best-practices)

## How Integration Tests Work

### Architecture

Integration tests for dbt-deltastream use a **session-scoped resource management** approach to efficiently test adapter functionality against a live DeltaStream instance:

1. **Session Setup** (`tests/conftest.py`):
   - Creates a unique database and schema at the start of the test session
   - Database name: `it_db_<timestamp>`
   - Schema name: `it_schema_<timestamp>`
   - Configured via the `integration_test_resources` fixture

2. **Test Execution**:
   - All tests share the same database and schema
   - Tests create resources with unique timestamp-based names to avoid conflicts
   - Tests are marked with `@pytest.mark.integration`
   - Resources created during tests are automatically cleaned up

3. **Session Cleanup** (`tests/conftest.py`):
   - At the end of the session, `clean_database_with_children()` removes all resources
   - Handles dependencies correctly (drops relations before schemas, schemas before databases)
   - Also runs a final cleanup hook (`pytest_sessionfinish`) to catch any orphaned databases

### Fixtures

The test suite provides class-scoped fixtures defined in `tests/functional/adapter/conftest.py`:

- `integration_database`: Provides the test database name
- `integration_schema`: Provides the test schema name  
- `integration_database_schema`: Provides both as a tuple for convenience

These fixtures use the session-level resources created in `tests/conftest.py`.

### Helper Functions

The `test_helpers.py` module provides centralized resource management:

- `clean_database_with_children()`: Comprehensive database cleanup
- `drop_relation_with_retry()`: Drop relations with retry logic (handles running queries)
- `terminate_all_queries()`: Terminate running queries before cleanup
- `list_resources()`: List resources of a specific type
- `generate_timestamp_suffix()`: Create unique names for test resources

## Test Organization

### Directory Structure

```
tests/functional/adapter/
├── README.md                        # This file
├── conftest.py                      # Class-scoped fixtures
├── test_helpers.py                  # Resource management utilities
├── test_basic.py                    # Basic dbt functionality tests (mostly skipped)
├── test_create_database.py          # Database creation tests
├── materialization/                 # Resource materialization tests
│   ├── test_create_entity_from_source.py
│   ├── test_create_stream_as_select.py
│   ├── test_create_descriptor_source_*.py
│   └── ...
├── stream_materialization/          # Stream-specific tests
│   ├── test_create_stream_as_select.py
│   └── test_create_stream_from_source.py
├── table_materialization/           # Table materialization tests
│   ├── test_create_table_as_select.py
│   └── test_create_table_with_window_aggregation.py
├── advanced_windowing/              # Window function tests
│   ├── test_tumble_window_aggregation.py
│   ├── test_hop_window_size_advance.py
│   └── ...
├── query_operations/                # Query management tests
│   ├── test_list_queries.py
│   ├── test_full_query_lifecycle.py
│   └── ...
├── multi_entity/                    # Multi-entity/join tests
├── stream_enrichment/               # Stream enrichment tests
└── schema_creation/                 # Schema management tests
```

### Test Categories

- **Unit Tests**: Tests that don't require a live DeltaStream connection (NOT marked with `@pytest.mark.integration`)
- **Integration Tests**: Tests that require a live DeltaStream connection (marked with `@pytest.mark.integration`)

## Running Tests

### Prerequisites

1. **Environment Setup**: Create a `.env` file in the project root with your DeltaStream credentials:

```bash
DELTASTREAM_API_TOKEN=your_api_token_here
DELTASTREAM_ORGANIZATION_ID=your_org_id_here
DELTASTREAM_URL=https://api.deltastream.io/v2  # Optional, defaults to this
```

You can copy `.env.example` and fill in your values.

2. **Install Dependencies**:

```bash
make install
```

### Running Tests

#### Run All Integration Tests

```bash
make integration-tests
```

This command:
- Automatically loads environment variables from `.env` if present
- Runs all tests marked with `@pytest.mark.integration`
- Creates/cleans up session-level database and schema

#### Run Specific Test File

```bash
make integration-test TEST_FILE=tests/functional/adapter/materialization/test_create_entity_from_source.py
```

#### Run Tests with pytest Directly

```bash
# Run all integration tests with verbose output
uv run pytest tests/functional/adapter -m integration -v

# Run specific test
uv run pytest tests/functional/adapter/materialization/test_create_entity_from_source.py -v

# Run with captured output (see print statements)
uv run pytest tests/functional/adapter -m integration -v -s
```

#### Run Only Unit Tests

```bash
make unit-tests
```

This excludes integration tests (runs tests NOT marked with `@pytest.mark.integration`).

### Test Output

Integration tests produce:
- Session setup logs showing database/schema creation
- Test execution output
- Cleanup logs showing resource deletion
- Any warnings or errors encountered

## Adding Integration Tests

### Basic Test Template

```python
"""
Test description here.

This test verifies that the adapter can [describe functionality].
"""

import pytest
from datetime import datetime
from dbt.tests.util import run_dbt, write_file


@pytest.mark.integration
def test_your_feature(project, integration_database, integration_schema):
    """Test your specific feature."""
    # Generate unique resource name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    resource_name = f"it_resource_{timestamp}"
    
    # Your test code here
    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {resource_name}
        description: "Test resource"
        config:
          materialized: stream
          parameters:
            store: trial_store
            topic: pageviews
            'value.format': JSON
        columns:
          - name: viewtime
            type: BIGINT
          - name: userid
            type: VARCHAR
""".lstrip()

    write_file(sources_yml, project.project_root, "models", "sources.yml")
    run_dbt(["run-operation", "create_sources"], expect_pass=True)
    
    # Assertions
    # No explicit cleanup needed - session cleanup handles it
```

### Best Practices for Writing Tests

#### 1. **Always Use Unique Resource Names**

```python
from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
resource_name = f"it_{resource_type}_{timestamp}"
```

This ensures:
- Tests can run in parallel without conflicts
- Resources are identifiable for debugging
- Cleanup can target test resources specifically

#### 2. **Use Fixtures for Database/Schema Context**

```python
@pytest.mark.integration
def test_example(project, integration_database, integration_schema):
    # Use integration_database and integration_schema in your test
    pass
```

Don't create your own database/schema - use the shared session-level resources.

#### 3. **Mark Integration Tests Correctly**

```python
@pytest.mark.integration
def test_feature():
    pass
```

This ensures the test:
- Only runs when `make integration-tests` is executed
- Skips during unit test runs
- Has proper setup/teardown via fixtures

#### 4. **Handle Expected Failures Gracefully**

If a test is expected to fail under certain conditions:

```python
@pytest.mark.skip(reason="SQL syntax not supported yet - tracking in issue #123")
@pytest.mark.integration
def test_unsupported_feature():
    pass
```

Or for temporary issues:

```python
@pytest.mark.integration
@pytest.mark.xfail(reason="Known issue with partition limits")
def test_feature():
    pass
```

#### 5. **Use Descriptive Test Names**

```python
def test_create_stream_from_kafka_topic():  # Good
def test_stream():  # Bad
```

Test names should clearly describe what is being tested.

## Resource Cleanup Best Practices

### Centralized Cleanup (Recommended)

**DO**: Rely on session-level cleanup in `conftest.py`

```python
@pytest.mark.integration
def test_create_stream(project, integration_database, integration_schema):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    stream_name = f"it_stream_{timestamp}"
    
    # Create stream
    # ... test code ...
    
    # NO explicit cleanup needed!
    # Session cleanup will handle it automatically
```

The session-level `integration_test_resources` fixture ensures all resources are cleaned up at the end.

### Manual Cleanup (Only When Necessary)

**DON'T**: Add manual cleanup unless absolutely required (e.g., testing deletion functionality)

```python
# Only do this if testing cleanup/deletion is part of the test objective
try:
    run_dbt(
        ["run-operation", "run_query", 
         "--args", f"{{sql: 'DROP RELATION {resource_name};'}}"],
        expect_pass=True,
    )
except Exception as e:
    # If manual cleanup fails, session cleanup will handle it
    logger.warning("Warning: Manual cleanup failed: %s", e)
```

### Using Helper Functions

When you need custom cleanup logic:

```python
from tests.functional.adapter.test_helpers import (
    drop_relation_with_retry,
    terminate_all_queries,
    clean_database_with_children,
)

# Drop a specific relation with retry logic
drop_relation_with_retry(
    relation_name=f"{integration_schema}.{stream_name}",
    relation_type="STREAM",
    database=integration_database,
    schema=integration_schema,
)

# Terminate queries before dropping resources
terminate_all_queries(database=integration_database, schema=integration_schema)

# Full database cleanup (used by session fixture)
results = clean_database_with_children(
    database=integration_database,
    drop_schemas=True,
    drop_database=True,
)
```

### Why Centralized Cleanup?

1. **Consistency**: All tests follow the same cleanup pattern
2. **Reliability**: Cleanup happens even if tests fail or are interrupted
3. **Efficiency**: Batch cleanup is faster than individual cleanups
4. **Simplicity**: Tests are easier to write and maintain
5. **Dependency Handling**: Proper cleanup order (relations → schemas → databases)
6. **Retry Logic**: Built-in handling for resources with running queries

## Troubleshooting

### Common Issues

#### Test Failures Due to Resource Limits

```
Error: unable to create topic as it would exceed max partitions
```

**Solution**: The test environment has partition limits. Mark test as skipped or use existing topics.

#### Authentication Errors

```
Error: DELTASTREAM_API_TOKEN and DELTASTREAM_ORGANIZATION_ID must be set
```

**Solution**: Create a `.env` file with valid credentials or set environment variables.

#### Cleanup Failures

```
Warning: Failed to drop resource - query still running
```

**Solution**: The cleanup system automatically retries. If persistent, check for long-running queries.

### Debug Mode

Run tests with verbose output and printed statements:

```bash
uv run pytest tests/functional/adapter -m integration -v -s
```

This shows:
- Session setup/teardown logs
- All print statements
- Detailed error traces

## See Also

- [../../documentation/syntax_support.md](../../documentation/syntax_support.md) - DeltaStream SQL syntax support
- [../../documentation/dbt_adapter_features.md](../../documentation/dbt_adapter_features.md) - Adapter feature support
- [../../CONTRIBUTING.md](../../CONTRIBUTING.md) - General contribution guidelines
- [test_helpers.py](test_helpers.py) - Resource management utilities source code
- [conftest.py](conftest.py) - Test fixtures and configuration

