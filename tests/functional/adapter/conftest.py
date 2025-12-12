"""
Configuration and fixtures for integration tests.

This module provides class-scoped fixtures for database and schema management
that are shared across all integration tests to improve efficiency and reduce
redundant setup/teardown operations.
"""

import logging
import pytest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def integration_database(integration_test_resources):
    """
    Provide the integration test database name.

    This uses the database created at session start.
    """
    db_name, _, _, _ = integration_test_resources
    logger.debug("Returning integration_database: %s", db_name)
    yield db_name


@pytest.fixture(scope="class")
def integration_schema(integration_test_resources):
    """
    Provide the integration test schema name.

    This uses the schema created at session start.
    """
    _, schema_name, _, _ = integration_test_resources
    yield schema_name


@pytest.fixture(scope="class")
def integration_store(integration_test_resources):
    """
    Provide the integration test store name.

    This uses the store configured at session start.
    """
    _, _, store_name, _ = integration_test_resources
    yield store_name


@pytest.fixture(scope="class")
def integration_database_schema(integration_database, integration_schema):
    """
    Provide both database and schema names as a tuple.

    This is a convenience fixture that combines both the database and schema
    fixtures for tests that need both values.

    Args:
        integration_database: The database fixture.
        integration_schema: The schema fixture.

    Yields:
        tuple[str, str]: A tuple of (database_name, schema_name).
    """
    yield (integration_database, integration_schema)


@pytest.fixture(scope="class")
def integration_prefix(integration_test_resources, integration_run_id):
    """
    Provide the integration test prefix from environment variables.

    This prefix is used to namespace integration test resources and models.

    Yields:
        str: The prefix string from DELTASTREAM_IT_PREFIX environment variable
             with the session run id appended to ensure xdist isolation.
    """
    import os

    prefix = os.getenv("DELTASTREAM_IT_PREFIX", "").strip()
    if prefix:
        yield f"{prefix}{integration_run_id}_"
    else:
        yield f"{integration_run_id}_"


@pytest.fixture(scope="class")
def integration_entity_names(integration_test_resources):
    """
    Provide the integration test entity names with timestamps.

    This returns the entity names that were created at session start,
    including timestamps to ensure uniqueness across test runs.

    Yields:
        dict[str, str]: A dictionary mapping base entity names to their full
                        timestamped names (e.g., {"pageviews": "dbte2e_pageviews_20251209_230244_033"}).
    """
    _, _, _, entity_names = integration_test_resources
    yield entity_names
