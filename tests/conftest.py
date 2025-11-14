import os
from collections.abc import Mapping
from datetime import datetime
import logging

import pytest
import asyncio
from deltastream.api.conn import APIConnection

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local
from tests.functional.adapter.test_helpers import clean_database_with_children

pytest_plugins = ["dbt.tests.fixtures.project"]

# Set up logger
logger = logging.getLogger(__name__)


_ENV_NAMES: Mapping[str, str] = {
    "token": "DELTASTREAM_API_TOKEN",
    "organization_id": "DELTASTREAM_ORGANIZATION_ID",
    "database": "DELTASTREAM_DATABASE",
    "schema": "DELTASTREAM_SCHEMA",
    "store": "DELTASTREAM_STORE",  # Existing store to use (optional)
    "create_store": "DELTASTREAM_CREATE_STORE",  # Whether to create a store (true/false)
    "create_store_sql": "DELTASTREAM_CREATE_STORE_SQL",  # SQL template for store creation
    "entity_name_prefix": "DELTASTREAM_IT_PREFIX",  # Prefix for entity names
}


async def _list_entities_in_store(conn: APIConnection, store_name: str) -> list[str]:
    """List all entities in a given store."""
    entities = []
    try:
        result = await conn.query(f'LIST ENTITIES IN STORE "{store_name}";')
        async for row in result:
            if isinstance(row, dict):
                entity_name = row.get("Name", "") or row.get("name", "")
            elif isinstance(row, list) and len(row) > 0:
                entity_name = str(row[0]) if row else ""
            else:
                continue

            if entity_name:
                entities.append(entity_name)
    except Exception as e:
        logger.warning(
            "[SESSION SETUP] Could not list entities in store %s: %s", store_name, e
        )

    return entities


async def _drop_entity_if_exists(
    conn: APIConnection, entity_name: str, store_name: str
) -> bool:
    """Drop an entity if it exists. Returns True if dropped successfully."""
    try:
        logger.info("[SESSION SETUP] Dropping existing entity: %s", entity_name)
        drop_result = await conn.query(
            f'DROP ENTITY "{entity_name}" IN STORE "{store_name}";'
        )
        async for _ in drop_result:
            pass
        logger.info("[SESSION SETUP] Successfully dropped entity: %s", entity_name)
        return True
    except Exception as e:
        logger.warning("[SESSION SETUP] Failed to drop entity %s: %s", entity_name, e)
        return False


async def _create_entity(
    conn: APIConnection, entity_name: str, store_name: str
) -> None:
    """Create a new entity in the store."""
    entity_sql = f"""
CREATE ENTITY "{entity_name}" IN STORE "{store_name}" WITH (
  'kafka.partitions' = 1,
  'kafka.replicas' = 1,
  'kafka.topic.retention.ms' = '172800000');
"""
    logger.info("[SESSION SETUP] Creating entity: %s", entity_name)
    logger.debug("[SESSION SETUP] Creating entity SQL: %s", entity_sql)

    result = await conn.query(entity_sql)
    async for _ in result:
        pass
    logger.info("[SESSION SETUP] Successfully created entity: %s", entity_name)


async def _wait_for_store_ready(
    conn: APIConnection, store_name: str, max_wait_time: int = 120
) -> None:
    """Wait for a store to be ready by attempting to DESCRIBE it."""
    wait_interval = 5
    elapsed = 0

    while elapsed < max_wait_time:
        try:
            result = await conn.query(f'DESCRIBE STORE "{store_name}";')
            async for _ in result:
                pass
            logger.info("[SESSION SETUP] Store %s is ready", store_name)
            return
        except Exception as e:
            if "not ready" in str(e).lower() or "3D006" in str(e):
                logger.info(
                    "[SESSION SETUP] Store %s not ready yet, waiting... (%ds/%ds)",
                    store_name,
                    elapsed,
                    max_wait_time,
                )
                await asyncio.sleep(wait_interval)
                elapsed += wait_interval
            else:
                # Different error, re-raise
                raise e

    raise Exception(
        f"Store {store_name} did not become ready within {max_wait_time} seconds"
    )


async def _setup_entities_for_integration_tests(
    conn: APIConnection, store_name: str, entity_prefix: str = ""
) -> None:
    """Set up entities for integration tests by cleaning up existing ones and creating fresh ones."""
    # First wait for the store to be ready for basic operations
    await _wait_for_store_ready(conn, store_name)

    base_entities = ["pageviews", "users", "shipments"]
    entities = [
        f"{entity_prefix}{entity}" if entity_prefix else entity
        for entity in base_entities
    ]

    # Try to list and drop existing entities, but don't fail if this doesn't work
    logger.info(
        "[SESSION SETUP] Checking for existing entities in store: %s", store_name
    )
    try:
        existing_entities = await _list_entities_in_store(conn, store_name)
        entities_to_drop = [
            entity for entity in entities if entity in existing_entities
        ]

        logger.info(
            "[SESSION SETUP] Found existing entities to drop: %s", entities_to_drop
        )

        # Drop existing entities
        for entity_name in entities_to_drop:
            await _drop_entity_if_exists(conn, entity_name, store_name)
    except Exception as e:
        logger.warning(
            "[SESSION SETUP] Could not check for existing entities, proceeding with creation: %s",
            e,
        )

    # Now create the entities with retry logic for "not ready" errors
    for entity_name in entities:
        await _create_entity_with_retry(conn, entity_name, store_name)


async def _create_entity_with_retry(
    conn: APIConnection,
    entity_name: str,
    store_name: str,
    max_retries: int = 24,
    retry_interval: int = 5,
) -> None:
    """Create an entity with retry logic for 'store not ready' errors."""
    entity_sql = f"""
CREATE ENTITY "{entity_name}" IN STORE "{store_name}" WITH (
  'kafka.partitions' = 1,
  'kafka.replicas' = 1,
  'kafka.topic.retention.ms' = '172800000');
"""

    for attempt in range(max_retries):
        try:
            logger.info(
                "[SESSION SETUP] Creating entity: %s (attempt %d/%d)",
                entity_name,
                attempt + 1,
                max_retries,
            )
            if attempt == 0:
                logger.debug("[SESSION SETUP] Creating entity SQL: %s", entity_sql)

            result = await conn.query(entity_sql)
            async for _ in result:
                pass
            logger.info("[SESSION SETUP] Successfully created entity: %s", entity_name)
            return

        except Exception as e:
            error_str = str(e).lower()
            if (
                "3d006" in error_str or "not ready" in error_str
            ) and attempt < max_retries - 1:
                logger.warning(
                    "[SESSION SETUP] Store not ready for entity %s, retrying in %ds... (attempt %d/%d)",
                    entity_name,
                    retry_interval,
                    attempt + 1,
                    max_retries,
                )
                await asyncio.sleep(retry_interval)
                continue
            elif "42710" in error_str or "already exists" in error_str:
                # Entity was created successfully on a previous attempt
                logger.info(
                    "[SESSION SETUP] Entity %s already exists (created successfully on previous attempt)",
                    entity_name,
                )
                return
            else:
                # Different error or max retries reached
                raise e

    raise Exception(
        f"Failed to create entity {entity_name} after {max_retries} attempts"
    )


def _required_env() -> dict[str, str]:
    return {key: os.getenv(env_name, "") for key, env_name in _ENV_NAMES.items()}


def _get_deltastream_connection() -> APIConnection:
    """
    Create a DeltaStream API connection using environment variables.

    Returns:
        APIConnection instance
    """
    token = os.environ.get("DELTASTREAM_API_TOKEN", "")
    organization_id = os.environ.get("DELTASTREAM_ORGANIZATION_ID", "")
    url = os.environ.get("DELTASTREAM_URL", "https://api.deltastream.io/v2")

    if not token or not organization_id:
        raise ValueError(
            "DELTASTREAM_API_TOKEN and DELTASTREAM_ORGANIZATION_ID must be set"
        )

    async def token_provider() -> str:
        return token

    return APIConnection(
        server_url=url,
        token_provider=token_provider,
        session_id=None,
        timezone="UTC",
        organization_id=organization_id,
        role_name=None,
        database_name="deltastream",
        schema_name="public",
        store_name=None,
    )


@pytest.fixture(scope="session")
def dbt_profile_target() -> str:
    return "dev"


@pytest.fixture(scope="session")
def integration_test_resources():
    """
    Create integration test database, schema, and optionally store at session start.

    This ensures the resources exist before any dbt operations.
    """
    # Check if we have the required credentials
    required = _required_env()
    missing_envs = [
        env_name
        for key, env_name in _ENV_NAMES.items()
        if key in ["token", "organization_id"] and not required[key]
    ]

    if missing_envs:
        pytest.skip(
            "Integration tests require configured DeltaStream credentials. Missing: "
            f"{', '.join(missing_envs)}. Configure these as environment variables."
        )

    # Generate unique database and schema names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    db_name = f"it_db_{timestamp}"
    schema_name = f"it_schema_{timestamp}"

    # Determine store configuration
    create_store = required.get("create_store", "").lower() in ("true", "1", "yes")
    existing_store = required.get("store", "").strip()
    store_name = None

    if create_store:
        # Generate unique store name for integration tests
        store_name = f"it_store_{timestamp}"
        logger.info(
            "[SESSION SETUP] Will create integration test store: %s", store_name
        )
    elif existing_store:
        # Use existing store
        store_name = existing_store
        logger.info("[SESSION SETUP] Will use existing store: %s", store_name)
    else:
        logger.info(
            "[SESSION SETUP] No store configured - tests will use default store behavior"
        )

    logger.info("[SESSION SETUP] Creating integration test database: %s", db_name)

    # Create database
    async def create_resources():
        conn = _get_deltastream_connection()
        try:
            # Create database
            result = await conn.query(f"CREATE DATABASE {db_name};")
            async for _ in result:
                pass
            logger.info("[SESSION SETUP] Successfully created database: %s", db_name)

            # Create schema
            result = await conn.query(
                f"CREATE SCHEMA {schema_name} IN DATABASE {db_name};"
            )
            async for _ in result:
                pass
            logger.info("[SESSION SETUP] Successfully created schema: %s", schema_name)

            # Create store if requested
            if create_store and store_name:
                store_sql_template = required.get("create_store_sql", "").strip()

                if not store_sql_template:
                    raise ValueError(
                        "DELTASTREAM_CREATE_STORE_SQL must be set when creating a store"
                    )

                # Format the SQL template with the store name
                create_store_sql = store_sql_template.format(store_name=store_name)

                logger.info(
                    "[SESSION SETUP] Creating store with SQL: %s", create_store_sql
                )
                result = await conn.query(create_store_sql)
                async for _ in result:
                    pass
                logger.info(
                    "[SESSION SETUP] Successfully created store: %s", store_name
                )

                # Create entities for integration tests
                entity_prefix = required.get("entity_name_prefix", "").strip()
                await _setup_entities_for_integration_tests(
                    conn, store_name, entity_prefix
                )

            return db_name, schema_name, store_name
        except Exception as e:
            logger.error("[SESSION SETUP] Failed to create resources: %s", e)
            raise e

    try:
        db_name, schema_name, store_name = asyncio.run(create_resources())
    except Exception as e:
        pytest.fail(f"Failed to create integration test resources: {e}")

    # Yield the names for use by other fixtures
    yield db_name, schema_name, store_name

    # Cleanup at end of session
    logger.info("[SESSION CLEANUP] Cleaning up integration test resources...")
    logger.info("[SESSION CLEANUP] Starting cleanup for database: %s", db_name)
    results = clean_database_with_children(
        database=db_name, drop_schemas=True, drop_database=True
    )
    logger.info(
        "[SESSION CLEANUP] Cleanup completed. Dropped %d resources.",
        len(results["dropped"]),
    )
    if results["failed"]:
        logger.warning(
            "[SESSION CLEANUP] Warning: %d resources failed to drop: %s",
            len(results["failed"]),
            results["failed"],
        )
    if results["errors"]:
        logger.error("[SESSION CLEANUP] Errors: %s", results["errors"])
        raise Exception(f"Cleanup failed with errors: {results['errors']}")


@pytest.fixture(scope="session")
def dbt_profile_data(dbt_profile_target, integration_test_resources):
    # Get required environment variables
    required = _required_env()

    missing_envs = [
        env_name for key, env_name in _ENV_NAMES.items() if not required[key]
    ]

    # For integration tests, use the dynamically created resources
    db_name, schema_name, store_name = integration_test_resources

    if missing_envs:
        # Only skip if we don't have token/org_id (database/schema are handled above)
        missing_required = [
            env
            for env in missing_envs
            if env in ["DELTASTREAM_API_TOKEN", "DELTASTREAM_ORGANIZATION_ID"]
        ]
        if missing_required:
            formatted = ", ".join(missing_required)
            pytest.skip(
                "Integration tests require configured DeltaStream credentials. Missing: "
                f"{formatted}. Configure these as environment variables."
            )

    url = os.environ.get("DELTASTREAM_URL", "https://api.deltastream.io/v2")

    target_data = {
        "type": "deltastream",
        "threads": 1,
        "url": url,
        "token": required["token"],
        "organization_id": required["organization_id"],
        "database": db_name,
        "schema": schema_name,
    }

    # Add store to profile if one was created or specified
    if store_name:
        target_data["store"] = store_name

    output = {
        "test": {
            "target": dbt_profile_target,
            "outputs": {dbt_profile_target: target_data},
        }
    }
    logger.debug("DEBUG: dbt_profile_data output: %s", output)
    return output


def pytest_sessionfinish(session, exitstatus):
    """
    Hook that runs at the end of the test session to clean up any remaining integration test databases and stores.
    This ensures cleanup happens even if tests fail or are interrupted.
    """
    # Check if we have the required credentials
    required = _required_env()
    if not required.get("token") or not required.get("organization_id"):
        logger.info(
            "[SESSION CLEANUP] Skipping cleanup - missing DeltaStream credentials"
        )
        return

    logger.info(
        "[SESSION CLEANUP] Running final cleanup of all integration test databases and stores..."
    )

    async def cleanup_all():
        conn = _get_deltastream_connection()
        try:
            # List all databases and clean them up
            result = await conn.query("LIST DATABASES;")
            it_databases = []
            async for row in result:
                db_name = ""
                if isinstance(row, dict):
                    db_name = row.get("Name", "") or row.get("name", "")
                elif isinstance(row, list) and len(row) > 0:
                    db_name = str(row[0]) if row else ""

                if db_name and db_name.startswith("it_db"):
                    it_databases.append(db_name)

            logger.info(
                "[SESSION CLEANUP] Found %d integration test databases to clean",
                len(it_databases),
            )

            for db_name in it_databases:
                try:
                    logger.info("[SESSION CLEANUP] Cleaning database: %s", db_name)
                    results = clean_database_with_children(
                        database=db_name, drop_schemas=True, drop_database=True
                    )
                    logger.info(
                        "[SESSION CLEANUP] Dropped %d resources from %s",
                        len(results["dropped"]),
                        db_name,
                    )
                    if results["failed"] or results["errors"]:
                        logger.warning(
                            "[SESSION CLEANUP] Issues with %s: failed=%s, errors=%s",
                            db_name,
                            results["failed"],
                            results["errors"],
                        )
                except Exception as e:
                    logger.error("[SESSION CLEANUP] Error cleaning %s: %s", db_name, e)

            # List and clean up integration test stores
            result = await conn.query("LIST STORES;")
            it_stores = []
            async for row in result:
                store_name = ""
                if isinstance(row, dict):
                    store_name = row.get("Name", "") or row.get("name", "")
                elif isinstance(row, list) and len(row) > 0:
                    store_name = str(row[0]) if row else ""

                if store_name and store_name.startswith("it_store"):
                    it_stores.append(store_name)

            logger.info(
                "[SESSION CLEANUP] Found %d integration test stores to clean",
                len(it_stores),
            )

            for store_name in it_stores:
                try:
                    logger.info("[SESSION CLEANUP] Dropping store: %s", store_name)
                    drop_result = await conn.query(f"DROP STORE {store_name};")
                    async for _ in drop_result:
                        pass
                    logger.info(
                        "[SESSION CLEANUP] Successfully dropped store: %s", store_name
                    )
                except Exception as e:
                    logger.error(
                        "[SESSION CLEANUP] Error dropping store %s: %s", store_name, e
                    )

        except Exception as e:
            logger.error("[SESSION CLEANUP] Error during cleanup: %s", e)

    try:
        asyncio.run(cleanup_all())
    except Exception as e:
        logger.error("[SESSION CLEANUP] Final cleanup failed: %s", e)
