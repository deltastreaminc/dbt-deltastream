"""
Helper functions for integration tests.
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from functools import wraps
from typing import Any, Dict, List, Literal, Optional

from deltastream.api.conn import APIConnection
from deltastream.api.error import SQLError

# Configure logging
logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_MAX_WAIT_SECONDS = int(os.environ.get("DELTASTREAM_MAX_WAIT_SECONDS", "300"))
DEFAULT_RETRY_INTERVAL_SECONDS = int(os.environ.get("DELTASTREAM_RETRY_INTERVAL", "5"))
DEFAULT_API_URL = os.environ.get("DELTASTREAM_URL", "https://api.deltastream.io/v2")

# System schemas that should not be dropped
PROTECTED_SCHEMAS = {"information_schema", "pg_catalog"}

# Resource type definitions
ResourceType = Literal[
    "CHANGELOG",
    "COMPUTE_POOL",
    "DESCRIPTOR_SOURCE",
    "ENTITY",
    "FUNCTION_SOURCE",
    "FUNCTION",
    "RELATION",
    "STORE",
    "STREAM",
]

# Resource type mapping for list and drop commands
RESOURCE_TYPES = {
    "CHANGELOG": {"list": "LIST RELATIONS", "filter_col": "Name"},
    "COMPUTE_POOL": {"list": "LIST COMPUTE_POOLS", "filter_col": "Name"},
    "DESCRIPTOR_SOURCE": {"list": "LIST DESCRIPTOR_SOURCES", "filter_col": "Name"},
    "ENTITY": {"list": "LIST ENTITIES", "filter_col": "Name"},
    "FUNCTION_SOURCE": {"list": "LIST FUNCTION_SOURCES", "filter_col": "Name"},
    "FUNCTION": {"list": "LIST FUNCTIONS", "filter_col": "Name"},
    "RELATION": {"list": "LIST RELATIONS", "filter_col": "Name"},
    "STORE": {"list": "LIST STORES", "filter_col": "Name"},
    "STREAM": {"list": "LIST RELATIONS", "filter_col": "Name"},
}

# Dependency order for dropping resources (most dependent first)
# Note: Do not drop ENTITYs as they may cause authorization issues with topics
DROP_ORDER = [
    "RELATION",  # Materialized views, tables, views
    "STREAM",  # Streaming resources
    "CHANGELOG",  # Change data capture streams
    "FUNCTION",
    "COMPUTE_POOL",
    "FUNCTION_SOURCE",
    "DESCRIPTOR_SOURCE",
    "SCHEMA_REGISTRY",
    # Note: Not dropping ENTITY or STORE as they may cause issues
]


# Custom exceptions
class DeltaStreamCleanupError(Exception):
    """Base exception for cleanup operations."""

    pass


class ResourceNotFoundError(DeltaStreamCleanupError):
    """Raised when a resource doesn't exist."""

    pass


class OperationTimeoutError(DeltaStreamCleanupError):
    """Raised when an operation times out."""

    pass


def retry_on_failure(
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
    retryable_exceptions: Optional[List[type]] = None,
):
    """
    Decorator for retrying async operations on specific failures.
    Args:
        max_wait_seconds: Maximum time to wait before giving up
        retry_interval_seconds: Time to wait between retries
        retryable_exceptions: List of exception types that should trigger retries.
                            If None, defaults to [SQLError]
    """
    if retryable_exceptions is None:
        retryable_exceptions = [SQLError]

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            last_error = ""

            while (time.time() - start_time) < max_wait_seconds:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    # Check if this is an exception we should retry
                    should_retry = any(
                        isinstance(e, exc_type) for exc_type in retryable_exceptions
                    )

                    if should_retry:
                        error_msg = str(e).lower()
                        last_error = error_msg

                        # Check for retryable errors
                        retryable_keywords = [
                            "running queries",
                            "referenced relations",
                            "2bp01",
                            "retry",
                            "running",
                            "referenced",
                        ]

                        if any(keyword in error_msg for keyword in retryable_keywords):
                            elapsed = time.time() - start_time
                            remaining = max_wait_seconds - elapsed
                            logger.warning(
                                "Operation failed, retrying in %ds (%.0fs remaining): %s",
                                retry_interval_seconds,
                                remaining,
                                error_msg,
                            )
                            await asyncio.sleep(retry_interval_seconds)
                        else:
                            # Non-retryable error - re-raise immediately
                            raise
                    else:
                        # Not a retryable exception type - re-raise immediately
                        raise

            # Timed out
            raise OperationTimeoutError(
                f"Operation {func.__name__} timed out after {max_wait_seconds}s. "
                f"Last error: {last_error}"
            )

        return wrapper

    return decorator


@asynccontextmanager
async def get_deltastream_connection(
    database: Optional[str] = None, schema: Optional[str] = None
):
    """
    Context manager for DeltaStream API connections.
    Args:
        database: Optional database name (overrides env var)
        schema: Optional schema name (overrides env var)
    Yields:
        APIConnection instance
    """
    conn = _create_deltastream_connection(database, schema)
    try:
        yield conn
    finally:
        # Note: APIConnection doesn't have a close method
        pass


def _create_deltastream_connection(
    database: Optional[str] = None, schema: Optional[str] = None
) -> APIConnection:
    """
    Create a DeltaStream API connection using environment variables.
    Args:
        database: Optional database name (overrides env var)
        schema: Optional schema name (overrides env var)
    Returns:
        APIConnection instance
    """
    token = os.environ.get("DELTASTREAM_API_TOKEN", "")
    organization_id = os.environ.get("DELTASTREAM_ORGANIZATION_ID", "")
    url = DEFAULT_API_URL

    async def token_provider() -> str:
        return token

    return APIConnection(
        server_url=url,
        token_provider=token_provider,
        session_id=None,
        timezone="UTC",
        organization_id=organization_id,
        role_name=None,
        database_name=database,
        schema_name=schema,
        store_name=None,
    )


def validate_resource_name(name: str) -> str:
    """
    Validate and return a safe resource name.
    Args:
        name: Resource name to validate
    Returns:
        Validated resource name
    Raises:
        ValueError: If the name contains invalid characters
    """
    if not name or not isinstance(name, str):
        raise ValueError("Resource name must be a non-empty string")

    # Check for potentially dangerous characters
    dangerous_chars = [";", '"', "'", "\\", "\n", "\r", "\t"]
    if any(char in name for char in dangerous_chars):
        raise ValueError(f"Invalid characters in resource name: {name!r}")

    return name.strip()


def generate_timestamp_suffix() -> str:
    """
    Generate a timestamp suffix for creating unique resource names in tests.
    Returns:
        A string in format YYYYMMDD_HHMMSS_MMM (milliseconds precision)
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]


def _get_deltastream_connection(
    database: Optional[str] = None, schema: Optional[str] = None
) -> APIConnection:
    """
    Create a DeltaStream API connection using environment variables.
    Args:
        database: Optional database name (overrides env var)
        schema: Optional schema name (overrides env var)
    Returns:
        APIConnection instance
    """
    return _create_deltastream_connection(database, schema)


# Resource type mapping for list and drop commands
RESOURCE_TYPES = {
    "CHANGELOG": {"list": "LIST RELATIONS", "filter_col": "Name"},
    "COMPUTE_POOL": {"list": "LIST COMPUTE_POOLS", "filter_col": "Name"},
    "DESCRIPTOR_SOURCE": {"list": "LIST DESCRIPTOR_SOURCES", "filter_col": "Name"},
    "ENTITY": {"list": "LIST ENTITIES", "filter_col": "Name"},
    "FUNCTION_SOURCE": {"list": "LIST FUNCTION_SOURCES", "filter_col": "Name"},
    "FUNCTION": {"list": "LIST FUNCTIONS", "filter_col": "Name"},
    "RELATION": {"list": "LIST RELATIONS", "filter_col": "Name"},
    "STORE": {"list": "LIST STORES", "filter_col": "Name"},
    "STREAM": {"list": "LIST RELATIONS", "filter_col": "Name"},
}

# Dependency order for dropping resources (most dependent first)
# Note: Do not drop ENTITYs as they may cause authorization issues with topics
DROP_ORDER = [
    "RELATION",  # Materialized views, tables, views
    "STREAM",  # Streaming resources
    "CHANGELOG",  # Change data capture streams
    "FUNCTION",
    "COMPUTE_POOL",
    "FUNCTION_SOURCE",
    "DESCRIPTOR_SOURCE",
    # Note: SCHEMA_REGISTRY not included as it's not created in integration tests
]


def drop_relation_with_retry(
    relation_name: str,
    relation_type: str = "RELATION",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Attempt to drop a DeltaStream relation with retry logic using direct API.
    Materialized views and other streaming resources may have running queries
    that prevent immediate deletion. This function retries the drop operation
    until it succeeds or times out.
    Args:
        relation_name: Name of the relation to drop
        relation_type: Type of relation (RELATION, STREAM, CHANGELOG, etc.)
        database: Optional database name (uses env var if not provided)
        schema: Optional schema name (uses env var if not provided)
        max_wait_seconds: Maximum time to wait in seconds
        retry_interval_seconds: Time to wait between retries
    Returns:
        True if successfully dropped, False if timed out
    """
    return run_async_safely(
        drop_relation_with_retry_async(
            relation_name,
            relation_type,
            database,
            schema,
            max_wait_seconds,
            retry_interval_seconds,
        )
    )


@retry_on_failure()
async def drop_relation_with_retry_async(
    relation_name: str,
    relation_type: str = "RELATION",
    database: Optional[str] = None,
    schema: Optional[str] = None,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Async version of drop_relation_with_retry.
    """
    # Validate input
    relation_name = validate_resource_name(relation_name)

    # Always use DROP RELATION for all relation types
    sql = f"DROP RELATION {relation_name};"

    try:
        async with get_deltastream_connection(database, schema) as conn:
            result = await conn.query(sql)
            # Consume the result to ensure command completes
            async for _ in result:
                pass
        return True
    except SQLError as e:
        error_message = str(e).lower()
        # Check if resource doesn't exist - this is OK for cleanup
        if "does not exist" in error_message or "not found" in error_message:
            logger.info(
                "%s %s does not exist, skipping cleanup", relation_type, relation_name
            )
            return True
        # Re-raise for retry decorator to handle
        raise


def drop_schema_with_retry(
    schema: str,
    database: str,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Attempt to drop a schema with retry logic.
    Args:
        schema: Name of the schema to drop
        database: Database name
        max_wait_seconds: Maximum time to wait in seconds
        retry_interval_seconds: Time to wait between retries
    Returns:
        True if successfully dropped, False if timed out
    """
    return run_async_safely(
        drop_schema_with_retry_async(
            schema, database, max_wait_seconds, retry_interval_seconds
        )
    )


@retry_on_failure()
async def drop_schema_with_retry_async(
    schema: str,
    database: str,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Async version of drop_schema_with_retry.
    """
    # Validate input
    schema = validate_resource_name(schema)
    database = validate_resource_name(database)

    sql = f"DROP SCHEMA {schema};"

    try:
        async with get_deltastream_connection(database, "public") as conn:
            result = await conn.query(sql)
            async for _ in result:
                pass
        return True
    except SQLError as e:
        error_message = str(e).lower()
        # Check if schema doesn't exist - this is OK for cleanup
        if "does not exist" in error_message or "not found" in error_message:
            logger.info("Schema %s does not exist, skipping", schema)
            return True
        # Re-raise for retry decorator to handle
        raise


def drop_database_with_retry(
    database: str,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Attempt to drop a database with retry logic.
    Args:
        database: Name of the database to drop
        max_wait_seconds: Maximum time to wait in seconds
        retry_interval_seconds: Time to wait between retries
    Returns:
        True if successfully dropped, False if timed out
    """
    return run_async_safely(
        drop_database_with_retry_async(
            database, max_wait_seconds, retry_interval_seconds
        )
    )


@retry_on_failure()
async def drop_database_with_retry_async(
    database: str,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Async version of drop_database_with_retry.
    """
    # Validate input
    database = validate_resource_name(database)

    sql = f"DROP DATABASE {database};"

    try:
        async with get_deltastream_connection() as conn:
            result = await conn.query(sql)
            async for _ in result:
                pass
        return True
    except SQLError as e:
        error_message = str(e).lower()
        # Check if database doesn't exist - this is OK for cleanup
        if "does not exist" in error_message or "not found" in error_message:
            logger.info("Database %s already dropped", database)
            return True
        # Re-raise for retry decorator to handle
        raise


def cleanup_resources(
    resources: List[Dict[str, Any]],
    database: Optional[str] = None,
    schema: Optional[str] = None,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
) -> None:
    """
    Clean up multiple DeltaStream resources with retry logic using direct API.
    Args:
        resources: List of dicts with 'name' and 'type' keys
                  Example: [{'name': 'my_mv', 'type': 'RELATION'},
                           {'name': 'my_stream', 'type': 'STREAM'}]
        database: Optional database name (uses env var if not provided)
        schema: Optional schema name (uses env var if not provided)
        max_wait_seconds: Maximum time to wait for each resource
    """
    # First, try to terminate all running queries to allow resources to be dropped
    try:
        logger.info("Terminating running queries before cleanup...")
        terminate_all_queries(database, schema)
        # Give queries a moment to terminate
        time.sleep(2)
    except Exception as e:
        logger.warning("Could not terminate queries: %s", e)

    # Now proceed with dropping resources
    for resource in resources:
        name = resource.get("name")
        resource_type = resource.get("type", "RELATION")

        if not name:
            logger.warning("Resource missing 'name' key, skipping")
            continue

        # Global resources that don't need database context
        global_resources = {
            "STORE",
            "SCHEMA_REGISTRY",
            "COMPUTE_POOL",
            "DESCRIPTOR_SOURCE",
            "ENTITY",
            "FUNCTION",
            "FUNCTION_SOURCE",
        }

        # Determine database/schema context for dropping
        db_context = None if resource_type in global_resources else database
        schema_context = None if resource_type in global_resources else schema

        try:
            success = drop_relation_with_retry(
                name,
                resource_type,
                database=db_context,
                schema=schema_context,
                max_wait_seconds=max_wait_seconds,
            )
            if not success:
                logger.warning(
                    "Failed to drop %s %s within timeout", resource_type, name
                )
        except Exception as e:
            logger.warning("Error cleaning up %s %s: %s", resource_type, name, e)


def list_resources(
    resource_type: str,
    database: Optional[str] = None,
    schema: Optional[str] = None,
) -> List[Dict[str, str]]:
    """
    List DeltaStream resources of a specific type using direct API.
    Args:
        resource_type: Type of resource to list (RELATION, STREAM, etc.)
        database: Optional database name
        schema: Optional schema filter
    Returns:
        List of resource dictionaries with Name and Type fields
    """
    if resource_type not in RESOURCE_TYPES:
        logger.warning("Unknown resource type %s", resource_type)
        return []

    list_cmd = RESOURCE_TYPES[resource_type]["list"]

    # Global resources that don't need database context
    global_resources = {
        "STORE",
        "SCHEMA_REGISTRY",
        "COMPUTE_POOL",
        "DESCRIPTOR_SOURCE",
        "ENTITY",
        "FUNCTION",
        "FUNCTION_SOURCE",
    }

    if resource_type in global_resources:
        # For global resources, create connection without database context
        conn = _get_deltastream_connection()
    else:
        # For database-scoped resources, use database context
        conn = _get_deltastream_connection(database, schema)

    async def _list_resources():
        resources: List[Dict[str, str]] = []

        # For RELATION, STREAM, CHANGELOG types, list relations and filter by type
        if resource_type in ["RELATION", "STREAM", "CHANGELOG"]:
            try:
                schemas = await _list_schemas_async(database or "deltastream")
                logger.debug("Found %d schema(s): %s", len(schemas), ", ".join(schemas))
            except Exception as e:
                logger.warning(
                    "Skipping relation listing for invalid database %s: %s", database, e
                )
                return resources  # Return empty list if database is invalid

            for schema_name in schemas:
                try:
                    scoped_cmd = f"LIST RELATIONS IN SCHEMA {database}.{schema_name}"
                    result = await conn.query(f"{scoped_cmd};")
                    async for row in result:
                        if isinstance(row, dict):
                            # Ensure we have both Name and Type
                            name = row.get("Name") or row.get("name")
                            rel_type = row.get("Type") or row.get("type")
                            if name and rel_type:
                                # For RELATION type, include all relation types
                                # For STREAM/CHANGELOG, only include matching types
                                if (
                                    resource_type == "RELATION"
                                    or rel_type.upper() == resource_type
                                ):
                                    resources.append(
                                        {
                                            "Name": name,
                                            "Type": rel_type,
                                            "Schema": schema_name,
                                        }
                                    )
                        elif isinstance(row, list) and len(row) >= 2:
                            # Assuming [Name, Type, ...] order
                            rel_type = str(row[1])
                            # For RELATION type, include all relation types
                            # For STREAM/CHANGELOG, only include matching types
                            if (
                                resource_type == "RELATION"
                                or rel_type.upper() == resource_type
                            ):
                                resources.append(
                                    {
                                        "Name": str(row[0]),
                                        "Type": rel_type,
                                        "Schema": schema_name,
                                    }
                                )
                except Exception as e:
                    logger.warning(
                        "Could not list relations in schema %s: %s", schema_name, e
                    )
        else:
            # For other resource types, use the standard list command
            try:
                result = await conn.query(f"{list_cmd};")
                async for row in result:
                    if isinstance(row, dict):
                        resources.append(row)
                    elif isinstance(row, list) and len(row) > 0:
                        resources.append({"Name": str(row[0]) if row else ""})
            except Exception as e:
                logger.warning("Could not list %s: %s", resource_type, e)

        return resources

    return run_async_safely(_list_resources())


async def _list_schemas_async(database: str) -> list[str]:
    """
    List all schemas in a database (async helper).
    Args:
        database: Database name
    Returns:
        List of schema names
    Raises:
        Exception: If database is invalid or doesn't exist
    """
    conn = _get_deltastream_connection(database, "public")

    try:
        result = await conn.query("LIST SCHEMAS;")
        schemas: list[str] = []
        async for row in result:
            schema_name = ""
            if isinstance(row, dict):
                schema_name = row.get("Name", "") or row.get("name", "")
            elif isinstance(row, list) and len(row) > 0:
                schema_name = str(row[0]) if row else ""

            if schema_name:
                schemas.append(schema_name)
        return schemas
    except Exception as e:
        logger.warning("Could not list schemas for database %s: %s", database, e)
        # Re-raise the exception instead of returning fallback
        raise


def list_schemas(database: str) -> list[str]:
    """
    List all schemas in a database.
    Args:
        database: Database name
    Returns:
        List of schema names
    """
    return run_async_safely(_list_schemas_async(database))


def terminate_all_queries(
    database: Optional[str] = None, schema: Optional[str] = None
) -> bool:
    """
    Terminate all running queries using direct API.
    Args:
        database: Optional database name
        schema: Optional schema name
    Returns:
        True if successful, False otherwise
    """
    conn = _get_deltastream_connection(database, schema)

    async def _terminate_queries():
        try:
            logger.info("Terminating all running queries...")

            # Terminate queries multiple times to handle any that might be created during termination
            for attempt in range(3):
                logger.info("Query termination attempt %d/3...", attempt + 1)

                # First, list all running queries
                result = await conn.query("LIST QUERIES;")
                query_ids: List[str] = []
                async for row in result:
                    # row could be a list or dict, extract query ID
                    if isinstance(row, dict):
                        query_id = (
                            row.get("id")
                            or row.get("query_id")
                            or row.get("Id")
                            or row.get("Query_Id")
                        )
                        if query_id:
                            query_ids.append(str(query_id))
                    elif isinstance(row, list) and len(row) > 0:
                        # Assume first column is the ID
                        query_ids.append(str(row[0]))

                if not query_ids:
                    logger.info("No running queries found on attempt %d", attempt + 1)
                    break

                logger.info("Found %d running queries to terminate", len(query_ids))

                # Terminate each query
                terminated_count = 0
                for query_id in query_ids:
                    try:
                        terminate_result = await conn.query(
                            f"TERMINATE QUERY {query_id};"
                        )
                        async for _ in terminate_result:
                            pass
                        terminated_count += 1
                        logger.debug("Terminated query %s", query_id)
                    except Exception as e:
                        logger.warning("Could not terminate query %s: %s", query_id, e)

                logger.info(
                    "Terminated %d/%d queries on attempt %d",
                    terminated_count,
                    len(query_ids),
                    attempt + 1,
                )

                # Wait a bit before checking again
                if attempt < 2:  # Don't wait after the last attempt
                    await asyncio.sleep(3)

            # Final wait to ensure all terminations have taken effect
            await asyncio.sleep(2)
            return True
        except Exception as e:
            logger.warning("Could not terminate queries: %s", e)
            return False

    return run_async_safely(_terminate_queries())


def clean_database_with_children(
    database: str,
    schema: Optional[str] = None,
    fingerprint: Optional[str] = None,
    drop_schemas: bool = True,
    drop_database: bool = True,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> Dict[str, List[str]]:
    """
    Clean up a DeltaStream database and all its children resources using direct API.
    This function performs comprehensive cleanup by:
    1. Terminating all running queries
    2. Listing and dropping all resources in dependency order
    3. Optionally dropping all schemas (if drop_schemas=True)
    4. Optionally dropping the database itself (if drop_database=True)
    5. Only dropping resources that match the fingerprint (if provided)
    Args:
        database: Name of the database to clean
        schema: Optional specific schema to clean (if None, cleans all schemas)
        fingerprint: Optional fingerprint to filter resources (e.g., timestamp)
                    Only resources containing this string will be dropped
        drop_schemas: If True, drop all schemas after resources
        drop_database: If True, drop the database after schemas
        max_wait_seconds: Maximum time to wait for each resource
        retry_interval_seconds: Time between retries
    Returns:
        Dictionary with cleanup results:
        {
            "dropped": ["resource1", "resource2"],
            "failed": ["resource3"],
            "errors": ["error message 1"]
        }
    """
    results: Dict[str, List[str]] = {"dropped": [], "failed": [], "errors": []}

    logger.info("=" * 80)
    logger.info("Starting comprehensive cleanup for database: %s", database)
    if schema:
        logger.info("  Schema: %s", schema)
    if fingerprint:
        logger.info("  Fingerprint filter: %s", fingerprint)
    if drop_schemas:
        logger.info("  Will drop schemas after resources")
    if drop_database:
        logger.info("  Will drop database after schemas")
    logger.info("=" * 80)

    # Step 0: Check if database exists
    logger.info("[CLEANUP] Step 0: Checking if database exists...")
    try:
        # Try to list schemas to verify database exists
        list_schemas(database)
        logger.info("  Database %s exists, proceeding with cleanup", database)
    except Exception as e:
        logger.warning("Database %s does not exist or is invalid: %s", database, e)
        logger.info("  Skipping cleanup for non-existent database")
        return results  # Return empty results if database doesn't exist

    # Step 1: Terminate all running queries
    logger.info("[CLEANUP] Step 1: Terminating running queries...")
    terminate_all_queries(database, schema)

    # Step 2: Drop resources in dependency order
    logger.info("[CLEANUP] Step 2: Dropping resources in dependency order...")

    for resource_type in DROP_ORDER:
        logger.info("[CLEANUP] Processing %ss...", resource_type)

        # Terminate queries before processing each resource type
        logger.info(
            "[CLEANUP] Terminating queries before processing %ss...", resource_type
        )
        terminate_all_queries(database, schema)

        # List resources of this type
        try:
            resources = list_resources(resource_type, database, schema)
            for resource in resources:
                resource_name = resource.get("Name") or resource.get("name")
                if not resource_name:
                    continue

                # Filter by fingerprint if provided
                if fingerprint and fingerprint not in resource_name:
                    logger.debug("Skipping %s (no fingerprint match)", resource_name)
                    continue

                # For RELATION type, use the actual Type from LIST RELATIONS
                actual_type = resource_type
                if resource_type == "RELATION":
                    actual_type = (
                        resource.get("Type") or resource.get("type") or resource_type
                    )
                    actual_type = actual_type.upper()  # Ensure uppercase for SQL

                    # Get schema for fully qualified name
                    resource_schema = (
                        resource.get("Schema")
                        or resource.get("schema")
                        or schema
                        or "public"
                    )
                    # Construct fully qualified name: database.schema.relation
                    fully_qualified_name = (
                        f"{database}.{resource_schema}.{resource_name}"
                    )
                else:
                    # For non-relation resources, use simple name
                    fully_qualified_name = resource_name

                # Global resources that don't need database context
                global_resources = {
                    "STORE",
                    "SCHEMA_REGISTRY",
                    "COMPUTE_POOL",
                    "DESCRIPTOR_SOURCE",
                    "ENTITY",
                    "FUNCTION",
                    "FUNCTION_SOURCE",
                }

                # Determine database/schema context for dropping
                db_context = None if actual_type in global_resources else database
                schema_context = None if actual_type in global_resources else schema

                # Drop the resource
                success = drop_relation_with_retry(
                    fully_qualified_name,
                    actual_type,
                    database=db_context,
                    schema=schema_context,
                    max_wait_seconds=max_wait_seconds,
                    retry_interval_seconds=retry_interval_seconds,
                )
                if success:
                    results["dropped"].append(f"{actual_type}:{fully_qualified_name}")
                    logger.info(
                        "      ✓ Dropped %s: %s", actual_type, fully_qualified_name
                    )
                else:
                    results["failed"].append(f"{actual_type}:{fully_qualified_name}")
                    results["errors"].append(
                        f"Failed to drop {actual_type}: {fully_qualified_name}"
                    )
                    logger.error(
                        "      ✗ Failed to drop %s: %s",
                        actual_type,
                        fully_qualified_name,
                    )
        except Exception as e:
            logger.warning("Could not process %ss: %s", resource_type, e)

    # Step 3: Drop all schemas (if requested)
    if drop_schemas:
        logger.info("[CLEANUP] Step 3: Dropping all schemas...")
        try:
            schemas = list_schemas(database)
            # Filter out system schemas if needed (e.g., don't drop 'public' if it's protected)
            for schema_name in schemas:
                # Skip system schemas that might be protected
                if schema_name.lower() in PROTECTED_SCHEMAS:
                    logger.debug("Skipping system schema: %s", schema_name)
                    continue

                logger.info("    Dropping schema: %s", schema_name)
                success = drop_schema_with_retry(
                    schema_name,
                    database,
                    max_wait_seconds=max_wait_seconds,
                    retry_interval_seconds=retry_interval_seconds,
                )
                if success:
                    results["dropped"].append(f"SCHEMA:{schema_name}")
                    logger.info("      ✓ Dropped schema: %s", schema_name)
                else:
                    error_msg = f"Failed to drop schema: {schema_name}"
                    results["errors"].append(error_msg)
                    logger.error("      ✗ %s", error_msg)
        except Exception as e:
            logger.warning("    Warning: Could not drop schemas: %s", e)

    # Step 4: Drop the database (if requested)
    if drop_database:
        logger.info("[CLEANUP] Step 4: Dropping database %s...", database)
        success = drop_database_with_retry(
            database,
            max_wait_seconds=max_wait_seconds,
            retry_interval_seconds=retry_interval_seconds,
        )

        if success:
            logger.info("  Successfully dropped database %s", database)
            results["dropped"].append(f"DATABASE:{database}")
        else:
            error_msg = f"Failed to drop database {database}"
            logger.error("  %s", error_msg)
            results["errors"].append(error_msg)

    # Print summary
    logger.info("=" * 80)
    logger.info("Cleanup Summary for database: %s", database)
    if fingerprint:
        logger.info("  Fingerprint: %s", fingerprint)
    logger.info("  Dropped: %d resources", len(results["dropped"]))
    logger.info("  Failed: %d resources", len(results["failed"]))
    logger.info("  Errors: %d errors", len(results["errors"]))
    logger.info("=" * 80)

    return results


def cleanup_resources_by_fingerprint(
    fingerprint: str,
    database: str,
    schema: str,
    resource_list: Optional[List[Dict[str, Any]]] = None,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
) -> Dict[str, List[str]]:
    """
    Clean up resources that match a specific fingerprint using direct API.
    This is useful for cleaning up all resources from a specific test run.
    If resource_list is not provided, all resources will be discovered automatically.
    Args:
        fingerprint: Fingerprint to match (e.g., timestamp like "20251111_204119_691")
                    Only resources containing this string will be dropped
        database: Database containing the resources
        schema: Schema containing the resources
        resource_list: Optional list of resource dicts with 'name' and 'type' keys
                      If None, resources will be discovered automatically
        max_wait_seconds: Maximum time to wait for each resource
    Returns:
        Dictionary with cleanup results
    """
    results: Dict[str, List[str]] = {"dropped": [], "failed": [], "errors": []}

    logger.info("=" * 80)
    logger.info("Cleaning up resources with fingerprint: %s", fingerprint)
    logger.info("  Database: %s", database)
    logger.info("  Schema: %s", schema)
    logger.info("=" * 80)

    # Terminate queries first
    terminate_all_queries(database, schema)

    # If resource_list not provided, discover resources automatically
    if resource_list is None:
        logger.info("Discovering resources automatically...")
        resources_by_type = {}
        for resource_type in DROP_ORDER:
            logger.info("  Discovering %ss...", resource_type)
            resources = list_resources(resource_type, database, schema)
            filtered = [
                r
                for r in resources
                if fingerprint in (r.get("Name") or r.get("name") or "")
            ]
            if filtered:
                resources_by_type[resource_type] = filtered
                logger.info(
                    "    Found %d %s(s) matching fingerprint",
                    len(filtered),
                    resource_type,
                )
    else:
        # Group provided resources by type
        resources_by_type = {}
        for resource in resource_list:
            rtype = resource.get("type", "RELATION")
            name = resource.get("name", "")

            # Filter by fingerprint
            if fingerprint and fingerprint not in name:
                continue

            if rtype not in resources_by_type:
                resources_by_type[rtype] = []
            resources_by_type[rtype].append(resource)

    # Drop resources in dependency order
    for resource_type in DROP_ORDER:
        if resource_type not in resources_by_type:
            continue

        logger.info("[CLEANUP] Dropping %ss...", resource_type)
        for resource in resources_by_type[resource_type]:
            resource_name = resource.get("Name") or resource.get("name")
            if not resource_name:
                continue

            # Double-check fingerprint match
            if fingerprint and fingerprint not in resource_name:
                continue

            # For RELATION type, use the actual Type from LIST RELATIONS
            actual_type = resource_type
            if resource_type == "RELATION":
                actual_type = (
                    resource.get("Type") or resource.get("type") or resource_type
                )
                actual_type = actual_type.upper()  # Ensure uppercase for SQL

                # Get schema for fully qualified name
                resource_schema = (
                    resource.get("Schema") or resource.get("schema") or schema
                )
                # Construct fully qualified name: database.schema.relation
                fully_qualified_name = f"{database}.{resource_schema}.{resource_name}"
            else:
                # For non-relation resources, use simple name
                fully_qualified_name = resource_name

            # Global resources that don't need database context
            global_resources = {
                "STORE",
                "SCHEMA_REGISTRY",
                "COMPUTE_POOL",
                "DESCRIPTOR_SOURCE",
                "ENTITY",
                "FUNCTION",
                "FUNCTION_SOURCE",
            }

            # Determine database/schema context for dropping
            db_context = None if actual_type in global_resources else database
            schema_context = None if actual_type in global_resources else schema

            success = drop_relation_with_retry(
                fully_qualified_name,
                actual_type,
                database=db_context,
                schema=schema_context,
                max_wait_seconds=max_wait_seconds,
            )

            if success:
                results["dropped"].append(f"{actual_type}:{fully_qualified_name}")
                logger.info("  ✓ Dropped %s: %s", actual_type, fully_qualified_name)
            else:
                results["failed"].append(f"{actual_type}:{fully_qualified_name}")
                results["errors"].append(
                    f"Failed to drop {actual_type}: {fully_qualified_name}"
                )
                logger.error(
                    "  ✗ Failed to drop %s: %s", actual_type, fully_qualified_name
                )

    # Print summary
    logger.info("=" * 80)
    logger.info("Cleanup Summary (fingerprint: %s)", fingerprint)
    logger.info("  Dropped: %d resources", len(results["dropped"]))
    logger.info("  Failed: %d resources", len(results["failed"]))
    logger.info("=" * 80)

    return results


def drop_store_with_retry(
    store_name: str,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Attempt to drop a DeltaStream store with retry logic.
    Args:
        store_name: Name of the store to drop
        max_wait_seconds: Maximum time to wait in seconds
        retry_interval_seconds: Time to wait between retries
    Returns:
        True if successfully dropped, False if timed out
    """
    return run_async_safely(
        drop_store_with_retry_async(
            store_name, max_wait_seconds, retry_interval_seconds
        )
    )


@retry_on_failure()
async def drop_store_with_retry_async(
    store_name: str,
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Async version of drop_store_with_retry.
    """
    # Validate input
    store_name = validate_resource_name(store_name)

    sql = f"DROP STORE {store_name};"

    try:
        async with get_deltastream_connection() as conn:
            result = await conn.query(sql)
            # Consume the result to ensure command completes
            async for _ in result:
                pass
        return True
    except SQLError as e:
        error_message = str(e).lower()
        # Check if store doesn't exist - this is OK for cleanup
        if "does not exist" in error_message or "not found" in error_message:
            logger.info("Store %s does not exist, skipping cleanup", store_name)
            return True
        # Re-raise for retry decorator to handle
        raise


def create_store_with_retry(
    store_name: str,
    store_type: str = "KAFKA",
    uris: str = "",
    sasl_hash_function: str = "AWS_MSK_IAM",
    aws_region: str = "us-east-1",
    iam_role_arn: str = "",
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Attempt to create a DeltaStream store with retry logic.
    Args:
        store_name: Name of the store to create
        store_type: Type of store (KAFKA, etc.)
        uris: Comma-separated list of broker URIs
        sasl_hash_function: SASL hash function for authentication
        aws_region: AWS region for MSK IAM
        iam_role_arn: IAM role ARN for MSK IAM authentication
        max_wait_seconds: Maximum time to wait in seconds
        retry_interval_seconds: Time to wait between retries
    Returns:
        True if successfully created, False if timed out
    """
    return run_async_safely(
        create_store_with_retry_async(
            store_name,
            store_type,
            uris,
            sasl_hash_function,
            aws_region,
            iam_role_arn,
            max_wait_seconds,
            retry_interval_seconds,
        )
    )


@retry_on_failure()
async def create_store_with_retry_async(
    store_name: str,
    store_type: str = "KAFKA",
    uris: str = "",
    sasl_hash_function: str = "AWS_MSK_IAM",
    aws_region: str = "us-east-1",
    iam_role_arn: str = "",
    max_wait_seconds: int = DEFAULT_MAX_WAIT_SECONDS,
    retry_interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
) -> bool:
    """
    Async version of create_store_with_retry.
    """
    # Validate input
    store_name = validate_resource_name(store_name)

    if not uris:
        raise ValueError("Store URIs must be provided")
    if not iam_role_arn:
        raise ValueError("IAM role ARN must be provided for MSK IAM authentication")

    sql = f"""
    CREATE STORE {store_name}
    WITH (
        'type' = {store_type},
        'uris' = '{uris}',
        'kafka.sasl.hash_function' = {sasl_hash_function},
        'kafka.msk.aws_region' = '{aws_region}',
        'kafka.msk.iam_role_arn' = '{iam_role_arn}'
    );
    """.strip()

    try:
        async with get_deltastream_connection() as conn:
            result = await conn.query(sql)
            # Consume the result to ensure command completes
            async for _ in result:
                pass
        return True
    except SQLError as e:
        error_message = str(e).lower()
        # Check if store already exists
        if "already exists" in error_message:
            logger.info("Store %s already exists", store_name)
            return True
        # Re-raise for retry decorator to handle
        raise


def list_stores() -> List[Dict[str, str]]:
    """
    List all DeltaStream stores.
    Returns:
        List of store dictionaries with Name and other fields
    """
    conn = _get_deltastream_connection()

    async def _list_stores():
        stores: List[Dict[str, str]] = []

        try:
            result = await conn.query("LIST STORES;")
            async for row in result:
                if isinstance(row, dict):
                    stores.append(row)
                elif isinstance(row, list) and len(row) > 0:
                    stores.append({"Name": str(row[0]) if row else ""})
        except Exception as e:
            logger.warning("Could not list stores: %s", e)

        return stores

    return run_async_safely(_list_stores())


def run_async_safely(coro):
    """
    Run an async coroutine, handling the case where an event loop is already running.
    Args:
        coro: The coroutine to run
    Returns:
        The result of the coroutine
    """
    try:
        _loop = asyncio.get_running_loop()
        # We're in an async context, create a task and wait for it
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(lambda: asyncio.run(coro))
            return future.result()
    except RuntimeError:
        # No running loop, safe to use asyncio.run()
        return asyncio.run(coro)
