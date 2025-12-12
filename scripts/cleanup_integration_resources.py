#!/usr/bin/env python3
"""
Standalone script to clean up all integration test resources.

This script reuses the cleanup functions from test_helpers.py and conftest.py to:
1. List all databases matching the it_db* pattern
2. For each database, terminate running queries and drop all child resources
3. Drop the databases themselves
4. Clean up entities with the integration test prefix in the specified store

Usage:
    python scripts/cleanup_integration_resources.py [--dry-run] [--entity-prefix PREFIX] [--store STORE]

Environment variables required:
    - DELTASTREAM_API_TOKEN: DeltaStream API token
    - DELTASTREAM_ORGANIZATION_ID: Organization ID
    - DELTASTREAM_STORE: Store name to clean entities from (can be overridden with --store)
    - DELTASTREAM_URL: DeltaStream API URL (optional)
    - DELTASTREAM_IT_PREFIX: Entity prefix to clean (optional, defaults to "dbte2e_")

Note: Only entities in the specified store will be cleaned to avoid permission errors on other stores.
"""

import argparse
import logging
import os
import sys

# Add the project root to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.conftest import (
    _drop_entity_if_exists,
    _get_deltastream_connection,
    _list_entities_in_store,
    _list_integration_databases,
)
from tests.functional.adapter.test_helpers import (
    clean_database_with_children,
    run_async_safely,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Default entity prefix from environment
DEFAULT_ENTITY_PREFIX = os.environ.get("DELTASTREAM_IT_PREFIX", "dbte2e_")
DEFAULT_STORE = os.environ.get("DELTASTREAM_STORE", "")


async def cleanup_entities_with_prefix(
    prefix: str, store_name: str, dry_run: bool = False
) -> dict[str, list[str]]:
    """Clean up entities matching a prefix in the specified store."""
    results: dict[str, list[str]] = {"dropped": [], "failed": [], "found": []}

    logger.info("=" * 60)
    logger.info("Cleaning entities with prefix: %s in store: %s", prefix, store_name)
    logger.info("=" * 60)

    conn = _get_deltastream_connection()
    entities = await _list_entities_in_store(conn, store_name)
    prefixed_entities = [e for e in entities if e.startswith(prefix)]

    if not prefixed_entities:
        logger.info(
            "  No entities with prefix '%s' found in store '%s'", prefix, store_name
        )
        return results

    logger.info(
        "  Found %d entities with prefix '%s' in store '%s':",
        len(prefixed_entities),
        prefix,
        store_name,
    )

    for entity_name in prefixed_entities:
        results["found"].append(f"{store_name}/{entity_name}")
        logger.info("  Found entity: %s (store: %s)", entity_name, store_name)

        if not dry_run:
            success = await _drop_entity_if_exists(conn, entity_name, store_name)
            if success:
                results["dropped"].append(f"{store_name}/{entity_name}")
                logger.info("    ✓ Dropped entity: %s", entity_name)
            else:
                results["failed"].append(f"{store_name}/{entity_name}")
                logger.error("    ✗ Failed to drop entity: %s", entity_name)

    return results


def cleanup_all(entity_prefix: str, store_name: str, dry_run: bool = False) -> None:
    """Clean up all integration test resources."""
    logger.info("=" * 60)
    logger.info("DeltaStream Integration Test Resource Cleanup")
    logger.info("=" * 60)
    if dry_run:
        logger.info("MODE: DRY RUN (no deletions will be performed)")
    logger.info("")

    # Validate store name is provided
    if not store_name:
        logger.error("DELTASTREAM_STORE environment variable must be set")
        logger.error(
            "This is required to avoid attempting cleanup on stores without permissions"
        )
        return

    all_dropped: list[str] = []
    all_failed: list[str] = []
    all_errors: list[str] = []

    # Step 1: Clean up integration test databases using test_helpers
    logger.info("Step 1: Cleaning integration test databases (it_db*)...")
    conn = _get_deltastream_connection()
    databases = run_async_safely(_list_integration_databases(conn))

    if not databases:
        logger.info("  No integration test databases found")
    else:
        logger.info("  Found %d integration test databases", len(databases))
        for db_name in databases:
            logger.info("  - %s", db_name)

        if not dry_run:
            for db_name in databases:
                logger.info("")
                logger.info("  Cleaning database: %s", db_name)
                # Derive fingerprint (timestamp) from db name: it_db_<timestamp>
                derived_fingerprint = db_name.replace("it_db_", "", 1)

                results = clean_database_with_children(
                    database=db_name,
                    fingerprint=derived_fingerprint,
                    drop_schemas=True,
                    drop_database=True,
                )
                all_dropped.extend(results["dropped"])
                all_failed.extend(results["failed"])
                all_errors.extend(results["errors"])

    logger.info("")

    # Step 2: Clean up ALL entities with IT prefix in the integration test store
    # Note: This is different from session cleanup which only drops base entities
    # This cleanup script drops ALL entities matching the prefix to ensure thorough cleanup
    logger.info(
        "Step 2: Cleaning ALL entities with prefix '%s' in store '%s'",
        entity_prefix,
        store_name,
    )
    entity_results = run_async_safely(
        cleanup_entities_with_prefix(entity_prefix, store_name, dry_run)
    )
    all_dropped.extend(entity_results["dropped"])
    all_failed.extend(entity_results["failed"])

    logger.info("")

    # Print summary
    logger.info("=" * 60)
    logger.info("Cleanup Summary")
    logger.info("=" * 60)
    if dry_run:
        logger.info("DRY RUN - No resources were deleted")
        logger.info("Would have processed:")
        logger.info("  - %d databases", len(databases))
        logger.info("  - %d entities", len(entity_results["found"]))
    else:
        logger.info("Total resources dropped: %d", len(all_dropped))
        logger.info("Total resources failed: %d", len(all_failed))
        logger.info("Total errors: %d", len(all_errors))

        if all_failed:
            logger.warning("Failed resources:")
            for resource in all_failed:
                logger.warning("  - %s", resource)

        if all_errors:
            logger.error("Errors encountered:")
            for error in all_errors:
                logger.error("  - %s", error)

    logger.info("=" * 60)


def main() -> None:
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Clean up DeltaStream integration test resources"
    )
    parser.add_argument(
        "--entity-prefix",
        default=DEFAULT_ENTITY_PREFIX,
        help=f"Prefix to filter entities (default: {DEFAULT_ENTITY_PREFIX})",
    )
    parser.add_argument(
        "--store",
        default=DEFAULT_STORE,
        help=f"Store name to clean entities from (default: {DEFAULT_STORE or 'from DELTASTREAM_STORE env var'})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List resources without deleting them",
    )

    args = parser.parse_args()

    # Validate required environment variables
    if not args.store:
        logger.error("ERROR: Store name is required")
        logger.error(
            "Please set DELTASTREAM_STORE environment variable or use --store argument"
        )
        sys.exit(1)

    try:
        cleanup_all(
            entity_prefix=args.entity_prefix,
            store_name=args.store,
            dry_run=args.dry_run,
        )
    except KeyboardInterrupt:
        logger.info("Cleanup interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error("Error during cleanup: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
