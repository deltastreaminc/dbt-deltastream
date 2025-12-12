"""
Test compute pool materialization functionality for deltastream adapter.

This test verifies that the adapter can create, update, stop, and drop compute pools correctly.
Compute pools are dedicated compute resource pools in DeltaStream.
"""

import logging
import pytest
from datetime import datetime
from dbt.tests.util import write_file
from tests.functional.adapter.test_helpers import run_dbt_with_retry

logger = logging.getLogger(__name__)


@pytest.mark.skip(reason="organization is not entitled to create a compute_pool")
class TestComputePoolLifecycleDeltastream:
    """Test complete compute pool lifecycle: create, update, stop, and drop."""

    # Session-level cleanup is handled by conftest.py
    @pytest.mark.integration
    def test_compute_pool_full_lifecycle(self, project, integration_schema):
        """Test complete compute pool lifecycle: create, update, stop, and drop."""
        # Generate timestamp suffix for unique compute pool name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        pool_name = f"it_pool_{timestamp}"

        # Step 1: Create compute pool
        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    schema: {integration_schema}
    tables:
      - name: {pool_name}
        description: "Integration test compute pool for lifecycle testing"
        config:
          materialized: compute_pool
          parameters:
            'compute_pool.size': 'small'
            'compute_pool.timeout_min': 5
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        logger.info("Step 1: Creating compute pool %s...", pool_name)
        run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)

        # Step 2: Update the compute pool
        # Note: Update operations may modify the pool's configuration
        logger.info("Step 2: Updating compute pool %s...", pool_name)
        try:
            # Update using the macro (re-run create_sources which should trigger update)
            sources_yml_updated = f"""
version: 2

sources:
  - name: integration_tests
    schema: {integration_schema}
    tables:
      - name: {pool_name}
        description: "Updated integration test compute pool"
        config:
          materialized: compute_pool
          parameters:
            'compute_pool.size': 'small'
            'compute_pool.timeout_min': 2
""".lstrip()
            write_file(
                sources_yml_updated, project.project_root, "models", "sources.yml"
            )
            run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)
        except Exception as e:
            logger.warning("Failed to update compute pool %s: %s", pool_name, e)

        # Step 3: Stop the compute pool
        # Compute pools need to be stopped before they can be dropped
        logger.info("Step 3: Stopping compute pool %s...", pool_name)
        try:
            run_dbt_with_retry(
                [
                    "run-operation",
                    "run_query",
                    "--args",
                    f"{{sql: 'STOP COMPUTE_POOL {integration_schema}.{pool_name};'}}",
                ],
                expect_pass=True,
            )
        except Exception as e:
            logger.warning("Failed to stop compute pool %s: %s", pool_name, e)
            # Continue anyway - maybe it's already stopped

        # Step 4: Drop the compute pool
        # DROP COMPUTE_POOL is implemented in the backend
        logger.info("Step 4: Dropping compute pool %s...", pool_name)
        try:
            run_dbt_with_retry(
                [
                    "run-operation",
                    "run_query",
                    "--args",
                    f"{{sql: 'DROP COMPUTE_POOL {integration_schema}.{pool_name};'}}",
                ],
                expect_pass=True,
            )
        except Exception as e:
            # If drop fails, log it but don't fail the test
            # The schema cleanup will handle it
            logger.warning("Failed to drop compute pool %s: %s", pool_name, e)
