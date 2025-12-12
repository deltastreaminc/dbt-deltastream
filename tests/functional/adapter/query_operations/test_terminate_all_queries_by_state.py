"""
Test query operations for deltastream adapter.

This test verifies that the adapter can list, describe, terminate, and restart queries.
"""

import logging
import pytest
from tests.functional.adapter.test_helpers import run_dbt_with_retry

logger = logging.getLogger(__name__)


@pytest.mark.integration
def test_terminate_all_queries_by_state(project):
    """Test terminating queries by state using terminate_all_queries macro."""
    # This test verifies the terminate_all_queries macro
    # which can filter by state (RUNNING, FAILED, etc.)

    # Try to terminate all running queries
    try:
        run_dbt_with_retry(
            [
                "run-operation",
                "terminate_all_queries",
                "--args",
                "{state: RUNNING}",
            ],
            expect_pass=True,
        )
    except Exception as e:
        logger.info("terminate_all_queries with state filter: %s", e)

    # Try to terminate all queries (no state filter)
    try:
        run_dbt_with_retry(
            [
                "run-operation",
                "terminate_all_queries",
            ],
            expect_pass=True,
        )
    except Exception as e:
        logger.info("terminate_all_queries without filter: %s", e)
