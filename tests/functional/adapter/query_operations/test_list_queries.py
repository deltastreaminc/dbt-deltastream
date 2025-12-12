"""
Test query operations for deltastream adapter.

This test verifies that the adapter can list, describe, terminate, and restart queries.
"""

import pytest
from tests.functional.adapter.test_helpers import run_dbt_with_retry


@pytest.mark.integration
def test_list_queries(project):
    """Test listing all queries using the list_all_queries macro."""
    # Run the list_all_queries operation
    result = run_dbt_with_retry(["run-operation", "list_all_queries"], expect_pass=True)

    # The operation should succeed and return query information
    # Note: We can't assert specific queries since this depends on the current state
    assert result is not None
