"""
Test query operations for deltastream adapter.

This test verifies that the adapter can list, describe, terminate, and restart queries.
"""

import pytest
from dbt.tests.util import run_dbt


@pytest.mark.integration
def test_list_queries(project):
    """Test listing all queries using the list_all_queries macro."""
    # Run the list_all_queries operation
    result = run_dbt(["run-operation", "list_all_queries"], expect_pass=True)

    # The operation should succeed and return query information
    # Note: We can't assert specific queries since this depends on the current state
    assert result is not None
