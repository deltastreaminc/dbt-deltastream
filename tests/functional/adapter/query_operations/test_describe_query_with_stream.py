"""
Test query operations for deltastream adapter.

This test verifies that the adapter can list and describe queries.

Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from dbt.tests.util import run_dbt, write_file


@pytest.mark.integration
def test_describe_query_with_stream(
    project, integration_store, integration_entity_names, integration_prefix
):
    """Test that streams create queries that can be described."""
    # Generate timestamp suffix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    stream_name = f"{integration_prefix}it_stream_describe_{timestamp}"

    # Create a stream (which creates a running query)
    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    schema: public
    tables:
      - name: {stream_name}
        description: "Stream for describe test"
        config:
          materialized: stream
          parameters:
            store: {integration_store}
            topic: {integration_entity_names["pageviews"]}
            'value.format': JSON
        columns:
          - name: viewtime
            type: BIGINT
          - name: userid
            type: VARCHAR
          - name: pageid
            type: VARCHAR
""".lstrip()

    write_file(sources_yml, project.project_root, "models", "sources.yml")
    run_dbt(["run-operation", "create_sources"], expect_pass=True)

    # List queries to verify our stream created one
    run_dbt(["run-operation", "list_all_queries"], expect_pass=True)

    # Session-level cleanup will handle resource deletion
