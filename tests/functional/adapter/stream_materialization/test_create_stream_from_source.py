"""
Test stream materialization functionality for deltastream adapter.

This test verifies that the adapter can create and drop streams correctly.
Streams are created from Kafka topics and can be used for stream processing.

Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from dbt.tests.util import run_dbt, write_file


@pytest.mark.integration
def test_create_stream_from_source(
    project,
    integration_database,
    integration_schema,
    integration_store,
    integration_entity_names,
):
    """Test creating a stream from a source configuration with explicit schema."""
    # Generate timestamp suffix for unique stream name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[
        :-3
    ]  # microseconds truncated to 3 digits
    stream_name = f"it_stream_{timestamp}"

    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {stream_name}
        description: "Integration test stream"
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

    # Run the operation to create sources
    run_dbt(["run-operation", "create_sources"], expect_pass=True)
