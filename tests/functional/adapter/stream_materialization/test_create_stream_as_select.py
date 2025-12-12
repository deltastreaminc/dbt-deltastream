"""
Test stream materialization functionality for deltastream adapter.

This test verifies that the adapter can create and drop streams correctly.
Streams are created from Kafka topics and can be used for stream processing.

Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from dbt.tests.util import write_file
from tests.functional.adapter.test_helpers import run_dbt_with_retry


@pytest.mark.integration
def test_create_stream_as_select(
    project,
    integration_database,
    integration_schema,
    integration_store,
    integration_entity_names,
    integration_prefix,
):
    """Test creating a stream using CSAS (Create Stream As Select)."""
    # Generate timestamp suffix for unique stream name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    base_stream_name = f"{integration_prefix}base_stream_{timestamp}"
    derived_stream_name = f"{integration_prefix}derived_stream_{timestamp}"

    # First, create a base stream from source
    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {base_stream_name}
        description: "Base integration test stream"
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
    run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)

    # Now create a derived stream using CSAS
    model_sql = f"""
{{{{
  config(
    materialized='stream',
    parameters={{
      'value.format': 'json',
    }}
  )
}}}}
SELECT
    viewtime,
    userid,
    pageid,
    TO_TIMESTAMP_LTZ(viewtime, 3) AS viewtime_ts
FROM {{{{ source('integration_tests', '{base_stream_name}') }}}}
""".lstrip()

    write_file(model_sql, project.project_root, "models", f"{derived_stream_name}.sql")

    # Run dbt to create the derived stream
    run_dbt_with_retry(["run"], expect_pass=True)
