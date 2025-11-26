"""
Test HOP window with size and advance parameters for deltastream adapter.

This test verifies HOP window functionality with overlapping windows.

Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from dbt.tests.util import run_dbt, write_file
from tests.functional.adapter.test_helpers import generate_timestamp_suffix


class TestHopWindowSizeAdvance:
    """Test HOP window with size and advance parameters specific to DeltaStream."""

    @pytest.mark.integration
    def test_hop_window_size_advance(
        self,
        project,
        integration_database,
        integration_schema,
        integration_store,
        integration_entity_names,
    ):
        """Test HOP window with different size and advance by parameters for overlapping windows."""
        # Generate timestamp suffix for unique names
        timestamp = generate_timestamp_suffix()
        stream_name = f"it_stream_hop_{timestamp}"
        mv_name = f"it_mv_hop_{timestamp}"
        sources_file = f"sources_hop_{timestamp}.yml"

        # Create base stream
        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {stream_name}
        description: "Stream for HOP window test"
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

        write_file(sources_yml, project.project_root, "models", sources_file)
        run_dbt(["run-operation", "create_sources"], expect_pass=True)

        # Create materialized view with HOP window: 10 minute window, 2 minute advance
        model_sql = f"""
{{{{
  config(
    materialized='materialized_view'
  )
}}}}
SELECT
    window_start,
    window_end,
    userid,
    COUNT(*) AS view_count,
    COUNT(DISTINCT pageid) AS unique_pages_viewed
FROM HOP({{{{ source('integration_tests', '{stream_name}') }}}}, SIZE 10 minute, ADVANCE BY 2 minute)
WITH ('timestamp'='viewtime')
GROUP BY window_start, window_end, userid
""".lstrip()

        write_file(model_sql, project.project_root, "models", f"{mv_name}.sql")

        # Run dbt to create the materialized view
        run_dbt(["run"], expect_pass=True)
