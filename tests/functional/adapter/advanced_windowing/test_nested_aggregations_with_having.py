"""
Test nested aggregations with HAVING clause for deltastream adapter.

This test verifies aggregations with HAVING clause for filtering results.

Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from dbt.tests.util import run_dbt, write_file
from tests.functional.adapter.test_helpers import generate_timestamp_suffix


class TestNestedAggregationsWithHaving:
    """Test nested aggregations with HAVING clause specific to DeltaStream."""

    @pytest.mark.integration
    def test_nested_aggregations_with_having(
        self,
        project,
        integration_database,
        integration_schema,
        integration_store,
        integration_entity_names,
    ):
        """Test aggregations with HAVING clause for filtering aggregated results."""
        # Generate timestamp suffix for unique names
        timestamp = generate_timestamp_suffix()
        stream_name = f"it_stream_having_{timestamp}"
        mv_name = f"it_mv_having_{timestamp}"
        sources_file = f"sources_having_{timestamp}.yml"

        # Create base stream
        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {stream_name}
        description: "Stream for HAVING clause test"
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

        # Create materialized view with HAVING clause to filter windows with high activity
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
    COUNT(*) AS view_count
FROM TUMBLE({{{{ source('integration_tests', '{stream_name}') }}}}, SIZE 1 minute)
WITH ('timestamp'='viewtime')
GROUP BY window_start, window_end, userid
HAVING COUNT(*) > 0
""".lstrip()

        write_file(model_sql, project.project_root, "models", f"{mv_name}.sql")

        # Run dbt to create the materialized view
        run_dbt(["run"], expect_pass=True)
