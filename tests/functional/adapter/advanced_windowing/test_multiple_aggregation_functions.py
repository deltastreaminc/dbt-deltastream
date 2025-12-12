"""
Test multiple aggregation functions for deltastream adapter.

This test verifies materialized views with multiple aggregation functions.

Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from dbt.tests.util import write_file
from tests.functional.adapter.test_helpers import run_dbt_with_retry
from tests.functional.adapter.test_helpers import generate_timestamp_suffix


class TestMultipleAggregationFunctions:
    """Test multiple aggregation functions specific to DeltaStream."""

    @pytest.mark.integration
    def test_multiple_aggregation_functions(
        self,
        project,
        integration_database,
        integration_schema,
        integration_store,
        integration_entity_names,
    ):
        """Test materialized view with multiple aggregation functions (COUNT, MIN, MAX, AVG)."""
        # Generate timestamp suffix for unique names
        timestamp = generate_timestamp_suffix()
        users_changelog = f"it_users_agg_{timestamp}"
        pageviews_stream = f"it_pageviews_agg_{timestamp}"
        joined_stream = f"it_joined_agg_{timestamp}"
        mv_name = f"it_mv_agg_{timestamp}"
        sources_file = f"sources_agg_{timestamp}.yml"

        # Get entity prefix from integration_entity_names
        prefix = integration_entity_names["pageviews"].replace("pageviews", "")
        joined_topic = f"{prefix}joined_agg_{timestamp}"

        # Create base streams
        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {pageviews_stream}
        description: "Pageviews for aggregation test"
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

      - name: {users_changelog}
        description: "Users for aggregation test"
        config:
          materialized: changelog
          primary_key: userid
          parameters:
            store: {integration_store}
            'topic': {integration_entity_names["users"]}
            'key.format': 'json'
            'key.type': 'STRUCT<userid VARCHAR>'
            'value.format': 'json'
        columns:
          - name: registertime
            type: BIGINT
          - name: userid
            type: VARCHAR
          - name: regionid
            type: VARCHAR
          - name: gender
            type: VARCHAR
""".lstrip()

        write_file(sources_yml, project.project_root, "models", sources_file)
        run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)

        # Create joined stream with explicit topic using entity prefix
        joined_sql = f"""
{{{{
  config(
    materialized='stream',
    parameters={{
      'value.format': 'json',
      'store': '{integration_store}',
      'topic': '{joined_topic}'
    }}
  )
}}}}
SELECT
    TO_TIMESTAMP_LTZ(p.viewtime, 3) AS viewtime,
    p.userid,
    p.pageid,
    u.regionid,
    u.gender
FROM {{{{ source('integration_tests', '{pageviews_stream}') }}}} p
JOIN {{{{ source('integration_tests', '{users_changelog}') }}}} u ON u.userid = p.userid
""".lstrip()

        write_file(joined_sql, project.project_root, "models", f"{joined_stream}.sql")

        # Create materialized view with multiple aggregations
        mv_sql = f"""
{{{{
  config(
    materialized='materialized_view'
  )
}}}}
SELECT
    window_start,
    window_end,
    regionid,
    gender,
    COUNT(*) AS total_views,
    COUNT(DISTINCT userid) AS unique_users,
    COUNT(DISTINCT pageid) AS unique_pages
FROM TUMBLE({{{{ ref('{joined_stream}') }}}}, SIZE 5 minute)
WITH ('timestamp'='viewtime')
GROUP BY window_start, window_end, regionid, gender
""".lstrip()

        write_file(mv_sql, project.project_root, "models", f"{mv_name}.sql")

        # Run dbt to create both stream and materialized view
        run_dbt_with_retry(["run"], expect_pass=True)
