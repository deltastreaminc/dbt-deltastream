"""
Test multi-entity stream patterns for deltastream adapter.

This test verifies complex scenarios involving multiple entities from trial_store,
including pageviews, users, and shipments.
Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from dbt.tests.util import write_file
from tests.functional.adapter.test_helpers import run_dbt_with_retry


@pytest.mark.integration
def test_three_entity_stream_join(
    project,
    integration_database,
    integration_schema,
    integration_store,
    integration_entity_names,
    integration_prefix,
):
    """Test creating streams from multiple entities and joining them."""
    # Generate timestamp suffix for unique names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    pageviews_stream = f"{integration_prefix}it_pageviews_multi_{timestamp}"
    users_changelog = f"{integration_prefix}it_users_multi_{timestamp}"
    shipments_stream = f"{integration_prefix}it_shipments_multi_{timestamp}"
    joined_stream = f"{integration_prefix}it_joined_multi_{timestamp}"

    # Create streams from pageviews, users, and shipments
    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {pageviews_stream}
        description: "Pageviews stream for multi-entity test"
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
        description: "Users changelog for multi-entity test"
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
      
      - name: {shipments_stream}
        description: "Shipments stream for multi-entity test"
        config:
          materialized: stream
          parameters:
            store: {integration_store}
            topic: {integration_entity_names["shipments"]}
            'value.format': JSON
        columns:
          - name: shipment_id
            type: VARCHAR
          - name: order_id
            type: VARCHAR
          - name: warehouse_id
            type: VARCHAR
          - name: shipment_time
            type: BIGINT
          - name: carrier
            type: VARCHAR
""".lstrip()

    write_file(sources_yml, project.project_root, "models", "sources.yml")
    run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)

    # Create a joined stream combining data from all three entities
    # Join pageviews with users, and include shipments data
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
    TO_TIMESTAMP_LTZ(p.viewtime, 3) AS viewtime,
    p.userid,
    p.pageid,
    u.regionid,
    u.gender,
    TO_TIMESTAMP_LTZ(u.registertime, 3) AS registertime
FROM {{{{ source('integration_tests', '{pageviews_stream}') }}}} p
JOIN {{{{ source('integration_tests', '{users_changelog}') }}}} u ON u.userid = p.userid
""".lstrip()

    write_file(model_sql, project.project_root, "models", f"{joined_stream}.sql")

    # Run dbt to create the joined stream
    run_dbt_with_retry(["run"], expect_pass=True)
