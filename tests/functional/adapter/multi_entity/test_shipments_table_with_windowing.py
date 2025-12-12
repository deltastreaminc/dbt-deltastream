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
def test_shipments_table_with_windowing(
    project,
    integration_database,
    integration_schema,
    integration_store,
    integration_entity_names,
):
    """Test creating a materialized view from shipments with time-based windowing."""
    # Generate timestamp suffix for unique names
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    shipments_stream = f"it_shipments_window_{timestamp}"
    mv_name = f"it_shipments_mv_{timestamp}"

    # Create shipments stream
    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {shipments_stream}
        description: "Shipments stream for windowing test"
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

    # Create materialized view with HOP window counting shipments per warehouse
    model_sql = f"""
{{{{
config(
materialized='materialized_view'
)
}}}}
SELECT 
window_start,
window_end,
warehouse_id,
carrier,
COUNT(*) AS shipment_count
FROM HOP({{{{ source('integration_tests', '{shipments_stream}') }}}}, SIZE 5 minute, ADVANCE BY 1 minute)
WITH ('timestamp'='shipment_time')
GROUP BY window_start, window_end, warehouse_id, carrier
""".lstrip()

    write_file(model_sql, project.project_root, "models", f"{mv_name}.sql")

    # Run dbt to create the materialized view
    run_dbt_with_retry(["run"], expect_pass=True)
