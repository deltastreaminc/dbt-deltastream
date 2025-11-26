"""
Test entity materialization functionality for deltastream adapter.

This test verifies that the adapter can create and drop entities correctly.
Entities are schema definitions in DeltaStream.
Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from dbt.tests.util import run_dbt, write_file


class TestCreateEntityDeltastream:
    """Test entity materialization functionality specific to DeltaStream."""

    @pytest.mark.integration
    def test_create_entity_from_source(
        self,
        project,
        integration_database,
        integration_schema,
        integration_entity_names,
    ):
        """Test creating an entity from a source configuration."""
        # Generate timestamp suffix for unique entity name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[
            :-3
        ]  # microseconds truncated to 3 digits
        # Get prefix from integration_entity_names
        prefix = integration_entity_names["pageviews"].replace("pageviews", "")
        entity_name = f"{prefix}it_entity_{timestamp}"

        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {entity_name}
        description: "Integration test entity"
        config:
          materialized: entity
        columns:
          - name: user_id
            type: VARCHAR
          - name: user_name
            type: VARCHAR
          - name: email
            type: VARCHAR
          - name: created_at
            type: BIGINT
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        # Run the operation to create sources
        run_dbt(["run-operation", "create_sources"], expect_pass=True)
