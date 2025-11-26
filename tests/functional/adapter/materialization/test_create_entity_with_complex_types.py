"""
Test entity materialization functionality for deltastream adapter.

This test verifies that the adapter can create entities with complex data types.
Entities are schema definitions in DeltaStream.

Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from dbt.tests.util import run_dbt, write_file


class TestCreateEntityDeltastream:
    """Test entity materialization functionality specific to DeltaStream."""

    @pytest.mark.integration
    def test_create_entity_with_complex_types(
        self,
        project,
        integration_database,
        integration_schema,
        integration_entity_names,
    ):
        """Test creating an entity with complex data types (ARRAY, STRUCT)."""
        # Generate timestamp suffix for unique entity name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        # Get prefix from integration_entity_names
        prefix = integration_entity_names["pageviews"].replace("pageviews", "")
        entity_name = f"{prefix}it_entity_complex_{timestamp}"

        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {entity_name}
        description: "Entity with complex types"
        config:
          materialized: entity
        columns:
          - name: user_id
            type: VARCHAR
          - name: tags
            type: ARRAY<VARCHAR>
          - name: metadata
            type: STRUCT<version INT, source VARCHAR>
          - name: address
            type: STRUCT<street VARCHAR, city VARCHAR, zipcode VARCHAR>
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        # Run the operation to create sources
        run_dbt(["run-operation", "create_sources"], expect_pass=True)

        # Session-level cleanup will handle resource deletion
