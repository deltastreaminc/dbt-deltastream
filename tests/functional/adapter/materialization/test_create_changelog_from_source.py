"""
Test creating a changelog from source configuration for deltastream adapter.

This test verifies that the adapter can create a changelog from a source configuration.
Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from dbt.tests.util import write_file
from tests.functional.adapter.test_helpers import run_dbt_with_retry


class TestCreateChangelogFromSource:
    """Test creating a changelog from a source configuration."""

    @pytest.mark.integration
    def test_create_changelog_from_source(
        self,
        project,
        integration_database,
        integration_schema,
        integration_store,
        integration_entity_names,
    ):
        """Test creating a changelog from a source configuration."""
        # Generate timestamp suffix for unique changelog name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[
            :-3
        ]  # microseconds truncated to 3 digits
        changelog_name = f"it_changelog_{timestamp}"

        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {changelog_name}
        description: "Integration test changelog"
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
          - name: interests
            type: ARRAY<VARCHAR>
          - name: contactinfo
            type: STRUCT<phone VARCHAR, city VARCHAR, "state" VARCHAR, zipcode VARCHAR>
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        # Run the operation to create sources
        run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)
