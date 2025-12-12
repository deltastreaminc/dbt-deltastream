"""
Test creating a changelog using CCAS (Create Changelog As Select) for deltastream adapter.

This test verifies that the adapter can create a changelog using a SELECT query.
Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from dbt.tests.util import write_file
from tests.functional.adapter.test_helpers import run_dbt_with_retry


class TestCreateChangelogAsSelect:
    """Test creating a changelog using CCAS (Create Changelog As Select)."""

    @pytest.mark.integration
    def test_create_changelog_as_select(
        self, project, integration_store, integration_entity_names, integration_prefix
    ):
        """Test creating a changelog using CCAS (Create Changelog As Select)."""
        # Generate timestamp suffix for unique names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        source_changelog_name = f"{integration_prefix}it_source_changelog_{timestamp}"
        derived_changelog_name = f"{integration_prefix}it_derived_changelog_{timestamp}"

        # First, create a source changelog
        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    schema: public
    tables:
      - name: {source_changelog_name}
        description: "Source changelog for integration test"
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

        write_file(sources_yml, project.project_root, "models", "sources.yml")
        run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)

        # Now create a derived changelog using CCAS
        model_sql = f"""
{{{{
  config(
    materialized='changelog',
    primary_key='userid',
    parameters={{
      'value.format': 'json',
    }}
  )
}}}}
SELECT 
    userid,
    regionid,
    gender,
    TO_TIMESTAMP_LTZ(registertime, 3) AS registertime_ts
FROM {{{{ source('integration_tests', '{source_changelog_name}') }}}}
""".lstrip()

        write_file(
            model_sql,
            project.project_root,
            "models",
            f"{derived_changelog_name}.sql",
        )

        # Run dbt to create the derived changelog
        run_dbt_with_retry(["run"], expect_pass=True)
