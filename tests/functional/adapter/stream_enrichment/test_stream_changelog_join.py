"""
Test stream enrichment patterns for deltastream adapter.

This test verifies common patterns like joining streams with changelogs,
similar to the hello_deltastream example.
Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from dbt.tests.util import run_dbt, write_file
from tests.functional.adapter.test_helpers import generate_timestamp_suffix


@pytest.mark.integration
def test_stream_changelog_join(
    project,
    integration_database,
    integration_schema,
    integration_store,
    integration_entity_names,
    integration_prefix,
):
    """Test creating an enriched stream by joining a stream with a changelog."""
    # Generate timestamp suffix for unique names
    timestamp = generate_timestamp_suffix()
    pageviews_stream = f"{integration_prefix}pageviews_{timestamp}"
    users_changelog = f"{integration_prefix}users_{timestamp}"
    enriched_stream = f"{integration_prefix}enriched_{timestamp}"

    # Create pageviews stream and users changelog
    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {pageviews_stream}
        description: "Pageviews stream for enrichment test"
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
        description: "Users changelog for enrichment test"
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
    run_dbt(["run-operation", "create_sources"], expect_pass=True)

    # Create enriched stream by joining pageviews with users
    model_sql = f"""
{{{{
  config(
    materialized='stream',
    parameters={{
      'value.format': 'json',
      'key.format': 'JSON',
      'key.columns': 'userid',
    }}
  )
}}}}
SELECT
    TO_TIMESTAMP_LTZ(p.viewtime, 3) AS viewtime,
    p.userid AS userid,
    p.pageid,
    TO_TIMESTAMP_LTZ(u.registertime, 3) AS registertime,
    u.regionid,
    u.gender,
    u.interests,
    u.contactinfo
FROM {{{{ source('integration_tests', '{pageviews_stream}') }}}} p
JOIN {{{{ source('integration_tests', '{users_changelog}') }}}} u ON u.userid = p.userid
""".lstrip()

    write_file(model_sql, project.project_root, "models", f"{enriched_stream}.sql")

    # Run dbt to create the enriched stream
    run_dbt(["run"], expect_pass=True)
