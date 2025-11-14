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
def test_stream_with_zipcode_extraction(
    project,
    integration_database,
    integration_schema,
    integration_store,
    integration_entity_names,
    integration_prefix,
):
    """Test creating a stream that extracts nested fields (zipcode from contactinfo)."""
    # Generate timestamp suffix for unique names
    timestamp = generate_timestamp_suffix()
    users_changelog = f"{integration_prefix}users_zip_{timestamp}"
    pageviews_stream = f"{integration_prefix}pageviews_zip_{timestamp}"
    enriched_stream = f"{integration_prefix}enriched_zip_{timestamp}"
    zipcode_stream = f"{integration_prefix}zipcode_{timestamp}"

    # Create source streams and changelog
    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {pageviews_stream}
        description: "Pageviews stream"
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
        description: "Users changelog"
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
          - name: contactinfo
            type: STRUCT<phone VARCHAR, city VARCHAR, "state" VARCHAR, zipcode VARCHAR>
""".lstrip()

    write_file(sources_yml, project.project_root, "models", "sources.yml")
    run_dbt(["run-operation", "create_sources"], expect_pass=True)

    # Create enriched stream
    enriched_sql = f"""
{{{{
config(
materialized='stream',
parameters={{
  'value.format': 'json',
}},
columns=[
  {{
    'name': 'viewtime',
    'type': 'TIMESTAMP_LTZ(3)'
  }},
  {{
    'name': 'userid',
    'type': 'VARCHAR'
  }},
  {{
    'name': 'pageid',
    'type': 'VARCHAR'
  }},
  {{
    'name': 'contactinfo',
    'type': 'STRUCT<phone VARCHAR, city VARCHAR, "state" VARCHAR, zipcode VARCHAR>'
  }}
]
)
}}}}
SELECT
TO_TIMESTAMP_LTZ(p.viewtime, 3) AS viewtime,
p.userid AS userid,
p.pageid,
u.contactinfo
FROM {{{{ source('integration_tests', '{pageviews_stream}') }}}} p
JOIN {{{{ source('integration_tests', '{users_changelog}') }}}} u ON u.userid = p.userid
""".lstrip()

    write_file(enriched_sql, project.project_root, "models", f"{enriched_stream}.sql")

    # Create zipcode extraction stream
    zipcode_sql = f"""
{{{{
config(
materialized='stream',
parameters={{
  'value.format': 'json',
}}
)
}}}}
SELECT
viewtime,
userid,
pageid,
contactinfo['zipcode'] AS zipcode
FROM {{{{ ref('{enriched_stream}') }}}}
""".lstrip()

    write_file(zipcode_sql, project.project_root, "models", f"{zipcode_stream}.sql")

    # Run dbt to create both streams
    run_dbt(["run"], expect_pass=True)
