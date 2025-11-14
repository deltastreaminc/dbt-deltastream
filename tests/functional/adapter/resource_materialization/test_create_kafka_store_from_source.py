"""
Test store, schema_registry, and other resource materializations for deltastream adapter.

This test verifies that the adapter can create and drop stores and schema registries.
Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from dbt.tests.util import run_dbt, write_file


@pytest.mark.integration
@pytest.mark.skip(
    reason="Skipped: Store creation requires valid external Kafka cluster credentials. "
    "This test uses dummy credentials (localhost:9092, test-user, test-password) which will fail. "
    "To enable this test, provide valid Kafka cluster details in the test configuration."
)
def test_create_kafka_store_from_source(project):
    """Test creating a Kafka store from a source configuration."""
    # Generate timestamp suffix for unique store name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[
        :-3
    ]  # microseconds truncated to 3 digits
    store_name = f"it_store_{timestamp}"

    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    schema: public
    tables:
      - name: {store_name}
        description: "Integration test Kafka store"
        config:
          materialized: store
          parameters:
            type: kafka
            kafka.cluster.id: 'test-cluster'
            uris: 'kafka://localhost:9092'
            'sasl.username': 'test-user'
            'sasl.password': 'test-password'
            'sasl.mechanism': 'PLAIN'
            'security.protocol': 'SASL_SSL'
""".lstrip()

    write_file(sources_yml, project.project_root, "models", "sources.yml")

    # Run the operation to create sources
    run_dbt(["run-operation", "create_sources"], expect_pass=True)
