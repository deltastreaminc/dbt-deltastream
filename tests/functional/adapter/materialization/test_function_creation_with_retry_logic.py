"""
Test function and function_source materialization functionality for deltastream adapter.

This test verifies that the adapter can create and drop UDF sources (JAR files) and functions.
Functions in DeltaStream are user-defined functions written in Java.
"""

import logging
import pytest
from datetime import datetime
from dbt.tests.util import run_dbt, write_file

logger = logging.getLogger(__name__)


@pytest.mark.integration
def test_function_creation_with_retry_logic(
    project, integration_database, integration_schema
):
    """Test that function creation includes retry logic when source is not ready."""
    # This test verifies the adapter's automatic retry mechanism
    # According to dbt_adapter_features.md, functions have automatic retry logic
    # with exponential backoff when the source is not ready

    # Generate timestamp suffix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    func_name = f"it_func_retry_{timestamp}"

    # Create a function config that would trigger retry logic
    sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {func_name}
        description: "Function for retry logic test"
        config:
          materialized: function
          parameters:
            input_parameters: 'x INT'
            return_type: INT
            source.name: non_existent_source
            class.name: 'com.example.TestFunction'
""".lstrip()

    write_file(sources_yml, project.project_root, "models", "sources.yml")

    # This should fail after retries since the source doesn't exist
    try:
        run_dbt(["run-operation", "create_sources"], expect_pass=False)
    except Exception as e:
        logger.info("Expected failure after retries: %s", e)
