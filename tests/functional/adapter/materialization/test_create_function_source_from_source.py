"""
Test function and function_source materialization functionality for deltastream adapter.

This test verifies that the adapter can create and drop UDF sources (JAR files) and functions.
Functions in DeltaStream are user-defined functions written in Java.
Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import logging
import pytest
from datetime import datetime
from pathlib import Path
from dbt.tests.util import write_file
from tests.functional.adapter.test_helpers import run_dbt_with_retry

logger = logging.getLogger(__name__)


class TestCreateFunctionSourceDeltastream:
    """Test function_source materialization functionality specific to DeltaStream."""

    @pytest.mark.integration
    def test_create_function_source_from_source(
        self, project, integration_database, integration_schema
    ):
        """Test creating a function source (JAR file) from a source configuration."""
        # Generate timestamp suffix for unique function source name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        func_source_name = f"it_func_src_{timestamp}"

        # Get the path to the actual JAR file in the test directory
        test_dir = Path(__file__).parent
        jar_path = test_dir / "flink-weighted-average-udf.jar"

        if not jar_path.exists():
            pytest.skip(f"JAR file not found at {jar_path}")

        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {func_source_name}
        description: "Integration test function source"
        config:
          materialized: function_source
          parameters:
            language: java
            file: '@{jar_path}'
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        # Run the operation to create sources
        try:
            run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)
        except Exception as e:
            logger.error("Failed to create function source: %s", e)
            raise
