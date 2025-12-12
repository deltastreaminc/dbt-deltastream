"""
Test function and function_source materialization functionality for deltastream adapter.

This test verifies that the adapter can create and drop UDF sources (JAR files) and functions.
Functions in DeltaStream are user-defined functions written in Java.
"""

import pytest
from datetime import datetime
from pathlib import Path
from dbt.tests.util import write_file
from tests.functional.adapter.test_helpers import run_dbt_with_retry


class TestCreateFunctionDeltastream:
    """Test function materialization functionality specific to DeltaStream."""

    # Session-level cleanup is handled by conftest.py
    def test_create_function_from_source(self, project, integration_schema):
        """Test creating a UDF from a source configuration with real JAR file."""
        # Generate timestamp suffix for unique names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        func_source_name = f"it_func_src_{timestamp}"
        func_name = f"it_func_{timestamp}"

        # Get the path to the actual JAR file in the test directory
        test_dir = Path(__file__).parent
        jar_path = test_dir / "flink-weighted-average-udf.jar"

        if not jar_path.exists():
            pytest.skip(f"JAR file not found at {jar_path}")

        # Create function source and function
        # The flink-weighted-average-udf.jar contains a WeightedAverage function
        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    schema: {integration_schema}
    tables:
      - name: {func_source_name}
        description: "Function source for UDF test"
        config:
          materialized: function_source
          parameters:
            language: java
            file: '@{jar_path}'
      - name: {func_name}
        description: "Integration test UDF"
        config:
          materialized: function
          parameters:
            input_parameters: 'value DOUBLE, weight DOUBLE'
            return_type: DOUBLE
            source.name: {func_source_name}
            class.name: 'io.deltastream.flink.WeightedAverage'
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        # Create the function source and function
        run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)
