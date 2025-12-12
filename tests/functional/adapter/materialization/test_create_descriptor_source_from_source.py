"""
Test descriptor_source materialization functionality for deltastream adapter.

This test verifies that the adapter can create and drop descriptor sources.
Descriptor sources are protocol buffer schema sources in DeltaStream.
Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import logging
import pytest
import shutil
import os
from datetime import datetime
from dbt.tests.util import write_file
from tests.functional.adapter.test_helpers import run_dbt_with_retry

logger = logging.getLogger(__name__)


class TestCreateDescriptorSourceDeltastream:
    """Test descriptor_source materialization functionality specific to DeltaStream."""

    @pytest.mark.integration
    def test_create_descriptor_source_from_source(
        self, project, integration_database, integration_schema
    ):
        """Test creating a descriptor source from a source configuration."""
        # Generate timestamp suffix for unique descriptor source name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[
            :-3
        ]  # microseconds truncated to 3 digits
        desc_source_name = f"it_desc_src_{timestamp}"

        # Copy the test descriptor file to the project directory
        test_desc_path = os.path.join(
            project.project_root, "tests", "functional", "adapter"
        )
        os.makedirs(test_desc_path, exist_ok=True)
        # Get the path to the descriptor file relative to this test file
        source_desc_file = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "test_schema.desc"
        )
        shutil.copy(
            source_desc_file,
            os.path.join(test_desc_path, "test_schema.desc"),
        )

        # Note: This test assumes a .desc file path is available
        # Descriptor sources use compiled protocol buffer .desc files
        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {desc_source_name}
        description: "Integration test descriptor source"
        config:
          materialized: descriptor_source
          parameters:
            file: './tests/functional/adapter/test_schema.desc'
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        # Run the operation to create sources
        try:
            run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)
        except Exception as e:
            logger.error("Failed to create descriptor source: %s", e)
            raise
