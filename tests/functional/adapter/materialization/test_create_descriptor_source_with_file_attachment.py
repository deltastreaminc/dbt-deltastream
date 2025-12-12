"""
Test descriptor_source materialization functionality for deltastream adapter.

This test verifies that the adapter can create descriptor sources with file attachments.
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
    def test_create_descriptor_source_with_file_attachment(
        self, project, integration_database, integration_schema
    ):
        """Test creating a descriptor source with file attachment support."""
        # Generate timestamp suffix for unique descriptor source name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        desc_source_name = f"it_desc_src_attach_{timestamp}"

        # Copy the sample descriptor file to the project directory
        protos_path = os.path.join(project.project_root, "protos", "compiled")
        os.makedirs(protos_path, exist_ok=True)
        # Get the path to the descriptor file relative to the repository root
        # This test file is in tests/functional/adapter/materialization/
        # The descriptor file is in protos/compiled/sample.desc
        repo_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        )
        source_desc_file = os.path.join(repo_root, "protos", "compiled", "sample.desc")
        shutil.copy(
            source_desc_file,
            os.path.join(protos_path, "sample.desc"),
        )

        # According to dbt_adapter_features.md, descriptor_source supports
        # file attachment with compiled .desc files
        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    database: {integration_database}
    schema: {integration_schema}
    tables:
      - name: {desc_source_name}
        description: "Descriptor source with file attachment"
        config:
          materialized: descriptor_source
          parameters:
            file: './protos/compiled/sample.desc'
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        # This should succeed with the actual .desc file
        try:
            run_dbt_with_retry(["run-operation", "create_sources"], expect_pass=True)
        except Exception as e:
            logger.error(
                "Failed to create descriptor source with file attachment: %s", e
            )
            raise

        # Session-level cleanup will handle resource deletion
