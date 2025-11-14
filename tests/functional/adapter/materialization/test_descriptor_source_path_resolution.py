"""
Test descriptor_source materialization functionality for deltastream adapter.

This test verifies that the adapter can create descriptor sources and handle path resolution.
Descriptor sources are protocol buffer schema sources in DeltaStream.

Resources are automatically cleaned up by the session-level fixture in conftest.py.
"""

import pytest
from datetime import datetime
from pathlib import Path
from dbt.tests.util import run_dbt, write_file


class TestCreateDescriptorSourceDeltastream:
    """Test descriptor_source materialization functionality specific to DeltaStream."""

    @pytest.mark.integration
    def test_descriptor_source_path_resolution(self, project):
        """Test that descriptor source correctly resolves file paths."""
        # Generate timestamp suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        desc_source_name = f"it_desc_src_path_{timestamp}"

        # Get the path to the test descriptor file
        test_dir = Path(__file__).parent.parent  # Go up to adapter directory
        desc_path = test_dir / "test_schema.desc"

        if not desc_path.exists():
            pytest.skip(f"Descriptor file not found at {desc_path}")

        # Copy the descriptor file to the project root so @ syntax can find it
        import shutil

        project_desc_path = Path(project.project_root) / "test_schema.desc"
        shutil.copy2(desc_path, project_desc_path)

        # Test with various path formats
        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    schema: public
    tables:
      - name: {desc_source_name}
        description: "Descriptor source for path resolution test"
        config:
          materialized: descriptor_source
          parameters:
            file: '@test_schema.desc'
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        # The descriptor source should be created successfully since test_schema.desc is now a valid protobuf descriptor file
        # The test verifies that the file path resolution works correctly
        run_dbt(["run-operation", "create_sources"], expect_pass=True)

        # Session-level cleanup will handle resource deletion
