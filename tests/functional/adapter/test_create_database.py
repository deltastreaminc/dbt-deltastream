"""
Test database creation functionality for deltastream adapter.

This test verifies that the adapter can create databases correctly through dbt operations.
"""

import pytest
from datetime import datetime
from dbt.tests.util import run_dbt, write_file


class TestCreateDatabaseDeltastream:
    """Test database creation functionality specific to DeltaStream."""

    @pytest.mark.integration
    def test_create_database_from_source(self, project):
        """Test creating a database from a source configuration."""
        # Generate timestamp suffix for unique database name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[
            :-3
        ]  # microseconds truncated to 3 digits
        db_name = f"it_db_{timestamp}"

        sources_yml = f"""
version: 2

sources:
  - name: integration_tests
    schema: public
    tables:
      - name: {db_name}
        config:
          materialized: database
""".lstrip()

        write_file(sources_yml, project.project_root, "models", "sources.yml")

        # Run the operation to create sources
        run_dbt(["run-operation", "create_sources"], expect_pass=True)

        # Clean up: drop the database created in the test
        try:
            run_dbt(
                [
                    "run-operation",
                    "run_query",
                    "--args",
                    f"{{sql: 'DROP DATABASE {db_name};'}}",
                ],
                expect_pass=True,
            )
        except Exception as e:
            # Log cleanup failure but don't fail the test
            print(f"Warning: Failed to clean up database {db_name}: {e}")
