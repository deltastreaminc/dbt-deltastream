"""
Test constraints functionality for deltastream adapter.

These tests verify constraint handling if supported by the platform.
"""

import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
)


# Only include constraint tests if DeltaStream supports them
# Comment out or skip tests that are not supported
@pytest.mark.skip(reason="DeltaStream constraint support needs to be verified")
class TestTableConstraintsColumnsEqualDeltastream(BaseTableConstraintsColumnsEqual):
    """Test table constraints column equality."""

    pass


@pytest.mark.skip(reason="DeltaStream constraint support needs to be verified")
class TestViewConstraintsColumnsEqualDeltastream(BaseViewConstraintsColumnsEqual):
    """Test view constraints column equality."""

    pass


@pytest.mark.skip(reason="DeltaStream constraint support needs to be verified")
class TestIncrementalConstraintsColumnsEqualDeltastream(
    BaseIncrementalConstraintsColumnsEqual
):
    """Test incremental constraints column equality."""

    pass


@pytest.mark.skip(reason="DeltaStream constraint support needs to be verified")
class TestConstraintsRuntimeDdlEnforcementDeltastream(
    BaseConstraintsRuntimeDdlEnforcement
):
    """Test constraints runtime DDL enforcement."""

    pass


@pytest.mark.skip(reason="DeltaStream constraint support needs to be verified")
class TestConstraintsRollbackDeltastream(BaseConstraintsRollback):
    """Test constraints rollback functionality."""

    pass


@pytest.mark.skip(reason="DeltaStream constraint support needs to be verified")
class TestIncrementalConstraintsRuntimeDdlEnforcementDeltastream(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    """Test incremental constraints runtime DDL enforcement."""

    pass


@pytest.mark.skip(reason="DeltaStream constraint support needs to be verified")
class TestIncrementalConstraintsRollbackDeltastream(BaseIncrementalConstraintsRollback):
    """Test incremental constraints rollback functionality."""

    pass


@pytest.mark.skip(reason="DeltaStream constraint support needs to be verified")
class TestModelConstraintsRuntimeEnforcementDeltastream(
    BaseModelConstraintsRuntimeEnforcement
):
    """Test model constraints runtime enforcement."""

    pass
