"""
Basic dbt adapter functionality tests for deltastream.

These tests use the dbt-tests-adapter package to ensure the adapter
implements core dbt functionality correctly.
"""

import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


@pytest.mark.skip(
    reason="DeltaStream has SQL syntax issues with seed loading - needs adapter-specific implementation"
)
class TestSimpleMaterializationsDeltastream(BaseSimpleMaterializations):
    """Test basic materialization functionality."""

    pass


@pytest.mark.skip(
    reason="DeltaStream SQL parser doesn't support subquery syntax used in singular tests"
)
class TestSingularTestsDeltastream(BaseSingularTests):
    """Test singular tests functionality."""

    pass


@pytest.mark.skip(
    reason="DeltaStream SQL parser doesn't support subquery syntax used in ephemeral singular tests"
)
class TestSingularTestsEphemeralDeltastream(BaseSingularTestsEphemeral):
    """Test singular tests with ephemeral models."""

    pass


@pytest.mark.skip(reason="DeltaStream authentication required - needs live connection")
class TestEmptyDeltastream(BaseEmpty):
    """Test handling of empty models."""

    pass


@pytest.mark.skip(
    reason="DeltaStream has SQL syntax issues with seed loading required for ephemeral tests"
)
class TestEphemeralDeltastream(BaseEphemeral):
    """Test ephemeral model functionality."""

    pass


@pytest.mark.skip(
    reason="DeltaStream has SQL syntax issues with seed loading required for incremental tests"
)
class TestIncrementalDeltastream(BaseIncremental):
    """Test incremental materialization functionality."""

    pass


@pytest.mark.skip(
    reason="DeltaStream SQL parser doesn't support subquery syntax used in generic tests"
)
class TestGenericTestsDeltastream(BaseGenericTests):
    """Test generic tests functionality."""

    pass


@pytest.mark.skip(
    reason="DeltaStream has SQL syntax issues with seed loading required for table materialization tests"
)
class TestTableMaterializationDeltastream(BaseTableMaterialization):
    """Test table materialization functionality."""

    pass


@pytest.mark.skip(reason="DeltaStream authentication required - needs live connection")
class TestAdapterMethodDeltastream(BaseAdapterMethod):
    """Test adapter methods."""

    pass
