"""
Test caching functionality for deltastream adapter.

These tests ensure proper relation caching behavior.
"""

import pytest

from dbt.tests.adapter.caching.test_caching import (
    BaseCachingTest,
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    BaseCachingSelectedSchemaOnly,
)


@pytest.mark.skip(
    reason="DeltaStream has SQL syntax issues with seed loading required for caching tests"
)
class TestCachingDeltastream(BaseCachingTest):
    """Test basic caching functionality."""

    pass


@pytest.mark.skip(
    reason="DeltaStream has SQL syntax issues with seed loading required for caching tests"
)
class TestCachingLowercaseModelDeltastream(BaseCachingLowercaseModel):
    """Test caching with lowercase model names."""

    pass


@pytest.mark.skip(
    reason="DeltaStream has SQL syntax issues with seed loading required for caching tests"
)
class TestCachingUppercaseModelDeltastream(BaseCachingUppercaseModel):
    """Test caching with uppercase model names."""

    pass


@pytest.mark.skip(
    reason="DeltaStream has SQL syntax issues with seed loading required for caching tests"
)
class TestCachingSelectedSchemaOnlyDeltastream(BaseCachingSelectedSchemaOnly):
    """Test caching with specific schema selection."""

    pass
