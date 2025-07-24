"""Test the deltastream__get_catalog_relations macro and parallel method"""

import pytest
from unittest.mock import Mock, patch
from dbt.adapters.deltastream.impl import DeltastreamAdapter
from dbt.adapters.deltastream.relation import (
    DeltastreamRelation,
    DeltastreamRelationType,
)
from dbt.adapters.contracts.relation import Path
import agate


@pytest.fixture
def mock_adapter():
    """Create a mock adapter for testing"""
    adapter = Mock(spec=DeltastreamAdapter)
    adapter.get_catalog_relations_parallel = (
        DeltastreamAdapter.get_catalog_relations_parallel.__get__(adapter)
    )

    # Mock dependencies
    adapter.config = Mock()
    adapter.config.threads = 4
    adapter.connections = Mock()
    adapter.logger = Mock()

    return adapter


@pytest.fixture
def sample_relation():
    """Create a sample relation for testing"""
    return DeltastreamRelation(
        Path(database="test_db", schema="test_schema", identifier="test_table"),
        type=DeltastreamRelationType.Table,
    )


class TestCatalogRelations:
    """Test catalog relations functionality"""

    def test_get_catalog_relations_parallel_empty_relations(self, mock_adapter):
        """Test parallel catalog method with empty relations list"""
        # Test with empty relations
        result = mock_adapter.get_catalog_relations_parallel([])

        # Should return empty agate table with correct schema
        assert isinstance(result, agate.Table)
        assert len(result.rows) == 0
        assert len(result.column_names) == 10
        expected_columns = [
            "table_database",
            "table_schema",
            "table_name",
            "table_type",
            "table_comment",
            "column_name",
            "column_index",
            "column_type",
            "column_comment",
            "table_owner",
        ]
        assert list(result.column_names) == expected_columns

    def test_get_catalog_relations_parallel_single_relation(
        self, mock_adapter, sample_relation
    ):
        """Test parallel catalog method with single relation"""
        # Mock query result - simulate DESCRIBE RELATION COLUMNS output
        mock_agate_table = Mock()
        mock_agate_table.rows = [
            ["id", "BIGINT", True, {}],
            ["name", "VARCHAR", False, {}],
        ]

        mock_adapter.connections.query.return_value = (None, mock_agate_table)
        mock_adapter.connections.get_thread_connection.return_value = Mock()

        # Test with single relation
        result = mock_adapter.get_catalog_relations_parallel([sample_relation])

        # Verify query was called with correct SQL
        expected_sql = 'DESCRIBE RELATION COLUMNS "test_db"."test_schema"."test_table";'
        mock_adapter.connections.query.assert_called_once_with(expected_sql)

        # Verify result structure
        assert isinstance(result, agate.Table)
        assert len(result.rows) == 2  # Two columns

        # Check first row
        first_row = result.rows[0]
        assert first_row[0] == "test_db"  # table_database
        assert first_row[1] == "test_schema"  # table_schema
        assert first_row[2] == "test_table"  # table_name
        assert first_row[3] == "TABLE"  # table_type
        assert first_row[5] == "id"  # column_name
        assert first_row[6] == 0  # column_index
        assert first_row[7] == "BIGINT"  # column_type

    def test_get_catalog_relations_parallel_multiple_relations(self, mock_adapter):
        """Test parallel catalog method with multiple relations"""
        # Create multiple relations
        relations = [
            DeltastreamRelation(
                Path(database="db1", schema="schema1", identifier="table1"),
                type=DeltastreamRelationType.Table,
            ),
            DeltastreamRelation(
                Path(database="db2", schema="schema2", identifier="table2"),
                type=DeltastreamRelationType.Table,
            ),
        ]

        # Mock query results for each relation
        mock_agate_table = Mock()
        mock_agate_table.rows = [["col1", "VARCHAR", True, {}]]

        mock_adapter.connections.query.return_value = (None, mock_agate_table)
        mock_adapter.connections.get_thread_connection.return_value = Mock()

        # Test with multiple relations
        result = mock_adapter.get_catalog_relations_parallel(relations)

        # Verify result structure
        assert isinstance(result, agate.Table)
        assert len(result.rows) == 2  # One column per relation
        assert mock_adapter.connections.query.call_count == 2

    def test_get_catalog_relations_parallel_error_handling(
        self, mock_adapter, sample_relation
    ):
        """Test error handling in parallel catalog method"""
        # Mock connection to raise an exception
        mock_adapter.connections.query.side_effect = Exception("Connection error")
        mock_adapter.connections.get_thread_connection.return_value = Mock()

        # Test should not crash and return empty results
        result = mock_adapter.get_catalog_relations_parallel([sample_relation])

        # Should return empty table when all relations fail
        assert isinstance(result, agate.Table)
        assert len(result.rows) == 0

    def test_get_catalog_relations_parallel_thread_configuration(self, mock_adapter):
        """Test that thread configuration is respected"""
        # Test with more relations than threads
        relations = [
            DeltastreamRelation(
                Path(database="db", schema="schema", identifier=f"table{i}"),
                type=DeltastreamRelationType.Table,
            )
            for i in range(10)
        ]

        mock_adapter.config.threads = 3  # Limit threads
        mock_agate_table = Mock()
        mock_agate_table.rows = [["col", "VARCHAR", True, {}]]
        mock_adapter.connections.query.return_value = (None, mock_agate_table)
        mock_adapter.connections.get_thread_connection.return_value = Mock()

        # Test should use limited number of threads
        result = mock_adapter.get_catalog_relations_parallel(relations)

        # All relations should still be processed
        assert isinstance(result, agate.Table)
        assert len(result.rows) == 10
