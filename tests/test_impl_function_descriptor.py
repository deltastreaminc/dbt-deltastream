"""Unit tests for new features in DeltastreamAdapter implementation."""

import os
import pytest
import tempfile
from unittest.mock import Mock, MagicMock, patch
from multiprocessing import get_context

import agate
import dbt_common.exceptions
from deltastream.api.error import SQLError, SqlState
from dbt.adapters.deltastream.impl import DeltastreamAdapter


@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    config = Mock()
    config.credentials = Mock()
    config.credentials.database = "test_db"
    config.credentials.schema = "test_schema"
    config.project_root = "/test/project"
    return config


@pytest.fixture
def adapter(mock_config):
    """Create adapter instance for testing."""
    adapter_instance = DeltastreamAdapter(mock_config, get_context("spawn"))
    adapter_instance.connections = Mock()
    adapter_instance.cache = Mock()
    return adapter_instance


class TestSharedFileSourceCreation:
    """Test the shared _create_source_with_file method."""

    def test_create_source_with_file_function_source(self, adapter):
        """Test creating function source with file attachment."""
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jar') as temp_file:
            temp_file.write(b"test jar content")
            temp_file_path = temp_file.name
        
        try:
            # Mock connection with thread-local storage
            mock_conn = Mock()
            mock_conn._pending_files = {}
            adapter.connections.get_thread_connection.return_value = mock_conn
            
            parameters = {
                'file': temp_file_path,
                'language': 'java',
                'other_param': 'value'
            }
            
            result_sql = adapter._create_source_with_file('function_source', 'test_func', parameters)
            
            # Verify SQL structure
            expected_filename = os.path.basename(temp_file_path)
            assert 'CREATE FUNCTION_SOURCE "test_func"' in result_sql
            assert f"'file' = '{expected_filename}'" in result_sql
            assert "'language' = 'java'" in result_sql
            assert "'other_param' = 'value'" in result_sql
            
            # Verify file was stored in connection
            assert hasattr(mock_conn, '_pending_files')
            assert 'function_source_test_func' in mock_conn._pending_files
            assert mock_conn._pending_files['function_source_test_func'] == temp_file_path
        
        finally:
            os.unlink(temp_file_path)

    def test_create_source_with_file_descriptor_source(self, adapter):
        """Test creating descriptor source with file attachment."""
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.desc') as temp_file:
            temp_file.write(b"test descriptor content")
            temp_file_path = temp_file.name
        
        try:
            # Mock connection
            mock_conn = Mock()
            mock_conn._pending_files = {}
            adapter.connections.get_thread_connection.return_value = mock_conn
            
            parameters = {
                'file': temp_file_path,
                'format': 'protobuf'
            }
            
            result_sql = adapter._create_source_with_file('descriptor_source', 'event_schema', parameters)
            
            # Verify SQL structure
            expected_filename = os.path.basename(temp_file_path)
            assert 'CREATE DESCRIPTOR_SOURCE "event_schema"' in result_sql
            assert f"'file' = '{expected_filename}'" in result_sql
            assert "'format' = 'protobuf'" in result_sql
            
            # Verify file was stored in connection
            assert 'descriptor_source_event_schema' in mock_conn._pending_files
            assert mock_conn._pending_files['descriptor_source_event_schema'] == temp_file_path
        
        finally:
            os.unlink(temp_file_path)

    def test_create_source_with_file_missing_file_param(self, adapter):
        """Test creating source without file parameter raises error."""
        parameters = {'language': 'java'}
        
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="requires a 'file' parameter"):
            adapter._create_source_with_file('function_source', 'test_func', parameters)

    def test_create_function_source_with_file_wrapper(self, adapter):
        """Test the function source wrapper method."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
        
        try:
            mock_conn = Mock()
            mock_conn._pending_files = {}
            adapter.connections.get_thread_connection.return_value = mock_conn
            
            parameters = {'file': temp_file_path}
            
            result = adapter.create_function_source_with_file('test_func', parameters)
            
            assert 'CREATE FUNCTION_SOURCE "test_func"' in result
        
        finally:
            os.unlink(temp_file_path)

    def test_create_descriptor_source_with_file_wrapper(self, adapter):
        """Test the descriptor source wrapper method."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file_path = temp_file.name
        
        try:
            mock_conn = Mock()
            mock_conn._pending_files = {}
            adapter.connections.get_thread_connection.return_value = mock_conn
            
            parameters = {'file': temp_file_path}
            
            result = adapter.create_descriptor_source_with_file('event_schema', parameters)
            
            assert 'CREATE DESCRIPTOR_SOURCE "event_schema"' in result
        
        finally:
            os.unlink(temp_file_path)


class TestFilePathResolution:
    """Test file path resolution functionality."""

    def test_resolve_file_path_absolute(self, adapter):
        """Test resolving absolute file paths."""
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            abs_path = temp_file.name
        
        try:
            result = adapter._resolve_file_path(abs_path)
            assert result == abs_path
        finally:
            os.unlink(abs_path)

    def test_resolve_file_path_relative(self, adapter):
        """Test resolving relative file paths."""
        # Create temp file in project root
        project_root = adapter.config.project_root
        with patch('os.path.join', return_value='/test/project/relative/file.jar'):
            with patch('os.path.exists', return_value=True):
                with patch('os.path.isfile', return_value=True):
                    result = adapter._resolve_file_path('relative/file.jar')
                    assert result == '/test/project/relative/file.jar'

    def test_resolve_file_path_at_syntax(self, adapter):
        """Test resolving file paths with @ syntax."""
        with patch('os.path.join', return_value='/test/project/schemas/file.proto'):
            with patch('os.path.exists', return_value=True):
                with patch('os.path.isfile', return_value=True):
                    result = adapter._resolve_file_path('@/schemas/file.proto')
                    assert result == '/test/project/schemas/file.proto'

    def test_resolve_file_path_at_syntax_no_leading_slash(self, adapter):
        """Test resolving file paths with @ syntax without leading slash."""
        with patch('os.path.join', return_value='/test/project/schemas/file.proto'):
            with patch('os.path.exists', return_value=True):
                with patch('os.path.isfile', return_value=True):
                    result = adapter._resolve_file_path('@schemas/file.proto')
                    assert result == '/test/project/schemas/file.proto'

    def test_resolve_file_path_not_exists(self, adapter):
        """Test resolving file path when file doesn't exist."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="File not found"):
            adapter._resolve_file_path('/nonexistent/file.jar')

    def test_resolve_file_path_is_directory(self, adapter):
        """Test resolving file path when path is a directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="Path is not a file"):
                adapter._resolve_file_path(temp_dir)


class TestBuildWithClause:
    """Test WITH clause building functionality."""

    def test_build_with_clause_empty(self, adapter):
        """Test building WITH clause with empty parameters."""
        result = adapter._build_with_clause({})
        assert result == ""

    def test_build_with_clause_string_values(self, adapter):
        """Test building WITH clause with string values."""
        parameters = {
            'file': 'function.jar',
            'language': 'java',
            'description': 'Test function'
        }
        
        result = adapter._build_with_clause(parameters)
        
        assert result.startswith(" WITH (")
        assert result.endswith(")")
        assert "'file' = 'function.jar'" in result
        assert "'language' = 'java'" in result
        assert "'description' = 'Test function'" in result

    def test_build_with_clause_quote_escaping(self, adapter):
        """Test building WITH clause with quote escaping."""
        parameters = {
            'description': "Test with 'single quotes' inside"
        }
        
        result = adapter._build_with_clause(parameters)
        
        assert "'description' = 'Test with ''single quotes'' inside'" in result

    def test_build_with_clause_non_string_values(self, adapter):
        """Test building WITH clause with non-string values."""
        parameters = {
            'timeout': 30,
            'enabled': True,
            'scale_factor': 1.5
        }
        
        result = adapter._build_with_clause(parameters)
        
        assert "'timeout' = 30" in result
        assert "'enabled' = True" in result
        assert "'scale_factor' = 1.5" in result

    def test_build_with_clause_mixed_values(self, adapter):
        """Test building WITH clause with mixed value types."""
        parameters = {
            'name': 'test',
            'count': 42,
            'active': False
        }
        
        result = adapter._build_with_clause(parameters)
        
        # Should contain all parameters with correct formatting
        assert "'name' = 'test'" in result
        assert "'count' = 42" in result
        assert "'active' = False" in result


class TestQuoteStripping:
    """Test quote stripping functionality."""

    def test_strip_quotes_quoted_identifier(self, adapter):
        """Test stripping quotes from quoted identifier."""
        result = adapter._strip_quotes('"quoted_identifier"')
        assert result == "quoted_identifier"

    def test_strip_quotes_unquoted_identifier(self, adapter):
        """Test handling unquoted identifier."""
        result = adapter._strip_quotes('unquoted_identifier')
        assert result == "unquoted_identifier"

    def test_strip_quotes_empty_string(self, adapter):
        """Test handling empty string."""
        result = adapter._strip_quotes('')
        assert result == ""

    def test_strip_quotes_only_quotes(self, adapter):
        """Test handling string with only quotes."""
        result = adapter._strip_quotes('""')
        assert result == ""

    def test_strip_quotes_partial_quotes(self, adapter):
        """Test handling string with partial quotes (should not strip)."""
        result = adapter._strip_quotes('"partial')
        assert result == '"partial'
        
        result = adapter._strip_quotes('partial"')
        assert result == 'partial"'


class TestResourceDetection:
    """Test resource detection methods with quote handling."""

    def _make_iterable_table(self, rows):
        """Helper to create a mock table that is properly iterable."""
        mock_table = Mock()
        mock_table.__len__ = Mock(return_value=len(rows))
        mock_table.rows = rows
        mock_table.__iter__ = lambda self: iter(rows)
        return mock_table

    def test_get_function_source_exists(self, adapter):
        """Test getting function source that exists."""
        # Create mock rows that simulate quoted names from DeltaStream
        mock_row1 = Mock()
        mock_row1.Name = '"test_function_source"'
        mock_row2 = Mock()
        mock_row2.Name = '"other_source"'
        
        mock_table = self._make_iterable_table([mock_row1, mock_row2])
        adapter.connections.query.return_value = (None, mock_table)
        
        result = adapter.get_function_source('test_function_source')
        
        assert result is not None
        assert result.identifier == 'test_function_source'
        assert result.resource_type == 'function_source'

    def test_get_function_source_not_exists(self, adapter):
        """Test getting function source that doesn't exist."""
        mock_row = Mock()
        mock_row.Name = '"other_source"'
        
        mock_table = self._make_iterable_table([mock_row])
        adapter.connections.query.return_value = (None, mock_table)
        
        result = adapter.get_function_source('nonexistent_source')
        
        assert result is None

    def test_get_function_source_dict_access(self, adapter):
        """Test getting function source with dict-style row access."""
        # Mock row as dict
        mock_row = {'Name': '"test_function_source"'}
        
        mock_table = self._make_iterable_table([mock_row])
        adapter.connections.query.return_value = (None, mock_table)
        
        result = adapter.get_function_source('test_function_source')
        
        assert result is not None
        assert result.identifier == 'test_function_source'

    def test_get_function_source_indexed_access(self, adapter):
        """Test getting function source with indexed row access."""
        # Mock row as list/tuple
        mock_row = ['"test_function_source"']
        
        mock_table = self._make_iterable_table([mock_row])
        adapter.connections.query.return_value = (None, mock_table)

        result = adapter.get_function_source('test_function_source')
        
        assert result is not None
        assert result.identifier == 'test_function_source'

    def test_get_descriptor_source_exists(self, adapter):
        """Test getting descriptor source that exists."""
        mock_row = Mock()
        mock_row.Name = '"event_schema"'
        
        mock_table = self._make_iterable_table([mock_row])
        adapter.connections.query.return_value = (None, mock_table)

        result = adapter.get_descriptor_source('event_schema')
        
        assert result is not None
        assert result.identifier == 'event_schema'
        assert result.resource_type == 'descriptor_source'

    def test_get_descriptor_source_not_exists(self, adapter):
        """Test getting descriptor source that doesn't exist."""
        mock_row = Mock()
        mock_row.Name = '"other_schema"'
        
        mock_table = self._make_iterable_table([mock_row])
        adapter.connections.query.return_value = (None, mock_table)

        result = adapter.get_descriptor_source('nonexistent_schema')
        
        assert result is None

    def test_get_function_exists_with_signature(self, adapter):
        """Test getting function that exists with matching signature."""
        mock_row = Mock()
        mock_row.Signature = 'test_func(input_values ARRAY<DOUBLE>, input_weights ARRAY<DOUBLE>)'
        
        mock_table = self._make_iterable_table([mock_row])
        adapter.connections.query.return_value = (None, mock_table)

        parameters = {
            'args': [
                {'name': 'input_values', 'type': 'ARRAY<DOUBLE>'},
                {'name': 'input_weights', 'type': 'ARRAY<DOUBLE>'}
            ]
        }
        
        result = adapter.get_function('test_func', parameters)
        
        assert result is not None
        assert result.identifier == 'test_func'
        assert result.resource_type == 'function'

    def test_get_function_not_exists_wrong_signature(self, adapter):
        """Test getting function with wrong signature."""
        mock_row = Mock()
        mock_row.Signature = 'test_func(other_param VARCHAR)'
        
        mock_table = self._make_iterable_table([mock_row])
        adapter.connections.query.return_value = (None, mock_table)

        parameters = {
            'args': [
                {'name': 'input_values', 'type': 'ARRAY<DOUBLE>'}
            ]
        }
        
        result = adapter.get_function('test_func', parameters)
        
        assert result is None

    def test_get_function_no_args(self, adapter):
        """Test getting function with no arguments."""
        mock_row = Mock()
        mock_row.Signature = 'test_func()'
        
        mock_table = self._make_iterable_table([mock_row])
        adapter.connections.query.return_value = (None, mock_table)

        parameters = {'args': []}
        
        result = adapter.get_function('test_func', parameters)
        
        assert result is not None

    def test_resource_detection_sql_error_handling(self, adapter):
        """Test resource detection with SQL errors."""
        # Test expected error (invalid relation)
        sql_error = SQLError(
            "Relation not found",
            SqlState.SQL_STATE_INVALID_RELATION,
            "statement_123"
        )
        adapter.connections.query.side_effect = sql_error
        
        result = adapter.get_function_source('test_source')
        assert result is None
        
        # Test unexpected error (should re-raise)
        unexpected_error = SQLError(
            "Unexpected error",
            SqlState.SQL_STATE_3D018,
            "statement_123"
        )
        adapter.connections.query.side_effect = unexpected_error
        
        with pytest.raises(SQLError):
            adapter.get_function_source('test_source')


class TestResourceCreationIntegration:
    """Test integration of all new features."""

    def test_create_deltastream_resource_function_source(self, adapter):
        """Test creating DeltaStream function source resource."""
        parameters = {'file': '/path/to/file.jar', 'language': 'java'}
        
        result = adapter.create_deltastream_resource('function_source', 'test_func', parameters)
        
        assert result is not None
        assert result.identifier == 'test_func'
        assert result.resource_type == 'function_source'
        assert result.parameters == parameters

    def test_create_deltastream_resource_descriptor_source(self, adapter):
        """Test creating DeltaStream descriptor source resource."""
        parameters = {'file': '/path/to/schema.desc', 'format': 'protobuf'}
        
        result = adapter.create_deltastream_resource('descriptor_source', 'event_schema', parameters)
        
        assert result is not None
        assert result.identifier == 'event_schema'
        assert result.resource_type == 'descriptor_source'
        assert result.parameters == parameters

    def test_create_deltastream_resource_unsupported_type(self, adapter):
        """Test creating unsupported resource type."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="Unsupported resource type"):
            adapter.create_deltastream_resource('unsupported_type', 'test_id', {})

    def test_get_resource_function_source(self, adapter):
        """Test getting resource for function source."""
        # Mock get_function_source
        expected_resource = adapter.DeltastreamResource('test_func', 'function_source', {})
        adapter.get_function_source = Mock(return_value=expected_resource)
        
        result = adapter.get_resource('function_source', 'test_func', {})
        
        assert result == expected_resource
        adapter.get_function_source.assert_called_once_with('test_func')

    def test_get_resource_descriptor_source(self, adapter):
        """Test getting resource for descriptor source."""
        # Mock get_descriptor_source
        expected_resource = adapter.DeltastreamResource('event_schema', 'descriptor_source', {})
        adapter.get_descriptor_source = Mock(return_value=expected_resource)
        
        result = adapter.get_resource('descriptor_source', 'event_schema', {})
        
        assert result == expected_resource
        adapter.get_descriptor_source.assert_called_once_with('event_schema')

    def test_get_resource_function(self, adapter):
        """Test getting resource for function."""
        parameters = {'args': []}
        expected_resource = adapter.DeltastreamResource('test_func', 'function', parameters)
        adapter.get_function = Mock(return_value=expected_resource)
        
        result = adapter.get_resource('function', 'test_func', parameters)
        
        assert result == expected_resource
        adapter.get_function.assert_called_once_with('test_func', parameters)

    def test_get_resource_unsupported_type(self, adapter):
        """Test getting resource for unsupported type."""
        with pytest.raises(dbt_common.exceptions.DbtRuntimeError, match="Unsupported resource type"):
            adapter.get_resource('unsupported_type', 'test_id', {}) 