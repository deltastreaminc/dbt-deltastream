"""Unit tests for new features in DeltastreamConnectionManager."""

import asyncio
import os
import pytest
import tempfile
import time
from unittest.mock import Mock, MagicMock, patch, call
from threading import Thread

import agate
from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.deltastream.connections import DeltastreamConnectionManager
from dbt_common.exceptions import DbtRuntimeError
from deltastream.api.error import SQLError, SqlState


@pytest.fixture
def mock_credentials():
    """Mock credentials for testing."""
    creds = Mock()
    creds.token = "test-token"
    creds.database = "test_db"
    creds.schema = "test_schema"
    creds.url = "https://api.deltastream.io/v2"
    creds.timezone = "UTC"
    creds.session_id = None
    creds.organization_id = "test_org"
    creds.role = None
    creds.store = None
    return creds


@pytest.fixture
def mock_connection(mock_credentials):
    """Mock connection for testing."""
    connection = Connection(
        type="deltastream",
        name="test",
        state=ConnectionState.OPEN,
        transaction_open=False,
        handle=Mock(),
        credentials=mock_credentials,
    )
    return connection


@pytest.fixture
def connection_manager():
    """Create a DeltastreamConnectionManager for testing."""
    import threading
    return DeltastreamConnectionManager(profile="test", mp_context=threading)


class TestFileAttachmentFeatures:
    """Test file attachment functionality."""

    def test_extract_pending_files_function_source(self, connection_manager, mock_connection):
        """Test extracting pending files for function source creation."""
        # Set up pending files on connection
        mock_connection._pending_files = {
            'function_source_my_func': '/path/to/function.jar',
            'descriptor_source_my_desc': '/path/to/descriptor.desc'
        }
        
        sql = 'CREATE FUNCTION_SOURCE "my_func" WITH (\'file\' = \'function.jar\');'
        
        files = connection_manager._extract_pending_files(sql, mock_connection._pending_files)
        
        assert files == ['/path/to/function.jar']
        assert 'function_source_my_func' not in mock_connection._pending_files
        assert 'descriptor_source_my_desc' in mock_connection._pending_files

    def test_extract_pending_files_descriptor_source(self, connection_manager, mock_connection):
        """Test extracting pending files for descriptor source creation."""
        mock_connection._pending_files = {
            'descriptor_source_event_schema': '/path/to/schema.desc'
        }
        
        sql = 'CREATE DESCRIPTOR_SOURCE "event_schema" WITH (\'file\' = \'schema.desc\');'
        
        files = connection_manager._extract_pending_files(sql, mock_connection._pending_files)
        
        assert files == ['/path/to/schema.desc']
        assert 'descriptor_source_event_schema' not in mock_connection._pending_files

    def test_extract_pending_files_no_match(self, connection_manager):
        """Test extracting pending files when no files match."""
        pending_files = {
            'function_source_other_func': '/path/to/other.jar'
        }
        
        sql = 'CREATE STREAM "my_stream";'
        
        files = connection_manager._extract_pending_files(sql, pending_files)
        
        assert files == []
        assert 'function_source_other_func' in pending_files

    def test_extract_pending_files_no_pending_files(self, connection_manager):
        """Test extracting pending files when no pending files exist."""
        pending_files = {}
        
        sql = 'CREATE FUNCTION_SOURCE "my_func" WITH (\'file\' = \'function.jar\');'
        
        files = connection_manager._extract_pending_files(sql, pending_files)
        
        assert files == []

    def test_async_exec_with_files_success(self, connection_manager, mock_connection):
        """Test successful execution with file attachments."""
        # This test is simplified since mocking complex async iterators is challenging
        # The functionality is tested through the integration tests
        
        # Create temporary file for testing
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"test content")
            temp_file_path = temp_file.name
        
        try:
            # Mock the API connection
            mock_api = Mock()
            
            # Create simple async mock that returns None (DDL operation)
            async def mock_exec_with_files(sql, files):
                return None
            
            mock_api.exec_with_files = mock_exec_with_files
            mock_connection.handle = mock_api
            
            # Mock get_thread_connection
            connection_manager.get_thread_connection = Mock(return_value=mock_connection)
            
            sql = 'CREATE FUNCTION_SOURCE "test_func";'
            files = [temp_file_path]
            
            # Call the method - should not raise exception
            import asyncio
            response, table = asyncio.run(connection_manager.async_exec_with_files(sql, files))
            
            assert response.code == "OK"
            assert isinstance(table, agate.Table)
            assert len(table.rows) == 0  # DDL operation returns empty table
        
        finally:
            # Clean up temporary file
            os.unlink(temp_file_path)

    def test_exec_with_files_file_not_found(self, connection_manager, mock_connection):
        """Test execution with file attachments when file doesn't exist."""
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        sql = 'CREATE FUNCTION_SOURCE "test_func";'
        files = ['/nonexistent/file.jar']
        
        with pytest.raises(DbtRuntimeError, match="File not found"):
            connection_manager.exec_with_files(sql, files)

    @pytest.mark.asyncio
    async def test_async_exec_with_files_no_columns(self, connection_manager, mock_connection):
        """Test execution with file attachments when no columns are returned (DDL)."""
        # Mock the API connection
        mock_api = Mock()
        
        # Create an async mock that returns None
        async def mock_exec_with_files(sql, files):
            return None
        
        mock_api.exec_with_files = mock_exec_with_files
        mock_connection.handle = mock_api
        
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        sql = 'CREATE FUNCTION_SOURCE "test_func";'
        files = ['/path/to/file.jar']
        
        # Mock file existence
        with patch('os.path.exists', return_value=True):
            response, table = await connection_manager.async_exec_with_files(sql, files)
        
        assert response.code == "OK"
        assert isinstance(table, agate.Table)
        assert len(table.rows) == 0

    def test_query_with_file_attachment(self, connection_manager, mock_connection):
        """Test query method with file attachment integration."""
        mock_connection._pending_files = {
            'function_source_test_func': '/path/to/function.jar'
        }
        
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        sql = 'CREATE FUNCTION_SOURCE "test_func" WITH (\'file\' = \'function.jar\');'
        
        # Mock exec_with_files to avoid file system operations
        with patch.object(connection_manager, 'exec_with_files') as mock_exec:
            mock_exec.return_value = (Mock(code="OK"), agate.Table([]))
            
            connection_manager.query(sql)
            
            mock_exec.assert_called_once_with(sql, ['/path/to/function.jar'])


class TestRetryLogicFeatures:
    """Test retry logic functionality."""

    def test_is_function_creation_true(self, connection_manager):
        """Test detection of function creation SQL."""
        sql_statements = [
            'CREATE FUNCTION my_func(...)',
            'create function test_func(...)',
            'CREATE FUNCTION "quoted_func"(...)'
        ]
        
        for sql in sql_statements:
            assert connection_manager._is_function_creation(sql) is True

    def test_is_function_creation_false(self, connection_manager):
        """Test detection of non-function creation SQL."""
        sql_statements = [
            'CREATE STREAM my_stream',
            'SELECT * FROM functions',
            'DROP FUNCTION my_func'
        ]
        
        for sql in sql_statements:
            assert connection_manager._is_function_creation(sql) is False

    def test_is_function_source_not_ready_error_true(self, connection_manager):
        """Test detection of function source not ready error."""
        sql_error = SQLError(
            "Function source is not ready",
            SqlState.SQL_STATE_3D018,
            "statement_123"
        )
        
        assert connection_manager._is_function_source_not_ready_error(sql_error) is True

    def test_is_function_source_not_ready_error_false(self, connection_manager):
        """Test detection of other SQL errors."""
        sql_error = SQLError(
            "Relation not found",
            SqlState.SQL_STATE_INVALID_RELATION,
            "statement_123"
        )
        
        assert connection_manager._is_function_source_not_ready_error(sql_error) is False

    def test_query_with_function_retry_success_immediate(self, connection_manager, mock_connection):
        """Test function creation that succeeds immediately."""
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        sql = 'CREATE FUNCTION test_func(...)'
        
        # Mock async_query to succeed immediately
        with patch.object(connection_manager, 'async_query') as mock_async:
            mock_response = (Mock(code="OK"), agate.Table([]))
            mock_async.return_value = mock_response
            
            with patch('asyncio.run', return_value=mock_response):
                result = connection_manager._query_with_function_retry(sql)
            
            assert result == mock_response

    def test_query_with_function_retry_success_after_retries(self, connection_manager, mock_connection):
        """Test function creation that succeeds after retries."""
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        sql = 'CREATE FUNCTION test_func(...)'
        
        # Create SQLError with function source not ready
        not_ready_error = SQLError(
            "Function source is not ready",
            SqlState.SQL_STATE_3D018,
            "statement_123"
        )
        
        call_count = 0
        def mock_async_query(sql):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:  # Fail first 2 times
                raise not_ready_error
            return (Mock(code="OK"), agate.Table([]))  # Succeed on 3rd try
        
        with patch.object(connection_manager, 'async_query', side_effect=mock_async_query):
            with patch('asyncio.run', side_effect=lambda coro: mock_async_query(sql)):
                with patch('time.sleep'):  # Mock sleep to speed up test
                    result = connection_manager._query_with_function_retry(sql, max_wait_seconds=30)
        
        assert result[0].code == "OK"
        assert call_count == 3

    def test_query_with_function_retry_timeout(self, connection_manager, mock_connection):
        """Test function creation that times out."""
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        sql = 'CREATE FUNCTION test_func(...)'
        
        # Create SQLError with function source not ready
        not_ready_error = SQLError(
            "Function source is not ready",
            SqlState.SQL_STATE_3D018,
            "statement_123"
        )
        
        def mock_async_query(sql):
            raise not_ready_error
        
        # Mock time.time to return increasing values that exceed max_wait_seconds
        current_time = 0
        def mock_time():
            nonlocal current_time
            if current_time == 0:
                current_time = 35  # First call gets 0, subsequent calls get 35 (elapsed > 30)
                return 0
            return 35
        
        with patch.object(connection_manager, 'async_query', side_effect=mock_async_query):
            with patch('asyncio.run', side_effect=lambda coro: mock_async_query(sql)):
                with patch('time.sleep'):  # Mock sleep to speed up test
                    with patch('time.time', side_effect=mock_time):  # Mock elapsed time > max_wait
                        with pytest.raises(SQLError):
                            connection_manager._query_with_function_retry(sql, max_wait_seconds=30)

    def test_query_with_function_retry_other_error(self, connection_manager, mock_connection):
        """Test function creation with non-retry error."""
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        sql = 'CREATE FUNCTION test_func(...)'
        
        # Create different SQLError
        other_error = SQLError(
            "Invalid relation",
            SqlState.SQL_STATE_INVALID_RELATION,
            "statement_123"
        )
        
        def mock_async_query(sql):
            raise other_error
        
        with patch.object(connection_manager, 'async_query', side_effect=mock_async_query):
            with patch('asyncio.run', side_effect=lambda coro: mock_async_query(sql)):
                with pytest.raises(SQLError):
                    connection_manager._query_with_function_retry(sql)

    def test_query_with_function_retry_wrapped_exception(self, connection_manager, mock_connection):
        """Test function creation with wrapped SQLError exception."""
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        sql = 'CREATE FUNCTION test_func(...)'
        
        # Create SQLError with function source not ready
        not_ready_error = SQLError(
            "Function source is not ready",
            SqlState.SQL_STATE_3D018,
            "statement_123"
        )
        
        # Wrap the SQLError in a DbtRuntimeError
        wrapped_error = DbtRuntimeError("Wrapped error")
        wrapped_error.__cause__ = not_ready_error
        
        call_count = 0
        def mock_async_query(sql):
            nonlocal call_count
            call_count += 1
            if call_count <= 1:  # Fail first time with wrapped error
                raise wrapped_error
            return (Mock(code="OK"), agate.Table([]))  # Succeed on 2nd try
        
        with patch.object(connection_manager, 'async_query', side_effect=mock_async_query):
            with patch('asyncio.run', side_effect=lambda coro: mock_async_query(sql)):
                with patch('time.sleep'):  # Mock sleep to speed up test
                    result = connection_manager._query_with_function_retry(sql, max_wait_seconds=30)
        
        assert result[0].code == "OK"
        assert call_count == 2

    def test_query_function_creation_calls_retry(self, connection_manager, mock_connection):
        """Test that query method calls retry logic for function creation."""
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        mock_connection._pending_files = {}
        
        sql = 'CREATE FUNCTION test_func(...)'
        
        with patch.object(connection_manager, '_query_with_function_retry') as mock_retry:
            mock_retry.return_value = (Mock(code="OK"), agate.Table([]))
            
            connection_manager.query(sql)
            
            mock_retry.assert_called_once_with(sql)

    def test_query_non_function_creation_no_retry(self, connection_manager, mock_connection):
        """Test that query method doesn't call retry logic for non-function creation."""
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        mock_connection._pending_files = {}
        
        sql = 'CREATE STREAM test_stream'
        
        with patch.object(connection_manager, '_query_with_function_retry') as mock_retry:
            with patch('asyncio.run') as mock_async_run:
                mock_async_run.return_value = (Mock(code="OK"), agate.Table([]))
                
                connection_manager.query(sql)
                
                mock_retry.assert_not_called()
                mock_async_run.assert_called_once()


class TestIntegrationFeatures:
    """Test integration of file attachment and retry features."""

    def test_query_with_both_features(self, connection_manager, mock_connection):
        """Test query with both file attachment and function creation (should prioritize file attachment)."""
        mock_connection._pending_files = {
            'function_source_test_func': '/path/to/function.jar'
        }
        
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        # SQL that would normally trigger both file attachment and retry logic
        sql = 'CREATE FUNCTION_SOURCE "test_func" WITH (\'file\' = \'function.jar\');'
        
        with patch.object(connection_manager, 'exec_with_files') as mock_exec_files:
            with patch.object(connection_manager, '_query_with_function_retry') as mock_retry:
                mock_exec_files.return_value = (Mock(code="OK"), agate.Table([]))
                
                connection_manager.query(sql)
                
                # Should use file attachment, not retry logic
                mock_exec_files.assert_called_once()
                mock_retry.assert_not_called()

    def test_query_priority_order(self, connection_manager, mock_connection):
        """Test that query method follows correct priority: files > function retry > normal execution."""
        connection_manager.get_thread_connection = Mock(return_value=mock_connection)
        
        # Test normal execution (no files, no function creation)
        mock_connection._pending_files = {}
        sql_normal = 'CREATE STREAM test_stream'
        
        with patch('asyncio.run') as mock_async_run:
            mock_async_run.return_value = (Mock(code="OK"), agate.Table([]))
            connection_manager.query(sql_normal)
            mock_async_run.assert_called_once()
        
        # Test function creation (no files, function creation)
        sql_function = 'CREATE FUNCTION test_func(...)'
        
        with patch.object(connection_manager, '_query_with_function_retry') as mock_retry:
            mock_retry.return_value = (Mock(code="OK"), agate.Table([]))
            connection_manager.query(sql_function)
            mock_retry.assert_called_once()
        
        # Test file attachment (files present, takes priority)
        mock_connection._pending_files = {'function_source_test': '/path/file.jar'}
        sql_files = 'CREATE FUNCTION_SOURCE "test" WITH (\'file\' = \'file.jar\');'
        
        with patch.object(connection_manager, 'exec_with_files') as mock_exec_files:
            mock_exec_files.return_value = (Mock(code="OK"), agate.Table([]))
            connection_manager.query(sql_files)
            mock_exec_files.assert_called_once() 