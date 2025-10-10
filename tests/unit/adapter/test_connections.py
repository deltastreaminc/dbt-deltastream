import threading  # added import
import pytest
from unittest.mock import Mock, patch
import agate
from dbt.adapters.contracts.connection import Connection, ConnectionState, Credentials
from dbt.adapters.deltastream.connections import DeltastreamConnectionManager
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt_common.exceptions import DbtRuntimeError


@pytest.fixture
def dummy_conn_manager():
    # Use threading as mp_context to supply a valid RLock
    return DeltastreamConnectionManager(profile="dummy_profile", mp_context=threading)


@pytest.fixture
def mock_credentials():
    creds = Mock(spec=Credentials)
    creds.api_key = "test-key"
    creds.endpoint = "http://test-endpoint"
    return creds


@pytest.fixture
def mock_connection(mock_credentials):
    connection = Connection(
        type="deltastream",
        name="test",
        state=ConnectionState.INIT,
        transaction_open=False,
        handle=None,
        credentials=mock_credentials,
    )
    return connection


@pytest.fixture
def mock_api_connection():
    # Replace the previous mock with a dummy async iterable result with proper column names.
    class DummyColumn:
        def __init__(self, name):
            self.name = name

    class DummyRows:
        async def __aiter__(self):
            for row in [[1, "a"], [2, "b"]]:
                yield row

        def columns(self):
            return [DummyColumn("col1"), DummyColumn("col2")]

    async def mock_query(sql):
        return DummyRows()

    mock_api = Mock()
    mock_api.query = mock_query
    return mock_api


class TestDeltastreamConnectionManager:
    def test_open_success(
        self, mock_connection, mock_api_connection, dummy_conn_manager
    ):
        with patch(
            "dbt.adapters.deltastream.connections.create_deltastream_client",
            return_value=mock_api_connection,
        ):
            result = dummy_conn_manager.open(mock_connection)
            assert result.state == ConnectionState.OPEN
            assert result.handle == mock_api_connection

    def test_open_failure(self, mock_connection, dummy_conn_manager):
        with patch(
            "dbt.adapters.deltastream.connections.create_deltastream_client",
            side_effect=Exception("Connection failed"),
        ):
            with pytest.raises(FailedToConnectError, match="Connection failed"):
                dummy_conn_manager.open(mock_connection)
            assert mock_connection.state == ConnectionState.FAIL
            assert mock_connection.handle is None

    def test_close(self, mock_connection, dummy_conn_manager):
        result = dummy_conn_manager.close(mock_connection)
        assert result.state == ConnectionState.CLOSED
        assert result.handle is None

    @pytest.mark.asyncio
    async def test_execute_success(
        self, mock_connection, mock_api_connection, dummy_conn_manager
    ):
        mock_connection.state = ConnectionState.OPEN
        mock_connection.handle = mock_api_connection
        # Override get_thread_connection to return our mock_connection
        dummy_conn_manager.get_thread_connection = lambda: mock_connection

        # Call async_query directly instead of execute to avoid nested event loop error
        response, table = await dummy_conn_manager.async_query("SELECT * FROM test")
        assert isinstance(table, agate.Table)
        assert len(table.rows) == 2
        # Updated to compare list values
        assert list(table.column_names) == ["col1", "col2"]

    def test_execute_failure(self, mock_connection, dummy_conn_manager):
        mock_connection.state = ConnectionState.OPEN
        mock_connection.handle = Mock()
        mock_connection.handle.query.side_effect = Exception("Query failed")
        dummy_conn_manager.get_thread_connection = lambda: mock_connection
        with pytest.raises(DbtRuntimeError, match="Query failed"):
            dummy_conn_manager.execute("SELECT * FROM test")

    def test_transaction_methods(self, mock_connection, dummy_conn_manager):
        # Test that transaction methods don't raise exceptions
        dummy_conn_manager.begin()  # Should do nothing
        dummy_conn_manager.commit()  # Should do nothing
        dummy_conn_manager.clear_transaction()  # Should do nothing
        dummy_conn_manager.add_begin_query()  # Should do nothing
        dummy_conn_manager.add_commit_query()  # Should do nothing

    def test_cancel_open(self, mock_connection, dummy_conn_manager):
        result = dummy_conn_manager.cancel_open()
        assert result is None

    def test_exception_handler(self, mock_connection, dummy_conn_manager):
        with pytest.raises(DbtRuntimeError, match="Test error"):
            with dummy_conn_manager.exception_handler("SELECT * FROM test"):
                raise Exception("Test error")
