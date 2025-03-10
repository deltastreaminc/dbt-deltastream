from typing import List, Optional, Tuple
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    ConnectionState,
)
from dbt.adapters.exceptions.connection import FailedToConnectError

from deltastream.api.conn import APIConnection
from deltastream.api.error import SQLError, SqlState

from .credentials import create_deltastream_client
from contextlib import contextmanager
from dbt_common.exceptions import DbtRuntimeError
import agate
import asyncio

logger = AdapterLogger("deltastream")


class DeltastreamConnectionManager(BaseConnectionManager):
    TYPE = "deltastream"

    # Dict mapping SQL states to whether they should be treated as expected errors
    EXPECTED_SQL_STATES = {
        SqlState.SQL_STATE_INVALID_RELATION: True,  # Invalid relation
        SqlState.SQL_STATE_DUPLICATE_SCHEMA: True,  # Schema already exists
        SqlState.SQL_STATE_DUPLICATE_RELATION: True,  # Table/relation already exists
    }

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        try:
            connection.handle = create_deltastream_client(connection.credentials)
            connection.state = ConnectionState.OPEN
            return connection

        except Exception as e:
            logger.debug(
                f"""Got an error when attempting to create a deltastream client: '{e}'"""
            )
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise FailedToConnectError(str(e))

    @classmethod
    def close(cls, connection):
        """Close the connection (Deltastream is using an API so it's not stateful)"""
        connection.handle = None
        connection.state = ConnectionState.CLOSED

        return connection

    def cancel_open(self) -> Optional[List[str]]:
        # TODO Implement connection cancellation logic
        return None

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        """Execute a query and return the result as a table while the exception is wrapped in a DbtRuntimeError"""
        try:
            return self.query(sql)
        except Exception as e:
            raise DbtRuntimeError(str(e))

    def query(self, sql: str) -> Tuple[AdapterResponse, "agate.Table"]:
        """
        Execute a query and return the result as a table while preserving the original exceptions.
        """
        result = asyncio.run(self.async_query(sql))
        return result

    async def async_query(self, sql: str) -> Tuple[AdapterResponse, "agate.Table"]:
        conn = self.get_thread_connection()
        api: APIConnection = conn.handle
        logger.debug(f"Executing: {sql}")
        rows = await api.query(sql)
        response = AdapterResponse("OK", "OK")
        columns = rows.columns()
        data = []
        async for row in rows:
            data.append(list(row) if row is not None else [])
        table = agate.Table(data, column_names=[col.name for col in columns])
        return response, table

    def cancel(self, connection):
        # TODO Implement connection cancellation logic
        if connection.state == "open":
            connection.state = "closed"

    def add_begin_query(self, *args, **kwargs):
        pass

    def add_commit_query(self, *args, **kwargs):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    def clear_transaction(self):
        pass

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except SQLError as e:
            logger.debug("SQL Error while running:\n{}".format(sql))
            logger.debug(f"SQL State: {e.code}, Message: {str(e)}")
            
            # Check if this is an expected error based on SQL state
            is_expected = self.EXPECTED_SQL_STATES.get(e.code, False)
            if is_expected:
                # Re-raise expected errors to be handled by the adapter
                raise
            else:
                # Wrap unexpected SQL errors
                raise DbtRuntimeError(f"SQL Error ({e.code}): {str(e)}")

        except Exception as e:
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            if isinstance(e, DbtRuntimeError):
                raise
            exc_message = str(e)
            raise DbtRuntimeError(exc_message)
