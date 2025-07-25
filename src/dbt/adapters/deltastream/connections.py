from typing import List, Optional, Tuple, Dict
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
import os

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
        # Check if there are pending files for this query
        conn = self.get_thread_connection()
        pending_files = getattr(conn, "_pending_files", {})

        # Look for file attachments based on the SQL content
        files_to_attach = self._extract_pending_files(sql, pending_files)

        # Use appropriate execution method
        if files_to_attach:
            return self.exec_with_files(sql, files_to_attach)
        else:
            # Check if this is a function creation that might need retry logic
            if self._is_function_creation(sql):
                return self._query_with_function_retry(sql)
            else:
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

    def _is_function_creation(self, sql: str) -> bool:
        """Check if the SQL is a function creation statement"""
        return "CREATE FUNCTION" in sql.upper()

    def _extract_pending_files(
        self, sql: str, pending_files: Dict[str, str]
    ) -> List[str]:
        """
        Extract and return files that should be attached to this SQL statement.

        This method provides a standardized interface for matching pending files
        with SQL statements based on resource type and identifier patterns.
        """
        files_to_attach = []

        # Define mappings between SQL patterns and file key prefixes
        file_mappings = [
            ("CREATE FUNCTION_SOURCE", "function_source_"),
            ("CREATE DESCRIPTOR_SOURCE", "descriptor_source_"),
        ]

        # Check each mapping to see if it matches the current SQL
        for sql_pattern, key_prefix in file_mappings:
            if sql_pattern in sql:
                # Find matching pending files for this resource type
                matching_keys = [
                    key for key in pending_files.keys() if key.startswith(key_prefix)
                ]

                for key in matching_keys:
                    # Extract identifier from the key (e.g., 'function_source_my_func' -> 'my_func')
                    identifier = key.replace(key_prefix, "")

                    # Check if this identifier appears in the SQL statement
                    if f'"{identifier}"' in sql:
                        files_to_attach.append(pending_files[key])
                        # Remove from pending files once used
                        del pending_files[key]
                        break  # Only attach one file per SQL statement

                # If we found a match, don't check other patterns
                if files_to_attach:
                    break

        return files_to_attach

    def _query_with_function_retry(
        self, sql: str, max_wait_seconds: int = 30
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        """Execute function creation SQL with retry logic for function source readiness"""
        import time

        start_time = time.time()
        retry_interval = 2  # Start with 2 second intervals
        max_retry_interval = 10  # Cap at 10 seconds

        while True:
            try:
                return asyncio.run(self.async_query(sql))
            except SQLError as e:
                elapsed_time = time.time() - start_time

                # Check if this is a function source not ready error using SQLState
                if (
                    self._is_function_source_not_ready_error(e)
                    and elapsed_time < max_wait_seconds
                ):
                    logger.info(
                        f"Function source not ready (SQLState: {e.code}), waiting {retry_interval}s before retry... (elapsed: {elapsed_time:.1f}s)"
                    )
                    time.sleep(retry_interval)
                    # Exponential backoff with cap
                    retry_interval = min(retry_interval * 1.5, max_retry_interval)
                    continue
                else:
                    # Re-raise if it's a different error or we've exceeded max wait time
                    if (
                        elapsed_time >= max_wait_seconds
                        and self._is_function_source_not_ready_error(e)
                    ):
                        logger.error(
                            f"Function source still not ready after {max_wait_seconds}s, giving up (SQLState: {e.code})"
                        )
                    raise
            except Exception as e:
                # Handle non-SQLError exceptions (like DbtRuntimeError wrapping SQLError)
                elapsed_time = time.time() - start_time

                # Try to extract SQLError from wrapped exceptions
                if hasattr(e, "__cause__") and isinstance(e.__cause__, SQLError):
                    sql_error = e.__cause__
                    if (
                        self._is_function_source_not_ready_error(sql_error)
                        and elapsed_time < max_wait_seconds
                    ):
                        logger.info(
                            f"Function source not ready (SQLState: {sql_error.code}), waiting {retry_interval}s before retry... (elapsed: {elapsed_time:.1f}s)"
                        )
                        time.sleep(retry_interval)
                        retry_interval = min(retry_interval * 1.5, max_retry_interval)
                        continue
                    elif (
                        elapsed_time >= max_wait_seconds
                        and self._is_function_source_not_ready_error(sql_error)
                    ):
                        logger.error(
                            f"Function source still not ready after {max_wait_seconds}s, giving up (SQLState: {sql_error.code})"
                        )

                # For other exceptions or timeout, re-raise
                raise

    def _is_function_source_not_ready_error(self, sql_error: SQLError) -> bool:
        """Check if the SQLError indicates that a function source is not ready"""
        # SQLState 3D018 is the specific error code for "function source is not ready"
        return sql_error.code == SqlState.SQL_STATE_3D018

    def exec_with_files(
        self, sql: str, files: List[str]
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        """Execute a query with file attachments and return the result as a table"""
        try:
            return asyncio.run(self.async_exec_with_files(sql, files))
        except Exception as e:
            raise DbtRuntimeError(str(e))

    async def async_exec_with_files(
        self, sql: str, files: List[str]
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        """Execute a query with file attachments asynchronously"""
        conn = self.get_thread_connection()
        api: APIConnection = conn.handle
        logger.debug(f"Executing with files: {sql}")
        logger.debug(f"Files: {files}")

        # Validate files exist
        for file_path in files:
            if not os.path.exists(file_path):
                raise DbtRuntimeError(f"File not found: {file_path}")

        # Execute query with files - let connector handle filename and content type
        rows = await api.exec_with_files(sql, files)
        response = AdapterResponse("OK", "OK")

        # Handle the case where there might be no result rows for DDL operations
        if rows is None:
            return response, agate.Table([])

        columns = rows.columns()
        data = []
        async for row in rows:
            data.append(list(row) if row is not None else [])

        # If no columns (DDL operation), return empty table
        if not columns:
            return response, agate.Table([])

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
