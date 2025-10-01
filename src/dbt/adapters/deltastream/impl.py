from dataclasses import dataclass
from dbt.adapters.contracts.relation import Path
from dbt.adapters.events.logging import AdapterLogger
from typing import Any, Dict, List, Optional
import concurrent.futures

import dbt_common.exceptions
from dbt_common.contracts.constraints import (
    ConstraintType,
)
from multiprocessing.context import SpawnContext
from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from deltastream.api.error import SQLError
from dbt.adapters.deltastream.connections import DeltastreamConnectionManager
from dbt.adapters.deltastream.relation import (
    DeltastreamRelation,
    DeltastreamRelationType,
)
from dbt.adapters.base import (
    BaseAdapter,
    BaseRelation,
)
from dbt.adapters.deltastream.column import DeltastreamColumn
import agate
from deltastream.api.error import SqlState
import os

logger = AdapterLogger("Deltastream")


@dataclass
class DeltastreamConfig(AdapterConfig):
    partition_by: Optional[Dict[str, Any]] = None


class DeltastreamAdapter(BaseAdapter):
    Relation = DeltastreamRelation
    Column = DeltastreamColumn
    ConnectionManager = DeltastreamConnectionManager

    AdapterSpecificConfigs = DeltastreamConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    _capabilities = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(
                support=Support.NotImplemented
            ),
            Capability.TableLastModifiedMetadata: CapabilitySupport(
                support=Support.NotImplemented
            ),
        }
    )

    def __init__(self, config, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)
        self.connections: DeltastreamConnectionManager = self.connections

    @classmethod
    def is_cancelable(cls) -> bool:
        return False  # TODO implement DeltaStream query cancellation

    def drop_relation(self, relation: DeltastreamRelation) -> None:
        is_cached = self._schema_is_cached(relation.database, relation.schema or "")
        if is_cached:
            self.cache_dropped(relation)

        table_ref = self.get_fully_qualified_relation_str(relation)
        try:
            self.connections.query(f"DROP RELATION {table_ref};")
        except SQLError as e:
            raise dbt_common.exceptions.DbtDatabaseError(
                f"Error dropping relation {relation}: {str(e)}"
            )

    def truncate_relation(self, relation: DeltastreamRelation) -> None:
        """Truncate a relation in DeltaStream"""
        table_ref = self.get_fully_qualified_relation_str(relation)
        try:
            self.connections.query(f"TRUNCATE RELATION {table_ref};")
        except SQLError as e:
            raise dbt_common.exceptions.DbtDatabaseError(
                f"Error truncating relation {relation}: {str(e)}"
            )

    @available
    def rename_catalog_columns(self, table: agate.Table):
        mapping = {
            "database_name": "table_database",
            "schema_name": "table_schema",
            "name": "table_name",
            "relation_type": "table_type",
            "primary_key": "primary_key",
            "owner": "table_owner",
        }
        renamed = table.rename(mapping)
        # Build base rows with empty table_comment inserted between primary_key and table_owner
        # original row order: [table_database, table_schema, table_name, table_type, primary_key, table_owner]
        # New row will have: table_database, table_schema, table_name, table_type, primary_key,
        # empty table_comment, empty column_name, column_index, empty column_type, empty column_comment, table_owner
        new_rows = []
        for idx, row in enumerate(renamed.rows):
            new_row = [
                row[0],  # table_database
                row[1],  # table_schema
                row[2],  # table_name
                row[3],  # table_type
                row[4],  # primary_key
                "",  # table_comment
                "",  # column_name
                idx,  # column_index
                "",  # column_type
                "",  # column_comment
                row[5],  # table_owner
            ]
            new_rows.append(new_row)
        new_columns = [
            "table_database",
            "table_schema",
            "table_name",
            "table_type",
            "primary_key",
            "table_comment",
            "column_name",
            "column_index",
            "column_type",
            "column_comment",
            "table_owner",
        ]
        # Set column types: reuse existing types for positions 0-4 and 10; use agate.Text for empty fields; agate.Number for index.
        existing_types = (
            list(renamed.column_types)
            if hasattr(renamed, "column_types")
            else [agate.Text()] * len(renamed.column_names)
        )
        new_types = [
            existing_types[0],  # table_database
            existing_types[1],  # table_schema
            existing_types[2],  # table_name
            existing_types[3],  # table_type
            existing_types[4],  # primary_key
            agate.Text(cast_nulls=False),  # table_comment
            agate.Text(cast_nulls=False),  # column_name
            agate.Number(),  # column_index
            agate.Text(cast_nulls=False),  # column_type
            agate.Text(cast_nulls=False),  # column_comment
            (
                existing_types[5] if len(existing_types) > 5 else agate.Text()
            ),  # table_owner
        ]
        return agate.Table(new_rows, new_columns, column_types=new_types)

    def rename_relation(
        self, from_relation: DeltastreamRelation, to_relation: DeltastreamRelation
    ) -> None:
        """Rename a relation in DeltaStream"""
        raise dbt_common.exceptions.DbtRuntimeError(
            "Renaming is not currently supported in DeltaStream"
        )

    def list_relations_without_caching(
        self, schema_relation: BaseRelation
    ) -> List[BaseRelation]:
        """Return a list of relations in the schema without using the cache"""
        try:
            (_, agate_table) = self.connections.query(
                'SHOW RELATIONS IN SCHEMA "{}"."{}";'.format(
                    schema_relation.database, schema_relation.schema
                )
            )
            relations = [
                DeltastreamRelation(
                    Path(
                        database=schema_relation.database,
                        schema=schema_relation.schema,
                        identifier=self._strip_quotes(row[0]),
                    ),
                    type=DeltastreamRelationType.Table,
                )
                for row in agate_table.rows
            ]
            logger.debug(f"Found relations: {relations}")
            return relations  # type: ignore
        except Exception as e:
            logger.error(f"Error listing relations: {str(e)}")
            return []

    def get_columns_in_relation(
        self, relation: DeltastreamRelation
    ) -> List[DeltastreamColumn]:
        """Get the column definitions for a relation"""
        try:
            (_, agate_table) = self.connections.query(
                'DESCRIBE RELATION COLUMNS "{}"."{}"."{}";'.format(
                    relation.database, relation.schema, relation.identifier
                )
            )
            columns = []
            for row in agate_table.rows:
                column_info = DeltastreamColumn(
                    column=row[0],  # column name
                    dtype=row[1],  # data type
                    mode="NULLABLE" if row[2] else "REQUIRED",  # mode (nullable or not)
                )
                columns.append(column_info)
            return columns
        except Exception as e:
            logger.error(f"get_columns_in_relation error: {str(e)}")
            return []

    def debug_query(self) -> None:
        self.execute("CAN I CREATE_QUERY;")

    def expand_column_types(
        self,
        goal: BaseRelation,
        current: BaseRelation,
    ) -> None:
        """No type expansion is needed in DeltaStream"""
        pass

    def expand_target_column_types(
        self, from_relation: DeltastreamRelation, to_relation: DeltastreamRelation
    ) -> None:
        # This is a no-op on Deltastream
        pass

    @staticmethod
    def _strip_quotes(identifier: str) -> str:
        if identifier.startswith('"') and identifier.endswith('"'):
            identifier = identifier[1:-1]
        return identifier

    def get_relation(
        self, database: str, schema: str, identifier: str
    ) -> Optional[DeltastreamRelation]:
        if self._schema_is_cached(database, schema):
            return super().get_relation(
                database=database, schema=schema, identifier=identifier
            )

        try:
            (response, table) = self.connections.query(
                sql='DESCRIBE RELATION "{}"."{}"."{}";'.format(
                    database, schema, identifier
                )
            )
            if response is None or getattr(response, "code", None) == "OK":
                return DeltastreamRelation(
                    Path(database=database, schema=schema, identifier=identifier),
                    type=DeltastreamRelationType.Table,  # TODO expect that we can retrieve the type in the future
                )
            else:
                return None
        except SQLError as e:
            # Handle expected SQL states that indicate relation does not exist
            if e.code in [
                SqlState.SQL_STATE_INVALID_RELATION,
                SqlState.SQL_STATE_INVALID_SCHEMA,
            ]:
                return None
            raise

    class DeltastreamResource:
        """A class representing a Deltastream resource (e.g., compute pool, store, entity)"""

        identifier: str
        resource_type: str
        parameters: Dict[str, Any]

        def __init__(
            self, identifier: str, resource_type: str, parameters: Dict[str, Any]
        ):
            self.identifier = identifier
            self.resource_type = resource_type
            self.parameters = parameters

    @available
    def create_deltastream_resource(
        self, resource_type: str, identifier: str, parameters: Dict[str, Any]
    ) -> Optional["DeltastreamResource"]:
        """Create a DeltaStream resource (e.g., compute pool, store, entity, function, function_source, descriptor_source, schema_registry)"""
        try:
            if resource_type in [
                "compute_pool",
                "entity",
                "store",
                "function",
                "function_source",
                "descriptor_source",
                "schema_registry",
            ]:
                return self.DeltastreamResource(identifier, resource_type, parameters)
            else:
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"Unsupported resource type: {resource_type}"
                )
        except SQLError as e:
            raise dbt_common.exceptions.DbtDatabaseError(
                f"Error creating {resource_type} {identifier}: {str(e)}"
            )

    def _create_source_with_file(
        self, resource_type: str, identifier: str, parameters: Dict[str, Any]
    ) -> str:
        """Generic method to create a source with file attachment"""
        try:
            file_path = parameters.get("file")
            if not file_path:
                raise dbt_common.exceptions.DbtRuntimeError(
                    f"{resource_type.title()} {identifier} requires a 'file' parameter"
                )

            # Resolve file path (support both absolute and relative paths)
            resolved_path = self._resolve_file_path(file_path)

            # Store the resolved path for later use
            # We'll use the connection's thread-local storage to pass the file path
            conn = self.connections.get_thread_connection()
            if not hasattr(conn, "_pending_files"):
                conn._pending_files = {}
            conn._pending_files[f"{resource_type}_{identifier}"] = resolved_path

            # Build the SQL statement - use placeholder for file parameter since actual file is attached
            sql_parameters = parameters.copy()
            # Use a placeholder file name that indicates the file is attached
            file_name = os.path.basename(resolved_path)
            sql_parameters["file"] = file_name
            with_clause = self._build_with_clause(sql_parameters)

            # Generate appropriate CREATE statement based on resource type
            resource_type_upper = resource_type.upper()
            sql = f'CREATE {resource_type_upper} "{identifier}"{with_clause};'

            return sql

        except SQLError as e:
            raise dbt_common.exceptions.DbtDatabaseError(
                f"Error creating {resource_type} {identifier}: {str(e)}"
            )

    @available
    def create_function_source_with_file(
        self, identifier: str, parameters: Dict[str, Any]
    ) -> str:
        """Create a function source with file attachment"""
        return self._create_source_with_file("function_source", identifier, parameters)

    @available
    def create_descriptor_source_with_file(
        self, identifier: str, parameters: Dict[str, Any]
    ) -> str:
        """Create a descriptor source with file attachment"""
        return self._create_source_with_file(
            "descriptor_source", identifier, parameters
        )

    def _resolve_file_path(self, file_path: str) -> str:
        """Resolve file path, supporting both absolute and relative paths"""
        # Handle special @ syntax (e.g., @/schemas/file.proto)
        if file_path.startswith("@"):
            # For @ syntax, treat as relative path from project root without the @
            clean_path = file_path[1:]  # Remove the @
            project_root = getattr(self.config, "project_root", os.getcwd())
            resolved_path = os.path.join(project_root, clean_path.lstrip("/"))
        # Handle absolute paths
        elif os.path.isabs(file_path):
            resolved_path = file_path
        else:
            # Handle relative paths - resolve relative to project root
            project_root = getattr(self.config, "project_root", os.getcwd())
            resolved_path = os.path.join(project_root, file_path)

        # Validate that the file exists
        if not os.path.exists(resolved_path):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"File not found: {resolved_path} (original path: {file_path})"
            )

        # Validate that it's actually a file (not a directory)
        if not os.path.isfile(resolved_path):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Path is not a file: {resolved_path} (original path: {file_path})"
            )

        return resolved_path

    def _build_with_clause(self, parameters: Dict[str, Any]) -> str:
        """Build WITH clause for SQL statements"""
        if not parameters:
            return ""

        param_parts = []
        for key, value in parameters.items():
            if isinstance(value, str):
                # Escape single quotes in the value by doubling them
                escaped_value = value.replace("'", "''")
                param_parts.append(f"'{key}' = '{escaped_value}'")
            else:
                param_parts.append(f"'{key}' = {value}")

        return f" WITH ({', '.join(param_parts)})" if param_parts else ""

    @available
    def get_resource(
        self, resource_type: str, identifier: str, parameters: Dict[str, Any]
    ) -> Optional["DeltastreamResource"]:
        """Get a resource configuration if it exists"""
        if resource_type == "compute_pool":
            return self.get_compute_pool(identifier)
        elif resource_type == "store":
            return self.get_store(identifier)
        elif resource_type == "entity":
            store = parameters.get("store", None)
            return self.get_entity(identifier, store)
        elif resource_type == "function":
            return self.get_function(identifier, parameters)
        elif resource_type == "function_source":
            return self.get_function_source(identifier)
        elif resource_type == "descriptor_source":
            return self.get_descriptor_source(identifier)
        elif resource_type == "schema_registry":
            return self.get_schema_registry(identifier)
        else:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Unsupported resource type: {resource_type}"
            )

    @available
    def get_compute_pool(self, identifier: str) -> Optional["DeltastreamResource"]:
        """Get a compute pool configuration if it exists"""
        try:
            # List all compute pools and check if the requested one exists
            # DESCRIBE COMPUTE_POOL doesn't exist so we need to list compute pools and check if there's the one we look for that exists
            (_, table) = self.connections.query("LIST COMPUTE_POOLS;")
            if table and len(table) > 0:
                # Extract names from the result and check if our identifier exists
                compute_pool_names = [row["Name"] for row in table]
                if identifier in compute_pool_names:
                    return self.DeltastreamResource(identifier, "compute_pool", {})
            return None
        except SQLError as e:
            if e.code == SqlState.SQL_STATE_INVALID_RELATION:
                return None
            raise

    @available
    def get_store(self, identifier: str) -> Optional["DeltastreamResource"]:
        """Get a store configuration if it exists"""
        try:
            (_, table) = self.connections.query(f'DESCRIBE STORE "{identifier}";')
            if table and len(table) > 0:
                return self.DeltastreamResource(identifier, "store", {})
            return None
        except SQLError as e:
            if e.code in [
                SqlState.SQL_STATE_INVALID_RELATION,
                SqlState.SQL_STATE_INVALID_STORE,
            ]:  # store not found
                return None
            raise

    @available
    def get_entity(
        self, identifier: str, store: Optional[str] = None
    ) -> Optional["DeltastreamResource"]:
        """Get an entity configuration if it exists"""
        try:
            if store:
                sql = f'DESCRIBE ENTITY "{identifier}" IN STORE "{store}";'
            else:
                sql = f'DESCRIBE ENTITY "{identifier}";'
            (_, table) = self.connections.query(sql)
            if table and len(table) > 0:
                parameters = {"store": store} if store else {}
                return self.DeltastreamResource(identifier, "entity", parameters)
            return None
        except SQLError as e:
            if e.code in [
                SqlState.SQL_STATE_INVALID_RELATION,
                SqlState.SQL_STATE_INVALID_PARAMETER,
            ]:  # entity/topic not found
                return None
            raise

    @available
    def get_function(
        self, identifier: str, parameters: Dict[str, Any]
    ) -> Optional["DeltastreamResource"]:
        """Get a function configuration if it exists"""
        try:
            # List all functions and check if the requested one exists
            # We need to check by function signature since functions can be overloaded
            (_, table) = self.connections.query("LIST FUNCTIONS;")
            if table and len(table) > 0:
                # Build the function signature to match
                args = parameters.get("args", [])
                if args:
                    # For signature matching, we need both arg names and types
                    arg_signature_parts = []
                    for arg in args:
                        arg_name = arg.get("name", "arg")
                        arg_type = arg.get("type", "VARCHAR")
                        arg_signature_parts.append(f"{arg_name} {arg_type}")
                    signature = f"{identifier}({', '.join(arg_signature_parts)})"
                else:
                    signature = f"{identifier}()"

                # Check if our function signature exists in the list
                for row in table:
                    if hasattr(row, "Signature") and row.Signature.startswith(
                        signature
                    ):
                        return self.DeltastreamResource(
                            identifier, "function", parameters
                        )
                    # Handle case where the row is a dict
                    elif (
                        isinstance(row, dict)
                        and "Signature" in row
                        and row["Signature"].startswith(signature)
                    ):
                        return self.DeltastreamResource(
                            identifier, "function", parameters
                        )
            return None
        except SQLError as e:
            if e.code == SqlState.SQL_STATE_INVALID_RELATION:
                return None
            raise

    @available
    def get_function_source(self, identifier: str) -> Optional["DeltastreamResource"]:
        """Get a function source configuration if it exists"""
        try:
            # List all function sources and check if the requested one exists
            (_, table) = self.connections.query("LIST FUNCTION_SOURCES;")
            logger.debug(
                f"LIST FUNCTION_SOURCES returned {len(table) if table else 0} rows"
            )
            if table and len(table) > 0:
                # Extract function source names
                all_names = []
                for row in table:
                    if hasattr(row, "Name"):
                        all_names.append(row.Name)
                    elif isinstance(row, dict) and "Name" in row:
                        all_names.append(row["Name"])
                    elif hasattr(row, "__getitem__"):
                        try:
                            name = row[0]  # Try first column
                            all_names.append(name)
                        except (IndexError, KeyError):
                            continue

                # Check if our function source exists in the list
                for row in table:
                    row_name = None
                    if hasattr(row, "Name"):
                        row_name = row.Name
                    elif isinstance(row, dict) and "Name" in row:
                        row_name = row["Name"]
                    elif hasattr(row, "__getitem__"):
                        try:
                            row_name = row[0]  # Try first column
                        except (IndexError, KeyError):
                            continue

                    if row_name:
                        # Strip quotes from the name for comparison
                        clean_name = self._strip_quotes(row_name)
                        if clean_name == identifier:
                            return self.DeltastreamResource(
                                identifier, "function_source", {}
                            )
            return None
        except SQLError as e:
            if e.code == SqlState.SQL_STATE_INVALID_RELATION:
                return None
            raise

    @available
    def get_descriptor_source(self, identifier: str) -> Optional["DeltastreamResource"]:
        """Get a descriptor source configuration if it exists"""
        try:
            # List all descriptor sources and check if the requested one exists
            (_, table) = self.connections.query("LIST DESCRIPTOR_SOURCES;")
            if table and len(table) > 0:
                # Check if our descriptor source exists in the list
                for row in table:
                    row_name = None
                    if hasattr(row, "Name"):
                        row_name = row.Name
                    elif isinstance(row, dict) and "Name" in row:
                        row_name = row["Name"]
                    elif hasattr(row, "__getitem__"):
                        try:
                            row_name = row[0]  # Try first column
                        except (IndexError, KeyError):
                            continue

                    if row_name:
                        # Strip quotes from the name for comparison
                        clean_name = self._strip_quotes(row_name)
                        if clean_name == identifier:
                            return self.DeltastreamResource(
                                identifier, "descriptor_source", {}
                            )
            return None
        except SQLError as e:
            if e.code == SqlState.SQL_STATE_INVALID_RELATION:
                return None
            raise

    @available
    def get_schema_registry(self, identifier: str) -> Optional["DeltastreamResource"]:
        """Get a schema registry configuration if it exists"""
        try:
            # List all schema registries and check if the requested one exists
            (_, table) = self.connections.query("LIST SCHEMA_REGISTRIES;")
            if table and len(table) > 0:
                # Check if our schema registry exists in the list
                for row in table:
                    row_name = None
                    if hasattr(row, "Name"):
                        row_name = row.Name
                    elif isinstance(row, dict) and "Name" in row:
                        row_name = row["Name"]
                    elif hasattr(row, "__getitem__"):
                        try:
                            row_name = row[0]  # Try first column
                        except (IndexError, KeyError):
                            continue

                    if row_name:
                        # Strip quotes from the name for comparison
                        clean_name = self._strip_quotes(row_name)
                        if clean_name == identifier:
                            return self.DeltastreamResource(
                                identifier, "schema_registry", {}
                            )
            return None
        except SQLError as e:
            if e.code == SqlState.SQL_STATE_INVALID_RELATION:
                return None
            raise

    def create_schema(self, relation: DeltastreamRelation) -> None:
        """Create a schema in DeltaStream"""
        try:
            self.connections.query(
                'CREATE SCHEMA "{}" IN DATABASE "{}";'.format(
                    relation.schema, relation.database
                )
            )
        except SQLError as e:
            if e.code == SqlState.SQL_STATE_DUPLICATE_SCHEMA:
                return
            raise dbt_common.exceptions.DbtDatabaseError(
                f"Error creating schema {relation.schema}: {str(e)}"
            )

    def drop_schema(self, relation: DeltastreamRelation) -> None:
        """Drop a schema in DeltaStream"""
        try:
            self.connections.query(
                'DROP SCHEMA "{}"."{}";'.format(relation.database, relation.schema)
            )
            self.cache.drop_schema(relation.database, relation.schema)
        except SQLError as e:
            raise dbt_common.exceptions.DbtDatabaseError(
                f"Error dropping schema {relation.schema}: {str(e)}"
            )

    @available
    def list_schemas(self, database: str) -> List[str]:
        """List all schemas in the database"""
        try:
            (_, schemas) = self.connections.query(
                "SHOW SCHEMAS IN DATABASE {};".format(database)
            )
            return [schema["Name"] for schema in schemas]
        except SQLError as e:
            logger.error(f"Error listing schemas: {str(e)}")
            return []

    def get_fully_qualified_relation_str(self, relation: DeltastreamRelation) -> str:
        return f'"{relation.database}"."{relation.schema}"."{relation.identifier}"'

    # def _schema_is_cached(self, database: Optional[str], schema: str) -> bool:
    #     """Check if schema is cached"""
    #     if database is None:
    #         database = self.config.credentials.database
    #     if database is None:
    #         database = ""
    #     return super()._schema_is_cached(database, schema)

    # def get_column_schema_from_query(self, sql: str) -> Dict[str, Any]:
    #     conn = self.connections.get_thread_connection()
    #     client = conn.handle

    #     # Execute the query to get schema information
    #     try:
    #         result = client.execute_query(sql)
    #         column_info = result.get_schema()
    #         return {
    #             col.name: {
    #                 "name": col.name,
    #                 "type": col.type,
    #                 "nullable": True,  # Default to True as DeltaStream might not provide this info
    #             }
    #             for col in column_info
    #         }
    #     except Exception as e:
    #         logger.debug(f"Error getting column schema: {str(e)}")
    #         return {}

    def standardize_grants_dict(
        self, grants_table: agate.Table
    ) -> Dict[str, List[str]]:
        """Standardize grants table to dictionary of lists.

        Since DeltaStream doesn't support granular permissions yet, we return an empty dict
        """
        return {}

    def verify_database(self, database):
        pass

    @classmethod
    def quote(cls, identifier: str) -> str:
        """Quote an identifier for use in SQL"""
        return f'"{identifier}"'

    @classmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Convert text type to DeltaStream type"""
        return "VARCHAR"

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Convert number type to DeltaStream type"""
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "DOUBLE" if decimals else "BIGINT"

    @classmethod
    def convert_boolean_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Convert boolean type to DeltaStream type"""
        return "BOOLEAN"

    @classmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Convert datetime type to DeltaStream type"""
        return "TIMESTAMP"

    @classmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Convert date type to DeltaStream type"""
        return "DATE"

    @classmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        """Convert time type to DeltaStream type"""
        return "TIME"

    @classmethod
    def date_function(cls) -> str:
        return "current_date()"

    @available
    def get_catalog_relations_parallel(
        self, relations: List[BaseRelation]
    ) -> "agate.Table":
        """Get catalog information for relations using parallel DESCRIBE RELATION COLUMNS calls"""
        import agate

        # Handle empty relations case early
        if not relations:
            # Return empty table with correct schema
            column_names = [
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
            column_types = [
                agate.Text(),
                agate.Text(),
                agate.Text(),
                agate.Text(),
                agate.Text(),
                agate.Text(),
                agate.Number(),
                agate.Text(),
                agate.Text(),
                agate.Text(),
            ]
            return agate.Table([], column_names, column_types)

        # Get the number of threads to use for parallel processing
        # Use min of relations count and thread count from config
        max_workers = min(len(relations), getattr(self.config, "threads", 4))

        def describe_relation_columns(relation: BaseRelation) -> List[Dict[str, Any]]:
            """Describe columns for a single relation"""
            try:
                sql = 'DESCRIBE RELATION COLUMNS "{}"."{}"."{}";'.format(
                    relation.database, relation.schema, relation.identifier
                )

                (_, agate_table) = self.connections.query(sql)

                # Convert agate table rows to list of dicts
                result_rows = []
                for idx, row in enumerate(agate_table.rows):
                    result_rows.append(
                        {
                            "table_database": relation.database,
                            "table_schema": relation.schema,
                            "table_name": relation.identifier,
                            "table_type": "TABLE",  # DeltaStream doesn't distinguish types in DESCRIBE output
                            "table_comment": "",
                            "column_name": row[0],  # Name
                            "column_index": idx,
                            "column_type": row[1],  # Type
                            "column_comment": "",
                            "table_owner": "",
                        }
                    )

                return result_rows

            except Exception as e:
                logger.error(f"Error describing relation {relation}: {str(e)}")
                return []

        # Process relations in parallel
        all_rows = []

        if max_workers == 1:
            # Single-threaded execution
            for relation in relations:
                rows = describe_relation_columns(relation)
                all_rows.extend(rows)
        else:
            # Multi-threaded execution
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_relation = {
                    executor.submit(describe_relation_columns, relation): relation
                    for relation in relations
                }

                for future in concurrent.futures.as_completed(future_to_relation):
                    relation = future_to_relation[future]
                    try:
                        rows = future.result()
                        all_rows.extend(rows)
                    except Exception as e:
                        logger.error(f"Error processing relation {relation}: {str(e)}")

        # Create agate table from collected rows
        if not all_rows:
            # Return empty table with correct schema (this shouldn't happen with valid relations)
            column_names = [
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
            column_types = [
                agate.Text(),
                agate.Text(),
                agate.Text(),
                agate.Text(),
                agate.Text(),
                agate.Text(),
                agate.Number(),
                agate.Text(),
                agate.Text(),
                agate.Text(),
            ]
            return agate.Table([], column_names, column_types)

        # Extract column names and types from first row
        column_names = list(all_rows[0].keys())

        # Convert rows to list of lists for agate
        table_rows = []
        for row_dict in all_rows:
            table_rows.append([row_dict[col] for col in column_names])

        # Define column types
        column_types = [
            agate.Text(),  # table_database
            agate.Text(),  # table_schema
            agate.Text(),  # table_name
            agate.Text(),  # table_type
            agate.Text(),  # table_comment
            agate.Text(),  # column_name
            agate.Number(),  # column_index
            agate.Text(),  # column_type
            agate.Text(),  # column_comment
            agate.Text(),  # table_owner
        ]

        return agate.Table(table_rows, column_names, column_types)
