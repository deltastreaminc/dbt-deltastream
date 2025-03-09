from dataclasses import dataclass
from dbt.adapters.contracts.relation import Path
from dbt.adapters.events.logging import AdapterLogger
from typing import Any, Dict, List, Optional

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
            existing_types[5]
            if len(existing_types) > 5
            else agate.Text(),  # table_owner
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
                'DESCRIBE RELATION COLUMNS"{}"."{}"."{}";'.format(
                    relation.database, relation.schema, relation.identifier
                )
            )
            columns = []
            for row in agate_table.rows:
                column_info = DeltastreamColumn(
                    column=row[0],  # column name
                    dtype=row[1],  # data type
                    mode=row[2],  # mode (nullable or not)
                    fields=None,
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
            if e.code in [SqlState.SQL_STATE_INVALID_RELATION, SqlState.SQL_STATE_INVALID_SCHEMA]:
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
    def date_function(self) -> str:
        return "current_date()"
