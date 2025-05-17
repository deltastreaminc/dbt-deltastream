from multiprocessing import get_context
from unittest import mock
from decimal import Decimal

import agate

import unittest
from unittest.mock import patch, MagicMock

from dbt.adapters.deltastream.impl import DeltastreamAdapter
from dbt.adapters.deltastream.relation import (
    DeltastreamRelation,
)
from tests.utils import (
    config_from_parts_or_dicts,
    inject_adapter,
    TestAdapterConversions,
    load_internal_manifest_macros,
    mock_connection,
)

import dbt_common.exceptions
from dbt.context.query_header import generate_query_header_context
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import ManifestStateCheck
from dbt.context.providers import generate_runtime_macro_context


def _ds_conn():
    conn = MagicMock()
    conn.get.side_effect = lambda x: "deltastream" if x == "type" else None
    return conn


class BaseTestDeltastreamAdapter(unittest.TestCase):
    def setUp(self):
        self.raw_profile = {
            "outputs": {
                "default": {
                    "type": "deltastream",
                    "token": "test-token",
                    "database": "test_db",
                    "schema": "test_schema",
                    "threads": 1,
                    # Optional parameters with defaults
                    "url": "https://api.deltastream.io/v2",
                    "timezone": "UTC",
                    "session_id": None,
                    "organization_id": "prod_org",
                    "role": None,
                    "store": None,
                },
                "prod": {
                    "type": "deltastream",
                    "token": "prod-token",
                    "database": "prod_db",
                    "schema": "prod_schema",
                    "threads": 1,
                    "url": "https://api.deltastream.io/v2",
                    "timezone": "UTC",
                    "organization_id": "prod_org",
                },
            },
            "target": "default",
        }

        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "project-root": "/tmp/dbt/does-not-exist",
            "profile": "default",
            "config-version": 2,
        }
        self.qh_patch = None

        @mock.patch("dbt.parser.manifest.ManifestLoader.build_manifest_state_check")
        def _mock_state_check(self):
            all_projects = self.all_projects
            return ManifestStateCheck(
                vars_hash=FileHash.from_contents("vars"),
                project_hashes={
                    name: FileHash.from_contents(name) for name in all_projects
                },
                profile_hash=FileHash.from_contents("profile"),
            )

        self.load_state_check = mock.patch(
            "dbt.parser.manifest.ManifestLoader.build_manifest_state_check"
        )
        self.mock_state_check = self.load_state_check.start()
        self.mock_state_check.side_effect = _mock_state_check

    def tearDown(self):
        if self.qh_patch:
            self.qh_patch.stop()
        super().tearDown()

    def get_adapter(self, target) -> DeltastreamAdapter:
        project = self.project_cfg.copy()
        profile = self.raw_profile.copy()
        profile["target"] = target
        config = config_from_parts_or_dicts(
            project=project,
            profile=profile,
        )
        adapter = DeltastreamAdapter(config, get_context("spawn"))
        adapter.set_macro_resolver(load_internal_manifest_macros(config))
        adapter.set_macro_context_generator(generate_runtime_macro_context)
        adapter.connections.set_query_header(
            generate_query_header_context(config, adapter.get_macro_resolver())
        )

        self.qh_patch = patch.object(adapter.connections.query_header, "add")
        self.mock_query_header_add = self.qh_patch.start()
        self.mock_query_header_add.side_effect = lambda q: "/* dbt */\n{}".format(q)

        inject_adapter(adapter)
        return adapter


class TestDeltastreamAdapterAcquire(BaseTestDeltastreamAdapter):
    @patch(
        "dbt.adapters.deltastream.connections.DeltastreamConnectionManager.open",
        return_value=_ds_conn(),
    )
    def test_acquire_connection_validations(self, mock_open_connection):
        adapter = self.get_adapter("default")
        mock_open_connection.return_value = mock_connection("deltastream")
        connection = adapter.acquire_connection("dummy")

        connection.credentials.token = "test-token"
        connection.credentials.database = "test_db"
        connection.credentials.schema = "test_schema"
        connection.credentials.url = "https://api.deltastream.io/v2"
        connection.credentials.timezone = "UTC"
        connection.credentials.session_id = None
        connection.credentials.organization_id = None
        connection.credentials.role = None
        connection.credentials.store = None

        mock_open_connection.assert_not_called()
        connection.handle
        mock_open_connection.assert_called_once()

        self.assertEqual(connection.type, "deltastream")

    def test_cancel_open_connections_empty(self):
        adapter = self.get_adapter("default")
        self.assertEqual(adapter.cancel_open_connections(), None)


class TestDeltastreamRelation(unittest.TestCase):
    def setUp(self):
        self.relation = DeltastreamRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )

    def test_table_relation(self):
        self.assertEqual(self.relation.database, "test_db")
        self.assertEqual(self.relation.schema, "test_schema")
        self.assertEqual(self.relation.identifier, "test_table")
        self.assertEqual(self.relation.type, None)


class TestDeltastreamAdapter(unittest.TestCase):
    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.mock_conn.name = "test_conn"  # Added connection name

        self.config = MagicMock()
        self.config.credentials = MagicMock()
        self.config.credentials.database = "test_db"
        self.config.credentials.schema = "test_schema"

        self.adapter = DeltastreamAdapter(self.config, get_context("spawn"))
        self.adapter.connections = MagicMock()
        self.adapter.connections.get_thread_connection.return_value = self.mock_conn
        self.adapter.connections.get_if_exists.return_value = self.mock_conn
        self.adapter.connections.set_connection_name = (
            MagicMock()
        )  # Add mock for connection name setting

        self.relation = DeltastreamRelation.create(
            database="test_db",
            schema="test_schema",
            identifier="test_table",
        )

    def test_create_schema(self):
        self.adapter.create_schema(self.relation)
        self.adapter.connections.query.assert_called_once_with(
            'CREATE SCHEMA "test_schema" IN DATABASE "test_db";'
        )

    def test_drop_schema(self):
        self.adapter.drop_schema(self.relation)
        self.adapter.connections.query.assert_called_once_with(
            'DROP SCHEMA "test_db"."test_schema";'
        )

    def test_drop_relation(self):
        self.adapter.connections.set_connection_name("test_conn")  # Set connection name
        self.adapter.drop_relation(self.relation)
        self.adapter.connections.query.assert_called_once_with(
            'DROP RELATION "test_db"."test_schema"."test_table";'
        )

    def test_truncate_relation(self):
        self.adapter.connections.set_connection_name("test_conn")  # Set connection name
        self.adapter.truncate_relation(self.relation)
        self.adapter.connections.query.assert_called_once_with(
            'TRUNCATE RELATION "test_db"."test_schema"."test_table";'
        )

    def test_rename_relation_not_supported(self):
        with self.assertRaises(dbt_common.exceptions.DbtRuntimeError):
            self.adapter.rename_relation(self.relation, self.relation)

    def test_get_columns_in_relation(self):
        mock_agate_table = MagicMock()
        mock_agate_table.rows = [
            ["col1", "VARCHAR", "NULLABLE"],
            ["col2", "INTEGER", "NOT NULL"],
        ]
        self.adapter.connections.query.return_value = (None, mock_agate_table)

        columns = self.adapter.get_columns_in_relation(self.relation)
        self.assertEqual(len(columns), 2)
        self.assertEqual(columns[0].column, "col1")
        self.assertEqual(columns[0].dtype, "VARCHAR")
        self.assertEqual(columns[1].column, "col2")
        self.assertEqual(columns[1].dtype, "INTEGER")

    def test_list_relations_without_caching(self):
        mock_agate_table = MagicMock()
        mock_agate_table.rows = [["table1"], ["table2"]]
        self.adapter.connections.query.return_value = (None, mock_agate_table)

        relations = self.adapter.list_relations_without_caching(self.relation)
        self.assertEqual(len(relations), 2)
        self.assertEqual(relations[0].identifier, "table1")
        self.assertEqual(relations[1].identifier, "table2")

    def test_rename_catalog_columns(self):
        import agate

        rows = [
            ["db1", "sch1", "tbl1", "BASE TABLE", "id1", "owner1"],
            ["db2", "sch2", "tbl2", "VIEW", "id2", "owner2"],
        ]
        original_columns = [
            "database_name",
            "schema_name",
            "name",
            "relation_type",
            "primary_key",
            "owner",
        ]
        table = agate.Table(rows, original_columns)
        renamed_table = self.adapter.rename_catalog_columns(table)
        # Updated expected column order matching function output
        expected_columns = [
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
        self.assertEqual(list(renamed_table.column_names), expected_columns)
        self.assertEqual(
            renamed_table.rows[0].values(),
            (
                "db1",
                "sch1",
                "tbl1",
                "BASE TABLE",
                "id1",
                "",
                "",
                Decimal("0"),
                "",
                "",
                "owner1",
            ),
        )
        self.assertEqual(
            renamed_table.rows[1].values(),
            (
                "db2",
                "sch2",
                "tbl2",
                "VIEW",
                "id2",
                "",
                "",
                Decimal("1"),
                "",
                "",
                "owner2",
            ),
        )


class TestDeltastreamAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ["", "text"],
            ["a", "text"],
            ["some text", "text"],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ["VARCHAR", "VARCHAR", "VARCHAR"]

        for i, dtype in enumerate(expected):
            self.assertEqual(
                DeltastreamAdapter.convert_text_type(agate_table, i), dtype
            )

    def test_convert_number_type(self):
        rows = [
            ["", "23.98", "-1"],
            ["", "12.78", "-2"],
            ["", "79.41", "-3"],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ["BIGINT", "DOUBLE", "BIGINT"]

        for i, dtype in enumerate(expected):
            self.assertEqual(
                DeltastreamAdapter.convert_number_type(agate_table, i),
                dtype,
                "Column {} failed".format(i),
            )

    def test_convert_boolean_type(self):
        rows = [
            [True, True],
            [False, False],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ["BOOLEAN", "BOOLEAN"]

        for i, dtype in enumerate(expected):
            self.assertEqual(
                DeltastreamAdapter.convert_boolean_type(agate_table, i), dtype
            )

    def test_convert_datetime_type(self):
        rows = [
            ["2019-01-01 01:23:45", "2019-01-01 01:23:45"],
        ]
        agate_table = self._make_table_of(rows, agate.DateTime)
        expected = ["TIMESTAMP"]

        for i, dtype in enumerate(expected):
            self.assertEqual(
                DeltastreamAdapter.convert_datetime_type(agate_table, i), dtype
            )

    def test_convert_date_type(self):
        rows = [
            ["2019-01-01", "2019-01-01"],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ["DATE"]

        for i, dtype in enumerate(expected):
            self.assertEqual(
                DeltastreamAdapter.convert_date_type(agate_table, i), dtype
            )
