import pytest
from unittest.mock import MagicMock
from multiprocessing import get_context

import dbt_common.exceptions
from deltastream.api.error import SQLError, SqlState
from dbt.adapters.deltastream.impl import DeltastreamAdapter
from dbt.adapters.deltastream.relation import (
    DeltastreamRelation,
)


class DummyConfig:
    def __init__(self):
        self.credentials = MagicMock(database="dummy_db", schema="dummy_schema")
        self.log_cache_events = False


DummyConfigInstance = DummyConfig()


@pytest.fixture
def adapter():
    adapter_instance = DeltastreamAdapter(DummyConfigInstance, get_context("spawn"))
    adapter_instance.connections = MagicMock()
    adapter_instance.cache = MagicMock()
    return adapter_instance


@pytest.fixture
def dummy_relation():
    return DeltastreamRelation.create(
        database="dummy_db", schema="dummy_schema", identifier="dummy_table"
    )


def test_is_cancelable():
    assert not DeltastreamAdapter.is_cancelable()


def test_drop_relation_success(adapter, dummy_relation, monkeypatch):
    mock_cache_dropped = MagicMock()
    monkeypatch.setattr(adapter, "_schema_is_cached", lambda db, sch: True)
    adapter.cache_dropped = mock_cache_dropped
    adapter.connections.query.return_value = None
    adapter.drop_relation(dummy_relation)
    adapter.connections.query.assert_called_once_with(
        f"DROP RELATION {adapter.get_fully_qualified_relation_str(dummy_relation)};"
    )
    mock_cache_dropped.assert_called_once_with(dummy_relation)


def test_drop_relation_fail(adapter, dummy_relation):
    adapter._schema_is_cached = lambda db, sch: False
    adapter.connections.query.side_effect = SQLError("drop error", "XX000", "dummy")
    with pytest.raises(dbt_common.exceptions.DbtDatabaseError) as exc:
        adapter.drop_relation(dummy_relation)
    assert "Error dropping relation" in str(exc.value)


def test_truncate_relation(adapter, dummy_relation):
    adapter._schema_is_cached = lambda db, sch: False
    adapter.connections.query.return_value = None
    adapter.truncate_relation(dummy_relation)
    adapter.connections.query.assert_called_once_with(
        f"TRUNCATE RELATION {adapter.get_fully_qualified_relation_str(dummy_relation)};"
    )


def test_truncate_relation_fail(adapter, dummy_relation):
    adapter.connections.query.side_effect = SQLError("truncate error", "XX000", "dummy")
    with pytest.raises(dbt_common.exceptions.DbtDatabaseError) as exc:
        adapter.truncate_relation(dummy_relation)
    assert "Error truncating relation" in str(exc.value)


def test_rename_relation_not_supported(adapter, dummy_relation):
    with pytest.raises(dbt_common.exceptions.DbtRuntimeError):
        adapter.rename_relation(dummy_relation, dummy_relation)


def test_list_relations_without_caching_success(adapter):
    fake_agate = MagicMock()
    fake_agate.rows = [["table1"], ["table2"]]
    adapter.connections.query.return_value = (None, fake_agate)
    dummy = DeltastreamRelation.create(
        database="dummy_db", schema="dummy_schema", identifier="dummy"
    )
    relations = adapter.list_relations_without_caching(dummy)
    assert len(relations) == 2
    assert relations[0].identifier == "table1"
    assert relations[1].identifier == "table2"


def test_list_relations_without_caching_fail(adapter):
    adapter.connections.query.side_effect = Exception("fail")
    dummy = DeltastreamRelation.create(
        database="dummy_db", schema="dummy_schema", identifier="dummy"
    )
    relations = adapter.list_relations_without_caching(dummy)
    assert relations == []


def test_get_columns_in_relation_success(adapter, dummy_relation):
    fake_agate = MagicMock()
    fake_agate.rows = [["col1", "VARCHAR", "NULLABLE"], ["col2", "INTEGER", "NOT NULL"]]
    adapter.connections.query.return_value = (None, fake_agate)
    columns = adapter.get_columns_in_relation(dummy_relation)
    assert len(columns) == 2
    assert columns[0].column == "col1"
    assert columns[0].dtype == "VARCHAR"
    assert columns[1].column == "col2"
    assert columns[1].dtype == "INTEGER"


def test_get_columns_in_relation_fail(adapter, dummy_relation):
    adapter.connections.query.side_effect = Exception("fail")
    columns = adapter.get_columns_in_relation(dummy_relation)
    assert columns == []


def test_get_relation_cached(adapter, dummy_relation, monkeypatch):
    mock_get_relation = MagicMock(return_value=dummy_relation)
    monkeypatch.setattr(adapter, "_schema_is_cached", lambda db, sch: True)
    monkeypatch.setattr(type(adapter), "get_relation", staticmethod(mock_get_relation))
    result = adapter.get_relation("dummy_db", "dummy_schema", "dummy_table")
    mock_get_relation.assert_called_once_with("dummy_db", "dummy_schema", "dummy_table")
    assert result == dummy_relation


def test_get_relation_query_success(adapter, dummy_relation):
    adapter._schema_is_cached = lambda db, sch: False
    adapter.connections.query.return_value = (None, None)
    result = adapter.get_relation("dummy_db", "dummy_schema", "dummy_table")
    assert isinstance(result, DeltastreamRelation)
    assert result.database == "dummy_db"
    assert result.schema == "dummy_schema"
    assert result.identifier == "dummy_table"


def test_get_relation_query_fail(adapter):
    adapter._schema_is_cached = lambda db, sch: False
    error = SQLError("not found", "XX000", "dummy")
    adapter.connections.query.side_effect = error
    with pytest.raises(SQLError):
        adapter.get_relation("dummy_db", "dummy_schema", "dummy_table")


def test_get_relation_invalid_relation(adapter):
    adapter._schema_is_cached = lambda db, sch: False
    error = SQLError("relation not found", SqlState.SQL_STATE_INVALID_RELATION, "dummy")
    adapter.connections.query.side_effect = error
    result = adapter.get_relation("dummy_db", "dummy_schema", "dummy_table")
    assert result is None


def test_get_relation_invalid_schema(adapter):
    adapter._schema_is_cached = lambda db, sch: False
    error = SQLError("schema not found", SqlState.SQL_STATE_INVALID_SCHEMA, "dummy")
    adapter.connections.query.side_effect = error
    result = adapter.get_relation("dummy_db", "dummy_schema", "dummy_table")
    assert result is None


def test_get_relation_other_sql_error(adapter):
    adapter._schema_is_cached = lambda db, sch: False
    error = SQLError("some other error", "XX000", "dummy")
    adapter.connections.query.side_effect = error
    with pytest.raises(SQLError) as exc:
        adapter.get_relation("dummy_db", "dummy_schema", "dummy_table")
    assert str(exc.value) == "some other error"


def test_create_schema_success(adapter, dummy_relation):
    adapter.connections.query.return_value = None
    adapter.create_schema(dummy_relation)
    adapter.connections.query.assert_called_once_with(
        f'CREATE SCHEMA "{dummy_relation.schema}" IN DATABASE "{dummy_relation.database}";'
    )


def test_create_schema_duplicate(adapter, dummy_relation):
    error = SQLError("duplicate", SqlState.SQL_STATE_DUPLICATE_SCHEMA, "dummy")
    adapter.connections.query.side_effect = error
    adapter.create_schema(dummy_relation)


def test_create_schema_fail(adapter, dummy_relation):
    error = SQLError("other error", "XX000", "dummy")
    adapter.connections.query.side_effect = error
    with pytest.raises(dbt_common.exceptions.DbtDatabaseError) as exc:
        adapter.create_schema(dummy_relation)
    assert "Error creating schema" in str(exc.value)


def test_drop_schema(adapter, dummy_relation):
    adapter.connections.query.return_value = None
    adapter.drop_schema(dummy_relation)
    adapter.connections.query.assert_called_once_with(
        f'DROP SCHEMA "{dummy_relation.database}"."{dummy_relation.schema}";'
    )
    adapter.cache.drop_schema.assert_called_once_with(
        dummy_relation.database, dummy_relation.schema
    )


def test_list_schemas_success(adapter):
    fake_schemas = [{"Name": "schema1"}, {"Name": "schema2"}]
    adapter.connections.query.return_value = (None, fake_schemas)
    schemas = adapter.list_schemas("dummy_db")
    assert schemas == ["schema1", "schema2"]


def test_list_schemas_fail(adapter):
    adapter.connections.query.side_effect = SQLError("fail", "XX000", "dummy")
    schemas = adapter.list_schemas("dummy_db")
    assert schemas == []


def test_get_fully_qualified_relation_str(adapter, dummy_relation):
    fq = adapter.get_fully_qualified_relation_str(dummy_relation)
    expected = f'"{dummy_relation.database}"."{dummy_relation.schema}"."{dummy_relation.identifier}"'
    assert fq == expected


# Not implemented yet
def test_standardize_grants_dict(adapter):
    fake_table = MagicMock()
    assert adapter.standardize_grants_dict(fake_table) == {}


def test_quote_and_date_function():
    assert DeltastreamAdapter.quote("some_id") == '"some_id"'
    assert DeltastreamAdapter.date_function() == "current_date()"
