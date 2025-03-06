from dbt.adapters.deltastream.column import (
    DeltastreamColumn,
    get_nested_column_data_types,
    _format_nested_data_type,
    _PARENT_DATA_TYPE_KEY,
    # _update_nested_column_data_types  # indirectly tested via get_nested_column_data_types
)
from dbt.adapters.base.column import Column  # used for monkeypatching in tests


def test_flatten_no_nested():
    # Test flatten for column without nested fields
    col = DeltastreamColumn("col1", "STRING", [], None)
    flat = col.flatten()
    assert len(flat) == 1
    assert flat[0].column == "col1"
    assert flat[0].dtype == "STRING"


def test_flatten_nested():
    # Test flatten for nested column: parent with one child
    child = DeltastreamColumn("child", "INTEGER", [], None)
    parent = DeltastreamColumn("parent", "RECORD", [child], None)
    flat = parent.flatten()
    assert len(flat) == 1
    assert flat[0].column == "parent.child"
    assert flat[0].dtype == "INTEGER"


def test_quoted():
    # Test the quoted property wraps the column name correctly
    col = DeltastreamColumn("my_column", "STRING")
    assert col.quoted == "`my_column`"


def test_literal():
    # Test literal returns correct SQL expression
    col = DeltastreamColumn("col", "INTEGER")
    assert col.literal("123") == "cast(123 as INTEGER)"


def test_data_type_record():
    # Test data_type for RECORD type column with nested fields
    child = DeltastreamColumn("child", "INTEGER", [])
    record = DeltastreamColumn("record", "RECORD", [child])
    dt = record.data_type
    assert "STRUCT<" in dt
    assert "`child`" in dt
    assert "INTEGER" in dt


def test_data_type_record_empty():
    # Test RECORD with no subfields
    col = DeltastreamColumn("rec", "RECORD", [], None)
    assert col.data_type == "STRUCT<>"


def test_data_type_repeated():
    # Test data_type for column with REPEATED mode
    col = DeltastreamColumn("col", "STRING")
    col.mode = "REPEATED"
    dt = col.data_type
    assert dt.startswith("ARRAY<")


def test_numeric_type():
    # Test numeric_type returns the input type unchanged.
    assert DeltastreamColumn.numeric_type("num", None, None) == "num"


def test_is_integer_positive():
    # Test positive integer detection.
    col = DeltastreamColumn("col", "integer")
    assert col.is_integer() is True


def test_is_integer_negative():
    # Test non-integer detection.
    col = DeltastreamColumn("col", "string")
    assert not col.is_integer()


def test_repr():
    # Test __repr__ method returns a string containing name, data_type, and mode
    col = DeltastreamColumn("col", "STRING", [], "NULLABLE")
    rep = repr(col)
    assert "DeltastreamColumn" in rep
    assert "col" in rep
    assert "STRING" in rep
    assert "NULLABLE" in rep


def test_can_expand_to_wrong_type():
    col = DeltastreamColumn("col", "STRING")
    assert not col.can_expand_to("not a column")


def test_can_expand_to_base():
    # Monkey-patch the base Column.can_expand_to to simulate expansion support.
    original_can_expand = Column.can_expand_to
    Column.can_expand_to = lambda self, other: True  # always expandable
    try:
        col1 = DeltastreamColumn("col", "STRING", [], None)
        col2 = DeltastreamColumn("col", "STRING", [], None)
        # Same mode so should return True per both mode check and base method.
        assert col1.can_expand_to(col2)
    finally:
        Column.can_expand_to = original_can_expand


def test_get_nested_column_data_types_flat():
    columns = {"a": {"name": "a", "data_type": "string", "desc": "flat"}}
    result = get_nested_column_data_types(columns)
    # flat column should retain its type plus any extra fields
    assert result["a"]["data_type"] == "string"
    assert result["a"]["desc"] == "flat"


def test_get_nested_column_data_types_nested():
    # Test nested columns with constraints.
    columns = {
        "b.c": {"name": "b.c", "data_type": "int", "desc": "nested"},
        "b.d": {"name": "b.d", "data_type": "int"},
    }
    constraints = {"b.c": "unique", "b.d": "not null"}
    result = get_nested_column_data_types(columns, constraints)
    # Expected nested type is formatted as: struct<c int unique, d int not null>
    assert "b" in result
    assert result["b"]["data_type"] == "struct<c int unique, d int not null>"


def test_update_nested_column_data_types_flat_then_nested():
    # Trigger _update_nested_column_data_types branch where parent's value is a flat string.
    from dbt.adapters.deltastream.column import (
        _update_nested_column_data_types,
        _PARENT_DATA_TYPE_KEY,
    )

    nested = {}
    # First, add flat column "b", which sets nested["b"] as a string.
    _update_nested_column_data_types("b", "string", "constraint", nested)
    # Now call nested update for "b.c" to force conversion of nested["b"] to a dict.
    _update_nested_column_data_types("b.c", "int", None, nested)
    assert isinstance(nested["b"], dict)
    assert nested["b"][_PARENT_DATA_TYPE_KEY] == "string constraint"
    assert nested["b"]["c"] == "int"


def test_format_nested_data_type_str():
    # If input is a string, it is returned as is.
    assert _format_nested_data_type("string") == "string"


def test_format_nested_data_type_dict():
    # Build an unformatted dict representing a nested schema
    unformatted = {_PARENT_DATA_TYPE_KEY: "array", "col1": "int", "col2": "string"}
    # Since _format_nested_data_type pops the parent key, pass a copy.
    result = _format_nested_data_type(dict(unformatted))
    # Expected formatting: struct part first then wrapped in array<>
    expected = "array<struct<col1 int, col2 string>>"
    assert result == expected


def test_format_nested_data_type_with_parent_constraints():
    # Test _format_nested_data_type when a parent has constraints.
    from dbt.adapters.deltastream.column import (
        _format_nested_data_type,
        _PARENT_DATA_TYPE_KEY,
    )

    unformatted = {_PARENT_DATA_TYPE_KEY: "array notnull", "a": "int"}
    result = _format_nested_data_type(dict(unformatted))
    expected = "array<struct<a int>> notnull"
    assert result == expected
