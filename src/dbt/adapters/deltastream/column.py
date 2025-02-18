from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

from dbt.adapters.base.column import Column


_PARENT_DATA_TYPE_KEY = "__parent_data_type"

Self = TypeVar("Self", bound="DeltastreamColumn")


@dataclass
class DeltastreamColumn(Column):
    TYPE_LABELS = {
        "TEXT": "VARCHAR",
    }
    mode: Optional[str] = None
    fields: List[Self] = None  # type: ignore

    def __init__(
        self,
        column: str,
        dtype: str,
        fields: Optional[List[Self]] = None,
        mode: Optional[str] = None,
    ) -> None:
        super().__init__(column, dtype)
        self.fields = fields or []
        self.mode = mode


    @classmethod
    def _flatten_recursive(
        cls: Type[Self], col: Self, prefix: Optional[List[str]] = None
    ) -> List[Self]:
        if prefix is None:
            prefix = []

        if len(col.fields) == 0:
            prefixed_name = ".".join(prefix + [col.column])
            new_col = cls(prefixed_name, col.dtype, [], col.mode)
            return [new_col]

        new_fields = []
        for field in col.fields:
            new_prefix = prefix + [col.column]
            new_fields.extend(cls._flatten_recursive(field, new_prefix))

        return new_fields

    def flatten(self):
        return self._flatten_recursive(self)

    @property
    def quoted(self):
        return "`{}`".format(self.column)

    def literal(self, value):
        return "cast({} as {})".format(value, self.dtype)

    @property
    def data_type(self) -> str:
        if self.dtype.upper() == "RECORD":
            subcols = [
                "{} {}".format(col.quoted, col.data_type)
                for col in self.fields  # type: ignore[attr-defined]
            ]
            field_type = "STRUCT<{}>".format(", ".join(subcols))
        else:
            field_type = self.dtype

        if self.mode is not None and self.mode.upper() == "REPEATED":
            return "ARRAY<{}>".format(field_type)
        else:
            return field_type

    @classmethod
    def numeric_type(cls, dtype: str, precision: Any, scale: Any) -> str:
        return dtype

    def is_integer(self) -> bool:
        return self.dtype.lower() in [
            "tinyint",
            "smallint",
            "integer",
            "bigint",
        ]

    def can_expand_to(self, other_column: Column) -> bool:
        if not isinstance(other_column, DeltastreamColumn):
            return False

        # Add mode comparison
        if self.mode != other_column.mode:
            return False

        return super().can_expand_to(other_column)

    def __repr__(self) -> str:
        return "<DeltastreamColumn {} ({}, {})>".format(
            self.name, self.data_type, self.mode
        )


def get_nested_column_data_types(
    columns: Dict[str, Dict[str, Any]],
    constraints: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    columns:
        * Dictionary where keys are of flat columns names and values are dictionary of column attributes
        * column names with "." indicate a nested column within a STRUCT type
        * e.g. {"a": {"name": "a", "data_type": "varchar", ...}}
    constraints:
        * Dictionary where keys are flat column names and values are rendered constraints for the column
        * If provided, rendered column is included in returned "data_type" values.
    returns:
        * Dictionary where keys are root column names and values are corresponding nested data_type values.
        * Fields other than "name" and "data_type" are __not__ preserved in the return value for nested columns.
        * Fields other than "name" and "data_type" are preserved in the return value for flat columns.

    Example:
    columns: {
        "a": {"name": "a", "data_type": "varchar", "description": ...},
        "b.nested": {"name": "b.nested", "data_type": "varchar"},
        "b.nested2": {"name": "b.nested2", "data_type": "varchar"}
        }

    returns: {
        "a": {"name": "a", "data_type": "varchar"},
        "b": {"name": "b": "data_type": "struct<nested varchar, nested2 varchar>}
    }
    """
    constraints = constraints or {}

    nested_column_data_types: Dict[str, Optional[Union[str, Dict[str, Any]]]] = {}
    for column in columns.values():
        _update_nested_column_data_types(
            column["name"],
            column.get("data_type"),
            constraints.get(column["name"]),
            nested_column_data_types,
        )

    formatted_nested_column_data_types: Dict[str, Dict[str, Any]] = {}
    for column_name, unformatted_column_type in nested_column_data_types.items():
        formatted_nested_column_data_types[column_name] = {
            "name": column_name,
            "data_type": _format_nested_data_type(unformatted_column_type),
        }

    # add column configs back to flat columns
    for column_name in formatted_nested_column_data_types:
        if column_name in columns:
            formatted_nested_column_data_types[column_name].update(
                {
                    k: v
                    for k, v in columns[column_name].items()
                    if k not in formatted_nested_column_data_types[column_name]
                }
            )

    return formatted_nested_column_data_types


def _update_nested_column_data_types(
    column_name: str,
    column_data_type: Optional[str],
    column_rendered_constraint: Optional[str],
    nested_column_data_types: Dict[str, Optional[Union[str, Dict[str, Any]]]],
) -> None:
    """
    Recursively update nested_column_data_types given a column_name, column_data_type, and optional column_rendered_constraint.

    Examples:
    >>> nested_column_data_types = {}
    >>> DeltastreamAdapter._update_nested_column_data_types("a", "varchar", "not_null", nested_column_data_types)
    >>> nested_column_data_types
    {"a": "varchar not null"}
    >>> DeltastreamAdapter._update_nested_column_data_types("b.c", "varchar", "not_null", nested_column_data_types)
    >>> nested_column_data_types
    {"a": "varchar not null", "b": {"c": "varchar not null"}}
    >>> DeltastreamAdapter._update_nested_column_data_types("b.d", "varchar", None, nested_column_data_types)
    >>> nested_column_data_types
    {"a": "varchar not null", "b": {"c": "varchar not null", "d": "varchar"}}
    """
    column_name_parts = column_name.split(".")
    root_column_name = column_name_parts[0]

    if len(column_name_parts) == 1:
        # Base case: column is not nested - store its data_type concatenated with constraint if provided.
        column_data_type_and_constraints = (
            (
                column_data_type
                if column_rendered_constraint is None
                else f"{column_data_type} {column_rendered_constraint}"
            )
            if column_data_type
            else None
        )

        if existing_nested_column_data_type := nested_column_data_types.get(
            root_column_name
        ):
            assert isinstance(
                existing_nested_column_data_type, dict
            )  # keeping mypy happy
            # entry could already exist if this is a parent column -- preserve the parent data type under "_PARENT_DATA_TYPE_KEY"
            existing_nested_column_data_type.update(
                {_PARENT_DATA_TYPE_KEY: column_data_type_and_constraints}
            )
        else:
            nested_column_data_types.update(
                {root_column_name: column_data_type_and_constraints}
            )
    else:
        parent_data_type = nested_column_data_types.get(root_column_name)
        if isinstance(parent_data_type, dict):
            # nested dictionary already initialized
            pass
        elif parent_data_type is None:
            # initialize nested dictionary
            nested_column_data_types.update({root_column_name: {}})
        else:
            # a parent specified its base type -- preserve its data_type and potential rendered constraints
            # this is used to specify a top-level 'struct' or 'array' field with its own description, constraints, etc
            nested_column_data_types.update(
                {root_column_name: {_PARENT_DATA_TYPE_KEY: parent_data_type}}
            )

        # Recursively process rest of remaining column name
        remaining_column_name = ".".join(column_name_parts[1:])
        remaining_column_data_types = nested_column_data_types[root_column_name]
        assert isinstance(remaining_column_data_types, dict)  # keeping mypy happy
        _update_nested_column_data_types(
            remaining_column_name,
            column_data_type,
            column_rendered_constraint,
            remaining_column_data_types,
        )


def _format_nested_data_type(
    unformatted_nested_data_type: Optional[Union[str, Dict[str, Any]]],
) -> str:
    """
    Recursively format a (STRUCT) data type given an arbitrarily nested data type structure.

    Examples:
    >>> DeltastreamAdapter._format_nested_data_type("varchar")
    'varchar'
    >>> DeltastreamAdapter._format_nested_data_type({'c': 'varchar not_null', 'd': 'varchar'})
    'struct<c varchar not_null, d varchar>'
    >>> DeltastreamAdapter._format_nested_data_type({'c': 'varchar not_null', 'd': {'e': 'varchar'}})
    'struct<c varchar not_null, d struct<e varchar>>'
    """
    if unformatted_nested_data_type is None:
        return ""
    elif isinstance(unformatted_nested_data_type, str):
        return unformatted_nested_data_type
    else:
        parent_data_type, *parent_constraints = unformatted_nested_data_type.pop(
            _PARENT_DATA_TYPE_KEY, ""
        ).split() or [None]

        formatted_nested_types = [
            f"{column_name} {_format_nested_data_type(column_type) or ''}".strip()
            for column_name, column_type in unformatted_nested_data_type.items()
        ]

        formatted_nested_type = f"""struct<{", ".join(formatted_nested_types)}>"""

        if parent_data_type and parent_data_type.lower() == "array":
            formatted_nested_type = f"""array<{formatted_nested_type}>"""

        if parent_constraints:
            parent_constraints = " ".join(parent_constraints)
            formatted_nested_type = f"""{formatted_nested_type} {parent_constraints}"""

        return formatted_nested_type
