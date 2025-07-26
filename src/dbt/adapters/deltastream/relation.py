from dataclasses import dataclass
from typing import Optional, Type, Union

from dbt_common.dataclass_schema import StrEnum
from dbt.adapters.base.relation import RelationType

from dbt.adapters.utils import classproperty
from dbt.adapters.base.relation import (
    BaseRelation,
    ComponentName,
)
from dbt_common.utils.dict import filter_null_values


class DeltastreamRelationType(StrEnum):
    # Built-in materialization types.
    CTE = "cte"
    MaterializedView = "materialized_view"
    Table = "table"
    View = "view"

    # DeltaStream specific materialization types.
    Stream = "stream"
    Changelog = "changelog"

    # DeltaStream resource types.
    Store = "store"
    ComputePool = "compute_pool"
    Entity = "entity"
    Function = "function"
    FunctionSource = "function_source"
    DescriptorSource = "descriptor_source"
    SchemaRegistry = "schema_registry"


@dataclass(frozen=True, eq=False, repr=False)
class DeltastreamRelation(BaseRelation):
    type: Optional[Union[RelationType, DeltastreamRelationType]] = None  # type: ignore
    require_alias: bool = False

    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ) -> bool:
        search = filter_null_values(
            {
                ComponentName.Database: database,
                ComponentName.Schema: schema,
                ComponentName.Identifier: identifier,
            }
        )

        if not search:
            # nothing was passed in
            pass

        for k, v in search.items():
            if not self._is_exactish_match(k, v):
                return False

        return True

    @classproperty
    def get_relation_type(cls) -> Type[DeltastreamRelationType]:
        return DeltastreamRelationType

    @property
    def is_deltastream_materialized_view(self) -> bool:
        return self.type == DeltastreamRelationType.MaterializedView

    @property
    def is_stream(self) -> bool:
        return self.type == DeltastreamRelationType.Stream

    @property
    def is_table(self) -> bool:
        return self.type == DeltastreamRelationType.Table

    @property
    def is_changelog(self) -> bool:
        return self.type == DeltastreamRelationType.Changelog

    @property
    def is_view(self) -> bool:
        return self.type == DeltastreamRelationType.View

    @property
    def is_store(self) -> bool:
        return self.type == DeltastreamRelationType.Store

    @property
    def is_compute_pool(self) -> bool:
        return self.type == DeltastreamRelationType.ComputePool

    @property
    def is_entity(self) -> bool:
        return self.type == DeltastreamRelationType.Entity

    @property
    def is_function(self) -> bool:
        return self.type == DeltastreamRelationType.Function

    @property
    def is_function_source(self) -> bool:
        return self.type == DeltastreamRelationType.FunctionSource

    @property
    def is_descriptor_source(self) -> bool:
        return self.type == DeltastreamRelationType.DescriptorSource

    @property
    def is_schema_registry(self) -> bool:
        return self.type == DeltastreamRelationType.SchemaRegistry
