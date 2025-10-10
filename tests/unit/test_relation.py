from dbt.adapters.deltastream.relation import (
    DeltastreamRelation,
    DeltastreamRelationType,
)


# Test matches with no search args; should return True
def test_matches_no_args():
    relation = DeltastreamRelation(path="dummy", type=None, require_alias=False)
    assert relation.matches() is True


# Helper functions to simulate matching behavior
def always_true(self, key, value):
    return True


def always_false(self, key, value):
    return False


# Test matches when _is_exactish_match returns True for all parameters
def test_matches_all_true(monkeypatch):
    # Patch at class level instead of the instance.
    monkeypatch.setattr(DeltastreamRelation, "_is_exactish_match", always_true)
    relation = DeltastreamRelation(path="dummy", type=None, require_alias=False)
    assert relation.matches(database="db", schema="sch", identifier="id") is True


# Test matches when _is_exactish_match returns False for any parameter
def test_matches_any_false(monkeypatch):
    monkeypatch.setattr(DeltastreamRelation, "_is_exactish_match", always_false)
    relation = DeltastreamRelation(path="dummy", type=None, require_alias=False)
    assert relation.matches(database="db", schema="sch", identifier="id") is False


# Test materialization property methods
def test_materialization_properties():
    # MaterializedView
    mv_relation = DeltastreamRelation(
        path="dummy", type=DeltastreamRelationType.MaterializedView, require_alias=False
    )
    assert mv_relation.is_deltastream_materialized_view is True
    assert mv_relation.is_stream is False
    assert mv_relation.is_table is False
    assert mv_relation.is_changelog is False
    assert mv_relation.is_view is False

    # Stream
    stream_relation = DeltastreamRelation(
        path="dummy", type=DeltastreamRelationType.Stream, require_alias=False
    )
    assert stream_relation.is_stream is True

    # Table
    table_relation = DeltastreamRelation(
        path="dummy", type=DeltastreamRelationType.Table, require_alias=False
    )
    assert table_relation.is_table is True

    # Changelog
    changelog_relation = DeltastreamRelation(
        path="dummy", type=DeltastreamRelationType.Changelog, require_alias=False
    )
    assert changelog_relation.is_changelog is True

    # View
    view_relation = DeltastreamRelation(
        path="dummy", type=DeltastreamRelationType.View, require_alias=False
    )
    assert view_relation.is_view is True


def test_get_relation_type():
    assert DeltastreamRelation.get_relation_type == DeltastreamRelationType
