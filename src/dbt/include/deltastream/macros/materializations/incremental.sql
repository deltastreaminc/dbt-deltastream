{% materialization incremental, adapter='deltastream' %}
    {{ exceptions.CompilationError(
        """
        dbt-deltastream does not support incremental models, because all stream, changelog or views in
        deltastream are natively maintained incrementally.

        Use the `materialized_view` materialization instead.

        See: https://docs.deltastream.io/reference/sql-syntax/query/materialized-view
        """
    )}}
{% endmaterialization %}
