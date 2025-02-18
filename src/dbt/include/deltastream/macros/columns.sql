{% macro deltastream__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {%- if select_sql_header is not none -%}
    {{ select_sql_header }}
    {%- endif -%}
    --#dbt_sbq_parse_header#--
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
{% endmacro %}
