{% macro deltastream__create_database(database_name) -%}
  create database {{ database_name }};
{%- endmacro %}

{% macro deltastream__create_changelog_as(relation, sql, parameters) -%}
  create changelog {{ relation }}
  {{ deltastream__with_parameters(parameters) }}
  as {{ sql }};
{%- endmacro %}

{% macro deltastream__create_materialized_view_as(relation, sql, parameters) -%}
  create materialized view {{ relation }}
  {{ deltastream__with_parameters(parameters) }}
  as {{ sql }};
{%- endmacro %}

{% macro deltastream__create_stream_as(relation, sql, parameters) -%}
  create stream {{ relation }}
  {{ deltastream__with_parameters(parameters) }}
  as {{ sql }};
{%- endmacro %}

{% macro deltastream__create_table_as(relation, sql, parameters) -%}
  create table {{ relation }}
  {{ deltastream__with_parameters(parameters) }}
  as {{ sql }};
{%- endmacro %}

{% macro deltastream__create_changelog(relation, columns, parameters, primary_key) -%}
  create changelog {{ relation }}
  {{ deltastream__format_columns(columns,  true, primary_key) }}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__create_stream(relation, columns, parameters) -%}
  create stream {{ relation }}
  {{ deltastream__format_columns(columns) }}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__create_store(relation, parameters) -%}
  create store {{ relation.identifier }}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__create_entity(relation, parameters, store) -%}
  create entity {{ relation }}{% if store %} in store {{ store }}{% endif %}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__with_parameters(parameters) -%}
  {% if parameters.items() | length > 0 %}
    with (
    {%- for parameter, value in parameters.items() %}
      {%- if parameter == 'type' or parameter == 'kafka.sasl.hash_function' %}
      '{{ parameter }}' = {{ value }}{% if not loop.last %},{% endif %}
      {%- elif parameter == 'access_region' %}
      '{{ parameter }}' = "{{ value }}"{% if not loop.last %},{% endif %}
      {%- elif value is number %}
      '{{ parameter }}' = {{ value }}{% if not loop.last %},{% endif %}
      {%- else %}
      '{{ parameter }}' = '{{ value }}'{% if not loop.last %},{% endif %}
      {%- endif %}
    {%- endfor %}
    )
  {% endif %}
{%- endmacro %}

{% macro deltastream__format_columns(columns, include_primary_key=false, primary_key=None) %}
  (
    {%- for column_name, column_def in columns.items() %}
      {%- if column_def.get('type') is none %}
        {{ exceptions.raise_compiler_error("Column '" ~ column_name ~ "' must have a type defined.") }}
      {%- endif %}
      `{{ column_name }}` {{ column_def.get('type') }}{% if not column_def.get('nullable', true) %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- if include_primary_key and primary_key %}
      , PRIMARY KEY(
      {%- if primary_key is string %}
        {{ primary_key }}
      {%- else %}
        {%- for pk_column in primary_key -%}
          {{ pk_column }}{% if not loop.last %}, {% endif %}
        {%- endfor -%}
      {%- endif %}
      )
    {%- endif %}
  )
{% endmacro %}

{% macro deltastream__drop_relation(relation) -%}
  {% call statement('drop_relation') -%}
    {% if relation.type == 'store' %}
      drop store {{ relation.identifier }}
    {% elif relation.type == 'materialized view' %}
      drop materialized view {{ relation }}
    {% elif relation.type == 'stream' %}
      drop stream {{ relation }}
    {% elif relation.type == 'changelog' %}
      drop changelog {{ relation }}
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro deltastream__apply_grants(relation, grant_config, should_revoke) -%}
  {{ exceptions.raise_compiler_error(
        """
        dbt-deltastream does not implement the grants configuration.

        If this feature is important to you, please reach out!
        """
    )}}
{% endmacro %}

{% macro deltastream__sql_contains_select(sql) -%}
  {% if "select" in (sql | lower) %}
    {{ return(true) }}
  {% else %}
    {{ return(false) }}
  {% endif %}
{% endmacro %}

{% macro create_deltastream_database(database_name) -%}
  {% set query -%}
    {{ deltastream__create_database(database_name) }}
  {%- endset %}

  {% set query_run = run_query(query) %}

  {{ log('Created database: ' ~ database_name, info = True) }}
{% endmacro %}