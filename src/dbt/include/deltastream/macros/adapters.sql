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

{# dbt standard create_table_as macro override #}
{% macro deltastream__create_table_as(temporary, relation, compiled_code) -%}
  {{ deltastream__create_deltastream_table_as(relation, compiled_code, {}) }}
{%- endmacro %}

{% macro deltastream__create_deltastream_table_as(relation, sql, parameters) -%}
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

{% macro deltastream__create_store(resource, parameters) -%}
  create store {{ resource.identifier }}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__update_store(resource, parameters) -%}
  update store {{ resource.identifier }}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__create_compute_pool(resource, parameters) -%}
  create compute_pool {{ resource.identifier }}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__update_compute_pool(resource, parameters) -%}
  update compute_pool {{ resource.identifier }}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__create_entity(resource, parameters, store) -%}
  create entity "{{ resource.identifier }}"{% if store %} in store "{{ store }}"{% endif %}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__update_entity(resource, parameters, store) -%}
  update entity "{{ resource.identifier }}"{% if store %} in store "{{ store }}"{% endif %}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__create_function(resource, parameters) -%}
  create function {{ resource.identifier }}
  {%- if parameters.get('args') %}
    (
    {%- for arg in parameters.get('args') %}
      {{ arg['name'] }} {{ arg['type'] }}{% if not loop.last %}, {% endif %}
    {%- endfor %}
    )
  {%- else %}
    ()
  {%- endif %}
  returns {{ parameters.get('returns', 'VARCHAR') }}
  language {{ parameters.get('language', 'JAVA') }}
  {{ deltastream__with_function_parameters(parameters) }}
  ;
{%- endmacro %}


{% macro deltastream__drop_function(resource, parameters) -%}
  drop function {{ resource.identifier }}
  {%- if parameters.get('args') %}
    (
    {%- for arg in parameters.get('args') %}
      {{ arg['name'] }} {{ arg['type'] }}{% if not loop.last %}, {% endif %}
    {%- endfor %}
    )
  {%- else %}
    ()
  {%- endif %}
  ;
{%- endmacro %}

{% macro deltastream__create_function_source(resource, parameters) -%}
  {%- if parameters.get('file') %}
    {{ adapter.create_function_source_with_file(resource.identifier, parameters) }}
  {%- else %}
    create function_source {{ resource.identifier }}
    {{ deltastream__with_parameters(parameters) }}
    ;
  {%- endif %}
{%- endmacro %}



{% macro deltastream__drop_function_source(resource, parameters) -%}
  drop function_source {{ resource.identifier }}
  ;
{%- endmacro %}

{% macro deltastream__create_descriptor_source(resource, parameters) -%}
  {%- if parameters.get('file') %}
    {{ adapter.create_descriptor_source_with_file(resource.identifier, parameters) }}
  {%- else %}
    create descriptor_source {{ resource.identifier }}
    {{ deltastream__with_parameters(parameters) }}
    ;
  {%- endif %}
{%- endmacro %}



{% macro deltastream__drop_descriptor_source(resource, parameters) -%}
  drop descriptor_source {{ resource.identifier }}
  ;
{%- endmacro %}

{% macro deltastream__create_schema_registry(resource, parameters) -%}
  create schema_registry {{ adapter.quote(resource.identifier) }}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__update_schema_registry(resource, parameters) -%}
  update schema_registry {{ adapter.quote(resource.identifier) }}
  {{ deltastream__with_parameters(parameters) }}
  ;
{%- endmacro %}

{% macro deltastream__drop_schema_registry(resource, parameters) -%}
  drop schema_registry {{ adapter.quote(resource.identifier) }}
  ;
{%- endmacro %}

{% macro deltastream__filter_update_parameters(parameters) -%}
  {%- set filtered_params = {} -%}
  {%- for key, value in parameters.items() -%}
    {%- if key not in ('type', 'access_region') -%}
      {%- set _ = filtered_params.update({key: value}) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(filtered_params) }}
{%- endmacro %}

{% macro deltastream__update_schema_registry_filtered(resource, parameters) -%}
  {%- set filtered_params = deltastream__filter_update_parameters(parameters) -%}
  update schema_registry {{ adapter.quote(resource.identifier) }}
  {{ deltastream__with_parameters(filtered_params) }}
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
      {%- elif value is boolean %}
      '{{ parameter }}' = '{{ value | lower }}'{% if not loop.last %},{% endif %}
      {%- else %}
      '{{ parameter }}' = '{{ value }}'{% if not loop.last %},{% endif %}
      {%- endif %}
    {%- endfor %}
    )
  {% endif %}
{%- endmacro %}

{% macro deltastream__with_function_parameters(parameters) -%}
  {%- set excluded_params = ['args', 'returns', 'language'] -%}
  {%- set filtered_params = {} -%}
  {%- for parameter, value in parameters.items() -%}
    {%- if parameter not in excluded_params -%}
      {%- set _ = filtered_params.update({parameter: value}) -%}
    {%- endif -%}
  {%- endfor -%}
  {% if filtered_params.items() | length > 0 %}
    with (
    {%- for parameter, value in filtered_params.items() %}
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
  {# DeltaStream does not support grants, so this is a no-op #}
  {% if grant_config %}
    {{ log("DeltaStream does not support grants configuration. Skipping grant application.", info=True) }}
  {% endif %}
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

{% macro deltastream__terminate_query(query_id) -%}
  TERMINATE QUERY {{query_id}};
{% endmacro %}


{% macro deltastream__restart_query(query_id) -%}
  RESTART QUERY {{query_id}};
{% endmacro %}

{% macro deltastream__create_application(application_name, statements) -%}
  BEGIN APPLICATION {{ application_name }}
  {%- for statement in statements %}
    {{ statement.rstrip(';') }};
  {%- endfor %}
  END APPLICATION;
{%- endmacro %}
