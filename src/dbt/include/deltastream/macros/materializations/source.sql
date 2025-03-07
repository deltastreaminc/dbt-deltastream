{% macro create_source(node) %}
  {%- set identifier = node['identifier'] -%}
  {%- set parameters = node.config.parameters %}
  {%- set old_relation = adapter.get_relation(identifier=identifier,
                                            schema=schema,
                                            database=database) -%}
  {%- set materialized = node.config.get('materialized', 'stream') -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                              schema=schema,
                                              database=database,
                                              type="table") -%}

  {% if old_relation %}
    {{ log("Source " ~ old_relation ~ " already exists. Dropping.", info = True) }}
    {{ adapter.drop_relation(old_relation) }}
  {% endif %}

  {%- set source_sql %}
    {%- if materialized == 'stream' %}
      {{ deltastream__create_stream(target_relation, node.columns, parameters) }}
    {%- elif materialized == 'store' %}
      {{ deltastream__create_store(target_relation, parameters) }}
    {%- elif materialized == 'database' %}
      {{ deltastream__create_database(target_relation) }}
    {%- elif materialized == 'changelog' %}
      {%- set primary_key = node.config.primary_key %}
      {{ deltastream__create_changelog(target_relation, node.columns, parameters, primary_key) }}
    {%- endif %}
  {%- endset %}

  {{ log("Creating " ~ materialized ~ " source " ~ node.identifier ~ "...", info = True) }}
  {% set source_creation_results = run_query(source_sql) %}
  {{ log("Created " ~ materialized ~ " source " ~ node.identifier ~ "!", info = True) }}
{% endmacro %}

{% macro create_sources() %}
{% if execute %}
{% for node in graph.sources.values() -%}
  {{ create_source(node) }}
{%- endfor %}
{% endif %}
{% endmacro %}

{% macro create_source_by_name(source_name) %}
{% if execute %}
  {%- set ns = namespace(found_source=None) -%}
  {% for node in graph.sources.values() -%}
    {% if node.name == source_name %}
      {%- set ns.found_source = node %}
      {% break %}
    {% endif %}
  {%- endfor %}

  {% if ns.found_source is none %}
    {{ exceptions.raise_compiler_error("Source '" ~ source_name ~ "' not found in project") }}
  {% else %}
    {{ create_source(ns.found_source) }}
  {% endif %}
{% endif %}
{% endmacro %}