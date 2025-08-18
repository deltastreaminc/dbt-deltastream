{% macro create_source(node) %}
  {%- set identifier = node['identifier'] -%}
  {%- set parameters = node.config.parameters %}
  {%- set materialized = node.config.get('materialized', 'stream') -%}

  {# Check if it's a resource type #}
  {%- set is_resource = materialized in ['compute_pool', 'store', 'entity', 'function_source', 'descriptor_source', 'function', 'schema_registry'] -%}

  {%- if is_resource %}
    {%- set resource = adapter.create_deltastream_resource(materialized, identifier, parameters) -%}
    {%- set existing_resource = adapter.get_resource(materialized, identifier, parameters) -%}
    {%- set has_existing_resource = existing_resource is not none -%}

    {# Define which resources can be updated vs need to be recreated #}
    {%- set updatable_resources = ['compute_pool', 'store', 'entity', 'schema_registry'] -%}
    {%- set recreatable_resources = ['function_source', 'descriptor_source', 'function'] -%}

    {%- set source_sql %}
      {%- if materialized in updatable_resources %}
        {%- if has_existing_resource %}
          {%- if materialized == 'compute_pool' %}
            {{ deltastream__update_compute_pool(resource, parameters) }}
          {%- elif materialized == 'store' %}
            {{ deltastream__update_store(resource, parameters) }}
          {%- elif materialized == 'entity' %}
            {{ deltastream__update_entity(resource, parameters) }}
          {%- elif materialized == 'schema_registry' %}
            {{ deltastream__update_schema_registry_filtered(resource, parameters) }}
          {%- endif %}
        {%- else %}
          {%- if materialized == 'compute_pool' %}
            {{ deltastream__create_compute_pool(resource, parameters) }}
          {%- elif materialized == 'store' %}
            {{ deltastream__create_store(resource, parameters) }}
          {%- elif materialized == 'entity' %}
            {{ deltastream__create_entity(resource, parameters) }}
          {%- elif materialized == 'schema_registry' %}
            {{ deltastream__create_schema_registry(resource, parameters) }}
          {%- endif %}
        {%- endif %}
      {%- elif materialized in recreatable_resources %}
        {%- if has_existing_resource %}
          {# Drop existing resource first #}
          {%- if materialized == 'function_source' %}
            {%- set drop_sql %}{{ deltastream__drop_function_source(resource, parameters) }}{%- endset %}
            {% set drop_results = run_query(drop_sql) %}
            {{ deltastream__create_function_source(resource, parameters) }}
          {%- elif materialized == 'descriptor_source' %}
            {%- set drop_sql %}{{ deltastream__drop_descriptor_source(resource, parameters) }}{%- endset %}
            {% set drop_results = run_query(drop_sql) %}
            {{ deltastream__create_descriptor_source(resource, parameters) }}
          {%- elif materialized == 'function' %}
            {%- set drop_sql %}{{ deltastream__drop_function(resource, parameters) }}{%- endset %}
            {% set drop_results = run_query(drop_sql) %}
            {{ deltastream__create_function(resource, parameters) }}
          {%- endif %}
        {%- else %}
          {# Create new resource #}
          {%- if materialized == 'function_source' %}
            {{ deltastream__create_function_source(resource, parameters) }}
          {%- elif materialized == 'descriptor_source' %}
            {{ deltastream__create_descriptor_source(resource, parameters) }}
          {%- elif materialized == 'function' %}
            {{ deltastream__create_function(resource, parameters) }}
          {%- endif %}
        {%- endif %}
      {%- endif %}
    {%- endset %}
  
  {# Handle regular relations #}
  {% else %}
    {%- set old_relation = adapter.get_relation(identifier=identifier,
                                              schema=schema,
                                              database=database) -%}
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
      {%- elif materialized == 'database' %}
        {{ deltastream__create_database(identifier) }}
      {%- elif materialized == 'changelog' %}
        {%- set primary_key = node.config.primary_key %}
        {{ deltastream__create_changelog(target_relation, node.columns, parameters, primary_key) }}
      {%- else %}
        {{ exceptions.raise_compiler_error("Unsupported materialization type '" ~ materialized ~ "'. Supported types are: stream, store, database, compute_pool, changelog, entity, function_source, descriptor_source, function, schema_registry") }}
      {%- endif %}
    {%- endset %}
  {% endif %}

  {# Set the operation type based on resource type and existence #}
  {%- if is_resource and has_existing_resource %}
    {%- if materialized in recreatable_resources %}
      {%- set operation = "Recreating" -%}
    {%- else %}
      {%- set operation = "Updating" -%}
    {%- endif %}
  {%- else %}
    {%- set operation = "Creating" -%}
  {%- endif %}

  {{ log(operation ~ " " ~ materialized ~ " " ~ node.identifier ~ "...", info = True) }}
  {% set source_creation_results = run_query(source_sql) %}
  {{ log(operation | replace("ing", "ed") ~ " " ~ materialized ~ " " ~ node.identifier ~ "!", info = True) }}
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