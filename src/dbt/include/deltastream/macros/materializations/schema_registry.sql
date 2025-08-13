{% materialization schema_registry, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set resource = adapter.create_deltastream_resource('schema_registry', identifier, parameters) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='schema_registry') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {% if adapter.get_schema_registry(identifier) is not none %}
      {{ deltastream__update_schema_registry_filtered(resource, parameters) }}
      {{ log('Updated schema registry: ' ~ identifier) }}
    {% else %}
      {{ deltastream__create_schema_registry(resource, parameters) }}
      {{ log('Created schema registry: ' ~ identifier) }}
    {% endif %}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %} 