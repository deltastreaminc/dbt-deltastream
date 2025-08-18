{% materialization function_source, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set resource = adapter.create_deltastream_resource('function_source', identifier, parameters) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='function_source') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {% if adapter.get_function_source(identifier) is not none %}
      {{ deltastream__drop_function_source(resource, parameters) }}
      {{ deltastream__create_function_source(resource, parameters) }}
      {{ log('Recreated function source: ' ~ identifier) }}
    {% else %}
      {{ deltastream__create_function_source(resource, parameters) }}
      {{ log('Created function source: ' ~ identifier) }}
    {% endif %}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %} 