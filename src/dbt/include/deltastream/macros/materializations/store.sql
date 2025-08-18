{% materialization store, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set resource = adapter.create_deltastream_resource('store', identifier, parameters) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='store') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {% if adapter.get_store(identifier) is not none %}
      {{ deltastream__update_store(resource, parameters) }}
      {{ log('Updated store: ' ~ identifier) }}
    {% else %}
      {{ deltastream__create_store(resource, parameters) }}
      {{ log('Created store: ' ~ identifier) }}
    {% endif %}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}