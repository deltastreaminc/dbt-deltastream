{% materialization entity, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set store = parameters.get('store', None) %}
  {%- set resource = adapter.create_deltastream_resource('entity', identifier, parameters) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='entity') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {% if adapter.get_entity(identifier, store) is not none %}
      {{ deltastream__update_entity(resource, parameters, store) }}
      {{ log('Updated entity: ' ~ identifier) }}
    {% else %}
      {{ deltastream__create_entity(resource, parameters, store) }}
      {{ log('Created entity: ' ~ identifier) }}
    {% endif %}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}