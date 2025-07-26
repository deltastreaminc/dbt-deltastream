{% materialization compute_pool, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set resource = adapter.create_deltastream_resource('compute_pool', identifier, parameters) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='compute_pool') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {% if adapter.get_compute_pool(identifier) is not none %}
      {{ deltastream__update_compute_pool(resource, parameters) }}
      {{ log('Updated compute pool: ' ~ identifier) }}
    {% else %}
      {{ deltastream__create_compute_pool(resource, parameters) }}
      {{ log('Created compute pool: ' ~ identifier) }}
    {% endif %}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}