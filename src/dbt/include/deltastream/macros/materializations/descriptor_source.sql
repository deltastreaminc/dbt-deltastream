{% materialization descriptor_source, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set resource = adapter.create_deltastream_resource('descriptor_source', identifier, parameters) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='descriptor_source') -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {% if adapter.get_descriptor_source(identifier) is not none %}
      {{ deltastream__drop_descriptor_source(resource, parameters) }}
      {{ deltastream__create_descriptor_source(resource, parameters) }}
      {{ log('Recreated descriptor source: ' ~ identifier) }}
    {% else %}
      {{ deltastream__create_descriptor_source(resource, parameters) }}
      {{ log('Created descriptor source: ' ~ identifier) }}
    {% endif %}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %} 