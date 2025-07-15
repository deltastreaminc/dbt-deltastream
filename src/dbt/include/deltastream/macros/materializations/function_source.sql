{% materialization function_source, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set resource = adapter.create_deltastream_resource('function_source', identifier, parameters) -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {% if adapter.get_function_source(identifier) is not none %}
      {{ deltastream__drop_function_source(resource, parameters) }}
      {{ log('Dropped existing function_source: ' ~ identifier) }}
    {% endif %}
    {{ deltastream__create_function_source(resource, parameters) }}
    {{ log('Created function_source: ' ~ identifier) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'resources': [resource]}) }}
{% endmaterialization %} 