{% materialization function, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set resource = adapter.create_deltastream_resource('function', identifier, parameters) -%}

  {{ run_hooks(pre_hooks) }}

  {% if adapter.get_function(identifier, parameters) is not none %}
    {% call statement('drop_function') -%}
      {{ deltastream__drop_function(resource, parameters) }}
    {%- endcall %}
    {{ log('Dropped existing function: ' ~ identifier) }}
  {% endif %}
  
  {% call statement('create_function') -%}
    {{ deltastream__create_function(resource, parameters) }}
  {%- endcall %}
  {{ log('Created function: ' ~ identifier) }}

  {{ run_hooks(post_hooks) }}

  {{ return({'resources': [resource]}) }}
{% endmaterialization %} 