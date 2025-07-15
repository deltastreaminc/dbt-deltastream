{% materialization descriptor_source, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set resource = adapter.create_deltastream_resource('descriptor_source', identifier, parameters) -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {% if adapter.get_descriptor_source(identifier) is not none %}
      {{ deltastream__drop_descriptor_source(resource, parameters) }}
      {{ log('Dropped existing descriptor_source: ' ~ identifier) }}
    {% endif %}
    {{ deltastream__create_descriptor_source(resource, parameters) }}
    {{ log('Created descriptor_source: ' ~ identifier) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'resources': [resource]}) }}
{% endmaterialization %} 