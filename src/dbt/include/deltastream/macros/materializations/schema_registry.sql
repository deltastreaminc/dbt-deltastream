{% materialization schema_registry, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set resource = adapter.create_deltastream_resource('schema_registry', identifier, parameters) -%}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {%- set existing_resource = adapter.get_schema_registry_details(identifier) -%}
    {% if existing_resource is not none %}
      {%- set should_recreate = deltastream__should_recreate_schema_registry(existing_resource.parameters, parameters) -%}
      {% if should_recreate %}
        {{ log('Type or access_region changed for schema registry: ' ~ identifier ~ '. Dropping and recreating.') }}
        {{ deltastream__drop_schema_registry(resource, parameters) }}
        {{ deltastream__create_schema_registry(resource, parameters) }}
        {{ log('Recreated schema registry: ' ~ identifier) }}
      {% else %}
        {{ log('Updating schema registry: ' ~ identifier) }}
        {{ deltastream__update_schema_registry_filtered(resource, parameters) }}
        {{ log('Updated schema registry: ' ~ identifier) }}
      {% endif %}
    {% else %}
      {{ deltastream__create_schema_registry(resource, parameters) }}
      {{ log('Created schema registry: ' ~ identifier) }}
    {% endif %}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'resources': [resource]}) }}
{% endmaterialization %} 