{% materialization stream, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}

  {%- set old_relation = adapter.get_relation(identifier=identifier,
                                              schema=schema,
                                              database=database) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='stream') -%}

  {% if old_relation %}
    {{ log("Stream " ~ old_relation ~ " already exists. Dropping.", info = True) }}
    {{ adapter.drop_relation(old_relation) }}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {%- if deltastream__sql_contains_select(sql) %}
      {{ deltastream__create_stream_as(target_relation, sql, parameters) }}
    {%- else %}
      {{ deltastream__create_stream(target_relation, model.columns, parameters) }}
    {%- endif %}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
