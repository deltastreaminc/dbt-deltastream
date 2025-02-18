{% materialization table, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set old_relation = adapter.get_relation(identifier=identifier,
                                              schema=schema,
                                              database=database) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {% if old_relation %}
    {{ adapter.drop_relation(old_relation) }}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {{ deltastream__create_table_as(target_relation, sql, parameters) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
