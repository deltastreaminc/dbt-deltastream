{% materialization changelog, adapter='deltastream' %}
  {%- set identifier = model['alias'] -%}
  {%- set parameters = config.get('parameters', {}) %}
  {%- set old_relation = adapter.get_relation(identifier=identifier,
                                              schema=schema,
                                              database=database) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='changelog') -%}

  {% if old_relation %}
    {{ log("Changelog " ~ old_relation ~ " already exists. Dropping.", info = True) }}
    {{ adapter.drop_relation(old_relation) }}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% call statement('main') -%}
    {%- if deltastream__sql_contains_select(sql) %}
      {{ deltastream__create_changelog_as(target_relation, sql, parameters) }}
    {%- else %}
      {%- set primary_key = config.get('primary_key') -%}
      {{ deltastream__create_changelog(target_relation, model.columns, parameters, primary_key) }}
    {%- endif %}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
