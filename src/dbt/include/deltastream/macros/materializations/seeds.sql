{% materialization seed, adapter='deltastream' %}

  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set grant_config = config.get('grants') -%}
  {%- set agate_table = load_agate_table() -%}
  -- grab current tables grants config for comparison later on

  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- Validate that entity is configured
  {%- set entity = config.get('entity') or model['config'].get('entity') -%}
  {%- set store = config.get('store') or model['config'].get('store') -%}
  {%- if not entity -%}
    {{ exceptions.raise_compiler_error(
        "DeltaStream seeds must have 'entity' configured in the seed YAML configuration. " ~
        "Example: config: { entity: 'my_entity' }. Store is optional."
    ) }}
  {%- endif -%}

  -- Check if entity exists and fail if it doesn't
  {% if adapter.get_entity(entity, store) is none %}
    {{ exceptions.raise_compiler_error(
        "Entity '" ~ entity ~ "'" ~ ((" in store '" ~ store ~ "'") if store else "") ~ 
        " does not exist. Please create the entity first before seeding data into it."
    ) }}
  {% endif %}

  -- build model
  {% set code = 'INSERT' %}
  {% set rows_affected = (agate_table.rows | length) %}
  {% set statements_list = deltastream__load_csv_rows_as_list(model, agate_table, entity, store) %}

  {% for sql_statement in statements_list %}
    {% set result = run_query(sql_statement) %}
  {% endfor %}

  {% call noop_statement('main', code ~ ' ' ~ rows_affected, code, rows_affected) %}
    Executed {{ statements_list | length }} INSERT statements
  {% endcall %}

  {% set target_relation = this.incorporate(type='table') %}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
