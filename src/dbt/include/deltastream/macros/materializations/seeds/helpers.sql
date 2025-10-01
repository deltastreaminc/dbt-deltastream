{% macro deltastream__get_csv_sql(insert_sql) %}
    -- dbt seed --
    {{ insert_sql }}
{% endmacro %}

{% macro deltastream__get_batch_size() %}
  {{ return(1000) }}
{% endmacro %}

{% macro deltastream__get_binding_char() %}
  {{ return('%s') }}
{% endmacro %}

{% macro deltastream__quote_seed_column(column_name, quote_columns) %}
    {%- if quote_columns -%}
        {%- if quote_columns is string and quote_columns == '*' -%}
            {{ adapter.quote(column_name) }}
        {%- elif quote_columns is sequence and column_name in quote_columns -%}
            {{ adapter.quote(column_name) }}
        {%- else -%}
            {{ column_name }}
        {%- endif -%}
    {%- else -%}
        {{ column_name }}
    {%- endif -%}
{% endmacro %}

{# Helper macro to convert a value to JSON-serializable type #}
{% macro deltastream__convert_value_to_json_type(value) %}
  {% if value is number %}
    {% set value_str = value | string %}
    {% if '.' in value_str %}
      {% set converted_value = value | float %}
    {% else %}
      {% set converted_value = value | int %}
    {% endif %}
  {% elif value is none %}
    {% set converted_value = null %}
  {% else %}
    {% set converted_value = value | string %}
  {% endif %}
  {{ return(converted_value) }}
{% endmacro %}

{# Helper macro to convert a row to a record dictionary #}
{% macro deltastream__row_to_record_dict(row, column_names) %}
  {% set record_dict = {} %}
  {% for col_name in column_names %}
    {% set idx = loop.index0 %}
    {% set value = row[idx] %}
    {% set converted_value = deltastream__convert_value_to_json_type(value) %}
    {% set record_dict = record_dict.update({col_name: converted_value}) %}
  {% endfor %}
  {{ return(record_dict) }}
{% endmacro %}

{# Helper macro to process agate table rows into JSON records #}
{% macro deltastream__process_agate_rows(agate_table) %}
  {%- set batch_size = deltastream__get_batch_size() -%}
  {% set all_json_records = [] %}
  
  {% for chunk in agate_table.rows | batch(batch_size) %}
    {% for row in chunk %}
      {% set record_dict = deltastream__row_to_record_dict(row, agate_table.column_names) %}
      {% do all_json_records.append(record_dict) %}
    {% endfor %}
  {% endfor %}
  
  {{ return(all_json_records) }}
{% endmacro %}

{# Helper macro to generate INSERT statement for a record #}
{% macro deltastream__generate_insert_statement(record, entity, store, with_params, include_with_params) %}
  {% set json_value = record | tojson | replace("'", "''") %}
  {% set insert_sql -%}
INSERT INTO ENTITY "{{ entity }}"{% if store %} IN STORE "{{ store }}"{% endif %} VALUE('{{ json_value }}'){% if include_with_params %}
{{ deltastream__with_parameters(with_params) }}{% else %};{% endif %}
  {%- endset %}
  {{ return(insert_sql) }}
{% endmacro %}

{% macro deltastream__load_csv_rows(model, agate_table, entity, store) %}
  {%- set with_params = config.get('with_params', {}) or model['config'].get('with_params', {}) -%}
  {% set json_records = deltastream__process_agate_rows(agate_table) %}
  {% set statements = [] %}

  {% for record in json_records %}
    {% set single_sql = deltastream__generate_insert_statement(record, entity, store, with_params, true) %}
    {% do statements.append(single_sql) %}
  {% endfor %}

  {# Return all SQL statements joined together #}
  {% set all_sql = statements | join(';\\n') %}
  {{ return(all_sql) }}
{% endmacro %}

{% macro deltastream__load_csv_rows_as_list(model, agate_table, entity, store) %}
  {% set json_records = deltastream__process_agate_rows(agate_table) %}
  {% set statements = [] %}

  {% for record in json_records %}
    {% set single_sql = deltastream__generate_insert_statement(record, entity, store, {}, false) %}
    {% do statements.append(single_sql.strip()) %}
  {% endfor %}

  {{ return(statements) }}
{% endmacro %}

{% macro deltastream__get_seed_column_quoted_csv(model, column_names) %}
  {%- set quote_seed_column = config.get('quote_columns') or model['config'].get('quote_columns', None) -%}
    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- do quoted.append(deltastream__quote_seed_column(col, quote_seed_column)) -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}
{% endmacro %}
