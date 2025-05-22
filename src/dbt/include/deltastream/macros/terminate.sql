{% macro terminate_query(query_id) %}
  {% set sql = deltastream__terminate_query(query_id) %}
  {% set terminate_result = run_query(sql) %}
  {{ log('Terminated query: ' ~ query_id, info = True) }}
{% endmacro %}

{% macro terminate_all_queries(state=None) %}
  {% set sql = "LIST QUERIES;" %}
  {% set result = run_query(sql) %}
  {% set queries = result.rows %}
  {% if state %}
    {% set running_queries = queries | selectattr('ActualState', 'equalto', state) | list %}
  {% else %}
    {% set running_queries = queries %}
  {% endif %}
  {% if running_queries|length == 0 %}
    {{ log('No queries to terminate.' if not state else 'No queries with state ' ~ state ~ ' to terminate.', info=True) }}
  {% else %}
    {% for query in running_queries %}
      {{ terminate_query(query['ID']) }}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro list_all_queries() %}
  {% set sql = "LIST QUERIES;" %}
  {% set result = run_query(sql) %}
  {% set queries = result.rows %}
  {% if queries|length == 0 %}
    {{ log('No queries found.', info=True) }}
  {% else %}
    {% set columns = result.column_names %}
    {% set header = ' | '.join(columns) %}
    {% set separator = '-' * header|length %}
    {{ log('\n' + header, info=True) }}
    {{ log(separator, info=True) }}
    {% for query in queries %}
      {% set ns = namespace(row='') %}
      {% for col in columns %}
        {% if not loop.first %}{% set ns.row = ns.row + ' | ' %}{% endif %}
        {% set value = query[col] %}
        {% set ns.row = ns.row + (value|string if value is not none else '') %}
      {% endfor %}
      {{ log(ns.row, info=True) }}
    {% endfor %}
    {% set first_row = queries[0] %}
    {{ log('First row: ' ~ first_row, info=True) }}
    {% for col in columns %}
      {{ log(col ~ ': ' ~ first_row[col], info=True) }}
    {% endfor %}

  {% endif %}
{% endmacro %}