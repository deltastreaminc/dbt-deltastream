{% macro restart_query(query_id) %}
  {% if not query_id %}
    {{ exceptions.raise_compiler_error("query_id is required") }}
  {% endif %}
  
  {% set sql = deltastream__restart_query(query_id) %}
  {% set restart_result = run_query(sql) %}
  {{ log('Restarted query: ' ~ query_id, info = True) }}
  {{ return(restart_result) }}
{% endmacro %}

{% macro describe_query(query_id) %}
  {% if not query_id %}
    {{ exceptions.raise_compiler_error("query_id is required") }}
  {% endif %}
  
  {% set sql = "DESCRIBE QUERY " ~ query_id ~ ";" %}
  {% set result = run_query(sql) %}
  
  {% set ns = namespace(state=None, error=None) %}
  
  {% for row in result.rows %}
    {% if row['Property'] == 'state' %}
      {% set ns.state = row['Value'] %}
    {% elif row['Property'] == 'error' %}
      {% set ns.error = row['Value'] %}
    {% endif %}
  {% endfor %}
  
  {{ log('Query description for: ' ~ query_id, info=True) }}
  {% if ns.state %}
    {{ log('State: ' ~ ns.state, info=True) }}
  {% endif %}
  {% if ns.error %}
    {{ log('Error: ' ~ ns.error, info=True) }}
  {% endif %}
  
  {{ return(result) }}
{% endmacro %}
