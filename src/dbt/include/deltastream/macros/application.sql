{% macro application(application_name, statements) %}
  {% if not application_name %}
    {{ exceptions.raise_compiler_error("application_name is required") }}
  {% endif %}
  {% if not statements or statements|length == 0 %}
    {{ exceptions.raise_compiler_error("At least one statement is required") }}
  {% endif %}
  
  {% set sql = deltastream__create_application(application_name, statements) %}
  {% set result = run_query(sql) %}
  {{ log('Created application: ' ~ application_name ~ ' with ' ~ statements|length ~ ' statements', info = True) }}
  {{ return(result) }}
{% endmacro %}
