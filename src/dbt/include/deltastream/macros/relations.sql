{% macro deltastream__get_relations() -%}

  {%- call statement('relations', fetch_result=True) -%}
    LIST RELATIONS;
  {%- endcall -%}

  {{ return(load_result('relations').table) }}
{% endmacro %}

{% macro deltastream_get_relations() %}
  {{ return(deltastream__get_relations()) }}
{% endmacro %}
