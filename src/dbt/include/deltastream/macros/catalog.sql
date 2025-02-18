{% macro deltastream__get_catalog(information_schema, schemas) -%}
  
  {%- call statement('catalog', fetch_result=True) -%}
      select
          database_name,
          schema_name,
          name,
          relation_type,
          primary_key,
          "owner"
      from deltastream.sys."relations"
      where database_name = '{{ database }}'
        and schema_name in (
            {%- for schema in schemas -%}
                '{{ schema }}'{% if not loop.last %}, {% endif %}
            {%- endfor -%}
        );
  {%- endcall %}

  {%- set catalog_result = load_result('catalog').table -%}
  {%- set renamed = adapter.rename_catalog_columns(catalog_result) -%}
  
  {{ return(renamed) }}
{%- endmacro %}