{% macro test_positive_value(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endmacro %}


{% macro test_valid_discount_range(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 0 OR {{ column_name }} > 1

{% endmacro %}


{% macro test_sales_amount_matches_calculation(model) %}

SELECT *
FROM {{ model }}
WHERE ABS(sales_amount - (quantity * unit_price * (1 - discount))) > 0.01

{% endmacro %}

{% macro drop_stream(stream_name) %}
  drop stream {{stream_name}}
{% endmacro %}
