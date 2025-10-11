{{
  config(
    materialized='stream',
    parameters={
      'topic': 'dim_product',
      'value.format': 'JSON'
    }
  )
}}

-- Product dimension stream
-- In production, this would come from CDC on the product master table

SELECT
    product_id,
    product_name,
    category,
    brand,
    unit_cost
FROM {{ ref('dim_product_seed') }}
