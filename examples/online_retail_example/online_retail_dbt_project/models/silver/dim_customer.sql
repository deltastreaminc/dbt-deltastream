{{
  config(
    materialized='stream',
    parameters={
      'topic': 'dim_customer',
      'value.format': 'JSON'
    }
  )
}}

-- Customer dimension stream
-- In production, this would come from CDC on the customer master table

SELECT
    customer_id,
    customer_name,
    customer_segment,
    loyalty_tier,
    email
FROM {{ ref('dim_customer_seed') }}
