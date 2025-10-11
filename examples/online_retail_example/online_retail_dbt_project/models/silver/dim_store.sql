{{
  config(
    materialized='stream',
    parameters={
      'topic': 'dim_store',
      'value.format': 'JSON'
    }
  )
}}

-- Store dimension stream
-- In production, this would come from CDC on the store master table

SELECT
    store_id,
    store_name,
    region,
    city,
    country
FROM {{ ref('dim_store_seed') }}
