{{
  config(
    materialized='table',
    parameters={
      'store': 'iceberg_store',
      'location': 's3://{{ env_var("S3_BUCKET", "deltastream-bronze") }}/bronze/fact_sales/'
    }
  )
}}

-- Bronze layer: Immutable raw data for compliance and replay capability
-- This table stores all sales transactions exactly as received from the source

SELECT
    transaction_id,
    event_time,
    product_id,
    customer_id,
    store_id,
    quantity,
    unit_price,
    discount,
    sales_amount,
    CURRENT_TIMESTAMP AS ingestion_time
FROM {{ source('kafka_infra', 'fact_sales_raw') }}
