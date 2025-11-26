{{
  config(
    materialized='stream',
    parameters={
      'value.format': var("value_format"),
      'store':  var("store_name") 
    }
  )
}}

SELECT 
    p.event_time,
    p.event_time_utc,
    p.transaction_id,
    p.product_id,
    p.customer_id,
    p.store_id,
    p.quantity,
    p.unit_price,
    p.discount,
    p.sales_amount,
    p.product_name,
    p.msrp_price,
    c.customer_name,
    c.address_city as customer_address_city,
    c.address_state as customer_address_state

FROM {{ ref('silver_enriched_by_product_info_01') }} p
JOIN {{ ref( 'dim_customer_sink') }} c 
ON p.customer_id = c.customer_id

