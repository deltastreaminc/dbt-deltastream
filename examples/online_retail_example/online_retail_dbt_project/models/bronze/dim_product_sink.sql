{{
  config(
    materialized='stream',
    parameters={
      'value.format': 'json',
      'store': 'pubMSK01',
      'topic.partitions': 1,
      'topic.replicas': 3
    }
  )
}}

select 
`after`->product_id as product_id,
`after`->product_name as product_name,
`after`->product_price as product_price
from {{source('kafka_infra', 'dim_product')}} 
with('postgresql.slot.name' = 'slot001', 'value.format' = 'json')
where `after` is not null

