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
`after`->customer_id as customer_id,
`after`->customer_name as customer_name,
`after`->address_city as address_city,
`after`->address_state as address_state
from {{source('kafka_infra', 'dim_customer')}} 
with('postgresql.slot.name' = 'slot001', 'value.format' = 'json')
where `after` is not null
