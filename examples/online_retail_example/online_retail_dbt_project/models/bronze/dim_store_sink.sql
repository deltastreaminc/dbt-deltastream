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
`after`->store_id as store_id,
`after`->store_name as store_name,
`after`->store_city as store_city,
`after`->store_state as store_state
from {{source('kafka_infra', 'dim_store')}} 
with('postgresql.slot.name' = 'slot001', 'value.format' = 'json')
where `after` is not null

