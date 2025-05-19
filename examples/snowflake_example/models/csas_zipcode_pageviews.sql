{{
  config(
    materialized = 'incremental',
    unique_key = ['userid', 'viewtime'],
    incremental_strategy = 'merge'
  )
}}

SELECT
    viewtime,
    userid,
    pageid,
    contactinfo:zipcode as zipcode 
FROM {{ ref('csas_enriched_pageviews') }}
WHERE contactinfo:zipcode is not null
{% if is_incremental() %}
    AND viewtime > (SELECT MAX(viewtime) FROM {{ this }})
{% endif %}