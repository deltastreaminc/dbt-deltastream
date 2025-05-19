{{
  config(
    materialized = 'incremental',
    unique_key = ['userid', 'viewtime'],
    incremental_strategy = 'merge'
  )
}}

SELECT
    p.viewtime,
    p.userid,
    p.pageid,
    u.regionid,
    u.gender,
    u.interests,
    u.contactinfo,
    u.registertime
FROM {{ source('bronze', 'pageviews') }} p
LEFT JOIN {{ source('bronze', 'users_log') }} u
    ON p.userid = u.userid
{% if is_incremental() %}
    WHERE p.viewtime > (SELECT MAX(viewtime) FROM {{ this }})
{% endif %}