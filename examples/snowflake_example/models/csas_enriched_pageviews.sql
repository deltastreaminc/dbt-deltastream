{{
  config(
    materialized='stream',
    parameters={
      'value.format': 'json',
      'key.format': 'JSON',
      'store': 'trial_store'
    }
  )
}}
SELECT 
    TO_TIMESTAMP_LTZ(viewtime, 3) AS viewtime,
    p.userid AS userid,
    pageid,
    TO_TIMESTAMP_LTZ(registertime, 3) AS registertime,
    regionid,
    gender,
    interests,
    contactinfo
FROM {{ source('kafka', 'pageviews') }} p
JOIN {{ source('kafka', 'users_log') }} u ON u.userid = p.userid