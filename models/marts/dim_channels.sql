{{ config(materialized='table') }}

SELECT DISTINCT
    channel_id,
    channel_title,
    channel_username
FROM {{ ref('stg_telegram_messages') }}
WHERE channel_id IS NOT NULL