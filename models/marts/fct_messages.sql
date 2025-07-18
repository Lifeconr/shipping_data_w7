{{ config(materialized='table') }}

SELECT
    m.message_id,
    m.channel_id,
    m.message_date AS date_id,
    m.message_text,
    m.message_length,
    m.has_image,
    m.media_path,
    m.media_type
FROM {{ ref('stg_telegram_messages') }} m
JOIN {{ ref('dim_channels') }} c ON m.channel_id = c.channel_id
JOIN {{ ref('dim_dates') }} d ON m.message_date = d.date_id