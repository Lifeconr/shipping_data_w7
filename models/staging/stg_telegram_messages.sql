{{ config(materialized='view') }}

SELECT
    message_id,
    channel_id,
    date::DATE AS message_date,
    COALESCE(text, '') AS message_text,
    media_path,
    media_type,
    channel_title,
    channel_username,
    LENGTH(COALESCE(text, '')) AS message_length,
    CASE WHEN media_path IS NOT NULL THEN TRUE ELSE FALSE END AS has_image
FROM raw.telegram_messages
WHERE message_id IS NOT NULL
  AND channel_id IS NOT NULL