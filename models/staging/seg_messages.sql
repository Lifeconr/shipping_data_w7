{{ config(materialized='view') }}

SELECT
  message_id,
  channel_id,
  date::DATE,
  COALESCE(text, '') AS text,
  media_path,
  channel_title
FROM messages
WHERE text IS NOT NULL