```sql
{{ config(materialized='table') }}

SELECT
    d.detection_id,
    d.message_id,
    d.image_path,
    d.detected_object_class,
    d.confidence_score
FROM raw.image_detections d
JOIN {{ ref('fct_messages') }} m ON d.message_id = m.message_id
WHERE d.confidence_score >= 0.5  -- Filter low-confidence detections