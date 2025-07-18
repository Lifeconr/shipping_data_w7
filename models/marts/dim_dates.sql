{{ config(materialized='table') }}

WITH date_range AS (
    SELECT generate_series(
        '2025-01-01'::DATE,
        '2025-12-31'::DATE,
        interval '1 day'
    ) AS date
)
SELECT
    date AS date_id,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(DOW FROM date) AS day_of_week,
    TO_CHAR(date, 'Month') AS month_name,
    TO_CHAR(date, 'Day') AS day_name
FROM date_range