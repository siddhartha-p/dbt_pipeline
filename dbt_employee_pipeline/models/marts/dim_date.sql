{{ config(
    materialized='incremental',
    unique_key='date_key'
) }}

WITH dates AS (
    SELECT
        TO_CHAR(datum, 'YYYYMMDD')::INT AS date_key,
        datum AS date_actual,
        TO_CHAR(datum, 'Day') AS day_name,
        EXTRACT(ISODOW FROM datum)::INT AS day_of_week,
        EXTRACT(DAY FROM datum)::INT AS day_of_month,
        EXTRACT(MONTH FROM datum)::INT AS month_actual,
        TO_CHAR(datum, 'Month') AS month_name,
        EXTRACT(QUARTER FROM datum)::INT AS quarter,
        EXTRACT(ISOYEAR FROM datum)::INT AS year,
        CASE WHEN EXTRACT(ISODOW FROM datum) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday,
        (datum = (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE) AS is_last_day_of_month
    FROM GENERATE_SERIES('2025-01-01'::DATE, '2026-12-31'::DATE, INTERVAL '1 day') AS datum
)

{% if is_incremental() %}
SELECT *
FROM dates
WHERE date_key NOT IN (SELECT date_key FROM {{ this }})
{% else %}
SELECT *
FROM dates
{% endif %}
