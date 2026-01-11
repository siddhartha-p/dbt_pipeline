INSERT INTO gold.dim_date
SELECT
    TO_CHAR(datum, 'yyyymmdd')::INT AS date_key,
    datum AS date_actual,
    TO_CHAR(datum, 'Day') AS day_name,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    EXTRACT(DAY FROM datum) AS day_of_month,
    EXTRACT(MONTH FROM datum) AS month_actual,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(ISOYEAR FROM datum) AS year,
    CASE
        WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    FALSE AS is_holiday, 
    (datum = (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE) AS is_last_day_of_month
FROM (SELECT '2025-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 730) AS SEQUENCE(DAY)) DQ
ON CONFLICT (date_key) DO NOTHING;