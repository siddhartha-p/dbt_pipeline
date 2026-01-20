{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['client_employee_id', 'date_key','department_name']
) }}

WITH affected_keys AS (
    SELECT DISTINCT
        ge.client_employee_id,
        gd.date_key,
        st.department_name
    FROM {{ ref('stg_timesheets') }} st
    JOIN {{ ref('dim_employees') }} ge
        ON st.client_employee_id = ge.client_employee_id
    JOIN {{ ref('dim_date') }} gd
        ON st.punch_apply_date = gd.date_actual

    {% if is_incremental() %}
    WHERE st.loaded_at >
        (SELECT COALESCE(MAX(loaded_at), '2000-01-01'::timestamp)
         FROM {{ this }})
    {% endif %}
),
aggregated_attendance AS (
    SELECT
        ge.client_employee_id,
        gd.date_key,
        st.department_name,
        MAX(st.home_department_name) AS home_department_name,

        COUNT(*) AS total_punches,
        SUM(st.hours_worked) AS total_hours_worked,

        MIN(st.punch_in_datetime) AS first_punch_in,
        MAX(st.punch_out_datetime) AS last_punch_out,

        SUM(
            CASE
                WHEN st.punch_in_datetime >
                     st.scheduled_start_datetime + INTERVAL '5 minute'
                THEN 1 ELSE 0
            END
        ) AS late_arrival_count,

        SUM(
            CASE
                WHEN st.punch_out_datetime <
                     st.scheduled_end_datetime - INTERVAL '5 minute'
                THEN 1 ELSE 0
            END
        ) AS early_departure_count,

        SUM(
            CASE
                WHEN (st.punch_out_datetime - st.punch_in_datetime) >
                     (st.scheduled_end_datetime - st.scheduled_start_datetime)
                     + INTERVAL '5 minute'
                THEN 1 ELSE 0
            END
        ) AS overtime_count,

        MAX(st.loaded_at) AS loaded_at,
        MAX(st.source_file) AS source_file

    FROM {{ ref('stg_timesheets') }} st
    JOIN {{ ref('dim_employees') }} ge
        ON st.client_employee_id = ge.client_employee_id
    JOIN {{ ref('dim_date') }} gd
        ON st.punch_apply_date = gd.date_actual
    JOIN affected_keys ak
        ON ge.client_employee_id = ak.client_employee_id
       AND gd.date_key = ak.date_key

    GROUP BY
        ge.client_employee_id,
        gd.date_key,
        st.department_name
)
SELECT *
FROM aggregated_attendance
