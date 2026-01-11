
DELETE FROM gold.fact_attendance
WHERE (employee_key, date_key) IN (    
    SELECT DISTINCT 
        ge.employee_key,
        gd.date_key
    FROM silver.timesheets st 
    JOIN gold.dim_employees ge ON st.client_employee_id = ge.client_employee_id
    JOIN gold.dim_date gd ON st.punch_apply_date = gd.date_actual
    WHERE st.loaded_at > (
        SELECT COALESCE(MAX(loaded_at), '2000-01-01'::timestamp) 
        FROM gold.fact_attendance
    )
);

INSERT INTO gold.fact_attendance
SELECT 
    ge.employee_key,
    gd.date_key, 
    st.department_name,
    MAX(st.home_department_name) AS home_department_name,
    COUNT(*) AS total_punches,
    SUM(hours_worked) AS total_hours_worked,
    MIN(punch_in_datetime) AS first_punch_in,
    MAX(punch_out_datetime) AS last_punch_out,
    SUM(CASE
        WHEN punch_in_datetime > scheduled_start_datetime + INTERVAL '5 minute' 
        THEN 1 ELSE 0 
    END) AS late_arrival_count,
    SUM(CASE
        WHEN punch_out_datetime < scheduled_end_datetime - INTERVAL '5 minute' 
        THEN 1 ELSE 0 
    END) AS early_departure_count,
    SUM(CASE
        WHEN (punch_out_datetime - punch_in_datetime) > 
             (scheduled_end_datetime - scheduled_start_datetime) + INTERVAL '5 minute' 
        THEN 1 ELSE 0
    END) AS overtime_count,
    MAX(st.loaded_at) AS loaded_at,
    MAX(st.source_file) AS source_file 
FROM silver.timesheets st 
JOIN gold.dim_employees ge ON st.client_employee_id = ge.client_employee_id
JOIN gold.dim_date gd ON st.punch_apply_date = gd.date_actual
WHERE (ge.employee_key, gd.date_key) IN (
    SELECT DISTINCT 
        ge2.employee_key,
        gd2.date_key
    FROM silver.timesheets st2 
    JOIN gold.dim_employees ge2 ON st2.client_employee_id = ge2.client_employee_id
    JOIN gold.dim_date gd2 ON st2.punch_apply_date = gd2.date_actual
    WHERE st2.loaded_at > (
        SELECT COALESCE(MAX(loaded_at), '2000-01-01'::timestamp) 
        FROM gold.fact_attendance
    )
)
GROUP BY 
    ge.employee_key, 
    st.department_name,
    gd.date_key;    