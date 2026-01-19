{{ config(
    materialized='incremental',
    unique_key='client_employee_id'
) }}

WITH bronze_employees AS (

    SELECT
        client_employee_id,
        first_name,
        middle_name,
        last_name,
        preferred_name,
        job_code,
        job_title,
        job_start_date::date,
        organization_id,
        organization_name,
        department_id,
        TRIM(
            REGEXP_REPLACE(department_name, '^' || department_id || '\s*-\s*', '', 'i')
        ) AS department_name,
        dob::date,
        hire_date::date,
        recent_hire_date::date,
        (recent_hire_date::date + interval '1 year')::date AS anniversary_date,
        nullif(term_date,'')::date AS term_date,
        nullif(years_of_experience,'')::numeric(4,1) AS years_of_experience,
        work_email,
        address,
        city,
        state,
        zip,
        country,
        manager_employee_id,
        manager_employee_name,
        fte_status,
        nullif(is_per_deim,'')::boolean AS is_per_deim,
        cell_phone,
        work_phone,
        nullif(scheduled_weekly_hour,'')::integer AS scheduled_weekly_hour,
        nullif(active_status,'')::boolean AS active_status,
        termination_reason,
        clinical_level,
        loaded_at,
        source_file
    FROM {{ source('bronze', 'employees') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT COALESCE(MAX(loaded_at), '2000-01-01'::timestamp) FROM {{ this }})
    {% endif %}

)

SELECT * FROM bronze_employees