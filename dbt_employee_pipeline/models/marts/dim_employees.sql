{{ config(
    materialized='incremental',
    unique_key='client_employee_id'
) }}

SELECT
    client_employee_id,

    concat_ws(' ', first_name, middle_name, last_name) AS fullname,

    work_email,
    dob,
    years_of_experience,
    job_code,
    job_title,
    organization_name,
    department_name,
    manager_employee_id,
    job_start_date,
    hire_date,
    recent_hire_date,
    anniversary_date,
    scheduled_weekly_hour,
    term_date,

    -- tenure in years
    (
        CASE
            WHEN term_date IS NOT NULL
                THEN term_date - recent_hire_date
            ELSE current_date - recent_hire_date
        END
    ) / 365 AS tenure,

    active_status AS is_active,

    CASE
        WHEN hire_date = recent_hire_date THEN FALSE
        ELSE TRUE
    END AS is_rehired,

    CASE
        WHEN term_date IS NOT NULL
             AND (term_date - recent_hire_date) < 180
            THEN TRUE
        ELSE FALSE
    END AS is_early_attrition,

    loaded_at,
    source_file

FROM {{ ref('stg_employees') }}   -- silver.employees model

{% if is_incremental() %}
WHERE loaded_at >
      (SELECT COALESCE(MAX(loaded_at), '2000-01-01'::timestamp)
       FROM {{ this }})
{% endif %}
