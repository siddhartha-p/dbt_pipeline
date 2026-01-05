CREATE TABLE IF NOT EXISTS gold.fact_attendance (
    client_employee_id VARCHAR(100) NOT NULL,
    punch_apply_date date not null,
    department_name VARCHAR(255),
    home_department_name VARCHAR(255),
    total_punches INTEGER NOT NULL,
    total_hours_worked numeric(5,2),
    first_punch_in TIMESTAMP NOT NULL,
    last_punch_out TIMESTAMP NOT NULL,
    late_arrival_count INTEGER,
    early_departure_count integer,
    overtime_count integer,
    loaded_at TIMESTAMP NOT NULL,
    source_file VARCHAR(500) NOT NULL
)