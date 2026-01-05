insert into gold.fact_attendance
select 
    client_employee_id,
    punch_apply_date,
--    max(department_id) as primary_department_id, 
    max(department_name) as department_name,
    max(home_department_name) as home_department_name,
    count(*) as total_punches,
    sum(hours_worked) as total_hours_worked,
    min(punch_in_datetime) as first_punch_in,
    max(punch_out_datetime) as last_punch_out,
    sum(case
    	when 
    		punch_in_datetime>scheduled_start_datetime+interval '5 minute' then 1
    		else 0 
    end )as late_arrival_count,
    sum(case
    	when 
    		punch_out_datetime<scheduled_end_datetime-interval '5 minute' then 1
    		else 0 
    end )as early_departure_count,
    sum(case
			when (punch_out_datetime -punch_in_datetime)>(scheduled_end_datetime-scheduled_start_datetime) + interval '5 minute' then 1
			else 0
    end )as overtime_count,
    max(loaded_at) as loaded_at,
    max(source_file) as source_file 
from silver.timesheets
group by 
    client_employee_id, 
    department_name,
    punch_apply_date
    ;