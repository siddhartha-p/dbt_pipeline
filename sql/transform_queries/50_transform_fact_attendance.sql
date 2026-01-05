insert into gold.fact_attendance
select 
    ge.employee_key,
    gd.date_key, 
    st.department_name,
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
    max(st.loaded_at) as loaded_at,
    max(st.source_file) as source_file 
from silver.timesheets st join gold.dim_employees ge on st.client_employee_id =ge.client_employee_id
join gold.dim_date gd on st.punch_apply_date =gd.date_actual
group by 
    ge.employee_key, 
    st.department_name,
    gd.date_key
    ;
