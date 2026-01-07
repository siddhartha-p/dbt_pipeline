insert into silver.timesheets(client_employee_id,department_id,department_name,
	home_department_id,home_department_name,pay_code,punch_in_comment,punch_out_comment,
	hours_worked,punch_apply_date,punch_in_datetime,punch_out_datetime,scheduled_start_datetime,
	scheduled_end_datetime,loaded_at,source_file)	
with remove_non_valid_employee_data as (
select 
	t.* 
from bronze.timesheets t join bronze.employees e on t.client_employee_id =e.client_employee_id
),
remove_empty_punch_data as (
select * from remove_non_valid_employee_data where punch_in_datetime <>'' and punch_out_datetime<>''  and punch_apply_date<>''
),
replace_null_strings as (
select 
	 client_employee_id,
	 nullif(department_id,'') as department_id,
	 nullif(department_name,'') as department_name,
	 nullif(home_department_id,'') as home_department_id,
	 nullif(home_department_name,'') as home_department_name,
	 string_to_array(regexp_replace(nullif(pay_code,''),'\s*\|\s*','|','g'),'|') as pay_code,
	 string_to_array(regexp_replace(nullif(punch_in_comment,'[NULL]'),'\s*\|\s*','|','g'),'|') as punch_in_comment,
	 string_to_array(regexp_replace(nullif(punch_out_comment,'[NULL]'),'\s*\|\s*','|','g'),'|') as punch_out_comment,
	 nullif(hours_worked,'') as hours_worked ,
	 punch_apply_date,
	 punch_in_datetime,
	 punch_out_datetime,
	 nullif(scheduled_start_datetime,'') as scheduled_start_datetime,
	 nullif(scheduled_end_datetime,'') as scheduled_end_datetime,
	 loaded_at,
	 source_file
from remove_empty_punch_data
),
casted_data as (
    select  
        client_employee_id,
        department_id,
        department_name,
        home_department_id ,
        home_department_name,
        pay_code,
        punch_in_comment,
        punch_out_comment,
        case
            when hours_worked ~ '^[0-9]+(\.[0-9]+)?$' 
            then cast(hours_worked as numeric(5,2))
            else null 
        end as hours_worked,      
        case 
            when punch_apply_date ~ '^\d{4}-\d{2}-\d{2}$'
                 and punch_apply_date::date is not null
            then cast(punch_apply_date as date)
            else null 
        end as punch_apply_date,     
        case 
            when punch_in_datetime ~ '^\d{4}-\d{2}-\d{2}[ t]\d{2}:\d{2}:\d{2}'
                 and punch_in_datetime::timestamp is not null
            then cast(punch_in_datetime as timestamp)
            else null 
        end as punch_in_datetime,
        case 
            when punch_out_datetime ~ '^\d{4}-\d{2}-\d{2}[ t]\d{2}:\d{2}:\d{2}'
                 and punch_out_datetime::timestamp is not null
            then cast(punch_out_datetime as timestamp)
            else null 
        end as punch_out_datetime,
        case 
            when scheduled_start_datetime ~ '^\d{4}-\d{2}-\d{2}[ t]\d{2}:\d{2}:\d{2}'
                 and scheduled_start_datetime::timestamp is not null
            then cast(scheduled_start_datetime as timestamp)
            else null 
        end as scheduled_start_datetime,       
        case 
            when scheduled_end_datetime ~ '^\d{4}-\d{2}-\d{2}[ t]\d{2}:\d{2}:\d{2}'
                 and scheduled_end_datetime::timestamp is not null
            then cast(scheduled_end_datetime as timestamp)
            else null 
        end as scheduled_end_datetime,
        loaded_at,
        source_file
    from replace_null_strings 
)
select * from casted_data 
where punch_apply_date is not null 
    and punch_in_datetime is not null 
    and punch_out_datetime is not null
    and loaded_at>(select coalesce(max(loaded_at),'2000-01-01'::timestamp) from silver.timesheets)
ON CONFLICT (client_employee_id, punch_apply_date, punch_in_datetime)
DO UPDATE SET
    department_id = EXCLUDED.department_id,
    department_name = EXCLUDED.department_name,
    home_department_id = EXCLUDED.home_department_id,
    home_department_name = EXCLUDED.home_department_name,
    pay_code = EXCLUDED.pay_code,
    punch_in_comment = EXCLUDED.punch_in_comment,
    punch_out_comment = EXCLUDED.punch_out_comment,
    hours_worked = EXCLUDED.hours_worked,
    punch_out_datetime = EXCLUDED.punch_out_datetime,
    scheduled_start_datetime = EXCLUDED.scheduled_start_datetime,
    scheduled_end_datetime = EXCLUDED.scheduled_end_datetime,
    loaded_at = EXCLUDED.loaded_at,
    source_file = EXCLUDED.source_file