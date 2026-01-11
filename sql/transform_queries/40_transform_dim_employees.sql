insert into gold.dim_employees(client_employee_id,fullname,work_email,dob,years_of_experience,job_code,job_title,
		organization_name,department_name,manager_employee_id,job_start_date,
		hire_date,recent_hire_date,anniversary_date,scheduled_weekly_hour,term_date,tenure,is_active,is_rehired,
		is_early_attrition,loaded_at,source_file)
select
	client_employee_id,
	concat_ws(' ',first_name,middle_name,last_name)as fullname,
	work_email,
	dob,
	years_of_experience ,
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
	(case
		when term_date is not null then  term_date - recent_hire_date
		else current_date-recent_hire_date
	end )/365 as tenure,
	active_status as is_active,
	case
		when hire_date=recent_hire_date then false
		else true
	end as is_rehired,
	case
		when term_date -recent_hire_date < 180 then true
		else false
	end as is_early_attrition,
	loaded_at,
	source_file
from silver.employees  
ON CONFLICT (client_employee_id) DO UPDATE SET
    fullname = EXCLUDED.fullname,
    work_email = EXCLUDED.work_email,
    dob = EXCLUDED.dob,
    years_of_experience = EXCLUDED.years_of_experience,
    job_code = EXCLUDED.job_code,
    job_title = EXCLUDED.job_title,
    organization_name = EXCLUDED.organization_name,
    department_name = EXCLUDED.department_name,
    manager_employee_id = EXCLUDED.manager_employee_id,
    job_start_date = EXCLUDED.job_start_date,
    hire_date = EXCLUDED.hire_date,
    recent_hire_date = EXCLUDED.recent_hire_date,
    anniversary_date = EXCLUDED.anniversary_date,
    scheduled_weekly_hour = EXCLUDED.scheduled_weekly_hour,
    term_date = EXCLUDED.term_date,
    tenure = EXCLUDED.tenure,
    is_active = EXCLUDED.is_active,
    is_rehired = EXCLUDED.is_rehired,
    is_early_attrition = EXCLUDED.is_early_attrition,
    loaded_at = EXCLUDED.loaded_at,
    source_file = EXCLUDED.source_file;