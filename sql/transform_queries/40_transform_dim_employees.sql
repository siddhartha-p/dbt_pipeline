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
from silver.employees  ;