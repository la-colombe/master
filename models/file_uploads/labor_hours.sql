select distinct md5(l.employee_code || l.start_time || l.end_time || l.home_cost_center || l.worked_cost_center) as unique_key, 
l.employee_code, 
l.start_time, 
l.end_time, 
l.home_cost_center, 
l.worked_cost_center, 
t.title as employee_title,
from file_uploads.cafe_labor_hours l
left join file_uploads.employee_titles t on t.employee_code = l.employee_code