select distinct md5(l.employee_code || l.worked_cost_center || "datetime" || round(datediff(minute, new_datetime, next_hour)::decimal(16,2) / 60, 2)) as unique_key, 
l.employee_code, 
worked_cost_center, "datetime" as date_time, 
round(datediff(minute, new_datetime, next_hour)::decimal(16,2) / 60, 2) as labor_hours,
t.title as employee_title
from (
select *,
case
  when date_trunc('hour', start_time) = datetime then start_time
  else datetime
end as new_datetime,
nvl(lead(datetime) over (partition by employee_code, worked_cost_center, start_time order by employee_code, worked_cost_center, datetime), end_time) as next_hour
from file_uploads.cafe_labor_hours l
left join file_uploads.hourly_calendar h on h.datetime >= date_trunc('hour', start_time)  and h.datetime <= end_time
) l
left join file_uploads.employee_titles t on t.employee_code = l.employee_code
where datediff(minute, new_datetime, next_hour) > 0
