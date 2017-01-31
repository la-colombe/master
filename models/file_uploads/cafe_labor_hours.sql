{{
  config({
    "materialized" : "table",
    "unique_key" : "unique_key"
    })
}}
select distinct md5(employee_code || start_time || end_time || home_cost_center || worked_cost_center) as unique_key, employee_code, start_time, end_time, home_cost_center, worked_cost_center
from file_uploads.cafe_labor_hours