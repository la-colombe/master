{{
  config({
    "materialized" : "table",
    "unique_key" : "unique_key"
    })
}}
select l.unique_key, 
l.employee_code, 
l.date_time, 
l.labor_hours, 
l.employee_title,
c.square as cafe_name
from {{ref('labor_hours')}} l
left join {{ref('cafe_mapping')}} c on c.store_number = l.worked_cost_center