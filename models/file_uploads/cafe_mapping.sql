{{
  config({
    "materialized" : "table",
    "unique_key" : "store_number"
    })
}}

select 

store_number,
name,
display_name,
store_type,
region,
open_date,
comp_date,
timezone,

--Mappings
micros,
square,
warehouse,
sage,

--Attributes
cc,
st,
t,
cust,
loc,

--Calculated Column
case
	when comp_date > current_date then 'Non Comp'
	else 'Comp'
end as is_comp

from google_sheets.cafe_mapping