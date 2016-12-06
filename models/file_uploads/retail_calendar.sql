{{
  config({
    "materialized" : "table",
    "unique_key" : "date",
    "sort" : "date",
    "dist" : "date",
    })
}}

select 

date,
fyear,
fperiod,
fweek,
fday,
fday_of_week,
fday_of_period,
CEIL(fperiod::float / 3) as fquarter

from public.retail_calendar