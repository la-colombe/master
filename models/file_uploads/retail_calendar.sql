{{
  config({
    "materialized" : "table",
    "unique_key" : "date",
    "sort" : "date",
    "dist" : "date",
    })
}}

with today as(
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
where date = current_date
)

select 

date,
fyear,
fperiod,
fweek,
fday,
fday_of_week,
fday_of_period,
CEIL(fperiod::float / 3) as fquarter,
case
	when fweek = (select fweek from today) - 1 then true
	else false
end as is_last_week,
case
	when fperiod = (select fperiod from today) - 1 then true
	else false
end as is_last_month,
case
	when fyear = (select fyear from today) then true
	else false
end as is_ty,
case
	when fyear = (select fyear from today) -1 then true
	else false
end as is_ly,
case
	when fday_of_week < (select fday_of_week from today) then true
	else false
end as is_wtd,
case
	when fday_of_period < (select fday_of_period from today) then true
	else false
end as is_mtd,
case
	when fquarter = (select fquarter from today) 
		and fperiod < (select fperiod from today)
		then true
	when fquarter = (select fquarter from today) 
		and fperiod = (select fperiod from today) 
		and fday_of_period < (select fday_of_period from today) then true
	else false
end as is_qtd,
case
	when fday < (select fday from today) then true
	else false
end as is_ytd

from public.retail_calendar