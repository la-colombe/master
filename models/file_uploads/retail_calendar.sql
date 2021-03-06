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
	date_trunc('day', date) as date,
	fyear,
	fperiod,
	fweek,
	fday,
	fday_of_week,
	fday_of_period,
	fquarter,
	fday_of_quarter
from google_sheets.retail_calendar
where date_trunc('day', date) = current_date
)

select 

date_trunc('day', date) as date,
fyear,
fperiod,
fweek,
fday,
fday_of_week,
fday_of_period,
fquarter,
fday_of_quarter,
case
	when fday = (select fday from today) - 1 then true
	when (select fday from today) = 1 and fday = (select max(fday) from google_sheets.retail_calendar trc where trc.fyear = rc.fyear - 1) then true
	else false
end as is_yesterday,
case
	when fweek = (select fweek from today) - 1 then true
	when (select fweek from today) = 1 and fweek = 52 then true --(select max(fweek) from google_sheets.retail_calendar trc where trc.fyear = rc.fyear - 1) then true
	else false
end as is_last_week,

case
	when fperiod = (select fperiod from today) - 1 then true
	when (select fperiod from today) = 1 and fperiod =  12 then true
	else false
end as is_last_month,
case
	when fquarter = (select fquarter from today) - 1 then true
	when (select fquarter from today) = 1 and fquarter = 4 then true
	else false
end as is_last_quarter,
case
	when fweek = (select fweek from today) then true
	else false
end as is_this_week,
case
	when fperiod = (select fperiod from today) then true
	else false
end as is_this_month,
case
	when fquarter = (select fquarter from today) then true
	else false
end as is_this_quarter,
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
	when fday_of_quarter < (select fday_of_quarter from today) then true
	else false
end as is_qtd,
case
	when fday < (select fday from today) then true
	else false
end as is_ytd,
case
	when fyear = (select fyear from today) then true
	when fyear = (select fyear from today) - 1 and fday >= (select fday from today) then true
	else false
end as is_within_last_12_months

from google_sheets.retail_calendar rc