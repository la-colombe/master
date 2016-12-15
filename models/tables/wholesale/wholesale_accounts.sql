{{
  config({
    "materialized" : "table",
    "sort" : "second_paid_coffee_invoice_date",
    "unique_key" : "account_id"
    })
}}

select 

a.name,
a.second_paid_coffee_invoice_date,
a.first_invoice_date,
a.most_recent_invoice_date,
a.division,
--a.sales_rep_id,
--a.regional_manager_id,
--a.divisional_manager_id,
a.customer_code,
a.company_code,
--a.primary_account_manager_id,
--a.secondary_account_manager_id,
--a.call_frequency,
a.min_vol,
a.new_tier,
a.sales_rep_name,
a.primary_account_manager_name,
a.secondary_account_manager_name,
a.total_invoices,
a.total_paid_coffee_invoices,
a.total_quantity,
a.total_extension,
a.total_weight,
a.total_coffee_weight,
a.total_coffee_extension,
a.weeks_active,
a.average_weekly_coffee_revenue,
a.average_weekly_coffee_volume,
a.average_coffee_price,
a.total_asset_value,
a.total_invested_asset_value,
a.total_customer_owned_asset_value,
a.overdue_balance_30_day,
a.overdue_balance_60_day,
a.overdue_balance_90_day,
a.overdue_balance_120_day,
a.average_days_overdue,
a.current_balance,


case
    when a.customer_code in (select warehouse from analytics.cafe_mapping) then 'Cafe'
    when left(a.customer_code,2)  = '01' then 'Philly'
    when left(a.customer_code,2)  = '15' then 'CPG'
  	when left(a.customer_code,2)  = '20' or left(a.customer_code,2)  = '40' or left(a.customer_code,2)  = '55' then 'New York'
  	when left(a.customer_code,2)  = '30' then 'DC'
  	when left(a.customer_code,2)  = '50' then 'Chicago'
  	when left(a.customer_code,2)  = '60' or left(a.customer_code,2)  = '25' then 'West Coast'
    when left(a.customer_code,2)  = '61' then 'California'
    when left(a.customer_code,2)  = '64' then 'Florida'
    when left(a.customer_code,2)  = '65' then 'Boston'
  	when left(a.customer_code,2)  = '70' then 'Nationals'
	else 'Other'
end as region,

case
  when a.customer_code like '15%' then 'CPG'
  when a.customer_code in (select warehouse from analytics.cafe_mapping) then 'Internal'
  when a.customer_code like '%LCTX%' or a.customer_code like '%LACX%' then 'Internal'
  else 'Hospitality'
end as customer_type,

case 
  when substring(a.customer_code, 5, 1) = 'C' then 'Cafe'
  when substring(a.customer_code, 5, 1) = 'D' then 'Distributor'
  when substring(a.customer_code, 5, 1) = 'S' then 'Small Market/Retail'
  when substring(a.customer_code, 5, 1) = 'R' then 'Restaurant'
  when substring(a.customer_code, 5, 1) = 'P' then 'People'
  when substring(a.customer_code, 5, 1) = 'O' then 'Office'
  when substring(a.customer_code, 5, 1) = 'H' then 'Hotels'
  when substring(a.customer_code, 5, 1) = 'G' then 'Groups'
  when substring(a.customer_code, 5, 1) = 'F' then 'Catering/Bakery'
end as business_type,

rc.fday as second_paid_coffee_invoice_date_fday,
rc.fweek as second_paid_coffee_invoice_date_fweek,
rc.fperiod as second_paid_coffee_invoice_date_fperiod,
rc.fyear as second_paid_coffee_invoice_date_fyear,
rc.fquarter as second_paid_coffee_invoice_date_fquarter,
rc.fday_of_week second_paid_coffee_invoice_date_fday_of_week,
rc.fday_of_period second_paid_coffee_invoice_date_fday_of_period,
rc.is_comp as is_comp


from {{ref('warehouse_accounts')}} a
left join {{ref('retail_calendar')}} rc on rc.date = date_trunc('day', a.second_paid_coffee_invoice_date)