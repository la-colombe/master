{{
  config({
    "materialized" : "table",
    "sort" : "second_paid_coffee_invoice_date",
    "unique_key" : "account_id"
    })
}}

select 

a.name,
case
  when s.new_second_coffee_order_date is not null then s.new_second_coffee_order_date
  else a.second_paid_coffee_invoice_date
end as second_paid_coffee_invoice_date,
case
  when s.new_second_coffee_order_date is not null then dateadd(day, 365, s.new_second_coffee_order_date)
  else dateadd(day, 365, a.second_paid_coffee_invoice_date) 
end as comp_date,
a.first_invoice_date,
a.most_recent_invoice_date,
a.division,
a.average_time_since_previous_paid_coffee_invoice,
--a.sales_rep_id,
--a.regional_manager_id,
--a.divisional_manager_id,
a.customer_code,
a.company_code,
--a.primary_account_manager_id,
--a.secondary_account_manager_id,
--a.call_frequency,
--nvl(a.min_vol,0) as min_vol,
a.min_vol,
a.new_tier,
a.group_code,
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
a.average_days_to_payment,
a.payment_terms,
a.sent_to_collections,
a.churn_date,
a.churn_status,
a.price_tier,

a.address_line_1,
a.address_line_2,
a.city,
a.state,
a.country,
a.zip,
a.email,
a.phone,
a.tax,

case
  when a.customer_code in (select warehouse from analytics.cafe_mapping) then 'Cafe'
	else coalesce(a.division, 'Other')
end as region,

case
  when a.customer_code like '15%' or a.customer_code like '70%' then 'CPG'
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
end as business_type

from {{ref('warehouse_accounts')}} a
left join {{ref('wholesale_second_coffee_order_date')}} s on s.customer_code = a.customer_code