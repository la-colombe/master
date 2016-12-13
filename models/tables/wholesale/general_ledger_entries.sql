{{
  config({
    "materialized" : "table",
    "sort" : "detail_posting_date",
    "unique_key" : "line_item_id"
    })
}}

select

entry_number,
line_item_id,
account_id,

detail_create_date,
detail_posting_date,
entry_posting_date,


credit,
debit,

detail_comment,
entry_comment,
source_journal,
source_module,

account_number,
account_name,
account_type,
account_group,
account_category,

main_account_code,
group_code,
category_code,
type_code,

rc.fday as posting_date_date_fday,
rc.fweek as posting_date_date_fweek,
rc.fperiod as posting_date_date_fperiod,
rc.fyear as posting_date_date_fyear,
rc.fquarter as posting_date_date_fquarter,
rc.fday_of_week posting_date_date_fday_of_week,
rc.fday_of_period posting_date_date_fday_of_period


from {{ref('general_ledger_entry_detail')}} d
left join {{ref('retail_calendar')}} rc on rc.date = d.detail_posting_date