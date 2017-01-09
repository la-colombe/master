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


credit,
debit,

detail_comment,
source_journal,
source_module,

full_account_number,
full_account_name,
account_type,
account_group,
account_category,

account_code, 
cost_center_code, 
division_code,

a.name as account,
cc.name as cost_center,
d.name as division


from {{ref('general_ledger_entry_detail')}} e
join {{ref('gl_account_mapping')}} a on a.account = e.account_code
join {{ref('gl_cost_center_mapping')}} cc on cc.cost_center = e.cost_center_code
join {{ref('gl_division_mapping')}} d on d.division = e.division_code