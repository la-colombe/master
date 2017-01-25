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
a.group as group,
cc.name as cost_center,
case cost_center_code
	when 10070 then 'CPG'
	else d.name
end as division,
a.type as type,
a.statement as statement


from {{ref('general_ledger_entry_detail')}} e
left join {{ref('gl_account_mapping')}} a on a.account = e.account_code
left join {{ref('gl_cost_center_mapping')}} cc on cc.cost_center = e.cost_center_code
left join {{ref('gl_division_mapping')}} d on d.division = e.division_code