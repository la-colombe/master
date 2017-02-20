{{
  config({
    "materialized" : "table",
    "unique_key" : "lot_code"
    })
}}

select 
replace(lotcodenodashesorspaces10digits, '-', '') as lot_code,
date_trunc('day', rundate) as run_date,
skubeingmanufactured as sku,
cansrejectedbysleever as cans_rejected_by_sleever,
cansrejectedbytaptone as cans_rejected_by_taptone,
cansrejectedpostfillerfiltec as cans_rejected_post_filler,
emailaddress as email,
totalbatchedproductgallons as gallons_batched,
totalcanscountedbytaptone as cans_counted_by_taptone,
totalcansfilledfiltec as cans_filled,
totalcanssleeved as cans_sleeved
from google_sheets.waste 