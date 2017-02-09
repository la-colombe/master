select
division,
cost_center,
account,
account_group as group,
account_type as type,
account_statement as statement,
cost_center_agg as pl_cost_center
from google_sheets.gl_full_account a