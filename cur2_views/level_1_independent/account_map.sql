-- View: account_map
-- Dependencies: CUR
-- Description: Account mapping with parent/child relationships
-- Output: cur2_view/01_account_map.parquet

CREATE OR REPLACE VIEW account_map AS 
SELECT DISTINCT
  line_item_usage_account_id AS account_id,
  MAX_BY(line_item_usage_account_name, line_item_usage_start_date) AS account_name,
  MAX_BY(bill_payer_account_id, line_item_usage_start_date) AS parent_account_id,
  MAX_BY(bill_payer_account_name, line_item_usage_start_date) AS parent_account_name
FROM
  CUR
GROUP BY line_item_usage_account_id