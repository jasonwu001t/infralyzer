-- Migration Acceleration Program (MAP) Credits Analysis
-- Description: Rewarded MAP credits grouped by day, account ID and credit description
-- Output: Daily MAP credits with account details and credit descriptions
-- Source: AWS CUR Query Library - AWS Cost Management section

SELECT 
  line_item_usage_start_date::date AS day_line_item_usage_start_date,
  line_item_usage_account_id,
  line_item_line_item_description,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE 
  line_item_line_item_type = 'Credit' 
  AND (
    line_item_line_item_description LIKE 'DBA_%' 
    OR line_item_line_item_description LIKE 'SLS_%'
    OR line_item_line_item_description LIKE '%MPE%'
  )
GROUP BY 
  line_item_usage_start_date::date,
  line_item_usage_account_id,
  line_item_line_item_description
ORDER BY
  day_line_item_usage_start_date,
  line_item_usage_account_id,
  line_item_line_item_description 