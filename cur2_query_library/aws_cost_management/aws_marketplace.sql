-- AWS Marketplace Subscription Costs Analysis
-- Description: AWS Marketplace subscription costs including product names and monthly totals by account
-- Output: Monthly subscription costs grouped by account and product name, including tax
-- Source: AWS CUR Query Library - AWS Cost Management section

SELECT 
  bill_payer_account_id,
  line_item_usage_account_id,
  CASE 
    WHEN line_item_usage_start_date IS NULL THEN '2025-01-01'::date
    ELSE line_item_usage_start_date::date
  END AS case_line_item_usage_start_time,
  bill_billing_entity,
  product_product_name,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM 
  CUR 
WHERE 
  bill_billing_entity = 'AWS Marketplace'
GROUP BY 
  bill_payer_account_id,
  line_item_usage_account_id,
  CASE 
    WHEN line_item_usage_start_date IS NULL THEN '2025-01-01'::date
    ELSE line_item_usage_start_date::date
  END,
  bill_billing_entity,
  product_product_name
ORDER BY 
  case_line_item_usage_start_time ASC,
  sum_line_item_unblended_cost DESC 