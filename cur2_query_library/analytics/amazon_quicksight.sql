-- Amazon QuickSight Monthly Cost and Usage Analysis
-- Description: Monthly unblended cost and usage information with categorized usage types
-- Output: Account details, usage type categories, and costs in descending order
-- Source: AWS CUR Query Library - Analytics section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  EXTRACT(year FROM line_item_usage_start_date) * 100 + EXTRACT(month FROM line_item_usage_start_date) AS month_line_item_usage_start_date, 
  CASE
    WHEN LOWER(line_item_usage_type) LIKE 'qs-user-enterprise%' THEN 'Users - Enterprise'
    WHEN LOWER(line_item_usage_type) LIKE 'qs-user-standard%' THEN 'Users - Standard'
    WHEN LOWER(line_item_usage_type) LIKE 'qs-reader%' THEN 'Reader Usage'
    WHEN LOWER(line_item_usage_type) LIKE '%spice' THEN 'SPICE'
    WHEN LOWER(line_item_usage_type) LIKE '%alerts%' THEN 'Alerts'
    ELSE line_item_usage_type
  END AS case_line_item_usage_type,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  product_product_name = 'Amazon QuickSight'
  AND line_item_line_item_type IN ('DiscountedUsage', 'Usage')
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id,
  EXTRACT(year FROM line_item_usage_start_date) * 100 + EXTRACT(month FROM line_item_usage_start_date), 
  CASE
    WHEN LOWER(line_item_usage_type) LIKE 'qs-user-enterprise%' THEN 'Users - Enterprise'
    WHEN LOWER(line_item_usage_type) LIKE 'qs-user-standard%' THEN 'Users - Standard'
    WHEN LOWER(line_item_usage_type) LIKE 'qs-reader%' THEN 'Reader Usage'
    WHEN LOWER(line_item_usage_type) LIKE '%spice' THEN 'SPICE'
    WHEN LOWER(line_item_usage_type) LIKE '%alerts%' THEN 'Alerts'
    ELSE line_item_usage_type
  END
ORDER BY
  month_line_item_usage_start_date,
  sum_line_item_unblended_cost DESC 