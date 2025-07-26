-- Top 50 Resource Movers Analysis
-- Description: Top 50 resources with highest cost delta and percentage change over time
-- Output: Resources with cost changes, delta calculations, and percentage changes
-- Source: AWS CUR Query Library - AWS Cost Management section
-- Note: Simplified for Polars compatibility - date calculations adjusted

WITH three_days_prior AS (
  SELECT
    line_item_resource_id as old_line_item_resource_id,
    line_item_usage_account_id,
    product_product_name,
    line_item_usage_start_date::date as usage_date,
    SUM(line_item_unblended_cost) as old_line_item_unblended_cost
  FROM
    CUR
  WHERE
    line_item_resource_id <> ''
    AND line_item_resource_id IS NOT NULL
    AND line_item_unblended_cost > 5
    -- Filter for approximate date range - adjust as needed for your data
    AND line_item_usage_start_date::date >= CURRENT_DATE - INTERVAL '10 days'
    AND line_item_usage_start_date::date <= CURRENT_DATE - INTERVAL '1 day'
  GROUP BY
    line_item_resource_id,
    line_item_usage_account_id,
    product_product_name,
    line_item_usage_start_date::date
),
two_days_prior AS (
  SELECT 
    line_item_resource_id as new_line_item_resource_id,
    line_item_usage_account_id,
    product_product_name,
    line_item_usage_start_date::date as usage_date,
    SUM(line_item_unblended_cost) as new_line_item_unblended_cost
  FROM
    CUR
  WHERE
    line_item_resource_id <> ''
    AND line_item_resource_id IS NOT NULL
    AND line_item_unblended_cost > 5
    -- Filter for approximate date range - adjust as needed for your data
    AND line_item_usage_start_date::date >= CURRENT_DATE - INTERVAL '5 days'
    AND line_item_usage_start_date::date <= CURRENT_DATE
  GROUP BY
    line_item_resource_id,
    line_item_usage_account_id,
    product_product_name,
    line_item_usage_start_date::date
)

SELECT
  a.line_item_usage_account_id,
  a.old_line_item_resource_id,
  a.old_line_item_unblended_cost AS cost_three_days_prior,
  b.new_line_item_unblended_cost AS cost_two_days_prior,
  (b.new_line_item_unblended_cost - a.old_line_item_unblended_cost) AS cost_delta,
  CASE 
    WHEN a.old_line_item_unblended_cost > 0 THEN 
      ((b.new_line_item_unblended_cost - a.old_line_item_unblended_cost) / a.old_line_item_unblended_cost) * 100
    ELSE 0
  END AS change_percentage,
  a.usage_date AS date_three_days_prior,
  b.usage_date AS date_two_days_prior,
  a.product_product_name
FROM
  three_days_prior a
  FULL OUTER JOIN two_days_prior b 
    ON a.old_line_item_resource_id = b.new_line_item_resource_id
    AND a.line_item_usage_account_id = b.line_item_usage_account_id
WHERE
  a.old_line_item_unblended_cost IS NOT NULL
  AND b.new_line_item_unblended_cost IS NOT NULL
ORDER BY
  cost_delta DESC,
  change_percentage DESC
LIMIT 50 