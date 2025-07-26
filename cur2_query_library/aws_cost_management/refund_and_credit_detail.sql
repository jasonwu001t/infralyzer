-- Refund and Credit Detail Analysis
-- Description: List of refunds and credits issued, grouped by account, month, service, and description
-- Output: Refunds and credits with account details, service breakdown, and line item descriptions
-- Source: AWS CUR Query Library - AWS Cost Management section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id, 
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') || '-01' AS month_line_item_usage_start_date,
  line_item_line_item_type,
  CASE
    WHEN (bill_billing_entity = 'AWS Marketplace' AND line_item_line_item_type NOT LIKE '%Discount%') THEN product_product_name
    WHEN (product_servicecode = '' OR product_servicecode IS NULL) THEN line_item_product_code
    ELSE product_servicecode
  END AS case_service,
  line_item_line_item_description,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  line_item_unblended_cost < 0
  AND line_item_line_item_type <> 'SavingsPlanNegation'
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id, 
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') || '-01',
  line_item_line_item_type,
  CASE
    WHEN (bill_billing_entity = 'AWS Marketplace' AND line_item_line_item_type NOT LIKE '%Discount%') THEN product_product_name
    WHEN (product_servicecode = '' OR product_servicecode IS NULL) THEN line_item_product_code
    ELSE product_servicecode
  END,
  line_item_line_item_description
ORDER BY
  month_line_item_usage_start_date ASC,
  sum_line_item_unblended_cost ASC 