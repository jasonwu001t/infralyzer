-- Amazon MSK Monthly Cost and Usage Analysis
-- Description: Monthly unblended cost and usage information for Amazon MSK (including OD and Serverless)
-- Output: Account details, usage type, operation, and costs in descending order
-- Source: AWS CUR Query Library - Analytics section

SELECT
  bill_payer_account_id,
  line_item_usage_account_id,
  line_item_product_code,
  line_item_line_item_description,
  line_item_operation,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  line_item_product_code = 'AmazonMSK'
  AND line_item_line_item_type NOT IN ('Tax','Refund','Credit')
GROUP BY
  bill_payer_account_id,
  line_item_usage_account_id, 
  line_item_product_code, 
  line_item_line_item_description, 
  line_item_operation
ORDER BY
  sum_line_item_unblended_cost DESC 