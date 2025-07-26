-- EC2 Unallocated Elastic IPs Analysis
-- Description: Cost for unallocated Elastic IPs, usage amount, and cost by account and region
-- Output: Unallocated EIPs, usage, and cost for optimization
-- Source: AWS CUR Query Library - Cost Optimization section

SELECT
  line_item_usage_account_id,
  line_item_usage_type,
  product_location,
  line_item_line_item_description,
  SUM(line_item_usage_amount) AS sum_line_item_usage_amount,
  SUM(line_item_unblended_cost) AS sum_line_item_unblended_cost
FROM
  CUR
WHERE
  line_item_product_code = 'AmazonEC2'
  AND line_item_usage_type LIKE '%ElasticIP:IdleAddress%'
GROUP BY
  line_item_usage_account_id,
  line_item_usage_type,
  product_location,
  line_item_line_item_description
ORDER BY
  sum_line_item_unblended_cost DESC,
  sum_line_item_usage_amount DESC 