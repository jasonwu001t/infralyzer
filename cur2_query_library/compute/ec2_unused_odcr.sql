-- EC2 Unused OnDemand Capacity Reservations (ODCR) Analysis
-- Description: Unused On-Demand Capacity Reservations with cost and utilization details
-- Output: ODCR details including unused amounts, total reservations, and associated costs
-- Source: AWS CUR Query Library - Compute section

WITH total_reservation_usage AS (
  SELECT 
    line_item_resource_id,
    SUM(line_item_usage_amount) AS total_line_item_usage_amount
  FROM 
    CUR
  WHERE 
    line_item_product_code = 'AmazonEC2'
    AND line_item_resource_id LIKE '%cr-%'
    AND line_item_usage_type LIKE '%Reservation:%'
    AND line_item_line_item_description LIKE '%Res%'
  GROUP BY 
    line_item_resource_id
)

SELECT 
  EXTRACT(YEAR FROM main.line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM main.line_item_usage_start_date)::text, 2, '0') AS month_usage_date,
  SPLIT_PART(main.line_item_resource_id, ':', 5) AS owner_account,
  main.line_item_usage_account_id AS consumer_account,
  main.line_item_usage_type,
  main.line_item_product_code, 
  main.line_item_line_item_description, 
  main.line_item_resource_id,
  t.total_line_item_usage_amount AS total_reservation_amount,
  SUM(main.line_item_usage_amount) AS unused_reservation_amount,
  SUM(main.line_item_unblended_cost) AS sum_unused_reservation_cost
FROM 
  CUR AS main
RIGHT JOIN 
  total_reservation_usage AS t
  ON main.line_item_resource_id = t.line_item_resource_id
WHERE 
  main.line_item_product_code = 'AmazonEC2'
  AND main.line_item_usage_type LIKE '%UnusedBox%' 
  AND main.line_item_resource_id LIKE '%cr-%'
  AND main.line_item_line_item_description LIKE '%Res%'
GROUP BY 
  EXTRACT(YEAR FROM main.line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM main.line_item_usage_start_date)::text, 2, '0'),
  SPLIT_PART(main.line_item_resource_id, ':', 5),
  main.line_item_usage_account_id,
  main.line_item_usage_type,
  main.line_item_product_code,
  main.line_item_line_item_description,
  main.line_item_resource_id,
  t.total_line_item_usage_amount
ORDER BY 
  sum_unused_reservation_cost DESC 