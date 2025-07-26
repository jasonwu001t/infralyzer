-- Data Processing Capacity Analysis
-- Description: Aggregated usage hours of compute services, ordered by largest to smallest number of hours
-- Output: Product name, product family, and total usage hours
-- Source: AWS CUR Query Library - Cost Efficiency section

SELECT 
    product_product_name,
    product_product_family,
    ROUND(SUM(line_item_usage_amount), 0) AS sum_line_item_usage_amount
FROM 
  CUR
WHERE 
  product_vcpu IS NOT NULL
  AND product_vcpu != ''
  AND line_item_line_item_type LIKE '%Usage%'
GROUP BY
    product_product_name,
    product_product_family
ORDER BY
    sum_line_item_usage_amount DESC 