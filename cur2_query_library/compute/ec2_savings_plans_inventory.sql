-- EC2 Savings Plans Inventory Analysis
-- Description: Inventory for EC2 Savings Plans with commitment, utilization, and term details
-- Output: Savings Plans details including utilization percentages and commitment information
-- Source: AWS CUR Query Library - Compute section

SELECT
  SPLIT_PART(savings_plan_savings_plan_a_r_n, '/', 2) AS split_savings_plan_savings_plan_a_r_n,
  bill_payer_account_id,
  line_item_usage_account_id,
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0') AS month_line_item_usage_start_date,
  savings_plan_offering_type,
  savings_plan_region,
  SUBSTR(savings_plan_start_time, 1, 10) AS day_savings_plan_start_time,
  SUBSTR(savings_plan_end_time, 1, 10) AS day_savings_plan_end_time,
  savings_plan_payment_option,
  savings_plan_purchase_term,
  SUM(savings_plan_recurring_commitment_for_billing_period) AS sum_savings_plan_recurring_committment_for_billing_period,
  SUM(savings_plan_total_commitment_to_date) AS sum_savings_plan_total_commitment_to_date, 
  SUM(savings_plan_used_commitment) AS sum_savings_plan_used_commitment,
  AVG(CASE
    WHEN line_item_line_item_type = 'SavingsPlanRecurringFee' THEN savings_plan_total_commitment_to_date
    ELSE NULL
  END) AS hourly_commitment,
  CASE 
    WHEN SUM(savings_plan_total_commitment_to_date) > 0 THEN 
      ROUND((SUM(savings_plan_used_commitment) / SUM(savings_plan_total_commitment_to_date)) * 100, 0)
    ELSE 0
  END AS calc_savings_plan_utilization_percent
FROM
  CUR
WHERE 
  savings_plan_savings_plan_a_r_n <> ''
  AND savings_plan_savings_plan_a_r_n IS NOT NULL
  AND line_item_line_item_type = 'SavingsPlanRecurringFee'
GROUP BY
  SPLIT_PART(savings_plan_savings_plan_a_r_n, '/', 2),
  bill_payer_account_id,
  line_item_usage_account_id,
  EXTRACT(YEAR FROM line_item_usage_start_date) || '-' || 
  LPAD(EXTRACT(MONTH FROM line_item_usage_start_date)::text, 2, '0'),
  savings_plan_offering_type,
  savings_plan_region,
  SUBSTR(savings_plan_start_time, 1, 10),
  SUBSTR(savings_plan_end_time, 1, 10),
  savings_plan_payment_option,
  savings_plan_purchase_term
ORDER BY
  split_savings_plan_savings_plan_a_r_n,
  month_line_item_usage_start_date 