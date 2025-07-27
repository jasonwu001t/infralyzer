-- View: kpi_tracker
-- Dependencies: summary_view, kpi_instance_all, kpi_ebs_storage_all, kpi_ebs_snap, kpi_s3_storage_all
-- Description: Comprehensive KPI tracking dashboard with all cost optimization metrics
-- Output: cur2_view/14_kpi_tracker.parquet

CREATE OR REPLACE VIEW kpi_tracker AS 
SELECT DISTINCT
  spend_all.billing_period,
  spend_all.payer_account_id,
  spend_all.linked_account_id,
  spend_all.spend_all_cost,
  spend_all.tags_json,
  instance_all.ec2_all_cost,
  instance_all.ec2_usage_cost,
  instance_all.ec2_spot_cost,
  instance_all.ec2_spot_potential_savings,
  instance_all.ec2_previous_generation_cost,
  instance_all.ec2_previous_generation_potential_savings,
  instance_all.ec2_graviton_eligible_cost,
  instance_all.ec2_graviton_cost,
  instance_all.ec2_graviton_potential_savings,
  instance_all.ec2_amd_eligible_cost,
  instance_all.ec2_amd_cost,
  instance_all.ec2_amd_potential_savings,
  instance_all.rds_all_cost,
  instance_all.rds_ondemand_cost,
  instance_all.rds_graviton_cost,
  instance_all.rds_graviton_eligible_cost,
  instance_all.rds_graviton_potential_savings,
  instance_all.rds_commit_potential_savings,
  instance_all.rds_commit_savings,
  instance_all.elasticache_all_cost,
  instance_all.elasticache_ondemand_cost,
  instance_all.elasticache_graviton_cost,
  instance_all.elasticache_graviton_eligible_cost,
  instance_all.elasticache_graviton_potential_savings,
  instance_all.elasticache_commit_potential_savings,
  instance_all.elasticache_commit_savings,
  ebs_all.ebs_all_cost,
  ebs_all.ebs_gp_all_cost,
  ebs_all.ebs_gp2_cost,
  ebs_all.ebs_gp3_cost,
  ebs_all.ebs_gp3_potential_savings,
  snap.ebs_snapshots_under_1yr_cost,
  snap.ebs_snapshots_over_1yr_cost,
  snap.ebs_snapshot_cost,
  s3_all.s3_all_storage_cost,
  s3_all.s3_standard_storage_cost,
  s3_all.s3_standard_storage_potential_savings,
  instance_all.compute_all_cost,
  instance_all.compute_ondemand_cost,
  instance_all.compute_commit_potential_savings,
  instance_all.compute_commit_savings,
  instance_all.dynamodb_all_cost,
  instance_all.dynamodb_committed_cost,
  instance_all.dynamodb_ondemand_cost,
  instance_all.dynamodb_commit_potential_savings,
  instance_all.dynamodb_commit_savings,
  instance_all.opensearch_all_cost,
  instance_all.opensearch_ondemand_cost,
  instance_all.opensearch_graviton_cost,
  instance_all.opensearch_graviton_eligible_cost,
  instance_all.opensearch_graviton_potential_savings,
  instance_all.opensearch_commit_potential_savings,
  instance_all.opensearch_commit_savings,
  instance_all.redshift_all_cost,
  instance_all.redshift_ondemand_cost,
  instance_all.redshift_commit_potential_savings,
  instance_all.redshift_commit_savings,
  instance_all.sagemaker_all_cost,
  instance_all.sagemaker_ondemand_cost,
  instance_all.sagemaker_commit_potential_savings,
  instance_all.sagemaker_commit_savings,
  instance_all.lambda_all_cost,
  instance_all.lambda_graviton_cost,
  instance_all.lambda_graviton_eligible_cost,
  instance_all.lambda_graviton_potential_savings,
  (CASE WHEN (instance_all.license_model IN ('License included', 'Bring your own license')) THEN 1 ELSE 0 END) AS rds_license,
  (CASE WHEN (instance_all.license_model LIKE 'No license required') THEN 1 ELSE 0 END) AS rds_no_license,
  instance_all.rds_sql_server_cost,
  instance_all.rds_oracle_cost
FROM
  (((((
   SELECT DISTINCT
     billing_period,
     payer_account_id,
     linked_account_id,
     tags_json,
     SUM(amortized_cost) AS spend_all_cost,
     SUM(unblended_cost) AS unblended_cost
   FROM
     summary_view
   WHERE (CAST(CONCAT(year, '-', month, '-01') AS DATE) >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 3 MONTH))
   GROUP BY 1, 2, 3, 4
)  spend_all
LEFT JOIN (
   SELECT DISTINCT
     billing_period,
     payer_account_id,
     linked_account_id,
     tags_json,
     SUM(ec2_all_cost) AS ec2_all_cost,
     SUM(ec2_usage_cost) AS ec2_usage_cost,
     SUM(ec2_spot_cost) AS ec2_spot_cost,
     SUM(ec2_spot_potential_savings) AS ec2_spot_potential_savings,
     SUM(ec2_previous_generation_cost) AS ec2_previous_generation_cost,
     SUM(ec2_previous_generation_potential_savings) AS ec2_previous_generation_potential_savings,
     SUM(ec2_graviton_eligible_cost) AS ec2_graviton_eligible_cost,
     SUM(ec2_graviton_cost) AS ec2_graviton_cost,
     SUM(ec2_graviton_potential_savings) AS ec2_graviton_potential_savings,
     SUM(ec2_amd_eligible_cost) AS ec2_amd_eligible_cost,
     SUM(ec2_amd_cost) AS ec2_amd_cost,
     SUM(ec2_amd_potential_savings) AS ec2_amd_potential_savings,
     SUM(rds_all_cost) AS rds_all_cost,
     SUM(rds_ondemand_cost) AS rds_ondemand_cost,
     SUM(rds_graviton_cost) AS rds_graviton_cost,
     SUM(rds_graviton_eligible_cost) AS rds_graviton_eligible_cost,
     SUM(rds_graviton_potential_savings) AS rds_graviton_potential_savings,
     SUM(rds_commit_potential_savings) AS rds_commit_potential_savings,
     SUM(rds_commit_savings) AS rds_commit_savings,
     SUM((CASE WHEN (license_model IN ('License included', 'Bring your own license')) THEN 1 ELSE 0 END)) AS rds_license,
     SUM((CASE WHEN (license_model LIKE 'No license required') THEN 1 ELSE 0 END)) AS rds_no_license,
     SUM(elasticache_all_cost) AS elasticache_all_cost,
     SUM(elasticache_ondemand_cost) AS elasticache_ondemand_cost,
     SUM(elasticache_graviton_cost) AS elasticache_graviton_cost,
     SUM(elasticache_graviton_eligible_cost) AS elasticache_graviton_eligible_cost,
     SUM(elasticache_graviton_potential_savings) AS elasticache_graviton_potential_savings,
     SUM(elasticache_commit_potential_savings) AS elasticache_commit_potential_savings,
     SUM(elasticache_commit_savings) AS elasticache_commit_savings,
     SUM(compute_all_cost) AS compute_all_cost,
     SUM(compute_ondemand_cost) AS compute_ondemand_cost,
     SUM(compute_commit_potential_savings) AS compute_commit_potential_savings,
     SUM(compute_commit_savings) AS compute_commit_savings,
     SUM(opensearch_all_cost) AS opensearch_all_cost,
     SUM(opensearch_ondemand_cost) AS opensearch_ondemand_cost,
     SUM(opensearch_graviton_cost) AS opensearch_graviton_cost,
     SUM(opensearch_graviton_eligible_cost) AS opensearch_graviton_eligible_cost,
     SUM(opensearch_graviton_potential_savings) AS opensearch_graviton_potential_savings,
     SUM(opensearch_commit_potential_savings) AS opensearch_commit_potential_savings,
     SUM(opensearch_commit_savings) AS opensearch_commit_savings,
     SUM(redshift_all_cost) AS redshift_all_cost,
     SUM(redshift_ondemand_cost) AS redshift_ondemand_cost,
     SUM(redshift_commit_potential_savings) AS redshift_commit_potential_savings,
     SUM(redshift_commit_savings) AS redshift_commit_savings,
     SUM(dynamodb_all_cost) AS dynamodb_all_cost,
     SUM(dynamodb_committed_cost) AS dynamodb_committed_cost,
     SUM(dynamodb_ondemand_cost) AS dynamodb_ondemand_cost,
     SUM(dynamodb_commit_potential_savings) AS dynamodb_commit_potential_savings,
     SUM(dynamodb_commit_savings) AS dynamodb_commit_savings,
     SUM(sagemaker_all_cost) AS sagemaker_all_cost,
     SUM(sagemaker_ondemand_cost) AS sagemaker_ondemand_cost,
     SUM(sagemaker_commit_potential_savings) AS sagemaker_commit_potential_savings,
     SUM(sagemaker_commit_savings) AS sagemaker_commit_savings,
     SUM(lambda_all_cost) AS lambda_all_cost,
     SUM(lambda_graviton_cost) AS lambda_graviton_cost,
     SUM(lambda_graviton_eligible_cost) AS lambda_graviton_eligible_cost,
     SUM(lambda_graviton_potential_savings) AS lambda_graviton_potential_savings,
     SUM(rds_sql_server_cost) AS rds_sql_server_cost,
     SUM(rds_oracle_cost) AS rds_oracle_cost,
     license_model
   FROM
     kpi_instance_all
   GROUP BY 1, 2, 3, 4, 37
)  instance_all ON ((instance_all.linked_account_id = spend_all.linked_account_id) AND (instance_all.billing_period = spend_all.billing_period) AND (instance_all.payer_account_id = spend_all.payer_account_id) AND (instance_all.tags_json = spend_all.tags_json)))
LEFT JOIN (
   SELECT DISTINCT
     billing_period,
     payer_account_id,
     linked_account_id,
     tags_json,
     SUM(ebs_all_cost) AS ebs_all_cost,
     SUM((ebs_gp3_cost + ebs_gp2_cost)) AS ebs_gp_all_cost,
     SUM(ebs_gp3_cost) AS ebs_gp3_cost,
     SUM(ebs_gp2_cost) AS ebs_gp2_cost,
     SUM(ebs_gp3_potential_savings) AS ebs_gp3_potential_savings
   FROM
     kpi_ebs_storage_all
   GROUP BY 1, 2, 3, 4
)  ebs_all ON ((ebs_all.linked_account_id = spend_all.linked_account_id) AND (ebs_all.billing_period = spend_all.billing_period) AND (ebs_all.payer_account_id = spend_all.payer_account_id) AND (ebs_all.tags_json = spend_all.tags_json)))
LEFT JOIN (
   SELECT DISTINCT
     billing_period,
     payer_account_id,
     linked_account_id,
     tags_json,
     SUM(ebs_snapshots_under_1yr_cost) AS ebs_snapshots_under_1yr_cost,
     SUM(ebs_snapshots_over_1yr_cost) AS ebs_snapshots_over_1yr_cost,
     SUM(ebs_snapshot_cost) AS ebs_snapshot_cost
   FROM
     kpi_ebs_snap
   GROUP BY 1, 2, 3, 4
)  snap ON ((snap.linked_account_id = spend_all.linked_account_id) AND (snap.billing_period = spend_all.billing_period) AND (snap.payer_account_id = spend_all.payer_account_id) AND (snap.tags_json = spend_all.tags_json)))
LEFT JOIN (
   SELECT DISTINCT
     billing_period,
     payer_account_id,
     linked_account_id,
     tags_json,
     SUM(s3_all_storage_cost) AS s3_all_storage_cost,
     SUM(s3_standard_storage_cost) AS s3_standard_storage_cost,
     SUM(s3_standard_storage_potential_savings) AS s3_standard_storage_potential_savings
   FROM
     kpi_s3_storage_all
   GROUP BY 1, 2, 3, 4
)  s3_all ON ((s3_all.linked_account_id = spend_all.linked_account_id) AND (s3_all.billing_period = spend_all.billing_period) AND (s3_all.payer_account_id = spend_all.payer_account_id) AND (s3_all.tags_json = spend_all.tags_json)))
WHERE (spend_all.billing_period >= (DATE_TRUNC('month', CURRENT_TIMESTAMP) - INTERVAL 3 MONTH))
