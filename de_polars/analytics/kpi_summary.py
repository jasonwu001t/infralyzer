"""
KPI Summary Analytics - Comprehensive cost metrics dashboard powered by kpi_tracker.sql
"""
import polars as pl
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pathlib import Path

from ..engine.duckdb_engine import DuckDBEngine
from ..engine.data_config import DataConfig


class KPISummaryAnalytics:
    """
    Comprehensive KPI summary analytics powered by kpi_tracker.sql.
    
    This is the ⭐ NEW primary endpoint providing comprehensive cost metrics
    aggregated from all specialized KPI views for complete cost optimization insights.
    """
    
    def __init__(self, engine: DuckDBEngine):
        """Initialize KPI Summary Analytics with DuckDB engine."""
        self.engine = engine
        self.config = engine.config
    
    def get_comprehensive_summary(self, 
                                 billing_period: Optional[str] = None,
                                 payer_account_id: Optional[str] = None,
                                 linked_account_id: Optional[str] = None,
                                 tags_filter: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Get comprehensive KPI summary for cost metrics dashboard.
        
        This is the primary endpoint that powers the comprehensive cost metrics dashboard,
        providing aggregated data from summary_view, kpi_instance_all, kpi_ebs_storage_all,
        kpi_ebs_snap, kpi_s3_storage_all and other specialized KPI views.
        
        Args:
            billing_period: Filter by specific billing period (YYYY-MM format)
            payer_account_id: Filter by specific payer account
            linked_account_id: Filter by specific linked account  
            tags_filter: Filter by tag key-value pairs
        
        Returns:
            Complete KPI metrics structure matching API design
        """
        try:
            # Load and execute kpi_tracker.sql query
            kpi_sql = self._load_kpi_tracker_sql()
            
            # Apply filters to the query
            filtered_sql = self._apply_filters(kpi_sql, billing_period, payer_account_id, linked_account_id, tags_filter)
            
            # Execute the KPI query
            kpi_result = self.engine.query(filtered_sql)
            
            # Transform to API response format
            return self._transform_to_api_response(kpi_result, billing_period, payer_account_id, linked_account_id, tags_filter)
            
        except Exception as e:
            print(f"❌ Error generating KPI summary: {e}")
            return self._get_error_response(str(e))
    
    def _load_kpi_tracker_sql(self) -> str:
        """Load the kpi_tracker.sql query from cur2_views/level_3_final/"""
        # Look for kpi_tracker.sql in the project
        sql_paths = [
            "cur2_views/level_3_final/kpi_tracker.sql",
            "../cur2_views/level_3_final/kpi_tracker.sql",
            "../../cur2_views/level_3_final/kpi_tracker.sql"
        ]
        
        for sql_path in sql_paths:
            if Path(sql_path).exists():
                with open(sql_path, 'r') as f:
                    return f.read()
        
        # Fallback: Generate basic KPI query if kpi_tracker.sql not found
        print("⚠️  kpi_tracker.sql not found, using fallback query")
        return self._get_fallback_kpi_query()
    
    def _get_fallback_kpi_query(self) -> str:
        """Fallback KPI query if kpi_tracker.sql is not available."""
        return f"""
        -- Fallback KPI Summary Query
        SELECT 
            '{datetime.now().strftime('%Y-%m')}' as billing_period,
            'default' as payer_account_id,
            'default' as linked_account_id,
            COALESCE(SUM(unblended_cost), 0) as spend_all_cost,
            COALESCE(SUM(unblended_cost), 0) as unblended_cost,
            '{{}}' as tags_json,
            
            -- EC2 Metrics (estimated)
            COALESCE(SUM(CASE WHEN product_servicecode = 'AmazonEC2' THEN unblended_cost ELSE 0 END), 0) as ec2_all_cost,
            COALESCE(SUM(CASE WHEN product_servicecode = 'AmazonEC2' AND line_item_usage_type LIKE '%BoxUsage%' THEN unblended_cost ELSE 0 END), 0) as ec2_usage_cost,
            0 as ec2_spot_cost,
            0 as ec2_spot_potential_savings,
            0 as ec2_previous_generation_cost,
            0 as ec2_previous_generation_potential_savings,
            0 as ec2_graviton_eligible_cost,
            0 as ec2_graviton_cost,
            0 as ec2_graviton_potential_savings,
            0 as ec2_amd_eligible_cost,
            0 as ec2_amd_cost,
            0 as ec2_amd_potential_savings,
            
            -- RDS Metrics (estimated)
            COALESCE(SUM(CASE WHEN product_servicecode = 'AmazonRDS' THEN unblended_cost ELSE 0 END), 0) as rds_all_cost,
            COALESCE(SUM(CASE WHEN product_servicecode = 'AmazonRDS' THEN unblended_cost ELSE 0 END), 0) as rds_ondemand_cost,
            0 as rds_graviton_cost,
            0 as rds_graviton_eligible_cost,
            0 as rds_graviton_potential_savings,
            0 as rds_commit_potential_savings,
            0 as rds_commit_savings,
            0 as rds_license,
            0 as rds_no_license,
            0 as rds_sql_server_cost,
            0 as rds_oracle_cost,
            
            -- Storage Metrics (estimated)
            COALESCE(SUM(CASE WHEN product_servicecode = 'AmazonS3' THEN unblended_cost ELSE 0 END), 0) as s3_all_storage_cost,
            0 as ebs_all_cost,
            0 as ebs_gp_all_cost,
            0 as ebs_gp2_cost,
            0 as ebs_gp3_cost,
            0 as ebs_gp3_potential_savings,
            0 as ebs_snapshots_under_1yr_cost,
            0 as ebs_snapshots_over_1yr_cost,
            0 as ebs_snapshot_cost,
            0 as s3_standard_storage_cost,
            0 as s3_standard_storage_potential_savings,
            
            -- Compute Services (estimated)
            COALESCE(SUM(CASE WHEN product_servicecode IN ('AmazonEC2', 'AmazonRDS', 'AWSLambda') THEN unblended_cost ELSE 0 END), 0) as compute_all_cost,
            0 as compute_ondemand_cost,
            0 as compute_commit_potential_savings,
            0 as compute_commit_savings,
            0 as dynamodb_all_cost,
            0 as lambda_all_cost
        FROM {self.config.table_name}
        WHERE unblended_cost > 0
        """
    
    def _apply_filters(self, sql: str, billing_period: Optional[str], payer_account_id: Optional[str], 
                      linked_account_id: Optional[str], tags_filter: Optional[Dict[str, str]]) -> str:
        """Apply filters to the KPI SQL query."""
        # Start with original query
        filtered_sql = sql
        
        # Add WHERE clauses for filters
        where_conditions = []
        
        if billing_period:
            where_conditions.append(f"billing_period = '{billing_period}'")
        
        if payer_account_id:
            where_conditions.append(f"payer_account_id = '{payer_account_id}'")
        
        if linked_account_id:
            where_conditions.append(f"linked_account_id = '{linked_account_id}'")
        
        if tags_filter:
            # Convert tags filter to JSON-like filter (simplified)
            for key, value in tags_filter.items():
                where_conditions.append(f"resource_tags LIKE '%{key}%:{value}%'")
        
        # Add filters if any exist
        if where_conditions:
            if "WHERE" in filtered_sql.upper():
                filtered_sql += " AND " + " AND ".join(where_conditions)
            else:
                filtered_sql += " WHERE " + " AND ".join(where_conditions)
        
        return filtered_sql
    
    def _transform_to_api_response(self, kpi_result: pl.DataFrame, billing_period: Optional[str],
                                  payer_account_id: Optional[str], linked_account_id: Optional[str],
                                  tags_filter: Optional[Dict[str, str]]) -> Dict[str, Any]:
        """Transform DuckDB query result to API response format."""
        if kpi_result.is_empty():
            return self._get_empty_response()
        
        # Get first row of results (aggregated data)
        row = kpi_result.row(0, named=True)
        
        # Build comprehensive response structure
        response = {
            "summary_metadata": {
                "query_date": datetime.now().isoformat(),
                "billing_periods": [billing_period] if billing_period else ["latest"],
                "total_accounts": 1,  # Will be calculated from actual data
                "data_source": "local_parquet" if self.engine.has_local_data() else "s3_parquet"
            },
            "overall_spend": {
                "billing_period": billing_period or "latest",
                "payer_account_id": payer_account_id or row.get("payer_account_id", "unknown"),
                "linked_account_id": linked_account_id or row.get("linked_account_id", "unknown"),
                "spend_all_cost": float(row.get("spend_all_cost", 0)),
                "unblended_cost": float(row.get("unblended_cost", 0)),
                "tags_json": row.get("tags_json", "{}")
            },
            "ec2_metrics": {
                "ec2_all_cost": float(row.get("ec2_all_cost", 0)),
                "ec2_usage_cost": float(row.get("ec2_usage_cost", 0)),
                "ec2_spot_cost": float(row.get("ec2_spot_cost", 0)),
                "ec2_spot_potential_savings": float(row.get("ec2_spot_potential_savings", 0)),
                "ec2_previous_generation_cost": float(row.get("ec2_previous_generation_cost", 0)),
                "ec2_previous_generation_potential_savings": float(row.get("ec2_previous_generation_potential_savings", 0)),
                "ec2_graviton_eligible_cost": float(row.get("ec2_graviton_eligible_cost", 0)),
                "ec2_graviton_cost": float(row.get("ec2_graviton_cost", 0)),
                "ec2_graviton_potential_savings": float(row.get("ec2_graviton_potential_savings", 0)),
                "ec2_amd_eligible_cost": float(row.get("ec2_amd_eligible_cost", 0)),
                "ec2_amd_cost": float(row.get("ec2_amd_cost", 0)),
                "ec2_amd_potential_savings": float(row.get("ec2_amd_potential_savings", 0))
            },
            "rds_metrics": {
                "rds_all_cost": float(row.get("rds_all_cost", 0)),
                "rds_ondemand_cost": float(row.get("rds_ondemand_cost", 0)),
                "rds_graviton_cost": float(row.get("rds_graviton_cost", 0)),
                "rds_graviton_eligible_cost": float(row.get("rds_graviton_eligible_cost", 0)),
                "rds_graviton_potential_savings": float(row.get("rds_graviton_potential_savings", 0)),
                "rds_commit_potential_savings": float(row.get("rds_commit_potential_savings", 0)),
                "rds_commit_savings": float(row.get("rds_commit_savings", 0)),
                "rds_license": int(row.get("rds_license", 0)),
                "rds_no_license": int(row.get("rds_no_license", 0)),
                "rds_sql_server_cost": float(row.get("rds_sql_server_cost", 0)),
                "rds_oracle_cost": float(row.get("rds_oracle_cost", 0))
            },
            "storage_metrics": {
                "ebs_all_cost": float(row.get("ebs_all_cost", 0)),
                "ebs_gp_all_cost": float(row.get("ebs_gp_all_cost", 0)),
                "ebs_gp2_cost": float(row.get("ebs_gp2_cost", 0)),
                "ebs_gp3_cost": float(row.get("ebs_gp3_cost", 0)),
                "ebs_gp3_potential_savings": float(row.get("ebs_gp3_potential_savings", 0)),
                "ebs_snapshots_under_1yr_cost": float(row.get("ebs_snapshots_under_1yr_cost", 0)),
                "ebs_snapshots_over_1yr_cost": float(row.get("ebs_snapshots_over_1yr_cost", 0)),
                "ebs_snapshot_cost": float(row.get("ebs_snapshot_cost", 0)),
                "s3_all_storage_cost": float(row.get("s3_all_storage_cost", 0)),
                "s3_standard_storage_cost": float(row.get("s3_standard_storage_cost", 0)),
                "s3_standard_storage_potential_savings": float(row.get("s3_standard_storage_potential_savings", 0))
            },
            "compute_services": {
                "compute_all_cost": float(row.get("compute_all_cost", 0)),
                "compute_ondemand_cost": float(row.get("compute_ondemand_cost", 0)),
                "compute_commit_potential_savings": float(row.get("compute_commit_potential_savings", 0)),
                "compute_commit_savings": float(row.get("compute_commit_savings", 0)),
                "dynamodb_all_cost": float(row.get("dynamodb_all_cost", 0)),
                "lambda_all_cost": float(row.get("lambda_all_cost", 0))
            }
        }
        
        # Calculate savings summary
        response["savings_summary"] = self._calculate_savings_summary(response)
        
        return response
    
    def _calculate_savings_summary(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate total savings summary from all metrics."""
        ec2_savings = (
            metrics["ec2_metrics"]["ec2_spot_potential_savings"] +
            metrics["ec2_metrics"]["ec2_previous_generation_potential_savings"] +
            metrics["ec2_metrics"]["ec2_graviton_potential_savings"] +
            metrics["ec2_metrics"]["ec2_amd_potential_savings"]
        )
        
        rds_savings = (
            metrics["rds_metrics"]["rds_graviton_potential_savings"] +
            metrics["rds_metrics"]["rds_commit_potential_savings"]
        )
        
        storage_savings = (
            metrics["storage_metrics"]["ebs_gp3_potential_savings"] +
            metrics["storage_metrics"]["s3_standard_storage_potential_savings"]
        )
        
        compute_savings = metrics["compute_services"]["compute_commit_potential_savings"]
        
        total_potential_savings = ec2_savings + rds_savings + storage_savings + compute_savings
        
        current_savings = (
            metrics["rds_metrics"]["rds_commit_savings"] +
            metrics["compute_services"]["compute_commit_savings"]
        )
        
        return {
            "total_potential_savings": round(total_potential_savings, 2),
            "graviton_savings_potential": round(
                metrics["ec2_metrics"]["ec2_graviton_potential_savings"] + 
                metrics["rds_metrics"]["rds_graviton_potential_savings"], 2
            ),
            "commitment_savings_potential": round(rds_savings + compute_savings, 2),
            "storage_optimization_potential": round(storage_savings, 2),
            "spot_instance_potential": round(metrics["ec2_metrics"]["ec2_spot_potential_savings"], 2),
            "current_monthly_savings": round(current_savings, 2),
            "annualized_savings_opportunity": round(total_potential_savings * 12, 2)
        }
    
    def _get_empty_response(self) -> Dict[str, Any]:
        """Get empty response structure when no data is found."""
        return {
            "summary_metadata": {
                "query_date": datetime.now().isoformat(),
                "billing_periods": [],
                "total_accounts": 0,
                "data_source": "no_data"
            },
            "overall_spend": {"spend_all_cost": 0, "unblended_cost": 0},
            "ec2_metrics": {},
            "rds_metrics": {},
            "storage_metrics": {},
            "compute_services": {},
            "savings_summary": {"total_potential_savings": 0}
        }
    
    def _get_error_response(self, error_msg: str) -> Dict[str, Any]:
        """Get error response structure."""
        return {
            "error": True,
            "message": error_msg,
            "summary_metadata": {
                "query_date": datetime.now().isoformat(),
                "data_source": "error"
            }
        }