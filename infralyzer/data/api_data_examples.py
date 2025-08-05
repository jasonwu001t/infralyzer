"""
Examples and utilities for working with AWS API data sources (Pricing and Savings Plans)
"""
from typing import Dict, List, Optional
import polars as pl
from pathlib import Path

from ..engine.data_config import DataConfig
from ..finops_engine import FinOpsEngine


class ApiDataExamples:
    """Examples and utilities for working with API data sources."""
    
    def __init__(self, engine: FinOpsEngine):
        """Initialize with FinOps engine."""
        self.engine = engine
        
    def example_config_with_api_data(self) -> Dict:
        """
        Example configuration that enables API data sources.
        
        Returns:
            Dictionary with example configuration
        """
        return {
            "s3_bucket": "your-billing-bucket",
            "s3_data_prefix": "your-data-prefix",
            "data_export_type": "CUR2.0",
            "aws_region": "us-east-1",
            
            # Enable API data sources
            "enable_pricing_api": True,
            "enable_savings_plans_api": True,
            
            # Configure API data collection
            "pricing_api_regions": ["us-east-1", "us-west-2", "eu-west-1"],
            "pricing_api_instance_types": ["t3.micro", "t3.small", "m5.large", "c5.xlarge"],
            "savings_plans_include_rates": True,
            "api_cache_max_age_days": 1,
            
            # Local cache for performance
            "local_data_path": "/path/to/local/cache"
        }
    
    def get_example_join_queries(self) -> Dict[str, str]:
        """
        Get example SQL queries that join CUR2.0 data with API data.
        
        Returns:
            Dictionary of query names and SQL statements
        """
        queries = {}
        
        # Example 1: Join CUR2.0 with pricing data to analyze cost efficiency
        queries["cost_efficiency_analysis"] = """
        SELECT 
            c.line_item_product_code,
            c.product_instance_type,
            c.product_region,
            c.line_item_operation,
            SUM(c.line_item_unblended_cost) as actual_cost,
            AVG(p.price_per_hour_usd) as current_on_demand_rate,
            SUM(c.line_item_usage_amount) as usage_hours,
            SUM(c.line_item_usage_amount * p.price_per_hour_usd) as theoretical_on_demand_cost,
            SUM(c.line_item_unblended_cost) - SUM(c.line_item_usage_amount * p.price_per_hour_usd) as savings_amount,
            CASE 
                WHEN SUM(c.line_item_usage_amount * p.price_per_hour_usd) > 0 
                THEN (1 - SUM(c.line_item_unblended_cost) / SUM(c.line_item_usage_amount * p.price_per_hour_usd)) * 100
                ELSE 0 
            END as savings_percentage
        FROM CUR c
        LEFT JOIN aws_pricing p ON (
            c.product_instance_type = p.instance_type 
            AND c.product_region = p.region_code
            AND c.product_operating_system = p.operating_system
            AND c.product_tenancy = p.tenancy
        )
        WHERE c.line_item_product_code = 'AmazonEC2'
            AND c.line_item_line_item_type = 'Usage'
            AND p.price_per_hour_usd IS NOT NULL
        GROUP BY 
            c.line_item_product_code,
            c.product_instance_type, 
            c.product_region,
            c.line_item_operation
        ORDER BY savings_amount DESC
        """
        
        # Example 2: Analyze Savings Plans coverage and utilization
        queries["savings_plans_coverage"] = """
        SELECT 
            c.bill_billing_period_start_date,
            c.product_instance_type,
            c.product_region,
            SUM(c.line_item_unblended_cost) as total_cost,
            SUM(CASE WHEN c.savings_plan_savings_plan_a_r_n IS NOT NULL 
                     THEN c.line_item_unblended_cost ELSE 0 END) as savings_plan_covered_cost,
            SUM(CASE WHEN c.savings_plan_savings_plan_a_r_n IS NULL 
                     THEN c.line_item_unblended_cost ELSE 0 END) as on_demand_cost,
            sp.commitment_amount_hourly,
            sp.savings_plan_type,
            sp.payment_option,
            sp.term_duration_years,
            CASE 
                WHEN SUM(c.line_item_unblended_cost) > 0 
                THEN (SUM(CASE WHEN c.savings_plan_savings_plan_a_r_n IS NOT NULL 
                               THEN c.line_item_unblended_cost ELSE 0 END) / SUM(c.line_item_unblended_cost)) * 100
                ELSE 0 
            END as coverage_percentage
        FROM CUR c
        LEFT JOIN aws_savings_plans sp ON c.savings_plan_savings_plan_a_r_n = sp.savings_plan_arn
        WHERE c.line_item_product_code = 'AmazonEC2'
            AND c.line_item_line_item_type = 'Usage'
        GROUP BY 
            c.bill_billing_period_start_date,
            c.product_instance_type,
            c.product_region,
            sp.commitment_amount_hourly,
            sp.savings_plan_type,
            sp.payment_option,
            sp.term_duration_years
        ORDER BY total_cost DESC
        """
        
        # Example 3: Identify opportunities for Savings Plans based on consistent usage
        queries["savings_plans_opportunities"] = """
        WITH monthly_usage AS (
            SELECT 
                c.product_instance_type,
                c.product_region,
                c.bill_billing_period_start_date,
                SUM(c.line_item_usage_amount) as monthly_hours,
                AVG(p.price_per_hour_usd) as on_demand_rate,
                SUM(c.line_item_unblended_cost) as monthly_cost
            FROM CUR c
            LEFT JOIN aws_pricing p ON (
                c.product_instance_type = p.instance_type 
                AND c.product_region = p.region_code
            )
            WHERE c.line_item_product_code = 'AmazonEC2'
                AND c.line_item_line_item_type = 'Usage'
                AND c.savings_plan_savings_plan_a_r_n IS NULL  -- Only on-demand usage
                AND p.price_per_hour_usd IS NOT NULL
            GROUP BY 
                c.product_instance_type,
                c.product_region,
                c.bill_billing_period_start_date
        ),
        usage_consistency AS (
            SELECT 
                product_instance_type,
                product_region,
                AVG(monthly_hours) as avg_monthly_hours,
                MIN(monthly_hours) as min_monthly_hours,
                MAX(monthly_hours) as max_monthly_hours,
                STDDEV(monthly_hours) as usage_stddev,
                AVG(monthly_cost) as avg_monthly_cost,
                COUNT(*) as months_of_data
            FROM monthly_usage
            GROUP BY product_instance_type, product_region
            HAVING COUNT(*) >= 3  -- At least 3 months of data
        )
        SELECT 
            u.product_instance_type,
            u.product_region,
            u.avg_monthly_hours,
            u.min_monthly_hours,
            u.usage_stddev,
            u.avg_monthly_cost * 12 as annual_cost,
            CASE 
                WHEN u.usage_stddev / NULLIF(u.avg_monthly_hours, 0) < 0.2 
                THEN 'Low Variability - Good SP Candidate'
                WHEN u.usage_stddev / NULLIF(u.avg_monthly_hours, 0) < 0.5 
                THEN 'Medium Variability - Consider SP'
                ELSE 'High Variability - SP May Not Be Optimal'
            END as savings_plan_recommendation,
            u.min_monthly_hours as recommended_hourly_commitment
        FROM usage_consistency u
        WHERE u.avg_monthly_hours > 50  -- Minimum usage threshold
        ORDER BY u.avg_monthly_cost DESC
        """
        
        # Example 4: Compare actual Savings Plans rates with current pricing
        queries["savings_plans_rate_comparison"] = """
        SELECT 
            spr.instance_type,
            spr.region as savings_plan_region,
            spr.usage_type,
            spr.operation,
            spr.rate as savings_plan_rate,
            p.price_per_hour_usd as current_on_demand_rate,
            CASE 
                WHEN p.price_per_hour_usd > 0 
                THEN (1 - spr.rate / p.price_per_hour_usd) * 100
                ELSE 0 
            END as savings_percentage,
            sp.commitment_amount_hourly,
            sp.payment_option,
            sp.term_duration_years
        FROM aws_savings_plans_rates spr
        LEFT JOIN aws_pricing p ON (
            spr.instance_type = p.instance_type 
            AND spr.region = p.region_code
        )
        LEFT JOIN aws_savings_plans sp ON spr.savings_plan_id = sp.savings_plan_id
        WHERE spr.service_code = 'AmazonEC2'
            AND p.price_per_hour_usd IS NOT NULL
            AND spr.rate > 0
        ORDER BY savings_percentage DESC
        """
        
        # Example 5: RDS cost analysis with pricing data
        queries["rds_cost_analysis"] = """
        SELECT 
            c.product_instance_class,
            c.product_region,
            c.product_database_engine,
            c.product_deployment_option,
            SUM(c.line_item_unblended_cost) as actual_cost,
            SUM(c.line_item_usage_amount) as usage_hours,
            AVG(rp.price_per_hour_usd) as current_on_demand_rate,
            SUM(c.line_item_usage_amount * rp.price_per_hour_usd) as theoretical_cost,
            SUM(c.line_item_unblended_cost) - SUM(c.line_item_usage_amount * rp.price_per_hour_usd) as cost_difference
        FROM CUR c
        LEFT JOIN aws_rds_pricing rp ON (
            c.product_instance_class = rp.instance_class 
            AND c.product_region = rp.region_code
            AND c.product_database_engine = rp.database_engine
            AND c.product_deployment_option = rp.deployment_option
        )
        WHERE c.line_item_product_code = 'AmazonRDS'
            AND c.line_item_line_item_type = 'Usage'
            AND rp.price_per_hour_usd IS NOT NULL
        GROUP BY 
            c.product_instance_class,
            c.product_region,
            c.product_database_engine,
            c.product_deployment_option
        ORDER BY actual_cost DESC
        """
        
        return queries
    
    def run_api_data_analysis(self) -> Dict[str, pl.DataFrame]:
        """
        Run comprehensive analysis using API data sources.
        
        Returns:
            Dictionary of analysis results as Polars DataFrames
        """
        print("Running comprehensive API data analysis...")
        
        results = {}
        queries = self.get_example_join_queries()
        
        for analysis_name, sql_query in queries.items():
            try:
                print(f"\nRunning analysis: {analysis_name}")
                result_df = self.engine.query(sql_query)
                results[analysis_name] = result_df
                print(f"{analysis_name}: {len(result_df)} rows")
                
                # Show sample results
                if len(result_df) > 0:
                    print(f"📋 Sample results for {analysis_name}:")
                    print(result_df.head(3))
                    
            except Exception as e:
                print(f"Error in {analysis_name}: {e}")
                results[analysis_name] = pl.DataFrame()
        
        return results
    
    def get_available_api_tables(self) -> List[str]:
        """
        Get list of available API data tables.
        
        Returns:
            List of table names
        """
        tables = []
        
        if self.engine.config.enable_pricing_api:
            tables.extend(["aws_pricing", "aws_rds_pricing"])
            
        if self.engine.config.enable_savings_plans_api:
            tables.extend(["aws_savings_plans", "aws_savings_plans_rates"])
            
        return tables
    
    def describe_table_schemas(self) -> Dict[str, Dict]:
        """
        Get schema information for API data tables.
        
        Returns:
            Dictionary with table schemas
        """
        schemas = {}
        
        # AWS Pricing table schema
        schemas["aws_pricing"] = {
            "description": "EC2 instance pricing from AWS Pricing API",
            "key_columns": ["region_code", "instance_type", "operating_system", "tenancy"],
            "join_columns": {
                "CUR2.0": [
                    "product_region -> region_code",
                    "product_instance_type -> instance_type", 
                    "product_operating_system -> operating_system",
                    "product_tenancy -> tenancy"
                ]
            },
            "useful_for": [
                "Cost efficiency analysis",
                "On-demand rate lookups",
                "Instance specification analysis"
            ]
        }
        
        # AWS RDS Pricing table schema  
        schemas["aws_rds_pricing"] = {
            "description": "RDS instance pricing from AWS Pricing API",
            "key_columns": ["region_code", "instance_class", "database_engine", "deployment_option"],
            "join_columns": {
                "CUR2.0": [
                    "product_region -> region_code",
                    "product_instance_class -> instance_class",
                    "product_database_engine -> database_engine",
                    "product_deployment_option -> deployment_option"
                ]
            },
            "useful_for": [
                "RDS cost analysis",
                "Database pricing comparisons"
            ]
        }
        
        # AWS Savings Plans table schema
        schemas["aws_savings_plans"] = {
            "description": "Active Savings Plans from SavingsPlans API",
            "key_columns": ["savings_plan_id", "savings_plan_arn", "savings_plan_type", "region"],
            "join_columns": {
                "CUR2.0": [
                    "savings_plan_arn -> savings_plan_savings_plan_a_r_n"
                ]
            },
            "useful_for": [
                "Savings Plans coverage analysis",
                "Commitment tracking",
                "Cost allocation"
            ]
        }
        
        # AWS Savings Plans Rates table schema
        schemas["aws_savings_plans_rates"] = {
            "description": "Detailed rates for Savings Plans from SavingsPlans API",
            "key_columns": ["savings_plan_id", "instance_type", "region", "usage_type", "rate"],
            "join_columns": {
                "CUR2.0": [
                    "savings_plan_id -> derived from savings_plan_savings_plan_a_r_n",
                    "instance_type -> product_instance_type",
                    "region -> product_region"
                ]
            },
            "useful_for": [
                "Detailed Savings Plans rate analysis",
                "Comparing SP rates with on-demand pricing"
            ]
        }
        
        return schemas
    
    def export_api_data_for_external_use(self, output_directory: str) -> Dict[str, str]:
        """
        Export API data to files for use in external tools.
        
        Args:
            output_directory: Directory to save exported files
        
        Returns:
            Dictionary mapping table names to exported file paths
        """
        output_dir = Path(output_directory)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        exported_files = {}
        available_tables = self.get_available_api_tables()
        
        for table_name in available_tables:
            try:
                # Query the full table
                df = self.engine.query(f"SELECT * FROM {table_name}")
                
                if not df.is_empty():
                    # Export as parquet (most efficient)
                    parquet_file = output_dir / f"{table_name}.parquet"
                    df.write_parquet(parquet_file)
                    
                    # Also export as CSV for broader compatibility
                    csv_file = output_dir / f"{table_name}.csv"
                    df.write_csv(csv_file)
                    
                    exported_files[table_name] = {
                        "parquet": str(parquet_file),
                        "csv": str(csv_file),
                        "rows": len(df),
                        "columns": len(df.columns)
                    }
                    
                    print(f"Exported {table_name}: {len(df)} rows -> {parquet_file}")
                    
            except Exception as e:
                print(f"Error exporting {table_name}: {e}")
        
        return exported_files