#!/usr/bin/env python3
"""
Example: Using AWS Pricing API and SavingsPlans API with CUR2.0 data

This example demonstrates how to use the new API data sources to enrich 
CUR2.0 analysis with pricing and Savings Plans information.
"""

from de_polars.engine.data_config import DataConfig, DataExportType
from de_polars.finops_engine import FinOpsEngine
from de_polars.data.api_data_examples import ApiDataExamples


def main():
    """Main example function."""
    
    # 1. Configure with API data sources enabled
    print("🔧 Configuring FinOps engine with API data sources...")
    
    config = DataConfig(
        # Core CUR2.0 configuration (remember: CUR2.0 only per user requirement)
        s3_bucket="cid-014498620306-data-local",
        s3_data_prefix="cur2/014498620306/cid-cur2/data", 
        data_export_type=DataExportType.CUR_2_0,
        table_name="CUR",
        
        # AWS credentials and region
        aws_region="us-east-1",
        # aws_profile="your-profile",  # Uncomment if using AWS profiles
        
        # Date filtering (optional)
        date_start="2024-08",
        date_end="2024-12",
        
        # Enable API data sources
        enable_pricing_api=True,
        enable_savings_plans_api=True,
        
        # Configure API data collection
        pricing_api_regions=["us-east-1", "us-west-2", "eu-west-1"],
        pricing_api_instance_types=["t3.micro", "t3.small", "m5.large", "c5.xlarge"],
        savings_plans_include_rates=True,
        api_cache_max_age_days=1,
        
        # Local cache for better performance
        local_data_path="./local_data_cache",
        prefer_local_data=True
    )
    
    # 2. Initialize FinOps engine
    print("🚀 Initializing FinOps engine...")
    engine = FinOpsEngine(config)
    
    # 3. Initialize API data examples helper
    api_examples = ApiDataExamples(engine)
    
    # 4. Show available API tables
    print("\n📋 Available API data tables:")
    available_tables = api_examples.get_available_api_tables()
    for table in available_tables:
        print(f"  - {table}")
    
    # 5. Show table schemas and join information
    print("\n📊 Table schemas and join patterns:")
    schemas = api_examples.describe_table_schemas()
    for table_name, schema_info in schemas.items():
        print(f"\n{table_name}:")
        print(f"  Description: {schema_info['description']}")
        print(f"  Key columns: {', '.join(schema_info['key_columns'])}")
        print(f"  Join with CUR2.0: {', '.join(schema_info['join_columns']['CUR2.0'])}")
    
    # 6. Run example queries that join CUR2.0 with API data
    print("\n🔍 Running example join analyses...")
    
    # Example 1: Simple cost efficiency check
    print("\n📊 Example 1: Cost efficiency analysis")
    try:
        cost_efficiency_query = """
        SELECT 
            product_instance_type,
            product_region,
            SUM(line_item_unblended_cost) as actual_cost,
            SUM(line_item_usage_amount) as usage_hours,
            AVG(aws_pricing.price_per_hour_usd) as current_rate,
            COUNT(*) as line_items
        FROM CUR
        LEFT JOIN aws_pricing ON (
            CUR.product_instance_type = aws_pricing.instance_type 
            AND CUR.product_region = aws_pricing.region_code
            AND CUR.product_operating_system = aws_pricing.operating_system
        )
        WHERE line_item_product_code = 'AmazonEC2'
            AND line_item_line_item_type = 'Usage'
            AND aws_pricing.price_per_hour_usd IS NOT NULL
        GROUP BY product_instance_type, product_region
        ORDER BY actual_cost DESC
        LIMIT 10
        """
        
        result = engine.query(cost_efficiency_query)
        print(f"✅ Found {len(result)} instance types with pricing data")
        if len(result) > 0:
            print("📋 Top 5 results:")
            print(result.head(5))
        
    except Exception as e:
        print(f"❌ Error in cost efficiency analysis: {e}")
    
    # Example 2: Savings Plans coverage
    print("\n📊 Example 2: Savings Plans coverage analysis")
    try:
        savings_coverage_query = """
        SELECT 
            bill_billing_period_start_date,
            SUM(line_item_unblended_cost) as total_cost,
            SUM(CASE WHEN savings_plan_savings_plan_a_r_n IS NOT NULL 
                     THEN line_item_unblended_cost ELSE 0 END) as sp_covered_cost,
            COUNT(DISTINCT aws_savings_plans.savings_plan_id) as active_savings_plans
        FROM CUR
        LEFT JOIN aws_savings_plans ON CUR.savings_plan_savings_plan_a_r_n = aws_savings_plans.savings_plan_arn
        WHERE line_item_product_code = 'AmazonEC2'
        GROUP BY bill_billing_period_start_date
        ORDER BY bill_billing_period_start_date DESC
        LIMIT 5
        """
        
        result = engine.query(savings_coverage_query)
        print(f"✅ Found {len(result)} billing periods")
        if len(result) > 0:
            print("📋 Results:")
            print(result)
        
    except Exception as e:
        print(f"❌ Error in Savings Plans analysis: {e}")
    
    # 7. Run comprehensive analysis suite
    print("\n🔬 Running comprehensive API data analysis suite...")
    try:
        analysis_results = api_examples.run_api_data_analysis()
        
        print(f"\n✅ Completed {len(analysis_results)} analyses:")
        for name, df in analysis_results.items():
            if not df.is_empty():
                print(f"  - {name}: {len(df)} rows")
            else:
                print(f"  - {name}: No data")
                
    except Exception as e:
        print(f"❌ Error in comprehensive analysis: {e}")
    
    # 8. Export API data for external use
    print("\n💾 Exporting API data for external tools...")
    try:
        export_results = api_examples.export_api_data_for_external_use("./exported_api_data")
        
        print("✅ Exported files:")
        for table_name, file_info in export_results.items():
            if isinstance(file_info, dict):
                print(f"  - {table_name}: {file_info['rows']} rows")
                print(f"    Parquet: {file_info['parquet']}")
                print(f"    CSV: {file_info['csv']}")
            
    except Exception as e:
        print(f"❌ Error exporting data: {e}")
    
    print("\n🎉 Example completed! ")
    print("\n💡 Key benefits of API data integration:")
    print("  - Real-time pricing data for cost analysis")
    print("  - Savings Plans coverage and utilization tracking") 
    print("  - Enhanced cost optimization insights")
    print("  - Seamless joins with existing CUR2.0 data")
    print("\n🔗 Tables are automatically registered and ready for SQL joins!")


if __name__ == "__main__":
    main()