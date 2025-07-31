"""
Test 3: Query Local Parquet Files
=================================

This test demonstrates how to query local parquet files for fast analytics.
Requires local data to be downloaded first (run test_2_download_local.py).
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from de_polars import FinOpsEngine, DataConfig, DataExportType

def test_query_local():
    """Test querying local parquet files"""
    
    print("⚡ Test 3: Query Local Parquet Files")
    print("=" * 50)
    
    # Configuration for local data access
    local_path = "./test_local_data"
    table_name = 'FOCUS'

    config = DataConfig(
        # s3_bucket='billing-data-exports-cur',          
        # s3_data_prefix='cur2/cur2/data',          
        # data_export_type=DataExportType.CUR_2_0,     

        s3_bucket='billing-data-exports-focus',          
        s3_data_prefix='focus1/focus1/data',       
        data_export_type=DataExportType.FOCUS_1_0,     
        table_name=table_name,                        
        date_start='2025-01',                    
        date_end='2025-07',
        local_data_path=local_path,
        prefer_local_data=True  # Use local data
    )
    
    try:
        # Initialize engine to get data source specific path
        print("🚀 Initializing FinOps Engine for local data access...")
        engine = FinOpsEngine(config)
        
        # Check if specific data source data exists
        data_source_path = config.local_bucket_path
        if not data_source_path or not os.path.exists(data_source_path):
            print(f"❌ Local data not found for this data source at {data_source_path}")
            print("💡 Run test_2_download_local.py first to download data for this data source")
            return False
        
        # Test 1: Basic count query
        print("\n📊 Test 1: Basic count query...")
        result = engine.query("SELECT COUNT(*) as total_records FROM {}".format(table_name))
        print(f"✅ Total records: {result['total_records'][0]:,}")
        
        # Test 2: Service breakdown
        print("\n🔧 Test 2: Service breakdown...")
        services = engine.query("""
            SELECT 
                *
            FROM {}
            LIMIT 10
        """.format(table_name))

        print (services)
        # print(f"✅ Top 10 Services by Cost:")
        # for row in services.iter_rows(named=True):
        #     print(f"   • {row['service']}: ${row['total_cost']:,.2f} ({row['line_items']:,} items)")
        
        # # Test 3: Daily cost trends
        # print("\n📈 Test 3: Daily cost trends...")
        # daily_costs = engine.query("""
        #     SELECT 
        #         line_item_usage_start_date as date,
        #         SUM(line_item_unblended_cost) as daily_cost,
        #         COUNT(*) as line_items
        #     FROM {}
        #     GROUP BY line_item_usage_start_date 
        #     ORDER BY line_item_usage_start_date
        #     LIMIT 10
        # """.format(table_name))
        
        # print(f"✅ Daily Cost Trends (first 10 days):")
        # for row in daily_costs.iter_rows(named=True):
        #     print(f"   • {row['date']}: ${row['daily_cost']:,.2f} ({row['line_items']:,} items)")
        
        # # Test 4: Performance comparison
        # print("\n⏱️  Test 4: Performance test...")
        # import time
        
        # start_time = time.time()
        # complex_query = engine.query("""
        #     SELECT 
        #         product_servicecode,
        #         line_item_usage_type,
        #         SUM(line_item_unblended_cost) as cost,
        #         AVG(line_item_unblended_cost) as avg_cost,
        #         COUNT(*) as count
        #     FROM {}
        #     GROUP BY product_servicecode, line_item_usage_type
        #     ORDER BY cost DESC
        #     LIMIT 50
        # """.format(table_name))
        # end_time = time.time()
        
        # query_time = end_time - start_time
        # print(f"✅ Complex aggregation query completed in {query_time:.2f} seconds")
        # print(f"   Processed {len(complex_query)} service/usage type combinations")
        
        # print(f"\n🎉 Test 3 PASSED: Successfully queried local parquet files!")
        # return True
        
    except Exception as e:
        print(f"❌ Test 3 FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_query_local()