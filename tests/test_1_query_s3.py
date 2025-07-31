"""
Test 1: Query S3 Parquet Files Directly
========================================

This test demonstrates how to query S3 parquet files directly without downloading them locally.
Simple test to verify S3 connectivity and basic querying functionality.
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from de_polars import FinOpsEngine, DataConfig, DataExportType

def test_query_s3():
    """Test querying S3 parquet files directly"""
    
    print("🔍 Test 1: Querying S3 Parquet Files Directly")
    print("=" * 50)
    
    # Configuration for S3 access
    config = DataConfig(
        # s3_bucket='billing-data-exports-cur',          
        # s3_data_prefix='cur2/cur2/data',         
        s3_bucket='billing-data-exports-focus',          
        s3_data_prefix='focus1/focus1/data',       
        data_export_type=DataExportType.FOCUS_1_0,               
        table_name='FOCUS',                        
        date_start='2025-01',                    
        date_end='2025-07',
        prefer_local_data=False  # Force S3 access
    )
    
    try:
        # Initialize engine
        print("🚀 Initializing FinOps Engine...")
        engine = FinOpsEngine(config)
        
        # # Test basic query
        # print("\n📊 Running basic S3 query...")
        # result = engine.query("SELECT COUNT(*) as total_records FROM CUR LIMIT 10")
        # print(f"✅ Total records in S3: {result['total_records'][0]:,}")
        
        # Test sample data query
        # print("\n📋 Getting sample data...")
        # sample = engine.query("SELECT * FROM CUR LIMIT 5")
        sample = engine.query("SELECT DISTINCT billing_period FROM FOCUS") # LIMIT 5")
        print (sample)
        # print(f"✅ Sample data retrieved: {len(sample)} rows × {len(sample.columns)} columns")
        # print(f"Columns: {list(sample.columns)}")
        
        # # Test aggregate query
        # print("\n💰 Running cost aggregation query...")
        # cost_summary = engine.query("""
        #     SELECT 
        #         COUNT(*) as total_line_items,
        #         SUM(line_item_unblended_cost) as total_cost,
        #         COUNT(DISTINCT product_servicecode) as unique_services
        #     FROM CUR
        # """)
        
        # print(f"✅ Cost Summary from S3:")
        # print(f"   • Total line items: {cost_summary['total_line_items'][0]:,}")
        # print(f"   • Total cost: ${cost_summary['total_cost'][0]:,.2f}")
        # print(f"   • Unique services: {cost_summary['unique_services'][0]}")
        
        # print(f"\n🎉 Test 1 PASSED: Successfully queried S3 parquet files!")
        return True
        
    except Exception as e:
        print(f"❌ Test 1 FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_query_s3()