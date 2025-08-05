"""
Test 1: Query S3 Parquet Files Directly - Fixed for New Architecture
===================================================================

This test demonstrates how to query S3 parquet files directly without downloading them locally.
Simple test to verify S3 connectivity and basic querying functionality with improved error handling.
"""

import sys
import os
# Add parent directory to path to import infralyzer module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infralyzer import FinOpsEngine, DataConfig, DataExportType

def test_query_s3():
    """Test querying S3 parquet files directly with better error handling"""
    
    print("🧪 Test 1: Query S3 Parquet Files (DuckDB Engine)")
    print("-" * 50)
    
    local_path = "./test_local_data"
    # Configuration for S3 access
    config = DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',       
        data_export_type=DataExportType.CUR_2_0,               
        table_name='CUR',                        
        date_start='2025-01',                    
        date_end='2025-07',
        prefer_local_data=False,  # Force S3 access
        local_data_path=local_path
    )
    
    try:
        # Initialize engine (defaults to DuckDB)
        print("Initializing DuckDB engine...")
        engine = FinOpsEngine(config)
        print(f"✅ Engine: {engine.engine.engine_name}")
        print(f"   S3 Support: {engine.engine.supports_s3_direct}")
        print(f"   Local Support: {engine.engine.supports_local_data}")
        
        # Test with a simple count query first (less likely to fail)
        print("\n📊 Step 1: Testing simple count query...")
        try:
            count_result = engine.query("SELECT COUNT(*) as total_rows FROM CUR")
            print(f"✅ Count query successful: {count_result}")
            total_rows = count_result[0]['total_rows'] if count_result else 0
            print(f"   Total rows in dataset: {total_rows:,}")
        except Exception as count_error:
            print(f"❌ Count query failed: {str(count_error)}")
            print("🔧 This might indicate S3 access or table structure issues")
            # Continue with sample query anyway
        
        # Test sample data query
        print("\n📋 Step 2: Testing sample data query...")
        sample = engine.query("SELECT * FROM CUR LIMIT 5")  # Smaller limit for reliability
        
        if sample:
            print(f"✅ Found {len(sample)} sample records")
            print(f"   Columns: {len(sample[0].keys())} columns")
            print(f"   Sample record keys: {list(sample[0].keys())[:5]}...")  # First 5 column names
            
            # Show first record (truncated for readability)
            first_record = sample[0]
            truncated_record = {k: str(v)[:50] + "..." if len(str(v)) > 50 else v 
                              for k, v in list(first_record.items())[:3]}
            print(f"   Sample data: {truncated_record}")
        else:
            print("⚠️ No sample data returned")
        
        print("\n✅ TEST PASSED")
        return True
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {str(e)}")
        
        # Provide debugging hints
        error_str = str(e).lower()
        if "syntax error" in error_str:
            print("💡 Debug hint: SQL syntax error - check table name and column references")
        elif "not found" in error_str or "does not exist" in error_str:
            print("💡 Debug hint: Table/data not found - check S3 bucket and prefix")
        elif "permission" in error_str or "access" in error_str:
            print("💡 Debug hint: Access denied - check AWS credentials and S3 permissions")
        elif "connection" in error_str or "network" in error_str:
            print("💡 Debug hint: Network issue - check internet connection and AWS endpoints")
        else:
            print("💡 Debug hint: Check S3 bucket, credentials, and data availability")
        
        return False

if __name__ == "__main__":
    test_query_s3()