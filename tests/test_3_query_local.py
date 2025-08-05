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

from infralyzer import FinOpsEngine, DataConfig, DataExportType

def test_query_local():
    """Test querying local parquet files"""
    
    print("Test 3: Query Local Parquet Files")
    
    # Configuration for local data access
    local_path = "./test_local_data"
    table_name = 'FOCUS'

    config = DataConfig(
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
        # Initialize engine
        print("Initializing engine...")
        engine = FinOpsEngine(config)
        
        # Check if local data exists
        data_source_path = config.local_bucket_path
        if not data_source_path or not os.path.exists(data_source_path):
            print(f"Local data not found at {data_source_path}")
            print("Run test_2_download_local.py first")
            return False
        
        # Basic count query
        print("Running count query...")
        result = engine.query("SELECT COUNT(*) as total_records FROM {}".format(table_name))
        print(f"Total records: {result['total_records'][0]:,}")
        
        # Sample data query
        print("Getting sample data...")
        services = engine.query("""
            SELECT 
                *
            FROM {}
            LIMIT 10
        """.format(table_name))

        print(f"Sample data: {len(services)} rows x {len(services.columns)} columns")
        
        print("TEST PASSED")
        return True
        
    except Exception as e:
        print(f"TEST FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_query_local()