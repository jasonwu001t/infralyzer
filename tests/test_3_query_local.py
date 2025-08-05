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
    table_name = 'CUR'

    config = DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',       
        data_export_type=DataExportType.CUR_2_0,     
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
        
        print("Getting sample data...")
        # Default is now DataFrame! No need to specify format
        services = engine.query("""
            SELECT 
                *
            FROM {}
            LIMIT 2
        """.format(table_name))

        print ('QUERY RESULTS: ', services)
        
        print("TEST PASSED")
        return True
        
    except Exception as e:
        print(f"TEST FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_query_local()