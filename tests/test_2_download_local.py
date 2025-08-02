"""
Test 2: Download and Store All Data Locally
===========================================

This test demonstrates how to download S3 data to local storage for faster access
and reduced S3 API costs. Creates local parquet files from S3 data.
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from de_polars import FinOpsEngine, DataConfig, DataExportType
import shutil

def test_download_local():
    """Test downloading S3 data to local storage"""
    
    print("Test 2: Download and Store Data Locally")
    
    # Configuration for local storage
    local_path = "./test_local_data"
    config = DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',            
        data_export_type=DataExportType.CUR_2_0,   
        table_name='CUR',                        
        date_start='2024-06',                    
        date_end='2025-07',
        local_data_path=local_path,
        prefer_local_data=True
    )
    
    try:
        # Initialize engine
        print("Initializing engine...")
        engine = FinOpsEngine(config)
        
        # Clean up existing data if needed
        data_source_path = config.local_bucket_path
        if data_source_path and os.path.exists(data_source_path):
            print(f"Cleaning up existing data at {data_source_path}")
            shutil.rmtree(data_source_path)
        
        # Download data locally
        print(f"Downloading data from S3 to {local_path}...")
        download_result = engine.download_data_locally()
        
        # Verify local files exist
        if os.path.exists(local_path):
            local_files = []
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    if file.endswith('.parquet'):
                        local_files.append(os.path.join(root, file))
            
            print(f"Created {len(local_files)} parquet files")
            
            # Test basic query on local data
            result = engine.query("SELECT COUNT(*) as total_records FROM CUR")
            print(f"Total records: {result['total_records'][0]:,}")
        
        print("TEST PASSED")
        return True, local_path
        
    except Exception as e:
        print(f"TEST FAILED: {str(e)}")
        return False, None

if __name__ == "__main__":
    test_download_local()