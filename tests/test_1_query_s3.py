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
    
    print("Test 1: Query S3 Parquet Files")
    local_path = "./test_local_data"
    # Configuration for S3 access
    config = DataConfig(
        s3_bucket='billing-data-exports-focus',          
        s3_data_prefix='focus1/focus1/data',       
        data_export_type=DataExportType.FOCUS_1_0,               
        table_name='FOCUS',                        
        date_start='2025-01',                    
        date_end='2025-07',
        prefer_local_data=True,  # If False Force S3 access
        local_data_path=local_path
    )
    
    try:
        # Initialize engine
        print("Initializing engine...")
        engine = FinOpsEngine(config)
        
        # Test sample data query
        print("Querying billing periods...")
        sample = engine.query("SELECT DISTINCT billing_period FROM FOCUS")
        print(f"Found {len(sample)} billing periods:")
        print(sample)
        
        print("TEST PASSED")
        return True
        
    except Exception as e:
        print(f"TEST FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_query_s3()