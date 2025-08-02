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
    
    print("💾 Test 2: Download and Store Data Locally")
    print("=" * 50)
    
    # Configuration for local storage
    local_path = "./test_local_data"
    config = DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',            
        data_export_type=DataExportType.CUR_2_0,   

        # s3_bucket='billing-data-exports-focus',          
        # s3_data_prefix='focus1/focus1/data',       
        # data_export_type=DataExportType.FOCUS_1_0,     
        table_name='CUR',                        
        date_start='2024-06',                    
        date_end='2025-07',
        local_data_path=local_path,
        prefer_local_data=True
    )
    
    try:
        # Initialize engine to get data source specific path
        print("🚀 Initializing FinOps Engine...")
        engine = FinOpsEngine(config)
        
        # Clean up only the specific data source directory (not all local data)
        data_source_path = config.local_bucket_path
        if data_source_path and os.path.exists(data_source_path):
            print(f"🧹 Cleaning up existing data for this data source at {data_source_path}")
            shutil.rmtree(data_source_path)
        elif os.path.exists(local_path):
            # If local_bucket_path doesn't exist yet, create base directory structure
            print(f"📁 Creating base local data directory at {local_path}")
        else:
            print(f"📁 No existing data found, will create fresh local cache")
        
        # Download data locally
        print(f"\n⬇️  Downloading data from S3 to {local_path}...")
        print("This may take a few minutes depending on data size...")
        
        download_result = engine.download_data_locally()
        
        print(f"✅ Download completed!")
        print(f"Local data path: {local_path}")
        
        # Verify local files exist
        if os.path.exists(local_path):
            local_files = []
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    if file.endswith('.parquet'):
                        local_files.append(os.path.join(root, file))
            
            print(f"\n📁 Local parquet files created: {len(local_files)}")
            for i, file_path in enumerate(local_files[:5], 1):  # Show first 5 files
                file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                print(f"   {i}. {os.path.basename(file_path)} ({file_size:.1f} MB)")
            
            if len(local_files) > 5:
                print(f"   ... and {len(local_files) - 5} more files")
        
        # Test basic query on local data
        print(f"\n🔍 Testing query on downloaded local data...")
        result = engine.query("SELECT COUNT(*) as total_records FROM CUR")
        print(f"✅ Total records in local data: {result['total_records'][0]:,}")
        
        print(f"\n🎉 Test 2 PASSED: Successfully downloaded and stored data locally!")
        return True, local_path
        
    except Exception as e:
        print(f"❌ Test 2 FAILED: {str(e)}")
        return False, None

if __name__ == "__main__":
    test_download_local()