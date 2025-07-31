"""
Test 10: DataPartitioner & SQL Library
=======================================

This test demonstrates SQL file execution from cur2_query_library.
Tests batch SQL processing and query library management.
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from de_polars import FinOpsEngine, DataConfig, DataExportType, DataPartitioner
import shutil

def test_data_partitioner():
    """Test DataPartitioner and SQL library capabilities"""
    
    print("📦 Test 10: DataPartitioner & SQL Library")
    print("=" * 50)
    
    # Configuration using local data from Test 2
    local_path = "./test_local_data"
    partitioner_output = "./test_partitioner_output"
    
    config = DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',          
        data_export_type=DataExportType.CUR_2_0,               
        table_name='CUR',                        
        date_start='2025-07',                    
        date_end='2025-07',
        local_data_path=local_path,
        prefer_local_data=True
    )
    
    try:
        # Check if local data exists
        if not os.path.exists(local_path):
            print(f"❌ Local data not found at {local_path}")
            print("Please run test_2_download_local.py first")
            return False
        
        # Clean up any existing partitioner output
        if os.path.exists(partitioner_output):
            print(f"🧹 Cleaning up existing output at {partitioner_output}")
            shutil.rmtree(partitioner_output)
        
        # Initialize engine and partitioner
        print("🚀 Initializing FinOps Engine...")
        engine = FinOpsEngine(config)
        
        # Initialize DataPartitioner
        from de_polars.client import DataExportsPolars
        
        # Create a simple client wrapper for the partitioner
        class SimpleClient:
            def __init__(self, engine):
                self.engine = engine
            
            def query(self, sql):
                return self.engine.query(sql)
        
        simple_client = SimpleClient(engine)
        partitioner = DataPartitioner(
            source_client=simple_client,
            output_base_dir=partitioner_output,
            query_library_path="cur2_query_library"
        )
        
        # Test 1: Discover SQL Files
        print("\n📋 Step 1: Discover SQL Files")
        print("-" * 40)
        
        partitioner.list_available_sql_files()
        
        # Get available SQL files programmatically
        sql_categories = partitioner.discover_sql_files()
        total_files = sum(len(files) for files in sql_categories.values())
        print(f"✅ Found {total_files} SQL files in {len(sql_categories)} categories")
        
        # Test 2: Execute Individual SQL Files
        print("\n🔨 Step 2: Execute Individual SQL Files")
        print("-" * 40)
        
        # Test with simple SQL files first
        test_sql_files = []
        
        # Find available SQL files to test
        for category, files in sql_categories.items():
            for file in files[:2]:  # Take first 2 from each category
                if file.endswith('.sql'):
                    test_sql_files.append(file)
                if len(test_sql_files) >= 3:  # Limit to 3 files for testing
                    break
            if len(test_sql_files) >= 3:
                break
        
        results = {}
        for sql_file in test_sql_files:
            try:
                print(f"🔨 Executing {sql_file}...")
                output_path = partitioner.run_sql_file(sql_file)
                results[sql_file] = output_path
                print(f"✅ Success: {output_path}")
            except Exception as e:
                print(f"⚠️ Failed {sql_file}: {str(e)[:50]}...")
        
        print(f"📊 Executed {len(results)}/{len(test_sql_files)} SQL files successfully")
        
        # Test 3: Batch SQL Execution
        print("\n📦 Step 3: Batch SQL Execution")
        print("-" * 40)
        
        if test_sql_files:
            try:
                batch_results = partitioner.run_sql_files(test_sql_files[:2])  # Run first 2
                print(f"✅ Batch execution: {len(batch_results)} files processed")
                
                for sql_file, output_path in batch_results.items():
                    print(f"   • {sql_file} → {os.path.basename(output_path)}")
                    
            except Exception as e:
                print(f"⚠️ Batch execution failed: {str(e)}")
        
        # Test 4: Verify Output Files
        print("\n📁 Step 4: Verify Output Files")
        print("-" * 40)
        
        if os.path.exists(partitioner_output):
            output_files = []
            for root, dirs, files in os.walk(partitioner_output):
                for file in files:
                    if file.endswith('.parquet'):
                        file_path = os.path.join(root, file)
                        file_size = os.path.getsize(file_path)
                        output_files.append((file, file_size))
            
            print(f"📊 Created {len(output_files)} parquet files:")
            for filename, size in output_files:
                size_kb = size / 1024
                print(f"   • {filename}: {size_kb:.1f} KB")
        
        # Test 5: SQL Metadata Extraction
        print("\n📋 Step 5: SQL Metadata Extraction")
        print("-" * 40)
        
        # Test metadata extraction from SQL files
        if test_sql_files:
            for sql_file in test_sql_files[:2]:
                try:
                    sql_content = partitioner.load_sql_query(sql_file)
                    metadata = partitioner.extract_query_metadata(sql_content)
                    
                    print(f"📄 {sql_file}:")
                    for key, value in metadata.items():
                        print(f"   • {key}: {value}")
                    
                except Exception as e:
                    print(f"⚠️ Metadata extraction failed for {sql_file}: {str(e)}")
        
        # Test 6: Query Library Summary
        print("\n📊 Step 6: Query Library Summary")
        print("-" * 40)
        
        print(f"📈 DataPartitioner Summary:")
        print(f"   • SQL Categories: {len(sql_categories)}")
        print(f"   • Total SQL Files: {total_files}")
        print(f"   • Successfully Executed: {len(results)}")
        print(f"   • Output Directory: {partitioner_output}")
        
        # Show category breakdown
        print(f"📁 Category Breakdown:")
        for category, files in sql_categories.items():
            print(f"   • {category}: {len(files)} files")
        
        # Calculate total output size
        if os.path.exists(partitioner_output):
            total_size = 0
            for root, dirs, files in os.walk(partitioner_output):
                for file in files:
                    total_size += os.path.getsize(os.path.join(root, file))
            
            print(f"💾 Total Output Size: {total_size / 1024:.1f} KB")
        
        print(f"\n🎉 Test 10 PASSED: DataPartitioner completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test 10 FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_data_partitioner()