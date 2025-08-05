"""
Test 5: DataPartitioner & SQL Library
=======================================

This test demonstrates SQL file execution from cur2_query_library.
Tests batch SQL processing and query library management.
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infralyzer import FinOpsEngine, DataConfig, DataExportType, DataPartitioner
import shutil

def test_data_partitioner():
    """Test Enhanced FinOpsEngine Query (SQL files) and DataPartitioner capabilities"""
    
    print("Test 5: Enhanced FinOpsEngine Query & SQL Library")
    
    # Configuration using local data from Test 2
    local_path = "./test_local_data"
    partitioner_output = "./test_partitioner_output"  # Only used for DataPartitioner file discovery

    config = DataConfig(
        s3_bucket= 'cid-014498620306-data-local',  
        s3_data_prefix='cur2/014498620306/cid-cur2/data',
        data_export_type=DataExportType.CUR_2_0,               
        table_name='CUR',                        
        date_start='2024-07',                    
        date_end='2025-07',
        local_data_path=local_path,
        prefer_local_data=True
    )
    
    try:
        # Check if local data exists
        if not os.path.exists(local_path):
            print(f"Local data not found at {local_path}")
            return False
        
        # Clean up any existing partitioner output
        if os.path.exists(partitioner_output):
            shutil.rmtree(partitioner_output)
        
        # Initialize engine and partitioner
        print("Initializing engine...")
        engine = FinOpsEngine(config)
        
        # Initialize DataPartitioner
        partitioner = DataPartitioner(
            source_client=engine,
            output_base_dir=partitioner_output,
            query_library_path="cur2_analytics"
        )
        # Discover SQL files
        sql_categories = partitioner.discover_sql_files()
        total_files = sum(len(files) for files in sql_categories.values())
        print(f"Found {total_files} SQL files")
        
        # Select SQL files for execution
        selected_sql_files = []
        for category, files in sql_categories.items():
            for file in files:
                if file.endswith('.sql'):
                    if category == ".":
                        full_path = f"cur2_analytics/{file}"
                    else:
                        full_path = f"cur2_analytics/{category}/{file}"
                    selected_sql_files.append(full_path)
            #     if len(selected_sql_files) >= 3:
            #         break
            # if len(selected_sql_files) >= 3:
            #     break
        
        # Execute SQL files and save results
        print(f"Executing {len(selected_sql_files)} SQL files...")
        results = {}
        for sql_file in selected_sql_files:
            try:
                result_df = engine.query(sql_file)
                results[sql_file] = result_df
                
                # Save result to partitioner_output folder as parquet
                file_name = sql_file.split('/')[-1].replace('.sql', '.parquet')
                output_file = f"{partitioner_output}/{file_name}"
                
                # Ensure output directory exists
                os.makedirs(partitioner_output, exist_ok=True)
                
                # Save DataFrame as parquet
                result_df.write_parquet(output_file)
                
                print(f"  {sql_file}: {len(result_df)} rows -> {file_name}")
            except Exception as e:
                print(f"  {sql_file}: Failed - {str(e)[:50]}")
        
        print(f"Executed {len(results)}/{len(selected_sql_files)} files successfully")
        
        # Verify output files
        if os.path.exists(partitioner_output):
            output_files = []
            for root, dirs, files in os.walk(partitioner_output):
                for file in files:
                    if file.endswith('.parquet'):
                        file_path = os.path.join(root, file)
                        file_size = os.path.getsize(file_path)
                        output_files.append((file, file_size))
            
            print(f"Created {len(output_files)} parquet files in {partitioner_output}")
            for filename, size in output_files:
                size_kb = size / 1024
                print(f"  {filename}: {size_kb:.1f} KB")
        
        # Test enhanced query method with parquet files using SQL syntax
        print("Testing enhanced query with parquet files...")
        if output_files:
            sample_parquet = f"{partitioner_output}/{output_files[0][0]}"
            parquet_result = engine.query(f"SELECT * FROM '{sample_parquet}'")
            print(parquet_result)

        return True
        
    except Exception as e:
        print(f"TEST FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_data_partitioner()