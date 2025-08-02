"""
Test 4: SQL Views with Dependencies
===================================

This test demonstrates how to execute SQL view files with dependencies.
Tests the cascading system where level 2 views depend on level 1 results.
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from de_polars import FinOpsEngine, DataConfig, DataExportType
import shutil
from pathlib import Path

def test_sql_views():
    """Test executing SQL views with dependencies"""
    
    print("Test 4: SQL Views with Dependencies")
    
    # Configuration using local data from Test 2
    local_path = "./test_local_data"
    views_output_path = "./test_views_output"
    
    config = DataConfig(
        s3_bucket='cid-014498620306-data-local',
        s3_data_prefix='cur2/014498620306/cid-cur2/data',
        data_export_type=DataExportType.CUR_2_0,               
        table_name='CUR',                        
        date_start='2024-06',                    
        date_end='2025-07',
        local_data_path=local_path,
        prefer_local_data=True
    )
    
    try:
        # Check if local data exists
        if not os.path.exists(local_path):
            print(f"Local data not found at {local_path}")
            print("Run test_2_download_local.py first")
            return False
        
        # Clean up any existing view output
        if os.path.exists(views_output_path):
            shutil.rmtree(views_output_path)
        os.makedirs(views_output_path, exist_ok=True)
        
        # Initialize engine
        print("Initializing engine...")
        engine = FinOpsEngine(config)
        
        # Execute Level 1 Independent Views
        print("Executing Level 1 views...")
        
        level1_views = [
            'account_map.sql',
            'summary_view.sql'
        ]
        
        level1_results = {}
        for view_file in level1_views:
            view_path = f"cur2_views/level_1_independent/{view_file}"
            if os.path.exists(view_path):
                print(f"  {view_file}...")
                
                # Read SQL content
                with open(view_path, 'r') as f:
                    sql_content = f.read()
                
                # Execute the view
                result = engine.query(sql_content)
                print(f"    {len(result)} rows x {len(result.columns)} columns")
                
                # Save result as parquet for level 2 dependencies
                output_file = f"{views_output_path}/{view_file.replace('.sql', '.parquet')}"
                result.write_parquet(output_file)
                level1_results[view_file.replace('.sql', '')] = output_file
        
        # Register Level 1 Results as Tables
        print("Registering Level 1 results as tables...")
        
        # We need to create a new engine connection and register the parquet files
        # This simulates how level 2 views would access level 1 results
        conn = engine.engine._get_duckdb_connection()
        
        for table_name, parquet_path in level1_results.items():
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
            print(f"  Registered: {table_name}")
        
        # Execute Dependent Query
        print("Executing dependent query...")
        
        # Create a simple query that depends on level 1 results
        dependent_query = """
        SELECT 
            a.account_id,
            a.account_name,
            SUM(s.unblended_cost) as total_cost,
            COUNT(*) as total_line_items,
            SUM(s.usage_quantity) as total_usage
        FROM account_map a
        LEFT JOIN summary_view s 
            ON a.account_id = s.linked_account_id
        GROUP BY a.account_id, a.account_name
        ORDER BY total_cost DESC
        LIMIT 10
        """
        
        dependent_result = conn.execute(dependent_query).fetchdf()
        
        # Convert to polars for consistency
        import polars as pl
        dependent_result_pl = pl.from_pandas(dependent_result)
        
        print(f"Dependent query: {len(dependent_result_pl)} rows x {len(dependent_result_pl.columns)} columns")
        
        # Save dependent result
        dependent_output = f"{views_output_path}/account_summary.parquet"
        dependent_result_pl.write_parquet(dependent_output)
        
        # Verify results
        print("Verifying results...")
        total_files = len(level1_results) + 1  # Level 1 + dependent result
        print(f"Created {total_files} output files")
        
        # Performance test - query the cached results
        print("Testing performance...")
        
        import time
        start_time = time.time()
        
        # Test querying the cached parquet files directly
        fast_query = f"""
        SELECT 
            COUNT(*) as total_accounts,
            SUM(total_cost) as total_spend,
            AVG(total_cost) as avg_spend_per_account,
            SUM(total_line_items) as total_items
        FROM read_parquet('{dependent_output}')
        WHERE total_cost > 0
        """
        
        perf_result = conn.execute(fast_query).fetchone()
        end_time = time.time()
        
        print(f"Performance query: {(end_time - start_time):.3f} seconds")
        print(f"Total accounts: {perf_result[0]}, Total spend: ${perf_result[1]:.2f}")
        
        conn.close()
        
        print("TEST PASSED")
        return True
        
    except Exception as e:
        print(f"TEST FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_sql_views()