"""
Test 5: SQL Views with Dependencies
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
    
    print("🏗️ Test 5: SQL Views with Dependencies")
    print("=" * 50)
    
    # Configuration using local data from Test 2
    local_path = "./test_local_data"
    views_output_path = "./test_views_output"
    
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
        
        # Clean up any existing view output
        if os.path.exists(views_output_path):
            print(f"🧹 Cleaning up existing views at {views_output_path}")
            shutil.rmtree(views_output_path)
        os.makedirs(views_output_path, exist_ok=True)
        
        # Initialize engine
        print("🚀 Initializing FinOps Engine...")
        engine = FinOpsEngine(config)
        
        # Test 1: Execute Level 1 Independent Views
        print("\n📊 Step 1: Execute Level 1 Independent Views")
        print("-" * 40)
        
        level1_views = [
            'account_map.sql',
            'summary_view.sql'
        ]
        
        level1_results = {}
        for view_file in level1_views:
            view_path = f"cur2_views/level_1_independent/{view_file}"
            if os.path.exists(view_path):
                print(f"🔨 Executing {view_file}...")
                
                # Read SQL content
                with open(view_path, 'r') as f:
                    sql_content = f.read()
                
                # Execute the view
                result = engine.query(sql_content)
                print(f"✅ {view_file}: {len(result)} rows × {len(result.columns)} columns")
                
                # Save result as parquet for level 2 dependencies
                output_file = f"{views_output_path}/{view_file.replace('.sql', '.parquet')}"
                result.write_parquet(output_file)
                level1_results[view_file.replace('.sql', '')] = output_file
                print(f"💾 Saved: {output_file}")
        
        # Test 2: Create temporary table from level 1 results for level 2
        print("\n🔗 Step 2: Register Level 1 Results as Tables")
        print("-" * 40)
        
        # We need to create a new engine connection and register the parquet files
        # This simulates how level 2 views would access level 1 results
        conn = engine.engine._get_duckdb_connection()
        
        for table_name, parquet_path in level1_results.items():
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
            print(f"📋 Registered table: {table_name}")
        
        # Test 3: Test a simple level 2 dependency
        print("\n🏗️ Step 3: Execute Dependent Query")
        print("-" * 40)
        
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
        
        print("🔨 Executing dependent query...")
        dependent_result = conn.execute(dependent_query).fetchdf()
        
        # Convert to polars for consistency
        import polars as pl
        dependent_result_pl = pl.from_pandas(dependent_result)
        
        print(f"✅ Dependent query: {len(dependent_result_pl)} rows × {len(dependent_result_pl.columns)} columns")
        
        # Save dependent result
        dependent_output = f"{views_output_path}/account_summary.parquet"
        dependent_result_pl.write_parquet(dependent_output)
        print(f"💾 Saved: {dependent_output}")
        
        # Test 4: Verify the dependency chain works
        print("\n🔍 Step 4: Verify Results")
        print("-" * 40)
        
        print("📈 Level 1 Results:")
        for table_name, parquet_path in level1_results.items():
            file_size = os.path.getsize(parquet_path) / 1024  # KB
            print(f"   • {table_name}: {file_size:.1f} KB")
        
        print("\n📈 Level 2 Result:")
        dependent_size = os.path.getsize(dependent_output) / 1024  # KB
        print(f"   • account_summary: {dependent_size:.1f} KB")
        
        print("\n📊 Sample Account Summary (Top 5):")
        for i, row in enumerate(dependent_result_pl.head(5).iter_rows(named=True)):
            cost = row.get('total_cost', 0) or 0
            items = row.get('total_line_items', 0) or 0
            print(f"   {i+1}. {row['account_name']}: ${cost:.2f} ({items} items)")
        
        # Test 5: Performance test - query the cached results
        print("\n⚡ Step 5: Performance Test")
        print("-" * 40)
        
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
        
        print(f"✅ Performance query completed in {(end_time - start_time):.3f} seconds")
        print(f"   • Total accounts with spend: {perf_result[0]}")
        print(f"   • Total spend: ${perf_result[1]:.2f}")
        print(f"   • Average spend per account: ${perf_result[2]:.2f}")
        print(f"   • Total line items: {perf_result[3]}")
        
        conn.close()
        
        print(f"\n🎉 Test 5 PASSED: Successfully executed SQL views with dependencies!")
        return True
        
    except Exception as e:
        print(f"❌ Test 5 FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_sql_views()