#!/usr/bin/env python3
"""
Test 1: Multi-Engine S3 Query Test
==================================

This test demonstrates querying S3 parquet files using all three query engines:
- DuckDB (default, fast analytics)
- Polars (modern DataFrame operations)  
- AWS Athena (serverless cloud queries)

Tests the same simple query across all engines for comparison.
"""

import sys
import os
import time
# Add parent directory to path to import infralyzer module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infralyzer import FinOpsEngine, DataConfig, DataExportType
from infralyzer.engine import QueryResultFormat


def create_test_config():
    """Create test configuration for S3 access."""
    return DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',       
        data_export_type=DataExportType.CUR_2_0,               
        table_name='CUR',                        
        date_start='2025-01',                    
        date_end='2025-07',
        prefer_local_data=False,  # Force S3 access
        local_data_path="./test_local_data"
    )


def test_simple_query_with_engine(engine_name, config):
    """Test a simple query with a specific engine."""
    print(f"\n🔧 Testing {engine_name.upper()} Engine")
    print("-" * 40)
    
    try:
        # Initialize engine
        start_time = time.time()
        engine = FinOpsEngine(config, engine_name=engine_name)
        init_time = time.time() - start_time
        
        print(f"✅ Engine initialized: {engine.engine.engine_name}")
        print(f"   - S3 Support: {engine.engine.supports_s3_direct}")
        print(f"   - Local Support: {engine.engine.supports_local_data}")
        print(f"   - Init Time: {init_time:.2f}s")
        
        # Test basic queries from simple to complex
        test_queries = [
            {
                "name": "Simple Count", 
                "sql": "SELECT COUNT(*) as total_rows FROM CUR",
                "description": "Count total records"
            },
            {
                "name": "Sample Data",
                "sql": "SELECT * FROM CUR LIMIT 5", 
                "description": "Get sample records"
            },
            {
                "name": "Service Summary",
                "sql": """SELECT 
                    product_servicecode, 
                    COUNT(*) as line_items,
                    SUM(line_item_unblended_cost) as total_cost
                FROM CUR 
                GROUP BY product_servicecode 
                ORDER BY total_cost DESC 
                LIMIT 3""",
                "description": "Top services by cost"
            }
        ]
        
        results = {}
        
        for i, query_test in enumerate(test_queries, 1):
            print(f"\n   Query {i}: {query_test['name']}")
            print(f"   Description: {query_test['description']}")
            
            try:
                start_time = time.time()
                
                # Execute query - default format (List[Dict])
                result = engine.query(query_test['sql'])
                
                exec_time = time.time() - start_time
                
                # Store results
                results[query_test['name']] = {
                    'rows': len(result),
                    'columns': len(result[0].keys()) if result else 0,
                    'execution_time': exec_time,
                    'sample_data': result[:2] if result else []  # First 2 rows
                }
                
                print(f"   ✅ Success: {len(result)} rows, {exec_time:.3f}s")
                
                # Show sample data for first query
                if i == 1 and result:
                    print(f"   Sample: {result[0]}")
                
            except Exception as query_error:
                print(f"   ❌ Query failed: {str(query_error)}")
                results[query_test['name']] = {'error': str(query_error)}
        
        return {
            'engine': engine_name,
            'success': True,
            'results': results,
            'engine_info': {
                'name': engine.engine.engine_name,
                's3_support': engine.engine.supports_s3_direct,
                'local_support': engine.engine.supports_local_data
            }
        }
        
    except Exception as e:
        print(f"❌ Engine {engine_name} failed to initialize: {str(e)}")
        return {
            'engine': engine_name,
            'success': False,
            'error': str(e)
        }


def test_query_formats(engine_name, config):
    """Test different query result formats with an engine."""
    print(f"\n📊 Testing Query Formats with {engine_name.upper()}")
    print("-" * 40)
    
    try:
        engine = FinOpsEngine(config, engine_name=engine_name)
        
        # Simple query for format testing
        sql = "SELECT COUNT(*) as total_rows FROM CUR"
        
        format_tests = [
            (QueryResultFormat.RECORDS, "List[Dict] (default)"),
            (QueryResultFormat.DATAFRAME, "pandas.DataFrame"),
            (QueryResultFormat.CSV, "CSV string")
        ]
        
        if engine_name == 'duckdb':  # Arrow format works best with DuckDB
            format_tests.append((QueryResultFormat.ARROW, "PyArrow Table"))
        
        for format_type, description in format_tests:
            try:
                start_time = time.time()
                result = engine.query(sql, format=format_type)
                exec_time = time.time() - start_time
                
                result_type = type(result).__name__
                print(f"   ✅ {description}: {result_type} ({exec_time:.3f}s)")
                
                # Show sample of result
                if format_type == QueryResultFormat.CSV:
                    print(f"      Sample: {str(result)[:50]}...")
                elif format_type == QueryResultFormat.RECORDS:
                    print(f"      Sample: {result}")
                else:
                    print(f"      Sample: {str(result)[:100]}...")
                    
            except Exception as e:
                print(f"   ❌ {description}: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Format testing failed: {str(e)}")
        return False


def run_comprehensive_test():
    """Run comprehensive multi-engine test."""
    print("🚀 MULTI-ENGINE S3 QUERY TEST")
    print("=" * 50)
    
    # Create configuration
    config = create_test_config()
    
    print(f"Configuration:")
    print(f"  S3 Bucket: {config.s3_bucket}")
    print(f"  S3 Prefix: {config.s3_data_prefix}")
    print(f"  Date Range: {config.date_start} to {config.date_end}")
    print(f"  Table: {config.table_name}")
    
    # Test all engines
    engines_to_test = ['duckdb', 'polars', 'athena']
    engine_results = {}
    
    for engine_name in engines_to_test:
        try:
            result = test_simple_query_with_engine(engine_name, config)
            engine_results[engine_name] = result
            
            # If basic queries work, test formats
            if result.get('success'):
                test_query_formats(engine_name, config)
                
        except Exception as e:
            print(f"❌ {engine_name} engine test failed: {e}")
            engine_results[engine_name] = {'success': False, 'error': str(e)}
    
    # Summary
    print("\n" + "=" * 50)
    print("🎯 MULTI-ENGINE TEST SUMMARY")
    print("=" * 50)
    
    successful_engines = []
    failed_engines = []
    
    for engine_name, result in engine_results.items():
        if result.get('success'):
            successful_engines.append(engine_name)
            print(f"✅ {engine_name.upper()}: SUCCESS")
            
            # Show query results summary
            if 'results' in result:
                for query_name, query_result in result['results'].items():
                    if 'error' not in query_result:
                        rows = query_result.get('rows', 0)
                        exec_time = query_result.get('execution_time', 0)
                        print(f"   - {query_name}: {rows} rows ({exec_time:.3f}s)")
                    else:
                        print(f"   - {query_name}: FAILED")
        else:
            failed_engines.append(engine_name)
            error = result.get('error', 'Unknown error')
            print(f"❌ {engine_name.upper()}: FAILED - {error}")
    
    print(f"\nResults: {len(successful_engines)}/{len(engines_to_test)} engines successful")
    
    if successful_engines:
        print(f"✅ Working engines: {', '.join(successful_engines)}")
    
    if failed_engines:
        print(f"❌ Failed engines: {', '.join(failed_engines)}")
    
    # Overall test result
    success = len(successful_engines) > 0
    if success:
        print("\n🎉 TEST PASSED: At least one engine is working!")
        print("💡 Tip: Different engines may have different performance characteristics")
    else:
        print("\n💥 TEST FAILED: No engines could execute queries")
        print("🔧 Check S3 credentials, bucket access, and data availability")
    
    return success


def test_query_s3():
    """Main test function for backward compatibility."""
    return run_comprehensive_test()


if __name__ == "__main__":
    success = run_comprehensive_test()
    sys.exit(0 if success else 1)