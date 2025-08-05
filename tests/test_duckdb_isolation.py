#!/usr/bin/env python3
"""
DuckDB Isolation Test - Isolate the exact DuckDB issue
=====================================================

This test isolates the DuckDB issue step by step to find the root cause.
"""

import sys
import os
import duckdb
# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infralyzer import FinOpsEngine, DataConfig, DataExportType


def test_raw_duckdb():
    """Test raw DuckDB without any Infralyzer code."""
    print("🔧 Testing Raw DuckDB")
    print("-" * 40)
    
    try:
        # Test 1: Basic DuckDB
        conn = duckdb.connect(":memory:")
        result = conn.execute("SELECT 42 as answer").fetchone()
        print(f"✅ Basic DuckDB: {result}")
        
        # Test 2: DuckDB with S3 extension
        conn.execute("LOAD httpfs")
        print("✅ S3 extension loaded")
        
        # Test 3: Simple query after S3 extension
        result = conn.execute("SELECT 'hello' as greeting").fetchone()
        print(f"✅ Post-S3 query: {result}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Raw DuckDB failed: {e}")
        return False


def test_duckdb_with_s3_config():
    """Test DuckDB with S3 configuration applied."""
    print("\n🔧 Testing DuckDB with S3 Configuration")
    print("-" * 40)
    
    try:
        # Create config
        config = DataConfig(
            s3_bucket='billing-data-exports-cur',          
            s3_data_prefix='cur2/cur2/data',       
            data_export_type=DataExportType.CUR_2_0,               
            table_name='CUR',                        
            date_start='2025-01',                    
            date_end='2025-07',
            prefer_local_data=False
        )
        
        # Get storage options
        from infralyzer.auth import get_storage_options
        storage_options = get_storage_options(config)
        print(f"Storage options keys: {list(storage_options.keys())}")
        
        # Create DuckDB connection
        conn = duckdb.connect(":memory:")
        conn.execute("LOAD httpfs")
        
        # Test before applying S3 config
        result = conn.execute("SELECT 'before_s3_config' as status").fetchone()
        print(f"✅ Before S3 config: {result}")
        
        # Apply S3 configuration step by step
        print("Applying S3 configuration...")
        
        if 'aws_region' in storage_options:
            region_cmd = f"SET s3_region='{storage_options['aws_region']}'"
            print(f"   Setting region: {region_cmd}")
            conn.execute(region_cmd)
        
        if 'aws_access_key_id' in storage_options:
            access_key_cmd = f"SET s3_access_key_id='{storage_options['aws_access_key_id'][:10]}...'"
            print(f"   Setting access key: {access_key_cmd}")
            conn.execute(f"SET s3_access_key_id='{storage_options['aws_access_key_id']}'")
        
        if 'aws_secret_access_key' in storage_options:
            print("   Setting secret key: SET s3_secret_access_key='***'")
            conn.execute(f"SET s3_secret_access_key='{storage_options['aws_secret_access_key']}'")
        
        if 'aws_session_token' in storage_options:
            print("   Setting session token: SET s3_session_token='***'")
            conn.execute(f"SET s3_session_token='{storage_options['aws_session_token']}'")
        
        # Test after applying S3 config
        print("Testing query after S3 configuration...")
        result = conn.execute("SELECT 'after_s3_config' as status").fetchone()
        print(f"✅ After S3 config: {result}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ DuckDB with S3 config failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_s3_file_access():
    """Test direct S3 file access with DuckDB."""
    print("\n🔧 Testing Direct S3 File Access")
    print("-" * 40)
    
    try:
        # Create config and get S3 files
        config = DataConfig(
            s3_bucket='billing-data-exports-cur',          
            s3_data_prefix='cur2/cur2/data',       
            data_export_type=DataExportType.CUR_2_0,               
            table_name='CUR',                        
            date_start='2025-01',                    
            date_end='2025-07',
            prefer_local_data=False
        )
        
        # Get S3 files
        from infralyzer.data.s3_data_manager import S3DataManager
        s3_manager = S3DataManager(config)
        s3_files = s3_manager.discover_data_files()
        
        if not s3_files:
            print("❌ No S3 files found")
            return False
        
        print(f"Found {len(s3_files)} S3 files")
        first_file = s3_files[0]
        print(f"Testing with: {first_file}")
        
        # Create DuckDB connection with S3 config
        from infralyzer.auth import get_storage_options
        storage_options = get_storage_options(config)
        
        conn = duckdb.connect(":memory:")
        conn.execute("LOAD httpfs")
        
        # Apply S3 configuration
        if 'aws_region' in storage_options:
            conn.execute(f"SET s3_region='{storage_options['aws_region']}'")
        if 'aws_access_key_id' in storage_options:
            conn.execute(f"SET s3_access_key_id='{storage_options['aws_access_key_id']}'")
        if 'aws_secret_access_key' in storage_options:
            conn.execute(f"SET s3_secret_access_key='{storage_options['aws_secret_access_key']}'")
        if 'aws_session_token' in storage_options:
            conn.execute(f"SET s3_session_token='{storage_options['aws_session_token']}'")
        
        # Test very simple S3 query
        print("Testing direct S3 file query...")
        query = f"SELECT COUNT(*) FROM read_parquet('{first_file}')"
        print(f"Query: {query}")
        
        result = conn.execute(query).fetchone()
        print(f"✅ S3 query result: {result}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ S3 file access failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_minimal_infralyzer():
    """Test minimal Infralyzer usage."""
    print("\n🔧 Testing Minimal Infralyzer Usage")
    print("-" * 40)
    
    try:
        config = DataConfig(
            s3_bucket='billing-data-exports-cur',          
            s3_data_prefix='cur2/cur2/data',       
            data_export_type=DataExportType.CUR_2_0,               
            table_name='CUR',                        
            date_start='2025-01',                    
            date_end='2025-07',
            prefer_local_data=False
        )
        
        engine = FinOpsEngine(config, engine_name="duckdb")
        print(f"✅ Engine created: {engine.engine.engine_name}")
        
        # Try to access the internal DuckDB connection
        duckdb_engine = engine.engine
        
        # Test simple query bypassing the normal query method
        print("Testing direct DuckDB connection access...")
        conn = duckdb_engine._get_duckdb_connection()
        
        # Test simple query on clean connection
        result = conn.execute("SELECT 'test' as value").fetchone()
        print(f"✅ Direct connection query: {result}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Minimal Infralyzer failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("🚀 DUCKDB ISOLATION TEST")
    print("Isolating the exact cause of the DuckDB issue")
    print("=" * 50)
    
    tests = [
        ("Raw DuckDB", test_raw_duckdb),
        ("DuckDB with S3 Config", test_duckdb_with_s3_config),
        ("S3 File Access", test_s3_file_access),
        ("Minimal Infralyzer", test_minimal_infralyzer)
    ]
    
    passed = 0
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
                break  # Stop at first failure to see where it breaks
        except Exception as e:
            print(f"❌ {test_name}: EXCEPTION - {e}")
            break
    
    print("\n" + "=" * 50)
    print(f"🎯 ISOLATION RESULTS: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("✅ All isolation tests passed - the issue might be in the query execution flow")
    else:
        print(f"❌ Issue isolated at test: {tests[passed][0]}")
    
    sys.exit(0 if passed == len(tests) else 1)