#!/usr/bin/env python3
"""
Test 1: Simple Multi-Engine Query Test
=====================================

Simple test: Run "SELECT * FROM CUR LIMIT 10" on all three engines:
- DuckDB (fast analytics)
- Polars (modern DataFrame)  
- AWS Athena (cloud serverless)
"""

import sys
import os
import pytest
# Add parent directory to path to import infralyzer module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infralyzer import FinOpsEngine, DataConfig, DataExportType


@pytest.fixture(params=["duckdb", "polars", "athena"])
def engine_name(request):
    """Test each engine."""
    return request.param


@pytest.fixture
def config():
    """Test configuration."""
    return create_test_config()


def create_test_config():
    """Create test configuration for all engines."""
    return DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',       
        data_export_type=DataExportType.CUR_2_0,               
        table_name='AA',                        
        date_start='2025-01',                    
        date_end='2025-07',
        prefer_local_data=False,  # Force S3 access
        local_data_path="./test_local_data",
        aws_region='us-west-2'  # Fix: Set correct region for Athena
    )

# NOTE: To use custom Athena results bucket (aws-athena-query-results-cid-014498620306-us-west-2):
# 1. Replace FinOpsEngine with direct AthenaEngine creation
# 2. Pass output_bucket parameter to AthenaEngine constructor
# 3. Ensure bucket exists and has proper permissions in same region


def test_simple_query_with_engine(engine_name, config):
    """Test query with engine-appropriate SQL for realistic comparison."""
    try:
        finops_engine = FinOpsEngine(config, engine_name=engine_name)
        engine_instance = finops_engine.engine
        sql = "SELECT * FROM AA LIMIT 10"
        
        # Execute query
        result = engine_instance.query(sql)
        print (result)
        print(f"{engine_name.upper()}: ✅ {len(result)} rows × {len(result.columns)} columns")
        return True
        
    except Exception as e:
        print(f"{engine_name.upper()}: ❌ {str(e)[:100]}...")
        return False


def run_simple_test():
    config = create_test_config()
    engines = ['duckdb', 'polars', 'athena']
    success_count = 0
    
    for engine_name in engines:
        print (engine_name)
        if test_simple_query_with_engine(engine_name, config):
            success_count += 1
    
    # print(f"\n🎯 Result: {success_count}/{len(engines)} engines working")
    return success_count > 0


def test_query_s3():
    """Main test function."""
    return run_simple_test()


if __name__ == "__main__":
    success = run_simple_test()
    sys.exit(0 if success else 1)