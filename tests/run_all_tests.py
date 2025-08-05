"""
Run All Tests - Execute all 12 FinOps tests in sequence
=====================================================

This script runs all tests in the correct order and provides a summary.
Tests 1-3 and 5-12 will run automatically, Test 4 (FastAPI) requires manual termination.
"""

import sys
import os
import time
import importlib.util

def run_test_module(test_file, test_function):
    """Import and run a test module"""
    try:
        # Import the module
        spec = importlib.util.spec_from_file_location("test_module", test_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Run the test function
        if hasattr(module, test_function):
            result = getattr(module, test_function)()
            return result
        else:
            print(f"Function {test_function} not found in {test_file}")
            return False
            
    except Exception as e:
        print(f"Error running {test_file}: {str(e)}")
        return False

def main():
    """Run all tests in sequence"""
    
    print("FinOps Test Suite - Running All Tests")
    print("Running Tests 1-3, 5-14 automatically.")
    print("Test 4 (FastAPI) starts a server - stop manually.")
    
    # Test configurations
    tests = [
        {
            "file": "test_1_query_s3.py",
            "function": "test_query_s3",
            "name": "Test 1: Query S3 Parquet Files",
            "auto": True
        },
        {
            "file": "test_2_download_local.py", 
            "function": "test_download_local",
            "name": "Test 2: Download Data Locally",
            "auto": True
        },
        {
            "file": "test_3_query_local.py",
            "function": "test_query_local", 
            "name": "Test 3: Query Local Parquet Files",
            "auto": True
        },
        {
            "file": "test_4_fastapi_endpoints.py",
            "function": "test_fastapi_endpoints",
            "name": "Test 4: FastAPI Endpoints",
            "auto": False
        },
        {
            "file": "test_5_sql_views.py",
            "function": "test_sql_views",
            "name": "Test 5: SQL Views with Dependencies",
            "auto": True
        },
        {
            "file": "test_6_mcp_server.py",
            "function": "test_mcp_server",
            "name": "Test 6: MCP Server Integration",
            "auto": True
        },
        {
            "file": "test_7_optimization.py",
            "function": "test_optimization_analytics",
            "name": "Test 7: Optimization Analytics",
            "auto": True
        },
        {
            "file": "test_8_ai_recommendations.py",
            "function": "test_ai_recommendations",
            "name": "Test 8: AI Recommendations",
            "auto": True
        },
        {
            "file": "test_9_data_export.py",
            "function": "test_data_export",
            "name": "Test 9: Data Export & Reporting",
            "auto": True
        },
        {
            "file": "test_10_data_partitioner.py",
            "function": "test_data_partitioner",
            "name": "Test 10: Data Partitioner",
            "auto": True
        },
        {
            "file": "test_11_utilities.py",
            "function": "test_utilities_formatters",
            "name": "Test 11: Utilities & Formatters",
            "auto": True
        },
        {
            "file": "test_12_kpi_comprehensive.py",
            "function": "main",
            "name": "Test 12: Comprehensive KPI Dashboard",
            "auto": True
        },
        {
            "file": "test_13_kpi_api_endpoint.py",
            "function": "main",
            "name": "Test 13: KPI API Endpoint",
            "auto": True
        },
        {
            "file": "test_14_sql_query_endpoint.py",
            "function": "main",
            "name": "Test 14: SQL Query API Endpoint",
            "auto": True
        }
    ]
    
    results = {}
    
    # Run automatic tests (all except Test 4 which needs manual termination)
    for test in tests:
        if test["auto"]:
            print(f"\nRunning {test['name']}")
            print("-" * 40)
            
            test_file = os.path.join(os.path.dirname(__file__), test["file"])
            if not os.path.exists(test_file):
                print(f"Test file not found: {test_file}")
                results[test["name"]] = False
                continue
            
            # Run the test
            start_time = time.time()
            result = run_test_module(test_file, test["function"])
            end_time = time.time()
            
            results[test["name"]] = result
            duration = end_time - start_time
            
            status = "PASSED" if result else "FAILED"
            print(f"\n{status} - {test['name']} (completed in {duration:.1f}s)")
            
            # Wait a bit between tests
            if result:
                time.sleep(2)
            else:
                print("Test failed - stopping execution")
                break
    
    # Print summary
    print(f"\nTEST SUMMARY")
    print("=" * 60)
    
    passed_tests = sum(1 for result in results.values() if result)
    total_tests = len([t for t in tests if t["auto"]])
    
    for test_name, result in results.items():
        status = "PASSED" if result else "FAILED"
        print(f"{status} - {test_name}")
    
    print(f"\nOverall: {passed_tests}/{total_tests} tests passed")
    
    # Handle FastAPI test (Test 4)
    if passed_tests == total_tests:
        print(f"\nReady for Test 4: FastAPI Endpoints")
        print("-" * 40)
        
        response = input("Do you want to start the FastAPI server? (y/n): ").lower()
        if response in ['y', 'yes']:
            # Find the FastAPI test
            fastapi_test = next(t for t in tests if t["file"] == "test_4_fastapi_endpoints.py")
            test_file = os.path.join(os.path.dirname(__file__), fastapi_test["file"])
            
            print(f"\nStarting {fastapi_test['name']}")
            print("Press Ctrl+C to stop the server when done testing")
            print("-" * 40)
            
            # Run FastAPI test
            run_test_module(test_file, fastapi_test["function"])
            
        else:
            print("Skipping FastAPI test. You can run it manually:")
            print("python test_4_fastapi_endpoints.py")
    
    else:
        print(f"\nCannot run Test 4 because previous tests failed")
        print("Please fix the failing tests first.")
    
    print(f"\nTest suite completed!")

if __name__ == "__main__":
    main()