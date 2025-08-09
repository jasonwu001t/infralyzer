#!/usr/bin/env python3
"""
Test Improved Query Error Handling - User-Friendly Error Messages

This script demonstrates the enhanced error handling in the /query endpoint 
that provides helpful guidance instead of raw database errors.
"""

import requests
import json
from typing import Dict, Any
from datetime import datetime


class QueryErrorTester:
    """Test client for improved query error handling."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.test_results = []
    
    def test_query_with_error(self, query: str, description: str, expected_error_type: str = None) -> Dict[str, Any]:
        """Test a query that should produce an error and verify user-friendly response."""
        
        print(f"\n🧪 Testing: {description}")
        print(f"📝 Query: {query}")
        
        payload = {
            "query": query,
            "engine": "duckdb",
            "output_format": "json"
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/api/v1/finops/query",
                json=payload,
                timeout=30
            )
            
            status_code = response.status_code
            
            if status_code == 400:  # Expected: Bad Request with helpful guidance
                error_detail = response.json().get('detail', {})
                
                print("✅ SUCCESS - Got user-friendly error response")
                print(f"🎯 Error Type: {error_detail.get('error_type', 'Unknown')}")
                print(f"💬 User-Friendly Message:")
                print(f"   {error_detail.get('error', 'No message provided')}")
                
                suggestions = error_detail.get('suggestions', [])
                if suggestions:
                    print(f"\n💡 Helpful Suggestions ({len(suggestions)} items):")
                    for i, suggestion in enumerate(suggestions, 1):
                        print(f"   {i}. {suggestion}")
                
                troubleshooting = error_detail.get('metadata', {}).get('troubleshooting_steps', [])
                if troubleshooting:
                    print(f"\n🔧 Troubleshooting Steps ({len(troubleshooting)} steps):")
                    for step in troubleshooting:
                        print(f"   {step}")
                
                metadata = error_detail.get('metadata', {})
                print(f"\n📊 Metadata:")
                print(f"   🚀 Engine: {metadata.get('engine_used', 'Unknown')}")
                print(f"   ⏱️ Execution Time: {metadata.get('execution_time_ms', 0):.1f}ms")
                print(f"   📋 Query Type: {metadata.get('query_type', 'Unknown')}")
                
                result = {
                    "status": "user_friendly_error",
                    "error_type": error_detail.get('error_type'),
                    "user_message": error_detail.get('error'),
                    "suggestions_count": len(suggestions),
                    "troubleshooting_steps_count": len(troubleshooting),
                    "has_metadata": bool(metadata)
                }
                
                if expected_error_type and error_detail.get('error_type') == expected_error_type:
                    print(f"✅ Correct error type detected: {expected_error_type}")
                elif expected_error_type:
                    print(f"⚠️ Expected {expected_error_type}, got {error_detail.get('error_type')}")
                
            elif status_code == 200:
                print("⚠️ UNEXPECTED - Query succeeded when it should have failed")
                result = {"status": "unexpected_success"}
                
            elif status_code == 500:
                print("❌ REGRESSION - Still getting 500 Internal Server Error")
                print(f"Response: {response.text}")
                result = {"status": "regression_500_error"}
                
            else:
                print(f"❓ UNEXPECTED STATUS: {status_code}")
                print(f"Response: {response.text}")
                result = {"status": f"unexpected_{status_code}"}
            
            # Store test result
            self.test_results.append({
                "description": description,
                "query": query,
                "expected_error_type": expected_error_type,
                "result": result,
                "timestamp": datetime.now().isoformat()
            })
            
            return result
            
        except Exception as e:
            print(f"💥 Exception: {e}")
            return {"status": "exception", "error": str(e)}
    
    def run_comprehensive_error_tests(self):
        """Run comprehensive error handling tests."""
        
        print("🧪 Improved Query Error Handling Test Suite")
        print("🎯 Testing user-friendly error messages and guidance")
        print("=" * 80)
        
        # Test 1: Column Not Found Error
        print(f"\n{'='*80}")
        print("📊 COLUMN NOT FOUND ERRORS")
        print("=" * 80)
        
        self.test_query_with_error(
            query="SELECT product_region, SUM(line_item_unblended_cost) FROM CUR GROUP BY product_region",
            description="Invalid column name (should be product_region_code)",
            expected_error_type="COLUMN_NOT_FOUND"
        )
        
        self.test_query_with_error(
            query="SELECT invalid_column, line_item_cost FROM CUR LIMIT 10",
            description="Completely invalid column name",
            expected_error_type="COLUMN_NOT_FOUND"
        )
        
        self.test_query_with_error(
            query="SELECT lineitem_cost FROM CUR LIMIT 5",
            description="Wrong column format (missing underscore)",
            expected_error_type="COLUMN_NOT_FOUND"
        )
        
        # Test 2: Table Not Found Error
        print(f"\n{'='*80}")
        print("🗂️ TABLE NOT FOUND ERRORS")
        print("=" * 80)
        
        self.test_query_with_error(
            query="SELECT * FROM BILLING_DATA LIMIT 10",
            description="Wrong table name (should be CUR)",
            expected_error_type="TABLE_NOT_FOUND"
        )
        
        self.test_query_with_error(
            query="SELECT * FROM aws_costs WHERE cost > 100",
            description="Non-existent table",
            expected_error_type="TABLE_NOT_FOUND"
        )
        
        # Test 3: SQL Syntax Errors
        print(f"\n{'='*80}")
        print("📝 SQL SYNTAX ERRORS")
        print("=" * 80)
        
        self.test_query_with_error(
            query="SELECT line_item_unblended_cost FROM CUR WHERE",
            description="Incomplete WHERE clause",
            expected_error_type="SQL_SYNTAX_ERROR"
        )
        
        self.test_query_with_error(
            query="SELECT line_item_unblended_cost, FROM CUR",
            description="Extra comma in SELECT",
            expected_error_type="SQL_SYNTAX_ERROR"
        )
        
        self.test_query_with_error(
            query="SELECT * FROM CUR GROUP BY",
            description="Incomplete GROUP BY clause",
            expected_error_type="SQL_SYNTAX_ERROR"
        )
        
        # Test 4: Complex Query Errors
        print(f"\n{'='*80}")
        print("🔧 COMPLEX QUERY ERRORS")
        print("=" * 80)
        
        self.test_query_with_error(
            query="""
            SELECT 
                wrong_column,
                SUM(line_item_unblended_cost) as total_cost
            FROM CUR 
            WHERE line_item_usage_start_date >= '2024-01-01'
            GROUP BY wrong_column
            ORDER BY total_cost DESC
            """,
            description="Complex query with wrong column name",
            expected_error_type="COLUMN_NOT_FOUND"
        )
        
        self.test_query_with_error(
            query="SELECT product_servicecode, AVG(invalid_function(line_item_cost)) FROM CUR GROUP BY 1",
            description="Invalid function usage",
            expected_error_type="EXECUTION_ERROR"
        )
    
    def run_regression_tests(self):
        """Test that valid queries still work correctly."""
        
        print(f"\n{'='*80}")
        print("✅ REGRESSION TESTS - Valid Queries Should Still Work")
        print("=" * 80)
        
        valid_queries = [
            {
                "query": "SELECT COUNT(*) as total_records FROM CUR",
                "description": "Simple count query"
            },
            {
                "query": "SELECT * FROM CUR LIMIT 1",
                "description": "Simple select with limit"
            },
            {
                "query": "SELECT product_servicecode, SUM(line_item_unblended_cost) as cost FROM CUR GROUP BY product_servicecode LIMIT 5",
                "description": "Aggregation query with valid columns"
            }
        ]
        
        for test_case in valid_queries:
            print(f"\n✅ Testing valid query: {test_case['description']}")
            print(f"📝 Query: {test_case['query']}")
            
            payload = {
                "query": test_case['query'],
                "engine": "duckdb",
                "output_format": "json"
            }
            
            try:
                response = requests.post(
                    f"{self.base_url}/api/v1/finops/query",
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    print(f"✅ SUCCESS - Got {result.get('row_count', 0)} rows in {result.get('execution_time_ms', 0):.1f}ms")
                else:
                    print(f"❌ FAILED - Status: {response.status_code}")
                    print(f"Response: {response.text}")
                    
            except Exception as e:
                print(f"💥 Exception: {e}")
    
    def demonstrate_error_improvements(self):
        """Demonstrate the before/after comparison conceptually."""
        
        print(f"\n{'='*80}")
        print("🔄 BEFORE vs AFTER - Error Handling Improvements")
        print("=" * 80)
        
        example_error = {
            "query": "SELECT product_region FROM CUR",
            "old_behavior": {
                "status_code": 500,
                "response": "DuckDB query error: Binder Error: Referenced column \"product_region\" not found in FROM clause! Candidate bindings: \"product_region_code\", \"product_operation\", \"product\"..."
            },
            "new_behavior": {
                "status_code": 400,
                "response": {
                    "error": "Column 'product_region' does not exist in the CUR table. Please check your query and verify the column name.",
                    "error_type": "COLUMN_NOT_FOUND",
                    "suggestions": [
                        "📝 Double-check the column name spelling and case sensitivity",
                        "🔍 Run 'SELECT * FROM CUR LIMIT 1' to see all available columns",
                        "💡 Did you mean one of these columns? product_region_code, product_operation, product",
                        "🏗️ Check if you're using the correct CUR 2.0 column names"
                    ],
                    "troubleshooting_steps": [
                        "1. Run 'SELECT * FROM CUR LIMIT 1' to see available columns",
                        "2. Check CUR 2.0 schema documentation for correct column names",
                        "3. Verify you're not using legacy CUR 1.0 column names"
                    ]
                }
            }
        }
        
        print("❌ BEFORE (Raw Database Error):")
        print(f"   Status: {example_error['old_behavior']['status_code']} Internal Server Error")
        print(f"   Message: {example_error['old_behavior']['response'][:100]}...")
        
        print("\n✅ AFTER (User-Friendly Guidance):")
        print(f"   Status: {example_error['new_behavior']['status_code']} Bad Request")
        print(f"   Message: {example_error['new_behavior']['response']['error']}")
        print(f"   Suggestions: {len(example_error['new_behavior']['response']['suggestions'])} helpful tips")
        print(f"   Troubleshooting: {len(example_error['new_behavior']['response']['troubleshooting_steps'])} guided steps")
        
        print(f"\n🎯 KEY IMPROVEMENTS:")
        print("• Changed from 500 Internal Server Error to 400 Bad Request")
        print("• User-friendly error messages instead of raw database errors")
        print("• Specific suggestions based on error type")
        print("• Step-by-step troubleshooting guidance")
        print("• Column name suggestions when available")
        print("• Links to helpful resources and endpoints")
    
    def print_test_summary(self):
        """Print summary of all test results."""
        
        if not self.test_results:
            print("\n📋 No tests recorded")
            return
        
        print(f"\n📋 TEST SUMMARY")
        print("=" * 80)
        print(f"Total tests run: {len(self.test_results)}")
        
        # Count results by status
        status_counts = {}
        for test in self.test_results:
            status = test['result']['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print(f"\nResults breakdown:")
        for status, count in status_counts.items():
            if status == "user_friendly_error":
                print(f"✅ User-friendly errors: {count}")
            elif status == "unexpected_success":
                print(f"⚠️ Unexpected successes: {count}")
            elif status == "regression_500_error":
                print(f"❌ 500 error regressions: {count}")
            else:
                print(f"❓ {status}: {count}")
        
        # Error type distribution
        error_types = {}
        for test in self.test_results:
            if test['result']['status'] == 'user_friendly_error':
                error_type = test['result']['error_type']
                error_types[error_type] = error_types.get(error_type, 0) + 1
        
        if error_types:
            print(f"\nError types detected:")
            for error_type, count in error_types.items():
                print(f"   {error_type}: {count} occurrences")
        
        # Quality metrics
        user_friendly_tests = [t for t in self.test_results if t['result']['status'] == 'user_friendly_error']
        if user_friendly_tests:
            avg_suggestions = sum(t['result']['suggestions_count'] for t in user_friendly_tests) / len(user_friendly_tests)
            avg_steps = sum(t['result']['troubleshooting_steps_count'] for t in user_friendly_tests) / len(user_friendly_tests)
            
            print(f"\nQuality metrics:")
            print(f"   Average suggestions per error: {avg_suggestions:.1f}")
            print(f"   Average troubleshooting steps: {avg_steps:.1f}")
            print(f"   All errors include metadata: {all(t['result']['has_metadata'] for t in user_friendly_tests)}")


def main():
    """Run comprehensive error handling improvement tests."""
    
    print("🧪 Improved Query Error Handling Test Suite")
    print("🎯 User-Friendly Error Messages & Guidance")
    print("📅 " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    try:
        tester = QueryErrorTester()
        
        # Test API connection
        try:
            response = requests.get(f"{tester.base_url}/api/v1/finops/query", timeout=5)
            if response.status_code in [405, 422]:  # Method not allowed or validation error is expected for GET
                print("✅ API connection successful")
            else:
                print(f"⚠️ API responded with status {response.status_code}")
        except Exception as e:
            print(f"❌ Cannot connect to API: {e}")
            print("   Make sure Infralyzer is running on http://localhost:8000")
            return
        
        # Run tests
        print("\n🎯 TEST OPTIONS:")
        print("1. Comprehensive error handling tests")
        print("2. Regression tests (valid queries)")
        print("3. Before/after demonstration")
        print("4. All of the above")
        
        choice = input("\nSelect option (1-4) or press Enter for all: ").strip()
        
        if choice in ['1', '4', '']:
            tester.run_comprehensive_error_tests()
        
        if choice in ['2', '4', '']:
            tester.run_regression_tests()
        
        if choice in ['3', '4', '']:
            tester.demonstrate_error_improvements()
        
        # Print summary
        tester.print_test_summary()
        
        print(f"\n🎉 Error Handling Improvement Testing Complete!")
        
        print(f"\n💡 KEY IMPROVEMENTS DEMONSTRATED:")
        print("• 🔄 Changed 500 Internal Server Error → 400 Bad Request")
        print("• 💬 Raw database errors → User-friendly messages")
        print("• 💡 Added specific suggestions based on error type")
        print("• 🔧 Included step-by-step troubleshooting guidance")
        print("• 🎯 Smart column name suggestions from error context")
        print("• 📚 References to helpful resources and endpoints")
        
        print(f"\n🔧 EXAMPLE USER EXPERIENCE:")
        print("Instead of: 'DuckDB query error: Binder Error: Referenced column...'")
        print("Users now get: 'Column does not exist. Did you mean product_region_code?'")
        print("Plus: Specific suggestions and troubleshooting steps!")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Testing interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()
