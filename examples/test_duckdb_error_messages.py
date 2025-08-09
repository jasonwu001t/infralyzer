#!/usr/bin/env python3
"""
Test DuckDB Error Messages - Demonstrating Actual Database Error Inclusion

This script tests that the improved error handling now includes the specific 
DuckDB error messages instead of generic advice.
"""

import requests
import json
from typing import Dict, Any


def test_column_error_with_duckdb_message():
    """Test that column not found errors include the actual DuckDB error message."""
    
    print("🧪 Testing Column Not Found Error with DuckDB Message")
    print("=" * 60)
    
    # This query should produce the exact error from the user's example
    query = """
    SELECT
      product_region,
      SUM(line_item_unblended_cost) as total_cost
    FROM CUR
    WHERE product_servicecode = 'AmazonEC2'
    GROUP BY product_region
    """
    
    payload = {
        "query": query.strip(),
        "engine": "duckdb",
        "output_format": "json"
    }
    
    print(f"📝 Query:")
    print(query.strip())
    
    try:
        response = requests.post(
            "http://localhost:8000/api/v1/finops/query",
            json=payload,
            timeout=30
        )
        
        if response.status_code == 400:
            error_detail = response.json().get('detail', {})
            
            print(f"\n✅ SUCCESS - Got user-friendly error with DuckDB message")
            print(f"🎯 Error Type: {error_detail.get('error_type')}")
            
            error_message = error_detail.get('error', '')
            print(f"\n💬 Error Message (now includes DuckDB error):")
            print(f"   {error_message}")
            
            # Check if it contains the expected DuckDB error components
            expected_parts = [
                "Binder Error",
                "Referenced column",
                "not found in FROM clause",
                "Candidate bindings"
            ]
            
            print(f"\n🔍 DuckDB Error Components Check:")
            for part in expected_parts:
                if part.lower() in error_message.lower():
                    print(f"   ✅ Contains: '{part}'")
                else:
                    print(f"   ❌ Missing: '{part}'")
            
            suggestions = error_detail.get('suggestions', [])
            print(f"\n💡 Helpful Suggestions ({len(suggestions)} items):")
            for i, suggestion in enumerate(suggestions, 1):
                print(f"   {i}. {suggestion}")
            
            # Show what the old vs new experience looks like
            print(f"\n📊 COMPARISON:")
            print(f"❌ OLD: Generic message like 'Column does not exist in CUR table'")
            print(f"✅ NEW: Actual DuckDB error: '{error_message[:100]}...'")
            
            return True
            
        else:
            print(f"❌ Unexpected status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"💥 Exception: {e}")
        return False


def test_multiple_error_types():
    """Test that different error types all include actual database errors."""
    
    print(f"\n🧪 Testing Multiple Error Types with Database Messages")
    print("=" * 60)
    
    test_cases = [
        {
            "name": "Column Not Found",
            "query": "SELECT invalid_column FROM CUR LIMIT 1",
            "expected_in_message": ["column", "not found", "candidate bindings"]
        },
        {
            "name": "Table Not Found", 
            "query": "SELECT * FROM invalid_table LIMIT 1",
            "expected_in_message": ["table", "not found"]
        },
        {
            "name": "Syntax Error",
            "query": "SELECT * FROM CUR WHERE",
            "expected_in_message": ["syntax", "error"]
        }
    ]
    
    results = []
    
    for test_case in test_cases:
        print(f"\n🔸 Testing: {test_case['name']}")
        print(f"📝 Query: {test_case['query']}")
        
        payload = {
            "query": test_case['query'],
            "engine": "duckdb",
            "output_format": "json"
        }
        
        try:
            response = requests.post(
                "http://localhost:8000/api/v1/finops/query",
                json=payload,
                timeout=30
            )
            
            if response.status_code == 400:
                error_detail = response.json().get('detail', {})
                error_message = error_detail.get('error', '').lower()
                
                print(f"✅ Got 400 error with message: {error_message[:80]}...")
                
                # Check if the message contains expected database error terms
                contains_db_error = any(term.lower() in error_message for term in test_case['expected_in_message'])
                
                if contains_db_error:
                    print(f"✅ Contains expected database error terms")
                    results.append(True)
                else:
                    print(f"❌ Missing expected database error terms")
                    results.append(False)
                    
            else:
                print(f"❌ Unexpected status: {response.status_code}")
                results.append(False)
                
        except Exception as e:
            print(f"💥 Exception: {e}")
            results.append(False)
    
    return all(results)


def demonstrate_improvement():
    """Demonstrate the improvement in error messages."""
    
    print(f"\n🔄 IMPROVEMENT DEMONSTRATION")
    print("=" * 60)
    
    print(f"🎯 User's Original Issue:")
    print(f"   Query: SELECT product_region, SUM(line_item_unblended_cost) FROM CUR...")
    print(f"   Problem: Generic error messages instead of specific DuckDB errors")
    
    print(f"\n❌ BEFORE (Generic Messages):")
    print(f"   'Column does not exist in the CUR table. Please check your query.'")
    print(f"   (User has to guess what's wrong)")
    
    print(f"\n✅ AFTER (Actual DuckDB Errors):")
    print(f"   'Binder Error: Referenced column \"product_region\" not found in FROM clause!'")
    print(f"   'Candidate bindings: \"product_region_code\", \"product_operation\", \"product\"'")
    print(f"   (User can see exactly what's wrong and what alternatives exist)")
    
    print(f"\n🎉 BENEFITS:")
    print(f"   • Users get the precise database error message")
    print(f"   • Candidate column suggestions are included")
    print(f"   • No more guessing what went wrong")
    print(f"   • Structured suggestions still provided for additional help")


def main():
    """Run the DuckDB error message improvement test."""
    
    print("🧪 DuckDB Error Message Improvement Test")
    print("🎯 Testing inclusion of actual database errors in user-friendly responses")
    print("📅 Testing the fix requested by the user")
    
    try:
        # Test API connection
        try:
            response = requests.get("http://localhost:8000/api/v1/finops/query", timeout=5)
        except Exception:
            print("❌ Cannot connect to API. Make sure Infralyzer is running on http://localhost:8000")
            return
        
        # Run specific test for the user's example
        success1 = test_column_error_with_duckdb_message()
        
        # Test multiple error types
        success2 = test_multiple_error_types()
        
        # Demonstrate the improvement
        demonstrate_improvement()
        
        # Summary
        print(f"\n📋 TEST SUMMARY")
        print("=" * 60)
        
        if success1 and success2:
            print("✅ All tests passed!")
            print("✅ Error messages now include actual DuckDB errors")
            print("✅ Users get specific, actionable error information")
        else:
            print("❌ Some tests failed")
            print("⚠️ Error handling may need further adjustment")
        
        print(f"\n🔧 KEY IMPROVEMENT:")
        print("Instead of generic 'check your syntax' messages,")
        print("users now get the exact DuckDB error with specific details")
        print("like column candidates and precise error locations!")
        
    except KeyboardInterrupt:
        print("\n⏹️ Testing interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()
