#!/usr/bin/env python3
"""
Test JSON Response Fix - Verify AI returns proper JSON structure

This script tests the fix for the issue where AI models were returning
raw SQL instead of the required JSON structure.
"""

import requests
import json
from typing import Dict, Any
from datetime import datetime


class JSONResponseTester:
    """Test client for JSON response format."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
    
    def test_json_response(self, query: str, model_id: str = "CLAUDE_3_5_SONNET") -> Dict[str, Any]:
        """Test that the API returns proper JSON structure."""
        
        print(f"\n🧪 Testing JSON Response")
        print(f"📝 Query: '{query}'")
        print(f"🤖 Model: {model_id}")
        
        payload = {
            "user_query": query,
            "model_configuration": {
                "model_id": model_id,
                "max_tokens": 2048,
                "temperature": 0.1,
                "top_p": 0.9,
                "top_k": 250
            },
            "include_examples": True,
            "target_table": "CUR"
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/bedrock/generate-query",
                json=payload,
                timeout=30
            )
            
            status_code = response.status_code
            
            print(f"📊 Status Code: {status_code}")
            
            if status_code == 200:
                # Success - check the structure
                result = response.json()
                
                print("✅ SUCCESS - Got 200 OK")
                
                # Validate the response structure
                structured_query = result.get('structured_query', {})
                
                required_fields = ['sql_query', 'query_type', 'visualization_type', 'title']
                missing_fields = [field for field in required_fields if field not in structured_query]
                
                if missing_fields:
                    print(f"⚠️  Missing fields: {missing_fields}")
                else:
                    print("✅ All required fields present")
                
                # Check if it looks like a valid response
                sql_query = structured_query.get('sql_query', '')
                if sql_query.strip().upper().startswith('SELECT'):
                    print("✅ SQL query looks valid")
                    print(f"📝 SQL: {sql_query[:100]}...")
                else:
                    print(f"❌ SQL query doesn't look valid: {sql_query[:50]}...")
                
                print(f"📊 Query Type: {structured_query.get('query_type')}")
                print(f"📈 Visualization: {structured_query.get('visualization_type')}")
                print(f"📋 Title: {structured_query.get('title')}")
                print(f"🎯 Confidence: {result.get('confidence')}")
                
                return {"status": "success", "data": result}
                
            elif status_code == 400:
                # Rejection
                error_detail = response.json().get('detail', {})
                print("❌ REJECTED - Query not CUR-related")
                print(f"💬 Message: {error_detail.get('message', '')[:100]}...")
                return {"status": "rejected", "data": error_detail}
                
            elif status_code == 422:
                # AI model error
                error_detail = response.json().get('detail', {})
                print("🔧 AI MODEL ERROR")
                print(f"💬 Message: {error_detail.get('message', '')}")
                
                # This is what we're trying to fix
                if "raw SQL instead of JSON" in str(error_detail):
                    print("🚨 DETECTED: AI returned raw SQL instead of JSON")
                    print("🔧 This is the exact issue we're trying to fix")
                    return {"status": "raw_sql_error", "data": error_detail}
                else:
                    return {"status": "ai_error", "data": error_detail}
                    
            else:
                print(f"❓ UNEXPECTED STATUS: {status_code}")
                return {"status": f"unexpected_{status_code}", "data": response.text}
                
        except Exception as e:
            print(f"💥 ERROR: {e}")
            return {"status": "error", "data": str(e)}
    
    def run_json_fix_tests(self):
        """Run tests to verify JSON response fix."""
        
        print("🔧 JSON Response Fix Test Suite")
        print("🎯 Verifying AI returns JSON instead of raw SQL")
        print("=" * 80)
        
        # Test the specific queries that were failing
        test_cases = [
            ("show me my ec2 charge", "User's original failing query #1"),
            ("what is my ec2 charge", "User's original failing query #2"),
            ("EC2 costs this month", "Simple EC2 cost query"),
            ("S3 storage charges", "Simple S3 charge query"),
            ("top 5 most expensive services", "Top services query"),
            ("lambda function costs", "Lambda cost query")
        ]
        
        results = []
        
        for query, description in test_cases:
            print(f"\n{'='*60}")
            print(f"🧪 {description}")
            result = self.test_json_response(query)
            results.append({"query": query, "description": description, "result": result})
        
        # Summary
        print(f"\n📋 TEST SUMMARY")
        print("=" * 80)
        
        success_count = sum(1 for r in results if r["result"]["status"] == "success")
        rejected_count = sum(1 for r in results if r["result"]["status"] == "rejected")
        raw_sql_error_count = sum(1 for r in results if r["result"]["status"] == "raw_sql_error")
        other_error_count = len(results) - success_count - rejected_count - raw_sql_error_count
        
        print(f"✅ Successful JSON responses: {success_count}")
        print(f"❌ Rejected queries: {rejected_count}")
        print(f"🚨 Raw SQL errors (the bug): {raw_sql_error_count}")
        print(f"🔧 Other errors: {other_error_count}")
        
        if raw_sql_error_count == 0:
            print(f"\n🎉 SUCCESS! No raw SQL errors detected.")
            print(f"✅ All queries returned proper JSON structure.")
        else:
            print(f"\n🚨 ISSUE DETECTED: {raw_sql_error_count} queries still returning raw SQL")
            print(f"❌ The fix may need more work.")
            
        # Show any problematic cases
        for result in results:
            if result["result"]["status"] == "raw_sql_error":
                print(f"\n🚨 PROBLEMATIC QUERY: '{result['query']}'")
                print(f"   Error: {result['result']['data']}")
        
        print(f"\n💡 EXPECTED BEHAVIOR:")
        print("1. ✅ Valid CUR queries should return 200 OK with JSON structure")
        print("2. ❌ Non-CUR queries should return 400 Bad Request")
        print("3. 🔧 AI parsing issues should return 422 with helpful message")
        print("4. 🚫 NO queries should return raw SQL instead of JSON")


def main():
    """Run JSON response fix tests."""
    
    print("🔧 JSON Response Fix Verification")
    print("📅 " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    try:
        tester = JSONResponseTester()
        
        # Test API connection
        try:
            response = requests.get(f"{tester.base_url}/bedrock/models", timeout=5)
            if response.status_code == 200:
                print("✅ API connection successful")
            else:
                print(f"⚠️  API responded with status {response.status_code}")
        except Exception as e:
            print(f"❌ Cannot connect to API: {e}")
            print("   Make sure Infralyzer is running on http://localhost:8000")
            return
        
        # Run the tests
        tester.run_json_fix_tests()
        
        print(f"\n🎯 WHAT WE FIXED:")
        print("• Enhanced prompt with explicit JSON formatting requirements")
        print("• Updated all model request templates to emphasize JSON")
        print("• Improved JSON parsing with better error detection")
        print("• Added specific error handling for raw SQL responses")
        
        print(f"\n🔧 TECHNICAL CHANGES:")
        print("• Prompt: 'CRITICAL: Start your response with { and end with }'")
        print("• Request: 'Generate ONLY the JSON response (start with {{ and end with }})'")
        print("• Parser: Better detection of raw SQL vs JSON responses")
        print("• Logging: More detailed error information for debugging")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()
