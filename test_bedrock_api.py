#!/usr/bin/env python3
"""
Test script for Bedrock API integration
"""

import requests
import json
import sys

def test_bedrock_generate_query():
    """Test the Bedrock generate-query endpoint."""
    url = "http://localhost:8000/api/v1/finops/bedrock/generate-query"
    
    # Test payload
    payload = {
        "user_query": "Show me the top 10 most expensive services last month",
        "model_config": {
            "model_id": "anthropic.claude-3-5-sonnet-20241022-v2:0",
            "max_tokens": 4096,
            "temperature": 0.1,
            "top_p": 0.9,
            "top_k": 250
        },
        "include_examples": True,
        "target_table": "CUR"
    }
    
    try:
        print("🚀 Testing Bedrock API endpoint...")
        print(f"URL: {url}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(url, json=payload, timeout=30)
        
        print(f"\n📊 Response Status: {response.status_code}")
        
        if response.ok:
            data = response.json()
            print("✅ Success!")
            print(f"Response: {json.dumps(data, indent=2)}")
            
            # Extract and print the generated SQL if available
            if 'structured_query' in data:
                print("\n📝 Generated SQL Query:")
                if isinstance(data['structured_query'], dict) and 'sql_query' in data['structured_query']:
                    print(data['structured_query']['sql_query'])
                elif isinstance(data['structured_query'], dict) and 'sql' in data['structured_query']:
                    print(data['structured_query']['sql'])
                else:
                    print(data['structured_query'])
                    
                print(f"\n🎯 Expected format: The API should return structured_query.sql_query containing just the SQL")
            
            return True
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Make sure the API server is running on localhost:8000")
        print("   Start the server with: python main.py")
        return False
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def test_health_check():
    """Test the health check endpoint."""
    try:
        print("\n🔍 Testing health check...")
        response = requests.get("http://localhost:8000/health", timeout=10)
        
        if response.ok:
            print("✅ Health check passed")
            print(f"Status: {response.json()}")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Server not running")
        return False

def main():
    """Main test function."""
    print("🧪 BEDROCK API INTEGRATION TEST")
    print("=" * 50)
    
    # Test health check first
    if not test_health_check():
        print("\n💡 Start the server first:")
        print("   cd infralyzer && python main.py")
        sys.exit(1)
    
    # Test Bedrock endpoint
    if test_bedrock_generate_query():
        print("\n🎉 All tests passed! The integration is working.")
    else:
        print("\n⚠️  Tests failed. Check the server logs for more details.")
        sys.exit(1)

if __name__ == "__main__":
    main()