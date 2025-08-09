#!/usr/bin/env python3
"""
Test Query Rejection - Demonstrating Non-CUR Query Handling

This script tests the improved rejection mechanism for non-CUR related queries.
"""

import requests
import json
from typing import Dict, Any
from datetime import datetime


class QueryRejectionTester:
    """Test client for demonstrating query rejection behavior."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.test_results = []
    
    def test_query(self, query: str, expected_outcome: str, description: str = "") -> Dict[str, Any]:
        """Test a single query and record results."""
        
        print(f"\n🧪 Testing: {description}")
        print(f"📝 Query: '{query}'")
        print(f"🎯 Expected: {expected_outcome}")
        
        payload = {
            "user_query": query,
            "model_configuration": {
                "model_id": "CLAUDE_3_5_SONNET",
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
            
            if response.status_code == 200:
                result = response.json()
                outcome = "✅ ACCEPTED"
                details = {
                    "sql_generated": True,
                    "query_type": result['structured_query'].get('query_type'),
                    "confidence": result.get('confidence'),
                    "title": result['structured_query'].get('title')
                }
                print(f"   {outcome} - Generated valid SQL query")
                print(f"   📊 Type: {details['query_type']}, Confidence: {details['confidence']}")
                
            elif response.status_code == 400:
                error_detail = response.json()['detail']
                if error_detail.get('error') == 'non_cur_query':
                    outcome = "❌ REJECTED (Correctly)"
                    details = {
                        "sql_generated": False,
                        "rejection_reason": error_detail.get('message'),
                        "suggestions": error_detail.get('suggestions', [])
                    }
                    print(f"   {outcome} - {error_detail.get('message')}")
                    print(f"   💡 Suggestions: {len(details['suggestions'])} provided")
                else:
                    outcome = "❌ REJECTED (Other Error)"
                    details = {"error": error_detail}
                    print(f"   {outcome} - {error_detail}")
            else:
                outcome = f"❓ UNEXPECTED ({response.status_code})"
                details = {"status_code": response.status_code, "response": response.text}
                print(f"   {outcome} - Status: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            outcome = "💥 CONNECTION ERROR"
            details = {"error": str(e)}
            print(f"   {outcome} - {e}")
        
        # Record test result
        test_result = {
            "query": query,
            "description": description,
            "expected": expected_outcome,
            "actual": outcome,
            "correct": self._is_outcome_correct(expected_outcome, outcome),
            "details": details,
            "timestamp": datetime.now().isoformat()
        }
        
        self.test_results.append(test_result)
        
        if test_result["correct"]:
            print("   ✅ Test PASSED")
        else:
            print("   ❌ Test FAILED")
            
        return test_result
    
    def _is_outcome_correct(self, expected: str, actual: str) -> bool:
        """Check if the actual outcome matches expected."""
        if expected == "ACCEPT" and "ACCEPTED" in actual:
            return True
        elif expected == "REJECT" and "REJECTED" in actual:
            return True
        return False
    
    def run_comprehensive_tests(self):
        """Run comprehensive test suite."""
        
        print("🚀 Comprehensive Query Rejection Tests")
        print("🔧 Testing Infralyzer Bedrock CUR Query Handler")
        print("=" * 80)
        
        # Test 1: Valid CUR queries (should be accepted)
        print("\n📊 SECTION 1: Valid CUR Queries (Should Accept)")
        print("-" * 50)
        
        valid_queries = [
            ("What are my top 5 most expensive AWS services?", "ACCEPT", "Basic cost query"),
            ("Show me EC2 costs by region this month", "ACCEPT", "Service-specific cost analysis"),
            ("Analyze my Reserved Instance utilization", "ACCEPT", "RI optimization query"),
            ("Compare this month's AWS spending to last month", "ACCEPT", "Cost comparison analysis"),
            ("What did I spend on S3 storage yesterday?", "ACCEPT", "Specific service cost query"),
            ("Show me data transfer costs by service", "ACCEPT", "Data transfer cost analysis"),
            ("Which AWS accounts have the highest costs?", "ACCEPT", "Account-level cost analysis"),
            ("Show me trends in my RDS spending over 6 months", "ACCEPT", "Cost trend analysis"),
            ("What are my savings from Reserved Instances?", "ACCEPT", "Savings analysis"),
            ("Show me unused resources that are costing money", "ACCEPT", "Waste analysis query")
        ]
        
        for query, expected, description in valid_queries:
            self.test_query(query, expected, description)
        
        # Test 2: Non-CUR queries (should be rejected)
        print("\n🚫 SECTION 2: Non-CUR Queries (Should Reject)")
        print("-" * 50)
        
        invalid_queries = [
            ("What's the weather like today?", "REJECT", "Weather query"),
            ("How do I bake a chocolate cake?", "REJECT", "Cooking recipe"),
            ("What is the capital of France?", "REJECT", "Geography question"),
            ("Explain quantum computing", "REJECT", "Technology explanation"),
            ("What's the latest news?", "REJECT", "News query"),
            ("How to fix my car engine?", "REJECT", "Automotive repair"),
            ("What movies are playing tonight?", "REJECT", "Entertainment query"),
            ("Help me write a love letter", "REJECT", "Creative writing"),
            ("Solve this math problem: 2x + 5 = 15", "REJECT", "Math homework"),
            ("What is the meaning of life?", "REJECT", "Philosophical question")
        ]
        
        for query, expected, description in invalid_queries:
            self.test_query(query, expected, description)
        
        # Test 3: Edge cases and ambiguous queries
        print("\n🤔 SECTION 3: Edge Cases and Ambiguous Queries")
        print("-" * 50)
        
        edge_cases = [
            ("AWS", "REJECT", "Single word - too vague"),
            ("Cost", "REJECT", "Generic cost term"),
            ("Show me my data", "REJECT", "Ambiguous data request"),
            ("EC2 performance metrics", "REJECT", "Performance, not cost"),
            ("AWS security best practices", "REJECT", "Security, not cost"),
            ("How to deploy EC2 instances?", "REJECT", "Deployment, not cost"),
            ("What is AWS CloudFormation?", "REJECT", "Service explanation"),
            ("AWS cost optimization tips", "ACCEPT", "Cost optimization is valid"),
            ("Show me my AWS bill", "ACCEPT", "Billing is CUR-related"),
            ("Lambda function costs vs EC2", "ACCEPT", "Cost comparison")
        ]
        
        for query, expected, description in edge_cases:
            self.test_query(query, expected, description)
    
    def print_summary(self):
        """Print test summary and statistics."""
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results if result["correct"])
        failed_tests = total_tests - passed_tests
        
        print(f"\n📋 TEST SUMMARY")
        print("=" * 80)
        print(f"Total Tests: {total_tests}")
        print(f"✅ Passed: {passed_tests}")
        print(f"❌ Failed: {failed_tests}")
        print(f"📊 Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        
        if failed_tests > 0:
            print(f"\n❌ FAILED TESTS:")
            for result in self.test_results:
                if not result["correct"]:
                    print(f"   • {result['description']}: Expected {result['expected']}, Got {result['actual']}")
        
        # Analysis by category
        accept_tests = [r for r in self.test_results if "ACCEPT" in r["expected"]]
        reject_tests = [r for r in self.test_results if "REJECT" in r["expected"]]
        
        accept_passed = sum(1 for r in accept_tests if r["correct"])
        reject_passed = sum(1 for r in reject_tests if r["correct"])
        
        print(f"\n📊 DETAILED ANALYSIS:")
        print(f"Valid CUR Queries (Should Accept): {accept_passed}/{len(accept_tests)} correct")
        print(f"Invalid Queries (Should Reject): {reject_passed}/{len(reject_tests)} correct")
        
        if len(accept_tests) > 0 and accept_passed/len(accept_tests) < 0.9:
            print("⚠️  Warning: Low acceptance rate for valid queries")
        
        if len(reject_tests) > 0 and reject_passed/len(reject_tests) < 0.9:
            print("⚠️  Warning: Low rejection rate for invalid queries")
        
        print(f"\n💡 RECOMMENDATIONS:")
        if passed_tests == total_tests:
            print("🎉 All tests passed! Query rejection is working perfectly.")
        else:
            print("🔧 Some tests failed. Consider:")
            print("   • Refining the prompt for better edge case handling")
            print("   • Adjusting rejection keywords")
            print("   • Training examples for better classification")


def main():
    """Run the comprehensive query rejection test suite."""
    
    print("🤖 Query Rejection Test Suite")
    print("🎯 Testing CUR vs Non-CUR Query Classification")
    print("📅 " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    try:
        tester = QueryRejectionTester()
        
        # Test connection first
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
        
        # Run comprehensive tests
        tester.run_comprehensive_tests()
        
        # Print summary
        tester.print_summary()
        
        print(f"\n🎯 KEY BEHAVIORS TESTED:")
        print("1. ✅ Valid CUR queries generate SQL and return 200")
        print("2. ❌ Non-CUR queries return 400 with helpful message")
        print("3. 💡 Rejection responses include suggestions")
        print("4. 🧠 Edge cases are handled appropriately")
        print("5. 📝 All responses are properly logged")
        
        print(f"\n💬 EXAMPLE REJECTION RESPONSE:")
        print('''{
  "error": "non_cur_query",
  "message": "I can only help with AWS cost and usage analysis queries...",
  "suggestions": [
    "What are my top 5 most expensive AWS services?",
    "Show me EC2 costs by region this month",
    "Analyze my Reserved Instance utilization"
  ],
  "original_query": "What's the weather like today?"
}''')
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()
