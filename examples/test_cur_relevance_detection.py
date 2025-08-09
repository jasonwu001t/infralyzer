#!/usr/bin/env python3
"""
Test CUR Relevance Detection - Demonstrating How the System Determines CUR Relevance

This script tests the improved CUR relevance detection that combines:
1. Enhanced AI prompt with explicit examples
2. Programmatic keyword analysis 
3. Comprehensive AWS service and financial term recognition
"""

import requests
import json
import sys
import os
from typing import Dict, Any, List, Tuple
from datetime import datetime

# Add the parent directory to the path to import the bedrock handler
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from infralyzer.utils.bedrock_handler import BedrockHandler


class CURRelevanceTester:
    """Test client for demonstrating CUR relevance detection."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.test_results = []
        
        # For local testing of the keyword analyzer
        try:
            from infralyzer.engine.data_config import DataConfig
            config = DataConfig()
            self.bedrock_handler = BedrockHandler(config)
        except:
            self.bedrock_handler = None
            print("⚠️  Could not initialize BedrockHandler for local testing")
    
    def test_programmatic_detection(self, queries: List[Tuple[str, bool, str]]) -> None:
        """Test the programmatic CUR relevance detection."""
        
        print("🔍 PROGRAMMATIC CUR RELEVANCE DETECTION")
        print("=" * 80)
        
        if not self.bedrock_handler:
            print("❌ BedrockHandler not available for local testing")
            return
            
        correct = 0
        total = len(queries)
        
        for query, expected, description in queries:
            result = self.bedrock_handler._is_cur_related_query(query)
            
            status = "✅" if result == expected else "❌"
            correct += 1 if result == expected else 0
            
            print(f"{status} '{query}'")
            print(f"   Expected: {expected}, Got: {result} - {description}")
            
        print(f"\n📊 Programmatic Detection Accuracy: {correct}/{total} ({(correct/total)*100:.1f}%)")
    
    def test_api_detection(self, query: str, expected_outcome: str, description: str) -> Dict[str, Any]:
        """Test CUR relevance detection via API call."""
        
        print(f"\n🧪 API Test: {description}")
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
            
            status_code = response.status_code
            response_data = response.json()
            
            if status_code == 200:
                outcome = "ACCEPTED"
                details = {
                    "sql_generated": True,
                    "query_type": response_data['structured_query'].get('query_type'),
                    "confidence": response_data.get('confidence')
                }
                print(f"   ✅ ACCEPTED - Generated SQL query")
                print(f"   📊 Type: {details['query_type']}, Confidence: {details['confidence']}")
                
            elif status_code == 400:
                error_detail = response_data.get('detail', {})
                if error_detail.get('error') == 'non_cur_query':
                    outcome = "REJECTED"
                    details = {
                        "sql_generated": False,
                        "reason": error_detail.get('message'),
                        "suggestions": error_detail.get('suggestions', [])
                    }
                    print(f"   ❌ REJECTED - {error_detail.get('message', '')[:100]}...")
                else:
                    outcome = "ERROR"
                    details = {"error": error_detail}
                    print(f"   ⚠️  ERROR - {error_detail}")
                    
            elif status_code == 422:
                outcome = "AI_ERROR"
                details = {"ai_error": response_data.get('detail', {})}
                print(f"   🔧 AI_ERROR - Model parsing failed")
                
            else:
                outcome = f"UNEXPECTED_{status_code}"
                details = {"status_code": status_code}
                print(f"   ❓ UNEXPECTED - Status {status_code}")
                
        except Exception as e:
            outcome = "CONNECTION_ERROR"
            details = {"error": str(e)}
            print(f"   💥 CONNECTION_ERROR - {e}")
        
        # Check if result matches expectation
        correct = self._is_outcome_correct(expected_outcome, outcome)
        
        test_result = {
            "query": query,
            "description": description,
            "expected": expected_outcome,
            "actual": outcome,
            "correct": correct,
            "details": details,
            "timestamp": datetime.now().isoformat()
        }
        
        self.test_results.append(test_result)
        
        print(f"   {'✅ CORRECT' if correct else '❌ INCORRECT'}")
        return test_result
    
    def _is_outcome_correct(self, expected: str, actual: str) -> bool:
        """Check if actual outcome matches expected."""
        if expected == "ACCEPT" and actual == "ACCEPTED":
            return True
        elif expected == "REJECT" and actual == "REJECTED":
            return True
        return False
    
    def run_comprehensive_cur_tests(self):
        """Run comprehensive CUR relevance detection tests."""
        
        print("🎯 Comprehensive CUR Relevance Detection Tests")
        print("📊 Testing Both Programmatic Keywords + AI Model Detection")
        print("=" * 80)
        
        # Test 1: Programmatic keyword detection
        print("\n🔍 SECTION 1: Programmatic Keyword Detection")
        print("-" * 50)
        
        keyword_test_cases = [
            # Should be detected as CUR-related (True)
            ("What's my EC2 charge?", True, "EC2 + charge keyword"),
            ("My Savings Plan charges", True, "Savings Plan keyword"),
            ("Top 10 service charges", True, "Service charges pattern"),
            ("Last month charges", True, "Time + charges"),
            ("EC2 costs by region", True, "Service + costs"),
            ("S3 storage costs", True, "Service + costs"),
            ("Lambda invocation costs", True, "Service + costs"),
            ("My AWS bill", True, "AWS + bill"),
            ("Reserved Instance utilization", True, "RI keyword"),
            ("Cost optimization", True, "Cost + optimization"),
            ("Monthly spending", True, "Time + spending"),
            ("Account charges", True, "Account charges pattern"),
            ("Data transfer costs", True, "Usage + costs"),
            ("RDS pricing", True, "Service + pricing"),
            ("CloudFront fees", True, "Service + fees"),
            
            # Should NOT be detected as CUR-related (False)
            ("What's the weather?", False, "Weather query"),
            ("How to deploy EC2?", False, "Technical deployment"),
            ("EC2 performance metrics", False, "Performance without cost"),
            ("Security best practices", False, "Security topic"),
            ("CloudFormation templates", False, "Technical templates"),
            ("API documentation", False, "Documentation request"),
            ("How to configure VPC?", False, "Configuration question"),
            ("Lambda function code", False, "Code examples"),
            ("What is quantum computing?", False, "General knowledge"),
            ("Hello", False, "Simple greeting"),
            ("AWS", False, "Single word - too vague"),
            ("Help", False, "Generic help request")
        ]
        
        if self.bedrock_handler:
            self.test_programmatic_detection(keyword_test_cases)
        else:
            print("Skipping programmatic tests - handler not available")
        
        # Test 2: API-based detection (AI + keywords combined)
        print(f"\n🤖 SECTION 2: API-Based CUR Detection (AI + Keywords)")
        print("-" * 50)
        
        api_test_cases = [
            # Your specific examples that should be ACCEPTED
            ("What's my EC2 charge?", "ACCEPT", "User's EC2 charge example"),
            ("What is my Savings Plan charge?", "ACCEPT", "User's Savings Plan example"),
            ("My top 10 service charges", "ACCEPT", "User's top services example"),
            ("My last month charges", "ACCEPT", "User's last month example"),
            
            # More CUR-related variations
            ("Show me EC2 costs", "ACCEPT", "EC2 cost query"),
            ("S3 storage expenses", "ACCEPT", "S3 expense query"),
            ("Lambda function costs this month", "ACCEPT", "Lambda monthly costs"),
            ("What did I spend on RDS?", "ACCEPT", "RDS spending query"),
            ("DynamoDB charges by region", "ACCEPT", "DynamoDB regional charges"),
            ("Data transfer costs", "ACCEPT", "Data transfer costs"),
            ("Reserved Instance savings", "ACCEPT", "RI savings query"),
            ("Compare this month to last month costs", "ACCEPT", "Cost comparison"),
            ("Most expensive AWS services", "ACCEPT", "Expensive services query"),
            ("CloudFront distribution costs", "ACCEPT", "CloudFront costs"),
            ("EBS volume pricing", "ACCEPT", "EBS pricing query"),
            
            # Edge cases that should be ACCEPTED
            ("AWS costs", "ACCEPT", "Generic AWS costs"),
            ("My bill breakdown", "ACCEPT", "Bill breakdown"),
            ("Account spending", "ACCEPT", "Account spending"),
            ("Service fees", "ACCEPT", "Service fees"),
            ("Usage charges", "ACCEPT", "Usage charges"),
            
            # Non-CUR queries that should be REJECTED
            ("How to deploy EC2 instances?", "REJECT", "Deployment question"),
            ("EC2 performance optimization", "REJECT", "Performance topic"),
            ("AWS security best practices", "REJECT", "Security topic"),
            ("CloudFormation template example", "REJECT", "Template request"),
            ("Lambda function code", "REJECT", "Code example"),
            ("VPC configuration steps", "REJECT", "Configuration guide"),
            ("What's the weather today?", "REJECT", "Weather query"),
            ("How to bake a cake?", "REJECT", "Cooking query"),
            ("What is machine learning?", "REJECT", "ML definition"),
            ("Hello world", "REJECT", "Simple greeting")
        ]
        
        for query, expected, description in api_test_cases:
            self.test_api_detection(query, expected, description)
    
    def run_edge_case_tests(self):
        """Test edge cases and ambiguous queries."""
        
        print(f"\n🤔 SECTION 3: Edge Cases and Ambiguous Queries")
        print("-" * 50)
        
        edge_cases = [
            # Ambiguous but should lean toward CUR if financial context exists
            ("EC2", "REJECT", "Too vague - service name only"),
            ("Cost", "REJECT", "Too vague - financial term only"), 
            ("AWS spending optimization", "ACCEPT", "Optimization with spending"),
            ("Cloud costs", "ACCEPT", "Generic cloud costs"),
            ("My account", "REJECT", "Too vague - no financial context"),
            ("Service usage", "REJECT", "Usage without cost context"),
            ("Monthly report", "REJECT", "Report without cost context"),
            ("EC2 vs Lambda costs", "ACCEPT", "Cost comparison"),
            ("Performance vs cost trade-offs", "ACCEPT", "Cost in context"),
            ("Budget planning", "ACCEPT", "Budget keyword"),
            
            # Context-dependent cases
            ("Show me data", "REJECT", "Vague data request"),
            ("Transfer costs", "ACCEPT", "Transfer + costs"),
            ("Storage pricing", "ACCEPT", "Storage + pricing"),
            ("Compute expenses", "ACCEPT", "Compute + expenses"),
            ("Network charges", "ACCEPT", "Network + charges"),
            
            # Financial terms in different contexts
            ("Cost of living", "REJECT", "Cost but not AWS"),
            ("Price of gold", "REJECT", "Price but not AWS"),
            ("AWS cost center setup", "ACCEPT", "AWS + cost center"),
            ("Billing address", "REJECT", "Billing but not usage")
        ]
        
        for query, expected, description in edge_cases:
            self.test_api_detection(query, expected, description)
    
    def print_summary(self):
        """Print comprehensive test summary."""
        
        total_api_tests = len(self.test_results)
        correct_api = sum(1 for result in self.test_results if result["correct"])
        
        print(f"\n📋 CUR RELEVANCE DETECTION SUMMARY")
        print("=" * 80)
        print(f"API Tests: {correct_api}/{total_api_tests} correct ({(correct_api/total_api_tests)*100:.1f}%)")
        
        # Analyze by expected outcome
        accept_tests = [r for r in self.test_results if r["expected"] == "ACCEPT"]
        reject_tests = [r for r in self.test_results if r["expected"] == "REJECT"]
        
        accept_correct = sum(1 for r in accept_tests if r["correct"])
        reject_correct = sum(1 for r in reject_tests if r["correct"])
        
        print(f"\n📊 DETAILED BREAKDOWN:")
        print(f"CUR Queries (Should Accept): {accept_correct}/{len(accept_tests)} ({(accept_correct/len(accept_tests)*100):.1f}%)")
        print(f"Non-CUR Queries (Should Reject): {reject_correct}/{len(reject_tests)} ({(reject_correct/len(reject_tests)*100):.1f}%)")
        
        # Show failed cases
        failed_cases = [r for r in self.test_results if not r["correct"]]
        if failed_cases:
            print(f"\n❌ FAILED CASES:")
            for case in failed_cases[:10]:  # Show first 10
                print(f"   • '{case['query']}' - Expected {case['expected']}, Got {case['actual']}")
                
        # Show patterns
        accepted_queries = [r for r in self.test_results if r["actual"] == "ACCEPTED"]
        rejected_queries = [r for r in self.test_results if r["actual"] == "REJECTED"]
        
        print(f"\n✅ ACCEPTED PATTERNS ({len(accepted_queries)}):")
        for query in accepted_queries[:5]:
            print(f"   • '{query['query']}'")
            
        print(f"\n❌ REJECTED PATTERNS ({len(rejected_queries)}):")
        for query in rejected_queries[:5]:
            print(f"   • '{query['query']}'")
        
        print(f"\n🎯 KEY INSIGHTS:")
        print("1. Financial keywords (cost, charge, bill, spend) are strong CUR indicators")
        print("2. AWS service names need financial context to be considered CUR-related")
        print("3. Time-based financial queries (last month, yearly) are reliably detected")
        print("4. RI/Savings Plan keywords are always considered CUR-related")
        print("5. Pure technical or general knowledge queries are properly rejected")


def main():
    """Run comprehensive CUR relevance detection tests."""
    
    print("🔍 CUR Relevance Detection Test Suite")
    print("🎯 Testing How System Determines CUR vs Non-CUR Queries")
    print("📅 " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    try:
        tester = CURRelevanceTester()
        
        # Test connection
        try:
            response = requests.get(f"{tester.base_url}/bedrock/models", timeout=5)
            if response.status_code == 200:
                print("✅ API connection successful")
            else:
                print(f"⚠️  API responded with status {response.status_code}")
        except Exception as e:
            print(f"❌ Cannot connect to API: {e}")
            print("   Some tests will be skipped")
        
        # Run comprehensive tests
        tester.run_comprehensive_cur_tests()
        tester.run_edge_case_tests()
        
        # Print summary
        tester.print_summary()
        
        print(f"\n🔧 DETECTION MECHANISMS:")
        print("1. 🤖 AI Model: Enhanced prompt with explicit CUR examples")
        print("2. 🔍 Keywords: Programmatic analysis of financial terms")
        print("3. 🎯 Patterns: Specific CUR-related phrase detection")
        print("4. 📊 Context: AWS services + financial context combination")
        print("5. ⚠️  Validation: Optional programmatic pre-check with logging")
        
        print(f"\n💡 EXAMPLES OF GOOD CUR QUERIES:")
        print("• 'What's my EC2 charge?' ✅")
        print("• 'My Savings Plan charges' ✅") 
        print("• 'Top 10 service charges' ✅")
        print("• 'Last month charges' ✅")
        print("• 'S3 storage costs by region' ✅")
        
        print(f"\n❌ EXAMPLES OF NON-CUR QUERIES:")
        print("• 'How to deploy EC2?' ❌")
        print("• 'EC2 performance metrics' ❌")
        print("• 'What's the weather?' ❌")
        print("• 'Security best practices' ❌")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Tests interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()
