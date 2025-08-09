#!/usr/bin/env python3
"""
Test FinOps Expert Chatbot - Demonstrating the FinOps Expert API Endpoint

This script tests the new FinOps expert chatbot that provides specialized
AWS cost optimization and billing guidance.
"""

import requests
import json
from typing import Dict, Any, List
from datetime import datetime


class FinOpsChatbotTester:
    """Test client for the FinOps expert chatbot."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.conversation_history = []
    
    def chat_with_finops_expert(
        self, 
        message: str, 
        context_type: str = "general",
        model_id: str = "CLAUDE_3_5_SONNET",
        conversation_id: str = None
    ) -> Dict[str, Any]:
        """Chat with the FinOps expert."""
        
        print(f"\n🧑‍💼 FinOps Expert Chat")
        print(f"💬 Your Question: {message}")
        print(f"🎯 Context: {context_type}")
        print(f"🤖 Model: {model_id}")
        
        payload = {
            "message": message,
            "conversation_id": conversation_id,
            "model_configuration": {
                "model_id": model_id,
                "max_tokens": 3072,
                "temperature": 0.2,  # Slightly higher for more creative recommendations
                "top_p": 0.9,
                "top_k": 250
            },
            "include_examples": True,
            "context_type": context_type
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/bedrock/finops-expert",
                json=payload,
                timeout=60  # Longer timeout for detailed responses
            )
            
            status_code = response.status_code
            
            if status_code == 200:
                result = response.json()
                
                print("✅ SUCCESS - Got expert response")
                print(f"📝 Response ({len(result['response'])} chars):")
                print("─" * 60)
                print(result['response'])
                print("─" * 60)
                
                print(f"\n💡 Recommendations ({len(result['recommendations'])} items):")
                for i, rec in enumerate(result['recommendations'], 1):
                    print(f"   {i}. {rec}")
                
                print(f"\n🔗 Related Topics ({len(result['related_topics'])} items):")
                for topic in result['related_topics']:
                    print(f"   • {topic}")
                
                print(f"\n📊 Metadata:")
                print(f"   🆔 Conversation ID: {result['conversation_id']}")
                print(f"   🤖 Model Used: {result['model_used']}")
                print(f"   🎯 Context Type: {result['context_type']}")
                print(f"   ⏰ Timestamp: {result['timestamp']}")
                
                # Store in conversation history
                self.conversation_history.append({
                    "message": message,
                    "response": result,
                    "timestamp": datetime.now().isoformat()
                })
                
                return {"status": "success", "data": result}
                
            elif status_code == 422:
                error_detail = response.json().get('detail', {})
                print("🔧 FinOps Expert Error")
                print(f"💬 Message: {error_detail.get('message', '')}")
                print(f"💡 Suggestions:")
                for suggestion in error_detail.get('suggestions', []):
                    print(f"   • {suggestion}")
                return {"status": "error", "data": error_detail}
                
            else:
                print(f"❓ Unexpected Status: {status_code}")
                return {"status": f"unexpected_{status_code}", "data": response.text}
                
        except Exception as e:
            print(f"💥 Error: {e}")
            return {"status": "exception", "data": str(e)}
    
    def run_finops_chatbot_demo(self):
        """Run comprehensive FinOps chatbot demonstration."""
        
        print("🧑‍💼 FinOps Expert Chatbot Demo")
        print("💰 Specialized AWS Cost Optimization & Billing Guidance")
        print("=" * 80)
        
        # Demo conversations organized by context type
        demo_conversations = [
            {
                "context": "general",
                "title": "General FinOps Guidance",
                "questions": [
                    "What is FinOps and how should we get started?",
                    "How do I set up cost visibility for my AWS organization?",
                    "What are the key FinOps metrics we should track?"
                ]
            },
            {
                "context": "cost_analysis", 
                "title": "Cost Analysis Expertise",
                "questions": [
                    "Our AWS costs increased 40% last month, how do I investigate?",
                    "How do I analyze cost trends and identify anomalies?",
                    "What's the best way to do cost allocation by department?"
                ]
            },
            {
                "context": "optimization",
                "title": "Cost Optimization Strategies", 
                "questions": [
                    "How can I optimize our EC2 costs without impacting performance?",
                    "What are the most effective cost optimization techniques?",
                    "How do I identify and eliminate waste in our AWS environment?"
                ]
            },
            {
                "context": "ri_sp",
                "title": "Reserved Instances & Savings Plans",
                "questions": [
                    "Should we buy Reserved Instances or Savings Plans?",
                    "How do I calculate ROI for RI purchases?",
                    "What's the best strategy for RI/SP coverage?"
                ]
            }
        ]
        
        # Run demonstrations for each context
        for demo in demo_conversations:
            print(f"\n{'='*80}")
            print(f"📋 {demo['title'].upper()} - Context: {demo['context']}")
            print(f"{'='*80}")
            
            for i, question in enumerate(demo['questions'], 1):
                print(f"\n🔸 Question {i}/{len(demo['questions'])}")
                result = self.chat_with_finops_expert(
                    message=question,
                    context_type=demo['context'],
                    conversation_id=f"{demo['context']}_demo"
                )
                
                # Brief pause between questions for readability
                if i < len(demo['questions']):
                    print("\n" + "─" * 40 + " Next Question " + "─" * 40)
    
    def run_interactive_chat_examples(self):
        """Show examples of different types of FinOps questions."""
        
        print(f"\n{'='*80}")
        print("🎯 INTERACTIVE FINOPS CHAT EXAMPLES")
        print("💡 Showing different types of expert guidance")
        print("=" * 80)
        
        example_scenarios = [
            {
                "scenario": "Cost Spike Investigation",
                "question": "We just got a $10,000 AWS bill and it's usually $3,000. Our EC2 costs are 5x higher. What should I check first?",
                "context": "cost_analysis"
            },
            {
                "scenario": "Multi-Account Cost Management", 
                "question": "We have 20 AWS accounts and want to implement chargeback. What's the best approach for cost allocation?",
                "context": "general"
            },
            {
                "scenario": "Right-sizing Strategy",
                "question": "Our EC2 instances are running at 10% CPU utilization. How do I right-size them safely?",
                "context": "optimization"
            },
            {
                "scenario": "RI vs SP Decision",
                "question": "We have steady EC2 workloads and some Lambda functions. Should we buy RIs, Savings Plans, or both?",
                "context": "ri_sp"
            },
            {
                "scenario": "Storage Cost Optimization",
                "question": "Our S3 costs are growing 30% per month. What are the best strategies to optimize storage costs?",
                "context": "optimization"
            },
            {
                "scenario": "Cost Anomaly Setup",
                "question": "How do I set up automated alerts for unexpected AWS cost increases?",
                "context": "cost_analysis"
            }
        ]
        
        for scenario in example_scenarios:
            print(f"\n📋 SCENARIO: {scenario['scenario']}")
            print("─" * 60)
            
            result = self.chat_with_finops_expert(
                message=scenario['question'],
                context_type=scenario['context'],
                model_id="CLAUDE_4_SONNET"  # Use higher capability model for complex scenarios
            )
            
            if result['status'] == 'success':
                # Highlight key aspects of the response
                data = result['data']
                print(f"\n🎯 KEY INSIGHTS:")
                print(f"   • Response quality: {len(data['response'])} characters")
                print(f"   • Actionable recommendations: {len(data['recommendations'])}")
                print(f"   • Related topics suggested: {len(data['related_topics'])}")
                
                # Show first recommendation as example
                if data['recommendations']:
                    print(f"   • Top recommendation: {data['recommendations'][0]}")
    
    def test_edge_cases(self):
        """Test edge cases and error handling."""
        
        print(f"\n{'='*80}")
        print("🧪 EDGE CASES & ERROR HANDLING")
        print("=" * 80)
        
        edge_cases = [
            {
                "name": "Non-FinOps Question",
                "question": "What's the weather like today?",
                "context": "general",
                "expected": "Should redirect to FinOps topics"
            },
            {
                "name": "Very Short Question",
                "question": "Help",
                "context": "general", 
                "expected": "Should ask for more specific FinOps question"
            },
            {
                "name": "Technical Question",
                "question": "How do I configure a VPC?",
                "context": "general",
                "expected": "Should focus on VPC cost aspects"
            },
            {
                "name": "Mixed Context Question",
                "question": "How do I optimize costs while ensuring security compliance?",
                "context": "optimization",
                "expected": "Should address both cost and compliance"
            }
        ]
        
        for case in edge_cases:
            print(f"\n🧪 Testing: {case['name']}")
            print(f"❓ Question: {case['question']}")
            print(f"🎯 Expected: {case['expected']}")
            
            result = self.chat_with_finops_expert(
                message=case['question'],
                context_type=case['context']
            )
            
            if result['status'] == 'success':
                print("✅ Response received - check if it stays focused on FinOps")
            else:
                print(f"⚠️  Got {result['status']} - {result.get('data', {}).get('message', 'Unknown error')}")
    
    def print_conversation_summary(self):
        """Print summary of all conversations."""
        
        if not self.conversation_history:
            print("\n📋 No conversations recorded")
            return
            
        print(f"\n📋 CONVERSATION SUMMARY")
        print("=" * 80)
        print(f"Total conversations: {len(self.conversation_history)}")
        
        total_recommendations = sum(len(conv['response']['recommendations']) for conv in self.conversation_history)
        total_topics = sum(len(conv['response']['related_topics']) for conv in self.conversation_history)
        
        print(f"Total recommendations provided: {total_recommendations}")
        print(f"Total related topics suggested: {total_topics}")
        
        # Most common context types
        contexts = [conv['response']['context_type'] for conv in self.conversation_history]
        context_counts = {ctx: contexts.count(ctx) for ctx in set(contexts)}
        
        print(f"\nContext type distribution:")
        for ctx, count in context_counts.items():
            print(f"   {ctx}: {count} conversations")
        
        # Average response length
        avg_length = sum(len(conv['response']['response']) for conv in self.conversation_history) / len(self.conversation_history)
        print(f"\nAverage response length: {avg_length:.0f} characters")


def main():
    """Run comprehensive FinOps chatbot testing."""
    
    print("🧑‍💼 FinOps Expert Chatbot Test Suite")
    print("💰 AWS Cost Optimization & Billing Guidance")
    print("📅 " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    try:
        tester = FinOpsChatbotTester()
        
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
        
        # Run comprehensive demos
        print("\n🎯 DEMO OPTIONS:")
        print("1. Quick interactive examples")
        print("2. Full context-based demonstration")
        print("3. Edge cases and error handling")
        print("4. All of the above")
        
        choice = input("\nSelect option (1-4) or press Enter for all: ").strip()
        
        if choice in ['1', '4', '']:
            tester.run_interactive_chat_examples()
        
        if choice in ['2', '4', '']:
            tester.run_finops_chatbot_demo()
        
        if choice in ['3', '4', '']:
            tester.test_edge_cases()
        
        # Print summary
        tester.print_conversation_summary()
        
        print(f"\n🎉 FinOps Expert Chatbot Testing Complete!")
        
        print(f"\n💡 KEY FEATURES DEMONSTRATED:")
        print("• Senior FinOps expert persona with 10+ years experience")
        print("• Context-aware responses (general, cost_analysis, optimization, ri_sp)")
        print("• Actionable recommendations extraction")
        print("• Related topics suggestions")
        print("• AWS-specific cost optimization guidance")
        print("• Structured response format with clear sections")
        
        print(f"\n🔧 API ENDPOINT:")
        print("POST /bedrock/finops-expert")
        print("Request: message, context_type, model_configuration")
        print("Response: expert_response, recommendations, related_topics")
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Testing interrupted by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()
