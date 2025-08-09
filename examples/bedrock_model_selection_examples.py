#!/usr/bin/env python3
"""
Bedrock Model Selection Examples - Practical demonstrations

This script shows how to use different Claude models for various cost analysis scenarios.
"""

import requests
import json
from typing import Dict, Any, Optional
from datetime import datetime


class BedrockAPIClient:
    """Client for interacting with Infralyzer Bedrock endpoints."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        
    def list_models(self) -> Dict[str, Any]:
        """List all available models."""
        response = requests.get(f"{self.base_url}/bedrock/models")
        return response.json()
    
    def generate_query(
        self, 
        user_query: str, 
        model_id: str = "CLAUDE_3_5_SONNET",
        temperature: float = 0.1,
        max_tokens: int = 2048
    ) -> Dict[str, Any]:
        """Generate structured query using specified model."""
        
        payload = {
            "user_query": user_query,
            "model_configuration": {
                "model_id": model_id,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "top_p": 0.9,
                "top_k": 250
            },
            "include_examples": True,
            "target_table": "CUR"
        }
        
        response = requests.post(f"{self.base_url}/bedrock/generate-query", json=payload)
        return response.json()
    
    def chat_with_knowledge_base(
        self,
        message: str,
        knowledge_base_id: str,
        model_id: str = "CLAUDE_3_5_SONNET",
        conversation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Chat with AI using knowledge base context."""
        
        payload = {
            "message": message,
            "knowledge_base_id": knowledge_base_id,
            "conversation_id": conversation_id,
            "model_configuration": {
                "model_id": model_id,
                "max_tokens": 4096,
                "temperature": 0.1,
                "top_p": 0.9,
                "top_k": 250
            },
            "include_sources": True
        }
        
        response = requests.post(f"{self.base_url}/bedrock/chat", json=payload)
        return response.json()


def example_1_model_comparison():
    """Example 1: Compare different models for the same query."""
    
    print("🔍 Example 1: Model Comparison for Cost Analysis")
    print("=" * 60)
    
    client = BedrockAPIClient()
    query = "What are my top 10 most expensive AWS services this month with cost breakdown?"
    
    models_to_test = [
        ("CLAUDE_4_OPUS", "🚀 Most Powerful"),
        ("CLAUDE_3_7_SONNET", "🧠 Hybrid Reasoning"), 
        ("CLAUDE_3_5_SONNET", "💎 Balanced Performance"),
        ("CLAUDE_3_5_HAIKU", "⚡ Fast Response")
    ]
    
    results = {}
    
    for model_id, description in models_to_test:
        print(f"\n📊 Testing {model_id} - {description}")
        try:
            start_time = datetime.now()
            result = client.generate_query(
                user_query=query,
                model_id=model_id,
                temperature=0.1,
                max_tokens=3072
            )
            end_time = datetime.now()
            
            response_time = (end_time - start_time).total_seconds()
            
            results[model_id] = {
                "sql_query": result["structured_query"]["sql_query"],
                "confidence": result["confidence"],
                "response_time": response_time,
                "model_used": result["model_used"]
            }
            
            print(f"  ✅ Confidence: {result['confidence']:.2f}")
            print(f"  ⏱️  Response time: {response_time:.2f}s")
            print(f"  📝 SQL preview: {result['structured_query']['sql_query'][:100]}...")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            results[model_id] = {"error": str(e)}
    
    print(f"\n📋 Summary:")
    for model_id, result in results.items():
        if "error" not in result:
            print(f"  {model_id}: Confidence {result['confidence']:.2f}, Time {result['response_time']:.2f}s")


def example_2_use_case_specific_models():
    """Example 2: Use case-specific model selection."""
    
    print("\n🎯 Example 2: Use Case-Specific Model Selection")
    print("=" * 60)
    
    client = BedrockAPIClient()
    
    use_cases = [
        {
            "name": "Complex Cost Optimization Analysis",
            "query": "Analyze my Reserved Instance utilization and provide detailed recommendations for optimization including potential savings calculations",
            "model": "CLAUDE_4_OPUS",
            "temperature": 0.0,
            "max_tokens": 4096,
            "reason": "Requires highest accuracy for financial recommendations"
        },
        {
            "name": "Production Cost Reporting", 
            "query": "Generate monthly cost report by service with trends",
            "model": "CLAUDE_4_SONNET",
            "temperature": 0.1,
            "max_tokens": 2048,
            "reason": "Balanced performance for regular reporting"
        },
        {
            "name": "Multi-step Analysis",
            "query": "First identify my highest cost drivers, then analyze cost trends, and finally provide optimization strategies",
            "model": "CLAUDE_3_7_SONNET", 
            "temperature": 0.2,
            "max_tokens": 3072,
            "reason": "Hybrid reasoning for complex multi-step analysis"
        },
        {
            "name": "Quick Cost Summary",
            "query": "What did I spend on EC2 yesterday?",
            "model": "CLAUDE_3_5_HAIKU",
            "temperature": 0.05,
            "max_tokens": 1024,
            "reason": "Fast response for simple queries"
        }
    ]
    
    for case in use_cases:
        print(f"\n📋 Use Case: {case['name']}")
        print(f"🔧 Model: {case['model']} - {case['reason']}")
        print(f"❓ Query: {case['query']}")
        
        try:
            result = client.generate_query(
                user_query=case['query'],
                model_id=case['model'],
                temperature=case['temperature'],
                max_tokens=case['max_tokens']
            )
            
            print(f"  ✅ Success! Confidence: {result['confidence']:.2f}")
            print(f"  📊 Query type: {result['structured_query'].get('query_type', 'N/A')}")
            print(f"  📈 Visualization: {result['structured_query'].get('visualization_type', 'N/A')}")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")


def example_3_temperature_experimentation():
    """Example 3: Temperature settings for different scenarios."""
    
    print("\n🌡️ Example 3: Temperature Settings Impact")
    print("=" * 60)
    
    client = BedrockAPIClient()
    query = "Suggest creative cost optimization strategies for my AWS infrastructure"
    
    temperature_scenarios = [
        (0.0, "🎯 Deterministic (Financial Calculations)"),
        (0.1, "📊 Low Creativity (Standard Analysis)"),
        (0.3, "💡 Moderate Creativity (Recommendations)"),
        (0.5, "🚀 High Creativity (Brainstorming)")
    ]
    
    for temp, description in temperature_scenarios:
        print(f"\n🌡️ Temperature: {temp} - {description}")
        
        try:
            result = client.generate_query(
                user_query=query,
                model_id="CLAUDE_3_5_SONNET",
                temperature=temp,
                max_tokens=2048
            )
            
            print(f"  ✅ Confidence: {result['confidence']:.2f}")
            print(f"  💭 Title: {result['structured_query'].get('title', 'N/A')}")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")


def example_4_token_optimization():
    """Example 4: Token optimization for different response lengths."""
    
    print("\n🎫 Example 4: Token Optimization")
    print("=" * 60)
    
    client = BedrockAPIClient()
    
    token_scenarios = [
        {
            "tokens": 512,
            "query": "Total EC2 cost today?",
            "description": "💬 Minimal Response"
        },
        {
            "tokens": 1024,
            "query": "Show me S3 costs by bucket for this month",
            "description": "📝 Short Analysis"
        },
        {
            "tokens": 2048,
            "query": "Analyze my RDS costs and provide optimization recommendations",
            "description": "📊 Standard Report"
        },
        {
            "tokens": 4096,
            "query": "Comprehensive cost analysis with trends, forecasts, and detailed optimization strategies",
            "description": "📚 Detailed Analysis"
        }
    ]
    
    for scenario in token_scenarios:
        print(f"\n🎫 {scenario['tokens']} tokens - {scenario['description']}")
        print(f"❓ Query: {scenario['query']}")
        
        try:
            result = client.generate_query(
                user_query=scenario['query'],
                model_id="CLAUDE_3_5_SONNET",
                temperature=0.1,
                max_tokens=scenario['tokens']
            )
            
            # Estimate actual tokens used (rough approximation)
            response_text = json.dumps(result['structured_query'])
            estimated_tokens = len(response_text.split()) * 1.3  # Rough token estimation
            
            print(f"  ✅ Success! Estimated tokens used: {estimated_tokens:.0f}/{scenario['tokens']}")
            print(f"  📊 Response length: {len(response_text)} characters")
            
        except Exception as e:
            print(f"  ❌ Error: {e}")


def example_5_knowledge_base_chat():
    """Example 5: Model selection for knowledge base chat."""
    
    print("\n💭 Example 5: Knowledge Base Chat with Different Models")
    print("=" * 60)
    
    client = BedrockAPIClient()
    
    # Note: You'll need to replace this with an actual knowledge base ID
    knowledge_base_id = "YOUR_KNOWLEDGE_BASE_ID"
    
    chat_scenarios = [
        {
            "model": "CLAUDE_4_OPUS",
            "message": "Based on our CUR data, what are the most significant cost optimization opportunities?",
            "description": "🚀 Comprehensive Analysis"
        },
        {
            "model": "CLAUDE_3_5_SONNET", 
            "message": "What trends do you see in our EC2 spending?",
            "description": "💎 Balanced Insight"
        },
        {
            "model": "CLAUDE_3_5_HAIKU",
            "message": "Quick summary of this month's AWS costs",
            "description": "⚡ Fast Summary"
        }
    ]
    
    print("⚠️ Note: This example requires a valid knowledge_base_id")
    print("   Replace 'YOUR_KNOWLEDGE_BASE_ID' with actual KB ID to run")
    
    for scenario in chat_scenarios:
        print(f"\n💬 {scenario['model']} - {scenario['description']}")
        print(f"📝 Message: {scenario['message']}")
        
        if knowledge_base_id != "YOUR_KNOWLEDGE_BASE_ID":
            try:
                result = client.chat_with_knowledge_base(
                    message=scenario['message'],
                    knowledge_base_id=knowledge_base_id,
                    model_id=scenario['model']
                )
                
                print(f"  ✅ Response length: {len(result['response'])} characters")
                print(f"  🔗 Sources used: {len(result['knowledge_sources'])}")
                print(f"  💬 Conversation ID: {result['conversation_id']}")
                
            except Exception as e:
                print(f"  ❌ Error: {e}")
        else:
            print("  ⏸️ Skipped - need valid knowledge_base_id")


def main():
    """Run all examples."""
    print("🤖 Bedrock Model Selection Examples")
    print("🔧 Infralyzer FinOps Platform")
    print("=" * 80)
    
    try:
        # First, list available models
        client = BedrockAPIClient()
        models_response = client.list_models()
        
        print(f"📋 Found {models_response.get('total_count', 0)} available models")
        print("\n🏷️ Predefined Model Configurations:")
        for model in models_response.get('predefined_configurations', [])[:5]:
            print(f"  • {model['model_name']}: {model['recommended_use']}")
        
        # Run examples
        example_1_model_comparison()
        example_2_use_case_specific_models() 
        example_3_temperature_experimentation()
        example_4_token_optimization()
        example_5_knowledge_base_chat()
        
        print("\n🎉 All examples completed!")
        print("\n💡 Tips:")
        print("  • Use Claude 4 Opus for critical financial analysis")
        print("  • Use Claude 3.5 Haiku for quick development and testing")
        print("  • Adjust temperature based on creativity needs")
        print("  • Monitor token usage to optimize costs")
        
    except requests.exceptions.ConnectionError:
        print("❌ Error: Cannot connect to Infralyzer API")
        print("   Make sure the server is running on http://localhost:8000")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
