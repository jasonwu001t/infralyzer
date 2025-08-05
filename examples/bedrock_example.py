#!/usr/bin/env python3
"""
AWS Bedrock Integration Example

This example demonstrates how to use the AWS Bedrock integration
for chatbot functionality and structured query generation.
"""

import asyncio
import json
from infralyzer import FinOpsEngine, DataConfig, DataExportType
from infralyzer.utils.bedrock_handler import BedrockHandler, BedrockModel, ModelConfiguration


async def main():
    """Main example function."""
    
    print("🤖 AWS Bedrock Integration Example")
    print("=" * 50)
    
    # Example configuration (update with your actual values)
    config = DataConfig(
        s3_bucket='your-cur-bucket',
        s3_data_prefix='cur2/data', 
        data_export_type=DataExportType.CUR_2_0,
        aws_region='us-east-1'
    )
    
    try:
        # Initialize Bedrock handler
        print("\n1. Initializing Bedrock handler...")
        bedrock = BedrockHandler(config, default_model=BedrockModel.CLAUDE_3_5_SONNET)
        print("✅ Bedrock handler initialized")
        
        # List available models (this will make an API call)
        print("\n2. Listing available models...")
        try:
            models = bedrock.list_available_models()
            print(f"✅ Found {len(models)} available models")
            
            # Show first few models
            for i, model in enumerate(models[:3]):
                print(f"   - {model['model_name']} ({model['provider_name']})")
                
        except Exception as e:
            print(f"⚠️  Could not list models (likely due to missing AWS credentials): {e}")
        
        # Demonstrate model configuration
        print("\n3. Model configuration examples...")
        
        # High precision config for factual queries
        precise_config = ModelConfiguration(
            model_id=BedrockModel.CLAUDE_3_5_SONNET,
            max_tokens=4096,
            temperature=0.1,
            top_p=0.9,
            top_k=250
        )
        print("✅ Precise configuration created")
        
        # Creative config for analysis
        creative_config = ModelConfiguration(
            model_id=BedrockModel.CLAUDE_3_SONNET,
            max_tokens=2048,
            temperature=0.7,
            top_p=0.95
        )
        print("✅ Creative configuration created")
        
        # Demonstrate structured query generation (mock)
        print("\n4. Structured query generation example...")
        
        example_queries = [
            "What are my top 5 most expensive services this month?",
            "Show me EC2 costs by region over the last 6 months",
            "Which accounts have the highest S3 storage costs?",
            "Show me daily cost trends for Lambda functions"
        ]
        
        for query in example_queries:
            print(f"\n   Query: '{query}'")
            
            # In a real scenario, this would call Bedrock
            # For demo purposes, we'll show the expected structure
            mock_result = generate_mock_structured_query(query)
            print(f"   SQL: {mock_result['sql_query'][:80]}...")
            print(f"   Type: {mock_result['query_type']}")
            print(f"   Visualization: {mock_result['visualization_type']}")
            print(f"   Confidence: {mock_result['confidence']}")
        
        # Knowledge base example
        print("\n5. Knowledge base configuration example...")
        
        from infralyzer.utils.bedrock_handler import KnowledgeBaseConfig
        
        kb_config = KnowledgeBaseConfig(
            name="CUR-FinOps-Knowledge",
            description="AWS Cost and Usage Report knowledge base for FinOps optimization",
            role_arn="arn:aws:iam::123456789012:role/BedrockRole",
            s3_bucket="your-cur-bucket",
            s3_prefix="cur2/data",
            chunk_size=500,
            overlap_percentage=15
        )
        print("✅ Knowledge base configuration created")
        print(f"   Name: {kb_config.name}")
        print(f"   Chunk size: {kb_config.chunk_size}")
        print(f"   Overlap: {kb_config.overlap_percentage}%")
        
        print("\n6. Available Bedrock models...")
        for model in BedrockModel:
            recommendation = get_model_recommendation(model)
            print(f"   - {model.name}: {recommendation}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n🎉 Example completed!")
    print("\nNext steps:")
    print("1. Set up AWS credentials with Bedrock permissions")
    print("2. Create an IAM role for Bedrock knowledge base access")
    print("3. Use the API endpoints to create knowledge bases and chat")
    print("4. Integrate with your frontend application")


def generate_mock_structured_query(user_query: str) -> dict:
    """Generate a mock structured query response."""
    
    if "top" in user_query.lower() and "expensive" in user_query.lower():
        return {
            "sql_query": "SELECT product_servicecode, SUM(line_item_unblended_cost) as total_cost FROM CUR WHERE DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE) AND line_item_unblended_cost > 0 GROUP BY 1 ORDER BY 2 DESC LIMIT 5",
            "query_type": "summary",
            "visualization_type": "bar_chart",
            "title": "Top 5 Services by Cost This Month",
            "description": "Services with highest spending in current month",
            "filters": {"date_range": "current_month"},
            "confidence": 0.95
        }
    
    elif "ec2" in user_query.lower() and "region" in user_query.lower():
        return {
            "sql_query": "SELECT DATE_TRUNC('month', line_item_usage_start_date) as month, product_region, SUM(line_item_unblended_cost) as monthly_cost FROM CUR WHERE product_servicecode = 'AmazonEC2' AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '6 months' GROUP BY 1, 2 ORDER BY 1, 3 DESC",
            "query_type": "trend",
            "visualization_type": "line_chart",
            "title": "EC2 Costs by Region (6 Months)",
            "description": "Monthly EC2 spending trends across regions",
            "filters": {"date_range": "last_6_months", "services": ["AmazonEC2"]},
            "confidence": 0.92
        }
    
    elif "account" in user_query.lower() and "s3" in user_query.lower():
        return {
            "sql_query": "SELECT line_item_usage_account_id, SUM(line_item_unblended_cost) as total_cost FROM CUR WHERE product_servicecode = 'AmazonS3' AND line_item_unblended_cost > 0 GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            "query_type": "summary",
            "visualization_type": "pie_chart",
            "title": "S3 Costs by Account",
            "description": "Account-level S3 storage costs",
            "filters": {"services": ["AmazonS3"]},
            "confidence": 0.88
        }
    
    else:
        return {
            "sql_query": "SELECT DATE(line_item_usage_start_date) as date, SUM(line_item_unblended_cost) as daily_cost FROM CUR WHERE product_servicecode = 'AWSLambda' AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY 1 ORDER BY 1",
            "query_type": "trend",
            "visualization_type": "line_chart",
            "title": "Daily Lambda Costs (30 Days)",
            "description": "Daily cost trends for Lambda functions",
            "filters": {"date_range": "last_30_days", "services": ["AWSLambda"]},
            "confidence": 0.85
        }


def get_model_recommendation(model: BedrockModel) -> str:
    """Get usage recommendation for a model."""
    recommendations = {
        BedrockModel.CLAUDE_3_5_SONNET: "Best overall performance for complex reasoning and analysis",
        BedrockModel.CLAUDE_3_SONNET: "Good balance of capability and speed for most tasks",
        BedrockModel.CLAUDE_3_HAIKU: "Fast and efficient for simple queries and summaries",
        BedrockModel.CLAUDE_3_OPUS: "Highest capability for complex analysis and reasoning",
        BedrockModel.TITAN_TEXT_G1_EXPRESS: "Cost-effective for basic text generation",
        BedrockModel.TITAN_TEXT_G1_LARGE: "Amazon's flagship model for general text tasks",
        BedrockModel.COHERE_COMMAND_TEXT: "Good for conversational and command-following tasks",
        BedrockModel.LLAMA2_70B_CHAT: "Open-source option for conversational AI",
        BedrockModel.AI21_J2_ULTRA: "Strong performance for business and analytical tasks"
    }
    return recommendations.get(model, "General purpose AI model")


if __name__ == "__main__":
    asyncio.run(main())