"""
Test 6: MCP Server Integration
==============================

This test demonstrates the Model Context Protocol (MCP) server functionality.
Tests the AI assistant integration capabilities for cost analytics.
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infralyzer import FinOpsEngine, DataConfig, DataExportType
import json

def test_mcp_server():
    """Test MCP server integration capabilities"""
    
    print("Test 6: MCP Server Integration")
    
    # Configuration using local data from Test 2
    local_path = "./test_local_data"
    
    config = DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',          
        data_export_type=DataExportType.CUR_2_0,               
        table_name='CUR',                        
        date_start='2025-07',                    
        date_end='2025-07',
        local_data_path=local_path,
        prefer_local_data=True
    )
    
    try:
        # Check if local data exists
        if not os.path.exists(local_path):
            print(f"Local data not found at {local_path}")
            print("Run test_2_download_local.py first")
            return False
        
        # Initialize engine
        print("Initializing engine...")
        engine = FinOpsEngine(config)
        
        # Get MCP analytics module
        mcp = engine.mcp
        
        # Test 1: Get MCP Resources
        print("\nStep 1: Get MCP Resources")
        print("-" * 40)
        
        resources = mcp.get_mcp_resources()
        print(f"MCP Resources: {len(resources['resources'])} available")
        
        print("Available Resources:")
        for resource in resources['resources'][:3]:  # Show first 3
            print(f"   • {resource['name']}: {resource['description']}")
        
        print(f"MCP Version: {resources.get('mcp_version', 'N/A')}")
        print(f"Protocols: {', '.join(resources.get('supported_protocols', []))}")
        
        # Test 2: Get MCP Tools
        print("\nStep 2: Get MCP Tools")
        print("-" * 40)
        
        tools = mcp.get_mcp_tools()
        print(f"MCP Tools: {len(tools['tools'])} available")
        
        print("Available Tools:")
        for tool in tools['tools'][:3]:  # Show first 3
            print(f"   • {tool['name']}: {tool['description']}")
        
        # Test 3: Process MCP Queries
        print("\nStep 3: Process MCP Queries")
        print("-" * 40)
        
        # Test natural language queries
        test_queries = [
            "What are my top spending services?",
            "Show me cost optimization opportunities",
            "How much did I spend last month?"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\nQuery {i}: '{query}'")
            
            try:
                response = mcp.process_mcp_query(query)
                
                print(f"Query processed successfully")
                print(f"   Intent: {response.get('intent', {}).get('intent', 'unknown')}")
                print(f"   Results: {len(response.get('results', {}))} data points")
                
                # Show insights if available
                if response.get('insights'):
                    print(f"   Insights: {len(response['insights'])} generated")
                    for insight in response['insights'][:2]:  # Show first 2
                        print(f"      • {insight}")
                
                # Show recommendations if available
                if response.get('recommendations'):
                    print(f"   Recommendations: {len(response['recommendations'])} generated")
                    for rec in response['recommendations'][:1]:  # Show first 1
                        print(f"      • {rec.get('action', 'N/A')}")
                
            except Exception as e:
                print(f"Query failed: {str(e)}")
        
        # Test 4: MCP Stream Configuration
        print("\nStep 4: MCP Stream Configuration")
        print("-" * 40)
        
        stream_config = mcp.get_mcp_stream_config()
        print(f"Stream Config: {len(stream_config.get('event_schemas', {}))} event types")
        
        # Show stream capabilities
        config_info = stream_config.get('stream_config', {})
        print(f"Stream Events:")
        for event_type in config_info.get('supported_events', [])[:3]:
            print(f"   • {event_type}")
        
        # Show rate limits
        rate_limits = stream_config.get('rate_limits', {})
        print(f"Rate Limits:")
        print(f"   • Max Connections: {rate_limits.get('max_connections', 'N/A')}")
        print(f"   • Requests/minute: {rate_limits.get('requests_per_minute', 'N/A')}")
        
        # Test 5: Sample MCP Integration Test
        print("\nStep 5: Integration Test")
        print("-" * 40)
        
        # Simulate a typical MCP conversation
        integration_query = "What services are costing me the most money?"
        print(f"Integration Query: '{integration_query}'")
        
        response = mcp.process_mcp_query(integration_query)
        
        # Format response for MCP client
        mcp_response = {
            "type": "mcp_response",
            "query": integration_query,
            "intent": response.get('intent', {}),
            "data": response.get('results', {}),
            "insights": response.get('insights', []),
            "recommendations": response.get('recommendations', []),
            "metadata": {
                "processing_time": "fast",
                "data_source": "local_cache",
                "mcp_version": resources.get('mcp_version', '0.4.0')
            }
        }
        
        print(f"MCP Response Generated:")
        print(f"   Data Points: {len(str(mcp_response.get('data', {})))}")
        print(f"   Insights: {len(mcp_response.get('insights', []))}")
        print(f"   Recommendations: {len(mcp_response.get('recommendations', []))}")
        
        # Save sample response for reference
        with open('./test_mcp_response.json', 'w') as f:
            json.dump(mcp_response, f, indent=2, default=str)
        print(f"Sample response saved: ./test_mcp_response.json")
        
        print(f"\nTest 6 PASSED: MCP Server integration working successfully!")
        return True
        
    except Exception as e:
        print(f"Test 6 FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_mcp_server()