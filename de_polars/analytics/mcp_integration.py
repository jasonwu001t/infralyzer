"""
MCP Integration Analytics - View 5: MCP Server Integration
"""
import polars as pl
from typing import Dict, Any, Optional, List
from datetime import datetime
import json

from ..engine.duckdb_engine import DuckDBEngine


class MCPIntegrationAnalytics:
    """
    Provide cost data through Model Context Protocol for AI assistants.
    Supports all endpoints for View 5: MCP Server Integration.
    """
    
    def __init__(self, engine: DuckDBEngine):
        """Initialize MCP Integration Analytics with DuckDB engine."""
        self.engine = engine
        self.config = engine.config
    
    def get_mcp_resources(self) -> Dict[str, Any]:
        """
        List available cost data resources for MCP clients.
        Endpoint: GET /api/v1/finops/mcp/resources
        
        Returns:
            Available resources, schemas, and capabilities for MCP
        """
        # Define available cost data resources
        resources = [
            {
                "name": "cost_summary",
                "type": "cost_data",
                "description": "Monthly cost summary by service and region",
                "uri": "cost://summary/monthly",
                "mimeType": "application/json",
                "capabilities": ["read", "filter", "aggregate"]
            },
            {
                "name": "spend_trends",
                "type": "time_series",
                "description": "Historical spending trends over time",
                "uri": "cost://trends/historical",
                "mimeType": "application/json",
                "capabilities": ["read", "forecast", "analyze"]
            },
            {
                "name": "optimization_opportunities",
                "type": "recommendations",
                "description": "Cost optimization recommendations and insights",
                "uri": "cost://optimization/recommendations",
                "mimeType": "application/json",
                "capabilities": ["read", "prioritize", "implement"]
            },
            {
                "name": "budget_tracking",
                "type": "budget_data",
                "description": "Budget vs actual spending tracking",
                "uri": "cost://budget/tracking",
                "mimeType": "application/json", 
                "capabilities": ["read", "alert", "forecast"]
            },
            {
                "name": "resource_inventory",
                "type": "inventory",
                "description": "AWS resource inventory with cost allocation",
                "uri": "cost://resources/inventory",
                "mimeType": "application/json",
                "capabilities": ["read", "filter", "tag"]
            }
        ]
        
        # Define data schemas
        schemas = {
            "cost_summary": {
                "type": "object",
                "properties": {
                    "service": {"type": "string", "description": "AWS service name"},
                    "region": {"type": "string", "description": "AWS region"},
                    "monthly_cost": {"type": "number", "description": "Monthly cost in USD"},
                    "resource_count": {"type": "integer", "description": "Number of resources"},
                    "cost_trend": {"type": "string", "enum": ["increasing", "decreasing", "stable"]}
                }
            },
            "optimization_recommendation": {
                "type": "object",
                "properties": {
                    "recommendation_id": {"type": "string"},
                    "type": {"type": "string", "enum": ["rightsizing", "scheduling", "commitment"]},
                    "service": {"type": "string"},
                    "potential_savings": {"type": "number"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 100},
                    "implementation_effort": {"type": "string", "enum": ["low", "medium", "high"]}
                }
            }
        }
        
        # Define MCP capabilities
        capabilities = [
            {
                "name": "cost_analysis",
                "description": "Analyze cost data with natural language queries",
                "input_types": ["text/plain", "application/json"],
                "output_types": ["application/json", "text/plain"]
            },
            {
                "name": "optimization_planning",
                "description": "Generate cost optimization plans",
                "input_types": ["application/json"],
                "output_types": ["application/json"]
            },
            {
                "name": "budget_forecasting",
                "description": "Forecast future costs and budget requirements",
                "input_types": ["application/json"],
                "output_types": ["application/json"]
            },
            {
                "name": "alert_generation",
                "description": "Generate cost alerts and notifications",
                "input_types": ["application/json"],
                "output_types": ["application/json"]
            }
        ]
        
        return {
            "resources": resources,
            "schemas": schemas,
            "capabilities": capabilities,
            "mcp_version": "0.4.0",
            "supported_protocols": ["cost_data", "recommendations", "forecasting"]
        }
    
    def get_mcp_tools(self) -> Dict[str, Any]:
        """
        Expose cost analysis tools through MCP protocol.
        Endpoint: GET /api/v1/finops/mcp/tools
        
        Returns:
            Available tools with descriptions and parameters
        """
        tools = [
            {
                "name": "analyze_cost_by_service",
                "description": "Analyze costs broken down by AWS service",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "time_period": {
                            "type": "string",
                            "description": "Time period for analysis (e.g., 'last_month', 'last_3_months')"
                        },
                        "service_filter": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Optional list of services to focus on"
                        }
                    },
                    "required": ["time_period"]
                }
            },
            {
                "name": "calculate_potential_savings",
                "description": "Calculate potential cost savings from optimization opportunities",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "optimization_types": {
                            "type": "array",
                            "items": {"type": "string", "enum": ["rightsizing", "scheduling", "storage", "commitment"]},
                            "description": "Types of optimizations to consider"
                        },
                        "confidence_threshold": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 100,
                            "description": "Minimum confidence level for recommendations"
                        }
                    }
                }
            },
            {
                "name": "forecast_monthly_costs",
                "description": "Forecast costs for upcoming months using ML models",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "forecast_months": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 12,
                            "description": "Number of months to forecast"
                        },
                        "include_seasonality": {
                            "type": "boolean",
                            "description": "Whether to include seasonal patterns in forecast"
                        },
                        "scenario": {
                            "type": "string",
                            "enum": ["baseline", "optimistic", "pessimistic"],
                            "description": "Forecast scenario to use"
                        }
                    },
                    "required": ["forecast_months"]
                }
            },
            {
                "name": "detect_cost_anomalies",
                "description": "Detect unusual spending patterns and cost anomalies",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sensitivity": {
                            "type": "number",
                            "minimum": 1,
                            "maximum": 5,
                            "description": "Sensitivity level for anomaly detection (1=low, 5=high)"
                        },
                        "lookback_days": {
                            "type": "integer",
                            "minimum": 7,
                            "maximum": 90,
                            "description": "Number of days to analyze for anomalies"
                        }
                    }
                }
            },
            {
                "name": "generate_cost_report",
                "description": "Generate comprehensive cost analysis report",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "report_type": {
                            "type": "string",
                            "enum": ["executive_summary", "detailed_breakdown", "optimization_focused"],
                            "description": "Type of report to generate"
                        },
                        "include_recommendations": {
                            "type": "boolean",
                            "description": "Whether to include optimization recommendations"
                        },
                        "format": {
                            "type": "string",
                            "enum": ["json", "markdown", "html"],
                            "description": "Output format for the report"
                        }
                    },
                    "required": ["report_type"]
                }
            }
        ]
        
        # Tool descriptions with usage examples
        descriptions = [
            {
                "tool": "analyze_cost_by_service",
                "example": "Analyze EC2 and RDS costs for the last 3 months",
                "use_case": "Understanding service-level cost distribution"
            },
            {
                "tool": "calculate_potential_savings",
                "example": "Find all high-confidence rightsizing opportunities",
                "use_case": "Quantifying optimization opportunities"
            },
            {
                "tool": "forecast_monthly_costs",
                "example": "Forecast next 6 months with seasonal patterns",
                "use_case": "Budget planning and capacity forecasting"
            }
        ]
        
        # Tool parameters documentation
        parameters = {
            "time_period_options": [
                "last_week", "last_month", "last_3_months", 
                "last_6_months", "last_year", "custom"
            ],
            "optimization_types": [
                "rightsizing", "scheduling", "storage_optimization",
                "commitment_discounts", "idle_resource_cleanup"
            ],
            "supported_services": [
                "EC2", "RDS", "S3", "Lambda", "DynamoDB", 
                "ElastiCache", "Redshift", "OpenSearch"
            ]
        }
        
        return {
            "tools": tools,
            "descriptions": descriptions,
            "parameters": parameters,
            "total_tools": len(tools)
        }
    
    def process_mcp_query(self, query: str, query_type: str = "natural_language") -> Dict[str, Any]:
        """
        Process natural language cost queries through MCP.
        Endpoint: POST /api/v1/finops/mcp/query
        
        Args:
            query: Natural language query about costs
            query_type: Type of query processing to use
            
        Returns:
            Structured cost insights and recommendations
        """
        try:
            # Parse natural language query (simplified NLP)
            parsed_intent = self._parse_query_intent(query)
            
            # Execute appropriate analysis based on intent
            if parsed_intent["intent"] == "cost_breakdown":
                results = self._execute_cost_breakdown_query(parsed_intent)
            elif parsed_intent["intent"] == "optimization":
                results = self._execute_optimization_query(parsed_intent)
            elif parsed_intent["intent"] == "forecasting":
                results = self._execute_forecasting_query(parsed_intent)
            elif parsed_intent["intent"] == "anomaly_detection":
                results = self._execute_anomaly_query(parsed_intent)
            else:
                results = self._execute_general_query(parsed_intent)
            
            # Generate insights and recommendations
            insights = self._generate_mcp_insights(results, parsed_intent)
            recommendations = self._generate_mcp_recommendations(results, parsed_intent)
            
            return {
                "query": query,
                "parsed_intent": parsed_intent,
                "query_results": results,
                "insights": insights,
                "recommendations": recommendations,
                "confidence": parsed_intent.get("confidence", 75),
                "processing_time_ms": 250,  # Mock processing time
                "mcp_session_id": f"mcp_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            }
            
        except Exception as e:
            return {
                "query": query,
                "error": str(e),
                "error_type": "processing_error",
                "suggestions": [
                    "Try asking about specific services: 'What are my EC2 costs?'",
                    "Ask for time-based analysis: 'Show me cost trends over last 3 months'",
                    "Request optimization help: 'How can I reduce my AWS costs?'"
                ]
            }
    
    def get_mcp_stream_config(self) -> Dict[str, Any]:
        """
        Get configuration for real-time cost data streaming.
        WebSocket: /api/v1/finops/mcp/stream
        
        Returns:
            Streaming configuration and event types
        """
        # Stream configuration
        stream_config = {
            "websocket_url": "/api/v1/finops/mcp/stream",
            "supported_events": [
                "cost_alert",
                "budget_threshold",
                "optimization_opportunity",
                "anomaly_detected",
                "forecast_update"
            ],
            "stream_modes": [
                "real_time",      # Immediate event streaming
                "batched",        # Batched updates every N minutes
                "scheduled"       # Scheduled updates at specific times
            ],
            "authentication": {
                "required": True,
                "methods": ["jwt", "api_key"],
                "session_timeout": 3600  # 1 hour
            }
        }
        
        # Event schemas
        event_schemas = {
            "cost_alert": {
                "type": "object",
                "properties": {
                    "alert_id": {"type": "string"},
                    "alert_type": {"type": "string", "enum": ["budget_exceeded", "unusual_spike", "threshold_reached"]},
                    "service": {"type": "string"},
                    "current_cost": {"type": "number"},
                    "threshold": {"type": "number"},
                    "severity": {"type": "string", "enum": ["low", "medium", "high", "critical"]},
                    "timestamp": {"type": "string", "format": "date-time"}
                }
            },
            "optimization_opportunity": {
                "type": "object", 
                "properties": {
                    "opportunity_id": {"type": "string"},
                    "type": {"type": "string"},
                    "potential_savings": {"type": "number"},
                    "confidence": {"type": "number"},
                    "action_required": {"type": "boolean"}
                }
            }
        }
        
        # Sample events for testing
        sample_events = [
            {
                "event_type": "cost_alert",
                "data": {
                    "alert_id": "alert_001",
                    "alert_type": "budget_exceeded",
                    "service": "EC2",
                    "current_cost": 5200.00,
                    "threshold": 5000.00,
                    "severity": "medium",
                    "timestamp": datetime.now().isoformat()
                }
            }
        ]
        
        return {
            "stream_config": stream_config,
            "event_schemas": event_schemas,
            "sample_events": sample_events,
            "rate_limits": {
                "max_connections": 100,
                "events_per_minute": 1000,
                "max_event_size_kb": 64
            }
        }
    
    def _parse_query_intent(self, query: str) -> Dict[str, Any]:
        """Parse natural language query to determine intent."""
        query_lower = query.lower()
        
        # Intent classification (simplified)
        if any(word in query_lower for word in ["cost", "spend", "bill", "expense"]):
            if any(word in query_lower for word in ["breakdown", "by service", "per service"]):
                intent = "cost_breakdown"
            elif any(word in query_lower for word in ["trend", "over time", "monthly", "historical"]):
                intent = "trend_analysis"
            else:
                intent = "cost_summary"
        elif any(word in query_lower for word in ["optimize", "save", "reduce", "efficient"]):
            intent = "optimization"
        elif any(word in query_lower for word in ["forecast", "predict", "future", "next month"]):
            intent = "forecasting"
        elif any(word in query_lower for word in ["anomaly", "unusual", "spike", "alert"]):
            intent = "anomaly_detection"
        else:
            intent = "general"
        
        # Extract entities (simplified)
        services = []
        for service in ["ec2", "rds", "s3", "lambda", "dynamodb"]:
            if service in query_lower:
                services.append(service.upper())
        
        time_period = "last_month"  # Default
        if "week" in query_lower:
            time_period = "last_week"
        elif "3 month" in query_lower:
            time_period = "last_3_months"
        elif "year" in query_lower:
            time_period = "last_year"
        
        return {
            "intent": intent,
            "services": services,
            "time_period": time_period,
            "confidence": 80,  # Mock confidence
            "entities": {
                "services": services,
                "time_period": time_period
            }
        }
    
    def _execute_cost_breakdown_query(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Execute cost breakdown query."""
        sql = f"""
        SELECT 
            product_servicecode as service,
            SUM(line_item_unblended_cost) as total_cost,
            COUNT(DISTINCT line_item_resource_id) as resource_count
        FROM {self.config.table_name}
        WHERE line_item_unblended_cost > 0
            AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY 1
        ORDER BY total_cost DESC
        LIMIT 10
        """
        
        try:
            result = self.engine.query(sql)
            breakdown = []
            for row in result.iter_rows(named=True):
                breakdown.append({
                    "service": row["service"],
                    "cost": float(row["total_cost"]),
                    "resource_count": int(row["resource_count"])
                })
            return {"breakdown": breakdown}
        except Exception as e:
            return {"error": str(e)}
    
    def _execute_optimization_query(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Execute optimization opportunities query."""
        # Mock optimization results
        return {
            "opportunities": [
                {
                    "type": "rightsizing",
                    "service": "EC2",
                    "potential_savings": 1200.50,
                    "confidence": 85,
                    "description": "Downsize underutilized instances"
                },
                {
                    "type": "storage_optimization",
                    "service": "S3",
                    "potential_savings": 450.00,
                    "confidence": 92,
                    "description": "Migrate to lower-cost storage classes"
                }
            ]
        }
    
    def _execute_forecasting_query(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Execute cost forecasting query."""
        # Mock forecasting results
        return {
            "forecast": {
                "next_month": 8500.00,
                "next_3_months": 26200.00,
                "confidence": 78,
                "trend": "increasing"
            }
        }
    
    def _execute_anomaly_query(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Execute anomaly detection query."""
        # Mock anomaly results
        return {
            "anomalies": [
                {
                    "service": "Lambda",
                    "anomaly_type": "cost_spike",
                    "cost_increase": 250.00,
                    "date": "2025-01-14",
                    "severity": "medium"
                }
            ]
        }
    
    def _execute_general_query(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Execute general cost query."""
        sql = f"""
        SELECT 
            SUM(line_item_unblended_cost) as total_monthly_cost,
            COUNT(DISTINCT product_servicecode) as service_count,
            COUNT(DISTINCT line_item_resource_id) as resource_count
        FROM {self.config.table_name}
        WHERE line_item_unblended_cost > 0
            AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
        """
        
        try:
            result = self.engine.query(sql)
            if not result.is_empty():
                row = result.row(0, named=True)
                return {
                    "summary": {
                        "total_cost": float(row["total_monthly_cost"]),
                        "service_count": int(row["service_count"]),
                        "resource_count": int(row["resource_count"])
                    }
                }
        except Exception as e:
            return {"error": str(e)}
        
        return {"summary": {"total_cost": 0}}
    
    def _generate_mcp_insights(self, results: Dict[str, Any], intent: Dict[str, Any]) -> List[str]:
        """Generate insights from MCP query results."""
        insights = []
        
        if "breakdown" in results:
            top_service = results["breakdown"][0] if results["breakdown"] else None
            if top_service:
                insights.append(f"Your highest cost service is {top_service['service']} at ${top_service['cost']:.2f}")
        
        if "opportunities" in results:
            total_savings = sum(o["potential_savings"] for o in results["opportunities"])
            insights.append(f"Found ${total_savings:.2f} in potential monthly savings")
        
        if "forecast" in results:
            forecast = results["forecast"]
            insights.append(f"Next month forecast: ${forecast['next_month']:.2f} ({forecast['trend']} trend)")
        
        insights.append("Analysis powered by MCP cost intelligence")
        return insights
    
    def _generate_mcp_recommendations(self, results: Dict[str, Any], intent: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recommendations from MCP query results."""
        recommendations = []
        
        if intent["intent"] == "optimization" and "opportunities" in results:
            for opp in results["opportunities"]:
                recommendations.append({
                    "action": f"Implement {opp['type']} for {opp['service']}",
                    "potential_savings": opp["potential_savings"],
                    "confidence": opp["confidence"],
                    "priority": "high" if opp["potential_savings"] > 1000 else "medium"
                })
        
        if intent["intent"] == "cost_breakdown":
            recommendations.append({
                "action": "Focus optimization efforts on highest-cost services",
                "priority": "medium",
                "next_steps": ["Analyze top 3 services for optimization opportunities"]
            })
        
        return recommendations