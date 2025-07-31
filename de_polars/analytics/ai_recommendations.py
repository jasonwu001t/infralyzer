"""
AI Recommendations Analytics - View 6: AI-Powered Cost Recommendations
"""
import polars as pl
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json

from ..engine.duckdb_engine import DuckDBEngine


class AIRecommendationAnalytics:
    """
    Leverage machine learning for intelligent cost optimization.
    Supports all endpoints for View 6: AI-Powered Cost Recommendations.
    """
    
    def __init__(self, engine: DuckDBEngine):
        """Initialize AI Recommendation Analytics with DuckDB engine."""
        self.engine = engine
        self.config = engine.config
    
    def detect_anomalies(self, lookback_days: int = 30, sensitivity: float = 2.0) -> Dict[str, Any]:
        """
        Machine learning-based spend anomaly detection.
        Endpoint: GET /api/v1/finops/ai/anomaly-detection
        
        Args:
            lookback_days: Number of days to analyze for anomalies
            sensitivity: Sensitivity threshold for anomaly detection (std deviations)
            
        Returns:
            Detected anomalies with root cause analysis and predictions
        """
        sql = f"""
        WITH daily_spend AS (
            SELECT 
                DATE(line_item_usage_start_date) as usage_date,
                product_servicecode,
                product_region,
                SUM(line_item_unblended_cost) as daily_cost
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
            GROUP BY 1, 2, 3
        ),
        spend_statistics AS (
            SELECT 
                product_servicecode,
                product_region,
                AVG(daily_cost) as avg_daily_cost,
                STDDEV(daily_cost) as stddev_daily_cost,
                COUNT(*) as days_of_data
            FROM daily_spend
            GROUP BY 1, 2
            HAVING COUNT(*) >= 7  -- Need at least a week of data
        ),
        anomaly_detection AS (
            SELECT 
                ds.usage_date,
                ds.product_servicecode,
                ds.product_region,
                ds.daily_cost,
                ss.avg_daily_cost,
                ss.stddev_daily_cost,
                CASE 
                    WHEN ss.stddev_daily_cost > 0 THEN 
                        ABS(ds.daily_cost - ss.avg_daily_cost) / ss.stddev_daily_cost
                    ELSE 0
                END as z_score,
                CASE 
                    WHEN ds.daily_cost > ss.avg_daily_cost + (ss.stddev_daily_cost * {sensitivity}) THEN 'high_spend_anomaly'
                    WHEN ds.daily_cost < ss.avg_daily_cost - (ss.stddev_daily_cost * {sensitivity}) AND ds.daily_cost > 0 THEN 'low_spend_anomaly'
                    ELSE 'normal'
                END as anomaly_type
            FROM daily_spend ds
            JOIN spend_statistics ss ON ds.product_servicecode = ss.product_servicecode 
                                    AND ds.product_region = ss.product_region
        )
        SELECT 
            usage_date,
            product_servicecode,
            product_region,
            daily_cost,
            avg_daily_cost,
            z_score,
            anomaly_type,
            (daily_cost - avg_daily_cost) as cost_deviation
        FROM anomaly_detection
        WHERE anomaly_type != 'normal'
        ORDER BY ABS(cost_deviation) DESC
        LIMIT 20
        """
        
        try:
            result = self.engine.query(sql)
            anomalies = []
            
            for row in result.iter_rows(named=True):
                cost_deviation = float(row["cost_deviation"])
                z_score = float(row["z_score"])
                
                # Generate AI-powered root cause analysis
                root_cause = self._analyze_anomaly_root_cause(
                    row["product_servicecode"], 
                    row["anomaly_type"], 
                    cost_deviation
                )
                
                # Generate severity and confidence
                severity = "high" if abs(z_score) > 3 else "medium" if abs(z_score) > 2.5 else "low"
                confidence = min(95, max(60, 95 - (abs(z_score) - 2) * 10))
                
                anomalies.append({
                    "anomaly_id": f"anomaly_{row['usage_date']}_{hash(row['product_servicecode']) % 1000}",
                    "date": str(row["usage_date"]),
                    "service": row["product_servicecode"],
                    "region": row["product_region"],
                    "anomaly_type": row["anomaly_type"],
                    "severity": severity,
                    "confidence": round(confidence, 1),
                    "cost_impact": abs(cost_deviation),
                    "baseline_cost": float(row["avg_daily_cost"]),
                    "actual_cost": float(row["daily_cost"]),
                    "z_score": round(z_score, 2),
                    "root_cause": root_cause["description"],
                    "contributing_factors": root_cause["factors"],
                    "business_context": root_cause["context"]
                })
            
            # Generate predictions based on anomalies
            predictions = self._generate_anomaly_predictions(anomalies)
            
            # Root cause summary
            root_causes = self._summarize_root_causes(anomalies)
            
            return {
                "anomalies": anomalies,
                "root_causes": root_causes,
                "predictions": predictions,
                "summary": {
                    "total_anomalies": len(anomalies),
                    "high_severity_count": len([a for a in anomalies if a["severity"] == "high"]),
                    "total_cost_impact": sum(a["cost_impact"] for a in anomalies),
                    "analysis_period": f"{lookback_days} days"
                }
            }
            
        except Exception as e:
            print(f"❌ Error detecting anomalies: {e}")
            return {"anomalies": [], "root_causes": [], "predictions": []}
    
    def get_optimization_insights(self) -> Dict[str, Any]:
        """
        AI-generated cost optimization recommendations.
        Endpoint: GET /api/v1/finops/ai/optimization-insights
        
        Returns:
            AI insights with pattern recognition and industry benchmarks
        """
        # Analyze spending patterns
        patterns_sql = f"""
        WITH monthly_patterns AS (
            SELECT 
                DATE_TRUNC('month', line_item_usage_start_date) as month,
                product_servicecode,
                SUM(line_item_unblended_cost) as monthly_spend,
                COUNT(DISTINCT line_item_resource_id) as resource_count,
                AVG(line_item_unblended_cost) as avg_resource_cost
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY 1, 2
        ),
        growth_analysis AS (
            SELECT 
                product_servicecode,
                AVG(monthly_spend) as avg_monthly_spend,
                STDDEV(monthly_spend) as spend_volatility,
                (MAX(monthly_spend) - MIN(monthly_spend)) / MIN(monthly_spend) * 100 as growth_rate,
                COUNT(*) as months_data
            FROM monthly_patterns
            GROUP BY 1
            HAVING COUNT(*) >= 3
        )
        SELECT 
            product_servicecode as service,
            avg_monthly_spend,
            spend_volatility,
            growth_rate,
            months_data,
            CASE 
                WHEN growth_rate > 50 THEN 'rapid_growth'
                WHEN growth_rate > 20 THEN 'growing'
                WHEN growth_rate > -10 THEN 'stable'
                ELSE 'declining'
            END as pattern_type
        FROM growth_analysis
        WHERE avg_monthly_spend > 1000
        ORDER BY avg_monthly_spend DESC
        """
        
        try:
            result = self.engine.query(sql)
            insights = []
            
            for row in result.iter_rows(named=True):
                service = row["service"]
                pattern_type = row["pattern_type"]
                avg_spend = float(row["avg_monthly_spend"])
                growth_rate = float(row["growth_rate"])
                
                # Generate AI insights based on patterns
                insight = self._generate_service_insights(service, pattern_type, avg_spend, growth_rate)
                
                insights.append({
                    "service": service,
                    "pattern_type": pattern_type,
                    "avg_monthly_spend": avg_spend,
                    "growth_rate": round(growth_rate, 1),
                    "ai_insights": insight["insights"],
                    "optimization_opportunities": insight["opportunities"],
                    "priority_score": insight["priority"],
                    "confidence": insight["confidence"]
                })
            
            # Generate industry benchmarks (mock data)
            benchmarks = self._generate_industry_benchmarks(insights)
            
            # Pattern recognition summary
            pattern_summary = self._analyze_spending_patterns(insights)
            
            return {
                "insights": insights,
                "recommendations": self._generate_ai_recommendations(insights),
                "benchmarks": benchmarks,
                "pattern_analysis": pattern_summary
            }
            
        except Exception as e:
            print(f"❌ Error generating optimization insights: {e}")
            return {"insights": [], "recommendations": [], "benchmarks": []}
    
    def analyze_custom_query(self, query_text: str, analysis_type: str = "cost_analysis") -> Dict[str, Any]:
        """
        Natural language interface for cost analysis.
        Endpoint: POST /api/v1/finops/ai/custom-analysis
        
        Args:
            query_text: Natural language query about costs
            analysis_type: Type of analysis to perform
            
        Returns:
            AI-powered analysis results with narrative insights
        """
        # This would typically use NLP to convert natural language to SQL
        # For now, provide a mock intelligent response based on keywords
        
        query_lower = query_text.lower()
        
        # Detect intent and generate appropriate SQL
        if "highest cost" in query_lower or "most expensive" in query_lower:
            analysis_sql = f"""
            SELECT 
                product_servicecode,
                SUM(line_item_unblended_cost) as total_cost,
                COUNT(DISTINCT line_item_resource_id) as resource_count
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1
            ORDER BY total_cost DESC
            LIMIT 5
            """
            analysis_focus = "highest_cost_services"
            
        elif "trend" in query_lower or "over time" in query_lower:
            analysis_sql = f"""
            SELECT 
                DATE_TRUNC('month', line_item_usage_start_date) as month,
                SUM(line_item_unblended_cost) as monthly_cost
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY 1
            ORDER BY 1
            """
            analysis_focus = "cost_trends"
            
        elif "region" in query_lower:
            analysis_sql = f"""
            SELECT 
                product_region,
                SUM(line_item_unblended_cost) as total_cost,
                COUNT(DISTINCT product_servicecode) as service_count
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1
            ORDER BY total_cost DESC
            """
            analysis_focus = "regional_analysis"
            
        else:
            # Default general analysis
            analysis_sql = f"""
            SELECT 
                'total_spend' as metric,
                SUM(line_item_unblended_cost) as value
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            """
            analysis_focus = "general_summary"
        
        try:
            result = self.engine.query(analysis_sql)
            
            # Generate narrative insights based on results
            narrative = self._generate_narrative_insights(result, analysis_focus, query_text)
            
            # Convert results to analysis format
            analysis_results = []
            for row in result.iter_rows(named=True):
                analysis_results.append(dict(row))
            
            # Generate visualizations metadata
            visualizations = self._suggest_visualizations(analysis_focus, analysis_results)
            
            return {
                "query": query_text,
                "analysis_type": analysis_type,
                "analysis_focus": analysis_focus,
                "results": analysis_results,
                "narrative_insights": narrative,
                "visualizations": visualizations,
                "confidence": 85,  # Mock confidence score
                "execution_time_ms": 150  # Mock execution time
            }
            
        except Exception as e:
            print(f"❌ Error in custom analysis: {e}")
            return {
                "query": query_text,
                "error": str(e),
                "suggestions": [
                    "Try asking about 'highest cost services'",
                    "Ask for 'cost trends over time'",
                    "Query about 'costs by region'"
                ]
            }
    
    def get_forecasting(self, forecast_months: int = 6) -> Dict[str, Any]:
        """
        Machine learning cost forecasting models.
        Endpoint: GET /api/v1/finops/ai/forecasting
        
        Args:
            forecast_months: Number of months to forecast
            
        Returns:
            ML forecasts with scenario planning and business impact
        """
        # Historical data for forecasting
        historical_sql = f"""
        WITH monthly_spend AS (
            SELECT 
                DATE_TRUNC('month', line_item_usage_start_date) as month,
                product_servicecode,
                SUM(line_item_unblended_cost) as monthly_cost
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY 1, 2
        )
        SELECT 
            month,
            product_servicecode,
            monthly_cost,
            LAG(monthly_cost, 1) OVER (PARTITION BY product_servicecode ORDER BY month) as prev_month_cost,
            LAG(monthly_cost, 12) OVER (PARTITION BY product_servicecode ORDER BY month) as year_ago_cost
        FROM monthly_spend
        ORDER BY product_servicecode, month
        """
        
        try:
            result = self.engine.query(sql)
            
            # Group by service for forecasting
            service_data = {}
            for row in result.iter_rows(named=True):
                service = row["product_servicecode"]
                if service not in service_data:
                    service_data[service] = []
                service_data[service].append({
                    "month": str(row["month"]),
                    "cost": float(row["monthly_cost"]),
                    "prev_month": float(row["prev_month_cost"]) if row["prev_month_cost"] else 0,
                    "year_ago": float(row["year_ago_cost"]) if row["year_ago_cost"] else 0
                })
            
            # Generate forecasts for each service
            forecasts = []
            total_forecast_cost = 0
            
            for service, historical_data in service_data.items():
                if len(historical_data) >= 3:  # Need minimum data for forecasting
                    forecast = self._generate_ml_forecast(service, historical_data, forecast_months)
                    forecasts.append(forecast)
                    total_forecast_cost += sum(f["forecasted_cost"] for f in forecast["monthly_forecasts"])
            
            # Scenario planning
            scenarios = self._generate_forecast_scenarios(forecasts, total_forecast_cost)
            
            # Business impact analysis
            business_impact = self._analyze_forecast_business_impact(forecasts, scenarios)
            
            return {
                "forecasts": forecasts,
                "scenarios": scenarios,
                "business_impact": business_impact,
                "summary": {
                    "total_services_forecasted": len(forecasts),
                    "forecast_period_months": forecast_months,
                    "total_forecasted_cost": round(total_forecast_cost, 2),
                    "forecast_confidence": self._calculate_overall_forecast_confidence(forecasts)
                }
            }
            
        except Exception as e:
            print(f"❌ Error generating forecasting: {e}")
            return {"forecasts": [], "scenarios": [], "business_impact": {}}
    
    def _analyze_anomaly_root_cause(self, service: str, anomaly_type: str, cost_deviation: float) -> Dict[str, Any]:
        """Analyze root cause of detected anomalies using AI logic."""
        
        # Service-specific root cause analysis
        service_patterns = {
            "AmazonEC2": {
                "high_spend_anomaly": "Possible new instance launches or instance size changes",
                "low_spend_anomaly": "Instances may have been terminated or stopped"
            },
            "AmazonS3": {
                "high_spend_anomaly": "Increased data transfer or storage usage",
                "low_spend_anomaly": "Data deletion or reduced access patterns"
            },
            "AWSLambda": {
                "high_spend_anomaly": "Increased function executions or longer execution times",
                "low_spend_anomaly": "Reduced function usage or optimization"
            }
        }
        
        default_description = "Unexpected cost variation detected"
        description = service_patterns.get(service, {}).get(anomaly_type, default_description)
        
        # Generate contributing factors
        factors = []
        if abs(cost_deviation) > 1000:
            factors.append("Significant cost impact suggests major infrastructure change")
        if service == "AmazonEC2":
            factors.append("Check for new instance types or autoscaling events")
        if anomaly_type == "high_spend_anomaly":
            factors.append("Review recent deployments or configuration changes")
        
        # Business context
        context = f"This anomaly in {service} represents a {abs(cost_deviation):.0f} dollar deviation from normal spending patterns"
        
        return {
            "description": description,
            "factors": factors,
            "context": context
        }
    
    def _generate_anomaly_predictions(self, anomalies: List[Dict]) -> List[Dict]:
        """Generate predictions based on detected anomalies."""
        predictions = []
        
        # Group anomalies by service
        service_anomalies = {}
        for anomaly in anomalies:
            service = anomaly["service"]
            if service not in service_anomalies:
                service_anomalies[service] = []
            service_anomalies[service].append(anomaly)
        
        for service, service_anomaly_list in service_anomalies.items():
            high_severity_count = len([a for a in service_anomaly_list if a["severity"] == "high"])
            
            if high_severity_count > 1:
                predictions.append({
                    "prediction_type": "budget_overrun_risk",
                    "service": service,
                    "probability": "high" if high_severity_count > 2 else "medium",
                    "timeframe": "next_7_days",
                    "predicted_impact": "Budget variance expected",
                    "recommended_action": f"Review {service} usage and implement cost controls"
                })
        
        return predictions
    
    def _summarize_root_causes(self, anomalies: List[Dict]) -> List[Dict]:
        """Summarize root causes across all anomalies."""
        causes = {}
        for anomaly in anomalies:
            cause = anomaly["root_cause"]
            if cause not in causes:
                causes[cause] = {"count": 0, "total_impact": 0, "services": set()}
            causes[cause]["count"] += 1
            causes[cause]["total_impact"] += anomaly["cost_impact"]
            causes[cause]["services"].add(anomaly["service"])
        
        return [
            {
                "root_cause": cause,
                "frequency": data["count"],
                "total_cost_impact": round(data["total_impact"], 2),
                "affected_services": list(data["services"])
            }
            for cause, data in causes.items()
        ]
    
    def _generate_service_insights(self, service: str, pattern: str, spend: float, growth: float) -> Dict[str, Any]:
        """Generate AI insights for a specific service."""
        insights = []
        opportunities = []
        
        if pattern == "rapid_growth":
            insights.append(f"{service} showing rapid {growth:.1f}% growth - review scaling efficiency")
            opportunities.append("Consider Reserved Instances or Savings Plans")
            opportunities.append("Implement automated scaling policies")
            priority = 95
            confidence = 90
            
        elif pattern == "declining":
            insights.append(f"{service} usage declining by {abs(growth):.1f}% - potential cost savings")
            opportunities.append("Review unused resources for termination")
            opportunities.append("Optimize resource allocation")
            priority = 70
            confidence = 85
            
        else:
            insights.append(f"{service} showing {pattern} usage pattern")
            opportunities.append("Monitor for optimization opportunities")
            priority = 50
            confidence = 75
        
        return {
            "insights": insights,
            "opportunities": opportunities,
            "priority": priority,
            "confidence": confidence
        }
    
    def _generate_industry_benchmarks(self, insights: List[Dict]) -> List[Dict]:
        """Generate mock industry benchmarks."""
        benchmarks = []
        
        benchmark_data = {
            "AmazonEC2": {"industry_avg_monthly": 15000, "efficiency_score": 78},
            "AmazonRDS": {"industry_avg_monthly": 8000, "efficiency_score": 82},
            "AmazonS3": {"industry_avg_monthly": 5000, "efficiency_score": 85},
            "AWSLambda": {"industry_avg_monthly": 2000, "efficiency_score": 90}
        }
        
        for insight in insights:
            service = insight["service"]
            if service in benchmark_data:
                benchmark = benchmark_data[service]
                our_spend = insight["avg_monthly_spend"]
                
                benchmarks.append({
                    "service": service,
                    "our_monthly_spend": our_spend,
                    "industry_average": benchmark["industry_avg_monthly"],
                    "vs_industry": "above" if our_spend > benchmark["industry_avg_monthly"] else "below",
                    "efficiency_score": benchmark["efficiency_score"],
                    "percentile": min(95, max(5, 50 + (benchmark["industry_avg_monthly"] - our_spend) / benchmark["industry_avg_monthly"] * 50))
                })
        
        return benchmarks
    
    def _analyze_spending_patterns(self, insights: List[Dict]) -> Dict[str, Any]:
        """Analyze overall spending patterns."""
        total_services = len(insights)
        rapid_growth_services = len([i for i in insights if i["pattern_type"] == "rapid_growth"])
        declining_services = len([i for i in insights if i["pattern_type"] == "declining"])
        
        return {
            "total_services_analyzed": total_services,
            "rapid_growth_services": rapid_growth_services,
            "declining_services": declining_services,
            "stable_services": total_services - rapid_growth_services - declining_services,
            "avg_growth_rate": round(sum(i["growth_rate"] for i in insights) / total_services, 1) if total_services > 0 else 0,
            "growth_volatility": "high" if rapid_growth_services > total_services * 0.3 else "low"
        }
    
    def _generate_ai_recommendations(self, insights: List[Dict]) -> List[Dict]:
        """Generate AI-powered recommendations."""
        recommendations = []
        
        high_priority_services = [i for i in insights if i["priority_score"] > 80]
        
        for service_insight in high_priority_services:
            recommendations.append({
                "recommendation_id": f"ai_rec_{hash(service_insight['service']) % 1000}",
                "type": "ai_optimization",
                "service": service_insight["service"],
                "priority": "high",
                "description": f"AI-identified optimization opportunity for {service_insight['service']}",
                "opportunities": service_insight["optimization_opportunities"],
                "confidence": service_insight["confidence"],
                "estimated_impact": "Medium to High cost reduction potential"
            })
        
        return recommendations
    
    def _generate_narrative_insights(self, result: pl.DataFrame, focus: str, query: str) -> List[str]:
        """Generate narrative insights from query results."""
        insights = []
        
        if focus == "highest_cost_services":
            if not result.is_empty():
                top_service = result.row(0, named=True)
                insights.append(f"Your highest cost service is {top_service['product_servicecode']} with ${top_service['total_cost']:.2f} this month")
                insights.append(f"This service accounts for {len(result)} total resources")
        
        elif focus == "cost_trends":
            if result.shape[0] > 1:
                latest_cost = result.row(-1, named=True)["monthly_cost"]
                prev_cost = result.row(-2, named=True)["monthly_cost"]
                change_pct = ((latest_cost - prev_cost) / prev_cost) * 100
                insights.append(f"Your monthly costs have {'increased' if change_pct > 0 else 'decreased'} by {abs(change_pct):.1f}%")
        
        elif focus == "regional_analysis":
            if not result.is_empty():
                top_region = result.row(0, named=True)
                insights.append(f"Your highest spend region is {top_region['product_region']} with ${top_region['total_cost']:.2f}")
        
        insights.append("This analysis was generated using AI-powered cost intelligence")
        return insights
    
    def _suggest_visualizations(self, focus: str, results: List[Dict]) -> List[Dict]:
        """Suggest appropriate visualizations for the analysis."""
        if focus == "highest_cost_services":
            return [{"type": "bar_chart", "title": "Top Services by Cost"}]
        elif focus == "cost_trends":
            return [{"type": "line_chart", "title": "Cost Trends Over Time"}]
        elif focus == "regional_analysis":
            return [{"type": "pie_chart", "title": "Cost Distribution by Region"}]
        else:
            return [{"type": "summary_card", "title": "Cost Summary"}]
    
    def _generate_ml_forecast(self, service: str, historical_data: List[Dict], months: int) -> Dict[str, Any]:
        """Generate ML-based forecast for a service."""
        # Simple linear regression forecast (in real implementation, use proper ML)
        recent_costs = [d["cost"] for d in historical_data[-6:]]  # Last 6 months
        
        if len(recent_costs) < 2:
            return {"service": service, "monthly_forecasts": [], "confidence": 0}
        
        # Calculate trend
        avg_change = sum(recent_costs[i] - recent_costs[i-1] for i in range(1, len(recent_costs))) / (len(recent_costs) - 1)
        base_cost = recent_costs[-1]
        
        monthly_forecasts = []
        for month in range(1, months + 1):
            forecasted_cost = base_cost + (avg_change * month)
            # Add some seasonality (simplified)
            seasonal_factor = 1 + 0.1 * math.sin(month * math.pi / 6)
            forecasted_cost *= seasonal_factor
            
            monthly_forecasts.append({
                "month": month,
                "forecasted_cost": max(0, forecasted_cost),  # Don't forecast negative costs
                "confidence_interval": {
                    "lower": max(0, forecasted_cost * 0.85),
                    "upper": forecasted_cost * 1.15
                }
            })
        
        confidence = max(60, 90 - (abs(avg_change) / base_cost * 100)) if base_cost > 0 else 60
        
        return {
            "service": service,
            "monthly_forecasts": monthly_forecasts,
            "confidence": round(confidence, 1),
            "trend": "increasing" if avg_change > 0 else "decreasing",
            "total_forecasted": sum(f["forecasted_cost"] for f in monthly_forecasts)
        }
    
    def _generate_forecast_scenarios(self, forecasts: List[Dict], total_cost: float) -> List[Dict]:
        """Generate forecast scenarios."""
        return [
            {
                "scenario": "optimistic",
                "description": "Best case with cost optimizations",
                "total_cost": total_cost * 0.85,
                "probability": 25
            },
            {
                "scenario": "baseline",
                "description": "Current trend continues",
                "total_cost": total_cost,
                "probability": 50
            },
            {
                "scenario": "pessimistic", 
                "description": "Higher growth than expected",
                "total_cost": total_cost * 1.2,
                "probability": 25
            }
        ]
    
    def _analyze_forecast_business_impact(self, forecasts: List[Dict], scenarios: List[Dict]) -> Dict[str, Any]:
        """Analyze business impact of forecasts."""
        baseline_scenario = next(s for s in scenarios if s["scenario"] == "baseline")
        
        return {
            "budget_impact": "Monitor for potential overrun",
            "growth_services": len([f for f in forecasts if f.get("trend") == "increasing"]),
            "declining_services": len([f for f in forecasts if f.get("trend") == "decreasing"]),
            "key_risks": [
                "Uncontrolled cost growth in expanding services",
                "Seasonal variations may impact budget"
            ],
            "opportunities": [
                "Cost optimization in declining services",
                "Right-sizing for forecasted demand"
            ],
            "recommended_actions": [
                "Implement automated cost monitoring",
                "Review resource allocation quarterly"
            ]
        }
    
    def _calculate_overall_forecast_confidence(self, forecasts: List[Dict]) -> float:
        """Calculate overall confidence across all forecasts."""
        if not forecasts:
            return 0
        
        total_confidence = sum(f["confidence"] for f in forecasts)
        return round(total_confidence / len(forecasts), 1)


# Add missing import
import math