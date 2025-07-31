"""
Discount Analytics - View 4: Private Discount Tracking & Negotiation
"""
import polars as pl
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json

from ..engine.duckdb_engine import DuckDBEngine


class DiscountAnalytics:
    """
    Track enterprise discounts and identify negotiation opportunities.
    Supports all endpoints for View 4: Private Discount Tracking & Negotiation.
    """
    
    def __init__(self, engine: DuckDBEngine):
        """Initialize Discount Analytics with DuckDB engine."""
        self.engine = engine
        self.config = engine.config
    
    def get_current_agreements(self) -> Dict[str, Any]:
        """
        Track Reserved Instances, Savings Plans, EDP discounts.
        Endpoint: GET /api/v1/finops/discounts/current-agreements
        
        Returns:
            Current discount agreements with utilization and renewal info
        """
        # Query for Reserved Instances and Savings Plans
        sql = f"""
        WITH discount_usage AS (
            SELECT 
                product_servicecode,
                pricing_term,
                reservation_arn,
                CASE 
                    WHEN line_item_line_item_type LIKE '%Reserved%' THEN 'Reserved Instance'
                    WHEN line_item_line_item_type LIKE '%SavingsPlans%' THEN 'Savings Plan'
                    WHEN line_item_line_item_type LIKE '%Spot%' THEN 'Spot Instance'
                    ELSE 'On-Demand'
                END as pricing_model,
                SUM(line_item_unblended_cost) as total_cost,
                SUM(line_item_usage_amount) as total_usage,
                COUNT(DISTINCT line_item_resource_id) as resource_count
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1, 2, 3, 4
        ),
        commitment_summary AS (
            SELECT 
                pricing_model,
                product_servicecode,
                SUM(total_cost) as monthly_cost,
                SUM(total_usage) as monthly_usage,
                SUM(resource_count) as total_resources
            FROM discount_usage
            WHERE pricing_model IN ('Reserved Instance', 'Savings Plan')
            GROUP BY 1, 2
        )
        SELECT 
            pricing_model as agreement_type,
            product_servicecode as service,
            monthly_cost,
            monthly_usage,
            total_resources,
            monthly_cost * 12 as annual_commitment
        FROM commitment_summary
        ORDER BY monthly_cost DESC
        """
        
        try:
            result = self.engine.query(sql)
            agreements = []
            total_annual_commitment = 0
            
            for row in result.iter_rows(named=True):
                annual_cost = float(row["annual_commitment"])
                total_annual_commitment += annual_cost
                
                # Mock utilization data (would come from CloudWatch or Cost Explorer)
                utilization_rate = min(95, max(65, 80 + (hash(row["service"]) % 30)))
                
                agreements.append({
                    "agreement_id": f"{row['agreement_type']}-{row['service']}-{hash(row['service']) % 1000}",
                    "type": row["agreement_type"],
                    "service": row["service"],
                    "monthly_cost": float(row["monthly_cost"]),
                    "annual_commitment": annual_cost,
                    "utilization_rate": utilization_rate,
                    "coverage_percentage": min(100, utilization_rate + 10),
                    "expiration_date": self._calculate_mock_expiration(),
                    "renewal_recommendation": "review" if utilization_rate < 75 else "renew",
                    "term_length": "1_year"  # Mock term
                })
            
            # Calculate overall utilization
            total_agreements = len(agreements)
            avg_utilization = sum(a["utilization_rate"] for a in agreements) / total_agreements if total_agreements > 0 else 0
            
            utilization_summary = {
                "average_utilization": round(avg_utilization, 1),
                "total_annual_commitment": round(total_annual_commitment, 2),
                "agreements_count": total_agreements,
                "underutilized_agreements": len([a for a in agreements if a["utilization_rate"] < 75])
            }
            
            # Mock renewal timeline
            renewals = self._generate_renewal_timeline(agreements)
            
            return {
                "agreements": agreements,
                "utilization": [utilization_summary],
                "renewals": renewals,
                "summary": {
                    "total_commitments": total_agreements,
                    "annual_value": round(total_annual_commitment, 2),
                    "avg_utilization": round(avg_utilization, 1)
                }
            }
            
        except Exception as e:
            print(f"❌ Error getting current agreements: {e}")
            return {"agreements": [], "utilization": [], "renewals": []}
    
    def get_negotiation_opportunities(self) -> Dict[str, Any]:
        """
        Identify services eligible for volume discounts.
        Endpoint: GET /api/v1/finops/discounts/negotiation-opportunities
        
        Returns:
            Negotiation opportunities with savings potential and benchmarks
        """
        sql = f"""
        WITH service_spend AS (
            SELECT 
                product_servicecode as service,
                SUM(line_item_unblended_cost) as annual_spend,
                COUNT(DISTINCT linked_account_id) as account_count,
                COUNT(DISTINCT line_item_resource_id) as resource_count,
                AVG(line_item_unblended_cost) as avg_resource_cost
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY 1
        ),
        high_spend_services AS (
            SELECT 
                service,
                annual_spend,
                account_count,
                resource_count,
                CASE 
                    WHEN annual_spend > 500000 THEN 20  -- 20% discount potential
                    WHEN annual_spend > 100000 THEN 15  -- 15% discount potential
                    WHEN annual_spend > 50000 THEN 10   -- 10% discount potential
                    ELSE 5                              -- 5% discount potential
                END as potential_discount_pct
            FROM service_spend
            WHERE annual_spend > 10000  -- Focus on significant spend
        )
        SELECT 
            service,
            annual_spend as current_spend,
            potential_discount_pct,
            (annual_spend * potential_discount_pct / 100) as estimated_savings,
            CASE 
                WHEN annual_spend > 500000 THEN 'high'
                WHEN annual_spend > 100000 THEN 'medium'
                ELSE 'low'
            END as negotiation_priority,
            account_count,
            resource_count
        FROM high_spend_services
        ORDER BY estimated_savings DESC
        """
        
        try:
            result = self.engine.query(sql)
            opportunities = []
            total_savings_potential = 0
            
            # Mock market benchmarks
            market_benchmarks = {
                "AmazonEC2": {"average_discount": 12, "top_quartile": 18},
                "AmazonRDS": {"average_discount": 10, "top_quartile": 15},
                "AmazonS3": {"average_discount": 8, "top_quartile": 12},
                "AWSLambda": {"average_discount": 5, "top_quartile": 8}
            }
            
            for row in result.iter_rows(named=True):
                current_spend = float(row["current_spend"])
                potential_discount = float(row["potential_discount_pct"])
                estimated_savings = float(row["estimated_savings"])
                total_savings_potential += estimated_savings
                
                service = row["service"]
                benchmark = market_benchmarks.get(service, {"average_discount": 8, "top_quartile": 12})
                
                opportunities.append({
                    "service": service,
                    "current_spend": current_spend,
                    "potential_discount": potential_discount,
                    "estimated_savings": estimated_savings,
                    "commitment_required": self._determine_commitment_requirement(current_spend),
                    "negotiation_priority": row["negotiation_priority"],
                    "market_benchmark": benchmark,
                    "account_coverage": int(row["account_count"]),
                    "resource_scale": int(row["resource_count"]),
                    "recommendation": self._generate_negotiation_recommendation(current_spend, potential_discount)
                })
            
            # Calculate market rate comparison
            market_rates = []
            for service, benchmark in market_benchmarks.items():
                market_rates.append({
                    "service": service,
                    "industry_average": benchmark["average_discount"],
                    "top_tier_discount": benchmark["top_quartile"],
                    "our_potential": next((o["potential_discount"] for o in opportunities if o["service"] == service), 0)
                })
            
            return {
                "opportunities": opportunities,
                "savings_potential": round(total_savings_potential, 2),
                "market_rates": market_rates,
                "negotiation_summary": {
                    "high_priority_services": len([o for o in opportunities if o["negotiation_priority"] == "high"]),
                    "total_annual_spend": sum(o["current_spend"] for o in opportunities),
                    "potential_annual_savings": round(total_savings_potential, 2)
                }
            }
            
        except Exception as e:
            print(f"❌ Error identifying negotiation opportunities: {e}")
            return {"opportunities": [], "savings_potential": 0, "market_rates": []}
    
    def get_usage_forecasting(self, forecast_months: int = 12) -> Dict[str, Any]:
        """
        Predict future usage patterns for commitment planning.
        Endpoint: GET /api/v1/finops/discounts/usage-forecasting
        
        Args:
            forecast_months: Number of months to forecast
            
        Returns:
            Usage forecasts with risk analysis and commitment recommendations
        """
        sql = f"""
        WITH monthly_usage AS (
            SELECT 
                DATE_TRUNC('month', line_item_usage_start_date) as month,
                product_servicecode,
                product_instance_type,
                SUM(line_item_usage_amount) as monthly_usage,
                SUM(line_item_unblended_cost) as monthly_cost
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY 1, 2, 3
        ),
        usage_trends AS (
            SELECT 
                product_servicecode,
                product_instance_type,
                AVG(monthly_usage) as avg_monthly_usage,
                STDDEV(monthly_usage) as usage_stddev,
                AVG(monthly_cost) as avg_monthly_cost,
                COUNT(*) as months_of_data
            FROM monthly_usage
            GROUP BY 1, 2
            HAVING COUNT(*) >= 3  -- Need at least 3 months of data
        )
        SELECT 
            product_servicecode as service,
            product_instance_type as instance_type,
            avg_monthly_usage,
            usage_stddev,
            avg_monthly_cost,
            months_of_data,
            CASE 
                WHEN usage_stddev / avg_monthly_usage < 0.2 THEN 'stable'
                WHEN usage_stddev / avg_monthly_usage < 0.5 THEN 'moderate'
                ELSE 'volatile'
            END as usage_pattern
        FROM usage_trends
        WHERE avg_monthly_usage > 0
        ORDER BY avg_monthly_cost DESC
        LIMIT 20
        """
        
        try:
            result = self.engine.query(sql)
            forecasts = []
            recommendations = []
            
            for row in result.iter_rows(named=True):
                avg_usage = float(row["avg_monthly_usage"])
                avg_cost = float(row["avg_monthly_cost"])
                usage_stddev = float(row["usage_stddev"]) if row["usage_stddev"] else 0
                usage_pattern = row["usage_pattern"]
                
                # Generate forecast for next 12 months
                monthly_forecasts = []
                base_growth_rate = 0.02  # 2% monthly growth assumption
                
                for month in range(1, forecast_months + 1):
                    # Add seasonal variation and growth
                    seasonal_factor = 1 + 0.1 * math.sin(month * math.pi / 6)  # Seasonal variation
                    growth_factor = (1 + base_growth_rate) ** month
                    forecasted_usage = avg_usage * seasonal_factor * growth_factor
                    
                    monthly_forecasts.append({
                        "month": month,
                        "forecasted_usage": round(forecasted_usage, 2),
                        "forecasted_cost": round(forecasted_usage * (avg_cost / avg_usage), 2)
                    })
                
                # Risk assessment
                risk_score = self._calculate_forecast_risk(usage_pattern, usage_stddev, avg_usage)
                
                forecasts.append({
                    "service": row["service"],
                    "instance_type": row["instance_type"],
                    "current_avg_usage": avg_usage,
                    "current_avg_cost": avg_cost,
                    "usage_pattern": usage_pattern,
                    "risk_score": risk_score,
                    "monthly_forecasts": monthly_forecasts,
                    "annual_forecast": {
                        "total_usage": sum(f["forecasted_usage"] for f in monthly_forecasts),
                        "total_cost": sum(f["forecasted_cost"] for f in monthly_forecasts)
                    }
                })
                
                # Generate commitment recommendation
                recommendation = self._generate_commitment_recommendation(
                    row["service"], avg_cost * 12, usage_pattern, risk_score
                )
                if recommendation:
                    recommendations.append(recommendation)
            
            # Risk analysis summary
            risk_analysis = self._analyze_portfolio_risk(forecasts)
            
            return {
                "forecasts": forecasts,
                "risk_analysis": risk_analysis,
                "recommendations": recommendations,
                "forecast_confidence": self._calculate_forecast_confidence(forecasts)
            }
            
        except Exception as e:
            print(f"❌ Error generating usage forecasting: {e}")
            return {"forecasts": [], "risk_analysis": {}, "recommendations": []}
    
    def simulate_commitment_scenarios(self, scenarios: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Simulate different discount scenarios and ROI analysis.
        Endpoint: POST /api/v1/finops/discounts/commitment-planning
        
        Args:
            scenarios: List of commitment scenarios to simulate
            
        Returns:
            Simulation results with ROI analysis and recommendations
        """
        simulation_results = []
        
        # Default scenarios if none provided
        if not scenarios:
            scenarios = [
                {"commitment_type": "1_year_ri", "coverage": 70, "discount": 30},
                {"commitment_type": "3_year_ri", "coverage": 50, "discount": 45},
                {"commitment_type": "savings_plan", "coverage": 80, "discount": 15}
            ]
        
        # Get current spend baseline
        baseline_annual_spend = self._get_annual_spend_baseline()
        
        for scenario in scenarios:
            commitment_type = scenario.get("commitment_type", "1_year_ri")
            coverage = scenario.get("coverage", 70)  # % of usage covered
            discount = scenario.get("discount", 30)  # % discount
            
            # Calculate scenario financials
            covered_spend = baseline_annual_spend * (coverage / 100)
            uncovered_spend = baseline_annual_spend * ((100 - coverage) / 100)
            
            # Savings calculation
            annual_savings = covered_spend * (discount / 100)
            net_cost = covered_spend * (1 - discount / 100) + uncovered_spend
            
            # ROI calculation (simplified)
            upfront_cost = covered_spend * 0.1  # Assume 10% upfront for commitments
            three_year_savings = annual_savings * 3
            roi = ((three_year_savings - upfront_cost) / upfront_cost) * 100 if upfront_cost > 0 else 0
            
            simulation_results.append({
                "scenario_id": f"scenario_{len(simulation_results) + 1}",
                "commitment_type": commitment_type,
                "coverage_percentage": coverage,
                "discount_percentage": discount,
                "annual_baseline_spend": baseline_annual_spend,
                "covered_spend": covered_spend,
                "annual_savings": annual_savings,
                "net_annual_cost": net_cost,
                "upfront_cost": upfront_cost,
                "three_year_roi": round(roi, 1),
                "payback_period_months": round((upfront_cost / (annual_savings / 12)), 1) if annual_savings > 0 else 0,
                "risk_level": self._assess_commitment_risk(commitment_type, coverage)
            })
        
        # ROI analysis summary
        roi_analysis = {
            "best_roi_scenario": max(simulation_results, key=lambda x: x["three_year_roi"]),
            "lowest_risk_scenario": min(simulation_results, key=lambda x: {"low": 1, "medium": 2, "high": 3}[x["risk_level"]]),
            "highest_savings_scenario": max(simulation_results, key=lambda x: x["annual_savings"])
        }
        
        # Generate recommendations
        recommendations = self._generate_commitment_recommendations(simulation_results, roi_analysis)
        
        return {
            "simulation_results": simulation_results,
            "roi_analysis": roi_analysis,
            "recommendations": recommendations,
            "baseline_spend": baseline_annual_spend
        }
    
    def _calculate_mock_expiration(self) -> str:
        """Generate mock expiration date for agreements."""
        # Random date between 30 days and 2 years from now
        import random
        days_ahead = random.randint(30, 730)
        expiration = datetime.now() + timedelta(days=days_ahead)
        return expiration.strftime("%Y-%m-%d")
    
    def _generate_renewal_timeline(self, agreements: List[Dict]) -> List[Dict]:
        """Generate renewal timeline for agreements."""
        renewals = []
        for agreement in agreements[:5]:  # Top 5 by value
            renewals.append({
                "agreement_id": agreement["agreement_id"],
                "service": agreement["service"],
                "expiration_date": agreement["expiration_date"],
                "annual_value": agreement["annual_commitment"],
                "renewal_status": "needs_review" if agreement["utilization_rate"] < 75 else "auto_renew",
                "days_until_expiration": (datetime.strptime(agreement["expiration_date"], "%Y-%m-%d") - datetime.now()).days
            })
        return sorted(renewals, key=lambda x: x["days_until_expiration"])
    
    def _determine_commitment_requirement(self, annual_spend: float) -> str:
        """Determine commitment requirement based on spend level."""
        if annual_spend > 500000:
            return "3_year"
        elif annual_spend > 100000:
            return "1_year"
        else:
            return "none"
    
    def _generate_negotiation_recommendation(self, spend: float, discount: float) -> str:
        """Generate negotiation recommendation."""
        if spend > 500000 and discount > 15:
            return "Schedule enterprise negotiation meeting"
        elif spend > 100000:
            return "Request volume discount review"
        else:
            return "Monitor for threshold achievement"
    
    def _calculate_forecast_risk(self, pattern: str, stddev: float, avg_usage: float) -> str:
        """Calculate risk level for usage forecasting."""
        if pattern == "volatile" or (stddev / avg_usage) > 0.5:
            return "high"
        elif pattern == "moderate":
            return "medium"
        else:
            return "low"
    
    def _generate_commitment_recommendation(self, service: str, annual_cost: float, pattern: str, risk: str) -> Optional[Dict]:
        """Generate commitment recommendation based on usage patterns."""
        if annual_cost < 10000:
            return None  # Too small for commitments
        
        if pattern == "stable" and risk == "low":
            return {
                "service": service,
                "recommendation": "Reserved Instance",
                "term": "3_year",
                "coverage": 80,
                "confidence": "high",
                "estimated_savings": annual_cost * 0.4
            }
        elif pattern == "moderate":
            return {
                "service": service,
                "recommendation": "Savings Plan",
                "term": "1_year",
                "coverage": 60,
                "confidence": "medium",
                "estimated_savings": annual_cost * 0.15
            }
        
        return None
    
    def _analyze_portfolio_risk(self, forecasts: List[Dict]) -> Dict[str, Any]:
        """Analyze overall portfolio risk."""
        total_forecasts = len(forecasts)
        high_risk_count = len([f for f in forecasts if f["risk_score"] == "high"])
        stable_pattern_count = len([f for f in forecasts if f["usage_pattern"] == "stable"])
        
        return {
            "portfolio_risk_level": "high" if high_risk_count > total_forecasts * 0.3 else "medium" if high_risk_count > 0 else "low",
            "stable_services_percentage": round((stable_pattern_count / total_forecasts) * 100, 1) if total_forecasts > 0 else 0,
            "high_risk_services": high_risk_count,
            "total_services": total_forecasts,
            "diversification_score": min(100, (len(set(f["service"] for f in forecasts)) / total_forecasts) * 100) if total_forecasts > 0 else 0
        }
    
    def _calculate_forecast_confidence(self, forecasts: List[Dict]) -> Dict[str, float]:
        """Calculate overall forecast confidence."""
        if not forecasts:
            return {"overall": 0, "by_service": {}}
        
        stable_count = len([f for f in forecasts if f["usage_pattern"] == "stable"])
        overall_confidence = (stable_count / len(forecasts)) * 100
        
        return {
            "overall": round(overall_confidence, 1),
            "by_service": {
                f["service"]: 90 if f["usage_pattern"] == "stable" else 70 if f["usage_pattern"] == "moderate" else 40
                for f in forecasts
            }
        }
    
    def _get_annual_spend_baseline(self) -> float:
        """Get baseline annual spend for simulation."""
        try:
            sql = f"SELECT SUM(line_item_unblended_cost) * 12 as annual_spend FROM {self.config.table_name} WHERE DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)"
            result = self.engine.query(sql)
            if not result.is_empty():
                return float(result.row(0)[0])
        except:
            pass
        return 100000  # Default fallback
    
    def _assess_commitment_risk(self, commitment_type: str, coverage: float) -> str:
        """Assess risk level for commitment scenarios."""
        if commitment_type.startswith("3_year") or coverage > 80:
            return "high"
        elif commitment_type.startswith("1_year") or coverage > 60:
            return "medium"
        else:
            return "low"
    
    def _generate_commitment_recommendations(self, scenarios: List[Dict], roi_analysis: Dict) -> List[Dict]:
        """Generate final commitment recommendations."""
        recommendations = []
        
        best_scenario = roi_analysis["best_roi_scenario"]
        recommendations.append({
            "recommendation": "Implement best ROI scenario",
            "scenario_id": best_scenario["scenario_id"],
            "rationale": f"Provides best 3-year ROI of {best_scenario['three_year_roi']}%",
            "priority": "high",
            "implementation_timeline": "30-60 days"
        })
        
        return recommendations


# Add missing import
import math