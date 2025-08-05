"""
Cost Optimization Analytics - View 2: Cost Optimization Intelligence
"""
import polars as pl
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..engine.duckdb_engine import DuckDBEngine


class OptimizationAnalytics:
    """
    Identify and quantify cost saving opportunities across AWS infrastructure.
    Supports all endpoints for View 2: Cost Optimization Intelligence.
    """
    
    def __init__(self, engine: DuckDBEngine):
        """Initialize Optimization Analytics with DuckDB engine."""
        self.engine = engine
        self.config = engine.config
    
    def get_idle_resources(self, utilization_threshold: float = 5.0) -> Dict[str, Any]:
        """
        Detect idle resources based on utilization patterns.
        Endpoint: GET /api/v1/finops/optimization/idle-resources
        
        Args:
            utilization_threshold: Utilization percentage threshold for idle detection
            
        Returns:
            Idle resources with potential savings and risk assessment
        """
        sql = f"""
        WITH resource_utilization AS (
            SELECT 
                line_item_resource_id as resource_id,
                product_servicecode as service,
                product_instance_type as instance_type,
                SUM(line_item_unblended_cost) as monthly_cost,
                COUNT(*) as usage_records,
                AVG(CASE 
                    WHEN line_item_usage_amount > 0 THEN line_item_usage_amount 
                    ELSE 0 
                END) as avg_utilization
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND product_servicecode IN ('AmazonEC2', 'AmazonRDS', 'ElasticLoadBalancing')
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1, 2, 3
        )
        SELECT 
            resource_id,
            service,
            instance_type,
            monthly_cost,
            avg_utilization,
            CASE 
                WHEN avg_utilization < {utilization_threshold} THEN 'idle'
                WHEN avg_utilization < {utilization_threshold * 2} THEN 'underutilized'
                ELSE 'active'
            END as status,
            CASE 
                WHEN avg_utilization < {utilization_threshold} THEN monthly_cost * 0.9  -- 90% savings for idle
                WHEN avg_utilization < {utilization_threshold * 2} THEN monthly_cost * 0.3  -- 30% savings for underutilized
                ELSE 0
            END as potential_savings
        FROM resource_utilization
        WHERE avg_utilization < {utilization_threshold * 2}  -- Only show idle/underutilized
        ORDER BY potential_savings DESC
        LIMIT 50
        """
        
        try:
            result = self.engine.query(sql)
            idle_resources = []
            total_potential_savings = 0
            
            for row in result.iter_rows(named=True):
                savings = float(row["potential_savings"])
                total_potential_savings += savings
                
                idle_resources.append({
                    "resource_id": row["resource_id"],
                    "service": row["service"],
                    "instance_type": row["instance_type"],
                    "monthly_cost": float(row["monthly_cost"]),
                    "utilization": float(row["avg_utilization"]),
                    "status": row["status"],
                    "potential_savings": savings,
                    "risk_level": self._assess_termination_risk(row["service"], row["status"])
                })
            
            risk_levels = self._calculate_risk_distribution(idle_resources)
            
            return {
                "idle_resources": idle_resources,
                "total_potential_savings": round(total_potential_savings, 2),
                "risk_levels": risk_levels
            }
            
        except Exception as e:
            print(f"Error detecting idle resources: {e}")
            return {"idle_resources": [], "total_potential_savings": 0, "risk_levels": {}}
    
    def get_rightsizing_recommendations(self) -> Dict[str, Any]:
        """
        Analyze over-provisioned instances and recommend optimal sizes.
        Endpoint: GET /api/v1/finops/optimization/rightsizing
        
        Returns:
            Rightsizing recommendations with savings potential
        """
        sql = f"""
        WITH instance_analysis AS (
            SELECT 
                line_item_resource_id as resource_id,
                product_instance_type as current_instance_type,
                SUM(line_item_unblended_cost) as monthly_cost,
                AVG(line_item_usage_amount) as avg_usage,
                COUNT(*) as usage_records
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND product_servicecode = 'AmazonEC2'
                AND product_instance_type IS NOT NULL
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1, 2
            HAVING COUNT(*) > 10  -- Ensure sufficient data
        )
        SELECT 
            resource_id,
            current_instance_type,
            monthly_cost,
            avg_usage,
            CASE 
                WHEN current_instance_type LIKE '%xlarge%' AND avg_usage < 50 THEN 
                    REPLACE(current_instance_type, 'xlarge', 'large')
                WHEN current_instance_type LIKE '%large%' AND avg_usage < 30 THEN 
                    REPLACE(current_instance_type, 'large', 'medium')
                WHEN current_instance_type LIKE '%medium%' AND avg_usage < 20 THEN 
                    REPLACE(current_instance_type, 'medium', 'small')
                ELSE current_instance_type
            END as recommended_instance_type,
            CASE 
                WHEN current_instance_type LIKE '%xlarge%' AND avg_usage < 50 THEN monthly_cost * 0.5
                WHEN current_instance_type LIKE '%large%' AND avg_usage < 30 THEN monthly_cost * 0.5
                WHEN current_instance_type LIKE '%medium%' AND avg_usage < 20 THEN monthly_cost * 0.5
                ELSE monthly_cost
            END as estimated_new_cost
        FROM instance_analysis
        WHERE avg_usage < 60  -- Only instances with low utilization
        ORDER BY (monthly_cost - estimated_new_cost) DESC
        LIMIT 20
        """
        
        try:
            result = self.engine.query(sql)
            recommendations = []
            total_savings_potential = 0
            
            for row in result.iter_rows(named=True):
                current_cost = float(row["monthly_cost"])
                new_cost = float(row["estimated_new_cost"])
                monthly_savings = current_cost - new_cost
                
                if monthly_savings > 0:  # Only include actual savings
                    total_savings_potential += monthly_savings
                    
                    recommendations.append({
                        "id": f"rightsizing_{row['resource_id']}",
                        "type": "rightsizing", 
                        "resource_id": row["resource_id"],
                        "current_config": {
                            "instance_type": row["current_instance_type"],
                            "monthly_cost": current_cost
                        },
                        "recommended_config": {
                            "instance_type": row["recommended_instance_type"],
                            "monthly_cost": new_cost
                        },
                        "savings": {
                            "monthly": round(monthly_savings, 2),
                            "annual": round(monthly_savings * 12, 2)
                        },
                        "confidence_score": self._calculate_confidence_score(row["avg_usage"]),
                        "risk_level": "low",
                        "implementation_effort": "easy"
                    })
            
            implementation_effort = self._calculate_implementation_effort(recommendations)
            
            return {
                "recommendations": recommendations,
                "savings_potential": round(total_savings_potential, 2),
                "implementation_effort": implementation_effort
            }
            
        except Exception as e:
            print(f"Error generating rightsizing recommendations: {e}")
            return {"recommendations": [], "savings_potential": 0, "implementation_effort": "unknown"}
    
    def get_cross_service_migration_opportunities(self) -> Dict[str, Any]:
        """
        Identify opportunities to migrate between services.
        Endpoint: GET /api/v1/finops/optimization/cross-service-migration
        
        Returns:
            Migration opportunities with business case and roadmap
        """
        # Simplified migration analysis - could be much more sophisticated
        sql = f"""
        WITH ec2_lambda_candidates AS (
            SELECT 
                'EC2_to_Lambda' as migration_type,
                COUNT(DISTINCT line_item_resource_id) as resource_count,
                SUM(line_item_unblended_cost) as current_monthly_cost,
                SUM(line_item_unblended_cost) * 0.3 as estimated_lambda_cost  -- Rough estimate
            FROM {self.config.table_name}
            WHERE product_servicecode = 'AmazonEC2'
                AND product_instance_type LIKE '%micro%'  -- Small instances are Lambda candidates
                AND line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
        )
        SELECT 
            migration_type,
            resource_count,
            current_monthly_cost,
            estimated_lambda_cost,
            (current_monthly_cost - estimated_lambda_cost) as potential_monthly_savings
        FROM ec2_lambda_candidates
        WHERE resource_count > 0
        """
        
        try:
            result = self.engine.query(sql)
            migration_opportunities = []
            
            for row in result.iter_rows(named=True):
                monthly_savings = float(row["potential_monthly_savings"])
                
                if monthly_savings > 0:
                    migration_opportunities.append({
                        "migration_type": row["migration_type"],
                        "affected_resources": int(row["resource_count"]),
                        "current_monthly_cost": float(row["current_monthly_cost"]),
                        "estimated_new_cost": float(row["estimated_lambda_cost"]),
                        "monthly_savings": round(monthly_savings, 2),
                        "annual_savings": round(monthly_savings * 12, 2),
                        "implementation_complexity": "medium",
                        "business_impact": "low_risk"
                    })
            
            # Create business case and roadmap
            business_case = self._create_migration_business_case(migration_opportunities)
            roadmap = self._create_migration_roadmap(migration_opportunities)
            
            return {
                "migration_opportunities": migration_opportunities,
                "business_case": business_case,
                "roadmap": roadmap
            }
            
        except Exception as e:
            print(f"Error analyzing migration opportunities: {e}")
            return {"migration_opportunities": [], "business_case": [], "roadmap": []}
    
    def get_vpc_optimization_recommendations(self) -> Dict[str, Any]:
        """
        Analyze cross-VPC and cross-AZ data transfer costs.
        Endpoint: GET /api/v1/finops/optimization/vpc-charges
        
        Returns:
            VPC optimization recommendations and network cost analysis
        """
        sql = f"""
        WITH data_transfer_costs AS (
            SELECT 
                product_region,
                product_location as availability_zone,
                SUM(CASE WHEN line_item_usage_type LIKE '%DataTransfer%' THEN line_item_unblended_cost ELSE 0 END) as transfer_cost,
                COUNT(DISTINCT line_item_resource_id) as resource_count
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND (line_item_usage_type LIKE '%DataTransfer%' OR line_item_usage_type LIKE '%Data%')
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1, 2
        )
        SELECT 
            product_region,
            availability_zone,
            transfer_cost,
            resource_count,
            transfer_cost * 0.7 as potential_savings  -- Assume 70% reduction possible
        FROM data_transfer_costs
        WHERE transfer_cost > 100  -- Focus on significant costs
        ORDER BY transfer_cost DESC
        """
        
        try:
            result = self.engine.query(sql)
            
            transfer_analysis = []
            total_savings = 0
            
            for row in result.iter_rows(named=True):
                savings = float(row["potential_savings"])
                total_savings += savings
                
                transfer_analysis.append({
                    "region": row["product_region"],
                    "availability_zone": row["availability_zone"],
                    "monthly_transfer_cost": float(row["transfer_cost"]),
                    "resource_count": int(row["resource_count"]),
                    "optimization_potential": savings
                })
            
            optimization_recommendations = [
                {
                    "recommendation": "Consolidate resources within same AZ",
                    "potential_savings": round(total_savings * 0.4, 2),
                    "complexity": "medium"
                },
                {
                    "recommendation": "Implement VPC peering optimization",
                    "potential_savings": round(total_savings * 0.3, 2),
                    "complexity": "high"
                }
            ]
            
            return {
                "transfer_analysis": transfer_analysis,
                "optimization_recommendations": optimization_recommendations,
                "savings": round(total_savings, 2)
            }
            
        except Exception as e:
            print(f"Error analyzing VPC costs: {e}")
            return {"transfer_analysis": [], "optimization_recommendations": [], "savings": 0}
    
    def implement_recommendation(self, recommendation_id: str, auto_approve: bool = False) -> Dict[str, Any]:
        """
        Track implementation of optimization recommendations.
        Endpoint: POST /api/v1/finops/optimization/implement-recommendation
        
        Args:
            recommendation_id: ID of the recommendation to implement
            auto_approve: Whether to auto-approve the implementation
            
        Returns:
            Implementation tracking information
        """
        # This would typically integrate with AWS APIs or automation tools
        # For now, return a mock implementation response
        
        implementation_status = {
            "implementation_id": f"impl_{recommendation_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "status": "scheduled" if auto_approve else "pending_approval",
            "tracking_metrics": {
                "expected_savings": 0,  # Would be filled from recommendation data
                "implementation_date": datetime.now().isoformat(),
                "monitoring_period": "30_days"
            },
            "approval_required": not auto_approve,
            "estimated_completion": "2-5 business days"
        }
        
        return implementation_status
    
    def _assess_termination_risk(self, service: str, status: str) -> str:
        """Assess risk level for terminating a resource."""
        if service == "ElasticLoadBalancing":
            return "high"  # Load balancers are critical
        elif service == "AmazonRDS" and status == "idle":
            return "medium"  # Databases need careful consideration
        elif status == "idle":
            return "low"  # Truly idle resources are safe to terminate
        else:
            return "medium"
    
    def _calculate_confidence_score(self, utilization: float) -> int:
        """Calculate confidence score based on utilization patterns."""
        if utilization < 20:
            return 95
        elif utilization < 40:
            return 85
        elif utilization < 60:
            return 70
        else:
            return 50
    
    def _calculate_risk_distribution(self, resources: List[Dict]) -> Dict[str, int]:
        """Calculate distribution of risk levels."""
        risk_counts = {"low": 0, "medium": 0, "high": 0}
        for resource in resources:
            risk_level = resource["risk_level"]
            risk_counts[risk_level] = risk_counts.get(risk_level, 0) + 1
        return risk_counts
    
    def _calculate_implementation_effort(self, recommendations: List[Dict]) -> str:
        """Calculate overall implementation effort."""
        if len(recommendations) < 5:
            return "easy"
        elif len(recommendations) < 15:
            return "medium"
        else:
            return "complex"
    
    def _create_migration_business_case(self, opportunities: List[Dict]) -> List[Dict]:
        """Create business case for migration opportunities."""
        if not opportunities:
            return []
        
        total_savings = sum(opp["annual_savings"] for opp in opportunities)
        
        return [
            {
                "case": "Cost Reduction",
                "annual_savings": total_savings,
                "payback_period": "3-6 months",
                "risk_assessment": "Medium"
            }
        ]
    
    def _create_migration_roadmap(self, opportunities: List[Dict]) -> List[Dict]:
        """Create implementation roadmap for migrations."""
        if not opportunities:
            return []
        
        return [
            {
                "phase": "Assessment",
                "duration": "2-4 weeks",
                "activities": ["Workload analysis", "Dependency mapping"]
            },
            {
                "phase": "Pilot Migration",
                "duration": "4-6 weeks", 
                "activities": ["Select pilot workloads", "Implement monitoring"]
            },
            {
                "phase": "Full Migration",
                "duration": "3-6 months",
                "activities": ["Migrate remaining workloads", "Optimize performance"]
            }
        ]