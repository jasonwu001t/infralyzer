"""
Cost Allocation Analytics - View 3: Cost Allocation & Tagging Management
"""
import polars as pl
import json
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..engine.duckdb_engine import DuckDBEngine


class AllocationAnalytics:
    """
    Improve cost visibility through proper allocation and tagging governance.
    Supports all endpoints for View 3: Cost Allocation & Tagging Management.
    """
    
    def __init__(self, engine: DuckDBEngine):
        """Initialize Allocation Analytics with DuckDB engine."""
        self.engine = engine
        self.config = engine.config
    
    def get_account_hierarchy(self) -> Dict[str, Any]:
        """
        Multi-account cost allocation and chargeback analysis.
        Endpoint: GET /api/v1/finops/allocation/account-hierarchy
        
        Returns:
            Account hierarchy with cost centers and allocation rules
        """
        sql = f"""
        WITH account_costs AS (
            SELECT 
                payer_account_id,
                linked_account_id,
                product_servicecode,
                SUM(line_item_unblended_cost) as total_cost,
                COUNT(DISTINCT line_item_resource_id) as resource_count
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1, 2, 3
        ),
        account_summary AS (
            SELECT 
                payer_account_id,
                linked_account_id,
                SUM(total_cost) as account_total_cost,
                SUM(resource_count) as total_resources
            FROM account_costs
            GROUP BY 1, 2
        )
        SELECT 
            payer_account_id,
            linked_account_id,
            account_total_cost,
            total_resources,
            CASE 
                WHEN account_total_cost > 10000 THEN 'Large'
                WHEN account_total_cost > 1000 THEN 'Medium'
                ELSE 'Small'
            END as account_size
        FROM account_summary
        ORDER BY account_total_cost DESC
        """
        
        try:
            result = self.engine.query(sql)
            accounts = []
            cost_centers = []
            
            for row in result.iter_rows(named=True):
                account_cost = float(row["account_total_cost"])
                
                accounts.append({
                    "account_id": row["linked_account_id"],
                    "payer_account_id": row["payer_account_id"],
                    "monthly_cost": account_cost,
                    "resource_count": int(row["total_resources"]),
                    "account_size": row["account_size"],
                    "cost_center": f"cost-center-{row['linked_account_id'][-4:]}"  # Mock cost center
                })
                
                # Create cost center aggregation
                cost_center_name = f"cost-center-{row['linked_account_id'][-4:]}"
                existing_cc = next((cc for cc in cost_centers if cc["name"] == cost_center_name), None)
                if existing_cc:
                    existing_cc["total_cost"] += account_cost
                    existing_cc["account_count"] += 1
                else:
                    cost_centers.append({
                        "name": cost_center_name,
                        "total_cost": account_cost,
                        "account_count": 1,
                        "allocation_method": "account_based"
                    })
            
            # Mock allocation rules
            allocation_rules = [
                {
                    "rule_id": "shared_services_allocation",
                    "description": "Allocate shared services costs by usage percentage",
                    "method": "proportional",
                    "applies_to": ["ElasticLoadBalancing", "AmazonVPC"]
                },
                {
                    "rule_id": "environment_allocation",
                    "description": "Allocate by environment tags",
                    "method": "tag_based",
                    "tag_key": "Environment"
                }
            ]
            
            return {
                "accounts": accounts,
                "cost_centers": cost_centers,
                "allocation_rules": allocation_rules
            }
            
        except Exception as e:
            print(f"❌ Error getting account hierarchy: {e}")
            return {"accounts": [], "cost_centers": [], "allocation_rules": []}
    
    def get_tagging_compliance(self) -> Dict[str, Any]:
        """
        Analyze tagging compliance across all resources.
        Endpoint: GET /api/v1/finops/allocation/tagging-compliance
        
        Returns:
            Tagging compliance metrics and untagged resources
        """
        sql = f"""
        WITH resource_tagging AS (
            SELECT 
                line_item_resource_id,
                product_servicecode,
                line_item_unblended_cost,
                CASE 
                    WHEN resource_tags IS NULL OR resource_tags = '' THEN 'untagged'
                    WHEN resource_tags LIKE '%Environment%' AND resource_tags LIKE '%Team%' THEN 'fully_tagged'
                    WHEN resource_tags LIKE '%Environment%' OR resource_tags LIKE '%Team%' THEN 'partially_tagged'
                    ELSE 'custom_tagged'
                END as tagging_status,
                resource_tags
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND line_item_resource_id IS NOT NULL
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
        ),
        tagging_summary AS (
            SELECT 
                tagging_status,
                product_servicecode,
                COUNT(DISTINCT line_item_resource_id) as resource_count,
                SUM(line_item_unblended_cost) as total_cost
            FROM resource_tagging
            GROUP BY 1, 2
        ),
        overall_stats AS (
            SELECT 
                COUNT(DISTINCT line_item_resource_id) as total_resources,
                SUM(line_item_unblended_cost) as total_cost
            FROM resource_tagging
        )
        SELECT 
            ts.tagging_status,
            ts.product_servicecode,
            ts.resource_count,
            ts.total_cost,
            ROUND((ts.resource_count::FLOAT / os.total_resources) * 100, 2) as resource_percentage,
            ROUND((ts.total_cost / os.total_cost) * 100, 2) as cost_percentage
        FROM tagging_summary ts
        CROSS JOIN overall_stats os
        ORDER BY ts.total_cost DESC
        """
        
        try:
            result = self.engine.query(sql)
            
            # Calculate compliance score
            total_resources = 0
            tagged_resources = 0
            untagged_resources = []
            tag_coverage = {}
            
            for row in result.iter_rows(named=True):
                resource_count = int(row["resource_count"])
                total_resources += resource_count
                
                if row["tagging_status"] != "untagged":
                    tagged_resources += resource_count
                else:
                    untagged_resources.append({
                        "service": row["product_servicecode"],
                        "resource_count": resource_count,
                        "cost_impact": float(row["total_cost"]),
                        "cost_percentage": float(row["cost_percentage"])
                    })
                
                # Track coverage by service
                service = row["product_servicecode"]
                if service not in tag_coverage:
                    tag_coverage[service] = {"total": 0, "tagged": 0}
                
                tag_coverage[service]["total"] += resource_count
                if row["tagging_status"] != "untagged":
                    tag_coverage[service]["tagged"] += resource_count
            
            # Calculate compliance score
            compliance_score = (tagged_resources / total_resources * 100) if total_resources > 0 else 0
            
            # Format tag coverage
            formatted_coverage = {}
            for service, counts in tag_coverage.items():
                coverage_pct = (counts["tagged"] / counts["total"] * 100) if counts["total"] > 0 else 0
                formatted_coverage[service] = {
                    "coverage_percentage": round(coverage_pct, 1),
                    "tagged_resources": counts["tagged"],
                    "total_resources": counts["total"]
                }
            
            return {
                "compliance_score": round(compliance_score, 1),
                "untagged_resources": untagged_resources,
                "tag_coverage": formatted_coverage,
                "total_resources": total_resources,
                "tagged_resources": tagged_resources
            }
            
        except Exception as e:
            print(f"❌ Error analyzing tagging compliance: {e}")
            return {"compliance_score": 0, "untagged_resources": [], "tag_coverage": {}}
    
    def get_cost_center_breakdown(self, period: Optional[str] = None) -> Dict[str, Any]:
        """
        Cost allocation by business unit, project, environment.
        Endpoint: GET /api/v1/finops/allocation/cost-center-breakdown
        
        Args:
            period: Optional billing period filter
            
        Returns:
            Cost center breakdown with chargeback and variance analysis
        """
        sql = f"""
        WITH tagged_costs AS (
            SELECT 
                line_item_resource_id,
                line_item_unblended_cost,
                product_servicecode,
                CASE 
                    WHEN resource_tags LIKE '%Environment%prod%' THEN 'Production'
                    WHEN resource_tags LIKE '%Environment%dev%' THEN 'Development'
                    WHEN resource_tags LIKE '%Environment%test%' THEN 'Testing'
                    ELSE 'Unallocated'
                END as environment,
                CASE 
                    WHEN resource_tags LIKE '%Team%platform%' THEN 'Platform'
                    WHEN resource_tags LIKE '%Team%data%' THEN 'Data'
                    WHEN resource_tags LIKE '%Team%product%' THEN 'Product'
                    ELSE 'Shared'
                END as team,
                CASE 
                    WHEN resource_tags LIKE '%Project%' THEN 
                        SUBSTRING(resource_tags FROM 'Project[\":]*([^,}\"]*)')
                    ELSE 'General'
                END as project
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
        )
        SELECT 
            environment,
            team,
            project,
            COUNT(DISTINCT line_item_resource_id) as resource_count,
            SUM(line_item_unblended_cost) as allocated_cost,
            COUNT(DISTINCT product_servicecode) as service_count
        FROM tagged_costs
        GROUP BY 1, 2, 3
        ORDER BY allocated_cost DESC
        """
        
        try:
            result = self.engine.query(sql)
            cost_centers = []
            allocations = []
            
            # Create mock budget for variance calculation
            budgets = {
                "Production": 50000,
                "Development": 15000,
                "Testing": 8000,
                "Unallocated": 5000
            }
            
            environment_totals = {}
            
            for row in result.iter_rows(named=True):
                allocated_cost = float(row["allocated_cost"])
                cost_center_id = f"{row['environment']}-{row['team']}-{row['project']}"
                
                cost_centers.append({
                    "cost_center_id": cost_center_id,
                    "environment": row["environment"],
                    "team": row["team"],
                    "project": row["project"],
                    "allocated_cost": allocated_cost,
                    "resource_count": int(row["resource_count"]),
                    "service_count": int(row["service_count"]),
                    "allocation_method": "tag_based"
                })
                
                # Track environment totals for variance
                env = row["environment"]
                environment_totals[env] = environment_totals.get(env, 0) + allocated_cost
                
                allocations.append({
                    "cost_center": cost_center_id,
                    "allocated_amount": allocated_cost,
                    "allocation_percentage": 0,  # Will be calculated
                    "chargeback_amount": allocated_cost,  # 1:1 for now
                    "allocation_method": "tag_based"
                })
            
            # Calculate variances
            variances = []
            for env, actual in environment_totals.items():
                budget = budgets.get(env, 0)
                if budget > 0:
                    variance_pct = ((actual - budget) / budget) * 100
                    variances.append({
                        "cost_center": env,
                        "budget": budget,
                        "actual": actual,
                        "variance": actual - budget,
                        "variance_percentage": round(variance_pct, 2),
                        "status": "over_budget" if variance_pct > 10 else "within_budget"
                    })
            
            return {
                "cost_centers": cost_centers,
                "allocations": allocations,
                "variances": variances
            }
            
        except Exception as e:
            print(f"❌ Error getting cost center breakdown: {e}")
            return {"cost_centers": [], "allocations": [], "variances": []}
    
    def create_tagging_rules(self, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Define automated tagging rules and policies.
        Endpoint: POST /api/v1/finops/allocation/tagging-rules
        
        Args:
            rules: List of tagging rules to create
            
        Returns:
            Rule creation status and affected resources
        """
        # This would typically integrate with AWS Organizations or Config
        # For now, return a mock response
        
        created_rules = []
        total_affected_resources = 0
        
        for rule in rules:
            rule_id = f"tag_rule_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Estimate affected resources based on rule criteria
            estimated_resources = self._estimate_rule_impact(rule)
            total_affected_resources += estimated_resources
            
            created_rules.append({
                "rule_id": rule_id,
                "status": "created",
                "rule_name": rule.get("name", "Unnamed Rule"),
                "tag_key": rule.get("tag_key"),
                "tag_value": rule.get("tag_value"),
                "resource_criteria": rule.get("criteria", {}),
                "affected_resources": estimated_resources,
                "enforcement_mode": rule.get("enforcement", "monitor")
            })
        
        return {
            "created_rules": created_rules,
            "total_rules": len(created_rules),
            "total_affected_resources": total_affected_resources,
            "status": "success"
        }
    
    def get_third_party_integration_status(self) -> Dict[str, Any]:
        """
        Support for external tagging tools integration status.
        Endpoint: GET /api/v1/finops/allocation/third-party-integration
        
        Returns:
            Integration status and data quality metrics
        """
        # Mock integration status - would integrate with actual tools
        integrations = [
            {
                "integration_name": "Terraform",
                "status": "connected",
                "last_sync": "2025-01-15T08:30:00Z",
                "resources_managed": 1250,
                "tag_coverage": 85.5,
                "data_quality_score": 92
            },
            {
                "integration_name": "CloudFormation",
                "status": "connected", 
                "last_sync": "2025-01-15T09:15:00Z",
                "resources_managed": 850,
                "tag_coverage": 78.2,
                "data_quality_score": 88
            },
            {
                "integration_name": "ServiceNow ITSM",
                "status": "disconnected",
                "last_sync": "2025-01-10T14:20:00Z",
                "resources_managed": 0,
                "tag_coverage": 0,
                "data_quality_score": 0,
                "error": "Authentication failed"
            }
        ]
        
        # Calculate overall sync status
        connected_integrations = [i for i in integrations if i["status"] == "connected"]
        sync_status = {
            "total_integrations": len(integrations),
            "connected": len(connected_integrations),
            "average_data_quality": round(
                sum(i["data_quality_score"] for i in connected_integrations) / len(connected_integrations)
                if connected_integrations else 0, 1
            ),
            "last_successful_sync": max(
                (i["last_sync"] for i in connected_integrations),
                default="Never"
            )
        }
        
        # Mock data quality metrics
        data_quality = [
            {
                "metric": "Tag Consistency",
                "score": 87.5,
                "issues": ["Inconsistent environment values", "Missing required tags"]
            },
            {
                "metric": "Data Completeness",
                "score": 92.3,
                "issues": ["Some resources missing cost center tags"]
            },
            {
                "metric": "Freshness",
                "score": 95.1,
                "issues": []
            }
        ]
        
        return {
            "integrations": integrations,
            "sync_status": sync_status,
            "data_quality": data_quality
        }
    
    def _estimate_rule_impact(self, rule: Dict[str, Any]) -> int:
        """Estimate how many resources a tagging rule would affect."""
        # This would query actual data in a real implementation
        # For now, return a mock estimate based on rule criteria
        
        criteria = rule.get("criteria", {})
        service = criteria.get("service")
        
        # Mock estimates based on service type
        service_estimates = {
            "EC2": 150,
            "RDS": 25,
            "S3": 200,
            "Lambda": 75
        }
        
        return service_estimates.get(service, 50)