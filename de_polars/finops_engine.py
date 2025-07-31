"""
Unified FinOps Engine - Main interface for all cost analytics functionality
"""
from typing import Dict, Any, Optional

from .engine import DuckDBEngine, DataConfig, DataExportType
from .analytics import (
    KPISummaryAnalytics,
    SpendAnalytics,
    OptimizationAnalytics,
    AllocationAnalytics,
    DiscountAnalytics,
    AIRecommendationAnalytics
)
from .analytics.mcp_integration import MCPIntegrationAnalytics


class FinOpsEngine:
    """
    Unified FinOps Engine providing comprehensive cost analytics capabilities.
    
    This is the primary interface for accessing all cost analytics functionality
    including KPI summaries, spend analytics, optimization recommendations,
    cost allocation, discount tracking, and AI-powered insights.
    
    Usage:
        # Basic setup
        config = DataConfig(
            s3_bucket='my-bucket',
            s3_data_prefix='cur2/cur2/data',
            data_export_type=DataExportType.CUR_2_0,
            local_data_path='./local_data'
        )
        
        engine = FinOpsEngine(config)
        
        # Access analytics modules
        kpi_summary = engine.kpi.get_comprehensive_summary()
        spend_trends = engine.spend.get_invoice_summary()
        optimization_ops = engine.optimization.get_idle_resources()
        
        # Direct SQL access
        result = engine.query("SELECT * FROM CUR LIMIT 10")
    """
    
    def __init__(self, config: DataConfig):
        """
        Initialize FinOps Engine with configuration.
        
        Args:
            config: DataConfig object with AWS credentials and data settings
        """
        # Initialize core DuckDB engine
        self.engine = DuckDBEngine(config)
        self.config = config
        
        # Initialize all analytics modules
        self._kpi = None
        self._spend = None
        self._optimization = None
        self._allocation = None
        self._discounts = None
        self._ai = None
        self._mcp = None
    
    @property
    def kpi(self) -> KPISummaryAnalytics:
        """Access KPI Summary Analytics module."""
        if self._kpi is None:
            self._kpi = KPISummaryAnalytics(self.engine)
        return self._kpi
    
    @property
    def spend(self) -> SpendAnalytics:
        """Access Spend Analytics module."""
        if self._spend is None:
            self._spend = SpendAnalytics(self.engine)
        return self._spend
    
    @property
    def optimization(self) -> OptimizationAnalytics:
        """Access Optimization Analytics module."""
        if self._optimization is None:
            self._optimization = OptimizationAnalytics(self.engine)
        return self._optimization
    
    @property
    def allocation(self) -> AllocationAnalytics:
        """Access Allocation Analytics module."""
        if self._allocation is None:
            self._allocation = AllocationAnalytics(self.engine)
        return self._allocation
    
    @property
    def discounts(self) -> DiscountAnalytics:
        """Access Discount Analytics module."""
        if self._discounts is None:
            self._discounts = DiscountAnalytics(self.engine)
        return self._discounts
    
    @property
    def ai(self) -> AIRecommendationAnalytics:
        """Access AI Recommendation Analytics module."""
        if self._ai is None:
            self._ai = AIRecommendationAnalytics(self.engine)
        return self._ai
    
    @property
    def mcp(self) -> MCPIntegrationAnalytics:
        """Access MCP Integration Analytics module."""
        if self._mcp is None:
            self._mcp = MCPIntegrationAnalytics(self.engine)
        return self._mcp
    
    # Direct engine access methods
    def query(self, sql: str, force_s3: bool = False):
        """Execute SQL query using the underlying DuckDB engine."""
        return self.engine.query(sql, force_s3)
    
    def has_local_data(self) -> bool:
        """Check if local data is available."""
        return self.engine.has_local_data()
    
    def download_data_locally(self, overwrite: bool = False, show_progress: bool = True) -> None:
        """Download S3 data to local storage."""
        return self.engine.download_data_locally(overwrite, show_progress)
    
    def check_local_data_status(self) -> Dict[str, Any]:
        """Check local data cache status."""
        return self.engine.check_local_data_status()
    
    def list_available_partitions(self):
        """List all available data partitions."""
        return self.engine.list_available_partitions()
    
    def schema(self) -> Dict[str, str]:
        """Get data schema information."""
        return self.engine.schema()
    
    def catalog(self) -> Dict[str, Any]:
        """Get data catalog information."""
        return self.engine.catalog()
    
    def sample(self, n: int = 10):
        """Get a sample of the data."""
        return self.engine.sample(n)
    
    def info(self) -> None:
        """Print information about the data source."""
        return self.engine.info()
    
    # Convenience methods for common operations
    def get_dashboard_data(self) -> Dict[str, Any]:
        """
        Get comprehensive dashboard data combining multiple analytics.
        
        Returns:
            Dictionary with data for all dashboard components
        """
        try:
            dashboard_data = {
                "kpi_summary": self.kpi.get_comprehensive_summary(),
                "spend_summary": self.spend.get_invoice_summary(),
                "top_services": self.spend.get_top_services(limit=5),
                "top_regions": self.spend.get_top_regions(limit=5),
                "optimization_opportunities": self.optimization.get_idle_resources(),
                "tagging_compliance": self.allocation.get_tagging_compliance(),
                "discount_agreements": self.discounts.get_current_agreements(),
                "ai_insights": self.ai.get_optimization_insights()
            }
            
            # Add metadata
            dashboard_data["metadata"] = {
                "generated_at": self.engine.config.table_name,  # Mock timestamp
                "data_source": "local" if self.has_local_data() else "s3",
                "data_freshness": "current_month"
            }
            
            return dashboard_data
            
        except Exception as e:
            print(f"❌ Error generating dashboard data: {e}")
            return {"error": str(e)}
    
    def run_cost_health_check(self) -> Dict[str, Any]:
        """
        Run comprehensive cost health check across all modules.
        
        Returns:
            Health check results with scores and recommendations
        """
        health_check = {
            "overall_score": 0,
            "category_scores": {},
            "findings": [],
            "recommendations": []
        }
        
        try:
            # KPI Health Check
            kpi_summary = self.kpi.get_comprehensive_summary()
            total_spend = kpi_summary.get("overall_spend", {}).get("spend_all_cost", 0)
            total_savings = kpi_summary.get("savings_summary", {}).get("total_potential_savings", 0)
            
            savings_ratio = (total_savings / total_spend * 100) if total_spend > 0 else 0
            kpi_score = min(100, savings_ratio * 2)  # Scale savings potential to score
            health_check["category_scores"]["cost_efficiency"] = round(kpi_score, 1)
            
            # Optimization Health Check
            idle_resources = self.optimization.get_idle_resources()
            idle_count = len(idle_resources.get("idle_resources", []))
            optimization_score = max(0, 100 - (idle_count * 5))  # Deduct points for idle resources
            health_check["category_scores"]["resource_optimization"] = round(optimization_score, 1)
            
            # Tagging Compliance Check
            tagging_compliance = self.allocation.get_tagging_compliance()
            compliance_score = tagging_compliance.get("compliance_score", 0)
            health_check["category_scores"]["tagging_compliance"] = compliance_score
            
            # Calculate overall score
            scores = list(health_check["category_scores"].values())
            health_check["overall_score"] = round(sum(scores) / len(scores), 1) if scores else 0
            
            # Generate findings and recommendations
            if compliance_score < 70:
                health_check["findings"].append("Low tagging compliance detected")
                health_check["recommendations"].append("Implement automated tagging policies")
            
            if idle_count > 5:
                health_check["findings"].append(f"{idle_count} idle resources found")
                health_check["recommendations"].append("Review and terminate unused resources")
            
            if savings_ratio > 20:
                health_check["findings"].append("High optimization potential identified")
                health_check["recommendations"].append("Prioritize cost optimization initiatives")
            
            return health_check
            
        except Exception as e:
            health_check["error"] = str(e)
            return health_check
    
    def generate_executive_summary(self) -> Dict[str, Any]:
        """
        Generate executive summary for leadership reporting.
        
        Returns:
            Executive summary with key metrics and insights
        """
        try:
            # Get core metrics
            kpi_summary = self.kpi.get_comprehensive_summary()
            spend_summary = self.spend.get_invoice_summary()
            health_check = self.run_cost_health_check()
            
            # Extract key metrics
            current_spend = kpi_summary.get("overall_spend", {}).get("spend_all_cost", 0)
            mom_change = spend_summary.get("mom_change", 0)
            total_savings_potential = kpi_summary.get("savings_summary", {}).get("total_potential_savings", 0)
            
            executive_summary = {
                "summary_date": kpi_summary.get("summary_metadata", {}).get("query_date"),
                "key_metrics": {
                    "current_monthly_spend": current_spend,
                    "month_over_month_change": mom_change,
                    "optimization_potential": total_savings_potential,
                    "cost_health_score": health_check.get("overall_score", 0)
                },
                "executive_insights": [
                    f"Current monthly spend: ${current_spend:,.2f}",
                    f"Month-over-month change: {mom_change:+.1f}%",
                    f"Optimization opportunity: ${total_savings_potential:,.2f} potential monthly savings",
                    f"Cost health score: {health_check.get('overall_score', 0):.1f}/100"
                ],
                "priority_actions": health_check.get("recommendations", [])[:3],  # Top 3 recommendations
                "detailed_findings": health_check.get("findings", [])
            }
            
            return executive_summary
            
        except Exception as e:
            return {"error": str(e), "message": "Unable to generate executive summary"}
    
    @classmethod
    def from_s3_config(cls, s3_bucket: str, s3_data_prefix: str, data_export_type: str, **kwargs):
        """
        Convenience constructor for S3-based configuration.
        
        Args:
            s3_bucket: S3 bucket name
            s3_data_prefix: S3 data prefix path
            data_export_type: Type of AWS data export
            **kwargs: Additional configuration parameters
        """
        config = DataConfig(
            s3_bucket=s3_bucket,
            s3_data_prefix=s3_data_prefix,
            data_export_type=DataExportType(data_export_type),
            **kwargs
        )
        return cls(config)
    
    @classmethod
    def from_local_config(cls, local_data_path: str, s3_bucket: str, s3_data_prefix: str, 
                         data_export_type: str, **kwargs):
        """
        Convenience constructor for local data configuration.
        
        Args:
            local_data_path: Local data cache directory
            s3_bucket: S3 bucket name (for fallback)
            s3_data_prefix: S3 data prefix path
            data_export_type: Type of AWS data export
            **kwargs: Additional configuration parameters
        """
        config = DataConfig(
            s3_bucket=s3_bucket,
            s3_data_prefix=s3_data_prefix,
            data_export_type=DataExportType(data_export_type),
            local_data_path=local_data_path,
            prefer_local_data=True,
            **kwargs
        )
        return cls(config)