"""
Spend Analytics - View 1: Actual Spend Analysis and Trend Analysis
"""
import polars as pl
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from ..engine.duckdb_engine import DuckDBEngine


class SpendAnalytics:
    """
    Real-time spend visibility and trend analysis for financial planning.
    Supports all endpoints for View 1: Actual Spend Analytics.
    """
    
    def __init__(self, engine: DuckDBEngine):
        """Initialize Spend Analytics with DuckDB engine."""
        self.engine = engine
        self.config = engine.config
    
    def get_invoice_summary(self, months_back: int = 12) -> Dict[str, Any]:
        """
        Get invoice summary with MoM/YoY changes and forecast.
        Endpoint: GET /api/v1/finops/spend/invoice/summary
        
        Args:
            months_back: Number of months to include in trend analysis
            
        Returns:
            Invoice total, MoM change %, YoY change %, trend data, forecast
        """
        sql = f"""
        WITH monthly_spend AS (
            SELECT 
                DATE_TRUNC('month', line_item_usage_start_date) as month,
                SUM(line_item_unblended_cost) as total_spend
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '{months_back} months'
            GROUP BY 1
            ORDER BY 1 DESC
        ),
        spend_with_changes AS (
            SELECT 
                month,
                total_spend,
                LAG(total_spend, 1) OVER (ORDER BY month) as prev_month_spend,
                LAG(total_spend, 12) OVER (ORDER BY month) as prev_year_spend
            FROM monthly_spend
        )
        SELECT 
            month,
            total_spend,
            CASE 
                WHEN prev_month_spend > 0 THEN 
                    ROUND(((total_spend - prev_month_spend) / prev_month_spend) * 100, 2)
                ELSE NULL 
            END as mom_change,
            CASE 
                WHEN prev_year_spend > 0 THEN 
                    ROUND(((total_spend - prev_year_spend) / prev_year_spend) * 100, 2)
                ELSE NULL 
            END as yoy_change
        FROM spend_with_changes
        ORDER BY month DESC
        """
        
        try:
            result = self.engine.query(sql)
            if result.is_empty():
                return self._get_empty_invoice_summary()
            
            # Get latest month data
            latest = result.row(0, named=True)
            
            # Build trend data
            trend_data = []
            for row in result.iter_rows(named=True):
                trend_data.append({
                    "month": str(row["month"])[:7],  # YYYY-MM format
                    "spend": float(row["total_spend"])
                })
            
            # Simple linear forecast for next month
            forecast = self._calculate_forecast(trend_data)
            
            return {
                "invoice_total": float(latest["total_spend"]),
                "mom_change": float(latest["mom_change"]) if latest["mom_change"] else 0,
                "yoy_change": float(latest["yoy_change"]) if latest["yoy_change"] else 0,
                "trend_data": trend_data,
                "forecast": forecast
            }
            
        except Exception as e:
            print(f"❌ Error getting invoice summary: {e}")
            return self._get_empty_invoice_summary()
    
    def get_top_regions(self, limit: int = 10) -> Dict[str, Any]:
        """
        Get top regions by spend with cost breakdown.
        Endpoint: GET /api/v1/finops/spend/regions/top
        
        Args:
            limit: Number of top regions to return
            
        Returns:
            Top regions with spend, percentage, MoM change
        """
        sql = f"""
        WITH current_month AS (
            SELECT 
                product_region,
                SUM(line_item_unblended_cost) as current_spend
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1
        ),
        prev_month AS (
            SELECT 
                product_region,
                SUM(line_item_unblended_cost) as prev_spend
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
            GROUP BY 1
        ),
        total_spend AS (
            SELECT SUM(current_spend) as total FROM current_month
        )
        SELECT 
            c.product_region as region_name,
            c.current_spend as spend,
            ROUND((c.current_spend / t.total) * 100, 2) as percentage,
            CASE 
                WHEN p.prev_spend > 0 THEN 
                    ROUND(((c.current_spend - p.prev_spend) / p.prev_spend) * 100, 2)
                ELSE NULL 
            END as mom_change
        FROM current_month c
        CROSS JOIN total_spend t
        LEFT JOIN prev_month p ON c.product_region = p.product_region
        WHERE c.product_region IS NOT NULL
        ORDER BY c.current_spend DESC
        LIMIT {limit}
        """
        
        try:
            result = self.engine.query(sql)
            regions = []
            
            for row in result.iter_rows(named=True):
                regions.append({
                    "name": row["region_name"],
                    "spend": float(row["spend"]),
                    "percentage": float(row["percentage"]),
                    "mom_change": float(row["mom_change"]) if row["mom_change"] else 0,
                    "details": {}  # Could be expanded with service breakdown
                })
            
            return {"regions": regions}
            
        except Exception as e:
            print(f"❌ Error getting top regions: {e}")
            return {"regions": []}
    
    def get_top_services(self, limit: int = 10) -> Dict[str, Any]:
        """
        Get top AWS services by spend with trend analysis.
        Endpoint: GET /api/v1/finops/spend/services/top
        
        Args:
            limit: Number of top services to return
            
        Returns:
            Top services with spend, percentage, trend, resources
        """
        sql = f"""
        WITH service_spend AS (
            SELECT 
                product_servicecode as service_name,
                SUM(line_item_unblended_cost) as total_spend,
                COUNT(DISTINCT line_item_resource_id) as resource_count
            FROM {self.config.table_name}
            WHERE line_item_unblended_cost > 0
                AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY 1
        ),
        total_spend AS (
            SELECT SUM(total_spend) as total FROM service_spend
        )
        SELECT 
            s.service_name,
            s.total_spend as spend,
            ROUND((s.total_spend / t.total) * 100, 2) as percentage,
            s.resource_count
        FROM service_spend s
        CROSS JOIN total_spend t
        WHERE s.service_name IS NOT NULL
        ORDER BY s.total_spend DESC
        LIMIT {limit}
        """
        
        try:
            result = self.engine.query(sql)
            services = []
            
            for row in result.iter_rows(named=True):
                services.append({
                    "name": row["service_name"],
                    "spend": float(row["spend"]),
                    "percentage": float(row["percentage"]),
                    "trend": "stable",  # Could be calculated with historical data
                    "resources": [{"count": int(row["resource_count"])}]
                })
            
            return {"services": services}
            
        except Exception as e:
            print(f"❌ Error getting top services: {e}")
            return {"services": []}
    
    def get_spend_breakdown(self, dimensions: List[str] = ["region", "service"]) -> Dict[str, Any]:
        """
        Multi-dimensional spend analysis.
        Endpoint: GET /api/v1/finops/spend/breakdown
        
        Args:
            dimensions: Dimensions to break down by (region, service, etc.)
            
        Returns:
            Multi-dimensional spend breakdown
        """
        # Build dynamic GROUP BY based on dimensions
        select_fields = []
        group_fields = []
        
        if "region" in dimensions:
            select_fields.append("product_region as region")
            group_fields.append("product_region")
            
        if "service" in dimensions:
            select_fields.append("product_servicecode as service")
            group_fields.append("product_servicecode")
        
        select_clause = ", ".join(select_fields) if select_fields else "'All' as dimension"
        group_clause = ", ".join(group_fields) if group_fields else ""
        
        sql = f"""
        SELECT 
            {select_clause},
            SUM(line_item_unblended_cost) as spend,
            COUNT(DISTINCT line_item_resource_id) as resource_count
        FROM {self.config.table_name}
        WHERE line_item_unblended_cost > 0
            AND DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE)
        {f'GROUP BY {group_clause}' if group_clause else ''}
        ORDER BY spend DESC
        LIMIT 50
        """
        
        try:
            result = self.engine.query(sql)
            breakdown = []
            
            for row in result.iter_rows(named=True):
                item = {
                    "spend": float(row["spend"]),
                    "resources": [{"count": int(row["resource_count"])}]
                }
                
                # Add dimension values
                if "region" in dimensions and "region" in row:
                    item["region"] = row["region"]
                if "service" in dimensions and "service" in row:
                    item["service"] = row["service"]
                
                breakdown.append(item)
            
            return {"breakdown": breakdown}
            
        except Exception as e:
            print(f"❌ Error getting spend breakdown: {e}")
            return {"breakdown": []}
    
    def export_spend_data(self, format: str = "csv", date_range: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Export spend data in various formats.
        Endpoint: POST /api/v1/finops/spend/export
        
        Args:
            format: Export format (csv, json, excel)
            date_range: Optional date range filter
            
        Returns:
            Export information with URL and metadata
        """
        # This would typically generate a file and return a download URL
        # For now, return metadata about what would be exported
        
        sql = f"""
        SELECT 
            line_item_usage_start_date,
            product_servicecode,
            product_region,
            line_item_unblended_cost,
            line_item_resource_id
        FROM {self.config.table_name}
        WHERE line_item_unblended_cost > 0
        """
        
        # Add date range filter if provided
        if date_range:
            if date_range.get("start"):
                sql += f" AND line_item_usage_start_date >= '{date_range['start']}'"
            if date_range.get("end"):
                sql += f" AND line_item_usage_start_date <= '{date_range['end']}'"
        
        sql += " ORDER BY line_item_usage_start_date DESC LIMIT 10000"
        
        try:
            result = self.engine.query(sql)
            
            return {
                "export_url": f"/exports/spend_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{format}",
                "format": format,
                "expires_at": (datetime.now() + timedelta(hours=24)).isoformat(),
                "record_count": result.shape[0],
                "file_size_mb": round(result.estimated_size("mb"), 2) if hasattr(result, 'estimated_size') else 0
            }
            
        except Exception as e:
            print(f"❌ Error preparing export: {e}")
            return {"error": str(e)}
    
    def _calculate_forecast(self, trend_data: List[Dict]) -> Dict[str, Any]:
        """Calculate simple linear forecast for next month."""
        if len(trend_data) < 2:
            return {"next_month": 0, "confidence": 0}
        
        # Simple linear trend calculation
        recent_values = [item["spend"] for item in trend_data[:3]]  # Last 3 months
        avg_spend = sum(recent_values) / len(recent_values)
        
        # Calculate trend
        if len(recent_values) >= 2:
            trend = (recent_values[0] - recent_values[1]) / recent_values[1] if recent_values[1] > 0 else 0
            forecast_spend = recent_values[0] * (1 + trend)
        else:
            forecast_spend = avg_spend
        
        return {
            "next_month": round(forecast_spend, 2),
            "confidence": 75  # Fixed confidence for now
        }
    
    def _get_empty_invoice_summary(self) -> Dict[str, Any]:
        """Get empty invoice summary when no data is available."""
        return {
            "invoice_total": 0,
            "mom_change": 0,
            "yoy_change": 0,
            "trend_data": [],
            "forecast": {"next_month": 0, "confidence": 0}
        }