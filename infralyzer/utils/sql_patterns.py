"""
Common SQL query patterns and utilities for Infralyzer analytics.
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from ..constants import BASIC_CUR_COLUMNS, DATE_FORMAT_MONTHLY, DATE_FORMAT_DAILY


class SQLPatterns:
    """Common SQL query patterns and builders for cost analytics."""
    
    @staticmethod
    def build_base_filter_clause(
        billing_period: Optional[str] = None,
        payer_account_id: Optional[str] = None,
        linked_account_id: Optional[str] = None,
        service_codes: Optional[List[str]] = None,
        min_cost: float = 0.0
    ) -> str:
        """
        Build standard WHERE clause for cost queries.
        
        Args:
            billing_period: Billing period filter (YYYY-MM format)
            payer_account_id: Payer account filter
            linked_account_id: Linked account filter
            service_codes: List of AWS service codes to include
            min_cost: Minimum cost threshold
            
        Returns:
            SQL WHERE clause string
        """
        conditions = [f"line_item_unblended_cost > {min_cost}"]
        
        if billing_period:
            conditions.append(f"DATE_TRUNC('month', line_item_usage_start_date) = '{billing_period}-01'")
        
        if payer_account_id:
            conditions.append(f"bill_payer_account_id = '{payer_account_id}'")
        
        if linked_account_id:
            conditions.append(f"line_item_usage_account_id = '{linked_account_id}'")
        
        if service_codes:
            service_list = "', '".join(service_codes)
            conditions.append(f"product_servicecode IN ('{service_list}')")
        
        return " AND ".join(conditions)
    
    @staticmethod
    def build_date_filter(
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        last_n_months: Optional[int] = None
    ) -> str:
        """
        Build date filter clause.
        
        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)  
            last_n_months: Number of months back from current date
            
        Returns:
            SQL date filter clause
        """
        if last_n_months:
            return f"line_item_usage_start_date >= CURRENT_DATE - INTERVAL '{last_n_months} months'"
        
        conditions = []
        if start_date:
            conditions.append(f"line_item_usage_start_date >= '{start_date}'")
        if end_date:
            conditions.append(f"line_item_usage_start_date <= '{end_date}'")
        
        return " AND ".join(conditions) if conditions else "1=1"
    
    @staticmethod
    def build_monthly_aggregation(
        select_columns: List[str],
        group_by_columns: List[str],
        table_name: str = "CUR",
        additional_filters: str = ""
    ) -> str:
        """
        Build monthly cost aggregation query.
        
        Args:
            select_columns: Additional columns to select
            group_by_columns: Columns to group by
            table_name: Source table name
            additional_filters: Additional WHERE conditions
            
        Returns:
            Complete SQL query string
        """
        base_columns = [
            "DATE_TRUNC('month', line_item_usage_start_date) as billing_month",
            "SUM(line_item_unblended_cost) as total_cost",
            "COUNT(DISTINCT line_item_resource_id) as resource_count"
        ]
        
        all_columns = base_columns + select_columns
        all_group_by = ["DATE_TRUNC('month', line_item_usage_start_date)"] + group_by_columns
        
        where_clause = "line_item_unblended_cost > 0"
        if additional_filters:
            where_clause += f" AND {additional_filters}"
        
        return f"""
        SELECT 
            {', '.join(all_columns)}
        FROM {table_name}
        WHERE {where_clause}
        GROUP BY {', '.join(all_group_by)}
        ORDER BY billing_month DESC, total_cost DESC
        """
    
    @staticmethod
    def build_service_summary(
        billing_period: Optional[str] = None,
        table_name: str = "CUR",
        limit: int = 20
    ) -> str:
        """
        Build service cost summary query.
        
        Args:
            billing_period: Billing period filter
            table_name: Source table name
            limit: Maximum number of services to return
            
        Returns:
            SQL query for service cost summary
        """
        base_filter = SQLPatterns.build_base_filter_clause(billing_period=billing_period)
        
        return f"""
        SELECT 
            product_servicecode as service,
            SUM(line_item_unblended_cost) as total_cost,
            COUNT(DISTINCT line_item_resource_id) as resource_count,
            COUNT(*) as line_item_count,
            AVG(line_item_unblended_cost) as avg_cost_per_line_item
        FROM {table_name}
        WHERE {base_filter}
        GROUP BY product_servicecode
        ORDER BY total_cost DESC
        LIMIT {limit}
        """
    
    @staticmethod
    def build_account_summary(
        billing_period: Optional[str] = None,
        table_name: str = "CUR"
    ) -> str:
        """
        Build account cost summary query.
        
        Args:
            billing_period: Billing period filter
            table_name: Source table name
            
        Returns:
            SQL query for account cost summary
        """
        base_filter = SQLPatterns.build_base_filter_clause(billing_period=billing_period)
        
        return f"""
        SELECT 
            line_item_usage_account_id as account_id,
            bill_payer_account_id as payer_account_id,
            SUM(line_item_unblended_cost) as total_cost,
            COUNT(DISTINCT product_servicecode) as service_count,
            COUNT(DISTINCT line_item_resource_id) as resource_count
        FROM {table_name}
        WHERE {base_filter}
        GROUP BY line_item_usage_account_id, bill_payer_account_id
        ORDER BY total_cost DESC
        """
    
    @staticmethod
    def build_resource_optimization_query(
        service_code: str,
        idle_threshold_days: int = 7,
        table_name: str = "CUR"
    ) -> str:
        """
        Build resource optimization analysis query.
        
        Args:
            service_code: AWS service code to analyze
            idle_threshold_days: Days threshold for considering resource idle
            table_name: Source table name
            
        Returns:
            SQL query for resource optimization analysis
        """
        return f"""
        WITH resource_usage AS (
            SELECT 
                line_item_resource_id,
                product_servicecode,
                SUM(line_item_unblended_cost) as total_cost,
                COUNT(DISTINCT DATE(line_item_usage_start_date)) as active_days,
                MIN(line_item_usage_start_date) as first_seen,
                MAX(line_item_usage_start_date) as last_seen,
                CURRENT_DATE - MAX(DATE(line_item_usage_start_date)) as days_since_last_use
            FROM {table_name}
            WHERE product_servicecode = '{service_code}'
                AND line_item_unblended_cost > 0
                AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY line_item_resource_id, product_servicecode
        )
        SELECT 
            line_item_resource_id,
            product_servicecode,
            total_cost,
            active_days,
            days_since_last_use,
            CASE 
                WHEN days_since_last_use > {idle_threshold_days} THEN 'idle'
                WHEN active_days < 7 THEN 'underutilized'
                ELSE 'active'
            END as optimization_opportunity,
            first_seen,
            last_seen
        FROM resource_usage
        WHERE days_since_last_use > 0
        ORDER BY total_cost DESC
        """
    
    @staticmethod
    def build_cost_trend_query(
        service_code: Optional[str] = None,
        months_back: int = 6,
        table_name: str = "CUR"
    ) -> str:
        """
        Build cost trend analysis query.
        
        Args:
            service_code: AWS service code to analyze (optional)
            months_back: Number of months to analyze
            table_name: Source table name
            
        Returns:
            SQL query for cost trend analysis
        """
        service_filter = f"AND product_servicecode = '{service_code}'" if service_code else ""
        
        return f"""
        WITH monthly_costs AS (
            SELECT 
                DATE_TRUNC('month', line_item_usage_start_date) as month,
                {'product_servicecode,' if not service_code else ''}
                SUM(line_item_unblended_cost) as monthly_cost
            FROM {table_name}
            WHERE line_item_unblended_cost > 0
                AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '{months_back} months'
                {service_filter}
            GROUP BY DATE_TRUNC('month', line_item_usage_start_date){', product_servicecode' if not service_code else ''}
        ),
        cost_with_lag AS (
            SELECT 
                month,
                {'product_servicecode,' if not service_code else ''}
                monthly_cost,
                LAG(monthly_cost) OVER (
                    {'PARTITION BY product_servicecode ' if not service_code else ''}
                    ORDER BY month
                ) as previous_month_cost
            FROM monthly_costs
        )
        SELECT 
            month,
            {'product_servicecode,' if not service_code else ''}
            monthly_cost,
            previous_month_cost,
            CASE 
                WHEN previous_month_cost IS NULL THEN NULL
                WHEN previous_month_cost = 0 THEN NULL
                ELSE ((monthly_cost - previous_month_cost) / previous_month_cost * 100)
            END as month_over_month_change_pct
        FROM cost_with_lag
        ORDER BY {'product_servicecode, ' if not service_code else ''}month DESC
        """


class QueryBuilder:
    """Advanced query builder for complex analytics queries."""
    
    def __init__(self, table_name: str = "CUR"):
        self.table_name = table_name
        self.select_columns = []
        self.where_conditions = []
        self.group_by_columns = []
        self.order_by_columns = []
        self.having_conditions = []
        self.limit_value = None
    
    def select(self, *columns: str) -> 'QueryBuilder':
        """Add SELECT columns."""
        self.select_columns.extend(columns)
        return self
    
    def where(self, condition: str) -> 'QueryBuilder':
        """Add WHERE condition."""
        self.where_conditions.append(condition)
        return self
    
    def group_by(self, *columns: str) -> 'QueryBuilder':
        """Add GROUP BY columns."""
        self.group_by_columns.extend(columns)
        return self
    
    def order_by(self, *columns: str) -> 'QueryBuilder':
        """Add ORDER BY columns."""
        self.order_by_columns.extend(columns)
        return self
    
    def having(self, condition: str) -> 'QueryBuilder':
        """Add HAVING condition."""
        self.having_conditions.append(condition)
        return self
    
    def limit(self, limit: int) -> 'QueryBuilder':
        """Set LIMIT."""
        self.limit_value = limit
        return self
    
    def build(self) -> str:
        """Build the final SQL query."""
        if not self.select_columns:
            raise ValueError("No SELECT columns specified")
        
        query_parts = [
            f"SELECT {', '.join(self.select_columns)}",
            f"FROM {self.table_name}"
        ]
        
        if self.where_conditions:
            query_parts.append(f"WHERE {' AND '.join(self.where_conditions)}")
        
        if self.group_by_columns:
            query_parts.append(f"GROUP BY {', '.join(self.group_by_columns)}")
        
        if self.having_conditions:
            query_parts.append(f"HAVING {' AND '.join(self.having_conditions)}")
        
        if self.order_by_columns:
            query_parts.append(f"ORDER BY {', '.join(self.order_by_columns)}")
        
        if self.limit_value:
            query_parts.append(f"LIMIT {self.limit_value}")
        
        return '\n'.join(query_parts)