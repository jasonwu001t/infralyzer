"""
Validation utilities for data quality and configuration validation
"""
import polars as pl
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import re


class DataValidator:
    """Utility for validating cost data quality and consistency."""
    
    @staticmethod
    def validate_cost_data(df: pl.DataFrame) -> Dict[str, Any]:
        """
        Validate cost data DataFrame for common quality issues.
        
        Args:
            df: Polars DataFrame with cost data
            
        Returns:
            Dictionary with validation results and recommendations
        """
        if df.is_empty():
            return {
                "valid": False,
                "issues": ["DataFrame is empty"],
                "recommendations": ["Check data source and filters"]
            }
        
        issues = []
        recommendations = []
        warnings = []
        
        # Check for required columns
        required_columns = ['line_item_unblended_cost']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            issues.append(f"Missing required columns: {missing_columns}")
            recommendations.append("Verify data export configuration")
        
        # Check for negative costs (should be rare)
        if 'line_item_unblended_cost' in df.columns:
            negative_costs = df.filter(pl.col('line_item_unblended_cost') < 0).height
            if negative_costs > 0:
                warnings.append(f"Found {negative_costs} rows with negative costs")
                recommendations.append("Review negative cost entries - may indicate credits or refunds")
        
        # Check for null values in critical columns
        critical_columns = ['line_item_unblended_cost', 'product_servicecode']
        for col in critical_columns:
            if col in df.columns:
                null_count = df.filter(pl.col(col).is_null()).height
                total_rows = df.height
                null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
                
                if null_percentage > 10:
                    issues.append(f"High null percentage in {col}: {null_percentage:.1f}%")
                    recommendations.append(f"Investigate data quality issues in {col}")
                elif null_percentage > 0:
                    warnings.append(f"Some null values in {col}: {null_percentage:.1f}%")
        
        # Check for data freshness
        if 'line_item_usage_start_date' in df.columns:
            try:
                latest_date = df.select(pl.col('line_item_usage_start_date').max()).item()
                if latest_date:
                    if isinstance(latest_date, str):
                        latest_date = datetime.fromisoformat(latest_date.replace('Z', '+00:00'))
                    
                    days_old = (datetime.now() - latest_date).days
                    if days_old > 7:
                        warnings.append(f"Data may be stale - latest date is {days_old} days old")
                        recommendations.append("Check if data refresh is needed")
            except Exception:
                warnings.append("Unable to validate data freshness")
        
        # Check for duplicate records
        if df.height > 0:
            unique_count = df.unique().height
            duplicate_count = df.height - unique_count
            if duplicate_count > 0:
                duplicate_percentage = (duplicate_count / df.height) * 100
                warnings.append(f"Found {duplicate_count} duplicate rows ({duplicate_percentage:.1f}%)")
                recommendations.append("Consider deduplication if duplicates are unexpected")
        
        return {
            "valid": len(issues) == 0,
            "total_rows": df.height,
            "total_columns": len(df.columns),
            "issues": issues,
            "warnings": warnings,
            "recommendations": recommendations,
            "data_quality_score": DataValidator._calculate_quality_score(issues, warnings, df.height)
        }
    
    @staticmethod
    def _calculate_quality_score(issues: List[str], warnings: List[str], total_rows: int) -> float:
        """Calculate a data quality score from 0-100."""
        if total_rows == 0:
            return 0.0
        
        score = 100.0
        
        # Deduct points for issues and warnings
        score -= len(issues) * 20  # Major issues
        score -= len(warnings) * 5  # Minor warnings
        
        return max(0.0, min(100.0, score))
    
    @staticmethod
    def validate_date_range(start_date: Optional[str], 
                          end_date: Optional[str], 
                          export_type: str) -> Dict[str, Any]:
        """
        Validate date range parameters for data export type.
        
        Args:
            start_date: Start date string
            end_date: End date string
            export_type: Data export type
            
        Returns:
            Validation result
        """
        issues = []
        
        # Expected date formats by export type
        expected_formats = {
            'CUR2.0': r'^\d{4}-\d{2}$',  # YYYY-MM
            'FOCUS1.0': r'^\d{4}-\d{2}$',  # YYYY-MM
            'COH': r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            'CARBON_EMISSION': r'^\d{4}-\d{2}$'  # YYYY-MM
        }
        
        format_pattern = expected_formats.get(export_type, r'^\d{4}-\d{2}$')
        format_description = {
            'CUR2.0': 'YYYY-MM (e.g., 2025-01)',
            'FOCUS1.0': 'YYYY-MM (e.g., 2025-01)', 
            'COH': 'YYYY-MM-DD (e.g., 2025-01-15)',
            'CARBON_EMISSION': 'YYYY-MM (e.g., 2025-01)'
        }.get(export_type, 'YYYY-MM')
        
        # Validate start_date format
        if start_date and not re.match(format_pattern, start_date):
            issues.append(f"start_date format invalid. Expected: {format_description}")
        
        # Validate end_date format
        if end_date and not re.match(format_pattern, end_date):
            issues.append(f"end_date format invalid. Expected: {format_description}")
        
        # Validate date logic
        if start_date and end_date and start_date > end_date:
            issues.append("start_date cannot be after end_date")
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "expected_format": format_description
        }


class ConfigValidator:
    """Utility for validating configuration parameters."""
    
    @staticmethod
    def validate_s3_config(s3_bucket: str, 
                          s3_prefix: str, 
                          data_export_type: str) -> Dict[str, Any]:
        """
        Validate S3 configuration parameters.
        
        Args:
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix path
            data_export_type: Data export type
            
        Returns:
            Validation result
        """
        issues = []
        warnings = []
        
        # Validate bucket name
        if not s3_bucket:
            issues.append("S3 bucket name is required")
        elif not ConfigValidator._is_valid_s3_bucket_name(s3_bucket):
            issues.append("S3 bucket name format is invalid")
        
        # Validate prefix
        if not s3_prefix:
            warnings.append("S3 prefix is empty - will search entire bucket")
        elif s3_prefix.startswith('/'):
            warnings.append("S3 prefix should not start with '/'")
        
        # Validate export type
        valid_export_types = ['CUR2.0', 'FOCUS1.0', 'COH', 'CARBON_EMISSION']
        if data_export_type not in valid_export_types:
            issues.append(f"Invalid data_export_type. Must be one of: {valid_export_types}")
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings
        }
    
    @staticmethod
    def _is_valid_s3_bucket_name(bucket_name: str) -> bool:
        """Validate S3 bucket name according to AWS rules."""
        if len(bucket_name) < 3 or len(bucket_name) > 63:
            return False
        
        # Must start and end with lowercase letter or number
        if not re.match(r'^[a-z0-9].*[a-z0-9]$', bucket_name):
            return False
        
        # Can contain lowercase letters, numbers, hyphens, and periods
        if not re.match(r'^[a-z0-9.-]+$', bucket_name):
            return False
        
        # Cannot contain consecutive periods
        if '..' in bucket_name:
            return False
        
        # Cannot be formatted as IP address
        if re.match(r'^\d+\.\d+\.\d+\.\d+$', bucket_name):
            return False
        
        return True
    
    @staticmethod
    def validate_local_path(local_path: str) -> Dict[str, Any]:
        """
        Validate local data path configuration.
        
        Args:
            local_path: Local directory path
            
        Returns:
            Validation result
        """
        issues = []
        warnings = []
        
        if not local_path:
            return {"valid": True, "issues": [], "warnings": ["No local path specified"]}
        
        # Check if path exists
        import os
        if not os.path.exists(local_path):
            warnings.append(f"Local path does not exist: {local_path}")
            
            # Check if parent directory exists and is writable
            parent_dir = os.path.dirname(local_path)
            if not os.path.exists(parent_dir):
                issues.append(f"Parent directory does not exist: {parent_dir}")
            elif not os.access(parent_dir, os.W_OK):
                issues.append(f"Cannot write to parent directory: {parent_dir}")
        else:
            # Check if path is writable
            if not os.access(local_path, os.W_OK):
                issues.append(f"Local path is not writable: {local_path}")
            
            # Check available space (warning if < 1GB)
            try:
                import shutil
                free_space = shutil.disk_usage(local_path).free
                if free_space < 1_000_000_000:  # 1GB
                    warnings.append(f"Low disk space available: {free_space / 1_000_000_000:.1f}GB")
            except Exception:
                warnings.append("Unable to check disk space")
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "warnings": warnings
        }