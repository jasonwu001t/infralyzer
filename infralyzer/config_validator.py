"""
Configuration validation utilities for Infralyzer.
"""
from typing import Dict, Any, List, Optional
import os
from pathlib import Path
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from .exceptions import ConfigurationError, AuthenticationError, ErrorCodes
from .logging_config import get_logger
from .constants import SUPPORTED_ENGINES, ERROR_MESSAGES


class ConfigValidator:
    """Comprehensive configuration validation for Infralyzer."""
    
    def __init__(self):
        self.logger = get_logger("infralyzer.ConfigValidator")
    
    def validate_engine_config(self, engine_name: str) -> None:
        """
        Validate engine configuration.
        
        Args:
            engine_name: Name of the query engine to validate
            
        Raises:
            ConfigurationError: If engine configuration is invalid
        """
        if not isinstance(engine_name, str):
            raise ConfigurationError(
                "Engine name must be a string",
                error_code=ErrorCodes.INVALID_CONFIG,
                context={"provided_type": type(engine_name).__name__}
            )
        
        if engine_name.lower() not in SUPPORTED_ENGINES:
            raise ConfigurationError(
                ERROR_MESSAGES["invalid_engine"].format(engines=", ".join(SUPPORTED_ENGINES)),
                error_code=ErrorCodes.INVALID_ENGINE_NAME,
                context={"requested_engine": engine_name, "supported_engines": SUPPORTED_ENGINES}
            )
        
        self.logger.debug(f"Engine configuration validated: {engine_name}")
    
    def validate_s3_config(self, s3_bucket: str, s3_data_prefix: str) -> Dict[str, Any]:
        """
        Validate S3 configuration and test access.
        
        Args:
            s3_bucket: S3 bucket name
            s3_data_prefix: S3 data prefix path
            
        Returns:
            Dictionary with validation results
            
        Raises:
            ConfigurationError: If S3 configuration is invalid
            AuthenticationError: If S3 access fails
        """
        validation_result = {
            "bucket_accessible": False,
            "prefix_exists": False,
            "has_data_files": False,
            "error_message": None
        }
        
        # Basic validation
        if not s3_bucket or not isinstance(s3_bucket, str):
            raise ConfigurationError(
                "S3 bucket name must be a non-empty string",
                error_code=ErrorCodes.INVALID_CONFIG,
                context={"s3_bucket": s3_bucket}
            )
        
        if not s3_data_prefix or not isinstance(s3_data_prefix, str):
            raise ConfigurationError(
                "S3 data prefix must be a non-empty string",
                error_code=ErrorCodes.INVALID_CONFIG,
                context={"s3_data_prefix": s3_data_prefix}
            )
        
        try:
            # Test S3 access
            s3_client = boto3.client('s3')
            
            # Check bucket access
            try:
                s3_client.head_bucket(Bucket=s3_bucket)
                validation_result["bucket_accessible"] = True
                self.logger.debug(f"S3 bucket accessible: {s3_bucket}")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    raise ConfigurationError(
                        f"S3 bucket not found: {s3_bucket}",
                        error_code=ErrorCodes.S3_BUCKET_NOT_FOUND,
                        context={"s3_bucket": s3_bucket}
                    )
                elif e.response['Error']['Code'] == '403':
                    raise AuthenticationError(
                        f"Access denied to S3 bucket: {s3_bucket}",
                        error_code=ErrorCodes.S3_ACCESS_DENIED,
                        context={"s3_bucket": s3_bucket}
                    )
                else:
                    raise
            
            # Check prefix and data files
            try:
                response = s3_client.list_objects_v2(
                    Bucket=s3_bucket,
                    Prefix=s3_data_prefix,
                    MaxKeys=10
                )
                
                if 'Contents' in response:
                    validation_result["prefix_exists"] = True
                    
                    # Check for parquet files
                    parquet_files = [
                        obj for obj in response['Contents'] 
                        if obj['Key'].endswith('.parquet')
                    ]
                    
                    if parquet_files:
                        validation_result["has_data_files"] = True
                        self.logger.info(f"Found {len(parquet_files)} data files in S3")
                    else:
                        validation_result["error_message"] = "No parquet files found in the specified prefix"
                        
            except ClientError:
                validation_result["error_message"] = "Unable to list objects in S3 prefix"
        
        except NoCredentialsError:
            raise AuthenticationError(
                "AWS credentials not found or invalid",
                error_code=ErrorCodes.INVALID_CREDENTIALS,
                context={"aws_profile": os.environ.get("AWS_PROFILE", "default")}
            )
        
        return validation_result
    
    def validate_local_config(self, local_data_path: str) -> Dict[str, Any]:
        """
        Validate local data configuration.
        
        Args:
            local_data_path: Path to local data directory
            
        Returns:
            Dictionary with validation results
        """
        validation_result = {
            "path_exists": False,
            "is_directory": False,
            "has_data_files": False,
            "total_files": 0,
            "total_size_mb": 0.0,
            "error_message": None
        }
        
        try:
            path = Path(local_data_path)
            
            if path.exists():
                validation_result["path_exists"] = True
                
                if path.is_dir():
                    validation_result["is_directory"] = True
                    
                    # Count parquet files
                    parquet_files = list(path.glob("**/*.parquet"))
                    validation_result["total_files"] = len(parquet_files)
                    
                    if parquet_files:
                        validation_result["has_data_files"] = True
                        
                        # Calculate total size
                        total_size = sum(f.stat().st_size for f in parquet_files)
                        validation_result["total_size_mb"] = total_size / (1024 * 1024)
                        
                        self.logger.info(f"Found {len(parquet_files)} local data files ({validation_result['total_size_mb']:.1f} MB)")
                    else:
                        validation_result["error_message"] = "No parquet files found in local directory"
                else:
                    validation_result["error_message"] = "Local data path is not a directory"
            else:
                validation_result["error_message"] = "Local data path does not exist"
                
        except Exception as e:
            validation_result["error_message"] = f"Error accessing local path: {str(e)}"
            self.logger.error(f"Local path validation failed: {str(e)}")
        
        return validation_result
    
    def validate_data_export_type(self, data_export_type: str) -> None:
        """
        Validate data export type configuration.
        
        Args:
            data_export_type: Type of AWS data export
            
        Raises:
            ConfigurationError: If data export type is invalid
        """
        valid_types = ["CUR_2_0", "FOCUS_1_0", "COH"]
        
        if data_export_type not in valid_types:
            raise ConfigurationError(
                f"Invalid data export type: {data_export_type}. Valid types: {valid_types}",
                error_code=ErrorCodes.INVALID_CONFIG,
                context={"provided_type": data_export_type, "valid_types": valid_types}
            )
        
        self.logger.debug(f"Data export type validated: {data_export_type}")
    
    def run_comprehensive_validation(self, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run comprehensive validation on configuration.
        
        Args:
            config_dict: Configuration dictionary to validate
            
        Returns:
            Comprehensive validation results
        """
        results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "s3_validation": None,
            "local_validation": None,
            "recommendations": []
        }
        
        try:
            # Validate required fields
            required_fields = ["s3_bucket", "s3_data_prefix", "data_export_type"]
            for field in required_fields:
                if field not in config_dict or not config_dict[field]:
                    results["errors"].append(f"Missing required field: {field}")
                    results["valid"] = False
            
            if not results["valid"]:
                return results
            
            # Validate engine if specified
            engine_name = config_dict.get("engine_name", "duckdb")
            try:
                self.validate_engine_config(engine_name)
            except ConfigurationError as e:
                results["errors"].append(str(e))
                results["valid"] = False
            
            # Validate data export type
            try:
                self.validate_data_export_type(config_dict["data_export_type"])
            except ConfigurationError as e:
                results["errors"].append(str(e))
                results["valid"] = False
            
            # Validate S3 configuration
            try:
                s3_validation = self.validate_s3_config(
                    config_dict["s3_bucket"],
                    config_dict["s3_data_prefix"]
                )
                results["s3_validation"] = s3_validation
                
                if not s3_validation["has_data_files"]:
                    results["warnings"].append("No data files found in S3 - check your data export configuration")
                    
            except (ConfigurationError, AuthenticationError) as e:
                results["errors"].append(f"S3 validation failed: {str(e)}")
                results["valid"] = False
            
            # Validate local configuration if specified
            if "local_data_path" in config_dict and config_dict["local_data_path"]:
                local_validation = self.validate_local_config(config_dict["local_data_path"])
                results["local_validation"] = local_validation
                
                if not local_validation["has_data_files"]:
                    results["recommendations"].append("Consider downloading S3 data locally for better performance")
                elif local_validation["total_size_mb"] > 1000:  # > 1GB
                    results["recommendations"].append("Large local dataset detected - consider using DuckDB engine for optimal performance")
            
            # Add general recommendations
            if results["valid"]:
                if config_dict.get("prefer_local_data", False) and not results.get("local_validation", {}).get("has_data_files", False):
                    results["warnings"].append("prefer_local_data is True but no local data files found")
                
                results["recommendations"].append("Regularly update local data cache for optimal performance")
                results["recommendations"].append("Monitor query performance and consider engine switching based on workload")
        
        except Exception as e:
            results["errors"].append(f"Validation failed with unexpected error: {str(e)}")
            results["valid"] = False
            self.logger.error(f"Comprehensive validation failed: {str(e)}")
        
        return results