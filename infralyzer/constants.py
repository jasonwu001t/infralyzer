"""
Infralyzer Constants - Centralized constants and configuration values.
"""

# Version information
VERSION = "1.0.0"

# Default configurations
DEFAULT_ENGINE = "duckdb"
DEFAULT_QUERY_FORMAT = "records"
DEFAULT_SAMPLE_SIZE = 10
DEFAULT_TIMEOUT_SECONDS = 300

# Supported engines
SUPPORTED_ENGINES = ["duckdb", "polars", "athena"]

# Query result formats
QUERY_FORMATS = ["records", "dataframe", "csv", "arrow", "raw"]

# Cache settings
DEFAULT_CACHE_SIZE = 100
DEFAULT_CACHE_TTL = 3600  # 1 hour

# Data validation thresholds
MAX_NULL_PERCENTAGE = 10.0
DATA_FRESHNESS_DAYS = 7
ANOMALY_DETECTION_SENSITIVITY = 2.0

# AWS service codes for optimization
COMPUTE_SERVICES = [
    "AmazonEC2",
    "AWSLambda", 
    "AmazonECS",
    "AmazonEKS"
]

STORAGE_SERVICES = [
    "AmazonS3",
    "AmazonEBS",
    "AmazonEFS",
    "AmazonFSx"
]

DATABASE_SERVICES = [
    "AmazonRDS",
    "AmazonRedshift",
    "AmazonDynamoDB",
    "AmazonElastiCache"
]

# API configurations
API_PREFIX = "/api/v1/finops"
MAX_QUERY_RESULTS = 10000
DEFAULT_PAGINATION_SIZE = 100

# File paths
DEFAULT_LOCAL_DATA_PATH = "./local_data"
DEFAULT_CACHE_PATH = "./cache"
DEFAULT_EXPORT_PATH = "./exports"

# Date formats
DATE_FORMAT_MONTHLY = "%Y-%m"
DATE_FORMAT_DAILY = "%Y-%m-%d"
DATE_FORMAT_ISO = "%Y-%m-%dT%H:%M:%S"

# SQL query patterns
BASIC_CUR_COLUMNS = [
    "line_item_usage_start_date",
    "line_item_usage_end_date", 
    "line_item_unblended_cost",
    "line_item_resource_id",
    "product_servicecode",
    "product_region",
    "bill_payer_account_id",
    "line_item_usage_account_id"
]

# Optimization thresholds
IDLE_RESOURCE_THRESHOLD_DAYS = 7
HIGH_COST_THRESHOLD = 1000.0
SAVINGS_OPPORTUNITY_MIN = 100.0
USAGE_EFFICIENCY_THRESHOLD = 0.3

# Error messages
ERROR_MESSAGES = {
    "invalid_engine": "Invalid query engine. Supported engines: {engines}",
    "missing_config": "Missing required configuration parameter: {param}",
    "data_not_found": "No data found for the specified criteria",
    "query_timeout": "Query execution timed out after {timeout} seconds", 
    "invalid_date_format": "Invalid date format. Expected: {format}",
    "insufficient_permissions": "Insufficient permissions to access resource: {resource}"
}

# Log levels
LOG_LEVELS = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50
}