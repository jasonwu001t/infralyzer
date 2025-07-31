"""
Example usage of DE-Polars utility functions

This demonstrates the utility functions available in the utils package
for formatting, validation, performance monitoring, and data export.
"""

from de_polars import (
    FinOpsEngine, DataConfig, DataExportType,
    CurrencyFormatter, NumberFormatter, DateFormatter,
    DataValidator, ConfigValidator,
    QueryProfiler, CacheManager, 
    DataExporter, ReportGenerator
)

print("🔧 DE-POLARS UTILITIES DEMONSTRATION")
print("=" * 50)

# Example 1: Currency and Number Formatting
print("\n💰 1. CURRENCY & NUMBER FORMATTING")
print("-" * 30)

# Currency formatting
cost_value = 125432.50
print(f"Raw value: {cost_value}")
print(f"Formatted: {CurrencyFormatter.format_currency(cost_value)}")
print(f"Large format: {CurrencyFormatter.format_large_currency(cost_value)}")
print(f"No symbol: {CurrencyFormatter.format_currency(cost_value, include_symbol=False)}")

# Percentage formatting  
growth_rate = 15.7
print(f"\nGrowth rate: {NumberFormatter.format_percentage(growth_rate)}")
print(f"Negative rate: {NumberFormatter.format_percentage(-3.2)}")

# Large numbers
resource_count = 1500000
print(f"Resource count: {NumberFormatter.format_large_number(resource_count)}")

# Example 2: Date Formatting
print("\n📅 2. DATE FORMATTING")
print("-" * 30)

from datetime import datetime
current_date = datetime.now()
print(f"Current date: {current_date}")
print(f"Billing period: {DateFormatter.format_billing_period(current_date, 'CUR2.0', 'YYYY-MM')}")
print(f"Display format: {DateFormatter.format_billing_period(current_date, 'CUR2.0', 'Mon YYYY')}")
print(f"Relative: {DateFormatter.format_relative_date(current_date)}")

# Date range description
date_range = DateFormatter.get_date_range_description('2024-11', '2025-01', 'CUR2.0')
print(f"Date range: {date_range}")

# Example 3: Configuration Validation
print("\n✅ 3. CONFIGURATION VALIDATION")
print("-" * 30)

# Validate S3 configuration
s3_validation = ConfigValidator.validate_s3_config(
    s3_bucket='my-cost-data-bucket',
    s3_prefix='cur2/cur2/data',
    data_export_type='CUR2.0'
)
print(f"S3 config valid: {s3_validation['valid']}")
if s3_validation['warnings']:
    print(f"Warnings: {s3_validation['warnings']}")

# Validate date range
date_validation = DataValidator.validate_date_range('2025-01', '2025-02', 'CUR2.0')
print(f"Date range valid: {date_validation['valid']}")
print(f"Expected format: {date_validation['expected_format']}")

# Validate local path
local_validation = ConfigValidator.validate_local_path('./local_data')
print(f"Local path valid: {local_validation['valid']}")

# Example 4: Query Performance Monitoring
print("\n⚡ 4. PERFORMANCE MONITORING")
print("-" * 30)

# Create profiler and cache manager
profiler = QueryProfiler()
cache = CacheManager(default_ttl=60)  # 1 minute cache

# Example of profiling a function
@profiler.profile_query("example_query")
def example_expensive_query():
    """Simulate an expensive query."""
    import time
    time.sleep(0.1)  # Simulate 100ms query
    return {"result": "sample data", "rows": 1000}

# Run the profiled function a few times
for i in range(3):
    result = example_expensive_query()

# Get performance statistics
stats = profiler.get_query_stats("example_query")
print(f"Query stats: {stats}")

performance_summary = profiler.get_performance_summary()
print(f"Performance summary: {performance_summary}")

# Example 5: Caching
print("\n🗄️ 5. CACHING DEMONSTRATION")
print("-" * 30)

# Cache some data
cache.set("user_preferences", {"theme": "dark", "currency": "USD"})
cache.set("monthly_total", 50000.00, ttl=30)  # 30 second cache

# Retrieve from cache
preferences = cache.get("user_preferences")
monthly_total = cache.get("monthly_total")
print(f"Cached preferences: {preferences}")
print(f"Cached monthly total: {monthly_total}")

# Cache statistics
cache_stats = cache.get_stats()
print(f"Cache stats: {cache_stats}")

# Example 6: Data Export
print("\n📤 6. DATA EXPORT")
print("-" * 30)

# Example data to export
sample_data = {
    "summary": {
        "total_cost": 125000.50,
        "optimization_potential": 15000.25,
        "services_count": 12
    },
    "top_services": [
        {"service": "EC2", "cost": 45000.00},
        {"service": "RDS", "cost": 18000.00},
        {"service": "S3", "cost": 8500.00}
    ]
}

# Export as JSON
json_export = DataExporter.export_to_json(sample_data, indent=2)
print("JSON Export (first 200 chars):")
print(json_export[:200] + "...")

# Export as formatted text report
text_report = DataExporter.export_summary_report(sample_data, format="txt")
print("\nText Report (first 300 chars):")
print(text_report[:300] + "...")

# Example 7: Report Generation
print("\n📊 7. EXECUTIVE REPORT GENERATION")
print("-" * 30)

# Mock data for executive report
mock_kpi_data = {
    "overall_spend": {"spend_all_cost": 125000.50},
    "savings_summary": {"total_potential_savings": 18750.25}
}

mock_spend_data = {
    "mom_change": 8.5
}

mock_optimization_data = {
    "idle_resources": [{"id": "i-123"}, {"id": "i-456"}]
}

# Generate executive summary
executive_summary = ReportGenerator.generate_executive_summary(
    mock_kpi_data, 
    mock_spend_data, 
    mock_optimization_data
)

print("Executive Summary:")
for key, value in executive_summary["executive_summary"]["key_metrics"].items():
    if isinstance(value, float) and "cost" in key:
        formatted_value = CurrencyFormatter.format_currency(value)
    elif isinstance(value, float) and "percentage" in key:
        formatted_value = NumberFormatter.format_percentage(value)
    else:
        formatted_value = str(value)
    print(f"  {key.replace('_', ' ').title()}: {formatted_value}")

print(f"\nHighlights:")
for highlight in executive_summary["executive_summary"]["highlights"]:
    print(f"  • {highlight}")

print(f"\nRecommendations:")
for rec in executive_summary["executive_summary"]["recommendations"]:
    print(f"  • {rec}")

# Example 8: Data Quality Validation (if you have real data)
print("\n🔍 8. DATA QUALITY VALIDATION")
print("-" * 30)

try:
    # This would work with actual data
    config = DataConfig(
        s3_bucket='example-bucket',
        s3_data_prefix='cur2/cur2/data',
        data_export_type=DataExportType.CUR_2_0,
        table_name='CUR'
    )
    
    print("✅ Configuration created successfully")
    print(f"   Export type: {config.data_export_type.value}")
    print(f"   Partition format: {config.partition_format}")
    print(f"   Date format: {config.date_format}")
    
except Exception as e:
    print(f"ℹ️  Config example: {e}")

print("\n" + "=" * 60)
print("✅ UTILITIES DEMONSTRATION COMPLETE")
print("=" * 60)
print("\n💡 Available Utility Classes:")
print("   🏷️  CurrencyFormatter - Format currency values")
print("   📊 NumberFormatter - Format numbers and percentages")
print("   📅 DateFormatter - Format dates and billing periods")
print("   ✅ DataValidator - Validate data quality")
print("   🔧 ConfigValidator - Validate configurations")
print("   ⚡ QueryProfiler - Monitor query performance")
print("   🗄️  CacheManager - In-memory caching")
print("   📤 DataExporter - Export data in multiple formats")
print("   📋 ReportGenerator - Generate executive reports")

print("\n🚀 These utilities support the entire FinOps analytics platform!")