"""
Example usage of the new modular de-polars architecture

This demonstrates the new modular structure and capabilities:
- Independent access to each analytics module
- Unified FinOpsEngine interface
- FastAPI integration
- Backward compatibility with original DataExportsPolars
"""

# Example 1: Using the new FinOpsEngine (recommended)
from de_polars import FinOpsEngine, DataConfig, DataExportType

# Create configuration
config = DataConfig(
    s3_bucket='my-cost-data-bucket',
    s3_data_prefix='cur2/cur2/data',
    data_export_type=DataExportType.CUR_2_0,
    table_name='CUR',
    local_data_path='./local_data',  # Enable local caching
    prefer_local_data=True
)

# Initialize unified engine
engine = FinOpsEngine(config)

# Download data locally for cost savings (one-time setup)
print("Downloading data locally...")
engine.download_data_locally()

print("\n🎯 1. COMPREHENSIVE KPI SUMMARY")
print("=" * 50)
# Get comprehensive KPI summary (⭐ NEW primary endpoint)
kpi_summary = engine.kpi.get_comprehensive_summary()
print(f"Total Monthly Spend: ${kpi_summary['overall_spend']['spend_all_cost']:,.2f}")
print(f"Total Savings Potential: ${kpi_summary['savings_summary']['total_potential_savings']:,.2f}")

print("\n💰 2. SPEND ANALYTICS")
print("=" * 50)
# Spend analytics
spend_summary = engine.spend.get_invoice_summary()
print(f"Invoice Total: ${spend_summary['invoice_total']:,.2f}")
print(f"Month-over-Month Change: {spend_summary['mom_change']:+.1f}%")

top_services = engine.spend.get_top_services(limit=3)
print(f"Top 3 Services by Cost:")
for service in top_services['services'][:3]:
    print(f"  - {service['name']}: ${service['spend']:,.2f}")

print("\n⚡ 3. OPTIMIZATION RECOMMENDATIONS")
print("=" * 50)
# Optimization insights
idle_resources = engine.optimization.get_idle_resources()
print(f"Idle Resources Found: {len(idle_resources['idle_resources'])}")
print(f"Potential Savings: ${idle_resources['total_potential_savings']:,.2f}")

rightsizing = engine.optimization.get_rightsizing_recommendations()
print(f"Rightsizing Opportunities: {len(rightsizing['recommendations'])}")
print(f"Rightsizing Savings: ${rightsizing['savings_potential']:,.2f}")

print("\n🏷️ 4. COST ALLOCATION & TAGGING")
print("=" * 50)
# Cost allocation
tagging_compliance = engine.allocation.get_tagging_compliance()
print(f"Tagging Compliance Score: {tagging_compliance['compliance_score']:.1f}%")
print(f"Untagged Resources: {len(tagging_compliance['untagged_resources'])}")

print("\n💳 5. DISCOUNT TRACKING")
print("=" * 50)
# Discount analysis
agreements = engine.discounts.get_current_agreements()
print(f"Current Agreements: {len(agreements['agreements'])}")

total_commitment = sum(a['annual_commitment'] for a in agreements['agreements'])
print(f"Total Annual Commitments: ${total_commitment:,.2f}")

print("\n🤖 6. AI-POWERED INSIGHTS")
print("=" * 50)
# AI recommendations
ai_insights = engine.ai.get_optimization_insights()
print(f"AI Insights Generated: {len(ai_insights['insights'])}")

anomalies = engine.ai.detect_anomalies(lookback_days=7)
print(f"Cost Anomalies Detected: {len(anomalies['anomalies'])}")

print("\n🔌 7. MCP INTEGRATION")
print("=" * 50)
# MCP integration
mcp_resources = engine.mcp.get_mcp_resources()
print(f"MCP Resources Available: {len(mcp_resources['resources'])}")

# Natural language query through MCP
mcp_query = engine.mcp.process_mcp_query("What are my highest cost services?")
print(f"MCP Query Processed: {mcp_query['query']}")

print("\n📊 8. COMPREHENSIVE DASHBOARD DATA")
print("=" * 50)
# Get all dashboard data in one call
dashboard_data = engine.get_dashboard_data()
print("Dashboard data includes:")
for key in dashboard_data.keys():
    if key != 'metadata':
        print(f"  - {key}")

print("\n🏥 9. COST HEALTH CHECK")
print("=" * 50)
# Run comprehensive health check
health_check = engine.run_cost_health_check()
print(f"Overall Cost Health Score: {health_check['overall_score']:.1f}/100")
print("Category Scores:")
for category, score in health_check['category_scores'].items():
    print(f"  - {category}: {score:.1f}")

print("\n👔 10. EXECUTIVE SUMMARY")
print("=" * 50)
# Generate executive summary
exec_summary = engine.generate_executive_summary()
print("Executive Insights:")
for insight in exec_summary['executive_insights']:
    print(f"  • {insight}")

print("\n" + "=" * 70)
print("✅ NEW MODULAR ARCHITECTURE DEMO COMPLETE")
print("=" * 70)

# Example 2: Backward compatibility with original DataExportsPolars
print("\n📚 BACKWARD COMPATIBILITY EXAMPLE")
print("=" * 50)

from de_polars import DataExportsPolars

# Original API still works exactly the same
data = DataExportsPolars(
    s3_bucket='my-cost-data-bucket',
    s3_data_prefix='cur2/cur2/data', 
    data_export_type='CUR2.0',
    table_name='CUR',
    local_data_path='./local_data'
)

# Original methods work unchanged
result = data.query("SELECT product_servicecode, SUM(line_item_unblended_cost) as cost FROM CUR GROUP BY 1 ORDER BY 2 DESC LIMIT 5")
print(f"Top services query returned {len(result)} rows")
print("Backward compatibility maintained! ✅")

# Example 3: FastAPI Integration
print("\n🚀 FASTAPI INTEGRATION EXAMPLE")
print("=" * 50)

# FastAPI app creation
from de_polars.api import create_finops_app

# Create FastAPI app
app = create_finops_app(
    s3_bucket='my-cost-data-bucket',
    s3_data_prefix='cur2/cur2/data',
    data_export_type='CUR2.0',
    local_data_path='./local_data'
)

print("FastAPI app created with endpoints:")
print("  📊 GET /api/v1/finops/kpi/summary - Comprehensive KPI dashboard")
print("  💰 GET /api/v1/finops/spend/invoice/summary - Spend analytics")
print("  ⚡ GET /api/v1/finops/optimization/idle-resources - Optimization")
print("  🏷️ GET /api/v1/finops/allocation/tagging-compliance - Allocation")
print("  💳 GET /api/v1/finops/discounts/current-agreements - Discounts")
print("  🤖 GET /api/v1/finops/ai/anomaly-detection - AI insights")
print("  🔌 GET /api/v1/finops/mcp/resources - MCP integration")
print("  📖 GET /docs - Interactive API documentation")

print("\n🎉 COMPLETE FINOPS PLATFORM READY!")
print("All API endpoints available and fully functional!")

# To run the FastAPI server:
# uvicorn example_new_modular_usage:app --host 0.0.0.0 --port 8000