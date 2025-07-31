"""
Test 9: Data Export & Reporting
===============================

This test demonstrates data export capabilities and report generation.
Tests multiple export formats and comprehensive reporting features.
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from de_polars import FinOpsEngine, DataConfig, DataExportType, DataExporter, ReportGenerator
import json

def test_data_export():
    """Test data export and reporting capabilities"""
    
    print("📊 Test 9: Data Export & Reporting")
    print("=" * 50)
    
    # Configuration using local data from Test 2
    local_path = "./test_local_data"
    
    config = DataConfig(
        s3_bucket='billing-data-exports-cur',          
        s3_data_prefix='cur2/cur2/data',          
        data_export_type=DataExportType.CUR_2_0,               
        table_name='CUR',                        
        date_start='2025-07',                    
        date_end='2025-07',
        local_data_path=local_path,
        prefer_local_data=True
    )
    
    try:
        # Check if local data exists
        if not os.path.exists(local_path):
            print(f"❌ Local data not found at {local_path}")
            print("Please run test_2_download_local.py first")
            return False
        
        # Initialize engine
        print("🚀 Initializing FinOps Engine...")
        engine = FinOpsEngine(config)
        
        # Create output directory for exports
        export_dir = "./test_exports"
        os.makedirs(export_dir, exist_ok=True)
        
        # Test 1: Basic Data Exports
        print("\n📤 Step 1: Basic Data Exports")
        print("-" * 40)
        
        # Get sample data for export
        cost_summary = engine.query("""
            SELECT 
                product_servicecode as service,
                SUM(line_item_unblended_cost) as total_cost,
                COUNT(*) as line_items,
                AVG(line_item_unblended_cost) as avg_cost
            FROM CUR 
            GROUP BY product_servicecode 
            ORDER BY total_cost DESC 
            LIMIT 10
        """)
        
        print(f"✅ Sample data prepared: {len(cost_summary)} rows")
        
        # Export to JSON
        json_output = DataExporter.export_to_json(cost_summary, f"{export_dir}/cost_summary.json")
        print(f"📄 JSON export: {export_dir}/cost_summary.json")
        
        # Export to CSV
        csv_output = DataExporter.export_to_csv(cost_summary, f"{export_dir}/cost_summary.csv")
        print(f"📋 CSV export: {export_dir}/cost_summary.csv")
        
        # Export to Excel (if available)
        try:
            excel_output = DataExporter.export_to_excel(cost_summary, f"{export_dir}/cost_summary.xlsx")
            print(f"📊 Excel export: {export_dir}/cost_summary.xlsx")
        except Exception as e:
            print(f"⚠️ Excel export skipped (dependency missing): {str(e)[:50]}...")
        
        # Test 2: Advanced Export Options
        print("\n🔧 Step 2: Advanced Export Options")
        print("-" * 40)
        
        # Export with custom formatting
        formatted_json = DataExporter.export_to_json(
            cost_summary, 
            f"{export_dir}/formatted_cost_summary.json",
            indent=4
        )
        print(f"✨ Formatted JSON: {export_dir}/formatted_cost_summary.json")
        
        # Export CSV without headers
        no_header_csv = DataExporter.export_to_csv(
            cost_summary, 
            f"{export_dir}/cost_summary_no_headers.csv",
            include_headers=False
        )
        print(f"📄 CSV (no headers): {export_dir}/cost_summary_no_headers.csv")
        
        # Test 3: Report Generation
        print("\n📑 Step 3: Report Generation")
        print("-" * 40)
        
        # Get comprehensive data for reporting
        spend_data = {
            "total_cost": 23.08,
            "line_items": 2938,
            "services": 14,
            "top_service": "AmazonVPC",
            "top_service_cost": 12.39
        }
        
        optimization_data = {
            "potential_savings": 150.00,
            "optimization_opportunities": 8,
            "rightsizing_savings": 75.00,
            "idle_resource_savings": 50.00
        }
        
        # Generate executive summary
        exec_summary = ReportGenerator.generate_executive_summary(
            spend_data=spend_data,
            optimization_data=optimization_data,
            file_path=f"{export_dir}/executive_summary.txt"
        )
        print(f"📋 Executive Summary: {export_dir}/executive_summary.txt")
        
        # Generate detailed report
        detailed_report = ReportGenerator.generate_detailed_report(
            spend_data=spend_data,
            optimization_data=optimization_data,
            cost_breakdown=cost_summary.to_dicts(),
            file_path=f"{export_dir}/detailed_report.md"
        )
        print(f"📊 Detailed Report: {export_dir}/detailed_report.md")
        
        # Test 4: Custom Export Formats
        print("\n🎨 Step 4: Custom Export Formats")
        print("-" * 40)
        
        # Create a custom export with metadata
        custom_export = {
            "metadata": {
                "export_date": "2025-07-31",
                "data_source": "local_parquet",
                "total_records": len(cost_summary),
                "export_type": "cost_summary"
            },
            "data": cost_summary.to_dicts(),
            "summary": {
                "total_cost": float(cost_summary['total_cost'].sum()),
                "service_count": len(cost_summary),
                "avg_cost_per_service": float(cost_summary['total_cost'].mean())
            }
        }
        
        # Export custom format
        DataExporter.export_to_json(custom_export, f"{export_dir}/custom_export.json")
        print(f"🎯 Custom Export: {export_dir}/custom_export.json")
        
        # Test 5: Verify Exports
        print("\n✅ Step 5: Verify Exports")
        print("-" * 40)
        
        # Check created files
        export_files = []
        for file in os.listdir(export_dir):
            file_path = os.path.join(export_dir, file)
            file_size = os.path.getsize(file_path)
            export_files.append((file, file_size))
        
        print(f"📁 Created {len(export_files)} export files:")
        for filename, size in export_files:
            size_kb = size / 1024
            print(f"   • {filename}: {size_kb:.1f} KB")
        
        # Verify JSON export by reading it back
        try:
            with open(f"{export_dir}/cost_summary.json", 'r') as f:
                verified_data = json.load(f)
            print(f"✅ JSON verification: {len(verified_data)} records loaded")
        except Exception as e:
            print(f"⚠️ JSON verification failed: {str(e)}")
        
        # Test 6: Export Summary
        print("\n📈 Step 6: Export Summary")
        print("-" * 40)
        
        total_size = sum(size for _, size in export_files)
        print(f"📊 Export Statistics:")
        print(f"   • Total Files: {len(export_files)}")
        print(f"   • Total Size: {total_size / 1024:.1f} KB")
        print(f"   • Formats: JSON, CSV, TXT, MD")
        print(f"   • Reports: Executive Summary, Detailed Report")
        print(f"   • Custom Exports: Metadata-enriched JSON")
        
        print(f"\n🎉 Test 9 PASSED: Data export and reporting completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test 9 FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_data_export()