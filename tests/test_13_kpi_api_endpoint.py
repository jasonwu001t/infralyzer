#!/usr/bin/env python3
"""
Test 13: KPI API Endpoint - Direct API testing without FastAPI server
Tests the KPI summary endpoint functionality directly
"""

import sys
import os
import json
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from de_polars import FinOpsEngine
from de_polars.engine.data_config import DataConfig, DataExportType


def main():
    print("🚀 Test 13: KPI API Endpoint - Direct Testing")
    print("=" * 60)
    
    try:
        # Configure for CUR 2.0 data (ensuring we use only CUR2.0 data)
        print("📊 Configuring for CUR 2.0 data source...")
        
        current_dir = os.getcwd()
        if current_dir.endswith('/tests'):
            local_data_path = '../test_local_data'
        else:
            local_data_path = 'test_local_data'
        
        config = DataConfig(
            s3_bucket='billing-data-exports-cur',
            s3_data_prefix='cur2/cur2/data', 
            data_export_type=DataExportType.CUR_2_0,
            table_name='CUR',
            date_start='2025-07',
            date_end='2025-07',
            local_data_path=local_data_path,
            prefer_local_data=True
        )
        
        # Initialize FinOps engine
        print("🔧 Initializing FinOps engine...")
        engine = FinOpsEngine(config)
        print("✅ Engine initialized successfully")
        
        # Test 1: Basic KPI Summary (no filters)
        print("\n🎯 Test 1: Basic KPI Summary")
        print("-" * 40)
        
        result = engine.kpi.get_comprehensive_summary()
        
        if "error" in result:
            print(f"❌ Error: {result['message']}")
            return
        
        print("✅ KPI Summary API executed successfully!")
        
        # Save full result to file
        output_file = "kpi_api_result.json"
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"💾 Full result saved to: {output_file}")
        
        # Test 2: Display Key Metrics
        print("\n📊 Test 2: Key Metrics Display")
        print("-" * 40)
        
        print(f"🏢 Metadata:")
        print(f"   📅 Query Date: {result['summary_metadata']['query_date']}")
        print(f"   📊 Data Source: {result['summary_metadata']['data_source']}")
        print(f"   🗃️ Export Type: {result['summary_metadata']['data_export_type']}")
        print(f"   📁 Records: {result['summary_metadata']['records_analyzed']}")
        
        print(f"\n💰 Overall Spend:")
        print(f"   💵 Total Cost: ${result['overall_spend']['spend_all_cost']:.2f}")
        print(f"   📅 Period: {result['overall_spend']['billing_period']}")
        print(f"   🏢 Account: {result['overall_spend']['payer_account_id']}")
        
        print(f"\n🖥️ EC2 Metrics:")
        print(f"   💵 Total Cost: ${result['ec2_metrics']['ec2_all_cost']:.2f}")
        print(f"   🎯 Usage Cost: ${result['ec2_metrics']['ec2_usage_cost']:.2f}")
        print(f"   ⚡ Spot Cost: ${result['ec2_metrics']['ec2_spot_cost']:.2f}")
        print(f"   🌱 Graviton Cost: ${result['ec2_metrics']['ec2_graviton_cost']:.2f}")
        
        print(f"\n🗄️ RDS Metrics:")
        print(f"   💵 Total Cost: ${result['rds_metrics']['rds_all_cost']:.2f}")
        print(f"   🎯 OnDemand Cost: ${result['rds_metrics']['rds_ondemand_cost']:.2f}")
        print(f"   🌱 Graviton Cost: ${result['rds_metrics']['rds_graviton_cost']:.2f}")
        
        print(f"\n💾 Storage Metrics:")
        print(f"   📀 EBS Cost: ${result['storage_metrics']['ebs_all_cost']:.2f}")
        print(f"   📷 Snapshot Cost: ${result['storage_metrics']['ebs_snapshot_cost']:.2f}")
        print(f"   🪣 S3 Cost: ${result['storage_metrics']['s3_all_storage_cost']:.2f}")
        
        print(f"\n⚡ Compute Services:")
        print(f"   💻 Total Cost: ${result['compute_services']['compute_all_cost']:.2f}")
        print(f"   λ Lambda Cost: ${result['compute_services']['lambda_all_cost']:.2f}")
        print(f"   🔗 DynamoDB Cost: ${result['compute_services']['dynamodb_all_cost']:.2f}")
        
        print(f"\n💡 Savings Summary:")
        print(f"   🎯 Total Potential: ${result['savings_summary']['total_potential_savings']:.2f}")
        print(f"   🌱 Graviton Potential: ${result['savings_summary']['graviton_savings_potential']:.2f}")
        print(f"   📊 Commitment Potential: ${result['savings_summary']['commitment_savings_potential']:.2f}")
        print(f"   💾 Storage Optimization: ${result['savings_summary']['storage_optimization_potential']:.2f}")
        print(f"   ⚡ Spot Instance Potential: ${result['savings_summary']['spot_instance_potential']:.2f}")
        print(f"   💰 Current Monthly Savings: ${result['savings_summary']['current_monthly_savings']:.2f}")
        print(f"   📅 Annualized Opportunity: ${result['savings_summary']['annualized_savings_opportunity']:.2f}")
        
        # Test 3: Filtered KPI Summary
        print("\n🎯 Test 3: Filtered KPI Summary")
        print("-" * 40)
        
        filtered_result = engine.kpi.get_comprehensive_summary(
            billing_period="2025-07-01 00:00:00",
            payer_account_id="014498620306"
        )
        
        if "error" not in filtered_result:
            print("✅ Filtered KPI summary executed successfully!")
            print(f"   💵 Filtered Total: ${filtered_result['overall_spend']['spend_all_cost']:.2f}")
            print(f"   📅 Period Filter: {filtered_result['overall_spend']['billing_period']}")
        else:
            print(f"⚠️ Filtered query returned: {filtered_result['message']}")
        
        # Test 4: API Response Structure Validation
        print("\n🎯 Test 4: API Response Structure Validation")
        print("-" * 40)
        
        required_sections = [
            'summary_metadata', 'overall_spend', 'ec2_metrics', 
            'rds_metrics', 'storage_metrics', 'compute_services', 'savings_summary'
        ]
        
        missing_sections = []
        for section in required_sections:
            if section not in result:
                missing_sections.append(section)
        
        if missing_sections:
            print(f"❌ Missing sections: {', '.join(missing_sections)}")
        else:
            print("✅ All required API sections present")
            
        # Count total fields
        total_fields = sum(len(result[section]) if isinstance(result[section], dict) else 1 
                          for section in required_sections)
        print(f"📊 Total API fields: {total_fields}")
        
        print(f"\n🎉 KPI API Endpoint test completed successfully!")
        print(f"📁 Results saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()