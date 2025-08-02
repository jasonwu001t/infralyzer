"""
Test 7: Optimization Analytics
==============================

This test demonstrates cost optimization recommendations and insights.
Tests idle resources, rightsizing, and cross-service migration opportunities.
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from de_polars import FinOpsEngine, DataConfig, DataExportType

def test_optimization_analytics():
    """Test optimization analytics capabilities"""
    
    print("🎯 Test 7: Optimization Analytics")
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
        
        # Get optimization analytics module
        optimization = engine.optimization
        
        # Test 1: Idle Resources Detection
        print("\n💤 Step 1: Detect Idle Resources")
        print("-" * 40)
        
        idle_resources = optimization.get_idle_resources(utilization_threshold=5.0)
        print(f"✅ Idle Resources Analysis: {len(idle_resources.get('idle_resources', []))} resources found")
        
        if idle_resources.get('summary'):
            summary = idle_resources['summary']
            print(f"💰 Potential Savings: ${summary.get('total_potential_savings', 0):.2f}")
            print(f"📊 Total Resources Analyzed: {summary.get('total_resources_analyzed', 0)}")
        
        # Show top idle resources
        resources = idle_resources.get('idle_resources', [])[:3]
        if resources:
            print(f"🔍 Top Idle Resources:")
            for i, resource in enumerate(resources, 1):
                savings = resource.get('monthly_savings', 0)
                resource_type = resource.get('resource_type', 'Unknown')
                print(f"   {i}. {resource_type}: ${savings:.2f}/month savings")
        
        # Test 2: Rightsizing Recommendations
        print("\n📏 Step 2: Rightsizing Recommendations")
        print("-" * 40)
        
        rightsizing = optimization.get_rightsizing_recommendations()
        print(f"✅ Rightsizing Analysis: {len(rightsizing.get('recommendations', []))} recommendations")
        
        if rightsizing.get('summary'):
            summary = rightsizing['summary']
            print(f"💰 Potential Annual Savings: ${summary.get('total_annual_savings', 0):.2f}")
            print(f"🏭 Instances Analyzed: {summary.get('instances_analyzed', 0)}")
        
        # Show top recommendations
        recommendations = rightsizing.get('recommendations', [])[:3]
        if recommendations:
            print(f"🎯 Top Rightsizing Recommendations:")
            for i, rec in enumerate(recommendations, 1):
                current = rec.get('current_instance_type', 'Unknown')
                recommended = rec.get('recommended_instance_type', 'Unknown')
                savings = rec.get('monthly_savings', 0)
                print(f"   {i}. {current} → {recommended}: ${savings:.2f}/month")
        
        # Test 3: Cross-Service Migration Opportunities
        print("\n🔄 Step 3: Cross-Service Migration Opportunities")
        print("-" * 40)
        
        migration = optimization.get_cross_service_migration_opportunities()
        print(f"✅ Migration Analysis: {len(migration.get('opportunities', []))} opportunities found")
        
        if migration.get('summary'):
            summary = migration['summary']
            print(f"💰 Total Migration Savings: ${summary.get('total_potential_savings', 0):.2f}")
            print(f"🔄 Migration Scenarios: {summary.get('migration_scenarios', 0)}")
        
        # Show migration opportunities
        opportunities = migration.get('opportunities', [])[:3]
        if opportunities:
            print(f"🚀 Top Migration Opportunities:")
            for i, opp in enumerate(opportunities, 1):
                from_service = opp.get('from_service', 'Unknown')
                to_service = opp.get('to_service', 'Unknown')
                savings = opp.get('monthly_savings', 0)
                print(f"   {i}. {from_service} → {to_service}: ${savings:.2f}/month")
        
        # Test 4: VPC Optimization
        print("\n🌐 Step 4: VPC Optimization Recommendations")
        print("-" * 40)
        
        vpc_optimization = optimization.get_vpc_optimization_recommendations()
        print(f"✅ VPC Analysis: {len(vpc_optimization.get('recommendations', []))} recommendations")
        
        if vpc_optimization.get('summary'):
            summary = vpc_optimization['summary']
            print(f"💰 VPC Savings Potential: ${summary.get('total_potential_savings', 0):.2f}")
            print(f"🌐 VPCs Analyzed: {summary.get('vpcs_analyzed', 0)}")
        
        # Show VPC recommendations
        vpc_recs = vpc_optimization.get('recommendations', [])[:3]
        if vpc_recs:
            print(f"🔧 Top VPC Optimizations:")
            for i, rec in enumerate(vpc_recs, 1):
                optimization_type = rec.get('optimization_type', 'Unknown')
                savings = rec.get('monthly_savings', 0)
                print(f"   {i}. {optimization_type}: ${savings:.2f}/month")
        
        # Test 5: Summary of All Optimizations
        print("\n📊 Step 5: Optimization Summary")
        print("-" * 40)
        
        # Calculate total potential savings
        total_savings = 0
        savings_sources = []
        
        if idle_resources.get('summary', {}).get('total_potential_savings'):
            idle_savings = idle_resources['summary']['total_potential_savings']
            total_savings += idle_savings
            savings_sources.append(f"Idle Resources: ${idle_savings:.2f}")
        
        if rightsizing.get('summary', {}).get('total_annual_savings'):
            rightsizing_monthly = rightsizing['summary']['total_annual_savings'] / 12
            total_savings += rightsizing_monthly
            savings_sources.append(f"Rightsizing: ${rightsizing_monthly:.2f}")
        
        if migration.get('summary', {}).get('total_potential_savings'):
            migration_savings = migration['summary']['total_potential_savings']
            total_savings += migration_savings
            savings_sources.append(f"Migration: ${migration_savings:.2f}")
        
        if vpc_optimization.get('summary', {}).get('total_potential_savings'):
            vpc_savings = vpc_optimization['summary']['total_potential_savings']
            total_savings += vpc_savings
            savings_sources.append(f"VPC Optimization: ${vpc_savings:.2f}")
        
        print(f"💎 Total Monthly Optimization Potential: ${total_savings:.2f}")
        print(f"📈 Annual Savings Potential: ${total_savings * 12:.2f}")
        
        if savings_sources:
            print(f"💰 Savings Breakdown:")
            for source in savings_sources:
                print(f"   • {source}")
        
        print(f"\n🎉 Test 7 PASSED: Optimization analytics completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test 7 FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_optimization_analytics()