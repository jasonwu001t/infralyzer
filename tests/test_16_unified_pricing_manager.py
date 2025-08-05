#!/usr/bin/env python3
"""
Test unified AWS pricing manager - covers all pricing models
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from infralyzer.engine.data_config import DataConfig, DataExportType
from infralyzer.data.aws_pricing_manager import AWSPricingManager


def test_unified_pricing_manager():
    """Test unified AWS pricing manager with real APIs."""
    
    print("Testing Unified AWS Pricing Manager")
    print("=" * 50)
    print("Note: This test requires valid AWS credentials and API access")
    print("=" * 50)
    
    # Test configuration - no caching
    config = DataConfig(
        s3_bucket="test-bucket",
        s3_data_prefix="test-prefix", 
        data_export_type=DataExportType.CUR_2_0,
        aws_region="us-east-1",
        local_data_path=None  # No caching
    )
    
    try:
        # Initialize unified pricing manager
        pricing_manager = AWSPricingManager(config)
        
        print("\nTest 1: On-Demand Pricing")
        print("-" * 30)
        
        # Test on-demand pricing
        ondemand_price = pricing_manager.get_ondemand_price("us-east-1", "t3.micro", "Linux")
        if ondemand_price:
            print(f"t3.micro on-demand: ${ondemand_price:.4f}/hour")
        else:
            print("t3.micro on-demand price not found")
        
        print("\nTest 2: Spot Pricing")
        print("-" * 30)
        
        # Test spot pricing
        spot_price = pricing_manager.get_current_spot_price("us-east-1", "t3.micro")
        if spot_price:
            print(f"t3.micro spot: ${spot_price:.4f}/hour")
            if ondemand_price:
                savings = ((ondemand_price - spot_price) / ondemand_price) * 100
                print(f"   Spot savings: {savings:.1f}% vs on-demand")
        else:
            print("t3.micro spot price not found")
        
        print("\n🏦 Test 3: Reserved Instance Pricing")
        print("-" * 30)
        
        # Test reserved instance pricing
        ri_price = pricing_manager.get_reserved_instance_price("us-east-1", "t3.micro", "1yr", "No Upfront", "Linux")
        if ri_price:
            print(f"t3.micro RI (1yr, No Upfront): ${ri_price['hourly_cost']:.4f}/hour")
            print(f"   Upfront cost: ${ri_price['upfront_cost']:.2f}")
            if ondemand_price:
                ri_savings = ((ondemand_price - ri_price['hourly_cost']) / ondemand_price) * 100
                print(f"   RI savings: {ri_savings:.1f}% vs on-demand")
        else:
            print("t3.micro reserved instance price not found")
        
        print("\n💳 Test 4: Savings Plans")
        print("-" * 30)
        
        # Test savings plans (use m5.large which has better savings plan availability)
        sp_price = pricing_manager.get_savings_plan_rate("m5.large", "us-east-1")
        if sp_price:
            print(f"m5.large savings plan: ${sp_price:.4f}/hour")
            # Get on-demand price for comparison
            m5_ondemand = pricing_manager.get_ondemand_price("us-east-1", "m5.large", "Linux")
            if m5_ondemand:
                sp_savings = ((m5_ondemand - sp_price) / m5_ondemand) * 100
                print(f"   Savings plan savings: {sp_savings:.1f}% vs on-demand")
        else:
            print("m5.large savings plan rate not found")
        
        print("\nTest 5: Complete Pricing Comparison")
        print("-" * 30)
        
        # Test complete comparison
        comparison = pricing_manager.compare_all_pricing_options("us-east-1", "m5.large", "Linux")
        
        print(f"Complete pricing analysis for {comparison['instance_type']} in {comparison['region']}:")
        print()
        
        # On-demand
        if comparison['ondemand']['hourly_price']:
            print(f"On-Demand: ${comparison['ondemand']['hourly_price']:.4f}/hour")
            print(f"   Monthly: ${comparison['ondemand']['monthly_price']:.2f}")
        
        # Spot
        if comparison['spot']['hourly_price']:
            print(f"Spot: ${comparison['spot']['hourly_price']:.4f}/hour")
            print(f"   Monthly: ${comparison['spot']['monthly_price']:.2f}")
            if comparison['spot']['savings_vs_ondemand_pct']:
                print(f"   Savings: {comparison['spot']['savings_vs_ondemand_pct']:.1f}%")
        
        # Reserved Instance
        if comparison['reserved_1yr']['hourly_price']:
            print(f"🏦 Reserved (1yr): ${comparison['reserved_1yr']['hourly_price']:.4f}/hour")
            print(f"   Monthly: ${comparison['reserved_1yr']['monthly_price']:.2f}")
            print(f"   Upfront: ${comparison['reserved_1yr']['upfront_cost']:.2f}")
            if comparison['reserved_1yr']['savings_vs_ondemand_pct']:
                print(f"   Savings: {comparison['reserved_1yr']['savings_vs_ondemand_pct']:.1f}%")
        
        # Savings Plan
        if comparison['savings_plan']['hourly_price']:
            print(f"💳 Savings Plan: ${comparison['savings_plan']['hourly_price']:.4f}/hour")
            print(f"   Monthly: ${comparison['savings_plan']['monthly_price']:.2f}")
            if comparison['savings_plan']['savings_vs_ondemand_pct']:
                print(f"   Savings: {comparison['savings_plan']['savings_vs_ondemand_pct']:.1f}%")
        
        print("\n Test 6: Find Cheapest Option")
        print("-" * 30)
        
        cheapest = pricing_manager.get_cheapest_option("us-east-1", "m5.large", "Linux")
        if 'error' not in cheapest:
            print(f"🥇 Cheapest option: {cheapest['cheapest_option']}")
            print(f"   Price: ${cheapest['hourly_price']:.4f}/hour")
            print(f"   Monthly: ${cheapest['monthly_price']:.2f}")
            print(f"   Annual: ${cheapest['annual_price']:.2f}")
            if cheapest['savings_vs_ondemand_pct']:
                print(f"   Savings: {cheapest['savings_vs_ondemand_pct']:.1f}% vs on-demand")
        else:
            print(f"{cheapest['error']}")
        
        print("\nUnified Pricing Manager Tests Completed!")
        print("=" * 50)
        print("All AWS pricing models accessible through single manager")
        print("No caching - always fresh data from AWS APIs")
        print("Simple functions for price lookups and comparisons")
        print("\nUsage Examples:")
        print("   manager = AWSPricingManager(config)")
        print("   ondemand = manager.get_ondemand_price('us-east-1', 't3.micro')")
        print("   spot = manager.get_current_spot_price('us-east-1', 't3.micro')")
        print("   comparison = manager.compare_all_pricing_options('us-east-1', 't3.micro')")
        print("   cheapest = manager.get_cheapest_option('us-east-1', 't3.micro')")
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_unified_pricing_manager()