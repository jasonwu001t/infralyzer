#!/usr/bin/env python3
"""
Simple Price Lookup Example with Real AWS APIs

This example shows how to use the simplified pricing functions to get
real on-demand and savings plan prices from AWS APIs based on instance attributes.
"""

from de_polars.engine.data_config import DataConfig, DataExportType
from de_polars.data.aws_pricing_manager import AWSPricingManager


def main():
    """Simple pricing example."""
    
    print("💰 Simple AWS Pricing Lookup Example - REAL APIs")
    print("=" * 55)
    print("⚠️  Note: This uses real AWS APIs and requires valid credentials")
    print("=" * 55)
    
    # Simple configuration
    config = DataConfig(
        s3_bucket="dummy-bucket",  # Not needed for API calls
        s3_data_prefix="dummy-prefix",
        data_export_type=DataExportType.CUR_2_0,
        aws_region="us-east-1",
        local_data_path=None  # No caching - always fresh data
    )
    
    # Initialize unified pricing manager
    pricing_manager = AWSPricingManager(config)
    
    # Example 1: Get on-demand price for a specific instance
    print("\n🔍 Example 1: Get On-Demand Price")
    print("-" * 30)
    
    instance_type = "m5.large"
    region = "us-east-1"
    operating_system = "Linux"
    
    print(f"Getting price for {instance_type} in {region} ({operating_system})...")
    
    # Make a real API call to AWS Pricing API
    price = pricing_manager.get_ondemand_price(region, instance_type, operating_system)
    
    if price:
        print(f"💸 On-Demand Price: ${price:.4f}/hour")
        print(f"💸 Monthly Cost: ${price * 24 * 30:.2f}")
        print(f"💸 Annual Cost: ${price * 24 * 365:.2f}")
    else:
        print("❌ Price not found")
    
    # Example 2: Get savings plan rate
    print("\n🔍 Example 2: Get Savings Plan Rate")
    print("-" * 30)
    
    print(f"Getting savings plan rate for {instance_type} in {region}...")
    
    # Make a real API call to AWS SavingsPlans API
    sp_rate = pricing_manager.get_savings_plan_rate(instance_type, region)
    
    if sp_rate:
        print(f"💳 Savings Plan Rate: ${sp_rate:.4f}/hour")
        print(f"💳 Monthly Cost: ${sp_rate * 24 * 30:.2f}")
        print(f"💳 Annual Cost: ${sp_rate * 24 * 365:.2f}")
    else:
        print("❌ Savings plan rate not found")
    
    # Example 3: Compare pricing
    print("\n🔍 Example 3: Price Comparison")
    print("-" * 30)
    
    if price and sp_rate:
        comparison = pricing_manager.compare_all_pricing_options(region, instance_type, operating_system)
        
        print(f"📊 Comparison for {comparison['instance_type']} in {comparison['region']}:")
        
        # On-demand pricing
        if comparison['ondemand']['hourly_price']:
            print(f"   On-Demand: ${comparison['ondemand']['hourly_price']:.4f}/hour (${comparison['ondemand']['monthly_price']:.2f}/month)")
        
        # Spot pricing
        if comparison['spot']['hourly_price']:
            print(f"   Spot: ${comparison['spot']['hourly_price']:.4f}/hour")
            if comparison['spot']['savings_vs_ondemand_pct']:
                print(f"     Savings: {comparison['spot']['savings_vs_ondemand_pct']:.1f}% vs on-demand")
        
        # Savings plan pricing
        if comparison['savings_plan']['hourly_price']:
            print(f"   Savings Plan: ${comparison['savings_plan']['hourly_price']:.4f}/hour (${comparison['savings_plan']['monthly_price']:.2f}/month)")
            if comparison['savings_plan']['savings_vs_ondemand_pct']:
                print(f"     Savings: {comparison['savings_plan']['savings_vs_ondemand_pct']:.1f}% vs on-demand")
        else:
            print("   No savings plan available")
    
    # Example 4: Compare multiple instances
    print("\n🔍 Example 4: Compare Multiple Instances")
    print("-" * 30)
    
    # Reduce list for real API calls to avoid too many requests
    instance_types = ["t3.micro", "m5.large"]
    
    print(f"Comparing instances in {region} (using real AWS APIs):")
    print()
    
    print(f"{'Instance':<12} {'On-Demand':<12} {'Savings Plan':<12} {'Savings':<8} {'Monthly Savings'}")
    print("-" * 60)
    
    for instance in instance_types:
        print(f"🔍 Fetching prices for {instance}...")
        
        # Get real on-demand price
        od_price = pricing_manager.get_ondemand_price(region, instance, operating_system)
        
        # Get real savings plan rate
        sp_price = pricing_manager.get_savings_plan_rate(instance, region)
        
        if od_price:
            if sp_price:
                savings_pct = ((od_price - sp_price) / od_price) * 100
                monthly_savings = (od_price - sp_price) * 24 * 30
                
                print(f"{instance:<12} ${od_price:<11.4f} ${sp_price:<11.4f} {savings_pct:<7.1f}% ${monthly_savings:.2f}")
            else:
                print(f"{instance:<12} ${od_price:<11.4f} Not available  N/A       N/A")
        else:
            print(f"{instance:<12} Not available")
    
    # Example 5: Real-world usage pattern
    print("\n🔍 Example 5: Real Usage Pattern")
    print("-" * 30)
    
    print("To use with the unified AWS pricing manager:")
    print()
    print("# 1. Initialize unified manager")
    print("pricing_manager = AWSPricingManager(config)")
    print()
    print("# 2. Get on-demand pricing")
    print("price = pricing_manager.get_ondemand_price('us-east-1', 'm5.large', 'Linux')")
    print()
    print("# 3. Get spot pricing")
    print("spot = pricing_manager.get_current_spot_price('us-east-1', 'm5.large')")
    print()
    print("# 4. Get savings plan rates")
    print("sp_rate = pricing_manager.get_savings_plan_rate('m5.large', 'us-east-1')")
    print()
    print("# 5. Compare all pricing options")
    print("comparison = pricing_manager.compare_all_pricing_options('us-east-1', 'm5.large', 'Linux')")
    print()
    print("# 6. Find cheapest option")
    print("cheapest = pricing_manager.get_cheapest_option('us-east-1', 'm5.large', 'Linux')")
    
    print("\n✅ Unified pricing manager examples completed!")
    print("\n💡 Key benefits:")
    print("   - All pricing models (On-Demand, Spot, Reserved, Savings Plans) in one manager")
    print("   - Simple function calls based on instance attributes")
    print("   - Real-time data directly from AWS APIs")
    print("   - No caching complexity - always fresh data")
    print("   - Easy comparison between on-demand and savings plan pricing")
    print("   - No complex SQL queries needed for basic price lookups")


if __name__ == "__main__":
    main()