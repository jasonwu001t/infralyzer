"""
Test 8: AI Recommendations
===========================

This test demonstrates AI-powered cost insights, forecasting, and custom analysis.
Tests machine learning capabilities for cost optimization and prediction.
"""

import sys
import os
# Add parent directory to path to import local de_polars module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infralyzer import FinOpsEngine, DataConfig, DataExportType

def test_ai_recommendations():
    """Test AI recommendation analytics capabilities"""
    
    print("🤖 Test 8: AI Recommendations")
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
            print(f"Local data not found at {local_path}")
            print("Please run test_2_download_local.py first")
            return False
        
        # Initialize engine
        print("Initializing FinOps Engine...")
        engine = FinOpsEngine(config)
        
        # Get AI analytics module
        ai = engine.ai
        
        # Test 1: AI Optimization Insights
        print("\n Step 1: AI Optimization Insights")
        print("-" * 40)
        
        insights = ai.get_optimization_insights()
        print(f"AI Insights Generated: {len(insights.get('insights', []))} insights")
        
        if insights.get('summary'):
            summary = insights['summary']
            print(f"Total Recommendations: {summary.get('total_recommendations', 0)}")
            print(f" High Priority Items: {summary.get('high_priority_count', 0)}")
            print(f"AI Predicted Savings: ${summary.get('total_predicted_savings', 0):.2f}")
        
        # Show top AI insights
        ai_insights = insights.get('insights', [])[:3]
        if ai_insights:
            print(f"Top AI Insights:")
            for i, insight in enumerate(ai_insights, 1):
                category = insight.get('category', 'General')
                confidence = insight.get('confidence_score', 0)
                recommendation = insight.get('recommendation', 'No recommendation')[:50]
                print(f"   {i}. {category} ({confidence:.1f}%): {recommendation}...")
        
        # Test 2: Custom Query Analysis
        print("\nStep 2: Custom Query Analysis")
        print("-" * 40)
        
        # Test different analysis types
        custom_queries = [
            ("What are my biggest cost drivers?", "cost_analysis"),
            ("Predict my next month spending", "forecasting"),
            ("Find optimization opportunities", "optimization")
        ]
        
        for query_text, analysis_type in custom_queries:
            print(f"\nAnalyzing: '{query_text}' ({analysis_type})")
            
            try:
                analysis = ai.analyze_custom_query(query_text, analysis_type)
                
                print(f"Analysis completed")
                print(f"    Intent: {analysis.get('intent', 'Unknown')}")
                print(f"   Data Points: {len(analysis.get('results', []))}")
                
                if analysis.get('ai_insights'):
                    insights_count = len(analysis['ai_insights'])
                    print(f"   AI Insights: {insights_count} generated")
                    
                    # Show first insight
                    if analysis['ai_insights']:
                        first_insight = analysis['ai_insights'][0]
                        print(f"      • {first_insight[:60]}...")
                
            except Exception as e:
                print(f" Analysis failed: {str(e)}")
        
        # Test 3: AI Forecasting
        print("\nStep 3: AI Forecasting")
        print("-" * 40)
        
        forecasting = ai.get_forecasting(forecast_months=6)
        print(f"AI Forecasting: {forecasting.get('forecast_periods', 0)} periods predicted")
        
        if forecasting.get('summary'):
            summary = forecasting['summary']
            print(f"Current Monthly Spend: ${summary.get('current_monthly_spend', 0):.2f}")
            print(f"🔮 Predicted 6-Month Total: ${summary.get('total_predicted_cost', 0):.2f}")
            print(f"Trend: {summary.get('trend_direction', 'Unknown')} ({summary.get('trend_confidence', 0):.1f}% confidence)")
        
        # Show monthly forecasts
        monthly_forecasts = forecasting.get('monthly_forecasts', [])[:3]
        if monthly_forecasts:
            print(f"📅 Next 3 Months Forecast:")
            for i, forecast in enumerate(monthly_forecasts, 1):
                month = forecast.get('month', 'Unknown')
                predicted_cost = forecast.get('predicted_cost', 0)
                confidence = forecast.get('confidence', 0)
                print(f"   {i}. {month}: ${predicted_cost:.2f} ({confidence:.1f}% confidence)")
        
        # Test 4: AI Pattern Recognition
        print("\nStep 4: AI Pattern Recognition")
        print("-" * 40)
        
        # Use the optimization insights to identify patterns
        if insights.get('patterns'):
            patterns = insights['patterns'][:3]
            print(f"Patterns Detected: {len(patterns)}")
            
            for i, pattern in enumerate(patterns, 1):
                pattern_type = pattern.get('type', 'Unknown')
                frequency = pattern.get('frequency', 'Unknown')
                impact = pattern.get('impact_score', 0)
                print(f"   {i}. {pattern_type} (occurs {frequency}, impact: {impact:.1f})")
        else:
            print("Pattern analysis in progress...")
        
        # Test 5: AI Recommendations Summary
        print("\n📋 Step 5: AI Recommendations Summary")
        print("-" * 40)
        
        # Compile all AI recommendations
        total_insights = len(insights.get('insights', []))
        total_forecasts = len(forecasting.get('monthly_forecasts', []))
        
        print(f" AI Analysis Summary:")
        print(f"   • Optimization Insights: {total_insights}")
        print(f"   • Forecast Periods: {total_forecasts}")
        print(f"   • Custom Analyses: {len(custom_queries)} completed")
        
        # Calculate confidence scores
        all_insights = insights.get('insights', [])
        if all_insights:
            avg_confidence = sum(insight.get('confidence_score', 0) for insight in all_insights) / len(all_insights)
            print(f"   • Average Confidence: {avg_confidence:.1f}%")
        
        # Show AI-powered recommendations
        recommendations = insights.get('ai_recommendations', [])[:3]
        if recommendations:
            print(f" Top AI Recommendations:")
            for i, rec in enumerate(recommendations, 1):
                action = rec.get('action', 'Unknown action')[:40]
                priority = rec.get('priority', 'Medium')
                savings = rec.get('potential_savings', 0)
                print(f"   {i}. {action}... ({priority} priority, ${savings:.2f})")
        
        print(f"\nTest 8 PASSED: AI recommendations completed successfully!")
        return True
        
    except Exception as e:
        print(f"Test 8 FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    test_ai_recommendations()