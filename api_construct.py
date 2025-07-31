"""
Decision needed: 
1. Avoid S3 data transfer cost, save post processed data locally to construct API
2. Do we query a single datafile or breakdown to different partitions. 
3. Caching?
4. Export function for each dataset
5. 

View 1 : Actual Spend
    1. Invoice : last month paid, month over month difference, trendline of invoice spend
    2. Top 10 Spend regions
    3. Top 10 Spend services
    4. Invoice driver by region dn service
    5. Click expans sub region/resource breakdown

View 2 : Optimization
    1. Idle waste
    2. Rightsizing
    3. Cross service migraiton for cost saving
    4. VPC charge relate to cross VPC traffic
    5. Cloudwatch logs integration

View 3 : Cost Allocation Setup
    1. Current Account level 
    2. Resources are not tagged (might due to non taggable resource, eg. API call)
    3. Function to intergrate 3rd party tagging

View 4 : Private Discount Tracking (Next negotiation opportunity)
    1. 

View 5 : MCP Server

View 6 : AI Recommandation base on Cost Saving Experiences scan through existing data

"""