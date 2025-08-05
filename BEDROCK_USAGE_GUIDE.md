# AWS Bedrock Integration Guide

## Overview

This guide covers the comprehensive AWS Bedrock integration in Infralyzer, which provides advanced AI chatbot functionality with knowledge base support for Cost and Usage Report (CUR) data analysis.

## Features

### 🤖 AI Chatbot with Multiple Model Support

- **Claude 3.5 Sonnet**: Best overall performance for complex reasoning
- **Claude 3 Haiku**: Fast and efficient for simple queries
- **Titan Text**: Cost-effective Amazon models
- **Cohere Command**: Conversational AI capabilities
- **Llama 2**: Open-source options

### 📚 Knowledge Base Management

- Create knowledge bases from CUR data
- Automated data ingestion and embedding
- Vector search for context-aware responses
- Source attribution and citation tracking

### 🔍 Structured Query Generation

- Natural language to SQL conversion
- CUR-specific query optimization
- Frontend-ready structured output
- Visualization type recommendations

## Quick Start

### 1. Prerequisites

Ensure your AWS credentials have the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:*",
        "bedrock-agent:*",
        "bedrock-agent-runtime:*",
        "s3:GetObject",
        "s3:ListBucket",
        "opensearch:*"
      ],
      "Resource": "*"
    }
  ]
}
```

### 2. Basic Usage

```python
from infralyzer import FinOpsEngine, DataConfig
from infralyzer.utils.bedrock_handler import BedrockHandler, BedrockModel

# Initialize
config = DataConfig(
    s3_bucket='your-bucket',
    s3_data_prefix='cur2/data',
    data_export_type='CUR2.0'
)

engine = FinOpsEngine(config)
bedrock = BedrockHandler(config, default_model=BedrockModel.CLAUDE_3_5_SONNET)

# List available models
models = bedrock.list_available_models()
print(f"Available models: {len(models)}")
```

## API Endpoints

### Model Management

#### List Available Models

```http
GET /api/v1/finops/bedrock/models
```

**Response:**

```json
{
  "success": true,
  "available_models": [
    {
      "model_id": "anthropic.claude-3-5-sonnet-20241022-v2:0",
      "model_name": "Claude-3-5-Sonnet",
      "provider_name": "Anthropic",
      "input_modalities": ["TEXT"],
      "output_modalities": ["TEXT"]
    }
  ],
  "predefined_configurations": [
    {
      "model_id": "anthropic.claude-3-5-sonnet-20241022-v2:0",
      "model_name": "CLAUDE_3_5_SONNET",
      "recommended_use": "Best overall performance for complex reasoning and analysis"
    }
  ]
}
```

### Knowledge Base Management

#### Create Knowledge Base

```http
POST /api/v1/finops/bedrock/knowledge-base
Content-Type: application/json

{
    "name": "CUR-Analysis-KB",
    "description": "Knowledge base for CUR cost analysis",
    "role_arn": "arn:aws:iam::123456789012:role/BedrockRole",
    "s3_bucket": "your-cur-bucket",
    "s3_prefix": "cur2/data",
    "chunk_size": 300,
    "overlap_percentage": 20
}
```

#### Create CUR-Specific Knowledge Base

```http
POST /api/v1/finops/bedrock/cur/knowledge-base?s3_bucket=your-bucket&s3_prefix=cur2/data&role_arn=arn:aws:iam::123456789012:role/BedrockRole
```

#### List Knowledge Bases

```http
GET /api/v1/finops/bedrock/knowledge-bases
```

### Chatbot Functionality

#### Chat with Knowledge Base

```http
POST /api/v1/finops/bedrock/chat
Content-Type: application/json

{
    "message": "What are my top 3 highest cost services this month?",
    "knowledge_base_id": "KB123456",
    "conversation_id": "conv-001",
    "model_config": {
        "model_id": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "max_tokens": 4096,
        "temperature": 0.1
    },
    "include_sources": true
}
```

**Response:**

```json
{
  "response": "Based on your CUR data, your top 3 highest cost services this month are...",
  "conversation_id": "conv-001",
  "knowledge_sources": [
    {
      "content": "Relevant cost data context...",
      "location": { "s3Location": { "uri": "s3://bucket/path" } },
      "metadata": { "service": "AmazonEC2", "cost": 1234.56 }
    }
  ],
  "model_used": "anthropic.claude-3-5-sonnet-20241022-v2:0",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Structured Query Generation

#### Generate CUR Query

```http
POST /api/v1/finops/bedrock/generate-query
Content-Type: application/json

{
    "user_query": "Show me EC2 costs by region for the last 6 months",
    "model_config": {
        "model_id": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "temperature": 0.1
    },
    "include_examples": true
}
```

**Response:**

```json
{
  "structured_query": {
    "sql_query": "SELECT DATE_TRUNC('month', line_item_usage_start_date) as month, product_region, SUM(line_item_unblended_cost) as monthly_cost FROM CUR WHERE product_servicecode = 'AmazonEC2' AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '6 months' GROUP BY 1, 2 ORDER BY 1, 3 DESC",
    "query_type": "trend",
    "visualization_type": "line_chart",
    "title": "EC2 Costs by Region (6 Months)",
    "description": "Monthly EC2 spending trends across regions",
    "filters": {
      "date_range": "last_6_months",
      "services": ["AmazonEC2"]
    },
    "confidence": 0.92
  },
  "original_query": "Show me EC2 costs by region for the last 6 months",
  "model_used": "anthropic.claude-3-5-sonnet-20241022-v2:0",
  "confidence": 0.92,
  "generated_at": "2024-01-15T10:30:00Z"
}
```

### Conversation Management

#### Get Conversation History

```http
GET /api/v1/finops/bedrock/conversation/{conversation_id}/history
```

#### Clear Conversation

```http
DELETE /api/v1/finops/bedrock/conversation/{conversation_id}
```

## Frontend Integration

### Using Structured Queries

The structured query endpoint returns frontend-ready data:

```javascript
// Example frontend integration
async function generateCostQuery(userInput) {
  const response = await fetch("/api/v1/finops/bedrock/generate-query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      user_query: userInput,
      include_examples: true,
    }),
  });

  const result = await response.json();
  const { structured_query } = result;

  // Execute the generated SQL
  const queryResponse = await fetch("/api/v1/finops/query/sql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sql: structured_query.sql_query,
      format: "json",
    }),
  });

  const data = await queryResponse.json();

  // Create visualization based on type
  createVisualization(
    data,
    structured_query.visualization_type,
    structured_query.title
  );
}
```

### Chat Interface

```javascript
// Example chat interface
async function sendChatMessage(message, knowledgeBaseId, conversationId) {
  const response = await fetch("/api/v1/finops/bedrock/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      message: message,
      knowledge_base_id: knowledgeBaseId,
      conversation_id: conversationId,
      include_sources: true,
    }),
  });

  const result = await response.json();

  // Display response and sources
  displayChatResponse(result.response);
  displayKnowledgeSources(result.knowledge_sources);

  return result.conversation_id;
}
```

## Model Selection Guide

### Claude 3.5 Sonnet

- **Best for**: Complex cost analysis, detailed explanations
- **Use cases**: Multi-step reasoning, comprehensive reports
- **Performance**: Highest accuracy, slower response

### Claude 3 Haiku

- **Best for**: Quick queries, simple summaries
- **Use cases**: Dashboard updates, basic Q&A
- **Performance**: Fastest response, good accuracy

### Titan Text Express

- **Best for**: Cost-effective operations, high volume
- **Use cases**: Automated query generation, batch processing
- **Performance**: Good speed, lower cost

## Best Practices

### 1. Knowledge Base Optimization

- Use descriptive names and chunk sizes appropriate for your data
- Include metadata in your S3 objects for better context
- Regularly update knowledge bases with new CUR data

### 2. Query Generation

- Be specific in natural language queries
- Include time ranges and service names when relevant
- Use conversation context for follow-up questions

### 3. Model Configuration

- Lower temperature (0.1-0.3) for factual queries
- Higher temperature (0.5-0.8) for creative analysis
- Adjust max_tokens based on expected response length

### 4. Conversation Management

- Use meaningful conversation IDs for tracking
- Clear conversations when context becomes stale
- Monitor conversation history for privacy compliance

## Troubleshooting

### Common Issues

1. **Permission Errors**

   - Verify IAM roles have Bedrock permissions
   - Check S3 bucket access for knowledge base creation

2. **Knowledge Base Creation Fails**

   - Ensure S3 bucket exists and has data
   - Verify OpenSearch Serverless collection permissions

3. **Query Generation Issues**

   - Use more specific natural language
   - Check if model supports your use case
   - Review example queries for formatting

4. **Chat Responses Empty**
   - Verify knowledge base is active and ingested
   - Check conversation context and history
   - Ensure proper model configuration

## Advanced Configuration

### Custom Model Configuration

```python
from infralyzer.utils.bedrock_handler import ModelConfiguration, BedrockModel

# High accuracy configuration
precise_config = ModelConfiguration(
    model_id=BedrockModel.CLAUDE_3_5_SONNET,
    max_tokens=8192,
    temperature=0.1,
    top_p=0.9,
    top_k=250
)

# Creative analysis configuration
creative_config = ModelConfiguration(
    model_id=BedrockModel.CLAUDE_3_SONNET,
    max_tokens=4096,
    temperature=0.7,
    top_p=0.95,
    stop_sequences=["Human:", "Assistant:"]
)
```

### Knowledge Base with Custom Embeddings

```python
from infralyzer.utils.bedrock_handler import KnowledgeBaseConfig

custom_kb = KnowledgeBaseConfig(
    name="Advanced-CUR-Analysis",
    description="Advanced CUR analysis with custom embeddings",
    role_arn="arn:aws:iam::123456789012:role/BedrockRole",
    s3_bucket="your-bucket",
    s3_prefix="cur2/processed",
    embedding_model_arn="arn:aws:bedrock:us-east-1::foundation-model/cohere.embed-english-v3",
    chunk_size=500,
    overlap_percentage=15
)
```

This completes the comprehensive AWS Bedrock integration for Infralyzer, providing advanced AI chatbot capabilities with CUR-specific knowledge base support and structured query generation for frontend applications.
