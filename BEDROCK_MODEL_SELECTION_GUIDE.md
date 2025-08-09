# Bedrock Model Selection Guide

This guide explains how to select and use different AI models when interacting with the Bedrock endpoints in Infralyzer.

## Available Models (Updated 2024)

### 🚀 Claude 4.0 Series (Latest & Most Powerful)

- **`CLAUDE_4_OPUS`**: `anthropic.claude-opus-4-20250514-v1:0`
  - 🚀 Most powerful model for advanced coding and complex reasoning tasks
  - Best for: Complex financial analysis, advanced query generation, detailed explanations
- **`CLAUDE_4_SONNET`**: `anthropic.claude-sonnet-4-20250514-v1:0`
  - ⚡ Balanced performance and cost for high-volume production workloads
  - Best for: Production environments, regular cost analysis, automated reporting

### 🧠 Claude 3.7 Series (Hybrid Reasoning)

- **`CLAUDE_3_7_SONNET`**: `anthropic.claude-3-7-sonnet-20250219-v1:0`
  - 🧠 Hybrid reasoning with extended thinking modes for complex problem-solving
  - Best for: Multi-step analysis, complex optimization recommendations

### 💎 Claude 3.5 Series (Optimized Performance)

- **`CLAUDE_3_5_SONNET`**: `anthropic.claude-3-5-sonnet-20241022-v2:0`
  - 💎 Excellent performance for complex reasoning and analysis
  - Best for: Advanced cost analysis, query optimization
- **`CLAUDE_3_5_HAIKU`**: `anthropic.claude-3-5-haiku-20241022-v1:0`
  - ⚡ Fast and efficient for quick responses and simple tasks
  - Best for: Quick queries, simple summaries, fast responses

### 🎯 Claude 3.0 Series (Foundational)

- **`CLAUDE_3_OPUS`**: `anthropic.claude-3-opus-20240229-v1:0`
- **`CLAUDE_3_SONNET`**: `anthropic.claude-3-sonnet-20240229-v1:0`
- **`CLAUDE_3_HAIKU`**: `anthropic.claude-3-haiku-20240307-v1:0`

### Other Models

- **Amazon Titan**, **Cohere**, **AI21**, **Meta Llama** models also available

## How to Select Models

### 1. List Available Models

**Endpoint:** `GET /bedrock/models`

```bash
curl -X GET "http://localhost:8000/bedrock/models"
```

**Response:**

```json
{
  "success": true,
  "available_models": [...],
  "predefined_configurations": [
    {
      "model_id": "anthropic.claude-opus-4-20250514-v1:0",
      "model_name": "CLAUDE_4_OPUS",
      "recommended_use": "🚀 Most powerful model for advanced coding and complex reasoning tasks",
      "is_predefined": true
    }
  ],
  "total_count": 25
}
```

### 2. Using Models in Chat Endpoint

**Endpoint:** `POST /bedrock/chat`

```json
{
  "message": "What are my top 5 most expensive services this month?",
  "knowledge_base_id": "KB123456",
  "model_configuration": {
    "model_id": "CLAUDE_4_OPUS",
    "max_tokens": 4096,
    "temperature": 0.1,
    "top_p": 0.9,
    "top_k": 250
  },
  "include_sources": true
}
```

### 3. Using Models in Query Generation

**Endpoint:** `POST /bedrock/generate-query`

```json
{
  "user_query": "Show me EC2 costs by instance type for the last 30 days",
  "model_configuration": {
    "model_id": "CLAUDE_3_5_SONNET",
    "max_tokens": 2048,
    "temperature": 0.05
  },
  "include_examples": true,
  "target_table": "CUR"
}
```

## Model Configuration Parameters

### Core Parameters

- **`model_id`**: The specific model to use (required)
- **`max_tokens`**: Maximum tokens to generate (1-8192, default: 4096)
- **`temperature`**: Randomness in generation (0.0-1.0, default: 0.1)
- **`top_p`**: Nucleus sampling parameter (0.0-1.0, default: 0.9)
- **`top_k`**: Top-k sampling parameter (1-500, default: 250)
- **`stop_sequences`**: Optional array of stop sequences

### Example Configurations by Use Case

#### 🎯 Precise Financial Analysis (Low Creativity)

```json
{
  "model_id": "CLAUDE_4_OPUS",
  "max_tokens": 4096,
  "temperature": 0.0,
  "top_p": 0.8,
  "top_k": 100
}
```

#### ⚖️ Balanced General Use

```json
{
  "model_id": "CLAUDE_3_5_SONNET",
  "max_tokens": 2048,
  "temperature": 0.1,
  "top_p": 0.9,
  "top_k": 250
}
```

#### 🚀 Creative Optimization Suggestions

```json
{
  "model_id": "CLAUDE_3_7_SONNET",
  "max_tokens": 3072,
  "temperature": 0.3,
  "top_p": 0.95,
  "top_k": 300
}
```

#### ⚡ Fast Simple Queries

```json
{
  "model_id": "CLAUDE_3_5_HAIKU",
  "max_tokens": 1024,
  "temperature": 0.05,
  "top_p": 0.85,
  "top_k": 150
}
```

## Model Selection Strategy

### By Use Case

| Use Case                           | Recommended Model       | Reason                        |
| ---------------------------------- | ----------------------- | ----------------------------- |
| Complex cost optimization analysis | `CLAUDE_4_OPUS`         | Most powerful reasoning       |
| Production cost reporting          | `CLAUDE_4_SONNET`       | Balanced performance/cost     |
| Multi-step financial analysis      | `CLAUDE_3_7_SONNET`     | Hybrid reasoning capabilities |
| Regular cost queries               | `CLAUDE_3_5_SONNET`     | Great performance             |
| Quick cost summaries               | `CLAUDE_3_5_HAIKU`      | Fast responses                |
| Budget-conscious usage             | `TITAN_TEXT_G1_EXPRESS` | Cost-effective                |

### By Performance Requirements

| Requirement             | Model Choice                                          |
| ----------------------- | ----------------------------------------------------- |
| **Highest Accuracy**    | Claude 4 Opus > Claude 3.7 Sonnet > Claude 3.5 Sonnet |
| **Fastest Response**    | Claude 3.5 Haiku > Titan Express > Claude 3 Haiku     |
| **Best Value**          | Claude 4 Sonnet > Claude 3.5 Sonnet > Claude 3 Sonnet |
| **Most Cost-Effective** | Titan Express > Cohere Light > Claude 3 Haiku         |

## Python SDK Example

```python
import requests
import json

# Configuration for different scenarios
configs = {
    "high_accuracy": {
        "model_id": "CLAUDE_4_OPUS",
        "max_tokens": 4096,
        "temperature": 0.0,
        "top_p": 0.8
    },
    "balanced": {
        "model_id": "CLAUDE_3_5_SONNET",
        "max_tokens": 2048,
        "temperature": 0.1,
        "top_p": 0.9
    },
    "fast": {
        "model_id": "CLAUDE_3_5_HAIKU",
        "max_tokens": 1024,
        "temperature": 0.05,
        "top_p": 0.85
    }
}

def generate_cost_query(query_text, scenario="balanced"):
    """Generate a structured query using specified model configuration."""

    payload = {
        "user_query": query_text,
        "model_configuration": configs[scenario],
        "include_examples": True,
        "target_table": "CUR"
    }

    response = requests.post(
        "http://localhost:8000/bedrock/generate-query",
        json=payload
    )

    return response.json()

# Example usage
result = generate_cost_query(
    "Show me EC2 Reserved Instance utilization",
    scenario="high_accuracy"
)

print(f"Generated SQL: {result['structured_query']['sql_query']}")
print(f"Model used: {result['model_used']}")
print(f"Confidence: {result['confidence']}")
```

## Best Practices

### 1. **Model Selection**

- Use Claude 4 Opus for complex financial analysis requiring highest accuracy
- Use Claude 4 Sonnet for production workloads balancing performance and cost
- Use Claude 3.5 Haiku for quick, simple queries and real-time responses
- Use Claude 3.7 Sonnet for complex multi-step reasoning tasks

### 2. **Parameter Tuning**

- **Lower temperature (0.0-0.1)** for precise financial calculations and SQL generation
- **Higher temperature (0.2-0.4)** for creative optimization suggestions
- **Lower top_p (0.7-0.8)** for focused, deterministic responses
- **Higher top_p (0.9-0.95)** for more diverse and creative responses

### 3. **Token Management**

- **1024 tokens**: Simple queries and summaries
- **2048 tokens**: Standard analysis and reporting
- **4096 tokens**: Complex analysis with detailed explanations
- **8192 tokens**: Comprehensive reports and multi-part analysis

### 4. **Cost Optimization**

- Use Haiku models for development and testing
- Use Sonnet models for production with moderate complexity
- Reserve Opus models for critical analysis requiring highest accuracy
- Monitor token usage and adjust max_tokens based on actual needs

## Error Handling

If a model is not available or access is restricted:

```json
{
  "detail": "Error generating query: Model anthropic.claude-opus-4-20250514-v1:0 is not available. Please check model access in AWS Bedrock console."
}
```

**Solution:** Ensure your AWS account has requested access to the specific Anthropic models in the Amazon Bedrock console.

## Monitoring and Logging

All model usage is logged with:

- Model ID used
- Token consumption
- Response time
- Error rates
- Cost per request

Check your AWS CloudWatch logs for detailed usage metrics and optimize model selection based on actual performance data.
