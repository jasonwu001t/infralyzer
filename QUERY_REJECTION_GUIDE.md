# Query Rejection System - CUR vs Non-CUR Validation

This guide explains the improved query rejection system that prevents non-CUR related queries from generating irrelevant SQL responses.

## 🎯 Problem Solved

**Before:** The system would generate fallback SQL queries even for completely unrelated prompts like "What's the weather?" or "How to bake a cake?"

**After:** The system now properly validates queries and rejects non-CUR related requests with helpful guidance.

## 🔍 How It Works

### 1. **Prompt-Level Validation**

The AI model receives explicit instructions to validate query relevance:

```
IMPORTANT: Only process queries related to AWS costs, usage, billing, financial analysis, or FinOps.

If the user query is NOT related to AWS costs, billing, usage analysis, or financial optimization, you MUST respond with:
{
  "error": "not_cur_related",
  "message": "I can only help with AWS cost and usage analysis queries...",
  "suggestions": ["What are my top 5 most expensive AWS services?", ...]
}
```

### 2. **Multi-Layer Detection**

The system uses multiple methods to handle different scenarios:

1. **Primary:** AI model classifies and rejects in structured JSON format
2. **Secondary:** Keyword detection for rejection responses
3. **Error Handling:** Proper exceptions for AI parsing failures - **NO FALLBACK SQL**

### 3. **Proper HTTP Response Codes**

- **✅ Valid CUR Query:** `200 OK` with structured SQL response
- **❌ Non-CUR Query:** `400 Bad Request` with rejection message
- **🔧 AI Parsing Error:** `422 Unprocessable Entity` with helpful suggestions
- **💥 System Error:** `500 Internal Server Error` for technical issues

## 📝 API Behavior

### Valid CUR Query Example

**Request:**

```json
POST /bedrock/generate-query
{
  "user_query": "What are my top 5 most expensive AWS services?",
  "model_configuration": {
    "model_id": "CLAUDE_3_5_SONNET"
  }
}
```

**Response (200 OK):**

```json
{
  "structured_query": {
    "sql_query": "SELECT product_servicecode, SUM(line_item_unblended_cost) as total_cost FROM CUR WHERE...",
    "query_type": "summary",
    "visualization_type": "bar_chart",
    "title": "Top 5 Services by Cost",
    "confidence": 0.95
  },
  "original_query": "What are my top 5 most expensive AWS services?",
  "model_used": "anthropic.claude-3-5-sonnet-20241022-v2:0",
  "generated_at": "2024-01-15T10:30:00Z"
}
```

### Non-CUR Query Example

**Request:**

```json
POST /bedrock/generate-query
{
  "user_query": "What's the weather like today?",
  "model_configuration": {
    "model_id": "CLAUDE_3_5_SONNET"
  }
}
```

**Response (400 Bad Request):**

```json
{
  "detail": {
    "error": "non_cur_query",
    "message": "I can only help with AWS cost and usage analysis queries. Your question appears to be about weather. Please ask questions about AWS costs, billing, usage, or financial optimization instead.",
    "suggestions": [
      "What are my top 5 most expensive AWS services?",
      "Show me EC2 costs by region this month",
      "Analyze my Reserved Instance utilization",
      "Compare this month's costs to last month",
      "Show me S3 storage costs by bucket"
    ],
    "original_query": "What's the weather like today?"
  }
}
```

### AI Parsing Error Example

**Request:**

```json
POST /bedrock/generate-query
{
  "user_query": "Show me EC2 costs",
  "model_configuration": {
    "model_id": "CLAUDE_3_5_HAIKU",
    "max_tokens": 100  // Very low limit causing truncation
  }
}
```

**Response (422 Unprocessable Entity):**

```json
{
  "detail": {
    "error": "ai_model_error",
    "message": "AI model failed to generate proper response. Missing fields: ['sql_query', 'title']. Please try rephrasing your query or try a different model.",
    "suggestions": [
      "Try rephrasing your query more clearly",
      "Use a different AI model (e.g., Claude 4 Opus for complex queries)",
      "Break complex queries into simpler parts",
      "Ensure your query is about AWS costs, usage, or billing"
    ]
  }
}
```

## 🎯 Query Classification

### ✅ **ACCEPTED Queries** (CUR-Related)

**Cost Analysis:**

- "What are my top 5 most expensive services?"
- "Show me AWS costs by account this month"
- "What did I spend on EC2 yesterday?"

**Usage Analysis:**

- "Show me EC2 usage by instance type"
- "Analyze my S3 storage consumption"
- "What's my data transfer usage?"

**Financial Optimization:**

- "Analyze my Reserved Instance utilization"
- "Show me potential cost savings"
- "Compare costs between regions"
- "What are my unused resources?"

**Billing & Reporting:**

- "Show me my AWS bill breakdown"
- "Generate a cost report by service"
- "Compare this month to last month"

**FinOps & Optimization:**

- "AWS cost optimization recommendations"
- "Show me cost anomalies"
- "Analyze my spending trends"

### ❌ **REJECTED Queries** (Non-CUR)

**General Knowledge:**

- "What's the weather today?"
- "Explain quantum computing"
- "What is the capital of France?"

**Technical Support:**

- "How to fix my EC2 instance?"
- "AWS security best practices"
- "How to deploy applications?"

**Non-Financial AWS:**

- "EC2 performance metrics"
- "CloudFormation templates"
- "Lambda function code examples"

**Personal/Entertainment:**

- "Help me write a letter"
- "What movies are playing?"
- "Recipe for chocolate cake"

### 🤔 **Edge Cases**

**Ambiguous but Valid:**

- "AWS cost optimization tips" → ✅ ACCEPTED (cost-related)
- "Show me my AWS bill" → ✅ ACCEPTED (billing-related)
- "Lambda vs EC2 costs" → ✅ ACCEPTED (cost comparison)

**Ambiguous and Invalid:**

- "AWS" → ❌ REJECTED (too vague)
- "Show me data" → ❌ REJECTED (ambiguous)
- "EC2 performance" → ❌ REJECTED (performance, not cost)

## 🔧 Implementation Details

### Rejection Keywords

The system detects rejection responses using keywords:

```python
rejection_keywords = [
    'not related to aws',
    'not cost related',
    'not billing related',
    'cannot help with',
    'not about aws costs',
    'not financial',
    'not finops',
    'outside my expertise',
    'not cur related',
    'not cost analysis'
]
```

### Response Structure

**Rejection Response Format:**

```python
{
    'error': 'not_cur_related',
    'message': 'Contextual rejection message',
    'suggestions': ['List of example queries'],
    'original_query': 'User input',
    'rejected': True,
    'timestamp': 'ISO timestamp'
}
```

**Valid Response Format:**

```python
{
    'sql_query': 'Generated SQL',
    'query_type': 'summary|trend|detail|comparison',
    'visualization_type': 'bar_chart|line_chart|pie_chart|table',
    'title': 'Chart title',
    'description': 'Query description',
    'confidence': 0.95,
    'filters': {...}
}
```

## 🧪 Testing

### Running Rejection Tests

Use the provided test script:

```bash
cd infralyzer/examples
python test_query_rejection.py
```

**Test Categories:**

1. **Valid CUR queries** - Should generate SQL (200 OK)
2. **Invalid non-CUR queries** - Should reject (400 Bad Request)
3. **Edge cases** - Ambiguous queries testing classification accuracy

### Example Test Output

```
🧪 Testing: Weather query
📝 Query: 'What's the weather like today?'
🎯 Expected: REJECT
   ❌ REJECTED (Correctly) - I can only help with AWS cost and usage analysis queries...
   💡 Suggestions: 5 provided
   ✅ Test PASSED

🧪 Testing: Basic cost query
📝 Query: 'What are my top 5 most expensive AWS services?'
🎯 Expected: ACCEPT
   ✅ ACCEPTED - Generated valid SQL query
   📊 Type: summary, Confidence: 0.95
   ✅ Test PASSED
```

## 🚀 Benefits

### 1. **User Experience**

- Clear rejection messages instead of irrelevant SQL
- Helpful suggestions for valid queries
- Immediate feedback on query scope
- **No misleading fallback SQL responses**

### 2. **System Reliability**

- Prevents resource waste on irrelevant processing
- Maintains focus on CUR/FinOps domain
- **Eliminates confusion from generic fallback responses**
- Proper error handling for AI parsing failures

### 3. **Developer Experience**

- Proper HTTP status codes for error handling (200/400/422/500)
- Structured error responses for frontend integration
- Comprehensive logging for debugging
- **Clear distinction between valid SQL and errors**

### 4. **Security & Performance**

- **Completely eliminates misleading SQL generation**
- Reduces computational overhead
- Maintains system boundaries
- Prevents false confidence from generic responses

## 📊 Monitoring & Metrics

The system logs all rejection events:

```python
logger.info(f"Rejected non-CUR query: {original_query}")
```

**Key Metrics to Track:**

- Rejection rate by query type
- Most common non-CUR query topics
- User behavior after rejection (do they rephrase?)
- False positive/negative rates

## 🔄 Continuous Improvement

### Feedback Loop

1. Monitor rejected queries for patterns
2. Identify legitimate CUR queries being rejected
3. Refine prompt instructions and keywords
4. Update examples and suggestions

### Model Tuning

- Adjust temperature for more deterministic classification
- Fine-tune examples in prompt for edge cases
- Update rejection keywords based on user patterns

## 💡 Best Practices

### For Users

1. **Be specific about costs/usage:** "EC2 costs" vs "EC2 performance"
2. **Include financial keywords:** "spending", "costs", "billing", "usage"
3. **Ask about optimization:** "savings", "recommendations", "waste"

### For Developers

1. **Handle 400 errors gracefully** in frontend
2. **Display suggestions** to guide users
3. **Log rejections** for analysis
4. **Monitor false positives** and adjust

### For Administrators

1. **Review rejection logs** weekly
2. **Update examples** based on common valid queries
3. **Monitor success rates** and adjust prompts
4. **Train users** on effective query patterns

---

The improved query rejection system ensures that Infralyzer maintains its focus on AWS cost and usage analysis while providing helpful guidance to users who stray outside the CUR domain.
