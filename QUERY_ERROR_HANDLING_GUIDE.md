# Query Error Handling Guide

This guide explains the improved error handling system for the `/api/v1/finops/query` endpoint that provides user-friendly guidance instead of raw database errors.

## 🔄 What Changed

### **Before (Raw Database Errors)**

```
HTTP 500 Internal Server Error

DuckDB query error: Binder Error: Referenced column "product_region" not found in FROM clause!
Candidate bindings: "product_region_code", "product_operation", "product", "product_to_region_code"...
```

### **After (User-Friendly Guidance)**

```
HTTP 400 Bad Request

{
  "error": "Column 'product_region' does not exist in the CUR table. Please check your query and verify the column name.",
  "error_type": "COLUMN_NOT_FOUND",
  "suggestions": [
    "📝 Double-check the column name spelling and case sensitivity",
    "🔍 Run 'SELECT * FROM CUR LIMIT 1' to see all available columns",
    "💡 Did you mean one of these columns? product_region_code, product_operation, product",
    "🏗️ Check if you're using the correct CUR 2.0 column names (not legacy CUR 1.0)",
    "📋 Use the /bedrock/finops-expert endpoint to get help with CUR column selection"
  ],
  "troubleshooting_steps": [
    "1. Run 'SELECT * FROM CUR LIMIT 1' to see available columns",
    "2. Check CUR 2.0 schema documentation for correct column names",
    "3. Verify you're not using legacy CUR 1.0 column names",
    "4. Use case-sensitive column names exactly as they appear"
  ]
}
```

## 🎯 Error Types & Responses

### **Column Not Found (`COLUMN_NOT_FOUND`)**

**Triggered by:** Invalid or misspelled column names

**Example Query:**

```sql
SELECT product_region, SUM(line_item_unblended_cost)
FROM CUR
GROUP BY product_region
```

**Response:**

```json
{
  "error": "Column 'product_region' does not exist in the CUR table. Please check your query and verify the column name.",
  "error_type": "COLUMN_NOT_FOUND",
  "suggestions": [
    "📝 Double-check the column name spelling and case sensitivity",
    "🔍 Run 'SELECT * FROM CUR LIMIT 1' to see all available columns",
    "📊 Review the CUR table schema documentation for correct column names",
    "💡 Did you mean one of these columns? product_region_code, product_operation, product",
    "🏗️ Check if you're using the correct CUR 2.0 column names (not legacy CUR 1.0)",
    "📋 Use the /bedrock/finops-expert endpoint to get help with CUR column selection"
  ],
  "troubleshooting_steps": [
    "1. Run 'SELECT * FROM CUR LIMIT 1' to see available columns",
    "2. Check CUR 2.0 schema documentation for correct column names",
    "3. Verify you're not using legacy CUR 1.0 column names",
    "4. Use case-sensitive column names exactly as they appear"
  ]
}
```

**Smart Features:**

- 🎯 **Extracts the specific column name** from the error
- 💡 **Suggests similar column names** found in the error message
- 📚 **Links to resources** for finding correct column names

### **Table Not Found (`TABLE_NOT_FOUND`)**

**Triggered by:** Wrong table names or missing data

**Example Query:**

```sql
SELECT * FROM BILLING_DATA LIMIT 10
```

**Response:**

```json
{
  "error": "The table you're trying to query doesn't exist or isn't available.",
  "error_type": "TABLE_NOT_FOUND",
  "suggestions": [
    "✅ Use 'CUR' as the main table name for Cost and Usage Report data",
    "📋 Available tables: CUR (main cost data), aws_pricing (pricing data), aws_savings_plans (savings plans)",
    "🔄 Check if your data has been properly loaded and configured",
    "📁 Verify your data source configuration (local vs S3)",
    "🔍 Try a simple query first: 'SELECT * FROM CUR LIMIT 1'"
  ],
  "troubleshooting_steps": [
    "1. Verify data source configuration (local vs S3)",
    "2. Check if CUR data has been loaded properly",
    "3. Test with 'SELECT 1' to verify basic engine functionality",
    "4. Contact administrator if data access issues persist"
  ]
}
```

### **SQL Syntax Error (`SQL_SYNTAX_ERROR`)**

**Triggered by:** Invalid SQL syntax, missing keywords, etc.

**Example Query:**

```sql
SELECT line_item_unblended_cost FROM CUR WHERE
```

**Response:**

```json
{
  "error": "There's a syntax error in your SQL query. Please review and correct the SQL syntax.",
  "error_type": "SQL_SYNTAX_ERROR",
  "suggestions": [
    "📝 Check for missing commas, quotation marks, or parentheses",
    "🔤 Verify proper SQL keyword spelling (SELECT, FROM, WHERE, etc.)",
    "⚠️ Ensure string values are properly quoted with single quotes ('value')",
    "🔍 Test with a simple query first: 'SELECT * FROM CUR LIMIT 1'",
    "📚 Review SQL syntax documentation for the specific function or clause you're using",
    "🎯 Use the /bedrock/generate-query endpoint to get AI-generated correct SQL"
  ],
  "troubleshooting_steps": [
    "1. Copy your query to a SQL editor for syntax highlighting",
    "2. Break down complex queries into smaller parts",
    "3. Test each part of the query individually",
    "4. Use the AI query generator for help with complex queries"
  ]
}
```

### **Access Denied (`ACCESS_DENIED`)**

**Triggered by:** Permission issues, AWS credential problems

**Response:**

```json
{
  "error": "Access denied. There's an issue with permissions or authentication.",
  "error_type": "ACCESS_DENIED",
  "suggestions": [
    "🔐 Check your AWS credentials configuration",
    "🪣 Verify you have read access to the S3 bucket containing CUR data",
    "⚙️ Ensure your data source configuration is correct",
    "🔄 Try switching to local data if available (set force_s3=false)",
    "📞 Contact your administrator to verify your permissions",
    "🛠️ Check the Infralyzer configuration for data source settings"
  ],
  "troubleshooting_steps": [
    "1. Verify AWS credentials are configured correctly",
    "2. Check S3 bucket permissions for CUR data",
    "3. Test with local data if available",
    "4. Contact your cloud administrator for permission review"
  ]
}
```

### **General Execution Error (`EXECUTION_ERROR`)**

**Triggered by:** Other query execution issues

**Response:**

```json
{
  "error": "Your query encountered an execution error. Please review your query and data availability.",
  "error_type": "EXECUTION_ERROR",
  "suggestions": [
    "📝 Double-check your query syntax and column names",
    "📊 Verify that your data is properly loaded and accessible",
    "🔍 Try a simpler query to test: 'SELECT COUNT(*) FROM CUR'",
    "📅 Check if you're querying data for time periods that exist in your dataset",
    "💰 Ensure you're using correct filter conditions (dates, service names, etc.)",
    "🚀 Use the FinOps expert chatbot for query assistance: /bedrock/finops-expert"
  ],
  "troubleshooting_steps": [
    "1. Start with a simple 'SELECT COUNT(*) FROM CUR' query",
    "2. Add filters and columns gradually to isolate the issue",
    "3. Check date ranges and filter values for validity",
    "4. Verify data exists for the time period you're querying"
  ]
}
```

## 🔧 Response Structure

All error responses now follow this consistent structure:

```json
{
  "detail": {
    "error": "User-friendly error message",
    "error_type": "ERROR_CATEGORY",
    "suggestions": [
      "📝 Actionable suggestion with emoji",
      "🔍 Another helpful tip",
      "💡 Smart suggestion with context"
    ],
    "metadata": {
      "query_type": "sql_query",
      "execution_time_ms": 150.5,
      "engine_used": "duckdb",
      "original_query": "SELECT product_region FROM...",
      "troubleshooting_steps": [
        "1. First troubleshooting step",
        "2. Second step with specific guidance",
        "3. Third step for resolution"
      ]
    }
  }
}
```

## 📊 HTTP Status Codes

| Status Code                   | Meaning          | When Used                                              |
| ----------------------------- | ---------------- | ------------------------------------------------------ |
| **200 OK**                    | Success          | Query executed successfully                            |
| **400 Bad Request**           | User Error       | Invalid query, wrong column/table names, syntax errors |
| **422 Unprocessable Entity**  | Validation Error | Request format issues, parameter validation            |
| **500 Internal Server Error** | System Error     | Unexpected system failures (rare)                      |

**Key Change:** Query errors now return `400 Bad Request` instead of `500 Internal Server Error`, indicating the error is on the user side and can be fixed.

## 💡 Smart Features

### **Column Name Suggestions**

When a column is not found, the system:

1. **Extracts the specific column name** from the error
2. **Finds similar columns** mentioned in the database error
3. **Suggests the most likely alternatives**

Example:

```
Query: SELECT product_region FROM CUR
→ "Did you mean one of these columns? product_region_code, product_operation, product"
```

### **Context-Aware Guidance**

Different error types get specialized suggestions:

- **Column errors** → Schema documentation, column listing queries
- **Table errors** → Available tables, data source configuration
- **Syntax errors** → SQL validation tools, AI query generator
- **Access errors** → Credential configuration, permission checks

### **Progressive Troubleshooting**

Each error type includes step-by-step troubleshooting:

1. **Immediate fixes** (simple syntax corrections)
2. **Verification steps** (test queries, data checks)
3. **Advanced diagnosis** (configuration review)
4. **Escalation paths** (contact administrator)

### **Resource Links**

Errors include references to helpful resources:

- 📋 `/bedrock/finops-expert` for query assistance
- 🎯 `/bedrock/generate-query` for AI-generated SQL
- 📚 Schema documentation and guides
- 🔍 Diagnostic queries to test your setup

## 🧪 Testing

Run the comprehensive test suite to see the improvements:

```bash
cd infralyzer/examples
python test_improved_query_error_handling.py
```

**Test Categories:**

1. **Column Not Found Errors** - Invalid column names with smart suggestions
2. **Table Not Found Errors** - Wrong table names with available alternatives
3. **SQL Syntax Errors** - Malformed queries with syntax guidance
4. **Complex Query Errors** - Multi-part queries with targeted advice
5. **Regression Tests** - Ensure valid queries still work correctly

## 🔄 Migration Guide

### **For API Clients**

Update your error handling to expect:

- **Status Code:** `400` instead of `500` for query errors
- **Response Structure:** Structured error object instead of plain text
- **Error Classification:** Use `error_type` field for programmatic handling

### **Before (Old Error Handling)**

```python
try:
    response = requests.post("/api/v1/finops/query", json=payload)
    if response.status_code == 500:
        print(f"Database error: {response.text}")
except Exception as e:
    print(f"Request failed: {e}")
```

### **After (New Error Handling)**

```python
try:
    response = requests.post("/api/v1/finops/query", json=payload)
    if response.status_code == 400:
        error_detail = response.json().get('detail', {})
        print(f"Query Error: {error_detail.get('error')}")

        # Show suggestions to user
        for suggestion in error_detail.get('suggestions', []):
            print(f"💡 {suggestion}")

        # Show troubleshooting steps
        for step in error_detail.get('metadata', {}).get('troubleshooting_steps', []):
            print(f"🔧 {step}")

    elif response.status_code == 200:
        # Process successful response
        result = response.json()

except Exception as e:
    print(f"Request failed: {e}")
```

## 📈 Benefits

### **For End Users**

- ✅ **Clear error messages** instead of database jargon
- 💡 **Actionable suggestions** to fix their queries
- 🔧 **Step-by-step guidance** for troubleshooting
- 🎯 **Smart suggestions** for similar column/table names

### **For Developers**

- 📊 **Structured error responses** for programmatic handling
- 🏷️ **Error type classification** for targeted UI handling
- 📝 **Rich metadata** for debugging and analytics
- 🔄 **Consistent response format** across all error types

### **For Support Teams**

- 📋 **Self-service resolution** reduces support tickets
- 🎯 **Specific error types** for faster issue classification
- 🔧 **Troubleshooting workflows** built into responses
- 📚 **Resource references** for user education

## 🚀 Future Enhancements

The error handling system is designed to be extensible:

1. **Machine Learning Suggestions** - Learn from common errors to provide better suggestions
2. **Interactive Fixes** - Provide "fix this for me" endpoints for common errors
3. **Query Templates** - Suggest complete query templates based on error context
4. **Real-time Validation** - Client-side validation to prevent errors before submission
5. **Error Analytics** - Track common errors to improve documentation and UX

---

The improved error handling transforms frustrating database errors into helpful learning opportunities, making the query API more accessible and user-friendly! 🎯
