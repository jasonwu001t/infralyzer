"""
AWS Bedrock Handler - Comprehensive chatbot and knowledge base management

This module provides a complete interface for AWS Bedrock services including:
- Multiple AI model support and configuration
- Knowledge base management for CUR-specific data
- Chatbot functionality with context management
- Structured query generation for frontend applications
"""

import json
import boto3
import time
from typing import Dict, Any, List, Optional, Union, Literal
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import logging

from ..auth import get_boto3_client
from ..engine.data_config import DataConfig
from ..exceptions import InfralyzerError

# Configure logging
logger = logging.getLogger(__name__)


class BedrockModel(str, Enum):
    """Supported Bedrock AI models."""
    CLAUDE_3_HAIKU = "anthropic.claude-3-haiku-20240307-v1:0"
    CLAUDE_3_SONNET = "anthropic.claude-3-sonnet-20240229-v1:0" 
    CLAUDE_3_OPUS = "anthropic.claude-3-opus-20240229-v1:0"
    CLAUDE_3_5_SONNET = "anthropic.claude-3-5-sonnet-20241022-v2:0"
    TITAN_TEXT_G1_LARGE = "amazon.titan-text-lite-v1"
    TITAN_TEXT_G1_EXPRESS = "amazon.titan-text-express-v1"
    COHERE_COMMAND_TEXT = "cohere.command-text-v14"
    COHERE_COMMAND_LIGHT_TEXT = "cohere.command-light-text-v14"
    AI21_J2_ULTRA = "ai21.j2-ultra-v1"
    AI21_J2_MID = "ai21.j2-mid-v1"
    LLAMA2_13B_CHAT = "meta.llama2-13b-chat-v1"
    LLAMA2_70B_CHAT = "meta.llama2-70b-chat-v1"


class KnowledgeBaseStatus(str, Enum):
    """Knowledge base status states."""
    CREATING = "CREATING"
    ACTIVE = "ACTIVE" 
    DELETING = "DELETING"
    UPDATING = "UPDATING"
    FAILED = "FAILED"


@dataclass
class ModelConfiguration:
    """Configuration for AI model parameters."""
    model_id: BedrockModel
    max_tokens: int = 4096
    temperature: float = 0.1
    top_p: float = 0.9
    top_k: int = 250
    stop_sequences: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API calls."""
        config = {
            "maxTokens": self.max_tokens,
            "temperature": self.temperature,
            "topP": self.top_p
        }
        
        if self.stop_sequences:
            config["stopSequences"] = self.stop_sequences
            
        # Model-specific parameters
        if "anthropic" in self.model_id.value:
            config["topK"] = self.top_k
        elif "cohere" in self.model_id.value:
            config["k"] = self.top_k
            
        return config


@dataclass
class KnowledgeBaseConfig:
    """Configuration for knowledge base creation."""
    name: str
    description: str
    role_arn: str
    s3_bucket: str
    s3_prefix: str
    embedding_model_arn: str = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
    data_source_type: str = "S3"
    chunk_size: int = 300
    overlap_percentage: int = 20


class BedrockHandler:
    """
    Comprehensive AWS Bedrock handler for chatbot and knowledge base functionality.
    
    Features:
    - Multiple AI model support with configuration management
    - Knowledge base creation and management for CUR data
    - Chatbot functionality with conversation context
    - Structured query generation for frontend applications
    - CUR-specific knowledge integration
    """
    
    def __init__(self, config: DataConfig, default_model: BedrockModel = BedrockModel.TITAN_TEXT_G1_EXPRESS):
        """
        Initialize Bedrock handler with configuration.
        
        Args:
            config: DataConfig with AWS credentials and settings
            default_model: Default AI model to use
        """
        self.config = config
        self.default_model = default_model
        self.conversation_context: Dict[str, List[Dict]] = {}
        
        # Initialize Bedrock clients
        self._bedrock_runtime = None
        self._bedrock_agent = None
        self._bedrock_agent_runtime = None
        
        # Knowledge base cache
        self._knowledge_bases: Dict[str, Dict] = {}
        
        # CUR-specific knowledge base setup
        self.cur_knowledge_base_id: Optional[str] = None
        
    def _get_bedrock_runtime(self):
        """Get Bedrock Runtime client."""
        if self._bedrock_runtime is None:
            creds = self.config.get_aws_credentials()
            self._bedrock_runtime = get_boto3_client('bedrock-runtime', **creds)
        return self._bedrock_runtime
    
    def _get_bedrock_agent(self):
        """Get Bedrock Agent client."""
        if self._bedrock_agent is None:
            creds = self.config.get_aws_credentials()
            self._bedrock_agent = get_boto3_client('bedrock-agent', **creds)
        return self._bedrock_agent
    
    def _get_bedrock_agent_runtime(self):
        """Get Bedrock Agent Runtime client."""
        if self._bedrock_agent_runtime is None:
            creds = self.config.get_aws_credentials()
            self._bedrock_agent_runtime = get_boto3_client('bedrock-agent-runtime', **creds)
        return self._bedrock_agent_runtime
    
    def list_available_models(self) -> List[Dict[str, Any]]:
        """
        List all available Bedrock models.
        
        Returns:
            List of available models with their capabilities
        """
        try:
            # Use bedrock client (not bedrock-runtime) for listing models
            creds = self.config.get_aws_credentials()
            bedrock = get_boto3_client('bedrock', **creds)
            response = bedrock.list_foundation_models()
            
            models = []
            for model in response.get('modelSummaries', []):
                models.append({
                    'model_id': model['modelId'],
                    'model_name': model['modelName'],
                    'provider_name': model['providerName'],
                    'input_modalities': model.get('inputModalities', []),
                    'output_modalities': model.get('outputModalities', []),
                    'inference_types': model.get('inferenceTypesSupported', []),
                    'model_lifecycle': model.get('modelLifecycle', {})
                })
            
            return models
            
        except Exception as e:
            logger.error(f"Error listing Bedrock models: {e}")
            raise InfralyzerError(f"Failed to list Bedrock models: {e}")
    
    def create_knowledge_base(self, kb_config: KnowledgeBaseConfig) -> Dict[str, Any]:
        """
        Create a new knowledge base for CUR data.
        
        Args:
            kb_config: Knowledge base configuration
            
        Returns:
            Knowledge base creation details
        """
        try:
            bedrock_agent = self._get_bedrock_agent()
            
            # Create knowledge base
            response = bedrock_agent.create_knowledge_base(
                name=kb_config.name,
                description=kb_config.description,
                roleArn=kb_config.role_arn,
                knowledgeBaseConfiguration={
                    'type': 'VECTOR',
                    'vectorKnowledgeBaseConfiguration': {
                        'embeddingModelArn': kb_config.embedding_model_arn
                    }
                },
                storageConfiguration={
                    'type': 'OPENSEARCH_SERVERLESS',
                    'opensearchServerlessConfiguration': {
                        'collectionArn': f"arn:aws:aoss:{self.config.aws_region}:{self.config.get_account_id()}:collection/{kb_config.name.lower()}",
                        'vectorIndexName': f"{kb_config.name.lower()}-index",
                        'fieldMapping': {
                            'vectorField': 'vector',
                            'textField': 'text',
                            'metadataField': 'metadata'
                        }
                    }
                }
            )
            
            knowledge_base = response['knowledgeBase']
            knowledge_base_id = knowledge_base['knowledgeBaseId']
            
            # Create data source
            data_source_response = bedrock_agent.create_data_source(
                knowledgeBaseId=knowledge_base_id,
                name=f"{kb_config.name}-datasource",
                description="CUR data source for cost analysis",
                dataSourceConfiguration={
                    'type': kb_config.data_source_type,
                    's3Configuration': {
                        'bucketArn': f"arn:aws:s3:::{kb_config.s3_bucket}",
                        'inclusionPrefixes': [kb_config.s3_prefix]
                    }
                },
                vectorIngestionConfiguration={
                    'chunkingConfiguration': {
                        'chunkingStrategy': 'FIXED_SIZE',
                        'fixedSizeChunkingConfiguration': {
                            'maxTokens': kb_config.chunk_size,
                            'overlapPercentage': kb_config.overlap_percentage
                        }
                    }
                }
            )
            
            # Cache knowledge base info
            self._knowledge_bases[knowledge_base_id] = {
                'knowledge_base': knowledge_base,
                'data_source': data_source_response['dataSource'],
                'config': kb_config
            }
            
            return {
                'knowledge_base_id': knowledge_base_id,
                'knowledge_base': knowledge_base,
                'data_source': data_source_response['dataSource'],
                'status': knowledge_base['status']
            }
            
        except Exception as e:
            logger.error(f"Error creating knowledge base: {e}")
            raise InfralyzerError(f"Failed to create knowledge base: {e}")
    
    def ingest_knowledge_base_data(self, knowledge_base_id: str) -> Dict[str, Any]:
        """
        Start ingestion job for knowledge base.
        
        Args:
            knowledge_base_id: ID of the knowledge base
            
        Returns:
            Ingestion job details
        """
        try:
            bedrock_agent = self._get_bedrock_agent()
            
            # Get data source ID
            if knowledge_base_id not in self._knowledge_bases:
                # Fetch knowledge base info
                kb_response = bedrock_agent.get_knowledge_base(knowledgeBaseId=knowledge_base_id)
                self._knowledge_bases[knowledge_base_id] = {'knowledge_base': kb_response['knowledgeBase']}
            
            # List data sources
            data_sources = bedrock_agent.list_data_sources(knowledgeBaseId=knowledge_base_id)
            data_source_id = data_sources['dataSourceSummaries'][0]['dataSourceId']
            
            # Start ingestion job
            response = bedrock_agent.start_ingestion_job(
                knowledgeBaseId=knowledge_base_id,
                dataSourceId=data_source_id,
                description="Ingest CUR cost data for analysis"
            )
            
            return {
                'ingestion_job': response['ingestionJob'],
                'job_id': response['ingestionJob']['ingestionJobId'],
                'status': response['ingestionJob']['status']
            }
            
        except Exception as e:
            logger.error(f"Error starting ingestion job: {e}")
            raise InfralyzerError(f"Failed to start ingestion job: {e}")
    
    def list_knowledge_bases(self) -> List[Dict[str, Any]]:
        """
        List all available knowledge bases.
        
        Returns:
            List of knowledge bases
        """
        try:
            bedrock_agent = self._get_bedrock_agent()
            response = bedrock_agent.list_knowledge_bases()
            
            knowledge_bases = []
            for kb in response.get('knowledgeBaseSummaries', []):
                knowledge_bases.append({
                    'knowledge_base_id': kb['knowledgeBaseId'],
                    'name': kb['name'],
                    'description': kb.get('description', ''),
                    'status': kb['status'],
                    'created_at': kb.get('createdAt'),
                    'updated_at': kb.get('updatedAt')
                })
            
            return knowledge_bases
            
        except Exception as e:
            logger.error(f"Error listing knowledge bases: {e}")
            raise InfralyzerError(f"Failed to list knowledge bases: {e}")
    
    def chat_with_knowledge_base(
        self,
        message: str,
        knowledge_base_id: str,
        conversation_id: Optional[str] = None,
        model_config: Optional[ModelConfiguration] = None
    ) -> Dict[str, Any]:
        """
        Chat with AI using knowledge base context.
        
        Args:
            message: User message
            knowledge_base_id: ID of knowledge base to use
            conversation_id: Optional conversation ID for context
            model_config: Optional model configuration
            
        Returns:
            AI response with knowledge base context
        """
        try:
            bedrock_agent_runtime = self._get_bedrock_agent_runtime()
            
            if model_config is None:
                model_config = ModelConfiguration(model_id=self.default_model)
            
            # Prepare session configuration
            session_config = {
                'kmsKeyArn': self.config.get('kms_key_arn', ''),
                'sessionTtl': 3600
            }
            
            # Chat with knowledge base
            response = bedrock_agent_runtime.retrieve_and_generate(
                input={
                    'text': message
                },
                retrieveAndGenerateConfiguration={
                    'type': 'KNOWLEDGE_BASE',
                    'knowledgeBaseConfiguration': {
                        'knowledgeBaseId': knowledge_base_id,
                        'modelArn': f"arn:aws:bedrock:{self.config.aws_region}::foundation-model/{model_config.model_id.value}",
                        'retrievalConfiguration': {
                            'vectorSearchConfiguration': {
                                'numberOfResults': 10
                            }
                        }
                    }
                },
                sessionConfiguration=session_config,
                sessionId=conversation_id or f"session-{int(time.time())}"
            )
            
            # Extract response
            output = response['output']
            citations = response.get('citations', [])
            
            # Process citations for context
            knowledge_sources = []
            for citation in citations:
                for reference in citation.get('retrievedReferences', []):
                    knowledge_sources.append({
                        'content': reference['content']['text'],
                        'location': reference['location'],
                        'metadata': reference.get('metadata', {})
                    })
            
            return {
                'response': output['text'],
                'conversation_id': response['sessionId'],
                'knowledge_sources': knowledge_sources,
                'model_used': model_config.model_id.value,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error chatting with knowledge base: {e}")
            raise InfralyzerError(f"Failed to chat with knowledge base: {e}")
    
    def generate_cur_structured_query(
        self,
        user_query: str,
        model_config: Optional[ModelConfiguration] = None,
        include_examples: bool = True
    ) -> Dict[str, Any]:
        """
        Generate structured SQL query for CUR data based on natural language input.
        
        Args:
            user_query: Natural language query about cost data
            model_config: Optional model configuration
            include_examples: Whether to include example queries in prompt
            
        Returns:
            Structured query response for frontend use
        """
        try:
            if model_config is None:
                model_config = ModelConfiguration(model_id=self.default_model)
            
            # Construct CUR-specific prompt
            system_prompt = self._build_cur_query_prompt(include_examples)
            
            # Format the request based on model type
            if "anthropic" in model_config.model_id.value:
                request_body = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": model_config.max_tokens,
                    "temperature": model_config.temperature,
                    "top_p": model_config.top_p,
                    "top_k": model_config.top_k,
                    "messages": [
                        {
                            "role": "user",
                            "content": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate the structured response:"
                        }
                    ]
                }
            elif "amazon.titan" in model_config.model_id.value:
                # Titan models use different parameter names
                request_body = {
                    "inputText": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate the structured response:",
                    "textGenerationConfig": {
                        "maxTokenCount": model_config.max_tokens,
                        "temperature": model_config.temperature,
                        "topP": model_config.top_p,
                        "stopSequences": model_config.stop_sequences or []
                    }
                }
            elif "cohere" in model_config.model_id.value:
                # Cohere models format
                request_body = {
                    "prompt": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate the structured response:",
                    "max_tokens": model_config.max_tokens,
                    "temperature": model_config.temperature,
                    "p": model_config.top_p,
                    "k": model_config.top_k,
                    "stop_sequences": model_config.stop_sequences or []
                }
            elif "meta.llama" in model_config.model_id.value:
                # Llama models format
                request_body = {
                    "prompt": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate the structured response:",
                    "max_gen_len": model_config.max_tokens,
                    "temperature": model_config.temperature,
                    "top_p": model_config.top_p
                }
            else:
                # Generic fallback
                request_body = {
                    "inputText": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate the structured response:",
                    "textGenerationConfig": {
                        "maxTokenCount": model_config.max_tokens,
                        "temperature": model_config.temperature,
                        "topP": model_config.top_p
                    }
                }
            
            # Call Bedrock
            bedrock_runtime = self._get_bedrock_runtime()
            response = bedrock_runtime.invoke_model(
                modelId=model_config.model_id.value,
                body=json.dumps(request_body)
            )
            
            # Parse response based on model type
            response_body = json.loads(response['body'].read())
            
            if "anthropic" in model_config.model_id.value:
                ai_response = response_body['content'][0]['text']
            elif "amazon.titan" in model_config.model_id.value:
                ai_response = response_body['results'][0]['outputText']
            elif "cohere" in model_config.model_id.value:
                ai_response = response_body['generations'][0]['text']
            elif "meta.llama" in model_config.model_id.value:
                ai_response = response_body['generation']
            else:
                # Try common response formats
                if 'results' in response_body and len(response_body['results']) > 0:
                    ai_response = response_body['results'][0].get('outputText', str(response_body))
                elif 'generation' in response_body:
                    ai_response = response_body['generation']
                elif 'content' in response_body:
                    ai_response = response_body['content'][0]['text'] if isinstance(response_body['content'], list) else response_body['content']
                else:
                    ai_response = str(response_body)
            
            # Parse structured response
            structured_query = self._parse_structured_response(ai_response, user_query)
            
            return {
                'structured_query': structured_query,
                'original_query': user_query,
                'model_used': model_config.model_id.value,
                'confidence': structured_query.get('confidence', 0.8),
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating structured query: {e}")
            raise InfralyzerError(f"Failed to generate structured query: {e}")
    
    def _build_cur_query_prompt(self, include_examples: bool = True) -> str:
        """Build CUR-specific query generation prompt."""
        
        base_prompt = """You are an expert AWS Cost and Usage Report (CUR) analyst. Convert natural language queries into structured SQL queries for cost analysis.

CUR Table Schema:
- line_item_usage_start_date: When usage started
- line_item_usage_end_date: When usage ended  
- line_item_unblended_cost: Actual cost amount
- line_item_resource_id: AWS resource identifier
- product_servicecode: AWS service (e.g., AmazonEC2, AmazonS3)
- product_region: AWS region
- bill_payer_account_id: Billing account
- line_item_usage_account_id: Resource owner account
- line_item_usage_type: Type of usage
- product_instance_type: Instance type for compute
- pricing_term: On-Demand, Reserved, Spot
- reservation_reservation_a_r_n: Reserved Instance ARN

Return ONLY a JSON object with this structure:
{
  "sql_query": "SELECT statement for the query",
  "query_type": "summary|trend|detail|comparison", 
  "visualization_type": "bar_chart|line_chart|pie_chart|table|metric_card",
  "title": "Chart title for frontend",
  "description": "Brief description of what this shows",
  "filters": {
    "date_range": "YYYY-MM-DD to YYYY-MM-DD or relative like 'last_30_days'",
    "services": ["AmazonEC2", "AmazonS3"],
    "accounts": ["123456789012"],
    "regions": ["us-east-1", "us-west-2"]
  },
  "confidence": 0.95
}"""

        if include_examples:
            examples = """

Example Queries:

User: "What are my top 5 most expensive services this month?"
{
  "sql_query": "SELECT product_servicecode, SUM(line_item_unblended_cost) as total_cost FROM CUR WHERE DATE_TRUNC('month', line_item_usage_start_date) = DATE_TRUNC('month', CURRENT_DATE) AND line_item_unblended_cost > 0 GROUP BY 1 ORDER BY 2 DESC LIMIT 5",
  "query_type": "summary",
  "visualization_type": "bar_chart", 
  "title": "Top 5 Services by Cost This Month",
  "description": "Services with highest spending in current month",
  "filters": {"date_range": "current_month"},
  "confidence": 0.95
}

User: "Show me EC2 costs by region over the last 6 months"
{
  "sql_query": "SELECT DATE_TRUNC('month', line_item_usage_start_date) as month, product_region, SUM(line_item_unblended_cost) as monthly_cost FROM CUR WHERE product_servicecode = 'AmazonEC2' AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '6 months' AND line_item_unblended_cost > 0 GROUP BY 1, 2 ORDER BY 1, 3 DESC",
  "query_type": "trend",
  "visualization_type": "line_chart",
  "title": "EC2 Costs by Region (6 Months)",
  "description": "Monthly EC2 spending trends across regions",
  "filters": {"date_range": "last_6_months", "services": ["AmazonEC2"]},
  "confidence": 0.92
}"""
            base_prompt += examples
        
        return base_prompt
    
    def _parse_structured_response(self, ai_response: str, original_query: str) -> Dict[str, Any]:
        """Parse AI response into structured format."""
        try:
            # Try to extract JSON from response
            import re
            json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
            
            if json_match:
                json_str = json_match.group()
                structured = json.loads(json_str)
                
                # Validate required fields
                required_fields = ['sql_query', 'query_type', 'visualization_type', 'title']
                for field in required_fields:
                    if field not in structured:
                        structured[field] = self._generate_fallback_value(field, original_query)
                
                return structured
            else:
                # Fallback structured response
                return self._generate_fallback_response(original_query)
                
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse AI response as JSON: {ai_response[:200]}...")
            return self._generate_fallback_response(original_query)
    
    def _generate_fallback_value(self, field: str, query: str) -> Any:
        """Generate fallback values for missing fields."""
        fallbacks = {
            'sql_query': f"SELECT product_servicecode, SUM(line_item_unblended_cost) as total_cost FROM CUR WHERE line_item_unblended_cost > 0 GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            'query_type': 'summary',
            'visualization_type': 'bar_chart',
            'title': f"Cost Analysis: {query[:50]}...",
            'description': 'Cost analysis based on user query',
            'confidence': 0.7
        }
        return fallbacks.get(field, '')
    
    def _generate_fallback_response(self, query: str) -> Dict[str, Any]:
        """Generate fallback structured response."""
        return {
            'sql_query': "SELECT product_servicecode, SUM(line_item_unblended_cost) as total_cost FROM CUR WHERE line_item_unblended_cost > 0 AND line_item_usage_start_date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            'query_type': 'summary',
            'visualization_type': 'bar_chart',
            'title': 'Top Services by Cost (Last 30 Days)',
            'description': 'Services with highest spending in the last 30 days',
            'filters': {'date_range': 'last_30_days'},
            'confidence': 0.7,
            'fallback': True,
            'original_query': query
        }
    
    def create_cur_knowledge_base(
        self,
        s3_bucket: str,
        s3_prefix: str,
        role_arn: str,
        name: str = "CUR-FinOps-Knowledge"
    ) -> Dict[str, Any]:
        """
        Create a CUR-specific knowledge base with cost optimization knowledge.
        
        Args:
            s3_bucket: S3 bucket with CUR data
            s3_prefix: S3 prefix for CUR data
            role_arn: IAM role ARN for Bedrock
            name: Knowledge base name
            
        Returns:
            Created knowledge base details
        """
        kb_config = KnowledgeBaseConfig(
            name=name,
            description="AWS Cost and Usage Report knowledge base for FinOps optimization",
            role_arn=role_arn,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            chunk_size=500,  # Larger chunks for cost data
            overlap_percentage=15
        )
        
        result = self.create_knowledge_base(kb_config)
        self.cur_knowledge_base_id = result['knowledge_base_id']
        
        return result
    
    def get_conversation_history(self, conversation_id: str) -> List[Dict[str, Any]]:
        """
        Get conversation history for a given conversation ID.
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            List of conversation messages
        """
        return self.conversation_context.get(conversation_id, [])
    
    def clear_conversation(self, conversation_id: str) -> bool:
        """
        Clear conversation history for a given conversation ID.
        
        Args:
            conversation_id: ID of the conversation to clear
            
        Returns:
            True if cleared successfully
        """
        if conversation_id in self.conversation_context:
            del self.conversation_context[conversation_id]
            return True
        return False