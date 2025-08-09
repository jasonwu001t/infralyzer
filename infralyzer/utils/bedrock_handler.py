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
    # Claude 3.0 Series
    CLAUDE_3_HAIKU = "anthropic.claude-3-haiku-20240307-v1:0"
    CLAUDE_3_SONNET = "anthropic.claude-3-sonnet-20240229-v1:0" 
    CLAUDE_3_OPUS = "anthropic.claude-3-opus-20240229-v1:0"
    
    # Claude 3.5 Series  
    CLAUDE_3_5_SONNET = "anthropic.claude-3-5-sonnet-20241022-v2:0"
    CLAUDE_3_5_HAIKU = "anthropic.claude-3-5-haiku-20241022-v1:0"
    
    # Claude 3.7 Series
    CLAUDE_3_7_SONNET = "anthropic.claude-3-7-sonnet-20250219-v1:0"
    
    # Claude 4.0 Series (Latest)
    CLAUDE_4_OPUS = "anthropic.claude-opus-4-20250514-v1:0"
    CLAUDE_4_SONNET = "anthropic.claude-sonnet-4-20250514-v1:0"
    
    # Amazon Titan Models
    TITAN_TEXT_G1_LARGE = "amazon.titan-text-lite-v1"
    TITAN_TEXT_G1_EXPRESS = "amazon.titan-text-express-v1"
    
    # Cohere Models
    COHERE_COMMAND_TEXT = "cohere.command-text-v14"
    COHERE_COMMAND_LIGHT_TEXT = "cohere.command-light-text-v14"
    
    # AI21 Models
    AI21_J2_ULTRA = "ai21.j2-ultra-v1"
    AI21_J2_MID = "ai21.j2-mid-v1"
    
    # Meta Llama Models
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
        include_examples: bool = True,
        validate_cur_relevance: bool = True
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
            
            # Optional: Pre-validate query relevance using programmatic check
            if validate_cur_relevance:
                is_cur_related = self._is_cur_related_query(user_query)
                logger.info(f"Programmatic CUR relevance check for '{user_query}': {is_cur_related}")
                
                # If programmatic check says it's NOT CUR-related, still let AI decide 
                # but log the discrepancy for monitoring
                if not is_cur_related:
                    logger.warning(f"Query may not be CUR-related based on keywords: '{user_query}'")
            
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
                            "content": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate ONLY the JSON response (start with {{ and end with }}):"
                        }
                    ]
                }
            elif "amazon.titan" in model_config.model_id.value:
                # Titan models use different parameter names
                request_body = {
                    "inputText": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate ONLY the JSON response (start with {{ and end with }}):",
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
                    "prompt": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate ONLY the JSON response (start with {{ and end with }}):",
                    "max_tokens": model_config.max_tokens,
                    "temperature": model_config.temperature,
                    "p": model_config.top_p,
                    "k": model_config.top_k,
                    "stop_sequences": model_config.stop_sequences or []
                }
            elif "meta.llama" in model_config.model_id.value:
                # Llama models format
                request_body = {
                    "prompt": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate ONLY the JSON response (start with {{ and end with }}):",
                    "max_gen_len": model_config.max_tokens,
                    "temperature": model_config.temperature,
                    "top_p": model_config.top_p
                }
            else:
                # Generic fallback
                request_body = {
                    "inputText": f"{system_prompt}\n\nUser Query: {user_query}\n\nGenerate ONLY the JSON response (start with {{ and end with }}):",
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
    
    def chat_with_finops_expert(
        self,
        message: str,
        conversation_id: Optional[str] = None,
        model_config: Optional[ModelConfiguration] = None,
        include_examples: bool = True,
        context_type: str = "general"
    ) -> Dict[str, Any]:
        """
        Chat with FinOps expert AI for cost optimization and AWS billing guidance.
        
        Args:
            message: User message about FinOps, cost optimization, or AWS billing
            conversation_id: Optional conversation ID for context
            model_config: Model configuration to use
            include_examples: Whether to include FinOps examples and best practices
            context_type: Type of FinOps context (general, cost_analysis, optimization, ri_sp)
            
        Returns:
            Dictionary containing expert response and recommendations
        """
        try:
            if model_config is None:
                model_config = ModelConfiguration(model_id=self.default_model)
            
            # Generate conversation ID if not provided
            if conversation_id is None:
                conversation_id = f"finops_chat_{int(datetime.now().timestamp())}"
            
            # Build FinOps expert prompt
            system_prompt = self._build_finops_expert_prompt(include_examples, context_type)
            
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
                            "content": f"{system_prompt}\n\nUser Question: {message}\n\nProvide expert FinOps guidance:"
                        }
                    ]
                }
            elif "amazon.titan" in model_config.model_id.value:
                request_body = {
                    "inputText": f"{system_prompt}\n\nUser Question: {message}\n\nProvide expert FinOps guidance:",
                    "textGenerationConfig": {
                        "maxTokenCount": model_config.max_tokens,
                        "temperature": model_config.temperature,
                        "topP": model_config.top_p,
                        "stopSequences": model_config.stop_sequences or []
                    }
                }
            elif "cohere" in model_config.model_id.value:
                request_body = {
                    "prompt": f"{system_prompt}\n\nUser Question: {message}\n\nProvide expert FinOps guidance:",
                    "max_tokens": model_config.max_tokens,
                    "temperature": model_config.temperature,
                    "p": model_config.top_p,
                    "k": model_config.top_k,
                    "stop_sequences": model_config.stop_sequences or []
                }
            elif "meta.llama" in model_config.model_id.value:
                request_body = {
                    "prompt": f"{system_prompt}\n\nUser Question: {message}\n\nProvide expert FinOps guidance:",
                    "max_gen_len": model_config.max_tokens,
                    "temperature": model_config.temperature,
                    "top_p": model_config.top_p
                }
            else:
                request_body = {
                    "inputText": f"{system_prompt}\n\nUser Question: {message}\n\nProvide expert FinOps guidance:",
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
                if 'results' in response_body and len(response_body['results']) > 0:
                    ai_response = response_body['results'][0].get('outputText', str(response_body))
                elif 'generation' in response_body:
                    ai_response = response_body['generation']
                elif 'content' in response_body:
                    ai_response = response_body['content'][0]['text'] if isinstance(response_body['content'], list) else response_body['content']
                else:
                    ai_response = str(response_body)
            
            # Parse the response to extract recommendations and related topics
            parsed_response = self._parse_finops_response(ai_response, context_type)
            
            return {
                'response': parsed_response['response'],
                'conversation_id': conversation_id,
                'recommendations': parsed_response['recommendations'],
                'related_topics': parsed_response['related_topics'],
                'model_used': model_config.model_id.value,
                'context_type': context_type,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in FinOps expert chat: {e}")
            raise InfralyzerError(f"Failed to get FinOps expert response: {e}")
    
    def _build_cur_query_prompt(self, include_examples: bool = True) -> str:
        """Build CUR-specific query generation prompt."""
        
        base_prompt = """You are an expert AWS Cost and Usage Report 2.0 (CUR2.0) analyst. Convert natural language queries into structured SQL queries for cost analysis.

IMPORTANT: Only process queries related to AWS costs, usage, billing, financial analysis, or FinOps.

CUR-RELATED QUERIES (ACCEPT THESE):
✅ Cost/Charge/Spend Questions:
   - "What's my EC2 charge?" / "EC2 costs" / "EC2 spending"
   - "My Savings Plan charges" / "Savings Plan costs"
   - "Top 10 service charges" / "Most expensive services"
   - "Last month charges" / "This month's bill"
   - "Data transfer costs" / "Storage charges"

✅ Usage Analysis:
   - "EC2 usage" / "S3 storage usage" / "Lambda invocations"
   - "Reserved Instance utilization"
   - "Compute hours" / "Storage GB-hours"

✅ Financial Keywords:
   - cost, charge, spend, bill, expense, fee, price, rate
   - savings, discount, credit, refund
   - budget, forecast, trend, comparison

✅ AWS Services (when combined with cost/usage):
   - EC2, S3, RDS, Lambda, DynamoDB, CloudFront, etc.
   - Any AWS service + cost/usage context

✅ Time-based Financial Queries:
   - "last month", "this quarter", "year over year"
   - "daily costs", "monthly spending", "hourly usage"

NON-CUR QUERIES (REJECT THESE):
❌ Pure Technical Questions:
   - "How to deploy EC2?" / "CloudFormation templates"
   - "Security best practices" / "Performance optimization"
   - "API documentation" / "Configuration steps"

❌ General Knowledge:
   - Weather, cooking, geography, entertainment
   - Math problems, creative writing, news

❌ Non-Financial AWS:
   - Performance metrics without cost context
   - Security configurations, networking setup
   - Code examples, troubleshooting

If the user query is NOT related to AWS costs, billing, usage analysis, or financial optimization, you MUST respond with:
{
  "error": "not_cur_related",
  "message": "I can only help with AWS cost and usage analysis queries. Your question appears to be about [detected topic]. Please ask questions about AWS costs, billing, usage, or financial optimization instead.",
  "suggestions": ["What are my top 5 most expensive AWS services?", "Show me EC2 costs by region", "Analyze my Reserved Instance utilization"]
}

Making sure you aware Legacy CUR and CUR2.0 are different. CUR2.0 has fixed columns and map string columns.

CUR 2.0 Table Schema (COST_AND_USAGE_REPORT):
CUR 2.0 has 125+ columns organized into 11 groups with fixed schema and map string columns.

BILL COLUMNS:
- bill_payer_account_id (string): Account ID of the paying account (management account for organizations)
- bill_payer_account_name (string): Name of the paying account
- bill_billing_period_start_date (timestamp): Billing period start date in UTC (YYYY-MM-DDTHH:mm:ssZ)
- bill_billing_period_end_date (timestamp): Billing period end date in UTC (YYYY-MM-DDTHH:mm:ssZ)

IDENTITY COLUMNS:
- identity_line_item_id (string): Unique identifier for each line item
- identity_time_interval (string): Time interval for the line item (YYYY-MM-DDTHH:mm:ssZ/YYYY-MM-DDTHH:mm:ssZ)

LINE ITEM COLUMNS:
- line_item_usage_account_id (string): Account ID that used this line item
- line_item_usage_account_name (string): Account name that used this line item
- line_item_product_code (string): Code of the product measured
- line_item_usage_type (string): Usage details (e.g., BoxUsage:t2.micro, DataTransfer-Out-Bytes)
- line_item_operation (string): Specific AWS operation (e.g., RunInstances, GetObject, PutObject)
- line_item_usage_start_date (timestamp): Usage start date/time in UTC
- line_item_usage_end_date (timestamp): Usage end date/time in UTC
- line_item_usage_amount (double): Amount of usage incurred
- line_item_unblended_rate (string): Rate for individual account's service usage
- line_item_unblended_cost (double): UnblendedRate × UsageAmount (actual cost)
- line_item_blended_rate (string): Average cost per SKU across organization
- line_item_blended_cost (double): BlendedRate × UsageAmount
- line_item_currency_code (string): Currency code (e.g., USD, EUR)
- line_item_line_item_description (string): Description of the line item type
- line_item_line_item_type (string): Type of charge (Usage, Fee, Credit, Tax, Refund, etc.)
- line_item_tax_type (string): Type of tax applied
- line_item_legal_entity (string): Seller of Record for the product/service
- line_item_resource_id (string): ID of the provisioned resource (if INCLUDE_RESOURCES=TRUE)

PRODUCT COLUMNS:
- product (map<string,string>): MAP COLUMN containing product attributes as key-value pairs
  Sub-values include: product_marketplaceid, product_deployment_option, product_license_model, 
  product_operating_system, product_processor_features, product_storage_media, product_volume_type, etc.
- product_servicecode (string): AWS service identifier (AmazonEC2, AmazonS3, AmazonRDS, etc.)
- product_sku (string): Unique product code
- product_product_family (string): Product category (Compute Instance, Storage, Data Transfer, etc.)
- product_instance_type (string): EC2 instance type (t2.micro, m5.large, etc.)
- product_instance_family (string): EC2 instance family (t2, m5, c5, etc.)
- product_region_code (string): AWS region code (us-east-1, eu-west-1, etc.)
- product_location (string): Geographic location description
- product_location_type (string): Location type (AWS Region, AWS Edge Location, etc.)
- product_operation (string): Operation type (RunInstances, CreateBucket, etc.)
- product_pricing_unit (string): Billing unit (Hours, GB-Month, Requests, etc.)
- product_usagetype (string): Detailed usage type
- product_from_location (string): Source location for data transfer
- product_from_location_type (string): Source location type
- product_from_region_code (string): Source region code
- product_to_location (string): Destination location for data transfer
- product_to_location_type (string): Destination location type
- product_to_region_code (string): Destination region code

PRICING COLUMNS:
- pricing_public_on_demand_cost (double): Public On-Demand cost for the usage
- pricing_public_on_demand_rate (string): Public On-Demand rate

COST CATEGORY COLUMNS:
- cost_category (map<string,string>): MAP COLUMN with cost category assignments as key-value pairs
  Sub-values are user-defined based on cost categorization rules (e.g., Environment:Production, Team:Finance)

RESOURCE TAGS COLUMNS:
- resource_tags (map<string,string>): MAP COLUMN with resource tags as key-value pairs
  Sub-values include all user-applied tags (e.g., user:Name, user:Environment, user:Project, user:Owner, etc.)

RESERVATION COLUMNS:
- reservation_reservation_a_r_n (string): ARN of the reservation for Reserved Instances (RI)
- reservation_availability_zone (string): AZ where reservation applies for Reserved Instances (RI)
- reservation_instance_type (string): Instance type for the reservation for Reserved Instances (RI)
- reservation_platform (string): Platform for the reservation for Reserved Instances (RI)
- reservation_region (string): Region where reservation applies for Reserved Instances (RI)
- reservation_scope (string): Scope of the reservation for Reserved Instances (RI) (AZ, Region)
- reservation_start_time (string): Reservation start time for Reserved Instances (RI)
- reservation_end_time (string): Reservation end time

SAVINGS PLAN COLUMNS:
- savings_plan_savings_plan_a_r_n (string): ARN of the savings plan
- savings_plan_instance_type_family (string): Instance family covered by savings plan
- savings_plan_region (string): Region where savings plan applies
- savings_plan_payment_option (string): Payment option (All Upfront, Partial Upfront, No Upfront)
- savings_plan_purchase_term (string): Term length (1 year, 3 years)

DISCOUNT COLUMNS:
- discount (double): Amount of discount applied to line item
- total_discount (double): Total discount amount

SPLIT LINE ITEM COLUMNS (if INCLUDE_SPLIT_COST_ALLOCATION_DATA=TRUE):
- split_line_item_* columns: Allocation data for splitting costs across business units
- split_line_item_usage_amount (double): Usage amount allocated to split line item
- split_line_item_unblended_cost (double): Cost allocated to split line item

Key Notes:
- Map columns (product, cost_category, resource_tags) can be queried using dot notation: product.operating_system, resource_tags.user:Name
- Use AWS Glue Crawler to infer table schema automatically
- Table configurations affect available columns: TIME_GRANULARITY, INCLUDE_RESOURCES, INCLUDE_SPLIT_COST_ALLOCATION_DATA

IMPORTANT: You MUST return ONLY a valid JSON object. Do NOT include any explanatory text, SQL comments, or additional content outside the JSON.

The response must be EXACTLY in this JSON format:
{
  "sql_query": "SELECT statement for the query using CUR table",
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
}

CRITICAL: Start your response with { and end with }. Nothing else."""

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
    
    def _build_finops_expert_prompt(self, include_examples: bool = True, context_type: str = "general") -> str:
        """Build FinOps expert chatbot prompt."""
        
        base_prompt = f"""You are a senior FinOps (Financial Operations) expert and AWS cost optimization specialist with 10+ years of experience. You provide practical, actionable guidance on:

🏢 FINOPS EXPERTISE AREAS:
- AWS Cost Optimization & Right-sizing
- Reserved Instance and Savings Plan Strategy
- Cost Allocation & Chargeback/Showback
- AWS Billing Analysis & Cost Anomaly Detection
- Cloud Financial Management Best Practices
- FinOps Framework Implementation
- Cost Governance & Policy Development
- Multi-Cloud Cost Management

💰 COST OPTIMIZATION FOCUS:
- EC2 Right-sizing & Instance Optimization
- Storage Cost Optimization (S3, EBS, EFS)
- Data Transfer Cost Reduction
- Unused/Idle Resource Identification
- Reserved Instance Coverage & Utilization
- Savings Plan Recommendations
- Spot Instance Strategy
- Auto-scaling Optimization

📊 AWS BILLING & CUR EXPERTISE:
- Cost and Usage Report (CUR) Analysis
- AWS Cost Explorer & Budgets Setup
- Cost Anomaly Detection Configuration
- Tagging Strategy for Cost Allocation
- AWS Organizations Consolidated Billing
- Cost Center and Department Allocations

🎯 CONTEXT TYPE: {context_type.upper()}"""

        if context_type == "cost_analysis":
            base_prompt += """
            
SPECIALIZED FOCUS - COST ANALYSIS:
- Deep dive into cost trends and patterns
- Identify cost drivers and unexpected charges
- Compare costs across time periods, services, accounts
- Root cause analysis for cost increases
- Cost breakdown by resource, tag, or dimension"""

        elif context_type == "optimization":
            base_prompt += """
            
SPECIALIZED FOCUS - COST OPTIMIZATION:
- Specific optimization recommendations
- Right-sizing strategies for compute and storage
- Reserved Instance and Savings Plan analysis
- Waste identification and elimination
- Architectural changes for cost efficiency"""

        elif context_type == "ri_sp":
            base_prompt += """
            
SPECIALIZED FOCUS - RESERVED INSTANCES & SAVINGS PLANS:
- RI vs SP comparison and recommendations
- Coverage and utilization analysis
- Purchase strategies and timing
- Exchange and modification options
- ROI calculations and business cases"""

        base_prompt += """

🗣️ COMMUNICATION STYLE:
- Provide practical, actionable advice
- Include specific AWS service recommendations
- Mention relevant AWS tools and features
- Give concrete examples and use cases
- Structure responses with clear sections
- Include cost estimates when possible
- Reference FinOps best practices

📋 RESPONSE FORMAT:
Structure your response with:
1. **Direct Answer** - Address the specific question
2. **Recommendations** - 3-5 actionable recommendations  
3. **AWS Tools** - Relevant AWS services/features to use
4. **Best Practices** - FinOps principles that apply
5. **Next Steps** - What the user should do next

🚫 IMPORTANT LIMITATIONS:
- Only provide FinOps, cost optimization, and AWS billing advice
- If asked about non-FinOps topics, politely redirect to cost-related aspects
- Always consider security and compliance in recommendations
- Mention when recommendations need business context or approval"""

        if include_examples:
            examples = """

💡 EXAMPLE INTERACTIONS:

User: "Our EC2 costs doubled last month, what should I check?"
FinOps Expert: 
**Direct Answer:** A doubling of EC2 costs suggests either increased usage, instance size changes, or new deployments. Here's a systematic approach to investigate:

**Recommendations:**
1. Check AWS Cost Explorer for EC2 usage patterns and instance type changes
2. Review CloudTrail logs for recent EC2 launches or modifications
3. Analyze CUR data to identify specific instance types driving cost increases
4. Examine auto-scaling group activities and scaling policies
5. Verify if Reserved Instance coverage decreased

**AWS Tools:** Use Cost Explorer's "EC2 Instance Right Sizing" recommendations, AWS Compute Optimizer, and Cost Anomaly Detection.

**Best Practices:** Implement cost allocation tags, set up budget alerts at 80% threshold, and establish regular cost review cadence.

**Next Steps:** Set up automated EC2 cost monitoring dashboard and implement approval workflows for large instance deployments.

User: "Should we buy Reserved Instances or Savings Plans?"
FinOps Expert:
**Direct Answer:** The choice depends on your workload patterns and flexibility needs. Here's how to decide:

**Recommendations:**
1. Savings Plans for mixed workloads (EC2, Lambda, Fargate) with flexibility needs
2. RIs for stable, predictable EC2 workloads with known instance families
3. Start with Compute Savings Plans for 70% of stable usage
4. Use RIs for remaining high-utilization, specific instance types
5. Monitor utilization for 3+ months before committing to long-term plans

**AWS Tools:** Cost Explorer RI/SP recommendations, AWS Cost Optimization Hub, and Billing Console's coverage reports.

**Best Practices:** Never exceed 80% coverage initially, review quarterly, and align commitments with business planning cycles.

**Next Steps:** Analyze historical usage patterns and start with 1-year terms to validate assumptions."""

            base_prompt += examples

        return base_prompt
    
    def _parse_finops_response(self, ai_response: str, context_type: str) -> Dict[str, Any]:
        """Parse FinOps expert response to extract recommendations and related topics."""
        
        # Extract recommendations (look for numbered lists or bullet points)
        recommendations = []
        import re
        
        # Look for numbered recommendations
        numbered_recommendations = re.findall(r'\d+\.\s*(.+?)(?=\d+\.|$)', ai_response, re.DOTALL)
        if numbered_recommendations:
            recommendations.extend([rec.strip().split('\n')[0] for rec in numbered_recommendations[:5]])
        
        # Look for bullet point recommendations
        if not recommendations:
            bullet_recommendations = re.findall(r'[-•*]\s*(.+?)(?=[-•*]|$)', ai_response, re.DOTALL)
            if bullet_recommendations:
                recommendations.extend([rec.strip().split('\n')[0] for rec in bullet_recommendations[:5]])
        
        # Fallback to extracting from "Recommendations" section
        if not recommendations:
            rec_section = re.search(r'(?:Recommendations?|Next Steps?)[:.]?\s*\n(.*?)(?:\n\*\*|\n#|$)', ai_response, re.DOTALL | re.IGNORECASE)
            if rec_section:
                rec_lines = [line.strip() for line in rec_section.group(1).split('\n') if line.strip()]
                recommendations = [line.lstrip('- •*1234567890.') for line in rec_lines[:5] if line.strip()]
        
        # Clean up recommendations
        recommendations = [rec.strip() for rec in recommendations if len(rec.strip()) > 10][:5]
        
        # Default recommendations based on context if none found
        if not recommendations:
            if context_type == "cost_analysis":
                recommendations = [
                    "Analyze cost trends using AWS Cost Explorer",
                    "Set up Cost Anomaly Detection for unexpected charges",
                    "Review top cost drivers by service and account",
                    "Implement cost allocation tags for better visibility"
                ]
            elif context_type == "optimization":
                recommendations = [
                    "Review AWS Compute Optimizer recommendations",
                    "Identify and terminate unused resources",
                    "Consider Reserved Instances for stable workloads",
                    "Implement auto-scaling for variable workloads"
                ]
            elif context_type == "ri_sp":
                recommendations = [
                    "Analyze current RI/SP utilization and coverage",
                    "Use Cost Explorer recommendations for RI purchases",
                    "Consider Savings Plans for flexibility",
                    "Review and optimize existing commitments"
                ]
            else:
                recommendations = [
                    "Implement FinOps best practices for cost visibility",
                    "Set up cost budgets and alerts",
                    "Review and optimize highest cost services",
                    "Establish regular cost review processes"
                ]
        
        # Generate related topics based on context and content
        related_topics = self._generate_related_finops_topics(ai_response, context_type)
        
        return {
            'response': ai_response,
            'recommendations': recommendations,
            'related_topics': related_topics
        }
    
    def _generate_related_finops_topics(self, response_content: str, context_type: str) -> List[str]:
        """Generate related FinOps topics based on response content and context."""
        
        base_topics = {
            "general": [
                "Cost optimization strategies",
                "Reserved Instance planning", 
                "AWS billing best practices",
                "Cost allocation and tagging",
                "FinOps framework implementation"
            ],
            "cost_analysis": [
                "Cost anomaly detection setup",
                "Multi-account cost management",
                "Cost forecasting and budgeting",
                "Service-specific cost optimization",
                "Cost trend analysis techniques"
            ],
            "optimization": [
                "Right-sizing recommendations",
                "Spot instance strategies",
                "Storage cost optimization",
                "Data transfer cost reduction",
                "Auto-scaling optimization"
            ],
            "ri_sp": [
                "RI vs Savings Plan comparison",
                "RI exchange and modification",
                "Savings Plan coverage strategies",
                "Commitment term optimization",
                "RI/SP utilization monitoring"
            ]
        }
        
        # Get base topics for context
        topics = base_topics.get(context_type, base_topics["general"])
        
        # Add content-specific topics based on keywords in response
        content_lower = response_content.lower()
        
        if "ec2" in content_lower or "instance" in content_lower:
            topics.append("EC2 cost optimization")
        if "s3" in content_lower or "storage" in content_lower:
            topics.append("Storage cost management")
        if "lambda" in content_lower or "serverless" in content_lower:
            topics.append("Serverless cost optimization")
        if "rds" in content_lower or "database" in content_lower:
            topics.append("Database cost optimization")
        if "tag" in content_lower:
            topics.append("Cost allocation tagging strategy")
        if "budget" in content_lower:
            topics.append("Budget and alerts setup")
        if "organization" in content_lower:
            topics.append("Multi-account cost management")
        
        # Return unique topics, limited to 7
        return list(set(topics))[:7]
    
    def _parse_structured_response(self, ai_response: str, original_query: str) -> Dict[str, Any]:
        """Parse AI response into structured format."""
        try:
            # Clean the response and try to extract JSON
            import re
            
            # Log the raw response for debugging
            logger.debug(f"Raw AI response: {ai_response[:500]}...")
            
            # First try to find JSON in the response
            json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
            
            if json_match:
                json_str = json_match.group()
                
                # Clean up common issues
                json_str = json_str.strip()
                
                # Try to parse the JSON
                structured = json.loads(json_str)
                
                # Validate it looks like our expected structure
                if not isinstance(structured, dict):
                    logger.error(f"AI response is not a JSON object: {type(structured)}")
                    raise json.JSONDecodeError("Response is not a JSON object", json_str, 0)
                
                # Check if this is a rejection response for non-CUR queries
                if 'error' in structured and structured.get('error') == 'not_cur_related':
                    logger.info(f"Rejected non-CUR query: {original_query}")
                    return self._generate_rejection_response(structured, original_query)
                
                # Validate required fields for valid CUR queries
                required_fields = ['sql_query', 'query_type', 'visualization_type', 'title']
                missing_fields = [field for field in required_fields if field not in structured]
                
                if missing_fields:
                    logger.error(f"AI response missing required fields: {missing_fields}")
                    raise InfralyzerError(f"AI model failed to generate proper response. Missing fields: {missing_fields}. Please try rephrasing your query or try a different model.")
                
                return structured
            else:
                # Check if response contains rejection keywords
                if self._is_rejection_response(ai_response):
                    return self._generate_rejection_response_from_text(ai_response, original_query)
                else:
                    # No valid JSON found and not a rejection - this is an error
                    logger.error(f"AI response does not contain valid JSON structure.")
                    logger.error(f"Full AI response: {ai_response}")
                    logger.error(f"Expected JSON format but got what appears to be: {type(ai_response).__name__}")
                    
                    # Check if it looks like raw SQL
                    if "SELECT" in ai_response.upper() and "FROM" in ai_response.upper():
                        logger.error("AI returned raw SQL instead of JSON - prompt may need adjustment")
                        raise InfralyzerError("AI model returned raw SQL instead of JSON structure. The model may need different parameters or prompt tuning.")
                    else:
                        raise InfralyzerError("AI model failed to generate structured JSON response. Please try rephrasing your query or try a different model.")
                
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse AI response as JSON: {ai_response[:200]}...")
            # Check if response contains rejection keywords
            if self._is_rejection_response(ai_response):
                return self._generate_rejection_response_from_text(ai_response, original_query)
            else:
                # JSON parsing failed and not a rejection - this is an error
                raise InfralyzerError("AI model returned invalid JSON format. Please try rephrasing your query or try a different model.")
    
    def _is_cur_related_query(self, query: str) -> bool:
        """
        Programmatically determine if a query is CUR-related using keyword analysis.
        This supplements the AI model's decision-making.
        """
        query_lower = query.lower()
        
        # Financial/Cost keywords
        financial_keywords = {
            'cost', 'costs', 'charge', 'charges', 'spend', 'spending', 'spent',
            'bill', 'billing', 'expense', 'expenses', 'fee', 'fees',
            'price', 'pricing', 'rate', 'rates', 'money', 'dollar', 'dollars',
            'budget', 'budgets', 'savings', 'discount', 'discounts',
            'credit', 'credits', 'refund', 'refunds', 'payment', 'payments'
        }
        
        # Usage/Analytics keywords  
        usage_keywords = {
            'usage', 'utilization', 'consumption', 'hours', 'gb-hours',
            'instances', 'requests', 'invocations', 'storage', 'transfer',
            'bandwidth', 'data', 'volume', 'capacity'
        }
        
        # Time-based financial keywords
        time_financial_keywords = {
            'monthly cost', 'daily cost', 'weekly cost', 'yearly cost',
            'last month', 'this month', 'previous month', 'current month',
            'last quarter', 'this quarter', 'last year', 'this year',
            'month over month', 'year over year', 'trend', 'trends',
            'forecast', 'projection', 'comparison', 'compare'
        }
        
        # AWS service keywords (when combined with financial context)
        aws_services = {
            'ec2', 'amazon ec2', 's3', 'amazon s3', 'rds', 'amazon rds',
            'lambda', 'aws lambda', 'dynamodb', 'amazon dynamodb',
            'cloudfront', 'amazon cloudfront', 'route53', 'amazon route53',
            'vpc', 'elastic load balancing', 'elb', 'alb', 'nlb',
            'ebs', 'elastic block store', 'efs', 'elastic file system',
            'redshift', 'amazon redshift', 'athena', 'amazon athena',
            'emr', 'elastic mapreduce', 'sagemaker', 'amazon sagemaker',
            'kinesis', 'amazon kinesis', 'sns', 'amazon sns',
            'sqs', 'amazon sqs', 'api gateway', 'amazon api gateway',
            'cloudwatch', 'amazon cloudwatch', 'cloudtrail', 'aws cloudtrail',
            'iam', 'identity and access management', 'kms', 'key management service',
            'secrets manager', 'aws secrets manager', 'systems manager',
            'elastic beanstalk', 'aws elastic beanstalk', 'fargate', 'aws fargate',
            'ecs', 'elastic container service', 'eks', 'elastic kubernetes service',
            'batch', 'aws batch', 'glue', 'aws glue', 'databrew', 'aws databrew'
        }
        
        # Reserved Instance and Savings Plan keywords
        ri_sp_keywords = {
            'reserved instance', 'reserved instances', 'ri', 'ris',
            'savings plan', 'savings plans', 'sp', 'compute savings plan',
            'ec2 instance savings plan', 'sagemaker savings plan'
        }
        
        # FinOps and optimization keywords
        finops_keywords = {
            'optimization', 'optimize', 'waste', 'unused', 'idle',
            'rightsizing', 'right sizing', 'efficiency', 'efficient',
            'allocation', 'chargeback', 'showback', 'tagging',
            'cost center', 'department', 'team costs', 'project costs'
        }
        
        # Check for financial keywords
        has_financial = any(keyword in query_lower for keyword in financial_keywords)
        
        # Check for usage keywords with potential cost context
        has_usage = any(keyword in query_lower for keyword in usage_keywords)
        
        # Check for time-based financial patterns
        has_time_financial = any(phrase in query_lower for phrase in time_financial_keywords)
        
        # Check for AWS services (need financial context too)
        has_aws_service = any(service in query_lower for service in aws_services)
        
        # Check for RI/SP keywords (always financial)
        has_ri_sp = any(keyword in query_lower for keyword in ri_sp_keywords)
        
        # Check for FinOps keywords
        has_finops = any(keyword in query_lower for keyword in finops_keywords)
        
        # Specific patterns that indicate CUR relevance
        cur_patterns = [
            'top 10 service', 'top 5 service', 'most expensive',
            'highest cost', 'lowest cost', 'cost breakdown',
            'cost analysis', 'cost report', 'spending report',
            'bill breakdown', 'invoice details', 'account charges'
        ]
        has_cur_pattern = any(pattern in query_lower for pattern in cur_patterns)
        
        # Decision logic
        if has_ri_sp or has_finops or has_time_financial or has_cur_pattern:
            return True
            
        if has_financial and (has_aws_service or has_usage):
            return True
            
        if has_financial and any(word in query_lower for word in ['aws', 'amazon', 'cloud']):
            return True
            
        # Edge cases that should be considered CUR-related
        if any(phrase in query_lower for phrase in [
            'my charges', 'my costs', 'my bill', 'my spending',
            'account charges', 'account costs', 'service charges'
        ]):
            return True
            
        return False
    
    def _is_rejection_response(self, response_text: str) -> bool:
        """Check if the response contains rejection keywords."""
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
        
        response_lower = response_text.lower()
        return any(keyword in response_lower for keyword in rejection_keywords)
    
    def _generate_rejection_response(self, structured_rejection: Dict[str, Any], original_query: str) -> Dict[str, Any]:
        """Generate proper rejection response for non-CUR queries."""
        return {
            'error': 'not_cur_related',
            'message': structured_rejection.get('message', 'I can only help with AWS cost and usage analysis queries. Please ask questions about AWS costs, billing, usage, or financial optimization instead.'),
            'suggestions': structured_rejection.get('suggestions', [
                'What are my top 5 most expensive AWS services?',
                'Show me EC2 costs by region this month', 
                'Analyze my Reserved Instance utilization',
                'Compare this month\'s costs to last month',
                'Show me S3 storage costs by bucket'
            ]),
            'original_query': original_query,
            'rejected': True,
            'timestamp': datetime.now().isoformat()
        }
    
    def _generate_rejection_response_from_text(self, response_text: str, original_query: str) -> Dict[str, Any]:
        """Generate rejection response when AI doesn't use proper JSON format."""
        return {
            'error': 'not_cur_related',
            'message': 'I can only help with AWS cost and usage analysis queries. Your question doesn\'t appear to be related to AWS costs, billing, usage, or financial optimization. Please ask questions about AWS financial data instead.',
            'suggestions': [
                'What are my top 5 most expensive AWS services?',
                'Show me EC2 costs by region this month',
                'Analyze my Reserved Instance utilization', 
                'Compare this month\'s costs to last month',
                'Show me data transfer costs by service'
            ],
            'original_query': original_query,
            'rejected': True,
            'ai_response': response_text[:200] + '...' if len(response_text) > 200 else response_text,
            'timestamp': datetime.now().isoformat()
        }
    
    def create_cur_knowledge_base(
        self,
        s3_bucket: str,
        s3_prefix: str,
        role_arn: str,
        name: str = "CUR-FinOps-Knowledge"
    ) -> Dict[str, Any]:
        """
        Create a CUR2.0-specific knowledge base with cost optimization knowledge.
        
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