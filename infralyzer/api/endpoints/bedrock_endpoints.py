"""
AWS Bedrock API endpoints - Comprehensive chatbot and knowledge base management

This module provides REST API endpoints for:
- Multiple AI model support and configuration
- Knowledge base management for CUR-specific data
- Chatbot functionality with context management  
- Structured query generation for frontend applications
"""

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from datetime import datetime

from ...finops_engine import FinOpsEngine
from ...utils.bedrock_handler import BedrockHandler, BedrockModel, ModelConfiguration, KnowledgeBaseConfig
from ...exceptions import InfralyzerError
from ..dependencies import get_finops_engine
from ..base_models import BaseResponse, ResponseStatus
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


# Pydantic Models for API requests/responses

class ModelConfigurationRequest(BaseModel):
    """Request model for AI model configuration."""
    model_config = {"protected_namespaces": ()}
    
    model_id: BedrockModel = Field(description="Bedrock model to use")
    max_tokens: int = Field(4096, ge=1, le=8192, description="Maximum tokens to generate")
    temperature: float = Field(0.1, ge=0.0, le=1.0, description="Randomness in generation")
    top_p: float = Field(0.9, ge=0.0, le=1.0, description="Nucleus sampling parameter")
    top_k: int = Field(250, ge=1, le=500, description="Top-k sampling parameter")
    stop_sequences: Optional[List[str]] = Field(None, description="Stop sequences for generation")


class KnowledgeBaseRequest(BaseModel):
    """Request model for knowledge base creation."""
    name: str = Field(description="Name of the knowledge base")
    description: str = Field(description="Description of the knowledge base")
    role_arn: str = Field(description="IAM role ARN for Bedrock access")
    s3_bucket: str = Field(description="S3 bucket containing knowledge data")
    s3_prefix: str = Field(description="S3 prefix for knowledge data")
    embedding_model_arn: Optional[str] = Field(None, description="Custom embedding model ARN")
    chunk_size: int = Field(300, ge=100, le=1000, description="Text chunk size for embedding")
    overlap_percentage: int = Field(20, ge=0, le=50, description="Chunk overlap percentage")


class ChatRequest(BaseModel):
    """Request model for chatbot interactions."""
    message: str = Field(description="User message", max_length=4000)
    knowledge_base_id: Optional[str] = Field(None, description="Knowledge base ID to use for context")
    conversation_id: Optional[str] = Field(None, description="Conversation ID for context")
    model_configuration: Optional[ModelConfigurationRequest] = Field(None, description="Model configuration")
    include_sources: bool = Field(True, description="Include knowledge sources in response")


class StructuredQueryRequest(BaseModel):
    """Request model for structured query generation."""
    user_query: str = Field(description="Natural language query about costs", max_length=1000)
    model_configuration: Optional[ModelConfigurationRequest] = Field(None, description="Model configuration")
    include_examples: bool = Field(True, description="Include example queries in prompt")
    target_table: str = Field("CUR", description="Target table name for queries")


class ChatResponse(BaseModel):
    """Response model for chat interactions."""
    response: str = Field(description="AI response")
    conversation_id: str = Field(description="Conversation ID")
    knowledge_sources: List[Dict[str, Any]] = Field(description="Knowledge sources used")
    model_used: str = Field(description="Model used for generation")
    timestamp: str = Field(description="Response timestamp")


class StructuredQueryResponse(BaseModel):
    """Response model for structured query generation."""
    structured_query: Dict[str, Any] = Field(description="Generated structured query")
    original_query: str = Field(description="Original natural language query")
    model_used: str = Field(description="Model used for generation")
    confidence: float = Field(description="Confidence score")
    generated_at: str = Field(description="Generation timestamp")


class FinOpsChatRequest(BaseModel):
    """Request model for FinOps expert chatbot interactions."""
    message: str = Field(description="User message about FinOps, cost optimization, or AWS billing", max_length=4000)
    conversation_id: Optional[str] = Field(None, description="Conversation ID for context")
    model_configuration: Optional[ModelConfigurationRequest] = Field(None, description="Model configuration")
    include_examples: bool = Field(True, description="Include FinOps examples and best practices")
    context_type: str = Field("general", description="Context type: general, cost_analysis, optimization, ri_sp")


class FinOpsChatResponse(BaseModel):
    """Response model for FinOps expert chatbot."""
    response: str = Field(description="FinOps expert response")
    conversation_id: str = Field(description="Conversation ID")
    recommendations: List[str] = Field(description="Specific FinOps recommendations")
    related_topics: List[str] = Field(description="Related FinOps topics to explore")
    model_used: str = Field(description="Model used for generation")
    context_type: str = Field(description="Context type used")
    timestamp: str = Field(description="Response timestamp")


# Helper function to get Bedrock handler
def get_bedrock_handler(engine: FinOpsEngine = Depends(get_finops_engine)) -> BedrockHandler:
    """Get configured Bedrock handler."""
    return BedrockHandler(engine.config)


# API Endpoints

@router.get("/bedrock/models", response_model=Dict[str, Any])
async def list_available_models(
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    List all available Bedrock AI models.
    
    **Features:**
    - Comprehensive model catalog with capabilities
    - Model provider information and supported modalities
    - Inference type support details
    """
    try:
        models = bedrock_handler.list_available_models()
        
        # Add our predefined model configurations
        predefined_models = []
        for model in BedrockModel:
            predefined_models.append({
                'model_id': model.value,
                'model_name': model.name,
                'recommended_use': _get_model_recommendation(model),
                'is_predefined': True
            })
        
        return {
            'success': True,
            'available_models': models,
            'predefined_configurations': predefined_models,
            'total_count': len(models),
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing models: {str(e)}")


@router.post("/bedrock/knowledge-base", response_model=Dict[str, Any])
async def create_knowledge_base(
    request: KnowledgeBaseRequest,
    background_tasks: BackgroundTasks,
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    Create a new knowledge base for CUR data.
    
    **Features:**
    - Automated knowledge base setup for cost data
    - Configurable embedding and chunking strategies
    - Asynchronous data ingestion
    """
    try:
        # Create knowledge base configuration
        kb_config = KnowledgeBaseConfig(
            name=request.name,
            description=request.description,
            role_arn=request.role_arn,
            s3_bucket=request.s3_bucket,
            s3_prefix=request.s3_prefix,
            embedding_model_arn=request.embedding_model_arn or "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1",
            chunk_size=request.chunk_size,
            overlap_percentage=request.overlap_percentage
        )
        
        # Create knowledge base
        result = bedrock_handler.create_knowledge_base(kb_config)
        
        # Start data ingestion in background
        background_tasks.add_task(
            _ingest_knowledge_base_data,
            bedrock_handler,
            result['knowledge_base_id']
        )
        
        return {
            'success': True,
            'knowledge_base_id': result['knowledge_base_id'],
            'knowledge_base': result['knowledge_base'],
            'data_source': result['data_source'],
            'status': result['status'],
            'ingestion_status': 'started',
            'created_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating knowledge base: {str(e)}")


@router.get("/bedrock/knowledge-bases", response_model=Dict[str, Any])
async def list_knowledge_bases(
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    List all available knowledge bases.
    
    **Features:**
    - Complete knowledge base inventory
    - Status and metadata information
    - Creation and update timestamps
    """
    try:
        knowledge_bases = bedrock_handler.list_knowledge_bases()
        
        return {
            'success': True,
            'knowledge_bases': knowledge_bases,
            'total_count': len(knowledge_bases),
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing knowledge bases: {str(e)}")


@router.post("/bedrock/knowledge-base/{knowledge_base_id}/ingest", response_model=Dict[str, Any])
async def start_knowledge_base_ingestion(
    knowledge_base_id: str,
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    Start data ingestion for a knowledge base.
    
    **Features:**
    - Manual ingestion trigger
    - Ingestion job status tracking
    - Progress monitoring
    """
    try:
        result = bedrock_handler.ingest_knowledge_base_data(knowledge_base_id)
        
        return {
            'success': True,
            'ingestion_job': result['ingestion_job'],
            'job_id': result['job_id'],
            'status': result['status'],
            'started_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error starting ingestion: {str(e)}")


@router.post("/bedrock/chat", response_model=ChatResponse)
async def chat_with_ai(
    request: ChatRequest,
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    Chat with AI using optional knowledge base context.
    
    **Features:**
    - Conversational AI with cost data context
    - Knowledge base integration for accurate responses
    - Source attribution and citation tracking
    - Conversation history management
    """
    try:
        # Convert request model to internal configuration
        model_config = None
        if request.model_configuration:
            model_config = ModelConfiguration(
                model_id=request.model_configuration.model_id,
                max_tokens=request.model_configuration.max_tokens,
                temperature=request.model_configuration.temperature,
                top_p=request.model_configuration.top_p,
                top_k=request.model_configuration.top_k,
                stop_sequences=request.model_configuration.stop_sequences
            )
        
        if request.knowledge_base_id:
            # Chat with knowledge base context
            result = bedrock_handler.chat_with_knowledge_base(
                message=request.message,
                knowledge_base_id=request.knowledge_base_id,
                conversation_id=request.conversation_id,
                model_config=model_config
            )
        else:
            # Simple chat without knowledge base
            # TODO: Implement simple chat functionality
            raise HTTPException(status_code=400, detail="Knowledge base ID required for chat")
        
        return ChatResponse(
            response=result['response'],
            conversation_id=result['conversation_id'],
            knowledge_sources=result['knowledge_sources'] if request.include_sources else [],
            model_used=result['model_used'],
            timestamp=result['timestamp']
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in chat: {str(e)}")


@router.post("/bedrock/finops-expert", response_model=FinOpsChatResponse)
async def chat_with_finops_expert(
    request: FinOpsChatRequest,
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    Chat with FinOps expert AI for cost optimization and AWS billing guidance.
    
    **Features:**
    - Senior FinOps expert with 10+ years experience
    - Specialized in AWS cost optimization and billing analysis
    - Provides actionable recommendations and best practices
    - Context-aware responses (general, cost_analysis, optimization, ri_sp)
    - Extracts specific recommendations and related topics
    
    **Use Cases:**
    - Cost optimization strategies and recommendations
    - Reserved Instance and Savings Plan guidance
    - AWS billing analysis and cost allocation
    - FinOps framework implementation advice
    - Cost anomaly investigation and resolution
    """
    try:
        # Convert request model to internal configuration
        model_config = None
        if request.model_configuration:
            model_config = ModelConfiguration(
                model_id=request.model_configuration.model_id,
                max_tokens=request.model_configuration.max_tokens,
                temperature=request.model_configuration.temperature,
                top_p=request.model_configuration.top_p,
                top_k=request.model_configuration.top_k,
                stop_sequences=request.model_configuration.stop_sequences
            )
        
        # Call the FinOps expert chat method
        result = bedrock_handler.chat_with_finops_expert(
            message=request.message,
            conversation_id=request.conversation_id,
            model_config=model_config,
            include_examples=request.include_examples,
            context_type=request.context_type
        )
        
        return FinOpsChatResponse(
            response=result['response'],
            conversation_id=result['conversation_id'],
            recommendations=result['recommendations'],
            related_topics=result['related_topics'],
            model_used=result['model_used'],
            context_type=result['context_type'],
            timestamp=result['timestamp']
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except InfralyzerError as e:
        # Handle FinOps expert errors
        raise HTTPException(
            status_code=422,
            detail={
                "error": "finops_expert_error",
                "message": str(e),
                "suggestions": [
                    "Try rephrasing your FinOps question more specifically",
                    "Use a different AI model for better results",
                    "Specify a context type (cost_analysis, optimization, ri_sp)",
                    "Ensure your question is related to AWS costs or FinOps"
                ]
            }
        )
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error in FinOps expert chat: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/bedrock/generate-query", response_model=StructuredQueryResponse)
async def generate_structured_query(
    request: StructuredQueryRequest,
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    Generate structured SQL query from natural language for CUR data analysis.
    
    **Features:**
    - Natural language to SQL conversion
    - CUR-specific query optimization
    - Frontend-ready structured output
    - Visualization suggestions
    """
    try:
        # Convert request model to internal configuration
        model_config = None
        if request.model_configuration:
            model_config = ModelConfiguration(
                model_id=request.model_configuration.model_id,
                max_tokens=request.model_configuration.max_tokens,
                temperature=request.model_configuration.temperature,
                top_p=request.model_configuration.top_p,
                top_k=request.model_configuration.top_k,
                stop_sequences=request.model_configuration.stop_sequences
            )
        
        result = bedrock_handler.generate_cur_structured_query(
            user_query=request.user_query,
            model_config=model_config,
            include_examples=request.include_examples
        )
        
        # Check if the query was rejected for being non-CUR related
        if 'error' in result['structured_query'] and result['structured_query']['error'] == 'not_cur_related':
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "non_cur_query",
                    "message": result['structured_query']['message'],
                    "suggestions": result['structured_query']['suggestions'],
                    "original_query": result['original_query']
                }
            )
        
        return StructuredQueryResponse(
            structured_query=result['structured_query'],
            original_query=result['original_query'],
            model_used=result['model_used'],
            confidence=result['confidence'],
            generated_at=result['generated_at']
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions (like our 400 for non-CUR queries)
        raise
    except InfralyzerError as e:
        # Handle AI model errors (parsing failures, missing fields, etc.)
        raise HTTPException(
            status_code=422, 
            detail={
                "error": "ai_model_error",
                "message": str(e),
                "suggestions": [
                    "Try rephrasing your query more clearly",
                    "Use a different AI model (e.g., Claude 4 Opus for complex queries)",
                    "Break complex queries into simpler parts",
                    "Ensure your query is about AWS costs, usage, or billing"
                ]
            }
        )
    except Exception as e:
        # Handle unexpected system errors
        logger.error(f"Unexpected error in query generation: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/bedrock/cur/knowledge-base", response_model=Dict[str, Any])
async def create_cur_knowledge_base(
    s3_bucket: str = Query(description="S3 bucket with CUR data"),
    s3_prefix: str = Query(description="S3 prefix for CUR data"),
    role_arn: str = Query(description="IAM role ARN for Bedrock"),
    name: str = Query("CUR-FinOps-Knowledge", description="Knowledge base name"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    Create a CUR-specific knowledge base with cost optimization knowledge.
    
    **Features:**
    - Pre-configured for AWS Cost and Usage Reports
    - Optimized chunking for cost data
    - Built-in FinOps best practices
    """
    try:
        result = bedrock_handler.create_cur_knowledge_base(
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            role_arn=role_arn,
            name=name
        )
        
        # Start data ingestion in background
        background_tasks.add_task(
            _ingest_knowledge_base_data,
            bedrock_handler,
            result['knowledge_base_id']
        )
        
        return {
            'success': True,
            'message': 'CUR knowledge base created successfully',
            'knowledge_base_id': result['knowledge_base_id'],
            'knowledge_base': result['knowledge_base'],
            'data_source': result['data_source'],
            'status': result['status'],
            'ingestion_status': 'started',
            'created_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating CUR knowledge base: {str(e)}")


@router.get("/bedrock/conversation/{conversation_id}/history", response_model=Dict[str, Any])
async def get_conversation_history(
    conversation_id: str,
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    Get conversation history for a given conversation ID.
    
    **Features:**
    - Complete conversation context
    - Message timestamps and metadata
    - Source attribution tracking
    """
    try:
        history = bedrock_handler.get_conversation_history(conversation_id)
        
        return {
            'success': True,
            'conversation_id': conversation_id,
            'message_count': len(history),
            'messages': history,
            'retrieved_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving conversation history: {str(e)}")


@router.delete("/bedrock/conversation/{conversation_id}", response_model=Dict[str, Any])
async def clear_conversation(
    conversation_id: str,
    bedrock_handler: BedrockHandler = Depends(get_bedrock_handler)
):
    """
    Clear conversation history for a given conversation ID.
    
    **Features:**
    - Complete conversation cleanup
    - Privacy and data management
    - Confirmation of deletion
    """
    try:
        success = bedrock_handler.clear_conversation(conversation_id)
        
        return {
            'success': success,
            'conversation_id': conversation_id,
            'message': 'Conversation cleared successfully' if success else 'Conversation not found',
            'cleared_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing conversation: {str(e)}")


# Helper functions

async def _ingest_knowledge_base_data(bedrock_handler: BedrockHandler, knowledge_base_id: str):
    """Background task to ingest knowledge base data."""
    try:
        result = bedrock_handler.ingest_knowledge_base_data(knowledge_base_id)
        # Log the result or store it for status tracking
        print(f"Ingestion started for KB {knowledge_base_id}: {result['job_id']}")
    except Exception as e:
        print(f"Error starting ingestion for KB {knowledge_base_id}: {e}")


def _get_model_recommendation(model: BedrockModel) -> str:
    """Get usage recommendation for a model."""
    recommendations = {
        # Claude 4.0 Series (Latest & Most Powerful)
        BedrockModel.CLAUDE_4_OPUS: "🚀 Most powerful model for advanced coding and complex reasoning tasks",
        BedrockModel.CLAUDE_4_SONNET: "⚡ Balanced performance and cost for high-volume production workloads",
        
        # Claude 3.7 Series (Hybrid Reasoning)
        BedrockModel.CLAUDE_3_7_SONNET: "🧠 Hybrid reasoning with extended thinking modes for complex problem-solving",
        
        # Claude 3.5 Series (Optimized Performance)
        BedrockModel.CLAUDE_3_5_SONNET: "💎 Excellent performance for complex reasoning and analysis",
        BedrockModel.CLAUDE_3_5_HAIKU: "⚡ Fast and efficient for quick responses and simple tasks",
        
        # Claude 3.0 Series (Foundational)
        BedrockModel.CLAUDE_3_OPUS: "🎯 High capability for complex analysis and reasoning",
        BedrockModel.CLAUDE_3_SONNET: "⚖️ Good balance of capability and speed for most tasks",
        BedrockModel.CLAUDE_3_HAIKU: "🏃 Fast and efficient for simple queries and summaries",
        
        # Other Models
        BedrockModel.TITAN_TEXT_G1_EXPRESS: "💰 Cost-effective for basic text generation",
        BedrockModel.TITAN_TEXT_G1_LARGE: "🏢 Amazon's flagship model for general text tasks",
        BedrockModel.COHERE_COMMAND_TEXT: "💬 Good for conversational and command-following tasks",
        BedrockModel.COHERE_COMMAND_LIGHT_TEXT: "🪶 Lightweight option for simple conversational tasks",
        BedrockModel.LLAMA2_70B_CHAT: "🔓 Open-source option for conversational AI",
        BedrockModel.LLAMA2_13B_CHAT: "🔓 Smaller open-source option for basic conversations",
        BedrockModel.AI21_J2_ULTRA: "📊 Strong performance for business and analytical tasks",
        BedrockModel.AI21_J2_MID: "📈 Mid-tier performance for general business tasks"
    }
    return recommendations.get(model, "General purpose AI model")