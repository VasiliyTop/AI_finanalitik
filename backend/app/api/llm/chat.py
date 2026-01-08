"""LLM chat endpoint."""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
import logging

from app.database import get_db
from app.models.schemas import ChatRequest, ChatResponse
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/llm/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    db: Session = Depends(get_db)
):
    """LLM chat endpoint for explaining data."""
    # Simple implementation - in production, use RAG with vector store
    try:
        if not settings.openai_api_key:
            return ChatResponse(
                message="LLM integration not configured. Please set OPENAI_API_KEY.",
                sources=None,
                confidence=None
            )
        
        # TODO: Implement RAG system with ChromaDB
        # For now, return a simple response
        response_text = f"I understand you're asking about: {request.message}. This feature requires full RAG implementation with vector store."
        
        return ChatResponse(
            message=response_text,
            sources=None,
            confidence=0.5
        )
    
    except Exception as e:
        logger.error(f"Chat error: {e}")
        return ChatResponse(
            message=f"Error processing request: {str(e)}",
            sources=None,
            confidence=None
        )
