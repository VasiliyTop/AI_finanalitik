"""Recommendations endpoints."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List

from app.database import get_db
from app.models.schemas import RecommendationsResponse, Recommendation
from app.analytics.recommendations import RecommendationsEngine

router = APIRouter()


@router.get("/recommendations", response_model=RecommendationsResponse)
async def get_recommendations(
    entity_ids: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db)
):
    """Get recommendations."""
    engine = RecommendationsEngine(db)
    recommendations = engine.generate_recommendations(entity_ids)
    
    return RecommendationsResponse(
        recommendations=recommendations,
        total_count=len(recommendations)
    )
