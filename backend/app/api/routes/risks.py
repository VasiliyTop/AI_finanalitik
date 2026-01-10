"""Risks endpoints."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List

from app.database import get_db
from app.models.schemas import RiskScore
from app.analytics.risk_scoring import RiskScorer

router = APIRouter()


@router.get("/risks/score", response_model=RiskScore)
async def get_risk_score(
    entity_ids: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db)
):
    """Get risk score."""
    scorer = RiskScorer(db)
    result = scorer.calculate_risk_score(entity_ids)
    
    return RiskScore(**result)
