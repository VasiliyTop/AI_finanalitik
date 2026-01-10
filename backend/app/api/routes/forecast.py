"""Forecast endpoints."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List

from app.database import get_db
from app.models.schemas import ForecastRequest, ForecastResponse
from app.analytics.forecast import ForecastEngine

router = APIRouter()


@router.get("/forecast/cashflow", response_model=ForecastResponse)
async def get_forecast(
    horizon_days: int = Query(14, ge=1, le=90),
    entity_ids: Optional[List[int]] = Query(None),
    include_uncertainty: bool = Query(True),
    db: Session = Depends(get_db)
):
    """Get cash flow forecast."""
    request = ForecastRequest(
        horizon_days=horizon_days,
        entity_ids=entity_ids,
        include_uncertainty=include_uncertainty
    )
    
    engine = ForecastEngine(db)
    result = engine.forecast_cashflow(request)
    
    return ForecastResponse(**result)
