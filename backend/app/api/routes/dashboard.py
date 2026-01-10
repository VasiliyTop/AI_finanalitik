"""Dashboard endpoints."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import date

from app.database import get_db
from app.models.schemas import DashboardFilters, DashboardMetrics
from app.analytics.metrics import MetricsCalculator

router = APIRouter()


@router.get("/dashboard/metrics", response_model=DashboardMetrics)
async def get_dashboard_metrics(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    entity_ids: Optional[List[int]] = Query(None),
    project_ids: Optional[List[int]] = Query(None),
    category_ids: Optional[List[int]] = Query(None),
    counterparty_ids: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db)
):
    """Get dashboard metrics."""
    filters = DashboardFilters(
        start_date=start_date,
        end_date=end_date,
        entity_ids=entity_ids,
        project_ids=project_ids,
        category_ids=category_ids,
        counterparty_ids=counterparty_ids
    )
    
    calculator = MetricsCalculator(db)
    
    balances = calculator.get_balances(filters)
    cashflow = calculator.get_cashflow(filters, period="daily")
    category_structure = calculator.get_category_structure(filters, top_n=10)
    top_counterparties = calculator.get_top_counterparties(filters, top_n=10)
    gap_analysis = calculator.get_gap_analysis(filters) if filters.start_date and filters.end_date else None
    ar_aging = calculator.get_ar_aging(filters)
    
    return DashboardMetrics(
        balances=balances,
        cashflow=cashflow,
        category_structure=category_structure,
        top_counterparties=top_counterparties,
        gap_analysis=gap_analysis,
        ar_aging=ar_aging
    )


@router.get("/dashboard/filters")
async def get_available_filters(
    db: Session = Depends(get_db)
):
    """Get available filter values."""
    from app.models.database_models import DimEntity, DimProject, DimCategory, DimCounterparty
    
    entities = db.query(DimEntity).all()
    projects = db.query(DimProject).all()
    categories = db.query(DimCategory).all()
    counterparties = db.query(DimCounterparty).limit(100).all()
    
    return {
        "entities": [{"id": e.id, "name": e.entity_name} for e in entities],
        "projects": [{"id": p.id, "name": p.project_name} for p in projects],
        "categories": [{"id": c.id, "name": c.category_name} for c in categories],
        "counterparties": [{"id": c.id, "name": c.counterparty_name} for c in counterparties]
    }
