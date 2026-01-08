"""Pydantic schemas for API requests and responses."""
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, datetime
from decimal import Decimal
from app.models.database_models import RiskLevel, ARAPType


# Import schemas
class ImportResponse(BaseModel):
    """Response for import operations."""
    import_id: int
    status: str
    rows_imported: int
    rows_failed: int
    quality_issues: List[dict] = []


# Dashboard schemas
class DateRange(BaseModel):
    """Date range filter."""
    start_date: date
    end_date: date


class DashboardFilters(BaseModel):
    """Dashboard filters."""
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    entity_ids: Optional[List[int]] = None
    project_ids: Optional[List[int]] = None
    category_ids: Optional[List[int]] = None
    counterparty_ids: Optional[List[int]] = None


class BalanceMetric(BaseModel):
    """Balance metric."""
    entity_id: int
    entity_name: str
    balance: Decimal
    currency: str = "RUR"


class CashflowMetric(BaseModel):
    """Cash flow metric."""
    period: str
    inflow: Decimal
    outflow: Decimal
    net_cf: Decimal


class CategoryStructure(BaseModel):
    """Category structure."""
    category_id: int
    category_name: str
    amount: Decimal
    percentage: float
    is_income: bool


class TopCounterparty(BaseModel):
    """Top counterparty."""
    counterparty_id: int
    counterparty_name: str
    total_amount: Decimal
    transaction_count: int
    is_income: bool


class GapAnalysis(BaseModel):
    """Gap analysis (money vs economy)."""
    period: str
    sales_amount: Decimal
    receipts_amount: Decimal
    sales_receipts_gap: Decimal
    purchases_amount: Decimal
    payments_amount: Decimal
    purchases_payments_gap: Decimal


class ARAging(BaseModel):
    """AR aging analysis."""
    counterparty_id: int
    counterparty_name: str
    total_ar: Decimal
    current: Decimal
    overdue_1_30: Decimal
    overdue_31_60: Decimal
    overdue_60_plus: Decimal
    overdue_percentage: float


class DashboardMetrics(BaseModel):
    """Complete dashboard metrics."""
    balances: List[BalanceMetric]
    cashflow: List[CashflowMetric]
    category_structure: List[CategoryStructure]
    top_counterparties: List[TopCounterparty]
    gap_analysis: Optional[List[GapAnalysis]] = None
    ar_aging: Optional[List[ARAging]] = None


# Forecast schemas
class ForecastRequest(BaseModel):
    """Forecast request."""
    horizon_days: int = Field(14, ge=1, le=90)
    entity_ids: Optional[List[int]] = None
    include_uncertainty: bool = True


class ForecastPoint(BaseModel):
    """Single forecast point."""
    date: date
    forecasted_cf: Decimal
    lower_bound: Optional[Decimal] = None
    upper_bound: Optional[Decimal] = None
    confidence: Optional[float] = None


class CashGap(BaseModel):
    """Cash gap (potential shortfall)."""
    date: date
    projected_balance: Decimal
    gap_amount: Decimal
    severity: str  # low, medium, high


class ForecastResponse(BaseModel):
    """Forecast response."""
    forecast_points: List[ForecastPoint]
    cash_gaps: List[CashGap]
    current_balance: Decimal
    forecasted_balance_end: Decimal


# Recommendations schemas
class Recommendation(BaseModel):
    """Single recommendation."""
    id: str
    action: str
    basis: str  # Data-based explanation
    expected_effect: str
    risk: str
    deadline: Optional[date] = None
    priority: int  # 1-10, higher is more important
    category: str  # cash_gap, ar_collection, expense_control, concentration


class RecommendationsResponse(BaseModel):
    """Recommendations response."""
    recommendations: List[Recommendation]
    total_count: int


# Risk scoring schemas
class CashRisk(BaseModel):
    """Cash risk indicators."""
    days_of_cash: float
    probability_of_gap: float
    risk_level: RiskLevel
    indicators: List[str]


class CounterpartyRisk(BaseModel):
    """Counterparty risk indicators."""
    overdue_ar_percentage: float
    concentration_top3: float
    risk_level: RiskLevel
    indicators: List[str]


class AnomalyRisk(BaseModel):
    """Anomaly risk indicators."""
    anomaly_count: int
    uncategorized_percentage: float
    risk_level: RiskLevel
    indicators: List[str]


class RiskScore(BaseModel):
    """Overall risk score."""
    overall_risk: RiskLevel
    cash_risk: CashRisk
    counterparty_risk: CounterpartyRisk
    anomaly_risk: AnomalyRisk
    score_details: dict


# Export schemas
class ExportRequest(BaseModel):
    """Export request."""
    format: str = Field("xlsx", pattern="^(xlsx|pdf)$")
    report_type: str = Field("dashboard", pattern="^(dashboard|forecast|recommendations|risks)$")
    filters: Optional[DashboardFilters] = None
    forecast_horizon: Optional[int] = None


class ExportResponse(BaseModel):
    """Export response."""
    file_path: str
    file_name: str
    file_size: int
    download_url: str


# LLM chat schemas
class ChatMessage(BaseModel):
    """Chat message."""
    role: str  # user, assistant
    content: str
    timestamp: datetime


class ChatRequest(BaseModel):
    """Chat request."""
    message: str
    context: Optional[dict] = None  # Additional context (page, filters, etc.)


class ChatResponse(BaseModel):
    """Chat response."""
    message: str
    sources: Optional[List[str]] = None  # Data sources used
    confidence: Optional[float] = None
