"""Risk scoring module."""
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, List
import logging
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from app.models.database_models import (
    FactCashflow, SnapshotARAP, DimCounterparty, RiskLevel
)

logger = logging.getLogger(__name__)


class RiskScorer:
    """Calculate risk scores."""
    
    def __init__(self, db: Session):
        """Initialize risk scorer."""
        self.db = db
    
    def calculate_risk_score(self, entity_ids: List[int] = None) -> Dict:
        """Calculate overall risk score."""
        cash_risk = self._calculate_cash_risk(entity_ids)
        counterparty_risk = self._calculate_counterparty_risk(entity_ids)
        anomaly_risk = self._calculate_anomaly_risk(entity_ids)
        
        # Determine overall risk (highest of the three)
        risk_levels = {
            RiskLevel.LOW: 1,
            RiskLevel.MEDIUM: 2,
            RiskLevel.HIGH: 3
        }
        
        overall_level = max(
            cash_risk["risk_level"],
            counterparty_risk["risk_level"],
            anomaly_risk["risk_level"],
            key=lambda x: risk_levels.get(x, 0)
        )
        
        return {
            "overall_risk": overall_level,
            "cash_risk": cash_risk,
            "counterparty_risk": counterparty_risk,
            "anomaly_risk": anomaly_risk,
            "score_details": {
                "cash_risk_score": risk_levels.get(cash_risk["risk_level"], 0),
                "counterparty_risk_score": risk_levels.get(counterparty_risk["risk_level"], 0),
                "anomaly_risk_score": risk_levels.get(anomaly_risk["risk_level"], 0)
            }
        }
    
    def _calculate_cash_risk(self, entity_ids: List[int] = None) -> Dict:
        """Calculate cash risk."""
        # Get current balance
        query = self.db.query(
            func.sum(FactCashflow.amount_rur).label("balance")
        )
        
        if entity_ids:
            query = query.filter(FactCashflow.entity_id.in_(entity_ids))
        
        current_balance = query.scalar() or Decimal("0")
        
        # Get average daily outflow
        outflow_query = self.db.query(
            func.avg(func.abs(FactCashflow.amount_rur)).label("avg_outflow")
        ).filter(
            FactCashflow.transaction_date >= date.today() - timedelta(days=30),
            FactCashflow.amount_rur < 0
        )
        
        if entity_ids:
            outflow_query = outflow_query.filter(FactCashflow.entity_id.in_(entity_ids))
        
        avg_outflow = outflow_query.scalar() or Decimal("0")
        
        # Calculate days of cash
        if avg_outflow > 0:
            days_of_cash = float(Decimal(str(current_balance)) / Decimal(str(avg_outflow)))
        else:
            days_of_cash = 999.0
        
        # Calculate probability of gap (simplified)
        # Check forecast for next 30 days
        from app.analytics.forecast import ForecastEngine
        from app.models.schemas import ForecastRequest
        
        forecast_engine = ForecastEngine(self.db)
        forecast_request = ForecastRequest(horizon_days=30, entity_ids=entity_ids)
        forecast_result = forecast_engine.forecast_cashflow(forecast_request)
        
        gaps = forecast_result.get("cash_gaps", [])
        probability_of_gap = len(gaps) / 30.0 if gaps else 0.0
        
        # Determine risk level
        if days_of_cash < 7 or probability_of_gap > 0.3:
            risk_level = RiskLevel.HIGH
        elif days_of_cash < 14 or probability_of_gap > 0.1:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW
        
        indicators = []
        if days_of_cash < 14:
            indicators.append(f"Низкий запас ликвидности: {days_of_cash:.1f} дней")
        if probability_of_gap > 0.1:
            indicators.append(f"Высокая вероятность разрыва: {probability_of_gap*100:.1f}%")
        if current_balance < 0:
            indicators.append("Отрицательный баланс")
        
        return {
            "days_of_cash": days_of_cash,
            "probability_of_gap": probability_of_gap,
            "risk_level": risk_level,
            "indicators": indicators
        }
    
    def _calculate_counterparty_risk(self, entity_ids: List[int] = None) -> Dict:
        """Calculate counterparty risk."""
        # Get overdue AR
        query = self.db.query(
            func.sum(SnapshotARAP.amount).label("overdue_ar"),
            func.sum(SnapshotARAP.amount).filter(
                SnapshotARAP.type == "AR"
            ).label("total_ar")
        ).filter(
            SnapshotARAP.type == "AR",
            SnapshotARAP.overdue_days > 30
        )
        
        if entity_ids:
            query = query.filter(SnapshotARAP.entity_id.in_(entity_ids))
        
        result = query.first()
        overdue_ar = result.overdue_ar or Decimal("0")
        
        # Get total AR
        total_ar_query = self.db.query(
            func.sum(SnapshotARAP.amount).label("total")
        ).filter(
            SnapshotARAP.type == "AR"
        )
        
        if entity_ids:
            total_ar_query = total_ar_query.filter(SnapshotARAP.entity_id.in_(entity_ids))
        
        total_ar = total_ar_query.scalar() or Decimal("1")
        overdue_percentage = float(overdue_ar / total_ar * 100) if total_ar > 0 else 0
        
        # Calculate concentration (top 3 customers)
        concentration_query = self.db.query(
            func.sum(SnapshotARAP.amount).label("top3_ar")
        ).join(
            DimCounterparty, SnapshotARAP.counterparty_id == DimCounterparty.id
        ).filter(
            SnapshotARAP.type == "AR"
        )
        
        if entity_ids:
            concentration_query = concentration_query.filter(SnapshotARAP.entity_id.in_(entity_ids))
        
        # Get top 3
        top3_query = self.db.query(
            SnapshotARAP.counterparty_id,
            func.sum(SnapshotARAP.amount).label("amount")
        ).filter(
            SnapshotARAP.type == "AR"
        )
        
        if entity_ids:
            top3_query = top3_query.filter(SnapshotARAP.entity_id.in_(entity_ids))
        
        top3_query = top3_query.group_by(SnapshotARAP.counterparty_id)
        top3_amounts = [row.amount for row in top3_query.order_by(func.sum(SnapshotARAP.amount).desc()).limit(3).all()]
        top3_total = sum(float(a) for a in top3_amounts)
        concentration_top3 = float(top3_total / total_ar * 100) if total_ar > 0 else 0
        
        # Determine risk level
        if overdue_percentage > 30 or concentration_top3 > 70:
            risk_level = RiskLevel.HIGH
        elif overdue_percentage > 15 or concentration_top3 > 50:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW
        
        indicators = []
        if overdue_percentage > 15:
            indicators.append(f"Высокий процент просроченной ДЗ: {overdue_percentage:.1f}%")
        if concentration_top3 > 50:
            indicators.append(f"Высокая концентрация на топ-3 клиентов: {concentration_top3:.1f}%")
        
        return {
            "overdue_ar_percentage": overdue_percentage,
            "concentration_top3": concentration_top3,
            "risk_level": risk_level,
            "indicators": indicators
        }
    
    def _calculate_anomaly_risk(self, entity_ids: List[int] = None) -> Dict:
        """Calculate anomaly risk."""
        # Count anomalies
        query = self.db.query(
            func.count(FactCashflow.id).label("anomaly_count"),
            func.count(FactCashflow.id).label("total_count")
        ).filter(
            FactCashflow.is_anomaly == True
        )
        
        if entity_ids:
            query = query.filter(FactCashflow.entity_id.in_(entity_ids))
        
        anomaly_result = query.first()
        anomaly_count = anomaly_result.anomaly_count if anomaly_result else 0
        
        # Count uncategorized
        uncategorized_query = self.db.query(
            func.count(FactCashflow.id).label("uncategorized_count")
        ).filter(
            FactCashflow.is_uncategorized == True
        )
        
        if entity_ids:
            uncategorized_query = uncategorized_query.filter(FactCashflow.entity_id.in_(entity_ids))
        
        uncategorized_count = uncategorized_query.scalar() or 0
        
        # Get total transactions
        total_query = self.db.query(
            func.count(FactCashflow.id).label("total")
        )
        
        if entity_ids:
            total_query = total_query.filter(FactCashflow.entity_id.in_(entity_ids))
        
        total_count = total_query.scalar() or 1
        uncategorized_percentage = float(uncategorized_count / total_count * 100)
        
        # Determine risk level
        if anomaly_count > 10 or uncategorized_percentage > 10:
            risk_level = RiskLevel.HIGH
        elif anomaly_count > 5 or uncategorized_percentage > 5:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW
        
        indicators = []
        if anomaly_count > 5:
            indicators.append(f"Обнаружено аномальных транзакций: {anomaly_count}")
        if uncategorized_percentage > 5:
            indicators.append(f"Высокий процент неклассифицированных: {uncategorized_percentage:.1f}%")
        
        return {
            "anomaly_count": anomaly_count,
            "uncategorized_percentage": uncategorized_percentage,
            "risk_level": risk_level,
            "indicators": indicators
        }
