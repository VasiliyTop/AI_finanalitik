"""Recommendations engine."""
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Dict, Optional
import logging
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_

from app.models.database_models import (
    FactCashflow, FactSales, FactPurchases, SnapshotARAP,
    DimCounterparty
)
from app.analytics.forecast import ForecastEngine

logger = logging.getLogger(__name__)


class RecommendationsEngine:
    """Generate actionable recommendations."""
    
    def __init__(self, db: Session):
        """Initialize recommendations engine."""
        self.db = db
        self.forecast_engine = ForecastEngine(db)
    
    def generate_recommendations(self, entity_ids: Optional[List[int]] = None) -> List[Dict]:
        """Generate all recommendations."""
        recommendations = []
        
        # Cash gap management
        recommendations.extend(self._cash_gap_recommendations(entity_ids))
        
        # AR collection
        recommendations.extend(self._ar_collection_recommendations(entity_ids))
        
        # Expense control
        recommendations.extend(self._expense_control_recommendations(entity_ids))
        
        # Concentration risks
        recommendations.extend(self._concentration_risk_recommendations(entity_ids))
        
        # Sort by priority
        recommendations.sort(key=lambda x: x["priority"], reverse=True)
        
        return recommendations
    
    def _cash_gap_recommendations(self, entity_ids: Optional[List[int]]) -> List[Dict]:
        """Recommendations for cash gap management."""
        recommendations = []
        
        # Get forecast
        from app.models.schemas import ForecastRequest
        forecast_request = ForecastRequest(horizon_days=30, entity_ids=entity_ids)
        forecast_result = self.forecast_engine.forecast_cashflow(forecast_request)
        
        # Check for gaps
        for gap in forecast_result.get("cash_gaps", []):
            gap_date = gap["date"]
            gap_amount = gap["gap_amount"]
            severity = gap["severity"]
            
            # Find upcoming payments that could be deferred
            payments_query = self.db.query(
                FactPurchases.planned_payment_date,
                func.sum(FactPurchases.expense_amount).label("total")
            ).filter(
                FactPurchases.planned_payment_date.isnot(None),
                FactPurchases.planned_payment_date >= date.today(),
                FactPurchases.planned_payment_date <= gap_date
            )
            
            if entity_ids:
                payments_query = payments_query.filter(FactPurchases.entity_id.in_(entity_ids))
            
            payments_query = payments_query.group_by(FactPurchases.planned_payment_date)
            upcoming_payments = payments_query.all()
            
            deferrable_amount = sum(float(p.total) for p in upcoming_payments if p.total < gap_amount)
            
            recommendations.append({
                "id": f"cash_gap_{gap_date}",
                "action": f"Перенести платежи на сумму {deferrable_amount:,.0f} руб. для покрытия разрыва {gap_date.strftime('%d.%m.%Y')}",
                "basis": f"Прогнозируемый кассовый разрыв {gap_amount:,.0f} руб. на {gap_date.strftime('%d.%m.%Y')}. До этой даты запланированы платежи на {sum(float(p.total) for p in upcoming_payments):,.0f} руб.",
                "expected_effect": f"Устранение разрыва на {gap_date.strftime('%d.%m.%Y')}, сохранение ликвидности",
                "risk": "Необходимо согласовать перенос с поставщиками",
                "deadline": gap_date - timedelta(days=7),
                "priority": 10 if severity == "high" else 7 if severity == "medium" else 5,
                "category": "cash_gap"
            })
        
        return recommendations
    
    def _ar_collection_recommendations(self, entity_ids: Optional[List[int]]) -> List[Dict]:
        """Recommendations for AR collection."""
        recommendations = []
        
        # Get overdue AR
        query = self.db.query(
            SnapshotARAP.counterparty_id,
            DimCounterparty.counterparty_name,
            func.sum(SnapshotARAP.amount).label("total_overdue")
        ).join(
            DimCounterparty, SnapshotARAP.counterparty_id == DimCounterparty.id
        ).filter(
            SnapshotARAP.type == "AR",
            SnapshotARAP.overdue_days > 30
        )
        
        if entity_ids:
            query = query.filter(SnapshotARAP.entity_id.in_(entity_ids))
        
        query = query.group_by(
            SnapshotARAP.counterparty_id,
            DimCounterparty.counterparty_name
        )
        
        overdue_ar = query.order_by(func.sum(SnapshotARAP.amount).desc()).limit(5).all()
        
        for cp_id, cp_name, total_overdue in overdue_ar:
            recommendations.append({
                "id": f"ar_collection_{cp_id}",
                "action": f"Ускорить инкассацию ДЗ от {cp_name} на сумму {total_overdue:,.0f} руб.",
                "basis": f"Просроченная дебиторская задолженность от {cp_name}: {total_overdue:,.0f} руб. (просрочка >30 дней)",
                "expected_effect": f"Высвобождение {total_overdue:,.0f} руб. для улучшения ликвидности",
                "risk": "Необходимо проверить договорные условия и статус документов",
                "deadline": date.today() + timedelta(days=14),
                "priority": 8,
                "category": "ar_collection"
            })
        
        return recommendations
    
    def _expense_control_recommendations(self, entity_ids: Optional[List[int]]) -> List[Dict]:
        """Recommendations for expense control."""
        recommendations = []
        
        # Compare current month vs previous month
        today = date.today()
        current_month_start = date(today.year, today.month, 1)
        prev_month_start = (current_month_start - timedelta(days=1)).replace(day=1)
        prev_month_end = current_month_start - timedelta(days=1)
        
        # Get expenses by category for current month
        current_query = self.db.query(
            func.sum(func.abs(FactCashflow.amount_rur)).label("total")
        ).filter(
            FactCashflow.transaction_date >= current_month_start,
            FactCashflow.amount_rur < 0
        )
        
        if entity_ids:
            current_query = current_query.filter(FactCashflow.entity_id.in_(entity_ids))
        
        current_total = current_query.scalar() or Decimal("0")
        
        # Get expenses for previous month
        prev_query = self.db.query(
            func.sum(func.abs(FactCashflow.amount_rur)).label("total")
        ).filter(
            FactCashflow.transaction_date >= prev_month_start,
            FactCashflow.transaction_date <= prev_month_end,
            FactCashflow.amount_rur < 0
        )
        
        if entity_ids:
            prev_query = prev_query.filter(FactCashflow.entity_id.in_(entity_ids))
        
        prev_total = prev_query.scalar() or Decimal("0")
        
        # Check for significant increase
        if prev_total > 0:
            growth_rate = float((current_total - prev_total) / prev_total * 100)
            
            if growth_rate > 20:  # More than 20% increase
                recommendations.append({
                    "id": "expense_growth",
                    "action": f"Проверить рост расходов: увеличение на {growth_rate:.1f}% по сравнению с предыдущим месяцем",
                    "basis": f"Расходы текущего месяца: {current_total:,.0f} руб., предыдущего: {prev_total:,.0f} руб.",
                    "expected_effect": "Выявление причин роста и оптимизация расходов",
                    "risk": "Может быть сезонный фактор или разовые платежи",
                    "deadline": date.today() + timedelta(days=7),
                    "priority": 6,
                    "category": "expense_control"
                })
        
        return recommendations
    
    def _concentration_risk_recommendations(self, entity_ids: Optional[List[int]]) -> List[Dict]:
        """Recommendations for concentration risks."""
        recommendations = []
        
        # Check customer concentration
        sales_query = self.db.query(
            FactSales.counterparty_id,
            DimCounterparty.counterparty_name,
            func.sum(FactSales.revenue_amount).label("total")
        ).join(
            DimCounterparty, FactSales.counterparty_id == DimCounterparty.id
        ).filter(
            FactSales.doc_date >= date.today() - timedelta(days=90)
        )
        
        if entity_ids:
            sales_query = sales_query.filter(FactSales.entity_id.in_(entity_ids))
        
        sales_query = sales_query.group_by(
            FactSales.counterparty_id,
            DimCounterparty.counterparty_name
        )
        
        top_customers = sales_query.order_by(func.sum(FactSales.revenue_amount).desc()).limit(3).all()
        
        if top_customers:
            total_sales = sum(float(c.total) for c in top_customers)
            
            # Get total sales
            total_query = self.db.query(
                func.sum(FactSales.revenue_amount).label("total")
            ).filter(
                FactSales.doc_date >= date.today() - timedelta(days=90)
            )
            
            if entity_ids:
                total_query = total_query.filter(FactSales.entity_id.in_(entity_ids))
            
            grand_total = total_query.scalar() or Decimal("1")
            concentration = float(total_sales / grand_total * 100) if grand_total > 0 else 0
            
            if concentration > 50:  # More than 50% from top 3
                recommendations.append({
                    "id": "customer_concentration",
                    "action": f"Диверсифицировать клиентскую базу: топ-3 клиента дают {concentration:.1f}% выручки",
                    "basis": f"Концентрация выручки: {concentration:.1f}% от топ-3 клиентов за последние 90 дней",
                    "expected_effect": "Снижение зависимости от ключевых клиентов, снижение риска",
                    "risk": "Потеря одного из ключевых клиентов может существенно повлиять на бизнес",
                    "deadline": None,
                    "priority": 5,
                    "category": "concentration"
                })
        
        return recommendations
