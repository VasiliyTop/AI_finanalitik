"""Analytics metrics calculation module."""
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict
import logging
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
import pandas as pd

from app.models.database_models import (
    FactCashflow, FactSales, FactPurchases, SnapshotARAP,
    DimEntity, DimCounterparty, DimCategory, DimProject
)
from app.models.schemas import DashboardFilters

logger = logging.getLogger(__name__)


class MetricsCalculator:
    """Calculate dashboard metrics."""
    
    def __init__(self, db: Session):
        """Initialize metrics calculator."""
        self.db = db
    
    def get_balances(self, filters: Optional[DashboardFilters] = None, 
                    as_of_date: Optional[date] = None) -> List[Dict]:
        """Get balances by entity as of date."""
        if as_of_date is None:
            as_of_date = date.today()
        
        # Get latest balance for each entity/account
        query = self.db.query(
            FactCashflow.entity_id,
            DimEntity.entity_name,
            func.max(FactCashflow.transaction_date).label("max_date")
        ).join(
            DimEntity, FactCashflow.entity_id == DimEntity.id
        ).filter(
            FactCashflow.transaction_date <= as_of_date
        )
        
        if filters and filters.entity_ids:
            query = query.filter(FactCashflow.entity_id.in_(filters.entity_ids))
        
        query = query.group_by(FactCashflow.entity_id, DimEntity.entity_name)
        latest_dates = query.all()
        
        balances = []
        for entity_id, entity_name, max_date in latest_dates:
            # Get balance for this entity on max_date
            balance_query = self.db.query(
                func.sum(FactCashflow.amount_rur).label("balance")
            ).filter(
                FactCashflow.entity_id == entity_id,
                FactCashflow.transaction_date <= max_date
            )
            
            result = balance_query.first()
            if result and result.balance:
                balances.append({
                    "entity_id": entity_id,
                    "entity_name": entity_name,
                    "balance": result.balance,
                    "currency": "RUR"
                })
        
        return balances
    
    def get_cashflow(self, filters: Optional[DashboardFilters] = None,
                    period: str = "daily") -> List[Dict]:
        """Get cash flow metrics by period."""
        query = self.db.query(FactCashflow)
        
        # Apply filters
        if filters:
            if filters.start_date:
                query = query.filter(FactCashflow.transaction_date >= filters.start_date)
            if filters.end_date:
                query = query.filter(FactCashflow.transaction_date <= filters.end_date)
            if filters.entity_ids:
                query = query.filter(FactCashflow.entity_id.in_(filters.entity_ids))
            if filters.project_ids:
                query = query.filter(FactCashflow.project_id.in_(filters.project_ids))
            if filters.category_ids:
                query = query.filter(FactCashflow.category_id.in_(filters.category_ids))
            if filters.counterparty_ids:
                query = query.filter(FactCashflow.counterparty_id.in_(filters.counterparty_ids))
        
        # Get data
        data = query.all()
        
        # Convert to DataFrame for easier aggregation
        df_data = []
        for cf in data:
            df_data.append({
                "date": cf.transaction_date,
                "amount": float(cf.amount_rur)
            })
        
        if not df_data:
            return []
        
        df = pd.DataFrame(df_data)
        df["date"] = pd.to_datetime(df["date"])
        df["period"] = df["date"] # Ensure period column exists for grouping
        
        # Group by period
        if period == "daily":
            df["period"] = df["date"].dt.date
        elif period == "weekly":
            df["period"] = df["date"].dt.to_period("W").astype(str)
        elif period == "monthly":
            df["period"] = df["date"].dt.to_period("M").astype(str)
        else:
            df["period"] = df["date"].dt.date
        
        # Calculate inflow, outflow, net
        df["inflow"] = df["amount"].apply(lambda x: x if x > 0 else 0)
        df["outflow"] = df["amount"].apply(lambda x: abs(x) if x < 0 else 0)
        
        grouped = df.groupby("period").agg({
            "inflow": "sum",
            "outflow": "sum"
        }).reset_index()
        
        grouped["net_cf"] = grouped["inflow"] - grouped["outflow"]
        
        result = []
        for _, row in grouped.iterrows():
            result.append({
                "period": str(row["period"]),
                "inflow": Decimal(str(row["inflow"])),
                "outflow": Decimal(str(row["outflow"])),
                "net_cf": Decimal(str(row["net_cf"]))
            })
        
        return sorted(result, key=lambda x: x["period"])
    
    def get_category_structure(self, filters: Optional[DashboardFilters] = None,
                               top_n: int = 10) -> List[Dict]:
        """Get category structure (top N categories)."""
        query = self.db.query(
            FactCashflow.category_id,
            DimCategory.category_name,
            DimCategory.is_income,
            func.sum(FactCashflow.amount_rur).label("total_amount")
        ).join(
            DimCategory, FactCashflow.category_id == DimCategory.id
        )
        
        # Apply filters
        if filters:
            if filters.start_date:
                query = query.filter(FactCashflow.transaction_date >= filters.start_date)
            if filters.end_date:
                query = query.filter(FactCashflow.transaction_date <= filters.end_date)
            if filters.entity_ids:
                query = query.filter(FactCashflow.entity_id.in_(filters.entity_ids))
        
        query = query.group_by(
            FactCashflow.category_id,
            DimCategory.category_name,
            DimCategory.is_income
        )
        
        results = query.order_by(func.abs(func.sum(FactCashflow.amount_rur)).desc()).limit(top_n).all()
        
        # Calculate total for percentage
        total = sum(abs(Decimal(str(r.total_amount))) for r in results)
        
        structure = []
        for cat_id, cat_name, is_income, amount in results:
            amount_dec = Decimal(str(amount))
            structure.append({
                "category_id": cat_id,
                "category_name": cat_name,
                "amount": amount_dec,
                "percentage": float(abs(amount_dec) / total * 100) if total > 0 else 0,
                "is_income": is_income
            })
        
        return structure
    
    def get_top_counterparties(self, filters: Optional[DashboardFilters] = None,
                               top_n: int = 10, is_income: Optional[bool] = None) -> List[Dict]:
        """Get top counterparties by transaction volume."""
        query = self.db.query(
            FactCashflow.counterparty_id,
            DimCounterparty.counterparty_name,
            func.sum(FactCashflow.amount_rur).label("total_amount"),
            func.count(FactCashflow.id).label("transaction_count")
        ).join(
            DimCounterparty, FactCashflow.counterparty_id == DimCounterparty.id
        )
        
        # Apply filters
        if filters:
            if filters.start_date:
                query = query.filter(FactCashflow.transaction_date >= filters.start_date)
            if filters.end_date:
                query = query.filter(FactCashflow.transaction_date <= filters.end_date)
            if filters.entity_ids:
                query = query.filter(FactCashflow.entity_id.in_(filters.entity_ids))
        
        # Filter by income/expense
        if is_income is not None:
            if is_income:
                query = query.filter(FactCashflow.amount_rur > 0)
            else:
                query = query.filter(FactCashflow.amount_rur < 0)
        
        query = query.group_by(
            FactCashflow.counterparty_id,
            DimCounterparty.counterparty_name
        )
        
        results = query.order_by(func.abs(func.sum(FactCashflow.amount_rur)).desc()).limit(top_n).all()
        
        top_counterparties = []
        for cp_id, cp_name, amount, count in results:
            top_counterparties.append({
                "counterparty_id": cp_id,
                "counterparty_name": cp_name,
                "total_amount": amount,
                "transaction_count": count,
                "is_income": float(amount) > 0
            })
        
        return top_counterparties
    
    def get_gap_analysis(self, filters: Optional[DashboardFilters] = None) -> List[Dict]:
        """Get gap analysis: sales vs receipts, purchases vs payments."""
        if not filters or not filters.start_date or not filters.end_date:
            return []
        
        # Get sales
        sales_query = self.db.query(
            func.sum(FactSales.revenue_amount).label("total_sales")
        ).filter(
            FactSales.doc_date >= filters.start_date,
            FactSales.doc_date <= filters.end_date
        )
        if filters.entity_ids:
            sales_query = sales_query.filter(FactSales.entity_id.in_(filters.entity_ids))
        total_sales = sales_query.scalar() or Decimal("0")
        
        # Get receipts (positive cashflow)
        receipts_query = self.db.query(
            func.sum(FactCashflow.amount_rur).label("total_receipts")
        ).filter(
            FactCashflow.transaction_date >= filters.start_date,
            FactCashflow.transaction_date <= filters.end_date,
            FactCashflow.amount_rur > 0
        )
        if filters.entity_ids:
            receipts_query = receipts_query.filter(FactCashflow.entity_id.in_(filters.entity_ids))
        total_receipts = receipts_query.scalar() or Decimal("0")
        
        # Get purchases
        purchases_query = self.db.query(
            func.sum(FactPurchases.expense_amount).label("total_purchases")
        ).filter(
            FactPurchases.doc_date >= filters.start_date,
            FactPurchases.doc_date <= filters.end_date
        )
        if filters.entity_ids:
            purchases_query = purchases_query.filter(FactPurchases.entity_id.in_(filters.entity_ids))
        total_purchases = purchases_query.scalar() or Decimal("0")
        
        # Get payments (negative cashflow)
        payments_query = self.db.query(
            func.sum(func.abs(FactCashflow.amount_rur)).label("total_payments")
        ).filter(
            FactCashflow.transaction_date >= filters.start_date,
            FactCashflow.transaction_date <= filters.end_date,
            FactCashflow.amount_rur < 0
        )
        if filters.entity_ids:
            payments_query = payments_query.filter(FactCashflow.entity_id.in_(filters.entity_ids))
        total_payments = payments_query.scalar() or Decimal("0")
        
        return [{
            "period": f"{filters.start_date} to {filters.end_date}",
            "sales_amount": total_sales,
            "receipts_amount": total_receipts,
            "sales_receipts_gap": total_sales - total_receipts,
            "purchases_amount": total_purchases,
            "payments_amount": total_payments,
            "purchases_payments_gap": total_purchases - total_payments
        }]
    
    def get_ar_aging(self, filters: Optional[DashboardFilters] = None) -> List[Dict]:
        """Get AR aging analysis."""
        query = self.db.query(
            SnapshotARAP.counterparty_id,
            DimCounterparty.counterparty_name,
            func.sum(SnapshotARAP.amount).label("total_ar")
        ).join(
            DimCounterparty, SnapshotARAP.counterparty_id == DimCounterparty.id
        ).filter(
            SnapshotARAP.type == "AR"
        )
        
        if filters and filters.entity_ids:
            query = query.filter(SnapshotARAP.entity_id.in_(filters.entity_ids))
        
        query = query.group_by(
            SnapshotARAP.counterparty_id,
            DimCounterparty.counterparty_name
        )
        
        results = query.all()
        
        aging = []
        for cp_id, cp_name, total_ar in results:
            # Get breakdown by overdue days
            detail_query = self.db.query(
                func.sum(SnapshotARAP.amount).label("amount")
            ).filter(
                SnapshotARAP.counterparty_id == cp_id,
                SnapshotARAP.type == "AR"
            )
            
            # Current (not overdue)
            current = detail_query.filter(
                or_(
                    SnapshotARAP.overdue_days.is_(None),
                    SnapshotARAP.overdue_days <= 0
                )
            ).scalar() or Decimal("0")
            
            # 1-30 days
            overdue_1_30 = detail_query.filter(
                and_(
                    SnapshotARAP.overdue_days > 0,
                    SnapshotARAP.overdue_days <= 30
                )
            ).scalar() or Decimal("0")
            
            # 31-60 days
            overdue_31_60 = detail_query.filter(
                and_(
                    SnapshotARAP.overdue_days > 30,
                    SnapshotARAP.overdue_days <= 60
                )
            ).scalar() or Decimal("0")
            
            # 60+ days
            overdue_60_plus = detail_query.filter(
                SnapshotARAP.overdue_days > 60
            ).scalar() or Decimal("0")
            
            total_overdue = overdue_1_30 + overdue_31_60 + overdue_60_plus
            overdue_percentage = float(total_overdue / total_ar * 100) if total_ar > 0 else 0
            
            aging.append({
                "counterparty_id": cp_id,
                "counterparty_name": cp_name,
                "total_ar": total_ar,
                "current": current,
                "overdue_1_30": overdue_1_30,
                "overdue_31_60": overdue_31_60,
                "overdue_60_plus": overdue_60_plus,
                "overdue_percentage": overdue_percentage
            })
        
        return sorted(aging, key=lambda x: x["overdue_percentage"], reverse=True)
