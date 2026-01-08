"""Cash flow forecasting module."""
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Optional, Dict
import logging
from sqlalchemy.orm import Session
from sqlalchemy import func
import pandas as pd
import numpy as np

from app.models.database_models import FactCashflow, FactSales, FactPurchases
from app.models.schemas import ForecastRequest, ForecastPoint, CashGap

logger = logging.getLogger(__name__)


class ForecastEngine:
    """Cash flow forecasting engine."""
    
    def __init__(self, db: Session):
        """Initialize forecast engine."""
        self.db = db
    
    def forecast_cashflow(self, request: ForecastRequest) -> Dict:
        """Generate cash flow forecast.
        
        Args:
            request: Forecast request with horizon and filters
        
        Returns:
            Dictionary with forecast points and cash gaps
        """
        # Get historical data
        historical = self._get_historical_cashflow(request)
        
        if len(historical) < 7:
            # Not enough data
            return {
                "forecast_points": [],
                "cash_gaps": [],
                "current_balance": Decimal("0"),
                "forecasted_balance_end": Decimal("0")
            }
        
        # Get current balance
        current_balance = self._get_current_balance(request)
        
        # Generate baseline forecast
        forecast_points = self._generate_baseline_forecast(
            historical, request.horizon_days, request.include_uncertainty
        )
        
        # Incorporate planned payments from 1C
        forecast_points = self._incorporate_planned_payments(
            forecast_points, request, current_balance
        )
        
        # Calculate projected balances
        forecast_points = self._calculate_balances(
            forecast_points, current_balance
        )
        
        # Identify cash gaps
        cash_gaps = self._identify_cash_gaps(forecast_points)
        
        return {
            "forecast_points": forecast_points,
            "cash_gaps": cash_gaps,
            "current_balance": current_balance,
            "forecasted_balance_end": forecast_points[-1]["projected_balance"] if forecast_points else current_balance
        }
    
    def _get_historical_cashflow(self, request: ForecastRequest) -> pd.DataFrame:
        """Get historical cash flow data."""
        query = self.db.query(
            FactCashflow.transaction_date,
            func.sum(FactCashflow.amount_rur).label("daily_cf")
        ).filter(
            FactCashflow.transaction_date >= date.today() - timedelta(days=90)
        )
        
        if request.entity_ids:
            query = query.filter(FactCashflow.entity_id.in_(request.entity_ids))
        
        query = query.group_by(FactCashflow.transaction_date)
        results = query.all()
        
        data = []
        for trans_date, daily_cf in results:
            data.append({
                "date": trans_date,
                "cf": float(daily_cf)
            })
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        
        # Fill missing dates with 0
        date_range = pd.date_range(
            start=df["date"].min(),
            end=df["date"].max(),
            freq="D"
        )
        df_full = pd.DataFrame({"date": date_range})
        df = df_full.merge(df, on="date", how="left").fillna(0)
        
        return df
    
    def _get_current_balance(self, request: ForecastRequest) -> Decimal:
        """Get current balance."""
        query = self.db.query(
            func.sum(FactCashflow.amount_rur).label("balance")
        )
        
        if request.entity_ids:
            query = query.filter(FactCashflow.entity_id.in_(request.entity_ids))
        
        result = query.scalar()
        return Decimal(str(result)) if result else Decimal("0")
    
    def _generate_baseline_forecast(self, historical: pd.DataFrame,
                                    horizon_days: int,
                                    include_uncertainty: bool) -> List[Dict]:
        """Generate baseline forecast using exponential smoothing."""
        if len(historical) < 7:
            return []
        
        # Use exponential smoothing
        alpha = 0.3  # Smoothing parameter
        forecast_values = []
        
        # Calculate baseline (average of last 30 days)
        recent_data = historical.tail(30)["cf"].values
        baseline = np.mean(recent_data)
        
        # Calculate volatility for uncertainty
        volatility = np.std(recent_data) if len(recent_data) > 1 else 0
        
        # Generate forecast
        forecast_dates = []
        start_date = date.today() + timedelta(days=1)
        
        for i in range(horizon_days):
            forecast_date = start_date + timedelta(days=i)
            forecast_dates.append(forecast_date)
            
            # Simple baseline forecast
            forecast_cf = Decimal(str(baseline))
            
            # Add uncertainty bounds
            lower_bound = None
            upper_bound = None
            confidence = None
            
            if include_uncertainty and volatility > 0:
                lower_bound = Decimal(str(baseline - 1.96 * volatility))
                upper_bound = Decimal(str(baseline + 1.96 * volatility))
                confidence = 0.95
            
            forecast_values.append({
                "date": forecast_date,
                "forecasted_cf": forecast_cf,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound,
                "confidence": confidence
            })
        
        return forecast_values
    
    def _incorporate_planned_payments(self, forecast_points: List[Dict],
                                      request: ForecastRequest,
                                      current_balance: Decimal) -> List[Dict]:
        """Incorporate planned payments from 1C."""
        # Get planned sales payments
        sales_query = self.db.query(
            FactSales.planned_payment_date,
            func.sum(FactSales.revenue_amount).label("amount")
        ).filter(
            FactSales.planned_payment_date.isnot(None),
            FactSales.planned_payment_date >= date.today(),
            FactSales.planned_payment_date <= date.today() + timedelta(days=request.horizon_days)
        )
        
        if request.entity_ids:
            sales_query = sales_query.filter(FactSales.entity_id.in_(request.entity_ids))
        
        sales_query = sales_query.group_by(FactSales.planned_payment_date)
        planned_sales = {row.planned_payment_date: row.amount for row in sales_query.all()}
        
        # Get planned purchase payments
        purchases_query = self.db.query(
            FactPurchases.planned_payment_date,
            func.sum(FactPurchases.expense_amount).label("amount")
        ).filter(
            FactPurchases.planned_payment_date.isnot(None),
            FactPurchases.planned_payment_date >= date.today(),
            FactPurchases.planned_payment_date <= date.today() + timedelta(days=request.horizon_days)
        )
        
        if request.entity_ids:
            purchases_query = purchases_query.filter(FactPurchases.entity_id.in_(request.entity_ids))
        
        purchases_query = purchases_query.group_by(FactPurchases.planned_payment_date)
        planned_purchases = {row.planned_payment_date: -row.amount for row in purchases_query.all()}
        
        # Update forecast points with planned payments
        for point in forecast_points:
            forecast_date = point["date"]
            
            # Add planned sales (positive)
            if forecast_date in planned_sales:
                point["forecasted_cf"] += Decimal(str(planned_sales[forecast_date]))
            
            # Add planned purchases (negative)
            if forecast_date in planned_purchases:
                point["forecasted_cf"] += Decimal(str(planned_purchases[forecast_date]))
        
        return forecast_points
    
    def _calculate_balances(self, forecast_points: List[Dict],
                           current_balance: Decimal) -> List[Dict]:
        """Calculate projected balances."""
        running_balance = current_balance
        
        for point in forecast_points:
            running_balance += point["forecasted_cf"]
            point["projected_balance"] = running_balance
        
        return forecast_points
    
    def _identify_cash_gaps(self, forecast_points: List[Dict]) -> List[Dict]:
        """Identify potential cash gaps."""
        gaps = []
        
        for point in forecast_points:
            balance = point.get("projected_balance", Decimal("0"))
            
            if balance < 0:
                gap_amount = abs(balance)
                
                # Determine severity
                if gap_amount < Decimal("100000"):
                    severity = "low"
                elif gap_amount < Decimal("500000"):
                    severity = "medium"
                else:
                    severity = "high"
                
                gaps.append({
                    "date": point["date"],
                    "projected_balance": balance,
                    "gap_amount": gap_amount,
                    "severity": severity
                })
        
        return gaps
