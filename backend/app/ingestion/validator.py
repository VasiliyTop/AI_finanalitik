"""Data validation module."""
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from decimal import Decimal
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates imported data for quality issues."""
    
    def __init__(self):
        """Initialize validator."""
        pass
    
    def validate_adesk(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """Validate Adesk cashflow data.
        
        Args:
            df: DataFrame with Adesk data
        
        Returns:
            Tuple of (validated_df, issues_list)
        """
        issues = []
        df_validated = df.copy()
        
        # Check required fields
        if "date" not in df_validated.columns:
            issues.append({
                "type": "missing_field",
                "severity": "error",
                "description": "Missing required field: date",
                "affected_rows": len(df_validated)
            })
            return df_validated, issues
        
        if "amount" not in df_validated.columns:
            issues.append({
                "type": "missing_field",
                "severity": "error",
                "description": "Missing required field: amount",
                "affected_rows": len(df_validated)
            })
            return df_validated, issues
        
        # Check for duplicates
        duplicate_mask = self._check_duplicates(df_validated, ["date", "amount", "description"])
        if duplicate_mask.any():
            df_validated.loc[duplicate_mask, "is_duplicate"] = True
            issues.append({
                "type": "duplicate",
                "severity": "warning",
                "description": f"Found {duplicate_mask.sum()} duplicate transactions",
                "affected_rows": int(duplicate_mask.sum())
            })
        else:
            df_validated["is_duplicate"] = False
        
        # Check for anomalies (z-score on amounts)
        if "amount" in df_validated.columns:
            anomaly_mask = self._check_anomalies(df_validated["amount"])
            if anomaly_mask.any():
                df_validated.loc[anomaly_mask, "is_anomaly"] = True
                issues.append({
                    "type": "anomaly",
                    "severity": "warning",
                    "description": f"Found {anomaly_mask.sum()} anomalous amounts",
                    "affected_rows": int(anomaly_mask.sum())
                })
            else:
                df_validated["is_anomaly"] = False
        
        # Check for uncategorized
        if "cashflow_category" in df_validated.columns:
            uncategorized_mask = (
                df_validated["cashflow_category"].isna() | 
                (df_validated["cashflow_category"].astype(str).str.strip() == "") |
                (df_validated["cashflow_category"].astype(str).str.lower() == "nan")
            )
            if uncategorized_mask.any():
                df_validated.loc[uncategorized_mask, "is_uncategorized"] = True
                issues.append({
                    "type": "uncategorized",
                    "severity": "warning",
                    "description": f"Found {uncategorized_mask.sum()} uncategorized transactions",
                    "affected_rows": int(uncategorized_mask.sum())
                })
            else:
                df_validated["is_uncategorized"] = False
        
        # Check date range
        if "date" in df_validated.columns:
            date_issues = self._check_date_range(df_validated["date"])
            if date_issues:
                issues.extend(date_issues)
        
        return df_validated, issues
    
    def validate_onec_sales(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """Validate 1C sales data."""
        issues = []
        df_validated = df.copy()
        
        # Check required fields
        required = ["doc_date", "revenue_amount"]
        for field in required:
            if field not in df_validated.columns:
                issues.append({
                    "type": "missing_field",
                    "severity": "error",
                    "description": f"Missing required field: {field}",
                    "affected_rows": len(df_validated)
                })
        
        if issues:
            return df_validated, issues
        
        # Check duplicates
        duplicate_mask = self._check_duplicates(df_validated, ["doc_date", "revenue_amount", "counterparty"])
        if duplicate_mask.any():
            issues.append({
                "type": "duplicate",
                "severity": "warning",
                "description": f"Found {duplicate_mask.sum()} duplicate sales",
                "affected_rows": int(duplicate_mask.sum())
            })
        
        return df_validated, issues
    
    def validate_onec_purchases(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """Validate 1C purchases data."""
        issues = []
        df_validated = df.copy()
        
        # Check required fields
        required = ["doc_date", "expense_amount"]
        for field in required:
            if field not in df_validated.columns:
                issues.append({
                    "type": "missing_field",
                    "severity": "error",
                    "description": f"Missing required field: {field}",
                    "affected_rows": len(df_validated)
                })
        
        if issues:
            return df_validated, issues
        
        # Check duplicates
        duplicate_mask = self._check_duplicates(df_validated, ["doc_date", "expense_amount", "counterparty"])
        if duplicate_mask.any():
            issues.append({
                "type": "duplicate",
                "severity": "warning",
                "description": f"Found {duplicate_mask.sum()} duplicate purchases",
                "affected_rows": int(duplicate_mask.sum())
            })
        
        return df_validated, issues
    
    def validate_onec_arap(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """Validate 1C AR/AP data."""
        issues = []
        df_validated = df.copy()
        
        # Check required fields
        required = ["snapshot_date", "amount", "type"]
        for field in required:
            if field not in df_validated.columns:
                issues.append({
                    "type": "missing_field",
                    "severity": "error",
                    "description": f"Missing required field: {field}",
                    "affected_rows": len(df_validated)
                })
        
        if issues:
            return df_validated, issues
        
        # Validate type field
        if "type" in df_validated.columns:
            invalid_types = ~df_validated["type"].isin(["AR", "AP"])
            if invalid_types.any():
                issues.append({
                    "type": "invalid_value",
                    "severity": "error",
                    "description": f"Found {invalid_types.sum()} rows with invalid type (must be AR or AP)",
                    "affected_rows": int(invalid_types.sum())
                })
        
        return df_validated, issues
    
    def _check_duplicates(self, df: pd.DataFrame, key_columns: List[str]) -> pd.Series:
        """Check for duplicate rows based on key columns."""
        available_cols = [col for col in key_columns if col in df.columns]
        if not available_cols:
            return pd.Series([False] * len(df), index=df.index)
        
        # Create a composite key
        key = df[available_cols].apply(lambda x: "|".join(x.astype(str)), axis=1)
        duplicates = key.duplicated(keep=False)
        return duplicates
    
    def _check_anomalies(self, series: pd.Series, z_threshold: float = 3.0) -> pd.Series:
        """Check for anomalies using z-score."""
        if len(series) < 3:
            return pd.Series([False] * len(series), index=series.index)
        
        # Convert to numeric
        numeric_series = pd.to_numeric(series, errors="coerce")
        numeric_series = numeric_series.dropna()
        
        if len(numeric_series) < 3:
            return pd.Series([False] * len(series), index=series.index)
        
        # Calculate z-scores
        mean = numeric_series.mean()
        std = numeric_series.std()
        
        if std == 0:
            return pd.Series([False] * len(series), index=series.index)
        
        z_scores = (numeric_series - mean) / std
        anomaly_mask = (z_scores.abs() > z_threshold)
        
        # Map back to original index
        result = pd.Series([False] * len(series), index=series.index)
        result.loc[numeric_series.index] = anomaly_mask
        
        return result
    
    def _check_date_range(self, date_series: pd.Series) -> List[Dict]:
        """Check for date range issues."""
        issues = []
        
        # Check for future dates
        today = datetime.now().date()
        future_dates = date_series.apply(
            lambda x: x.date() > today if isinstance(x, datetime) else False
        )
        if future_dates.any():
            issues.append({
                "type": "future_date",
                "severity": "warning",
                "description": f"Found {future_dates.sum()} transactions with future dates",
                "affected_rows": int(future_dates.sum())
            })
        
        # Check for very old dates (more than 10 years)
        old_date = datetime.now().replace(year=datetime.now().year - 10).date()
        old_dates = date_series.apply(
            lambda x: x.date() < old_date if isinstance(x, datetime) else False
        )
        if old_dates.any():
            issues.append({
                "type": "old_date",
                "severity": "info",
                "description": f"Found {old_dates.sum()} transactions with dates older than 10 years",
                "affected_rows": int(old_dates.sum())
            })
        
        return issues
