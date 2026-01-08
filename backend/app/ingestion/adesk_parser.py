"""Parser for Adesk XLS files."""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from decimal import Decimal
import logging

from app.ingestion.column_mapper import map_columns

logger = logging.getLogger(__name__)


class AdeskParser:
    """Parser for Adesk XLS 'Журнал операций' files."""
    
    REQUIRED_FIELDS = ["date", "amount"]
    OPTIONAL_FIELDS = [
        "cashflow_category", "description", "counterparty_name",
        "counterparty_inn", "entity", "project", "bank_account", "balance"
    ]
    
    def __init__(self):
        """Initialize parser."""
        pass
    
    def parse(self, file_path: str) -> pd.DataFrame:
        """Parse Adesk XLS file.
        
        Args:
            file_path: Path to XLS file
        
        Returns:
            DataFrame with normalized column names
        """
        logger.info(f"Parsing Adesk file: {file_path}")
        
        # Read Excel file
        try:
            df = pd.read_excel(file_path, engine='openpyxl')
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {str(e)}")
        
        if df.empty:
            raise ValueError("File is empty")
        
        # Map columns
        column_mapping = map_columns(df, "adesk")
        logger.info(f"Column mapping: {column_mapping}")
        
        # Check required fields
        missing_required = [field for field in self.REQUIRED_FIELDS if field not in column_mapping]
        if missing_required:
            raise ValueError(f"Missing required columns: {missing_required}")
        
        # Rename columns
        df_mapped = df.rename(columns={v: k for k, v in column_mapping.items()})
        
        # Select only mapped columns
        all_fields = self.REQUIRED_FIELDS + self.OPTIONAL_FIELDS
        available_fields = [f for f in all_fields if f in df_mapped.columns]
        df_result = df_mapped[available_fields].copy()
        
        # Add missing optional columns as None
        for field in self.OPTIONAL_FIELDS:
            if field not in df_result.columns:
                df_result[field] = None
        
        # Ensure required fields are present
        for field in self.REQUIRED_FIELDS:
            if field not in df_result.columns:
                raise ValueError(f"Required field {field} not found after mapping")
        
        logger.info(f"Parsed {len(df_result)} rows from Adesk file")
        return df_result
    
    def normalize_date(self, date_value) -> Optional[datetime]:
        """Normalize date value to datetime."""
        if pd.isna(date_value):
            return None
        
        if isinstance(date_value, datetime):
            return date_value
        
        if isinstance(date_value, str):
            # Try common date formats
            for fmt in ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(date_value, fmt)
                except ValueError:
                    continue
        
        # Try pandas parsing
        try:
            return pd.to_datetime(date_value)
        except:
            return None
    
    def normalize_amount(self, amount_value) -> Optional[Decimal]:
        """Normalize amount value to Decimal."""
        if pd.isna(amount_value):
            return None
        
        try:
            # Handle string with spaces/commas
            if isinstance(amount_value, str):
                amount_value = amount_value.replace(" ", "").replace(",", ".")
            
            return Decimal(str(float(amount_value)))
        except (ValueError, TypeError):
            return None
    
    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess parsed dataframe.
        
        Args:
            df: Parsed dataframe
        
        Returns:
            Preprocessed dataframe with normalized types
        """
        df_processed = df.copy()
        
        # Normalize dates
        if "date" in df_processed.columns:
            df_processed["date"] = df_processed["date"].apply(self.normalize_date)
            df_processed = df_processed[df_processed["date"].notna()]  # Remove rows with invalid dates
        
        # Normalize amounts
        if "amount" in df_processed.columns:
            df_processed["amount"] = df_processed["amount"].apply(self.normalize_amount)
            df_processed = df_processed[df_processed["amount"].notna()]  # Remove rows with invalid amounts
        
        # Normalize text fields
        text_fields = ["cashflow_category", "description", "counterparty_name", 
                      "entity", "project", "bank_account"]
        for field in text_fields:
            if field in df_processed.columns:
                df_processed[field] = df_processed[field].astype(str).replace("nan", None)
        
        # Normalize balance
        if "balance" in df_processed.columns:
            df_processed["balance"] = df_processed["balance"].apply(self.normalize_amount)
        
        return df_processed
