"""Parser for 1C CSV/XLS files."""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from decimal import Decimal
import logging

from app.ingestion.column_mapper import map_columns

logger = logging.getLogger(__name__)


class OneCParser:
    """Parser for 1C export files (sales, purchases, AR/AP, mapping)."""
    
    def __init__(self):
        """Initialize parser."""
        pass
    
    def parse(self, file_path: str, source_type: str) -> pd.DataFrame:
        """Parse 1C file.
        
        Args:
            file_path: Path to file (CSV or XLS)
            source_type: Type of source (onec_sales, onec_purchases, onec_arap, onec_mapping)
        
        Returns:
            DataFrame with normalized column names
        """
        logger.info(f"Parsing 1C file: {file_path} (type: {source_type})")
        
        # Determine file type and read
        file_ext = Path(file_path).suffix.lower()
        
        try:
            if file_ext in [".csv", ".txt"]:
                # Try different encodings
                for encoding in ["utf-8", "cp1251", "windows-1251"]:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding, sep=";")
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    df = pd.read_csv(file_path, encoding="utf-8", sep=",")
            elif file_ext in [".xls", ".xlsx"]:
                df = pd.read_excel(file_path, engine='openpyxl')
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
        except Exception as e:
            raise ValueError(f"Failed to read file: {str(e)}")
        
        if df.empty:
            raise ValueError("File is empty")
        
        # Map columns
        column_mapping = map_columns(df, source_type)
        logger.info(f"Column mapping: {column_mapping}")
        
        # Rename columns
        df_mapped = df.rename(columns={v: k for k, v in column_mapping.items()})
        
        # Select only mapped columns
        available_fields = [f for f in df_mapped.columns if f in column_mapping.values() or f in column_mapping.keys()]
        df_result = df_mapped[[f for f in available_fields if f in df_mapped.columns]].copy()
        
        logger.info(f"Parsed {len(df_result)} rows from 1C file")
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
    
    def preprocess_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess sales dataframe."""
        df_processed = df.copy()
        
        # Normalize dates
        if "doc_date" in df_processed.columns:
            df_processed["doc_date"] = df_processed["doc_date"].apply(self.normalize_date)
            df_processed = df_processed[df_processed["doc_date"].notna()]
        
        if "planned_payment_date" in df_processed.columns:
            df_processed["planned_payment_date"] = df_processed["planned_payment_date"].apply(self.normalize_date)
        
        # Normalize amounts
        if "revenue_amount" in df_processed.columns:
            df_processed["revenue_amount"] = df_processed["revenue_amount"].apply(self.normalize_amount)
            df_processed = df_processed[df_processed["revenue_amount"].notna()]
        
        return df_processed
    
    def preprocess_purchases(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess purchases dataframe."""
        df_processed = df.copy()
        
        # Normalize dates
        if "doc_date" in df_processed.columns:
            df_processed["doc_date"] = df_processed["doc_date"].apply(self.normalize_date)
            df_processed = df_processed[df_processed["doc_date"].notna()]
        
        if "planned_payment_date" in df_processed.columns:
            df_processed["planned_payment_date"] = df_processed["planned_payment_date"].apply(self.normalize_date)
        
        # Normalize amounts
        if "expense_amount" in df_processed.columns:
            df_processed["expense_amount"] = df_processed["expense_amount"].apply(self.normalize_amount)
            df_processed = df_processed[df_processed["expense_amount"].notna()]
        
        return df_processed
    
    def preprocess_arap(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess AR/AP dataframe."""
        df_processed = df.copy()
        
        # Normalize dates
        if "snapshot_date" in df_processed.columns:
            df_processed["snapshot_date"] = df_processed["snapshot_date"].apply(self.normalize_date)
            df_processed = df_processed[df_processed["snapshot_date"].notna()]
        
        if "due_date" in df_processed.columns:
            df_processed["due_date"] = df_processed["due_date"].apply(self.normalize_date)
        
        # Normalize amounts
        if "amount" in df_processed.columns:
            df_processed["amount"] = df_processed["amount"].apply(self.normalize_amount)
            df_processed = df_processed[df_processed["amount"].notna()]
        
        # Normalize type
        if "type" in df_processed.columns:
            df_processed["type"] = df_processed["type"].astype(str).str.upper()
            df_processed["type"] = df_processed["type"].replace({"ДЗ": "AR", "КЗ": "AP", "AR": "AR", "AP": "AP"})
        
        # Normalize overdue_days
        if "overdue_days" in df_processed.columns:
            df_processed["overdue_days"] = pd.to_numeric(df_processed["overdue_days"], errors="coerce")
        
        return df_processed
    
    def preprocess_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess mapping rules dataframe."""
        df_processed = df.copy()
        
        # Normalize text fields
        text_fields = ["source_system", "source_category", "target_category", 
                      "mapping_rule", "counterparty", "text_contains", "regex_pattern"]
        for field in text_fields:
            if field in df_processed.columns:
                df_processed[field] = df_processed[field].astype(str).replace("nan", None)
        
        return df_processed
