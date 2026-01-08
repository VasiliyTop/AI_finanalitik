"""Utility for mapping column names from config."""
import yaml
from pathlib import Path
from typing import Dict, List, Optional


def load_column_mapping() -> Dict:
    """Load column mapping configuration from YAML."""
    config_path = Path(__file__).parent.parent.parent.parent / "config" / "column_mapping.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def find_column(df_columns: List[str], possible_names: List[str]) -> Optional[str]:
    """Find column name in dataframe that matches any of the possible names."""
    df_columns_lower = [col.lower().strip() for col in df_columns]
    for name in possible_names:
        name_lower = name.lower().strip()
        for col in df_columns:
            if col.lower().strip() == name_lower:
                return col
    return None


def map_columns(df, source_type: str) -> Dict[str, str]:
    """Map dataframe columns to standard names based on config.
    
    Args:
        df: DataFrame with columns to map
        source_type: Type of source (adesk, onec_sales, onec_purchases, onec_arap, onec_mapping)
    
    Returns:
        Dictionary mapping standard names to actual column names
    """
    config = load_column_mapping()
    if source_type not in config:
        raise ValueError(f"Unknown source type: {source_type}")
    
    mapping = {}
    source_config = config[source_type]
    
    for standard_name, possible_names in source_config.items():
        found_col = find_column(df.columns.tolist(), possible_names)
        if found_col:
            mapping[standard_name] = found_col
    
    return mapping
