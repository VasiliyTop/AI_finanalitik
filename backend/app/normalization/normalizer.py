"""Data normalization module."""
from datetime import datetime, date
from decimal import Decimal
from typing import Optional, Dict, Any
import hashlib
import logging
from sqlalchemy.orm import Session

from app.models.database_models import (
    DimEntity, DimCounterparty, DimProject, DimCategory
)

logger = logging.getLogger(__name__)


class DataNormalizer:
    """Normalizes data to standard formats and creates dimension records."""
    
    def __init__(self, db: Session):
        """Initialize normalizer with database session."""
        self.db = db
        self._entity_cache: Dict[str, int] = {}
        self._counterparty_cache: Dict[str, int] = {}
        self._project_cache: Dict[str, int] = {}
        self._category_cache: Dict[str, int] = {}
    
    def normalize_date(self, date_value: Any) -> Optional[date]:
        """Normalize date to ISO date format."""
        if date_value is None:
            return None
        
        if isinstance(date_value, date):
            return date_value
        
        if isinstance(date_value, datetime):
            return date_value.date()
        
        if isinstance(date_value, str):
            # Try common formats
            for fmt in ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(date_value, fmt).date()
                except ValueError:
                    continue
        
        return None
    
    def normalize_currency(self, amount: Decimal, currency: str = "RUR", 
                          exchange_rate: Decimal = Decimal("1.0")) -> Decimal:
        """Normalize amount to RUR."""
        if currency.upper() == "RUR" or currency.upper() == "RUB":
            return amount
        return amount * exchange_rate
    
    def get_or_create_entity(self, entity_name: str, inn: Optional[str] = None) -> int:
        """Get or create entity dimension record."""
        if not entity_name:
            raise ValueError("Entity name cannot be empty")
        
        cache_key = entity_name.lower().strip()
        if cache_key in self._entity_cache:
            return self._entity_cache[cache_key]
        
        # Try to find existing
        entity = self.db.query(DimEntity).filter(
            DimEntity.entity_name == entity_name
        ).first()
        
        if entity:
            self._entity_cache[cache_key] = entity.id
            return entity.id
        
        # Create new
        entity = DimEntity(entity_name=entity_name, inn=inn)
        self.db.add(entity)
        self.db.flush()
        self._entity_cache[cache_key] = entity.id
        return entity.id
    
    def get_or_create_counterparty(self, counterparty_name: str, 
                                   inn: Optional[str] = None) -> Optional[int]:
        """Get or create counterparty dimension record."""
        if not counterparty_name:
            return None
        
        # Create cache key
        cache_key = f"{counterparty_name.lower().strip()}_{inn or ''}"
        if cache_key in self._counterparty_cache:
            return self._counterparty_cache[cache_key]
        
        # Try to find by name or INN
        counterparty = None
        if inn:
            counterparty = self.db.query(DimCounterparty).filter(
                DimCounterparty.inn == inn
            ).first()
        
        if not counterparty:
            counterparty = self.db.query(DimCounterparty).filter(
                DimCounterparty.counterparty_name == counterparty_name
            ).first()
        
        if counterparty:
            self._counterparty_cache[cache_key] = counterparty.id
            return counterparty.id
        
        # Create new
        counterparty = DimCounterparty(
            counterparty_name=counterparty_name,
            inn=inn
        )
        self.db.add(counterparty)
        self.db.flush()
        self._counterparty_cache[cache_key] = counterparty.id
        return counterparty.id
    
    def get_or_create_project(self, project_name: Optional[str]) -> Optional[int]:
        """Get or create project dimension record."""
        if not project_name:
            return None
        
        cache_key = project_name.lower().strip()
        if cache_key in self._project_cache:
            return self._project_cache[cache_key]
        
        # Try to find existing
        project = self.db.query(DimProject).filter(
            DimProject.project_name == project_name
        ).first()
        
        if project:
            self._project_cache[cache_key] = project.id
            return project.id
        
        # Create new
        project = DimProject(project_name=project_name)
        self.db.add(project)
        self.db.flush()
        self._project_cache[cache_key] = project.id
        return project.id
    
    def get_or_create_category(self, category_name: str, 
                              is_income: bool = False,
                              parent_category: Optional[str] = None) -> Optional[int]:
        """Get or create category dimension record."""
        if not category_name:
            return None
        
        cache_key = category_name.lower().strip()
        if cache_key in self._category_cache:
            return self._category_cache[cache_key]
        
        # Try to find existing
        category = self.db.query(DimCategory).filter(
            DimCategory.category_name == category_name
        ).first()
        
        if category:
            self._category_cache[cache_key] = category.id
            return category.id
        
        # Create new
        category = DimCategory(
            category_name=category_name,
            is_income=is_income,
            parent_category=parent_category
        )
        self.db.add(category)
        self.db.flush()
        self._category_cache[cache_key] = category.id
        return category.id
    
    def normalize_adesk_row(self, row: Dict) -> Dict:
        """Normalize a single Adesk row."""
        normalized = {}
        
        # Normalize date
        if "date" in row:
            normalized["transaction_date"] = self.normalize_date(row["date"])
        
        # Normalize amount
        if "amount" in row:
            amount = Decimal(str(row["amount"]))
            currency = row.get("currency", "RUR")
            exchange_rate = Decimal(str(row.get("exchange_rate", "1.0")))
            normalized["amount"] = amount
            normalized["currency"] = currency
            normalized["exchange_rate"] = exchange_rate
            normalized["amount_rur"] = self.normalize_currency(amount, currency, exchange_rate)
        
        # Get or create dimensions
        if "entity" in row and row["entity"]:
            normalized["entity_id"] = self.get_or_create_entity(
                row["entity"],
                row.get("entity_inn")
            )
        
        if "counterparty_name" in row and row["counterparty_name"]:
            normalized["counterparty_id"] = self.get_or_create_counterparty(
                row["counterparty_name"],
                row.get("counterparty_inn")
            )
        
        if "project" in row and row["project"]:
            normalized["project_id"] = self.get_or_create_project(row["project"])
        
        # Other fields
        normalized["description"] = row.get("description")
        normalized["bank_account"] = row.get("bank_account")
        
        if "balance" in row and row["balance"]:
            normalized["balance"] = Decimal(str(row["balance"]))
        
        # Quality flags
        normalized["is_duplicate"] = row.get("is_duplicate", False)
        normalized["is_anomaly"] = row.get("is_anomaly", False)
        normalized["is_uncategorized"] = row.get("is_uncategorized", False)
        
        return normalized
    
    def normalize_onec_sales_row(self, row: Dict) -> Dict:
        """Normalize a single 1C sales row."""
        normalized = {}
        
        # Normalize date
        if "doc_date" in row:
            normalized["doc_date"] = self.normalize_date(row["doc_date"])
        
        if "planned_payment_date" in row and row["planned_payment_date"]:
            normalized["planned_payment_date"] = self.normalize_date(row["planned_payment_date"])
        
        # Normalize amount
        if "revenue_amount" in row:
            normalized["revenue_amount"] = Decimal(str(row["revenue_amount"]))
        
        # Get or create dimensions
        if "entity" in row and row["entity"]:
            normalized["entity_id"] = self.get_or_create_entity(row["entity"])
        
        if "counterparty" in row and row["counterparty"]:
            normalized["counterparty_id"] = self.get_or_create_counterparty(row["counterparty"])
        
        if "project" in row and row["project"]:
            normalized["project_id"] = self.get_or_create_project(row["project"])
        
        normalized["contract"] = row.get("contract")
        
        return normalized
    
    def normalize_onec_purchases_row(self, row: Dict) -> Dict:
        """Normalize a single 1C purchases row."""
        normalized = {}
        
        # Normalize date
        if "doc_date" in row:
            normalized["doc_date"] = self.normalize_date(row["doc_date"])
        
        if "planned_payment_date" in row and row["planned_payment_date"]:
            normalized["planned_payment_date"] = self.normalize_date(row["planned_payment_date"])
        
        # Normalize amount
        if "expense_amount" in row:
            normalized["expense_amount"] = Decimal(str(row["expense_amount"]))
        
        # Get or create dimensions
        if "entity" in row and row["entity"]:
            normalized["entity_id"] = self.get_or_create_entity(row["entity"])
        
        if "counterparty" in row and row["counterparty"]:
            normalized["counterparty_id"] = self.get_or_create_counterparty(row["counterparty"])
        
        if "project" in row and row["project"]:
            normalized["project_id"] = self.get_or_create_project(row["project"])
        
        normalized["contract"] = row.get("contract")
        
        return normalized
    
    def normalize_onec_arap_row(self, row: Dict) -> Dict:
        """Normalize a single 1C AR/AP row."""
        normalized = {}
        
        # Normalize date
        if "snapshot_date" in row:
            normalized["snapshot_date"] = self.normalize_date(row["snapshot_date"])
        
        if "due_date" in row and row["due_date"]:
            normalized["due_date"] = self.normalize_date(row["due_date"])
        
        # Normalize amount
        if "amount" in row:
            normalized["amount"] = Decimal(str(row["amount"]))
        
        # Normalize type
        if "type" in row:
            type_val = str(row["type"]).upper()
            normalized["type"] = "AR" if type_val in ["AR", "ДЗ"] else "AP"
        
        # Get or create dimensions
        if "entity" in row and row["entity"]:
            normalized["entity_id"] = self.get_or_create_entity(row["entity"])
        
        if "counterparty" in row and row["counterparty"]:
            normalized["counterparty_id"] = self.get_or_create_counterparty(row["counterparty"])
        
        if "project" in row and row["project"]:
            normalized["project_id"] = self.get_or_create_project(row["project"])
        
        normalized["contract"] = row.get("contract")
        normalized["overdue_days"] = row.get("overdue_days")
        
        return normalized
