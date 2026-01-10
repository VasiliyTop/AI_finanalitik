"""Category mapping module."""
import yaml
import re
from pathlib import Path
from typing import Dict, List, Optional
import logging
from sqlalchemy.orm import Session

from app.models.database_models import MappingRule, DimCategory

logger = logging.getLogger(__name__)


class CategoryMapper:
    """Maps source categories to target categories using rules."""
    
    def __init__(self, db: Session):
        """Initialize mapper with database session."""
        self.db = db
        self._rules_cache: Optional[List[Dict]] = None
    
    def load_mapping_rules(self) -> List[Dict]:
        """Load mapping rules from YAML config and database."""
        if self._rules_cache is not None:
            return self._rules_cache
        
        rules = []
        
        # Load from YAML config
        config_path = Path(__file__).parent.parent.parent.parent / "config" / "category_mapping.yaml"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                if "mapping_rules" in config:
                    for rule in config["mapping_rules"]:
                        rules.append({
                            "rule_type": rule.get("rule_type", "default"),
                            "source_category": rule.get("source_category", "*"),
                            "target_category": rule.get("target_category"),
                            "priority": rule.get("priority", 10),
                            "counterparty_inn": rule.get("counterparty_inn"),
                            "text_contains": rule.get("text_contains", []),
                            "regex_pattern": rule.get("regex_pattern"),
                        })
        
        # Load from database
        db_rules = self.db.query(MappingRule).filter(MappingRule.is_active == True).all()
        for rule in db_rules:
            rules.append({
                "rule_type": rule.rule_type,
                "source_category": rule.source_category or "*",
                "target_category": rule.target_category,
                "priority": rule.priority,
                "counterparty_inn": rule.counterparty_inn,
                "text_contains": rule.text_contains.split(",") if rule.text_contains else [],
                "regex_pattern": rule.regex_pattern,
            })
        
        # Sort by priority (lower priority = higher precedence)
        rules.sort(key=lambda x: x["priority"])
        
        self._rules_cache = rules
        return rules
    
    def map_category(self, source_category: Optional[str], 
                    description: Optional[str] = None,
                    counterparty_inn: Optional[str] = None) -> Optional[str]:
        """Map source category to target category.
        
        Args:
            source_category: Source category name
            description: Transaction description (for text matching)
            counterparty_inn: Counterparty INN (for counterparty matching)
        
        Returns:
            Target category name or None
        """
        rules = self.load_mapping_rules()
        source_category = source_category or ""
        description = description or ""
        
        for rule in rules:
            # Check if source category matches
            if rule["source_category"] != "*" and rule["source_category"] != source_category:
                continue
            
            # Apply rule based on type
            if rule["rule_type"] == "counterparty":
                if counterparty_inn and rule.get("counterparty_inn") == counterparty_inn:
                    return rule["target_category"]
            
            elif rule["rule_type"] == "text_contains":
                text_contains = rule.get("text_contains", [])
                if isinstance(text_contains, str):
                    text_contains = [text_contains]
                
                combined_text = f"{source_category} {description}".lower()
                if any(text.lower() in combined_text for text in text_contains):
                    return rule["target_category"]
            
            elif rule["rule_type"] == "regex":
                regex_pattern = rule.get("regex_pattern")
                if regex_pattern:
                    combined_text = f"{source_category} {description}"
                    if re.search(regex_pattern, combined_text, re.IGNORECASE):
                        return rule["target_category"]
            
            elif rule["rule_type"] == "default":
                return rule["target_category"]
        
        # No match found
        return None
    
    def get_or_create_category(self, category_name: str, 
                              is_income: bool = False) -> Optional[int]:
        """Get or create category dimension record."""
        if not category_name:
            return None
        
        category = self.db.query(DimCategory).filter(
            DimCategory.category_name == category_name
        ).first()
        
        if category:
            return category.id
        
        # Create new
        category = DimCategory(
            category_name=category_name,
            is_income=is_income
        )
        self.db.add(category)
        self.db.flush()
        return category.id
    
    def apply_mapping_to_adesk_row(self, row: Dict) -> Dict:
        """Apply category mapping to Adesk row."""
        source_category = row.get("cashflow_category")
        description = row.get("description", "")
        counterparty_inn = row.get("counterparty_inn")
        
        # Map category
        target_category = self.map_category(
            source_category,
            description,
            counterparty_inn
        )
        
        if target_category:
            # Get or create category dimension
            category_id = self.get_or_create_category(
                target_category,
                is_income=(row.get("amount", 0) > 0)
            )
            row["category_id"] = category_id
            row["mapped_category"] = target_category
        else:
            # Use default uncategorized
            default_category = "Uncategorized"
            category_id = self.get_or_create_category(default_category, is_income=False)
            row["category_id"] = category_id
            row["mapped_category"] = default_category
            row["is_uncategorized"] = True
        
        return row
    
    def apply_mapping_to_onec_sales_row(self, row: Dict) -> Dict:
        """Apply category mapping to 1C sales row."""
        source_category = row.get("revenue_category")
        description = row.get("description", "")
        counterparty_inn = row.get("counterparty_inn")
        
        # Map category
        target_category = self.map_category(
            source_category,
            description,
            counterparty_inn
        )
        
        if target_category:
            category_id = self.get_or_create_category(target_category, is_income=True)
            row["category_id"] = category_id
        else:
            default_category = "Выручка"
            category_id = self.get_or_create_category(default_category, is_income=True)
            row["category_id"] = category_id
        
        return row
    
    def apply_mapping_to_onec_purchases_row(self, row: Dict) -> Dict:
        """Apply category mapping to 1C purchases row."""
        source_category = row.get("expense_category")
        description = row.get("description", "")
        counterparty_inn = row.get("counterparty_inn")
        
        # Map category
        target_category = self.map_category(
            source_category,
            description,
            counterparty_inn
        )
        
        if target_category:
            category_id = self.get_or_create_category(target_category, is_income=False)
            row["category_id"] = category_id
        else:
            default_category = "Закупки"
            category_id = self.get_or_create_category(default_category, is_income=False)
            row["category_id"] = category_id
        
        return row
