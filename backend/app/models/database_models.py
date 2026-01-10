"""SQLAlchemy database models."""
from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, Boolean, ForeignKey, Text, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
from app.database import Base


class RiskLevel(str, enum.Enum):
    """Risk level enumeration."""
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"


class ARAPType(str, enum.Enum):
    """AR/AP type enumeration."""
    AR = "AR"  # Accounts Receivable (ДЗ)
    AP = "AP"  # Accounts Payable (КЗ)


# Dimension tables
class DimEntity(Base):
    """Legal entity dimension."""
    __tablename__ = "dim_entity"
    
    id = Column(Integer, primary_key=True, index=True)
    entity_name = Column(String(255), nullable=False, unique=True, index=True)
    inn = Column(String(20), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    cashflows = relationship("FactCashflow", back_populates="entity")
    sales = relationship("FactSales", back_populates="entity")
    purchases = relationship("FactPurchases", back_populates="entity")
    arap = relationship("SnapshotARAP", back_populates="entity")


class DimCounterparty(Base):
    """Counterparty dimension."""
    __tablename__ = "dim_counterparty"
    
    id = Column(Integer, primary_key=True, index=True)
    counterparty_name = Column(String(255), nullable=False, index=True)
    inn = Column(String(20), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    cashflows = relationship("FactCashflow", back_populates="counterparty")
    sales = relationship("FactSales", back_populates="counterparty")
    purchases = relationship("FactPurchases", back_populates="counterparty")
    arap = relationship("SnapshotARAP", back_populates="counterparty")


class DimProject(Base):
    """Project dimension."""
    __tablename__ = "dim_project"
    
    id = Column(Integer, primary_key=True, index=True)
    project_name = Column(String(255), nullable=False, unique=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    cashflows = relationship("FactCashflow", back_populates="project")
    sales = relationship("FactSales", back_populates="project")
    purchases = relationship("FactPurchases", back_populates="project")
    arap = relationship("SnapshotARAP", back_populates="project")


class DimCategory(Base):
    """Category dimension."""
    __tablename__ = "dim_category"
    
    id = Column(Integer, primary_key=True, index=True)
    category_name = Column(String(255), nullable=False, unique=True, index=True)
    parent_category = Column(String(255), nullable=True)
    is_income = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    cashflows = relationship("FactCashflow", back_populates="category")
    sales = relationship("FactSales", back_populates="category")
    purchases = relationship("FactPurchases", back_populates="category")


# Fact tables
class FactCashflow(Base):
    """Cash flow transactions from Adesk."""
    __tablename__ = "fact_cashflow"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_date = Column(Date, nullable=False, index=True)
    amount = Column(Numeric(15, 2), nullable=False)
    currency = Column(String(3), default="RUR")
    exchange_rate = Column(Numeric(10, 4), default=1.0)
    amount_rur = Column(Numeric(15, 2), nullable=False)  # Normalized amount
    
    # Foreign keys
    entity_id = Column(Integer, ForeignKey("dim_entity.id"), nullable=False, index=True)
    counterparty_id = Column(Integer, ForeignKey("dim_counterparty.id"), nullable=True, index=True)
    project_id = Column(Integer, ForeignKey("dim_project.id"), nullable=True, index=True)
    category_id = Column(Integer, ForeignKey("dim_category.id"), nullable=True, index=True)
    
    # Additional fields
    description = Column(Text, nullable=True)
    bank_account = Column(String(255), nullable=True)
    balance = Column(Numeric(15, 2), nullable=True)  # Balance after transaction
    
    # Quality flags
    is_duplicate = Column(Boolean, default=False)
    is_anomaly = Column(Boolean, default=False)
    is_uncategorized = Column(Boolean, default=False)
    
    # Metadata
    import_batch_id = Column(Integer, ForeignKey("import_logs.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    entity = relationship("DimEntity", back_populates="cashflows")
    counterparty = relationship("DimCounterparty", back_populates="cashflows")
    project = relationship("DimProject", back_populates="cashflows")
    category = relationship("DimCategory", back_populates="cashflows")
    import_batch = relationship("ImportLog", back_populates="cashflows")


class FactSales(Base):
    """Sales from 1C."""
    __tablename__ = "fact_sales"
    
    id = Column(Integer, primary_key=True, index=True)
    doc_date = Column(Date, nullable=False, index=True)
    revenue_amount = Column(Numeric(15, 2), nullable=False)
    planned_payment_date = Column(Date, nullable=True, index=True)
    
    # Foreign keys
    entity_id = Column(Integer, ForeignKey("dim_entity.id"), nullable=False, index=True)
    counterparty_id = Column(Integer, ForeignKey("dim_counterparty.id"), nullable=True, index=True)
    project_id = Column(Integer, ForeignKey("dim_project.id"), nullable=True, index=True)
    category_id = Column(Integer, ForeignKey("dim_category.id"), nullable=True, index=True)
    
    # Additional fields
    contract = Column(String(255), nullable=True)
    
    # Metadata
    import_batch_id = Column(Integer, ForeignKey("import_logs.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    entity = relationship("DimEntity", back_populates="sales")
    counterparty = relationship("DimCounterparty", back_populates="sales")
    project = relationship("DimProject", back_populates="sales")
    category = relationship("DimCategory", back_populates="sales")
    import_batch = relationship("ImportLog", back_populates="sales")


class FactPurchases(Base):
    """Purchases from 1C."""
    __tablename__ = "fact_purchases"
    
    id = Column(Integer, primary_key=True, index=True)
    doc_date = Column(Date, nullable=False, index=True)
    expense_amount = Column(Numeric(15, 2), nullable=False)
    planned_payment_date = Column(Date, nullable=True, index=True)
    
    # Foreign keys
    entity_id = Column(Integer, ForeignKey("dim_entity.id"), nullable=False, index=True)
    counterparty_id = Column(Integer, ForeignKey("dim_counterparty.id"), nullable=True, index=True)
    project_id = Column(Integer, ForeignKey("dim_project.id"), nullable=True, index=True)
    category_id = Column(Integer, ForeignKey("dim_category.id"), nullable=True, index=True)
    
    # Additional fields
    contract = Column(String(255), nullable=True)
    
    # Metadata
    import_batch_id = Column(Integer, ForeignKey("import_logs.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    entity = relationship("DimEntity", back_populates="purchases")
    counterparty = relationship("DimCounterparty", back_populates="purchases")
    project = relationship("DimProject", back_populates="purchases")
    category = relationship("DimCategory", back_populates="purchases")
    import_batch = relationship("ImportLog", back_populates="purchases")


class SnapshotARAP(Base):
    """AR/AP aging snapshot from 1C."""
    __tablename__ = "snapshot_arap"
    
    id = Column(Integer, primary_key=True, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    amount = Column(Numeric(15, 2), nullable=False)
    type = Column(Enum(ARAPType), nullable=False, index=True)
    overdue_days = Column(Integer, nullable=True)
    due_date = Column(Date, nullable=True, index=True)
    
    # Foreign keys
    entity_id = Column(Integer, ForeignKey("dim_entity.id"), nullable=False, index=True)
    counterparty_id = Column(Integer, ForeignKey("dim_counterparty.id"), nullable=True, index=True)
    project_id = Column(Integer, ForeignKey("dim_project.id"), nullable=True, index=True)
    
    # Additional fields
    contract = Column(String(255), nullable=True)
    
    # Metadata
    import_batch_id = Column(Integer, ForeignKey("import_logs.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    entity = relationship("DimEntity", back_populates="arap")
    counterparty = relationship("DimCounterparty", back_populates="arap")
    project = relationship("DimProject", back_populates="arap")
    import_batch = relationship("ImportLog", back_populates="arap")


# Configuration tables
class MappingRule(Base):
    """Category mapping rules."""
    __tablename__ = "mapping_rules"
    
    id = Column(Integer, primary_key=True, index=True)
    source_system = Column(String(50), nullable=False, index=True)
    source_category = Column(String(255), nullable=True)  # NULL means any
    target_category = Column(String(255), nullable=False)
    rule_type = Column(String(50), nullable=False)  # counterparty, text_contains, regex, default
    priority = Column(Integer, nullable=False, default=10)
    
    # Rule-specific fields
    counterparty_inn = Column(String(20), nullable=True)
    text_contains = Column(Text, nullable=True)  # JSON array of strings
    regex_pattern = Column(String(500), nullable=True)
    
    # Metadata
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


# Logging and quality tables
class ImportLog(Base):
    """Import batch logs."""
    __tablename__ = "import_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    source_type = Column(String(50), nullable=False)  # adesk, onec_sales, onec_purchases, onec_arap
    file_name = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=True)
    rows_imported = Column(Integer, default=0)
    rows_failed = Column(Integer, default=0)
    status = Column(String(50), default="processing")  # processing, completed, failed
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    cashflows = relationship("FactCashflow", back_populates="import_batch")
    sales = relationship("FactSales", back_populates="import_batch")
    purchases = relationship("FactPurchases", back_populates="import_batch")
    arap = relationship("SnapshotARAP", back_populates="import_batch")
    quality_issues = relationship("QualityIssue", back_populates="import_batch")


class QualityIssue(Base):
    """Data quality issues."""
    __tablename__ = "quality_issues"
    
    id = Column(Integer, primary_key=True, index=True)
    import_batch_id = Column(Integer, ForeignKey("import_logs.id"), nullable=False, index=True)
    issue_type = Column(String(50), nullable=False)  # duplicate, anomaly, uncategorized, balance_mismatch
    severity = Column(String(20), default="warning")  # info, warning, error
    description = Column(Text, nullable=False)
    affected_rows = Column(Integer, default=1)
    details = Column(Text, nullable=True)  # JSON with additional details
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    import_batch = relationship("ImportLog", back_populates="quality_issues")
