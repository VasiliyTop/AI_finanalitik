"""Import endpoints."""
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from sqlalchemy.orm import Session
from typing import List
import shutil
from pathlib import Path
import logging

from app.database import get_db
from app.config import settings
from app.models.schemas import ImportResponse
from app.models.database_models import ImportLog
from app.ingestion.adesk_parser import AdeskParser
from app.ingestion.onec_parser import OneCParser
from app.ingestion.validator import DataValidator
from app.normalization.normalizer import DataNormalizer
from app.normalization.mapper import CategoryMapper
from app.normalization.quality import QualityAssurance

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/import/adesk", response_model=ImportResponse)
async def import_adesk(
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Import Adesk XLS file."""
    try:
        # Save uploaded file
        file_path = Path(settings.raw_files_dir) / file.filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Create import log
        import_log = ImportLog(
            source_type="adesk",
            file_name=file.filename,
            file_path=str(file_path),
            status="processing"
        )
        db.add(import_log)
        db.commit()
        db.refresh(import_log)
        
        # Parse file
        parser = AdeskParser()
        df = parser.parse(str(file_path))
        df = parser.preprocess(df)
        
        # Validate
        validator = DataValidator()
        df_validated, issues = validator.validate_adesk(df)
        
        # Normalize and import
        normalizer = DataNormalizer(db)
        mapper = CategoryMapper(db)
        
        rows_imported = 0
        rows_failed = 0
        
        from app.models.database_models import FactCashflow
        
        for _, row in df_validated.iterrows():
            try:
                # Normalize
                normalized = normalizer.normalize_adesk_row(row.to_dict())
                
                # Map category
                normalized = mapper.apply_mapping_to_adesk_row(normalized)
                
                # Create fact record
                cashflow = FactCashflow(
                    transaction_date=normalized["transaction_date"],
                    amount=normalized["amount"],
                    currency=normalized.get("currency", "RUR"),
                    exchange_rate=normalized.get("exchange_rate", 1.0),
                    amount_rur=normalized["amount_rur"],
                    entity_id=normalized["entity_id"],
                    counterparty_id=normalized.get("counterparty_id"),
                    project_id=normalized.get("project_id"),
                    category_id=normalized.get("category_id"),
                    description=normalized.get("description"),
                    bank_account=normalized.get("bank_account"),
                    balance=normalized.get("balance"),
                    is_duplicate=normalized.get("is_duplicate", False),
                    is_anomaly=normalized.get("is_anomaly", False),
                    is_uncategorized=normalized.get("is_uncategorized", False),
                    import_batch_id=import_log.id
                )
                db.add(cashflow)
                rows_imported += 1
            except Exception as e:
                logger.error(f"Failed to import row: {e}")
                rows_failed += 1
        
        # Quality assurance
        qa = QualityAssurance(db, import_log.id)
        for issue in issues:
            qa.create_issue(
                issue["type"],
                issue["severity"],
                issue["description"],
                issue["affected_rows"]
            )
        
        # Update import log
        import_log.rows_imported = rows_imported
        import_log.rows_failed = rows_failed
        import_log.status = "completed"
        db.commit()
        
        return ImportResponse(
            import_id=import_log.id,
            status="completed",
            rows_imported=rows_imported,
            rows_failed=rows_failed,
            quality_issues=issues
        )
    
    except Exception as e:
        logger.error(f"Import failed: {e}")
        if 'import_log' in locals():
            import_log.status = "failed"
            import_log.error_message = str(e)
            db.commit()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/import/onec/{source_type}", response_model=ImportResponse)
async def import_onec(
    source_type: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Import 1C file (sales, purchases, arap, mapping)."""
    valid_types = ["sales", "purchases", "arap", "mapping"]
    if source_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid source_type. Must be one of: {valid_types}")
    
    try:
        # Save uploaded file
        file_path = Path(settings.raw_files_dir) / file.filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Create import log
        import_log = ImportLog(
            source_type=f"onec_{source_type}",
            file_name=file.filename,
            file_path=str(file_path),
            status="processing"
        )
        db.add(import_log)
        db.commit()
        db.refresh(import_log)
        
        # Parse file
        parser = OneCParser()
        df = parser.parse(str(file_path), f"onec_{source_type}")
        
        # Preprocess based on type
        if source_type == "sales":
            df = parser.preprocess_sales(df)
            validator = DataValidator()
            df_validated, issues = validator.validate_onec_sales(df)
        elif source_type == "purchases":
            df = parser.preprocess_purchases(df)
            validator = DataValidator()
            df_validated, issues = validator.validate_onec_purchases(df)
        elif source_type == "arap":
            df = parser.preprocess_arap(df)
            validator = DataValidator()
            df_validated, issues = validator.validate_onec_arap(df)
        else:  # mapping
            df = parser.preprocess_mapping(df)
            issues = []
        
        # Normalize and import
        normalizer = DataNormalizer(db)
        mapper = CategoryMapper(db)
        
        rows_imported = 0
        rows_failed = 0
        
        if source_type == "sales":
            from app.models.database_models import FactSales
            for _, row in df_validated.iterrows():
                try:
                    normalized = normalizer.normalize_onec_sales_row(row.to_dict())
                    normalized = mapper.apply_mapping_to_onec_sales_row(normalized)
                    
                    sales = FactSales(**normalized, import_batch_id=import_log.id)
                    db.add(sales)
                    rows_imported += 1
                except Exception as e:
                    logger.error(f"Failed to import row: {e}")
                    rows_failed += 1
        
        elif source_type == "purchases":
            from app.models.database_models import FactPurchases
            for _, row in df_validated.iterrows():
                try:
                    normalized = normalizer.normalize_onec_purchases_row(row.to_dict())
                    normalized = mapper.apply_mapping_to_onec_purchases_row(normalized)
                    
                    purchases = FactPurchases(**normalized, import_batch_id=import_log.id)
                    db.add(purchases)
                    rows_imported += 1
                except Exception as e:
                    logger.error(f"Failed to import row: {e}")
                    rows_failed += 1
        
        elif source_type == "arap":
            from app.models.database_models import SnapshotARAP
            for _, row in df_validated.iterrows():
                try:
                    normalized = normalizer.normalize_onec_arap_row(row.to_dict())
                    
                    arap = SnapshotARAP(**normalized, import_batch_id=import_log.id)
                    db.add(arap)
                    rows_imported += 1
                except Exception as e:
                    logger.error(f"Failed to import row: {e}")
                    rows_failed += 1
        
        # Update import log
        import_log.rows_imported = rows_imported
        import_log.rows_failed = rows_failed
        import_log.status = "completed"
        db.commit()
        
        return ImportResponse(
            import_id=import_log.id,
            status="completed",
            rows_imported=rows_imported,
            rows_failed=rows_failed,
            quality_issues=issues
        )
    
    except Exception as e:
        logger.error(f"Import failed: {e}")
        if 'import_log' in locals():
            import_log.status = "failed"
            import_log.error_message = str(e)
            db.commit()
        raise HTTPException(status_code=500, detail=str(e))
