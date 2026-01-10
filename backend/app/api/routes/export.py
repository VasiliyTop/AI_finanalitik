"""Export endpoints."""
from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import date
from pathlib import Path
import xlsxwriter
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from app.database import get_db
from app.models.schemas import ExportRequest, ExportResponse, DashboardFilters
from app.analytics.metrics import MetricsCalculator
from app.analytics.forecast import ForecastEngine
from app.analytics.recommendations import RecommendationsEngine
from app.analytics.risk_scoring import RiskScorer
from app.config import settings

router = APIRouter()


@router.post("/export/report")
async def export_report(
    request: ExportRequest,
    db: Session = Depends(get_db)
):
    """Export report in XLS or PDF format."""
    output_dir = Path(settings.processed_files_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if request.format == "xlsx":
        file_path = output_dir / f"report_{request.report_type}_{date.today()}.xlsx"
        _export_xlsx(file_path, request, db)
    elif request.format == "pdf":
        file_path = output_dir / f"report_{request.report_type}_{date.today()}.pdf"
        _export_pdf(file_path, request, db)
    else:
        raise HTTPException(status_code=400, detail="Invalid format")
    
    return ExportResponse(
        file_path=str(file_path),
        file_name=file_path.name,
        file_size=file_path.stat().st_size,
        download_url=f"/api/export/download/{file_path.name}"
    )


@router.get("/export/download/{filename}")
async def download_file(filename: str):
    """Download exported file."""
    file_path = Path(settings.processed_files_dir) / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path)


def _export_xlsx(file_path: Path, request: ExportRequest, db: Session):
    """Export to XLSX."""
    workbook = xlsxwriter.Workbook(str(file_path))
    
    if request.report_type == "dashboard":
        worksheet = workbook.add_worksheet("Dashboard")
        calculator = MetricsCalculator(db)
        filters = request.filters
        
        # Write metrics
        row = 0
        worksheet.write(row, 0, "Metric")
        worksheet.write(row, 1, "Value")
        row += 1
        
        balances = calculator.get_balances(filters)
        for balance in balances:
            worksheet.write(row, 0, f"Balance: {balance['entity_name']}")
            worksheet.write(row, 1, float(balance['balance']))
            row += 1
        
        cashflow = calculator.get_cashflow(filters)
        for cf in cashflow:
            worksheet.write(row, 0, f"CF: {cf['period']}")
            worksheet.write(row, 1, float(cf['net_cf']))
            row += 1
    
    elif request.report_type == "forecast":
        worksheet = workbook.add_worksheet("Forecast")
        engine = ForecastEngine(db)
        from app.models.schemas import ForecastRequest
        forecast_request = ForecastRequest(
            horizon_days=request.forecast_horizon or 14,
            entity_ids=request.filters.entity_ids if request.filters else None
        )
        result = engine.forecast_cashflow(forecast_request)
        
        row = 0
        worksheet.write(row, 0, "Date")
        worksheet.write(row, 1, "Forecasted CF")
        worksheet.write(row, 2, "Projected Balance")
        row += 1
        
        for point in result["forecast_points"]:
            worksheet.write(row, 0, str(point["date"]))
            worksheet.write(row, 1, float(point["forecasted_cf"]))
            worksheet.write(row, 2, float(point.get("projected_balance", 0)))
            row += 1
    
    elif request.report_type == "recommendations":
        worksheet = workbook.add_worksheet("Recommendations")
        engine = RecommendationsEngine(db)
        recommendations = engine.generate_recommendations(
            request.filters.entity_ids if request.filters else None
        )
        
        row = 0
        worksheet.write(row, 0, "Action")
        worksheet.write(row, 1, "Basis")
        worksheet.write(row, 2, "Priority")
        row += 1
        
        for rec in recommendations:
            worksheet.write(row, 0, rec["action"])
            worksheet.write(row, 1, rec["basis"])
            worksheet.write(row, 2, rec["priority"])
            row += 1
    
    workbook.close()


def _export_pdf(file_path: Path, request: ExportRequest, db: Session):
    """Export to PDF."""
    c = canvas.Canvas(str(file_path), pagesize=letter)
    width, height = letter
    
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 50, f"Report: {request.report_type}")
    
    if request.report_type == "dashboard":
        calculator = MetricsCalculator(db)
        filters = request.filters
        balances = calculator.get_balances(filters)
        
        y = height - 100
        c.setFont("Helvetica", 12)
        for balance in balances:
            c.drawString(50, y, f"{balance['entity_name']}: {balance['balance']:,.0f}")
            y -= 20
    
    c.save()
