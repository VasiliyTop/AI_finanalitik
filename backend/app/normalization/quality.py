"""Quality assurance module."""
from typing import Dict, List
from datetime import datetime
from decimal import Decimal
import logging
from sqlalchemy.orm import Session

from app.models.database_models import QualityIssue, ImportLog, FactCashflow

logger = logging.getLogger(__name__)


class QualityAssurance:
    """Quality assurance checks and reporting."""
    
    def __init__(self, db: Session, import_batch_id: int):
        """Initialize QA with database session and import batch."""
        self.db = db
        self.import_batch_id = import_batch_id
    
    def create_issue(self, issue_type: str, severity: str, description: str,
                    affected_rows: int = 1, details: Dict = None):
        """Create a quality issue record."""
        issue = QualityIssue(
            import_batch_id=self.import_batch_id,
            issue_type=issue_type,
            severity=severity,
            description=description,
            affected_rows=affected_rows,
            details=str(details) if details else None
        )
        self.db.add(issue)
        return issue
    
    def check_balance_consistency(self, cashflows: List[Dict]) -> List[Dict]:
        """Check balance consistency (cumulative vs reported balance)."""
        issues = []
        
        # Group by account
        accounts = {}
        for cf in cashflows:
            account = cf.get("bank_account", "default")
            if account not in accounts:
                accounts[account] = []
            accounts[account].append(cf)
        
        for account, transactions in accounts.items():
            # Sort by date
            transactions.sort(key=lambda x: x.get("transaction_date"))
            
            # Calculate cumulative balance
            cumulative_balance = Decimal("0")
            for i, cf in enumerate(transactions):
                amount = cf.get("amount_rur", Decimal("0"))
                cumulative_balance += amount
                
                # Compare with reported balance if available
                reported_balance = cf.get("balance")
                if reported_balance is not None:
                    reported_balance = Decimal(str(reported_balance))
                    diff = abs(cumulative_balance - reported_balance)
                    
                    # Allow small differences (rounding)
                    if diff > Decimal("0.01"):
                        issues.append({
                            "type": "balance_mismatch",
                            "severity": "warning",
                            "description": f"Balance mismatch on {account}: cumulative={cumulative_balance}, reported={reported_balance}",
                            "affected_rows": 1,
                            "details": {
                                "account": account,
                                "date": str(cf.get("transaction_date")),
                                "difference": float(diff)
                            }
                        })
        
        return issues
    
    def generate_quality_report(self, cashflows: List[Dict] = None,
                               sales: List[Dict] = None,
                               purchases: List[Dict] = None,
                               arap: List[Dict] = None) -> Dict:
        """Generate comprehensive quality report."""
        report = {
            "total_issues": 0,
            "by_severity": {"error": 0, "warning": 0, "info": 0},
            "by_type": {},
            "issues": []
        }
        
        # Get existing issues from database
        db_issues = self.db.query(QualityIssue).filter(
            QualityIssue.import_batch_id == self.import_batch_id
        ).all()
        
        for issue in db_issues:
            report["total_issues"] += 1
            report["by_severity"][issue.severity] = report["by_severity"].get(issue.severity, 0) + 1
            report["by_type"][issue.issue_type] = report["by_type"].get(issue.issue_type, 0) + 1
            
            report["issues"].append({
                "type": issue.issue_type,
                "severity": issue.severity,
                "description": issue.description,
                "affected_rows": issue.affected_rows,
                "details": issue.details
            })
        
        # Check balance consistency if cashflows provided
        if cashflows:
            balance_issues = self.check_balance_consistency(cashflows)
            for issue in balance_issues:
                self.create_issue(
                    issue["type"],
                    issue["severity"],
                    issue["description"],
                    issue["affected_rows"],
                    issue.get("details")
                )
                report["total_issues"] += 1
                report["by_severity"][issue["severity"]] += 1
                report["by_type"][issue["type"]] = report["by_type"].get(issue["type"], 0) + 1
                report["issues"].append(issue)
        
        return report
    
    def get_uncategorized_count(self) -> int:
        """Get count of uncategorized transactions."""
        count = self.db.query(FactCashflow).filter(
            FactCashflow.import_batch_id == self.import_batch_id,
            FactCashflow.is_uncategorized == True
        ).count()
        return count
    
    def get_duplicate_count(self) -> int:
        """Get count of duplicate transactions."""
        count = self.db.query(FactCashflow).filter(
            FactCashflow.import_batch_id == self.import_batch_id,
            FactCashflow.is_duplicate == True
        ).count()
        return count
    
    def get_anomaly_count(self) -> int:
        """Get count of anomalous transactions."""
        count = self.db.query(FactCashflow).filter(
            FactCashflow.import_batch_id == self.import_batch_id,
            FactCashflow.is_anomaly == True
        ).count()
        return count
