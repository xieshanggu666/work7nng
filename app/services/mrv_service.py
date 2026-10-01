"""MRV 报告：年度监测报告生成、提交、核查批准与异常冲正。"""

import json
from datetime import datetime

from sqlalchemy.orm import Session

from app.models.allowance import ComplianceRecord
from app.models.report import MrvReport
from app.services.calculation_service import scope_totals
from app.services.quota_service import (
    freeze_allowance_for_report,
    reverse_approved_report,
)


def generate_report(db: Session, company_id: int, year: int) -> MrvReport:
    """汇总年度核算结果生成 MRV 报告（已存在则重建为草稿）。

    已批准报告承载排放确认、冻结和履约结果，不能直接覆盖；确有错误时应先冲正。
    """
    report = db.query(MrvReport).filter(MrvReport.company_id == company_id, MrvReport.year == year).first()
    if report and report.status == "approved":
        raise ValueError("报告已批准并形成履约结果，请先冲正后重新生成")

    totals = scope_totals(db, company_id, year)
    detail = {
        "scope1": totals["1"],
        "scope2": totals["2"],
        "scope3": totals["3"],
        "total": round(totals["1"] + totals["2"] + totals["3"], 4),
    }
    if not report:
        report = MrvReport(company_id=company_id, year=year)
        db.add(report)
    report.scope1 = detail["scope1"]
    report.scope2 = detail["scope2"]
    report.scope3 = detail["scope3"]
    report.total_emission = detail["total"]
    report.report_json = json.dumps(detail, ensure_ascii=False)
    report.status = "draft"
    report.generated_at = datetime.utcnow()
    db.commit()
    db.refresh(report)
    return report


def submit_report(db: Session, report: MrvReport) -> MrvReport:
    """企业提交报告待核查。"""
    if report.status != "draft":
        raise ValueError("仅草稿状态的报告可提交")
    report.status = "submitted"
    db.commit()
    db.refresh(report)
    return report


def approve_report(db: Session, report: MrvReport, verifier_id: int) -> MrvReport:
    """核查员批准报告，并在同一事务中冻结履约配额、建立履约记录。"""
    freeze_allowance_for_report(db, report, verifier_id)
    return report


def reverse_report(db: Session, report: MrvReport, operator_id: int, reason: str) -> ComplianceRecord:
    """冲正批准报告，回滚冻结、清缴、履约状态和配额状态。"""
    return reverse_approved_report(db, report, operator_id, reason)
