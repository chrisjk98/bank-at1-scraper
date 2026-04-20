from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class BankRecord:
    gvkey: str
    company_name: str


@dataclass
class URLResolutionResult:
    gvkey: str
    company_name: str
    annual_report_url: str | None = None
    source: str | None = None
    attempts: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass
class ParseResult:
    gvkey: str
    company_name: str
    text: str
    matched_sections: list[str]
    tables: list[list[list[str]]] = field(default_factory=list)
    ocr_used: bool = False


@dataclass
class AT1Metrics:
    gvkey: str
    company_name: str
    annual_report_url: str | None = None
    at1_capital_eur_mn: float | None = None
    at1_ratio_pct: float | None = None
    at1_instruments: list[str] = field(default_factory=list)
    regulatory_capital_requirements: str | None = None
    cet1_capital: float | None = None
    extraction_status: str = "failed"
    confidence_score: float = 0.0
    notes: str = ""

    def to_row(self) -> dict[str, Any]:
        return {
            "GVKey": self.gvkey,
            "Company Name": self.company_name,
            "Annual Report URL": self.annual_report_url or "",
            "AT1 Capital (EUR millions)": self.at1_capital_eur_mn if self.at1_capital_eur_mn is not None else "",
            "AT1 Ratio (%)": self.at1_ratio_pct if self.at1_ratio_pct is not None else "",
            "AT1 Instruments": "; ".join(self.at1_instruments),
            "CET1 Capital": self.cet1_capital if self.cet1_capital is not None else "",
            "Regulatory Capital Requirements": self.regulatory_capital_requirements or "",
            "Extraction Status": self.extraction_status,
            "Confidence Score": round(self.confidence_score, 3),
            "Notes/Errors": self.notes,
        }
