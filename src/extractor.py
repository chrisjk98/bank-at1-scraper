from __future__ import annotations

import re
from dataclasses import dataclass

from .models import AT1Metrics, ParseResult

AMOUNT_PATTERN = re.compile(r"(?:AT1|Additional Tier 1)[^\n]{0,80}?([0-9][0-9,\.]{1,20})\s*(?:EUR|€|million|mn)?", re.IGNORECASE)
RATIO_PATTERN = re.compile(r"(?:AT1|Additional Tier 1)[^\n]{0,120}?([0-9]{1,2}(?:\.[0-9]{1,2})?)\s*%", re.IGNORECASE)
CET1_PATTERN = re.compile(r"(?:CET1|Common Equity Tier 1)[^\n]{0,120}?([0-9]{1,2}(?:\.[0-9]{1,2})?)\s*%", re.IGNORECASE)
INSTRUMENT_PATTERN = re.compile(r"\b(perpetual|bond|preferred shares?|hybrid|notes?)\b", re.IGNORECASE)


@dataclass
class AT1Extractor:
    def extract(self, parse_result: ParseResult, annual_report_url: str | None = None) -> AT1Metrics:
        text = parse_result.text or ""
        metrics = AT1Metrics(
            gvkey=parse_result.gvkey,
            company_name=parse_result.company_name,
            annual_report_url=annual_report_url,
        )

        amount_match = AMOUNT_PATTERN.search(text)
        ratio_match = RATIO_PATTERN.search(text)
        cet1_match = CET1_PATTERN.search(text)

        if amount_match:
            metrics.at1_capital_eur_mn = self._parse_amount(amount_match.group(1))
        if ratio_match:
            metrics.at1_ratio_pct = float(ratio_match.group(1))
        if cet1_match:
            metrics.cet1_capital = float(cet1_match.group(1))

        instruments = sorted({m.group(1).lower() for m in INSTRUMENT_PATTERN.finditer(text)})
        metrics.at1_instruments = instruments

        if "requirement" in text.lower() and "capital" in text.lower():
            metrics.regulatory_capital_requirements = "Mentioned in report text"

        confidence = 0.0
        confidence += 0.4 if metrics.at1_capital_eur_mn is not None else 0.0
        confidence += 0.3 if metrics.at1_ratio_pct is not None else 0.0
        confidence += 0.2 if metrics.cet1_capital is not None else 0.0
        confidence += 0.1 if metrics.at1_instruments else 0.0
        metrics.confidence_score = min(1.0, confidence)

        if metrics.confidence_score >= 0.8:
            metrics.extraction_status = "success"
        elif metrics.confidence_score >= 0.4:
            metrics.extraction_status = "partial"
        else:
            metrics.extraction_status = "manual_review"
            metrics.notes = "Low-confidence extraction"

        return metrics

    @staticmethod
    def _parse_amount(raw_value: str) -> float:
        cleaned = raw_value.replace(",", "")
        return float(cleaned)
